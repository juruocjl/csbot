"""Major 作业通过率走势折线图（matplotlib 无头渲染）。

本模块只做「数据整理 + 画图」，不读数据库、不发消息，所有输入由调用方
（``plugins/major_hw/__init__.py`` 的 ``/作业走势`` 指令）传入，方便离线单测。

数据口径与 ``plugins/cs_server`` 严格一致：

* ``local_storage["hltvresult{event_id}"]`` 是**最新在前（倒序）**的赛果列表
  （见 ``hltv_watcher`` 的 ``snapshot = list(reversed(...))``、``save_current_homework_snapshot``
  用 ``finished_matches[0][3]`` 作为 ``latest_match_id``，以及 ``calc_val`` 里的
  ``result.reverse()``）。因此 ``match_count = k``（k >= 1）对应的赛果是
  ``list(reversed(games))[k - 1]``，与 ``cs_server._major_match_event(games, k)`` 完全对齐。
* 单条赛果为 ``(winner, loser, "13:8", match_id)``；``winner`` 的比分在前。

渲染使用 ``matplotlib.figure.Figure`` + ``FigureCanvasAgg``，不依赖全局 ``plt``，
不修改任何全局 ``rcParams``（字体/字号全部通过 ``FontProperties`` 逐对象传入），
因此可以在 ``asyncio.to_thread`` 里并发调用而不会互相踩踏或阻塞 NoneBot 事件循环。
"""

from __future__ import annotations

import io
import math
import threading
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Iterable, Sequence

import matplotlib

# 无头后端：本模块不使用 pyplot，import 阶段一次性确定后端即可。
try:  # pragma: no cover - 正常环境恒定成功
    matplotlib.use("Agg")
except Exception:  # pragma: no cover - 仅防御性兜底
    pass

from matplotlib import font_manager, ticker
from matplotlib.backends.backend_agg import FigureCanvasAgg
from matplotlib.colors import hsv_to_rgb
from matplotlib.figure import Figure
from matplotlib.font_manager import FontProperties

# 负号显示为方块的问题（本项目所有刻度均由本模块自行格式化，仍按规范显式关闭）
matplotlib.rcParams["axes.unicode_minus"] = False

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_FONT_PATH = REPO_ROOT / "assets" / "merged.ttf"
DEFAULT_DPI = 160

#: 画布配色（柔和浅色，投影/群里都清楚）
FIG_BG = "#f7f9fc"
AX_BG = "#ffffff"
GRID_COLOR = "#b9c3ce"
SPINE_COLOR = "#c9d2db"
TEXT_COLOR = "#1f2b38"
MUTED_TEXT = "#6b7a8b"

SCALE_LINEAR = "linear"
SCALE_LOG = "log"

PALETTE_MAPS = ("tab20", "tab20b", "tab20c")
MAX_NAME_CHARS = 12

_FONT_CACHE: dict[str, FontProperties | None] = {}
_FONT_LOCK = threading.Lock()


# --------------------------------------------------------------------------- #
# 数据模型
# --------------------------------------------------------------------------- #
@dataclass
class HomeworkSeries:
    """单个玩家的概率序列，``points`` 按 ``match_count`` 升序且已去重。"""

    uid: str
    name: str
    points: list[tuple[int, float]] = field(default_factory=list)

    @property
    def first_match_count(self) -> int | None:
        return self.points[0][0] if self.points else None

    @property
    def last_match_count(self) -> int | None:
        return self.points[-1][0] if self.points else None

    @property
    def latest_winrate(self) -> float | None:
        return self.points[-1][1] if self.points else None


# --------------------------------------------------------------------------- #
# 纯数据整理（可单测）
# --------------------------------------------------------------------------- #
def finite_float(value) -> float | None:
    """把任意值规约为有限 float；``NaN`` / ``inf`` / 非数字一律返回 ``None``。"""

    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(number):
        return None
    return number


def chronological_games(games: Iterable | None) -> list:
    """倒序存储的赛果 → 时间正序列表（与 ``cs_server`` 的 ``list(reversed(games))`` 一致）。"""

    if not games:
        return []
    return list(reversed(list(games)))


def build_series(
    rows: Iterable,
    names: dict[str, str] | None = None,
) -> list[HomeworkSeries]:
    """把 ``MajorHWSnapshot`` 行整理成每人一条序列。

    * 同一 ``uid`` 同一 ``match_count`` 有多条（赛中改作业）时取 ``created_at`` 最新的一条；
    * ``winrate`` 为 ``NaN`` / 非有限值的点直接忽略；
    * 中途入场玩家只从自己首个快照点开始，绝不补 0、也不要求长度一致。
    """

    names = names or {}
    buckets: dict[str, dict[int, tuple[int, float]]] = {}
    for row in rows:
        uid = str(getattr(row, "uid", "") or "").strip()
        if not uid:
            continue
        try:
            match_count = int(getattr(row, "match_count"))
        except (TypeError, ValueError):
            continue
        if match_count < 0:
            continue
        winrate = finite_float(getattr(row, "winrate", None))
        if winrate is None:
            continue
        try:
            created_at = int(getattr(row, "created_at", 0) or 0)
        except (TypeError, ValueError):
            created_at = 0

        bucket = buckets.setdefault(uid, {})
        previous = bucket.get(match_count)
        if previous is None or created_at >= previous[0]:
            bucket[match_count] = (created_at, winrate)

    series: list[HomeworkSeries] = []
    for uid, bucket in buckets.items():
        points = sorted((match_count, value) for match_count, (_, value) in bucket.items())
        series.append(HomeworkSeries(uid=uid, name=str(names.get(uid) or uid), points=points))
    return series


def sort_series(series: Sequence[HomeworkSeries]) -> list[HomeworkSeries]:
    """按「最新一场通过率」降序排序，同率按昵称稳定排序，保证图例顺序确定。"""

    return sorted(
        series,
        key=lambda item: (-(item.latest_winrate if item.latest_winrate is not None else -1.0), item.name),
    )


def display_name(name: str, limit: int = MAX_NAME_CHARS) -> str:
    """图例昵称截断，避免长群名片把图例撑爆。"""

    text = str(name or "").strip() or "未知"
    if len(text) <= limit:
        return text
    return text[: max(1, limit - 1)] + "…"


def first_zero_index(series: HomeworkSeries) -> int | None:
    """首次「从 >0 掉到 0」的 ``match_count``；从未归零返回 ``None``。"""

    previous: float | None = None
    for match_count, winrate in series.points:
        if winrate <= 0.0 and previous is not None and previous > 0.0:
            return match_count
        previous = winrate
    return None


def match_label(match_count: int, game, resolver: Callable[[str], str] | None = None) -> str:
    """第 ``match_count`` 场的 X 轴标签，例如 ``1. FaZe 13:8 Liquid``。"""

    if not isinstance(game, (list, tuple)) or len(game) < 3:
        return f"{match_count}. 第 {match_count} 场赛果更新"
    winner = _resolve_team(game[0], resolver)
    loser = _resolve_team(game[1], resolver)
    score = str(game[2]).strip()
    if score:
        return f"{match_count}. {winner} {score} {loser}"
    return f"{match_count}. {winner} 胜 {loser}"


def x_tick_labels(
    games: Iterable | None,
    max_match_count: int,
    resolver: Callable[[str], str] | None = None,
) -> list[str]:
    """``0..max_match_count`` 的完整刻度：``初始`` + 每场对阵比分。"""

    chronological = chronological_games(games)
    labels = ["初始"]
    for match_count in range(1, max(0, int(max_match_count)) + 1):
        game = chronological[match_count - 1] if match_count - 1 < len(chronological) else None
        labels.append(match_label(match_count, game, resolver))
    return labels


def _resolve_team(team, resolver: Callable[[str], str] | None) -> str:
    text = str(team or "").strip() or "未知队伍"
    if resolver is None:
        return text
    try:
        resolved = resolver(text)
    except Exception:
        return text
    return str(resolved or text)


# --------------------------------------------------------------------------- #
# 字体与配色
# --------------------------------------------------------------------------- #
def load_font(font_path: str | Path | None = None) -> FontProperties | None:
    """加载仓库内置中文字体；文件缺失时返回 ``None`` 由 matplotlib 回退默认字体。

    结果按路径缓存并加锁：``font_manager.addfont`` 会改全局字体缓存，
    多线程同时首次出图时重复调用可能互相踩踏，缓存后只加载一次。
    """

    path = Path(font_path) if font_path else DEFAULT_FONT_PATH
    if not path.is_absolute():
        path = REPO_ROOT / path
    key = str(path)
    with _FONT_LOCK:
        if key in _FONT_CACHE:
            return _FONT_CACHE[key]
        props: FontProperties | None = None
        if path.is_file():
            try:
                font_manager.fontManager.addfont(str(path))
                props = FontProperties(fname=str(path))
            except Exception:  # pragma: no cover - 字体损坏时不应影响出图
                props = None
        _FONT_CACHE[key] = props
        return props


def _font(base: FontProperties | None, size: float) -> FontProperties | None:
    if base is None:
        return None
    props = base.copy()
    props.set_size(size)
    return props


def build_palette(count: int) -> list[tuple[float, float, float, float]]:
    """高区分度调色板：``tab20`` → ``tab20b`` → ``tab20c`` → 黄金角 HSV 兜底。"""

    colors: list[tuple[float, float, float, float]] = []
    for map_name in PALETTE_MAPS:
        try:
            colors.extend(tuple(color) for color in matplotlib.colormaps[map_name].colors)
        except Exception:  # pragma: no cover - 老版本 matplotlib 兜底
            continue
    index = 0
    golden_ratio = 0.618033988749895
    hue = 0.13
    while len(colors) < max(1, count):
        hue = (hue + golden_ratio) % 1.0
        red, green, blue = hsv_to_rgb((hue, 0.62, 0.86))
        colors.append((red, green, blue, 1.0))
        index += 1
    return colors[: max(1, count)]


# --------------------------------------------------------------------------- #
# 渲染
# --------------------------------------------------------------------------- #
def resolve_scale(series: Sequence[HomeworkSeries], scale: str) -> str:
    """决定实际纵轴模式：全员无正概率时对数坐标无解，安全回退线性。"""

    requested = SCALE_LOG if str(scale).lower() == SCALE_LOG else SCALE_LINEAR
    if requested != SCALE_LOG:
        return SCALE_LINEAR
    if any(winrate > 0.0 for item in series for _, winrate in item.points):
        return SCALE_LOG
    return SCALE_LINEAR


def render_homework_chart(
    series: Sequence[HomeworkSeries],
    games: Iterable | None = None,
    *,
    stage_name: str = "Major",
    scale: str = SCALE_LINEAR,
    dpi: int = DEFAULT_DPI,
    font_path: str | Path | None = None,
    resolver: Callable[[str], str] | None = None,
    labels: Sequence[str] | None = None,
) -> bytes:
    """把整理好的序列渲染成 PNG 字节流（``dpi`` 默认 160，中文图例无方块）。"""

    ordered = sort_series([item for item in series if item.points])
    if not ordered:
        raise ValueError("没有可绘制的作业通过率数据")

    max_match_count = max(item.last_match_count or 0 for item in ordered)
    if labels is None:
        labels = x_tick_labels(games, max_match_count, resolver)
    else:
        labels = list(labels)
        max_match_count = max(max_match_count, len(labels) - 1)
        if len(labels) <= max_match_count:
            labels = list(labels) + [
                f"{index}. 第 {index} 场赛果更新" for index in range(len(labels), max_match_count + 1)
            ]

    # 对数模式在「全员无正概率」时必须回退线性，否则 matplotlib 会直接抛异常。
    effective_scale = resolve_scale(ordered, scale)

    font = load_font(font_path)
    return _render(
        ordered,
        labels,
        max_match_count=max_match_count,
        stage_name=stage_name,
        scale=effective_scale,
        dpi=max(72, int(dpi)),
        font=font,
    )


def _render(
    ordered: Sequence[HomeworkSeries],
    labels: Sequence[str],
    *,
    max_match_count: int,
    stage_name: str,
    scale: str,
    dpi: int,
    font: FontProperties | None,
) -> bytes:
    is_log = scale == SCALE_LOG
    tick_count = max(len(labels), max_match_count + 1)

    width = min(26.0, max(10.5, 7.8 + 0.62 * tick_count))
    height = 6.6 + max(0, len(ordered) - 14) * 0.12
    figure = Figure(figsize=(width, height), dpi=dpi, facecolor=FIG_BG)
    FigureCanvasAgg(figure)
    axes = figure.add_subplot(1, 1, 1)
    figure.subplots_adjust(left=0.06, right=0.96, top=0.92, bottom=0.30)

    axes.set_facecolor(AX_BG)
    for spine_name in ("top", "right"):
        axes.spines[spine_name].set_visible(False)
    for spine_name in ("left", "bottom"):
        axes.spines[spine_name].set_color(SPINE_COLOR)
        axes.spines[spine_name].set_linewidth(0.9)

    palette = build_palette(len(ordered))
    positives = [winrate for item in ordered for _, winrate in item.points if winrate > 0.0]
    floor_value = min(positives) / 5.0 if positives else 1e-4

    for index, item in enumerate(ordered):
        color = palette[index % len(palette)]
        _plot_series(axes, item, color, is_log=is_log, floor_value=floor_value)

    _style_y_axis(axes, is_log=is_log, positives=positives, floor_value=floor_value)

    positions = list(range(tick_count))
    labels = list(labels) + [""] * (tick_count - len(labels))
    rotation = 30 if tick_count <= 12 else 42 if tick_count <= 22 else 55
    tick_size = 8.6 if tick_count <= 14 else 8.0 if tick_count <= 22 else 7.4
    axes.set_xticks(positions)
    axes.set_xticklabels(labels, rotation=rotation, ha="right", rotation_mode="anchor")
    for tick_label in axes.get_xticklabels():
        tick_label.set_fontproperties(_font(font, tick_size))
        tick_label.set_color(MUTED_TEXT)
    for y_tick in axes.get_yticklabels():
        y_tick.set_fontproperties(_font(font, 9.0))
        y_tick.set_color(MUTED_TEXT)

    axes.set_xlim(-0.35, tick_count - 0.65)
    axes.grid(True, which="major", axis="both", color=GRID_COLOR, alpha=0.42, linewidth=0.8)
    if is_log:
        axes.grid(True, which="minor", axis="y", color=GRID_COLOR, alpha=0.16, linewidth=0.6)
    axes.set_axisbelow(True)

    title = f"{stage_name} 作业通过率走势 · {'对数' if is_log else '线性'}坐标"
    axes.set_title(title, loc="left", pad=14, color=TEXT_COLOR, fontproperties=_font(font, 15.5))
    axes.set_xlabel(
        f"比赛场次（共 {len(ordered)} 人，已结束 {max_match_count} 场，纵轴为 ≥5 队正确的概率）",
        labelpad=8,
        color=MUTED_TEXT,
        fontproperties=_font(font, 9.6),
    )
    axes.set_ylabel("通过率", labelpad=8, color=TEXT_COLOR, fontproperties=_font(font, 10.5))

    handles, legend_labels = axes.get_legend_handles_labels()
    if handles:
        legend = axes.legend(
            handles,
            legend_labels,
            title="玩家（最新通过率）",
            loc="upper left",
            bbox_to_anchor=(1.015, 1.0),
            borderaxespad=0.0,
            frameon=True,
            framealpha=0.96,
            edgecolor="#d6dde5",
            labelspacing=0.5,
            handlelength=1.9,
            columnspacing=1.0,
            prop=_font(font, 9.0),
            title_fontproperties=_font(font, 9.6),
        )
        legend.get_frame().set_linewidth(0.8)
        legend.get_title().set_color(TEXT_COLOR)

    buffer = io.BytesIO()
    figure.savefig(
        buffer,
        format="png",
        dpi=dpi,
        facecolor=figure.get_facecolor(),
        bbox_inches="tight",
        pad_inches=0.22,
    )
    return buffer.getvalue()


def _plot_series(
    axes,
    item: HomeworkSeries,
    color,
    *,
    is_log: bool,
    floor_value: float,
) -> None:
    """单条曲线：线性直接画到 0；对数只画正概率并把归零点拉到底部打 ×。"""

    label = f"{display_name(item.name)} ({(item.latest_winrate or 0.0) * 100:.2f}%)"

    if not is_log:
        xs = [match_count for match_count, _ in item.points]
        ys = [winrate * 100.0 for _, winrate in item.points]
        axes.plot(
            xs,
            ys,
            color=color,
            linewidth=1.9,
            marker="o",
            markersize=4.6,
            markerfacecolor="white",
            markeredgewidth=1.3,
            markeredgecolor=color,
            label=label,
            zorder=3,
            solid_capstyle="round",
        )
        zero_index = first_zero_index(item)
        if zero_index is not None:
            axes.plot(
                [zero_index],
                [0.0],
                linestyle="none",
                marker="x",
                markersize=8.4,
                markeredgewidth=1.9,
                color="#111820",
                zorder=6,
                label="_nolegend_",
            )
        return

    positive_points = [(match_count, winrate) for match_count, winrate in item.points if winrate > 0.0]
    if positive_points:
        axes.plot(
            [match_count for match_count, _ in positive_points],
            [winrate for _, winrate in positive_points],
            color=color,
            linewidth=1.9,
            marker="o",
            markersize=4.6,
            markerfacecolor="white",
            markeredgewidth=1.3,
            markeredgecolor=color,
            label=label,
            zorder=3,
            solid_capstyle="round",
        )
    else:
        # 全程 0：没有正概率可画，仅在图例占位（画一个底部 × 说明已淘汰）。
        first_match_count = item.first_match_count or 0
        axes.plot(
            [first_match_count],
            [floor_value],
            linestyle="none",
            marker="x",
            markersize=8.4,
            markeredgewidth=1.9,
            color="#111820",
            zorder=6,
            label=label,
        )

    zero_index = first_zero_index(item)
    if zero_index is None:
        return
    last_match_count, last_winrate = positive_points[-1]
    axes.plot(
        [last_match_count, zero_index],
        [last_winrate, floor_value],
        color=color,
        linewidth=1.4,
        linestyle=(0, (3, 2)),
        zorder=2,
        label="_nolegend_",
    )
    axes.plot(
        [zero_index],
        [floor_value],
        linestyle="none",
        marker="x",
        markersize=8.4,
        markeredgewidth=1.9,
        color="#111820",
        zorder=6,
        label="_nolegend_",
    )


def _style_y_axis(
    axes,
    *,
    is_log: bool,
    positives: Sequence[float],
    floor_value: float,
) -> None:
    if not is_log:
        axes.set_ylim(-5.0, 105.0)
        axes.set_yticks([0, 20, 40, 60, 80, 100])
        axes.set_yticklabels([f"{value}%" for value in (0, 20, 40, 60, 80, 100)])
        return

    maximum = max(positives) if positives else 1.0
    minimum = min(positives) if positives else floor_value
    bottom = max(minimum / 10.0, 1e-7)
    top = min(1.0, max(maximum * 1.05, 1.0))
    if top <= bottom:
        bottom = max(top / 50.0, 1e-7)
    axes.set_yscale("log")
    axes.set_ylim(bottom, top)
    tick_candidates = [1e-5, 1e-4, 1e-3, 1e-2, 1e-1, 1.0]
    ticks = [value for value in tick_candidates if bottom * 0.999 <= value <= top]
    if not ticks:  # 极端区间兜底，避免空刻度
        ticks = [bottom, top]
    axes.set_yticks(ticks)
    axes.set_yticklabels([_format_probability(value) for value in ticks])
    axes.yaxis.set_minor_formatter(ticker.NullFormatter())


def _format_probability(value: float) -> str:
    percent = value * 100.0
    if percent >= 1.0:
        return f"{percent:g}%"
    if percent >= 0.01:
        return f"{percent:.2f}%".rstrip("0").rstrip(".") + "%"
    return f"{percent:.4f}%".rstrip("0").rstrip(".") + "%"


def render_to_file(
    path: str | Path,
    series: Sequence[HomeworkSeries],
    games: Iterable | None = None,
    **kwargs,
) -> Path:
    """把图写到本地文件（供脚本/样图生成使用），返回实际路径。"""

    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(render_homework_chart(series, games, **kwargs))
    return target


__all__ = [
    "DEFAULT_DPI",
    "HomeworkSeries",
    "SCALE_LINEAR",
    "SCALE_LOG",
    "build_palette",
    "build_series",
    "chronological_games",
    "display_name",
    "finite_float",
    "first_zero_index",
    "load_font",
    "match_label",
    "render_homework_chart",
    "render_to_file",
    "resolve_scale",
    "sort_series",
    "x_tick_labels",
]
