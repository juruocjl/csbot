"""Major 作业走势折线图（``/作业走势``）离线单元测试。

覆盖：

1. 快照整理：同一 ``match_count`` 多条记录（赛中改作业）取 ``created_at`` 最新；
   ``NaN`` / ``inf`` 点被忽略；中途入场玩家从自己首个快照点起画、不补 0；
2. 赛果顺序：``local_storage["hltvresult{event_id}"]`` 为倒序，X 轴第 k 场标签
   必须与 ``cs_server._major_match_event`` / ``_major_match_event_detail`` 完全对齐；
3. 渲染：线性 / 对数坐标均产出合法 PNG（校验魔数与尺寸）；首次归零在两种模式下
   都被正确识别；全员 0 时对数坐标安全回退线性不崩溃；单人图两种坐标均合法；
4. 指令：``/作业走势`` 仅从 ``arg`` 提取 ``@``（不误算消息开头的触发 ``@bot``）、
   指定人筛选、混用「我 + 主播名 + log」、未知目标显式报错、空快照 / 部分无快照提示；
   ``/我的作业走势`` 默认只画自己、附带 ``@`` / 主播名自动合并对比。

测试使用临时文件 SQLite 与 AsyncMock Matcher，不连接生产库、不联网、不发 QQ 消息。
``fcntl`` 垫片仅为让 ``allmsg`` 可导入（``cs_server`` 依赖链），不改变业务逻辑。
"""

import json
import struct
import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest.mock import AsyncMock

# --- Windows test shim: allmsg imports fcntl (POSIX only) -------------------
if "fcntl" not in sys.modules:
    _fcntl = types.ModuleType("fcntl")
    _fcntl.LOCK_EX = 2
    _fcntl.LOCK_SH = 1
    _fcntl.LOCK_NB = 4
    _fcntl.LOCK_UN = 8
    _fcntl.flock = lambda fd, op: 0
    sys.modules["fcntl"] = _fcntl

import nonebot
from nonebot.adapters.onebot.v11 import Message, MessageSegment
from nonebot.exception import FinishedException
from sqlalchemy import delete

ROOT = Path(__file__).parents[1]
sys.path.insert(0, str(ROOT))

TMPDIR = Path(tempfile.mkdtemp(prefix="csbot_hw_chart_"))
DB_FILE = TMPDIR / "hw_chart_test.db"

nonebot.init(
    _env_file=None,
    driver="~fastapi",
    cs_database=f"sqlite+aiosqlite:///{DB_FILE.as_posix()}",
    cs_mysteam_id=0,
    cs_wmtoken="unused-test-token",
    cs_group_list=[],
    cs_event_group_list=[],
    major_name="2026-singapore",
    major_stage="stage1",
    major_event_id=9303,
)

for _plugin in (
    "models",
    "utils",
    "runtime_config",
    "cs_db_val",
    "cs_db_upd",
    "chat_history",
    "cs_ai",
    "allmsg",
    "pic",
    "major_hw",
    "hltv_watcher",
    "cs_server",
):
    assert nonebot.load_plugin(ROOT / "plugins" / _plugin) is not None, _plugin

from plugins.major_hw import db as major_db  # noqa: E402
from plugins.major_hw import major_stage_name  # noqa: E402
from plugins.models import (  # noqa: E402
    Base,
    MajorHW,
    MajorHWSnapshot,
    MajorSimulationSnapshot,
    StorageItem,
)
from plugins.utils import async_session_factory, engine, local_storage  # noqa: E402
from plugins import cs_server as CS  # noqa: E402

HW = nonebot.get_plugin("major_hw").module
CHART = HW.hw_chart

PNG_HEADER = b"\x89PNG\r\n\x1a\n"
STAGE = major_stage_name
EVENT_ID = 9303
GAMES_KEY = f"hltvresult{EVENT_ID}"

#: 与 hltv_watcher 一致：**倒序**存储（最新一场在 index 0），元素为
#: ``(winner, loser, "13:8", match_id)``。
#: 队名全部取自 ``assets/2026-singapore-stage1.json``，避免 ``get_major_team_name``
#: 对未知队名做模糊匹配后污染对齐断言。
FINISHED_MATCHES_NEWEST_FIRST = [
    ["Ninjas in Pyjamas", "M80", "13:8", "m4"],
    ["PARIVISION", "Wildcard", "16:14", "m3"],
    ["FaZe", "Liquid", "13:11", "m2"],
    ["HEROIC", "9z", "13:4", "m1"],
]

ROW_SEQ = 0


class FakeSnapshotRow:
    """``MajorHWSnapshot`` 的轻量替身，只暴露 chart.build_series 用到的字段。"""

    def __init__(self, uid: str, match_count: int, winrate, created_at: int):
        global ROW_SEQ
        ROW_SEQ += 1
        self.uid = uid
        self.match_count = match_count
        self.winrate = winrate
        self.created_at = created_at
        self.homework_text = f'hw-{ROW_SEQ}'


def _png_size(data: bytes) -> tuple[int, int]:
    """从 PNG 的 IHDR 里读宽高，顺带证明字节流结构完整。"""

    assert data[:8] == PNG_HEADER, "不是合法 PNG（魔数不符）"
    assert data[12:16] == b"IHDR", "PNG 缺少 IHDR"
    width, height = struct.unpack(">II", data[16:24])
    return width, height


async def _create_schema() -> None:
    tables = [
        MajorHW.__table__,
        MajorHWSnapshot.__table__,
        MajorSimulationSnapshot.__table__,
        StorageItem.__table__,
    ]
    async with engine.begin() as conn:
        await conn.run_sync(lambda sync_conn: Base.metadata.create_all(sync_conn, tables=tables))


async def _clear_db() -> None:
    async with async_session_factory() as session:
        async with session.begin():
            for model in (MajorHW, MajorHWSnapshot):
                await session.execute(delete(model))


async def _seed_games(games=FINISHED_MATCHES_NEWEST_FIRST) -> None:
    await local_storage.set(GAMES_KEY, json.dumps(games, ensure_ascii=False))


# --------------------------------------------------------------------------- #
# 1. 数据整理
# --------------------------------------------------------------------------- #
class BuildSeriesTest(unittest.TestCase):
    def test_same_match_count_keeps_latest_snapshot(self):
        rows = [
            FakeSnapshotRow("10001", 2, 0.11, created_at=100),
            FakeSnapshotRow("10001", 2, 0.42, created_at=300),  # 赛中改作业，最新
            FakeSnapshotRow("10001", 2, 0.33, created_at=200),
            FakeSnapshotRow("10001", 1, 0.55, created_at=90),
        ]
        series = CHART.build_series(rows)
        self.assertEqual(len(series), 1)
        self.assertEqual(series[0].uid, "10001")
        self.assertEqual(series[0].points, [(1, 0.55), (2, 0.42)])
        self.assertAlmostEqual(series[0].latest_winrate, 0.42)

    def test_nan_and_inf_are_ignored(self):
        rows = [
            FakeSnapshotRow("10001", 0, 0.8, created_at=1),
            FakeSnapshotRow("10001", 1, float("nan"), created_at=2),
            FakeSnapshotRow("10001", 2, float("inf"), created_at=3),
            FakeSnapshotRow("10001", 3, "-inf", created_at=4),
            FakeSnapshotRow("10001", 4, None, created_at=5),
            FakeSnapshotRow("10001", 5, 0.25, created_at=6),
        ]
        series = CHART.build_series(rows)
        self.assertEqual(series[0].points, [(0, 0.8), (5, 0.25)])

    def test_invalid_rows_are_skipped(self):
        rows = [
            FakeSnapshotRow("", 0, 0.5, created_at=1),
            FakeSnapshotRow("10001", -1, 0.5, created_at=2),
            FakeSnapshotRow("10001", 0, "not-a-number", created_at=3),
            FakeSnapshotRow("10001", 1, 0.9, created_at=4),
        ]
        series = CHART.build_series(rows)
        self.assertEqual([item.uid for item in series], ["10001"])
        self.assertEqual(series[0].points, [(1, 0.9)])

    def test_midway_entrant_starts_at_first_snapshot(self):
        """虚拟主播从第 10 场才有快照：不补 0、不断言长度、从 10 起画。"""

        rows = [FakeSnapshotRow("123456789", match_count, 0.5, match_count) for match_count in range(11)]
        rows += [FakeSnapshotRow("v_advent", match_count, 0.2, match_count) for match_count in (10, 11)]
        series = {item.uid: item for item in CHART.build_series(rows)}

        self.assertEqual(series["123456789"].first_match_count, 0)
        self.assertEqual(len(series["123456789"].points), 11)
        self.assertEqual(series["v_advent"].first_match_count, 10)
        self.assertEqual(series["v_advent"].points, [(10, 0.2), (11, 0.2)])
        # 中途入场者没有 0~9 场的点（绝不能补 0）
        self.assertNotIn(0, [match_count for match_count, _ in series["v_advent"].points])

    def test_names_and_sorting(self):
        rows = [
            FakeSnapshotRow("v_advent", 0, 0.2, created_at=1),
            FakeSnapshotRow("v_wanjiqi", 0, 0.6, created_at=1),
            FakeSnapshotRow("10001", 0, 0.4, created_at=1),
        ]
        series = CHART.build_series(rows, {"v_advent": "CS-advent", "v_wanjiqi": "玩机器"})
        names = {item.uid: item.name for item in series}
        self.assertEqual(names["v_advent"], "CS-advent")
        self.assertEqual(names["v_wanjiqi"], "玩机器")
        self.assertEqual(names["10001"], "10001")  # 未给名字时回退 uid

        ordered = CHART.sort_series(series)
        self.assertEqual([item.uid for item in ordered], ["v_wanjiqi", "10001", "v_advent"])

    def test_display_name_truncation(self):
        self.assertEqual(CHART.display_name("玩机器"), "玩机器")
        self.assertEqual(len(CHART.display_name("a" * 30)), CHART.MAX_NAME_CHARS)
        self.assertTrue(CHART.display_name("a" * 30).endswith("…"))
        self.assertEqual(CHART.display_name(""), "未知")


# --------------------------------------------------------------------------- #
# 2. 赛果顺序 / X 轴标签
# --------------------------------------------------------------------------- #
class MatchOrderTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        await _create_schema()
        await _clear_db()
        await _seed_games()

    def test_storage_is_newest_first(self):
        """先证明测试数据本身就是倒序，避免用假的「正序」自证。"""

        stored = json.loads(json.dumps(FINISHED_MATCHES_NEWEST_FIRST))
        self.assertEqual(stored[0][3], "m4")
        self.assertEqual(CHART.chronological_games(stored)[0][3], "m1")

    def test_labels_align_with_cs_server(self):
        games = FINISHED_MATCHES_NEWEST_FIRST
        # 与指令链路一致：传入 get_major_team_name 做队名归一
        labels = CHART.x_tick_labels(games, len(games), CS.get_major_team_name)
        self.assertEqual(labels[0], "初始")
        self.assertEqual(len(labels), len(games) + 1)

        for match_count in range(1, len(games) + 1):
            expected = CS._major_match_event(list(games), match_count)
            winner, loser, score = CS._major_match_event_detail(list(games), match_count)
            self.assertEqual(labels[match_count], f"{match_count}. {winner} {score} {loser}")
            self.assertIn(winner, expected)
            self.assertIn(loser, expected)
            self.assertIn(score, expected)

    def test_first_label_is_chronologically_earliest(self):
        labels = CHART.x_tick_labels(FINISHED_MATCHES_NEWEST_FIRST, 4)
        self.assertEqual(labels[1], "1. HEROIC 13:4 9z")
        self.assertEqual(labels[2], "2. FaZe 13:11 Liquid")
        self.assertEqual(labels[3], "3. PARIVISION 16:14 Wildcard")
        self.assertEqual(labels[4], "4. Ninjas in Pyjamas 13:8 M80")

    def test_missing_game_falls_back(self):
        labels = CHART.x_tick_labels(FINISHED_MATCHES_NEWEST_FIRST, 6)
        self.assertEqual(len(labels), 7)
        self.assertEqual(labels[5], "5. 第 5 场赛果更新")
        self.assertEqual(labels[6], "6. 第 6 场赛果更新")

    def test_empty_games_only_initial(self):
        self.assertEqual(CHART.x_tick_labels([], 0), ["初始"])
        self.assertEqual(CHART.x_tick_labels(None, 2), ["初始", "1. 第 1 场赛果更新", "2. 第 2 场赛果更新"])

    def test_team_alias_resolver(self):
        alias2full = {"PARIVISION": "PVISION", "FaZe": "FaZe Clan"}
        labels = CHART.x_tick_labels(FINISHED_MATCHES_NEWEST_FIRST, 4, alias2full.get)
        self.assertEqual(labels[2], "2. FaZe Clan 13:11 Liquid")
        self.assertEqual(labels[3], "3. PVISION 16:14 Wildcard")
        self.assertEqual(labels[4], "4. Ninjas in Pyjamas 13:8 M80")

    def test_broken_resolver_falls_back_to_raw_name(self):
        def _boom(_):
            raise RuntimeError("resolver down")

        labels = CHART.x_tick_labels(FINISHED_MATCHES_NEWEST_FIRST, 4, _boom)
        self.assertEqual(labels[4], "4. Ninjas in Pyjamas 13:8 M80")


# --------------------------------------------------------------------------- #
# 3. 渲染
# --------------------------------------------------------------------------- #
def _sample_series() -> list:
    """14 人合成数据：中文昵称、两位主播中途入场、两档归零淘汰。"""

    rows = []
    for index, uid in enumerate([f"1000{number}" for number in range(12)]):
        base = 0.62 - index * 0.045
        for match_count in range(0, 14):
            winrate = max(0.0, base - match_count * (0.035 + index * 0.002))
            rows.append(FakeSnapshotRow(uid, match_count, winrate, created_at=match_count))
    # 主播从中途第 10 场入场
    for match_count in (10, 11, 12, 13):
        rows.append(FakeSnapshotRow("v_advent", match_count, 0.2 - (match_count - 10) * 0.05, created_at=match_count))
        rows.append(FakeSnapshotRow("v_wanjiqi", match_count, 0.12 - (match_count - 10) * 0.03, created_at=match_count))

    names = {f"1000{number}": f"群友{number}号" for number in range(12)}
    names.update({"v_advent": "CS-advent", "v_wanjiqi": "玩机器"})
    return CHART.build_series(rows, names)


class RenderTest(unittest.TestCase):
    def test_linear_png_is_valid(self):
        png = CHART.render_homework_chart(_sample_series(), FINISHED_MATCHES_NEWEST_FIRST, stage_name=STAGE)
        width, height = _png_size(png)
        self.assertGreater(width, 800)
        self.assertGreater(height, 500)
        self.assertGreater(len(png), 20_000)
        self.assertEqual(CHART.render_homework_chart(
            _sample_series(), FINISHED_MATCHES_NEWEST_FIRST, stage_name=STAGE, scale="log"
        )[:8], PNG_HEADER)

    def test_log_png_is_valid_and_larger_canvas_when_long(self):
        short = _sample_series()
        png = CHART.render_homework_chart(short, FINISHED_MATCHES_NEWEST_FIRST, stage_name=STAGE, scale="log")
        width, height = _png_size(png)
        self.assertGreater(width, 800)
        self.assertGreater(height, 500)

        long_games = [[f"Team{i}", f"Rival{i}", f"13:{i}", f"m{i}"] for i in range(24)][::-1]
        long_series = [
            CHART.HomeworkSeries(uid=f"u{index}", name=f"玩家{index}", points=[(k, 0.5) for k in range(25)])
            for index in range(3)
        ]
        long_png = CHART.render_homework_chart(long_series, long_games, stage_name=STAGE)
        long_width, _ = _png_size(long_png)
        self.assertGreater(long_width, width)  # 场次多时画布自适应变宽

    def test_font_loading(self):
        font = CHART.load_font()
        self.assertIsNotNone(font, "assets/merged.ttf 必须能被 matplotlib 加载")
        self.assertIsNone(CHART.load_font(TMPDIR / "missing.ttf"))
        # 字体缺失时也不能崩（回退默认字体）
        self.assertEqual(
            CHART.render_homework_chart(
                _sample_series(), FINISHED_MATCHES_NEWEST_FIRST, font_path=TMPDIR / "missing.ttf"
            )[:8],
            PNG_HEADER,
        )

    def test_first_zero_detection(self):
        series = CHART.build_series([
            FakeSnapshotRow("u1", 0, 0.5, 0),
            FakeSnapshotRow("u1", 1, 0.2, 1),
            FakeSnapshotRow("u1", 2, 0.0, 2),
            FakeSnapshotRow("u1", 3, 0.0, 3),
            FakeSnapshotRow("u2", 0, 0.3, 0),
            FakeSnapshotRow("u2", 1, 0.1, 1),
        ])
        by_uid = {item.uid: item for item in series}
        self.assertEqual(CHART.first_zero_index(by_uid["u1"]), 2)  # 首次归零
        self.assertIsNone(CHART.first_zero_index(by_uid["u2"]))
        # 一开始就是 0 不算「归零淘汰」
        only_zero = CHART.HomeworkSeries(uid="u3", name="u3", points=[(0, 0.0), (1, 0.0)])
        self.assertIsNone(CHART.first_zero_index(only_zero))

    def test_first_zero_renders_in_both_scales(self):
        series = CHART.build_series([
            FakeSnapshotRow("u1", 0, 0.4, 0),
            FakeSnapshotRow("u1", 1, 0.0, 1),
            FakeSnapshotRow("u1", 2, 0.0, 2),
        ])
        for scale in ("linear", "log"):
            png = CHART.render_homework_chart(
                series, FINISHED_MATCHES_NEWEST_FIRST[:2], stage_name=STAGE, scale=scale
            )
            self.assertEqual(png[:8], PNG_HEADER, scale)

    def test_single_player_png_is_valid_in_both_scales(self):
        """按人筛选后最常见的 1 条线场景：图例、标题、纵轴都要正常出图。"""

        rows = [
            FakeSnapshotRow("u1", match_count, max(0.0, 0.6 - match_count * 0.05), match_count)
            for match_count in range(6)
        ]
        series = CHART.build_series(rows, {"u1": "孤独玩家"})
        for scale in ("linear", "log"):
            png = CHART.render_homework_chart(
                series, FINISHED_MATCHES_NEWEST_FIRST[:5], stage_name=STAGE, scale=scale
            )
            width, height = _png_size(png)
            self.assertGreater(width, 800, scale)
            self.assertGreater(height, 500, scale)
            self.assertGreater(len(png), 20_000, scale)

    def test_two_or_three_player_filter_png(self):
        rows = []
        for index, uid in enumerate(("10001", "10002", "10003")):
            for match_count in range(5):
                rows.append(
                    FakeSnapshotRow(uid, match_count, max(0.0, 0.5 - index * 0.1 - match_count * 0.05), match_count)
                )
        series = CHART.build_series(rows, {"10001": "甲", "10002": "乙", "10003": "丙"})
        png = CHART.render_homework_chart(series, FINISHED_MATCHES_NEWEST_FIRST[:4], stage_name=STAGE)
        self.assertEqual(png[:8], PNG_HEADER)

    def test_all_zero_log_falls_back_to_linear(self):
        series = [
            CHART.HomeworkSeries(uid="u1", name="全零甲", points=[(0, 0.0), (1, 0.0)]),
            CHART.HomeworkSeries(uid="u2", name="全零乙", points=[(0, 0.0)]),
        ]
        self.assertEqual(CHART.resolve_scale(series, "log"), "linear")
        self.assertEqual(CHART.resolve_scale(series, "linear"), "linear")
        png = CHART.render_homework_chart(series, FINISHED_MATCHES_NEWEST_FIRST[:2], scale="log")
        width, height = _png_size(png)
        self.assertGreater(width, 0)
        self.assertGreater(height, 0)

    def test_positive_series_keeps_log(self):
        series = CHART.build_series([FakeSnapshotRow("u1", 0, 0.01, 0)])
        self.assertEqual(CHART.resolve_scale(series, "log"), "log")
        self.assertEqual(CHART.resolve_scale(series, "LOG"), "log")
        self.assertEqual(CHART.resolve_scale(series, "linear"), "linear")
        self.assertEqual(CHART.resolve_scale(series, "unknown"), "linear")

    def test_empty_series_raises(self):
        with self.assertRaises(ValueError):
            CHART.render_homework_chart([], FINISHED_MATCHES_NEWEST_FIRST)

    def test_palette_is_distinct_for_many_players(self):
        colors = CHART.build_palette(14)
        self.assertEqual(len(colors), 14)
        self.assertEqual(len({tuple(round(channel, 6) for channel in color) for color in colors}), 14)
        self.assertGreaterEqual(len(CHART.build_palette(70)), 70)

    def test_concurrent_renders_are_safe(self):
        """``asyncio.to_thread`` 会并发调用，独立 Figure 不应互相踩踏。"""

        from concurrent.futures import ThreadPoolExecutor

        series = _sample_series()
        with ThreadPoolExecutor(max_workers=4) as pool:
            results = list(
                pool.map(
                    lambda item: CHART.render_homework_chart(
                        series, FINISHED_MATCHES_NEWEST_FIRST, stage_name=STAGE, scale=item[1]
                    ),
                    [(index, "linear" if index % 2 else "log") for index in range(4)],
                )
            )
        self.assertEqual(len(results), 4)
        for png in results:
            width, height = _png_size(png)
            self.assertGreater(width, 0)
            self.assertGreater(height, 0)


# --------------------------------------------------------------------------- #
# 4. /作业走势 指令
# --------------------------------------------------------------------------- #
class ChartCommandTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        await _create_schema()
        await _clear_db()
        await _seed_games()
        self.matcher = HW.hwchart
        self.mine_matcher = HW.hwmine
        self._orig_finish = {
            "chart": self.matcher.finish,
            "mine": self.mine_matcher.finish,
        }
        self._orig_send = {
            "chart": self.matcher.send,
            "mine": self.mine_matcher.send,
        }
        self.matcher.finish = AsyncMock(side_effect=FinishedException)
        self.mine_matcher.finish = AsyncMock(side_effect=FinishedException)
        self.matcher.send = AsyncMock()
        self.mine_matcher.send = AsyncMock()

        self._orig_member_name = HW._major_member_name
        HW._major_member_name = AsyncMock(side_effect=lambda bot, gid, uid: {
            "123456789": "群友甲",
            "10001": "群友A",
            "10002": "群友B",
            "999999999": "机器人自己",
            "v_advent": "CS-advent",
            "v_wanjiqi": "玩机器",
        }.get(str(uid), f"用户{uid}"))

        self._orig_render = CHART.render_homework_chart
        self.render_calls: list[dict] = []

        def _fake_render(series, games=None, **kwargs):
            self.render_calls.append({"series": series, "games": games, **kwargs})
            return self._orig_render(series, games, **kwargs)

        CHART.render_homework_chart = _fake_render

    async def asyncTearDown(self):
        self.matcher.finish = self._orig_finish["chart"]
        self.mine_matcher.finish = self._orig_finish["mine"]
        self.matcher.send = self._orig_send["chart"]
        self.mine_matcher.send = self._orig_send["mine"]
        HW._major_member_name = self._orig_member_name
        CHART.render_homework_chart = self._orig_render

    @staticmethod
    def _message(raw: Message | None = None):
        class _Message:
            def get_session_id(self):
                return "group_10086"

            def get_user_id(self):
                return "123456789"

            def get_message(self):
                return raw or Message("")

        return _Message()

    async def _run(self, arg="", *, mine: bool = False, raw: Message | None = None):
        matcher = self.mine_matcher if mine else self.matcher
        handler = HW.hwmine_function if mine else HW.hwchart_function
        arg_message = arg if isinstance(arg, Message) else Message(arg)
        with self.assertRaises(FinishedException):
            await handler(None, self._message(raw), arg_message)
        return matcher.finish.await_args.args[0] if matcher.finish.await_args else None

    async def _seed_snapshots(self):
        await major_db.save_simulation_snapshot(
            stage=STAGE,
            event_id=EVENT_ID,
            match_count=0,
            latest_match_id=None,
            total_weight=1.0,
            homework_rows=[
                ("123456789", json.dumps({"3-0": ["FaZe", "MOUZ"]}), 0.5, 4.1),
                ("10001", json.dumps({"3-0": ["FaZe", "MOUZ"]}), 0.42, 4.0),
                ("10002", json.dumps({"3-0": ["FaZe", "MOUZ"]}), 0.3, 3.7),
                ("999999999", json.dumps({"3-0": ["FaZe", "MOUZ"]}), 0.6, 4.4),
                ("v_advent", json.dumps({"3-0": ["FaZe", "MOUZ"]}), 0.2, 3.9),
                ("v_wanjiqi", json.dumps({"3-0": ["FaZe", "MOUZ"]}), 0.1, 3.5),
            ],
        )
        await major_db.save_simulation_snapshot(
            stage=STAGE,
            event_id=EVENT_ID,
            match_count=1,
            latest_match_id="m1",
            total_weight=1.0,
            homework_rows=[
                ("123456789", json.dumps({"3-0": ["FaZe", "MOUZ"]}), 0.35, 4.0),
                ("10001", json.dumps({"3-0": ["FaZe", "MOUZ"]}), 0.33, 3.9),
                ("10002", json.dumps({"3-0": ["FaZe", "MOUZ"]}), 0.25, 3.6),
                ("999999999", json.dumps({"3-0": ["FaZe", "MOUZ"]}), 0.55, 4.3),
                ("v_advent", json.dumps({"3-0": ["FaZe", "MOUZ"]}), 0.15, 3.8),
                ("v_wanjiqi", json.dumps({"3-0": ["FaZe", "MOUZ"]}), 0.08, 3.4),
            ],
        )

    async def test_sends_png_with_linear_default(self):
        await self._seed_snapshots()
        segment = await self._run("")
        self.assertIsNotNone(segment)
        self.assertEqual(segment.type, "image")
        self.assertEqual(len(self.render_calls), 1)
        self.assertEqual(self.render_calls[0]["scale"], "linear")
        self.assertEqual(self.render_calls[0]["stage_name"], STAGE)
        # 赛果列表原样透传（倒序 → 由 chart 内部转正序）
        self.assertEqual(self.render_calls[0]["games"], FINISHED_MATCHES_NEWEST_FIRST)
        names = {item.uid: item.name for item in self.render_calls[0]["series"]}
        self.assertEqual(names["v_advent"], "CS-advent")

    async def test_log_parameter_variants(self):
        await self._seed_snapshots()
        for text in ("log", "对数", "LOG", "对数坐标"):
            self.matcher.finish.reset_mock()
            self.render_calls.clear()
            await self._run(text)
            self.assertEqual(self.render_calls[0]["scale"], "log", text)

    async def test_linear_explicit_and_unknown_text(self):
        await self._seed_snapshots()
        for text in ("linear", "线性"):
            self.matcher.finish.reset_mock()
            self.render_calls.clear()
            await self._run(text)
            self.assertEqual(self.render_calls[0]["scale"], "linear", text)
        # 未识别 token 必须显式报错，不能静默回落线性并误画全员
        self.render_calls.clear()
        finished = await self._run("随便写点啥")
        self.assertIn("未识别目标：随便写点啥", finished)
        self.assertEqual(self.render_calls, [])

    def test_parse_chart_scale(self):
        self.assertEqual(HW.parse_chart_scale(Message("")), "linear")
        self.assertEqual(HW.parse_chart_scale(Message("log")), "log")
        self.assertEqual(HW.parse_chart_scale(Message(" 对数 ")), "log")
        self.assertEqual(HW.parse_chart_scale(Message("ＬＯＧ")), "log")  # NFKC 全角
        self.assertEqual(HW.parse_chart_scale(Message("linear")), "linear")
        self.assertEqual(HW.parse_chart_scale(None), "linear")

    async def test_empty_snapshot_hint(self):
        finished = await self._run("")
        self.assertIsInstance(finished, str)
        self.assertIn("暂无作业概率快照", finished)
        self.assertEqual(self.render_calls, [])

    async def test_playoff_hint(self):
        original = HW.config.major_stage
        HW.config.major_stage = "playoffs"
        try:
            finished = await self._run("log")
        finally:
            HW.config.major_stage = original
        self.assertIsInstance(finished, str)
        self.assertIn("淘汰赛", finished)
        self.assertIn("暂不支持", finished)
        self.assertEqual(self.render_calls, [])

    async def test_render_failure_hint(self):
        await self._seed_snapshots()

        def _boom(*args, **kwargs):
            raise RuntimeError("matplotlib exploded")

        CHART.render_homework_chart = _boom
        finished = await self._run("")
        self.assertIsInstance(finished, str)
        self.assertIn("失败", finished)

    async def test_name_resolution_failure_falls_back_to_uid(self):
        await self._seed_snapshots()

        async def _flaky(bot, gid, uid):
            if str(uid) == "v_advent":
                raise RuntimeError("onebot timeout")
            return "群友甲"

        HW._major_member_name = AsyncMock(side_effect=_flaky)
        await self._run("")
        names = {item.uid: item.name for item in self.render_calls[0]["series"]}
        self.assertEqual(names["v_advent"], "v_advent")  # 单个昵称失败不影响出图
        self.assertEqual(names["123456789"], "群友甲")

    async def test_at_filters_only_requested_players(self):
        await self._seed_snapshots()
        arg = Message([
            MessageSegment.at("10001"),
            MessageSegment.text(" "),
            MessageSegment.at("10002"),
        ])
        segment = await self._run(arg)
        self.assertEqual(segment.type, "image")
        uids = {item.uid for item in self.render_calls[0]["series"]}
        self.assertEqual(uids, {"10001", "10002"})

    async def test_trigger_at_bot_in_message_is_not_a_target(self):
        await self._seed_snapshots()
        trigger_message = Message([
            MessageSegment.at("999999999"),
            MessageSegment.text(" /作业走势 "),
            MessageSegment.at("10001"),
        ])
        arg = Message([MessageSegment.at("10001")])
        await self._run(arg, raw=trigger_message)
        # 999999999 也有快照，但它只是触发机器人的 @，绝不能被画进图里
        self.assertEqual([item.uid for item in self.render_calls[0]["series"]], ["10001"])

    async def test_self_streamer_and_log_can_be_mixed(self):
        await self._seed_snapshots()
        await self._run("我 玩机器 log")
        call = self.render_calls[0]
        self.assertEqual(call["scale"], "log")
        self.assertEqual({item.uid for item in call["series"]}, {"123456789", "v_wanjiqi"})

    async def test_chart_self_token_only_draws_sender(self):
        await self._seed_snapshots()
        await self._run("我")
        self.assertEqual([item.uid for item in self.render_calls[0]["series"]], ["123456789"])

    async def test_unknown_target_is_reported_not_silently_all(self):
        await self._seed_snapshots()
        finished = await self._run("查无此人")
        self.assertIsInstance(finished, str)
        self.assertIn("未识别目标：查无此人", finished)
        self.assertIn("@群友", finished)
        self.assertEqual(self.render_calls, [])
        self.matcher.send.assert_not_awaited()

    async def test_filter_all_missing_uses_friendly_hint(self):
        finished = await self._run("我")
        self.assertIn("你当前阶段暂无作业概率快照", finished)

        await self._run(Message([MessageSegment.at("10001")]))
        self.assertIn(
            "指定玩家当前阶段暂无作业概率快照", self.matcher.finish.await_args.args[0]
        )
        self.assertEqual(self.render_calls, [])

    async def test_partial_missing_target_sends_hint_then_chart(self):
        await self._seed_snapshots()
        arg = Message([MessageSegment.at("10001"), MessageSegment.text(" 10003")])
        segment = await self._run(arg)
        self.assertEqual(segment.type, "image")
        self.matcher.send.assert_awaited_once()
        self.assertIn("10003", self.matcher.send.await_args.args[0])
        self.assertEqual({item.uid for item in self.render_calls[0]["series"]}, {"10001"})

    async def test_my_chart_defaults_to_self_only(self):
        await self._seed_snapshots()
        await self._run("", mine=True)
        self.assertEqual([item.uid for item in self.render_calls[0]["series"]], ["123456789"])

    async def test_my_chart_merges_self_at_and_streamer(self):
        await self._seed_snapshots()
        arg = Message([MessageSegment.at("10001"), MessageSegment.text(" 玩机器 log")])
        await self._run(arg, mine=True)
        call = self.render_calls[0]
        self.assertEqual(call["scale"], "log")
        self.assertEqual(
            {item.uid for item in call["series"]}, {"123456789", "10001", "v_wanjiqi"}
        )

    async def test_my_chart_log_only_still_draws_self(self):
        await self._seed_snapshots()
        await self._run("log", mine=True)
        self.assertEqual(self.render_calls[0]["scale"], "log")
        self.assertEqual([item.uid for item in self.render_calls[0]["series"]], ["123456789"])

    def test_help_mentions_chart_commands(self):
        source = (ROOT / "plugins" / "major_hw" / "__init__.py").read_text(encoding="utf-8")
        self.assertIn("/作业走势 [@某人|我|主播名] [log]", source)
        self.assertIn("/我的作业走势 [@某人|主播名] [log]", source)
        self.assertIn('on_command("作业走势"', source)
        self.assertIn('on_command("我的作业走势"', source)
        for alias in ("作业折线图", "作业曲线", "我的作业折线图", "我的作业曲线"):
            self.assertIn(alias, source)

    def test_handlers_are_group_commands_with_priority(self):
        self.assertIsNotNone(HW.hwchart)
        self.assertIsNotNone(HW.hwmine)
        text = (ROOT / "plugins" / "major_hw" / "__init__.py").read_text(encoding="utf-8")
        self.assertRegex(text, r'hwchart = on_command\("作业走势",[^)]*priority=10')
        self.assertRegex(text, r'hwmine = on_command\("我的作业走势",[^)]*priority=10')
        self.assertIn(
            "async def hwmine_function(bot: Bot, message: GroupMessageEvent", text
        )


class ChartRequestParseTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        await _create_schema()

    async def test_at_targets_come_only_from_arg(self):
        arg = Message([
            MessageSegment.at("10001"),
            MessageSegment.text(" "),
            MessageSegment.at("all"),
            MessageSegment.text("，"),
            MessageSegment.at("10002"),
        ])
        self.assertEqual(HW.chart_at_targets(arg), ["10001", "10002"])
        request = await HW.parse_chart_request(arg, "123456789")
        self.assertEqual(request.uids, ["10001", "10002"])
        self.assertEqual(request.unknown, [])
        self.assertEqual(request.scale, "linear")

    async def test_self_virtual_and_log_tokens(self):
        request = await HW.parse_chart_request(Message("我、玩机器，LOG"), "123456789")
        self.assertEqual(request.uids, ["123456789", "v_wanjiqi"])
        self.assertEqual(request.scale, "log")
        self.assertEqual(request.unknown, [])

    async def test_fullwidth_nfkc_and_dedupe(self):
        arg = Message([MessageSegment.at("10001"), MessageSegment.text("　我 ＭＥ 10001")])
        request = await HW.parse_chart_request(arg, "123456789")
        self.assertEqual(request.uids, ["10001", "123456789"])

    async def test_numeric_uid_and_unknown_are_distinguished(self):
        request = await HW.parse_chart_request(Message("10003 查无此人"), "123456789")
        self.assertEqual(request.uids, ["10003"])
        self.assertEqual(request.unknown, ["查无此人"])


class CommandRoutingTest(unittest.TestCase):
    """真实 CommandRule 前缀解析：``/我的作业走势`` 不会被 ``/作业走势`` 吞掉。"""

    class _Event:
        def __init__(self, message: Message):
            self._message = message

        def get_type(self):
            return "message"

        def get_message(self):
            return self._message

    def _parse(self, message: Message):
        from nonebot.rule import TrieRule

        return TrieRule.get_value(None, self._Event(message), {})

    def test_my_chart_command_is_distinct(self):
        message = Message([
            MessageSegment.text("/我的作业走势 "),
            MessageSegment.at("10001"),
            MessageSegment.text(" 玩机器 log"),
        ])
        prefix = self._parse(message)
        self.assertEqual(prefix["raw_command"], "/我的作业走势")
        self.assertEqual(
            [segment.data["qq"] for segment in prefix["command_arg"] if segment.type == "at"],
            ["10001"],
        )

    def test_chart_command_keeps_arg(self):
        prefix = self._parse(Message("/作业走势 我 log"))
        self.assertEqual(prefix["raw_command"], "/作业走势")
        self.assertEqual(prefix["command_arg"].extract_plain_text().strip(), "我 log")

    def test_alias_routing(self):
        self.assertEqual(
            self._parse(Message("/我的作业曲线 我"))["raw_command"], "/我的作业曲线"
        )


class ChartModuleBoundaryTest(unittest.TestCase):
    def test_chart_has_no_network_or_global_plt(self):
        source = (ROOT / "plugins" / "major_hw" / "chart.py").read_text(encoding="utf-8")
        self.assertNotIn("import matplotlib.pyplot", source)
        self.assertIn('matplotlib.use("Agg")', source)
        self.assertIn("FigureCanvasAgg", source)
        self.assertIn("axes.unicode_minus", source)
        self.assertNotIn("requests", source)
        self.assertNotIn("httpx", source)
        # 不得触碰模块级 rcParams 之外的全局绘图状态
        self.assertNotRegex(source, r"plt\.")

    def test_no_frontend_path_referenced(self):
        for name in ("chart.py", "__init__.py"):
            source = (ROOT / "plugins" / "major_hw" / name).read_text(encoding="utf-8")
            self.assertNotIn("csbot-front", source)


if __name__ == "__main__":
    unittest.main(verbosity=2)
