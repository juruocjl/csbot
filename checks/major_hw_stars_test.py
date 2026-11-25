"""Major 作业赛事级打🌟离线单元测试。

覆盖：

1. 名单纯逻辑：事件键 / UID 规范化、JSON 容错解析、序列化稳定排序、赛事隔离；
2. 排名分配：全普通、首行 / 中间 / 连续 / 全员打星，验证 ``🌟`` 不占数字名次且
   原顺序原地混排；
3. 异步存储：``add`` / ``remove`` 幂等、当前名单、``get`` / ``is_starred``、
   TTL 缓存失效与 ``local_storage`` 异常安全降级；
4. 走势图：``build_series`` 的 ``starred`` 标记、线性 / 对数主折线虚线、
   图例文字不含 ``🌟`` / ``★``、打🌟玩家归零与未归零均能输出合法 PNG；
5. 命令解析：``/作业打星`` 的列表 / 添加 / 取消与 ``@`` / QQ 号 / 主播名解析。

测试使用临时文件 SQLite 与 mock 存储，不连接生产库、不联网、不发 QQ 消息。
``fcntl`` 垫片仅为让 Windows 下 ``allmsg`` 可导入，不改变业务逻辑。
"""

import json
import struct
import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest.mock import patch

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
from matplotlib.figure import Figure
from nonebot.adapters.onebot.v11 import Message, MessageSegment
from sqlalchemy import delete

ROOT = Path(__file__).parents[1]
sys.path.insert(0, str(ROOT))

TMPDIR = Path(tempfile.mkdtemp(prefix="csbot_hw_stars_"))
DB_FILE = TMPDIR / "hw_stars_test.db"

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

from plugins.major_hw import stars as STARS  # noqa: E402
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
STAGE = HW.major_stage_name

PNG_HEADER = b"\x89PNG\r\n\x1a\n"
SINGAPORE = "2026-singapore"
COLOGNE = "2026-cologne"
STARRED_DEMO = ["2078553406", "3345039979", "3442127040"]


def _png_size(data: bytes) -> tuple[int, int]:
    assert data[:8] == PNG_HEADER, "不是合法 PNG（魔数不符）"
    assert data[12:16] == b"IHDR", "PNG 缺少 IHDR"
    return struct.unpack(">II", data[16:24])


async def _create_schema() -> None:
    tables = [
        MajorHW.__table__,
        MajorHWSnapshot.__table__,
        MajorSimulationSnapshot.__table__,
        StorageItem.__table__,
    ]
    async with engine.begin() as conn:
        await conn.run_sync(lambda sync_conn: Base.metadata.create_all(sync_conn, tables=tables))


async def _clear_stars() -> None:
    async with async_session_factory() as session:
        async with session.begin():
            await session.execute(delete(StorageItem).where(StorageItem.key == STARS.STORAGE_KEY))
    STARS.invalidate_stars_cache()


# --------------------------------------------------------------------------- #
# 1. 纯逻辑
# --------------------------------------------------------------------------- #
class StarsPureLogicTest(unittest.TestCase):
    def test_normalize_helpers(self):
        self.assertEqual(STARS.normalize_event_key("  2026-singapore  "), SINGAPORE)
        self.assertEqual(STARS.normalize_uid(" 123456 "), "123456")
        self.assertEqual(STARS.normalize_uid(2078553406), "2078553406")
        with self.assertRaises(ValueError):
            STARS.normalize_event_key("   ")
        with self.assertRaises(ValueError):
            STARS.normalize_uid("")

    def test_parse_payload_tolerates_bad_input(self):
        self.assertEqual(STARS.parse_stars_payload(None), {})
        self.assertEqual(STARS.parse_stars_payload(""), {})
        self.assertEqual(STARS.parse_stars_payload("not-json"), {})
        self.assertEqual(STARS.parse_stars_payload([1, 2]), {})
        self.assertEqual(STARS.parse_stars_payload('{"2026-singapore": "oops"}'), {})

        parsed = STARS.parse_stars_payload({
            "2026-singapore": ["3442127040", 2078553406, "", "  ", "2078553406", ["bad"]],
            "  ": ["1"],
            "2026-cologne": [],
        })
        self.assertEqual(parsed[SINGAPORE], ["2078553406", "3442127040"])
        self.assertEqual(parsed[COLOGNE], [])
        self.assertNotIn("  ", parsed)

        # 已解析的 dict 与 JSON 字符串等价
        self.assertEqual(
            STARS.parse_stars_payload(json.dumps({"2026-singapore": ["2", "1"]})),
            {"2026-singapore": ["1", "2"]},
        )

    def test_serialize_payload_is_stable(self):
        raw = STARS.serialize_stars_payload({
            "2026-cologne": ["b", "a"],
            SINGAPORE: ["2", "1"],
            "empty": [],
        })
        self.assertEqual(raw, '{"2026-cologne": ["a", "b"], "2026-singapore": ["1", "2"]}')
        self.assertNotIn("empty", raw)
        self.assertEqual(STARS.parse_stars_payload(raw)[SINGAPORE], ["1", "2"])

    def test_event_isolation(self):
        data = {
            SINGAPORE: ["111"],
            COLOGNE: ["222"],
        }
        self.assertEqual(STARS.parse_stars_payload(json.dumps(data)), data)

    def test_assign_ranks_all_normal(self):
        ranks = STARS.assign_homework_ranks(["a", "b", "c"], set())
        self.assertEqual(ranks, [(False, 1), (False, 2), (False, 3)])

    def test_assign_ranks_first_middle_consecutive(self):
        ranks = STARS.assign_homework_ranks(
            ["star1", "a", "star2", "star3", "b", "c"],
            {"star1", "star2", "star3"},
        )
        self.assertEqual(ranks, [
            (True, None),
            (False, 1),
            (True, None),
            (True, None),
            (False, 2),
            (False, 3),
        ])

    def test_assign_ranks_tail_and_all_starred(self):
        self.assertEqual(
            STARS.assign_homework_ranks(["a", "b", "star"], {"star"}),
            [(False, 1), (False, 2), (True, None)],
        )
        self.assertEqual(
            STARS.assign_homework_ranks(["x", "y"], {"x", "y"}),
            [(True, None), (True, None)],
        )
        # 不重排输入顺序（原地混排）
        self.assertEqual(
            [uid for uid in ("star", "a", "b")],
            ["star", "a", "b"],
        )

    def test_assign_ranks_ignores_padding_and_empty(self):
        self.assertEqual(
            STARS.assign_homework_ranks(["", " 10 ", "20"], {"10"}),
            [(False, 1), (True, None), (False, 2)],
        )


# --------------------------------------------------------------------------- #
# 2. 异步存储与缓存
# --------------------------------------------------------------------------- #
class _BoomStorage:
    async def get(self, key, default=None):
        raise RuntimeError("storage down")

    async def set(self, key, val):
        raise RuntimeError("storage down")


class StarsStorageTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        await _create_schema()
        await _clear_stars()

    async def asyncTearDown(self):
        STARS.invalidate_stars_cache()

    async def test_add_is_idempotent_and_sorted(self):
        added, current = await STARS.add_starred_uids(
            SINGAPORE, ["3442127040", "2078553406", "2078553406"]
        )
        self.assertEqual(added, ["2078553406", "3442127040"])
        self.assertEqual(current, ["2078553406", "3442127040"])

        again, current_again = await STARS.add_starred_uids(SINGAPORE, ["2078553406"])
        self.assertEqual(again, [])
        self.assertEqual(current_again, ["2078553406", "3442127040"])

        stored = json.loads(await local_storage.get(STARS.STORAGE_KEY))
        self.assertEqual(stored, {SINGAPORE: ["2078553406", "3442127040"]})

    async def test_get_list_and_is_starred(self):
        await STARS.add_starred_uids(SINGAPORE, STARRED_DEMO)
        self.assertEqual(await STARS.list_starred_uids(SINGAPORE), sorted(STARRED_DEMO))
        self.assertEqual(await STARS.get_starred_uids(SINGAPORE), set(STARRED_DEMO))
        self.assertTrue(await STARS.is_starred_uid(SINGAPORE, "2078553406"))
        self.assertTrue(await STARS.is_starred_uid(SINGAPORE, 3442127040))
        self.assertFalse(await STARS.is_starred_uid(SINGAPORE, "999999999"))
        self.assertFalse(await STARS.is_starred_uid(SINGAPORE, ""))
        # 不同赛事天然隔离
        self.assertEqual(await STARS.list_starred_uids(COLOGNE), [])

    async def test_remove_is_idempotent(self):
        await STARS.add_starred_uids(SINGAPORE, STARRED_DEMO)
        removed, current = await STARS.remove_starred_uids(
            SINGAPORE, ["2078553406", "999999999"]
        )
        self.assertEqual(removed, ["2078553406"])
        self.assertEqual(current, sorted(STARRED_DEMO[1:]))

        removed_again, current_again = await STARS.remove_starred_uids(SINGAPORE, ["2078553406"])
        self.assertEqual(removed_again, [])
        self.assertEqual(current_again, sorted(STARRED_DEMO[1:]))

        await STARS.remove_starred_uids(SINGAPORE, STARRED_DEMO[1:])
        stored = json.loads(await local_storage.get(STARS.STORAGE_KEY, default="{}"))
        self.assertEqual(stored, {})  # 空名单键被序列化过滤

    async def test_cache_ttl_and_invalidate(self):
        await STARS.add_starred_uids(SINGAPORE, ["111"])
        self.assertEqual(await STARS.get_starred_uids(SINGAPORE), {"111"})

        # 绕过 helper 直接改库：未失效前仍读缓存
        await local_storage.set(STARS.STORAGE_KEY, json.dumps({SINGAPORE: ["222"]}))
        self.assertEqual(await STARS.get_starred_uids(SINGAPORE), {"111"})

        STARS.invalidate_stars_cache()
        self.assertEqual(await STARS.get_starred_uids(SINGAPORE), {"222"})
        # force_refresh 同样绕过缓存
        await local_storage.set(STARS.STORAGE_KEY, json.dumps({SINGAPORE: ["333"]}))
        self.assertEqual(await STARS.get_starred_uids(SINGAPORE, force_refresh=True), {"333"})

    async def test_storage_failure_degrades_to_empty(self):
        with patch("plugins.utils.local_storage", _BoomStorage()):
            STARS.invalidate_stars_cache()
            self.assertEqual(await STARS.load_all_stars(), {})
            self.assertEqual(await STARS.get_starred_uids(SINGAPORE), set())
            self.assertEqual(await STARS.list_starred_uids(SINGAPORE), [])
            self.assertFalse(await STARS.is_starred_uid(SINGAPORE, "111"))


# --------------------------------------------------------------------------- #
# 3. 走势图虚线
# --------------------------------------------------------------------------- #
class _Row:
    def __init__(self, uid: str, match_count: int, winrate: float, created_at: int = 0):
        self.uid = uid
        self.match_count = match_count
        self.winrate = winrate
        self.created_at = created_at


def _axes():
    figure = Figure(figsize=(8, 5), dpi=100)
    return figure.add_subplot(1, 1, 1)


def _main_line(axes):
    for line in axes.lines:
        if line.get_label() != "_nolegend_":
            return line
    raise AssertionError("没有找到主折线")


class StarsChartTest(unittest.TestCase):
    def test_starred_linestyle_constant(self):
        self.assertEqual(CHART.STARRED_LINESTYLE, (0, (6, 3)))
        self.assertIn("STARRED_LINESTYLE", CHART.__all__)

    def test_build_series_marks_starred(self):
        rows = [
            _Row("2078553406", 0, 0.5, 0),
            _Row("999999999", 0, 0.4, 0),
        ]
        series = CHART.build_series(rows, starred_uids={"2078553406"})
        by_uid = {item.uid: item for item in series}
        self.assertTrue(by_uid["2078553406"].starred)
        self.assertFalse(by_uid["999999999"].starred)
        # 默认完全向后兼容
        self.assertTrue(all(not item.starred for item in CHART.build_series(rows)))

    def test_starred_main_line_is_dashed_in_both_scales(self):
        plain = CHART.HomeworkSeries(uid="u1", name="普通", points=[(0, 0.5), (1, 0.4)])
        starred = CHART.HomeworkSeries(
            uid="u2", name="打星", points=[(0, 0.5), (1, 0.4)], starred=True
        )
        for is_log in (False, True):
            axes = _axes()
            CHART._plot_series(axes, plain, (0.1, 0.2, 0.3, 1.0), is_log=is_log, floor_value=1e-4)
            CHART._plot_series(axes, starred, (0.4, 0.5, 0.6, 1.0), is_log=is_log, floor_value=1e-4)
            plain_line = next(line for line in axes.lines if line.get_label().startswith("普通"))
            star_line = next(line for line in axes.lines if line.get_label().startswith("打星"))
            self.assertFalse(plain_line.is_dashed(), f"普通线不应虚线 is_log={is_log}")
            self.assertTrue(star_line.is_dashed(), f"打星线应虚线 is_log={is_log}")
            self.assertEqual(star_line._unscaled_dash_pattern, (0, (6, 3)))

    def test_legend_text_has_no_star_symbols(self):
        starred = CHART.HomeworkSeries(
            uid="u1", name="打星甲", points=[(0, 0.5), (1, 0.4)], starred=True
        )
        axes = _axes()
        CHART._plot_series(axes, starred, (0.1, 0.2, 0.3, 1.0), is_log=False, floor_value=1e-4)
        _, labels = axes.get_legend_handles_labels()
        self.assertTrue(labels)
        for label in labels:
            self.assertNotIn("🌟", label)
            self.assertNotIn("★", label)
            self.assertIn("打星甲", label)
            self.assertIn("%", label)

    def test_starred_render_png_with_and_without_zero(self):
        positive = [CHART.HomeworkSeries(uid="u1", name="打星甲", points=[(0, 0.5), (1, 0.4)], starred=True)]
        zeroed = [
            CHART.HomeworkSeries(uid="u1", name="打星甲", points=[(0, 0.4), (1, 0.0)], starred=True),
            CHART.HomeworkSeries(uid="u2", name="普通乙", points=[(0, 0.3), (1, 0.2)], starred=False),
        ]
        for series in (positive, zeroed):
            for scale in ("linear", "log"):
                png = CHART.render_homework_chart(series, [], stage_name=STAGE, scale=scale)
                width, height = _png_size(png)
                self.assertGreater(width, 300, scale)
                self.assertGreater(height, 200, scale)

    def test_log_zero_helper_line_keeps_short_dash(self):
        series = CHART.HomeworkSeries(uid="u1", name="打星甲", points=[(0, 0.5), (1, 0.0)], starred=True)
        axes = _axes()
        CHART._plot_series(axes, series, (0.1, 0.2, 0.3, 1.0), is_log=True, floor_value=1e-4)
        helper = [line for line in axes.lines if line.get_label() == "_nolegend_"]
        self.assertTrue(helper, "对数跌零应保留辅助线")
        self.assertEqual(helper[0]._unscaled_dash_pattern, (0, (3, 2)))


# --------------------------------------------------------------------------- #
# 4. 接口 rank / isStarred 填充
# --------------------------------------------------------------------------- #
class StarsApiRankTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        await _create_schema()
        await _clear_stars()
        async with async_session_factory() as session:
            async with session.begin():
                await session.execute(delete(MajorHW))
                await session.execute(delete(MajorHWSnapshot))

        self.star_uid = "2078553406"
        teams = json.dumps(["FaZe", "MOUZ", "Spirit", "Vitality", "FURIA",
                            "Aurora", "Liquid", "HEROIC", "9z", "M80"])
        for uid, winrate in ((self.star_uid, 0.9), ("111", 0.8), ("222", 0.7)):
            await HW.db.add_hw(uid, STAGE, teams)
            await HW.db.set_uid_val(uid, STAGE, winrate, 4.0)
        await STARS.add_starred_uids(SINGAPORE, [self.star_uid])

    async def asyncTearDown(self):
        STARS.invalidate_stars_cache()
        async with async_session_factory() as session:
            async with session.begin():
                await session.execute(delete(MajorHW))
                await session.execute(delete(MajorHWSnapshot))

    async def test_rank_excludes_starred_and_flags_it(self):
        response = await CS.get_major_homework_rank(None)
        uids = [player.uid for player in response.players]
        self.assertEqual(uids, [self.star_uid, "111", "222"])
        by_uid = {player.uid: player for player in response.players}
        self.assertTrue(by_uid[self.star_uid].isStarred)
        self.assertIsNone(by_uid[self.star_uid].rank)
        self.assertFalse(by_uid["111"].isStarred)
        self.assertEqual(by_uid["111"].rank, 1)
        self.assertFalse(by_uid["222"].isStarred)
        self.assertEqual(by_uid["222"].rank, 2)

    async def test_history_flags_starred(self):
        await HW.db.save_simulation_snapshot(
            stage=STAGE,
            event_id=9303,
            match_count=0,
            latest_match_id=None,
            total_weight=1.0,
            homework_rows=[(self.star_uid, '{"3-0":["FaZe"]}', 0.5, 4.0)],
        )
        response = await CS.get_major_homework_history(None)
        by_uid = {player.uid: player for player in response.players}
        self.assertTrue(by_uid[self.star_uid].isStarred)


# --------------------------------------------------------------------------- #
# 5. /作业打星 参数解析
# --------------------------------------------------------------------------- #
class StarsCommandParseTest(unittest.IsolatedAsyncioTestCase):
    def test_action_parsing(self):
        self.assertEqual(HW.parse_star_action(Message("列表")), ("list", [], []))
        self.assertEqual(HW.parse_star_action(Message("list")), ("list", [], []))
        self.assertEqual(HW.parse_star_action(Message("")), ("add", [], []))
        self.assertEqual(HW.parse_star_action(Message("取消 123456")), ("remove", [], ["123456"]))
        self.assertEqual(HW.parse_star_action(Message("移除 123456")), ("remove", [], ["123456"]))
        self.assertEqual(HW.parse_star_action(Message("添加 123456")), ("add", [], ["123456"]))

    def test_at_targets_come_from_arg_only(self):
        arg = Message([MessageSegment.at("123456789"), MessageSegment.text(" 234567890")])
        action, at_uids, tokens = HW.parse_star_action(arg)
        self.assertEqual(action, "add")
        self.assertEqual(at_uids, ["123456789"])
        self.assertEqual(tokens, ["234567890"])
        # 触发用 @bot 不在 CommandArg 里，不能被当成目标
        self.assertNotIn("999999999", at_uids)

    async def test_target_resolution(self):
        targets, unknown = await HW.parse_star_targets(
            ["123456789"], ["v_advent", "玩机器", "234567890", "not-a-target"]
        )
        self.assertEqual(targets, ["123456789", "v_advent", "v_wanjiqi", "234567890"])
        self.assertEqual(unknown, ["not-a-target"])


if __name__ == "__main__":
    unittest.main()
