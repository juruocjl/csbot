"""虚拟主播作业全链路离线单元测试。

覆盖：
1. 虚拟用户配置解析 / 合并 / 校验 / 别名解析；
2. 头像与展示名 helper（虚拟用户 vs 普通 QQ 用户）；
3. ``local_storage`` 动态覆盖与热注册；
4. ``cs_server`` 的 rank / history / personal / 淘汰赛接口返回 displayName 与配置头像；
5. ``/主播作业`` 列表、添加、录入（瑞士轮）与错误分支。

测试使用临时文件 SQLite 与 AsyncMock Matcher，不连接生产库、不联网、不发送 QQ 消息。
``fcntl`` 垫片仅为让 Windows 下 ``allmsg`` 可导入，不改变任何业务逻辑。
"""

import json
import sys
import tempfile
import types
import unittest
from pathlib import Path
from types import SimpleNamespace
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
from nonebot.adapters.onebot.v11 import Message
from nonebot.exception import FinishedException
from sqlalchemy import delete, select

ROOT = Path(__file__).parents[1]
sys.path.insert(0, str(ROOT))

TMPDIR = Path(tempfile.mkdtemp(prefix="csbot_virtual_users_"))
DB_FILE = TMPDIR / "virtual_users_test.db"

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
from plugins.major_hw import virtual_users as VU  # noqa: E402
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
QQ_AVATAR = "https://q1.qlogo.cn/g?b=qq&nk={uid}&s=100"
FRONT_PUBLIC = ROOT.parent / "csbot-front" / "public"

STAGE = major_stage_name
TEAMS = [
    "100 Thieves", "FaZe",
    "Liquid", "PARIVISION", "Eternal Fire", "HEROIC", "FlyQuest", "9z",
    "Chinggis Warriors", "Wildcard",
]


async def _create_schema() -> None:
    # SQLite 不支持某些业务表的复合主键自增，只建本测试使用的三张表
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
            for model in (MajorHW, MajorHWSnapshot, StorageItem):
                await session.execute(delete(model))


class VirtualUserParsingTest(unittest.TestCase):
    def test_uid_validation(self):
        self.assertTrue(VU.is_valid_uid("v_advent"))
        self.assertTrue(VU.is_valid_uid("v_wanjiqi"))
        self.assertTrue(VU.is_valid_uid("a" * 20))
        self.assertFalse(VU.is_valid_uid("a" * 21))
        self.assertFalse(VU.is_valid_uid(""))
        self.assertFalse(VU.is_valid_uid("CS-advent"))
        self.assertFalse(VU.is_valid_uid("玩机器"))
        self.assertFalse(VU.is_valid_uid("v advent"))

    def test_parse_entries_list_and_mapping(self):
        listed = VU.parse_entries(
            [{"uid": "v_a", "name": "主播A", "aliases": ["a", "aa"], "avatar": "/a.jpg"}]
        )
        self.assertEqual(len(listed), 1)
        self.assertEqual(listed[0].uid, "v_a")
        self.assertEqual(listed[0].aliases, ("a", "aa"))

        mapped = VU.parse_entries(
            {"v_b": {"name": "主播B", "aliases": "b,bb|bbb"}, "v_c": "主播C"}
        )
        self.assertEqual([user.uid for user in mapped], ["v_b", "v_c"])
        self.assertEqual(mapped[0].aliases, ("b", "bb", "bbb"))
        self.assertEqual(mapped[1].name, "主播C")

    def test_invalid_entries_are_skipped(self):
        users = VU.parse_entries(
            [
                {"uid": "too-long-uid-xxxxxxxxxx", "name": "bad"},
                {"uid": "bad uid", "name": "bad"},
                "not-a-dict",
                {"uid": "ok_uid"},
            ]
        )
        self.assertEqual([user.uid for user in users], ["ok_uid"])
        self.assertEqual(users[0].name, "ok_uid")

    def test_merge_override(self):
        defaults = VU.parse_entries([{"uid": "v_a", "name": "A"}, {"uid": "v_b", "name": "B"}])
        dynamic = VU.parse_entries([{"uid": "v_b", "name": "B2"}, {"uid": "v_c", "name": "C"}])
        merged = VU.merge_entries(defaults, dynamic)
        self.assertEqual([user.uid for user in merged], ["v_a", "v_b", "v_c"])
        self.assertEqual(merged[1].name, "B2")

    def test_registry_resolve(self):
        registry = VU.VirtualUserRegistry(
            VU.parse_entries(
                [
                    {"uid": "v_advent", "name": "CS-advent", "aliases": ["advent", "259"]},
                    {"uid": "v_wanjiqi", "name": "玩机器", "aliases": ["wanjiqi", "wjq", "玩机器CSGO"]},
                ]
            )
        )
        cases = {
            "v_advent": "v_advent",
            "V_ADVENT": "v_advent",
            "CS-advent": "v_advent",
            "cs advent": "v_advent",
            "advent": "v_advent",
            "259": "v_advent",
            "CSADVENT": "v_advent",
            "玩机器": "v_wanjiqi",
            "玩机器CSGO": "v_wanjiqi",
            "WJQ": "v_wanjiqi",
            "ＷＪＱ": "v_wanjiqi",  # NFKC 全角
        }
        for text, expected in cases.items():
            user = registry.resolve(text)
            self.assertIsNotNone(user, text)
            self.assertEqual(user.uid, expected, text)

        self.assertIsNone(registry.resolve("123456789"))
        self.assertIsNone(registry.resolve(""))
        self.assertIsNone(registry.resolve(None))

    def test_default_config_file(self):
        registry = VU.load_default_registry()
        self.assertEqual(len(registry), 2)
        advent = registry.get("v_advent")
        wanjiqi = registry.get("v_wanjiqi")
        self.assertIsNotNone(advent)
        self.assertIsNotNone(wanjiqi)
        self.assertEqual(advent.name, "CS-advent")
        self.assertEqual(wanjiqi.name, "玩机器")
        self.assertIn("259", advent.aliases)
        self.assertIn("wjq", wanjiqi.aliases)


class VirtualUserHelperTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        await _create_schema()
        await _clear_db()
        VU.invalidate_cache()

    async def asyncTearDown(self):
        VU.invalidate_cache()

    async def test_default_registry_loaded(self):
        registry = await VU.get_registry(force=True)
        self.assertEqual(len(registry), 2)
        self.assertEqual(registry.get("v_advent").name, "CS-advent")

    async def test_avatar_and_display_name(self):
        self.assertEqual(await VU.virtual_avatar("v_advent"), "/major-avatars/advent.jpg")
        self.assertEqual(await VU.virtual_display_name("v_advent"), "CS-advent")
        self.assertEqual(await VU.virtual_avatar("v_wanjiqi"), "/major-avatars/wanjiqi.jpg")
        self.assertEqual(await VU.virtual_display_name("v_wanjiqi"), "玩机器")

        # 普通 QQ uid 完全保持原逻辑
        self.assertEqual(await VU.virtual_avatar("123456789"), QQ_AVATAR.format(uid="123456789"))
        self.assertIsNone(await VU.virtual_display_name("123456789"))

    async def test_avatar_files_exist_in_frontend(self):
        registry = await VU.get_registry(force=True)
        for user in registry:
            self.assertTrue(user.avatar.startswith("/major-avatars/"), user.uid)
            local = FRONT_PUBLIC / user.avatar.lstrip("/")
            self.assertTrue(local.is_file(), f"缺少主播头像文件 {local}")

    async def test_local_storage_override(self):
        await local_storage.set(
            VU.STORAGE_KEY,
            json.dumps([
                {
                    "uid": "v_advent",
                    "name": "ADVENT-259",
                    "aliases": ["advent259"],
                    "avatar": "/major-avatars/advent.jpg",
                }
            ], ensure_ascii=False),
        )
        registry = await VU.get_registry(force=True)
        self.assertEqual(registry.get("v_advent").name, "ADVENT-259")
        self.assertEqual(await VU.virtual_display_name("v_advent"), "ADVENT-259")
        self.assertEqual(len(registry), 2)  # 未覆盖的默认项仍保留

    async def test_invalid_storage_payload_keeps_defaults(self):
        await local_storage.set(VU.STORAGE_KEY, "{not-json")
        registry = await VU.get_registry(force=True)
        self.assertEqual(len(registry), 2)

    async def test_register_virtual_user(self):
        user = await VU.register_virtual_user(
            "v_test", "测试主播", "/major-avatars/test.jpg", ["测测", "cs_test"]
        )
        self.assertEqual(user.uid, "v_test")
        self.assertEqual((await VU.get_registry(force=True)).get("v_test").name, "测试主播")
        self.assertEqual((await VU.get_registry()).resolve("测测").uid, "v_test")

        raw = json.loads(await local_storage.get(VU.STORAGE_KEY))
        self.assertEqual(raw[0]["uid"], "v_test")
        self.assertEqual(raw[0]["aliases"], ["测测", "cs_test"])

        with self.assertRaises(ValueError):
            await VU.register_virtual_user("bad uid", "坏", "")


class MajorHomeworkApiTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        await _create_schema()
        await _clear_db()
        VU.invalidate_cache()
        await major_db.add_hw("v_advent", STAGE, json.dumps(TEAMS))
        await major_db.add_hw("123456789", STAGE, json.dumps(TEAMS[::-1]))

    async def asyncTearDown(self):
        VU.invalidate_cache()

    async def test_rank_endpoint_virtual_and_qq(self):
        response = await CS.get_major_homework_rank(None)
        players = {player.uid: player for player in response.players}
        self.assertIn("v_advent", players)
        self.assertIn("123456789", players)

        advent = players["v_advent"]
        self.assertEqual(advent.avatar, "/major-avatars/advent.jpg")
        self.assertEqual(advent.displayName, "CS-advent")
        self.assertEqual(len(advent.picks["3-0"]), 2)
        self.assertEqual(len(advent.picks["3-1/3-2"]), 6)
        self.assertEqual(len(advent.picks["0-3"]), 2)

        normal = players["123456789"]
        self.assertEqual(normal.avatar, QQ_AVATAR.format(uid="123456789"))
        self.assertIsNone(normal.displayName)

    async def test_history_endpoint(self):
        await major_db.save_simulation_snapshot(
            stage=STAGE,
            event_id=9303,
            match_count=0,
            latest_match_id=None,
            total_weight=0.0,
            homework_rows=[("v_wanjiqi", json.dumps({"3-0": TEAMS[:2]}), 0.5, 4.0)],
        )
        response = await CS.get_major_homework_history(None)
        players = {player.uid: player for player in response.players}
        self.assertIn("v_wanjiqi", players)
        self.assertEqual(players["v_wanjiqi"].avatar, "/major-avatars/wanjiqi.jpg")
        self.assertEqual(players["v_wanjiqi"].displayName, "玩机器")

    async def test_personal_endpoint(self):
        response = await CS.get_major_homework_personal(
            uid="v_advent", info=SimpleNamespace(user_id="123456789")
        )
        self.assertEqual(response.avatar, "/major-avatars/advent.jpg")
        self.assertEqual(response.displayName, "CS-advent")
        self.assertEqual(response.uid, "v_advent")

        normal = await CS.get_major_homework_personal(
            uid="123456789", info=SimpleNamespace(user_id="123456789")
        )
        self.assertEqual(normal.avatar, QQ_AVATAR.format(uid="123456789"))
        self.assertIsNone(normal.displayName)

    async def test_playoff_members_uses_virtual_identity(self):
        await major_db.add_hw("v_wanjiqi", STAGE + "-quad", json.dumps(["FaZe", "Liquid", "NIP", "9z"]))
        state = {
            "winners": {"4强": [], "2强": [], "冠军": []},
            "losers": {"4强": [], "2强": [], "冠军": []},
            "eliminated": set(),
            "records": {},
        }
        players = await CS._major_playoff_homework_members(state)
        players_by_uid = {player.uid: player for player in players}
        self.assertIn("v_wanjiqi", players_by_uid)
        self.assertEqual(players_by_uid["v_wanjiqi"].avatar, "/major-avatars/wanjiqi.jpg")
        self.assertEqual(players_by_uid["v_wanjiqi"].displayName, "玩机器")

    async def test_response_models_backward_compatible(self):
        item = CS.MajorHomeworkRankItem(uid="1", avatar="x", picks={})
        self.assertIsNone(item.displayName)
        history = CS.MajorHomeworkHistoryItem(uid="1", avatar="x", points=[])
        self.assertIsNone(history.displayName)
        personal = CS.MajorHomeworkPersonalResponse(
            stage=STAGE, uid="1", avatar="x", categories=[], rows=[]
        )
        self.assertIsNone(personal.displayName)

    def test_cs_server_major_avatars_are_not_hardcoded(self):
        source = (ROOT / "plugins" / "cs_server" / "__init__.py").read_text(encoding="utf-8")
        self.assertNotIn("q1.qlogo.cn", source)
        self.assertGreaterEqual(source.count("virtual_avatar("), 4)
        self.assertGreaterEqual(source.count("virtual_display_name("), 4)


class StreamerCommandTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        await _create_schema()
        await _clear_db()
        VU.invalidate_cache()
        self.matcher = HW.hwstreamer
        self._orig_send = self.matcher.send
        self._orig_finish = self.matcher.finish
        self.matcher.send = AsyncMock()
        self.matcher.finish = AsyncMock(side_effect=FinishedException)

    async def asyncTearDown(self):
        self.matcher.send = self._orig_send
        self.matcher.finish = self._orig_finish
        VU.invalidate_cache()

    async def _run(self, text: str):
        with self.assertRaises(FinishedException):
            await HW.hwstreamer_function(None, Message(text))
        return (
            self.matcher.send.await_args.args[0] if self.matcher.send.await_args else "",
            self.matcher.finish.await_args.args[0] if self.matcher.finish.await_args else "",
        )

    async def test_list(self):
        _, finished = await self._run("列表")
        self.assertIn("CS-advent", finished)
        self.assertIn("玩机器", finished)
        self.assertIn("v_advent", finished)

    async def test_add_virtual_user(self):
        _, finished = await self._run("添加 v_new 新主播 /major-avatars/new.jpg 新新,xx")
        self.assertIn("v_new", finished)
        registry = await VU.get_registry(force=True)
        user = registry.get("v_new")
        self.assertIsNotNone(user)
        self.assertEqual(user.name, "新主播")
        self.assertEqual(user.aliases, ("新新", "xx"))
        self.assertIsNotNone(registry.resolve("新新"))

    async def test_add_requires_arguments(self):
        _, finished = await self._run("添加 v_new")
        self.assertIn("格式", finished)

    async def test_record_swiss_homework(self):
        sent, finished = await self._run("v_advent " + ",".join(TEAMS))
        self.assertIn("模糊匹配得到队伍", self.matcher.send.await_args_list[0].args[0])
        self.assertIn("CS-advent", finished)
        self.assertIn(">= 5 的概率", finished)

        row = await major_db.get_uid_hw("v_advent", STAGE)
        self.assertIsNotNone(row)
        self.assertEqual(json.loads(row.teams), TEAMS)

        async with async_session_factory() as session:
            snapshots = list((await session.execute(
                select(MajorHWSnapshot).where(MajorHWSnapshot.uid == "v_advent")
            )).scalars().all())
        self.assertEqual(len(snapshots), 1)
        self.assertEqual(json.loads(snapshots[0].homework_text)["3-0"], TEAMS[:2])

    async def test_record_by_alias_and_uid(self):
        await self._run("玩机器 " + ",".join(TEAMS))
        self.assertIsNotNone(await major_db.get_uid_hw("v_wanjiqi", STAGE))

    async def test_unknown_streamer(self):
        _, finished = await self._run("不存在的主播 " + ",".join(TEAMS))
        self.assertIn("未找到虚拟主播", finished)

    async def test_invalid_team_count(self):
        _, finished = await self._run("v_advent " + ",".join(TEAMS[:9]))
        self.assertIn("十只不同队伍", finished)

    async def test_usage_without_arguments(self):
        _, finished = await self._run("")
        self.assertIn("/主播作业", finished)


class TextTargetLookupTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        await _create_schema()
        await _clear_db()
        VU.invalidate_cache()

    async def test_text_target_returns_virtual_uid(self):
        self.assertEqual(await HW._major_text_target(Message("玩机器")), "v_wanjiqi")
        self.assertEqual(await HW._major_text_target(Message("v_advent")), "v_advent")
        self.assertIsNone(await HW._major_text_target(Message("123456789")))
        self.assertIsNone(await HW._major_text_target(Message("")))


if __name__ == "__main__":
    unittest.main(verbosity=2)
