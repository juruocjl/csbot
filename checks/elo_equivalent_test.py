"""ELO S 等效分逻辑与查询层单元测试。

使用内存 SQLite 与独立 elo_logic 模块，不连接生产库、不发送 QQ 消息。
"""

import hashlib
import importlib.util
import json
from pathlib import Path
import sys
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, patch

import nonebot
from sqlalchemy import insert
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine


ROOT = Path(__file__).parents[1]
sys.path.insert(0, str(ROOT))

LOGIC_PATH = ROOT / "plugins" / "cs_db_val" / "elo_logic.py"
SPEC = importlib.util.spec_from_file_location("cs_db_val_elo_logic_under_test", LOGIC_PATH)
assert SPEC and SPEC.loader
ELO_LOGIC = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = ELO_LOGIC
SPEC.loader.exec_module(ELO_LOGIC)

calc_effective_pvp_score = ELO_LOGIC.calc_effective_pvp_score
format_s_rank_badge_label = ELO_LOGIC.format_s_rank_badge_label

nonebot.init(
    driver="~fastapi",
    cs_database="sqlite+aiosqlite:///:memory:",
    cs_mysteam_id=0,
    cs_wmtoken="unused-test-token",
)
for plugin in ("models", "utils", "runtime_config", "cs_db_val"):
    assert nonebot.load_plugin(ROOT / "plugins" / plugin) is not None

from plugins import cs_db_val as queries
from plugins.models import MatchStatsPW


STEAMID = "76561190000000001"


def match_row(mid, *, steamid=STEAMID, time_stamp=1_700_000_000, pvp_score=2401,
              pvp_stars=0, mode="天梯", season="S41"):
    """按模型列类型构造一行比赛数据，避免遗漏非空列。"""
    values = {}
    for column in MatchStatsPW.__table__.columns:
        try:
            values[column.key] = column.type.python_type()
        except NotImplementedError:  # pragma: no cover - 当前模型无此类型
            values[column.key] = None
    values.update(
        mid=mid,
        steamid=steamid,
        timeStamp=time_stamp,
        pvpScore=pvp_score,
        pvpStars=pvp_stars,
        mode=mode,
        seasonId=season,
    )
    return values


class EloEquivalentLogicTest(unittest.TestCase):
    def test_effective_score_boundaries(self):
        cases = [
            (None, 0, 0),
            (None, 5, 0),
            (0, 0, 0),
            (0, 5, 0),
            (-1, 0, 0),
            (1500, 0, 1500),
            (2400, 0, 2400),
            (2401, 0, 2401),
            (2401, 1, 2426),
            (2401, 2, 2451),
            (2401, 24, 3001),
            (2401, 50, 3651),
            (2401, -3, 2401),
            (2401, None, 2401),
            (2502, 0, 2502),  # PVP周末联赛：保持原值，不回退
            (2350, 5, 2350),  # 非 S 段位异常星数不影响分数
            (2401.9, 1, 2426),  # 浮点原始分按 int 截断
        ]
        for score, stars, expected in cases:
            with self.subTest(score=score, stars=stars):
                self.assertEqual(calc_effective_pvp_score(score, stars), expected)

    def test_badge_boundaries(self):
        cases = [
            (None, 0, None),
            (None, 5, None),
            (0, 0, None),
            (2399, 5, None),
            (2400, 0, None),
            (2401, 0, "S ★0"),
            (2401, 2, "S ★2"),
            (2401, None, "S ★0"),
            (2401, -2, "S ★0"),
            (2502, 0, None),  # 周末联赛不加 S 标注
            (2502, 3, "S ★3"),
        ]
        for score, stars, expected in cases:
            with self.subTest(score=score, stars=stars):
                self.assertEqual(format_s_rank_badge_label(score, stars), expected)


class EloQueryTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.engine = create_async_engine("sqlite+aiosqlite:///:memory:")
        self.sessions = async_sessionmaker(self.engine, expire_on_commit=False)
        self.patches = [
            patch.object(queries, "async_session_factory", self.sessions),
            patch.object(
                queries.runtime_config,
                "get_seasons",
                AsyncMock(return_value=SimpleNamespace(current="S41", previous="S40")),
            ),
        ]
        for patched in self.patches:
            patched.start()
        async with self.engine.begin() as connection:
            await connection.run_sync(MatchStatsPW.__table__.create)

    async def asyncTearDown(self):
        for patched in reversed(self.patches):
            patched.stop()
        await self.engine.dispose()

    async def insert_matches(self, *rows):
        async with self.engine.begin() as connection:
            for row in rows:
                await connection.execute(insert(MatchStatsPW).values(**row))

    async def test_elo_detail_uses_latest_s_match_with_stars(self):
        await self.insert_matches(
            match_row("m1", time_stamp=1000, pvp_score=2399, pvp_stars=0),
            match_row("m2", time_stamp=2000, pvp_score=2401, pvp_stars=2),
        )

        detail = await queries.db.get_elo_detail(STEAMID, "本赛季")

        self.assertEqual(detail, (2451.0, 2, 2401, 2))
        self.assertEqual(format_s_rank_badge_label(detail[2], detail[3]), "S ★2")

    async def test_elo_detail_keeps_weekend_league_raw_score(self):
        await self.insert_matches(
            match_row("w1", time_stamp=1000, pvp_score=2502, pvp_stars=0, mode="PVP周末联赛"),
        )

        detail = await queries.db.get_elo_detail(STEAMID, "本赛季")

        self.assertEqual(detail, (2502.0, 1, 2502, 0))
        self.assertIsNone(format_s_rank_badge_label(detail[2], detail[3]))

    async def test_elo_detail_rejects_unranked_player(self):
        await self.insert_matches(
            match_row("u1", time_stamp=1000, pvp_score=0, pvp_stars=0),
        )

        with self.assertRaises(queries.NoValueError):
            await queries.db.get_elo_detail(STEAMID, "本赛季")
        with self.assertRaises(queries.NoValueError):
            await queries.get_elo(STEAMID, "本赛季")

    async def test_elo_detail_rejects_player_without_matches(self):
        with self.assertRaises(queries.NoValueError):
            await queries.db.get_elo_detail(STEAMID, "本赛季")

    async def test_get_elo_matches_detail_and_registration(self):
        await self.insert_matches(
            match_row("m1", time_stamp=1000, pvp_score=2401, pvp_stars=1),
            match_row("m2", time_stamp=2000, pvp_score=2401, pvp_stars=2),
        )

        config = queries.db.get_value_config("ELO")

        self.assertEqual(config.title, "天梯分数")
        self.assertEqual(config.allowed_time, ["本赛季", "上赛季"])
        self.assertTrue(config.reversed)
        self.assertEqual(await queries.get_elo(STEAMID, "本赛季"), (2451.0, 2))

    async def test_history_series_uses_effective_scores(self):
        await self.insert_matches(
            match_row("h1", time_stamp=1000, pvp_score=2399, pvp_stars=0),
            match_row("h2", time_stamp=2000, pvp_score=2401, pvp_stars=0),
            match_row("h3", time_stamp=3000, pvp_score=2401, pvp_stars=1),
            match_row("h4", time_stamp=4000, pvp_score=2401, pvp_stars=2),
            # 未定段与非法模式不计入曲线
            match_row("h5", time_stamp=5000, pvp_score=0, pvp_stars=0),
            match_row("h6", time_stamp=6000, pvp_score=2502, pvp_stars=0, mode="匹配"),
            # 其他玩家数据互不影响
            match_row("h7", steamid="76561190000000002", time_stamp=7000, pvp_score=3000),
        )

        history = await queries.db.get_pvp_score_history(STEAMID, "本赛季")

        self.assertEqual(history, [
            (1000, 2399),
            (2000, 2401),
            (3000, 2426),
            (4000, 2451),
        ])

    async def test_history_keeps_weekend_league_raw_score(self):
        await self.insert_matches(
            match_row("w1", time_stamp=1000, pvp_score=2401, pvp_stars=3, mode="天梯"),
            match_row("w2", time_stamp=2000, pvp_score=2530, pvp_stars=0, mode="PVP周末联赛"),
        )

        history = await queries.db.get_pvp_score_history(STEAMID, "本赛季")

        self.assertEqual(history, [(1000, 2476), (2000, 2530)])


class SourceManifestTest(unittest.TestCase):
    def test_manifest_hashes_match_modified_plugin_files(self):
        manifest_path = ROOT / "ai_runtime" / "source-manifest.json"
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        for relative in ("plugins/cs_db_val/__init__.py", "plugins/cs/__init__.py"):
            with self.subTest(path=relative):
                data = (ROOT / relative).read_bytes().replace(b"\r\n", b"\n")
                self.assertEqual(manifest["files"][relative], hashlib.sha256(data).hexdigest())


if __name__ == "__main__":
    unittest.main()
