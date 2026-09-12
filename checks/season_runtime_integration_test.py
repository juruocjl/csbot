"""Run against an isolated in-memory database; never connects to production."""

import ast
import asyncio
import json
from pathlib import Path
import sys
import time
from typing import Any
import unittest
from unittest.mock import AsyncMock, patch

import nonebot
from pydantic import BaseModel, Field
from sqlalchemy import insert, select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine


ROOT = Path(__file__).parents[1]
sys.path.insert(0, str(ROOT))
nonebot.init(
    driver="~fastapi",
    cs_database="sqlite+aiosqlite:///:memory:",
    cs_mysteam_id=0,
    cs_wmtoken="unused-test-token",
)
for plugin in ("models", "utils", "runtime_config", "cs_db_val", "cs_db_upd"):
    assert nonebot.load_plugin(ROOT / "plugins" / plugin) is not None

from plugins import cs_db_upd as updater
from plugins import cs_db_val as queries
from plugins import runtime_config as settings
from plugins.models import MatchStatsPW, RuntimeConfig, SteamBaseInfo, SteamDetailInfo, SteamExtraInfo


def load_watch_stage_code(sessions, manager):
    """Load the actual profile methods without starting unrelated bot integrations."""
    path = ROOT / "plugins" / "cs_server" / "__init__.py"
    tree = ast.parse(path.read_text(encoding="utf-8"))
    methods = {
        "__init__", "_snapshot_for_player", "_profiles_for_players", "_use_profile_season",
        "_profile_from_external", "_ladder_score_from_base", "_optional_int", "_optional_float",
    }
    nodes = []
    for node in tree.body:
        if isinstance(node, ast.ClassDef) and node.name in {
            "WatchStageLivePlayer", "WatchStagePlayerProfile", "WatchStageSnapshot",
        }:
            nodes.append(node)
        elif isinstance(node, ast.ClassDef) and node.name == "WatchStageManager":
            node.body = [method for method in node.body if getattr(method, "name", None) in methods]
            nodes.append(node)
        elif isinstance(node, ast.AsyncFunctionDef) and node.name == "update_season_stats_cache":
            node.decorator_list = []
            nodes.append(node)
    namespace = {
        "__name__": __name__, "asyncio": asyncio, "json": json, "time": time,
        "Any": Any, "BaseModel": BaseModel, "Field": Field, "select": select,
        "SteamBaseInfo": SteamBaseInfo, "SteamDetailInfo": SteamDetailInfo,
        "SteamExtraInfo": SteamExtraInfo, "SQLAlchemyError": SQLAlchemyError,
        "logger": nonebot.logger, "async_session_factory": sessions, "runtime_config": manager,
        "SEASON_STATS_CACHE": {}, "_background_jobs_disabled": lambda: False,
        "_calculate_global_stats": AsyncMock(return_value={}),
    }
    exec(compile(ast.Module(body=nodes, type_ignores=[]), str(path), "exec"), namespace)
    return namespace


def row_values(model, **overrides):
    values = {column.key: column.type.python_type() for column in model.__table__.columns}
    return values | overrides


class FakeResponse:
    def __init__(self, data):
        self.data = data

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        pass

    async def json(self):
        return self.data


class SeasonRuntimeIntegrationTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.engine = create_async_engine("sqlite+aiosqlite:///:memory:")
        self.sessions = async_sessionmaker(self.engine, expire_on_commit=False)
        self.patches = [
            patch.object(module, "async_session_factory", self.sessions)
            for module in (settings, queries, updater)
        ]
        for patched in self.patches:
            patched.start()
        async with self.engine.begin() as connection:
            for model in (RuntimeConfig, SteamBaseInfo, SteamDetailInfo, SteamExtraInfo, MatchStatsPW):
                await connection.run_sync(model.__table__.create)
        self.manager = settings.RuntimeConfigManager()
        self.manager.register_default("cs_ai_model", "environment-model")
        self.manager.register_default("cs_time_locations", {"测试地点": "Etc/UTC"})
        self.manager.register_default("live_watch_list", ["dy_6657"])
        await self.manager.ensure_defaults()
        await self.manager.set("cs_season_id", "S41", "admin")
        await self.manager.set("cs_last_season_id", "S40", "admin")

    async def asyncTearDown(self):
        for patched in reversed(self.patches):
            patched.stop()
        await self.engine.dispose()

    async def test_defaults_do_not_overwrite_saved_seasons(self):
        await self.manager.ensure_defaults()
        items = {item.key: item.value for item in await self.manager.list_values()}
        self.assertEqual(items["cs_season_id"], "S41")
        self.assertEqual(items["cs_last_season_id"], "S40")
        self.assertEqual(items["cs_ai_model"], "environment-model")
        self.assertEqual(items["cs_time_locations"], {"测试地点": "Etc/UTC"})
        self.assertEqual(items["live_watch_list"], ["dy_6657"])

    async def test_runtime_changes_refresh_filters_and_default_detail_season(self):
        async with self.sessions.begin() as session:
            for season in ("S39", "S40", "S41", "S42"):
                await session.execute(insert(SteamDetailInfo), row_values(
                    SteamDetailInfo, steamid="player", seasonId=season,
                ))
                await session.execute(insert(MatchStatsPW), row_values(
                    MatchStatsPW, steamid="player", mid=season, seasonId=season, mode="天梯",
                ))

        self.assertEqual((await queries.db.get_detail_info("player")).seasonId, "S41")
        self.assertEqual(await queries.db.get_matches_count("player", "本赛季"), 1)
        old_snapshot = await self.manager.get_seasons()
        await self.manager.set("cs_season_id", "S42", "admin")
        await self.manager.set("cs_last_season_id", "S39", "admin")

        self.assertEqual((old_snapshot.current, old_snapshot.previous), ("S41", "S40"))
        self.assertEqual((await queries.db.get_detail_info("player")).seasonId, "S42")
        self.assertEqual((await queries.db.get_detail_info("player", "S40")).seasonId, "S40")
        self.assertIn("S42", await queries.get_time_sql("本赛季"))
        self.assertIn("S39", await queries.get_time_sql("上赛季"))
        both = await queries.get_time_sql("两赛季")
        self.assertIn("S42", both)
        self.assertIn("S39", both)
        matches = await queries.db.get_matches("player", "两赛季", only_ladder=True)
        self.assertEqual({match.seasonId for match in matches}, {"S42", "S39"})
        async with self.sessions() as session:
            custom = await queries.get_custom_filter("player", "本赛季")
            self.assertEqual(list((await session.execute(select(MatchStatsPW).where(*custom))).scalars()), [])

    async def test_invalid_update_leaves_database_value_unchanged(self):
        with self.assertRaises(ValueError):
            await self.manager.set("cs_season_id", "invalid", "admin")
        self.assertEqual(await self.manager.get("cs_season_id"), "S41")

    async def test_model_time_locations_and_live_list_update_without_reseeding(self):
        self.assertEqual(await self.manager.get("cs_ai_model"), "environment-model")
        self.assertEqual(
            await self.manager.get("cs_time_locations"),
            {"测试地点": "Etc/UTC"},
        )
        self.assertEqual(await self.manager.get("live_watch_list"), ["dy_6657"])

        await self.manager.set("cs_ai_model", "hot-model", "admin")
        await self.manager.set(
            "cs_time_locations",
            {"北京": "Asia/Shanghai", "纽约": "America/New_York"},
            "admin",
        )
        await self.manager.set(
            "live_watch_list",
            ["dy_123", "bili_456"],
            "admin",
        )
        self.manager.register_default("cs_ai_model", "changed-environment-model")
        await self.manager.ensure_defaults()

        self.assertEqual(await self.manager.get("cs_ai_model"), "hot-model")
        self.assertEqual(
            await self.manager.get("cs_time_locations"),
            {"北京": "Asia/Shanghai", "纽约": "America/New_York"},
        )
        self.assertEqual(
            await self.manager.get("live_watch_list"),
            ["dy_123", "bili_456"],
        )

    async def test_watch_stage_snapshot_does_not_reuse_previous_season_profile(self):
        code = load_watch_stage_code(self.sessions, self.manager)
        watch_stage = code["WatchStageManager"]()
        async with self.sessions.begin() as session:
            await session.execute(insert(SteamBaseInfo), row_values(
                SteamBaseInfo, steamid="player", name="player", ladderScore=json.dumps([
                    {"season": "S41", "score": 2200}, {"season": "S42", "score": 3300},
                ]),
            ))
            for season, score in (("S41", 4100), ("S42", 4200)):
                await session.execute(insert(SteamDetailInfo), row_values(
                    SteamDetailInfo, steamid="player", seasonId=season, pvpScore=score,
                ))
        watch_stage.latest_snapshots["player"] = code["WatchStageSnapshot"](
            status="running", requestedSteamId="player",
            players=[code["WatchStageLivePlayer"](steamId="player", side="CT")],
        )
        first = await watch_stage._snapshot_for_player("player")
        self.assertEqual(first.profiles["player"].pvpScore, 4100)
        watch_stage.profile_cache["player"].faceitElo = 2000
        same = await watch_stage._snapshot_for_player("player")
        self.assertEqual(same.profiles["player"].faceitElo, 2000)
        await self.manager.set("cs_season_id", "S42", "admin")
        second = await watch_stage._snapshot_for_player("player")
        self.assertEqual(second.profiles["player"].pvpScore, 4200)
        base = await queries.db.get_base_info("player")
        self.assertEqual(watch_stage._ladder_score_from_base(base, "S42"), (3300, 0))
        await self.manager.set("cs_season_id", "S43", "admin")
        empty = await watch_stage._snapshot_for_player("player")
        self.assertIsNone(empty.profiles["player"].pvpScore)

    async def test_statistics_cache_warmup_uses_current_runtime_season(self):
        code = load_watch_stage_code(self.sessions, self.manager)
        await code["update_season_stats_cache"]()
        await self.manager.set("cs_season_id", "S42", "admin")
        await code["update_season_stats_cache"]()
        self.assertEqual(
            [call.args[0] for call in code["_calculate_global_stats"].call_args_list],
            ["S41", "S42"],
        )

    async def test_stats_card_and_extra_info_read_seasons_at_call_time(self):
        calls = []

        def post(url, *, json, **kwargs):
            calls.append(json["csgoSeasonId"])
            return FakeResponse({"statusCode": 0, "data": {
                "avatar": "", "name": "player", "ladderScoreList": [],
            }})

        client = unittest.mock.Mock(post=post)
        db = updater.DataManager()
        with patch.object(updater, "get_session", return_value=client), \
             patch.object(db, "_insert_detail_info", new_callable=AsyncMock), \
             patch.object(updater.asyncio, "sleep", new_callable=AsyncMock):
            async with self.sessions.begin() as session:
                await db._update_stats_card("player", session, interval=-1)
            await self.manager.set("cs_season_id", "S42", "admin")
            await self.manager.set("cs_last_season_id", "S41", "admin")
            async with self.sessions.begin() as session:
                await db._update_stats_card("player", session, interval=-1)
        self.assertEqual(calls, ["S41", "S40", "S42", "S41"])
        session = AsyncMock()
        session.get.return_value = None
        await db._update_extra_info("player", session)
        self.assertEqual(session.get.call_args_list[1].args[1], ("player", "S42"))
        self.assertEqual(session.get.call_args_list[2].args[1], ("player", "S41"))

    async def test_full_refresh_uses_one_snapshot_even_if_config_changes_mid_run(self):
        async with self.sessions.begin() as session:
            session.add(SteamBaseInfo(
                steamid="player", name="player", updateTime=0, updateMatchTime=0,
                avatarlink="", lasttime=0, ladderScore="[]",
            ))
        captures = []
        payloads = []

        async def stats_card(steamid, session, *, seasons):
            captures.append(seasons)
            await self.manager.set("cs_season_id", "S42", "admin")
            await self.manager.set("cs_last_season_id", "S41", "admin")

        async def extra_info(steamid, session, *, seasons):
            captures.append(seasons)

        def post(url, *, json, **kwargs):
            payloads.append(json)
            return FakeResponse({"statusCode": 0, "data": {"matchList": [], "dataPublic": True}})

        db = updater.DataManager()
        with patch.object(db, "_update_stats_card", side_effect=stats_card), \
             patch.object(db, "_update_extra_info", side_effect=extra_info), \
             patch.object(db, "_update_faceit_matches", new_callable=AsyncMock, return_value=[]), \
             patch.object(updater, "get_session", return_value=unittest.mock.Mock(post=post)), \
             patch.object(updater.asyncio, "sleep", new_callable=AsyncMock):
            await db.update_stats("player", interval=0)
            await db.update_stats("player", interval=0)
        self.assertEqual([item.current for item in captures], ["S41", "S41", "S42", "S42"])
        self.assertEqual(
            [payload["csgoSeasonId"] for payload in payloads if "csgoSeasonId" in payload],
            ["S41", "S40", "S42", "S41"],
        )


class SeasonCallSiteTest(unittest.TestCase):
    def test_async_helpers_are_awaited_at_every_call_site(self):
        names = {"get_time_sql", "get_ladder_filter", "get_custom_filter", "_snapshot_for_player"}
        for path in (ROOT / "plugins").rglob("*.py"):
            tree = ast.parse(path.read_text(encoding="utf-8"))
            parents = {child: node for node in ast.walk(tree) for child in ast.iter_child_nodes(node)}
            for node in ast.walk(tree):
                if not isinstance(node, ast.Call):
                    continue
                name = getattr(node.func, "id", getattr(node.func, "attr", None))
                if name in names:
                    with self.subTest(path=path, line=node.lineno):
                        self.assertIsInstance(parents[node], ast.Await)


if __name__ == "__main__":
    unittest.main()
