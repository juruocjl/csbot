"""Synthetic SQL/API acceptance: no production database, network or QQ sends."""
import copy
import importlib.util
import json
from pathlib import Path
import sys
import time
import types
import unittest
from types import SimpleNamespace

import nonebot
from fastapi import FastAPI, Header, HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

ROOT = Path(__file__).parents[1]
sys.path.insert(0, str(ROOT))
nonebot.init(_env_file=None, driver="~fastapi", cs_database="sqlite+aiosqlite:///:memory:")
for name in ("models", "utils"):
    assert nonebot.load_plugin(ROOT / "plugins" / name)
# Load the real modules without starting the production schedulers/commands.
package = types.ModuleType("plugins.hltv_watcher")
package.__path__ = [str(ROOT / "plugins" / "hltv_watcher")]
sys.modules[package.__name__] = package
from plugins.models import Base, ScheduleDelivery, ScheduleMatch, ScheduleTeamSource
from plugins.hltv_watcher.logic import EventInfo, event_from_api
from plugins.hltv_watcher.schedule_logic import legacy_results, parse_schedule_matches
from plugins.hltv_watcher.store import ScheduleStore
from plugins.hltv_watcher import api as schedule_api


def event(eid=1, name="普通杯", teams=("A", "B"), ids=("csgo_tm_1", "hltv_team_1"), grade="5"):
    now = int(time.time())
    return EventInfo(event_id=eid, name=name, grade=grade, status="live", start_ts=now - 3600,
                     end_ts=now + 86400, teams=teams,
                     team_refs=tuple({"external_id": i, "name": n, "logo": ""} for i, n in zip(ids, teams)))


def match(eid=1, mid="m1", status=0, names=("A", "B"), ids=("csgo_tm_1", "hltv_team_1"), start=None):
    return {"mc_info": {"id": mid, "plan_ts": str(start or int(time.time()) + 600), "format": "3", "star": "3",
            "t1_info": {"id": ids[0], "disp_name": names[0]}, "t2_info": {"id": ids[1], "disp_name": names[1]}},
            "tt_info": {"id": f"csgo_tt_{eid}", "disp_name": "普通杯"},
            "state": {"status": str(status), "t1_score": "0", "t2_score": "2", "bout_states": []}}


class ScheduleAcceptance(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.engine = create_async_engine("sqlite+aiosqlite:///:memory:")
        self.sessions = async_sessionmaker(self.engine, expire_on_commit=False)
        self.tables = [t for t in Base.metadata.sorted_tables if t.name.startswith("schedule_")]
        async with self.engine.begin() as conn:
            await conn.run_sync(lambda c: Base.metadata.create_all(c, tables=self.tables))
        self.store = ScheduleStore(self.sessions)
        self.event = event()
        await self.store.save_events([self.event])
        self.a = (await self.store.search("csgo_tm_1"))[0]["id"]
        self.b = (await self.store.search("hltv_team_1"))[0]["id"]

    async def asyncTearDown(self):
        await self.engine.dispose()

    async def ingest(self, raw, event_info=None, defaults=(), known=()):
        e = event_info or self.event
        await self.store.ingest(e, parse_schedule_matches(e.event_id, [raw]), set(defaults), ["100"], known)

    async def deliveries(self):
        async with self.sessions() as session:
            return (await session.scalars(select(ScheduleDelivery))).all()

    async def test_every_users_team_adds_event_and_last_unfollow_removes_reason(self):
        self.assertEqual((await self.store.watched([]))[0], [])
        await self.store.follow("111", self.a, "100")
        await self.store.follow("222", self.a, "200")
        self.assertEqual([e.event_id for e in (await self.store.watched([]))[0]], [1])
        await self.store.unfollow("111", self.a)
        self.assertEqual(len((await self.store.watched([]))[0]), 1)
        await self.store.unfollow("222", self.a)
        self.assertEqual((await self.store.watched([]))[0], [])
        self.assertEqual(len((await self.store.watched([], [1]))[0]), 1)

    async def test_identity_survives_rename_academy_and_numeric_suffix_collisions(self):
        self.assertNotEqual(self.a, self.b)
        await self.store.follow("111", self.a, "100")
        await self.store.save_events([event(teams=("A New", "A Academy"))])
        self.assertEqual((await self.store.follows("111"))[0]["id"], self.a)
        self.assertEqual((await self.store.search("A"))[0]["id"], self.a)
        self.assertEqual(await self.store.default_ids(["A Academy"]), {self.b})
        self.assertEqual(await self.store.default_ids(["A New"]), {self.a})

    async def test_schedule_filters_and_preserves_sides_and_no_future_fake_score(self):
        await self.store.follow("111", self.a, "100")
        raw = match(); await self.ingest(raw)
        data = await self.store.schedule([1], "111")
        self.assertEqual(data["matches"][0]["team1"]["team_id"], self.a)
        self.assertIsNone(data["matches"][0]["score1"])
        self.assertEqual((await self.store.schedule([1], "222"))["total"], 0)
        self.assertEqual((await self.store.schedule([]))["total"], 0)
        self.assertEqual(parse_schedule_matches(1, [match(eid=2)]), [])
        raw["state"]["status"] = "2"
        parsed = parse_schedule_matches(1, [raw])
        self.assertEqual(legacy_results(parsed), [("B", "A", "2:0", "m1")])
        self.assertEqual(parsed[0]["team1"]["name"], "A")

    async def test_two_teams_and_default_push_merge_per_group_and_are_idempotent(self):
        await self.store.follow("111", self.a, "100")
        await self.store.follow("111", self.b, "100")
        await self.store.follow("222", self.b, "100")
        await self.store.follow("333", self.a, "200")
        await self.ingest(match(), defaults=[self.a])
        await self.ingest(match(status=2), defaults=[self.a])
        await self.ingest(match(status=2), defaults=[self.a])
        rows = await self.deliveries()
        self.assertEqual(len(rows), 2)
        recipients = {r.group_id: json.loads(r.recipients) for r in rows}
        self.assertEqual(recipients, {"100": ["111", "222"], "200": ["333"]})
        claim = await self.store.claim_delivery({"100"}, {"100", "200"})
        await self.store.delivery_result(claim["key"], "sent")
        store_after_restart = ScheduleStore(self.sessions)
        claim2 = await store_after_restart.claim_delivery({"100"}, {"100", "200"})
        self.assertNotEqual(claim["group_id"], claim2["group_id"])

    async def test_baseline_and_missing_ids_do_not_notify_wrong_team(self):
        await self.store.follow("111", self.a, "100")
        await self.ingest(match(status=2, start=int(time.time()) - 86400))
        self.assertEqual(await self.deliveries(), [])
        await self.ingest(match(mid="unknown", status=2, ids=("", "")))
        self.assertEqual(await self.deliveries(), [])

    async def test_stars_are_preserved_without_triggering_result_corrections(self):
        await self.store.follow('111', self.a, '100')
        raw = match(status=0)
        await self.ingest(raw)
        self.assertEqual((await self.store.schedule([1], '111'))['matches'][0]['stars'], 3)
        raw['state']['status']='2'; await self.ingest(raw)
        raw['mc_info']['star']='5'; await self.ingest(raw)
        self.assertEqual(len(await self.deliveries()), 1)

    async def test_start_and_result_each_send_once_with_merged_mentions(self):
        await self.store.follow('111', self.a, '100')
        await self.store.follow('222', self.b, '100')
        await self.ingest(match(), defaults=[self.a])
        await self.ingest(match(status=1), defaults=[self.a])
        await self.ingest(match(status=1), defaults=[self.a])
        start = await self.store.claim_delivery({'100'}, {'100'})
        self.assertEqual(start['key'][2], 0)
        self.assertEqual(start['users'], ['111','222'])
        self.assertIn('比赛开始', start['text'])
        await self.store.delivery_result(start['key'], 'sent')
        self.assertIsNone(await self.store.claim_delivery({'100'}, {'100'}))
        await self.ingest(match(status=2), defaults=[self.a])
        result = await self.store.claim_delivery({'100'}, {'100'})
        self.assertEqual(result['key'][2], 1)
        self.assertIn('比赛结束', result['text'])
        self.assertEqual(len(await self.deliveries()), 2)

    async def test_following_already_live_match_does_not_backfill_start(self):
        raw = match(status=1, start=int(time.time()) - 3600)
        await self.ingest(raw)
        await self.store.follow('111', self.a, '100')
        await self.ingest(raw)
        self.assertEqual(await self.deliveries(), [])

    async def test_pending_start_is_cancelled_when_match_finishes(self):
        await self.store.follow('111', self.a, '100')
        await self.ingest(match())
        await self.ingest(match(status=1))
        await self.ingest(match(status=2))
        notification = await self.store.claim_delivery(set(), {'100'})
        self.assertEqual(notification['key'][2], 1)
        self.assertIn('比赛结束', notification['text'])

    async def test_major_without_followed_teams_has_no_start_broadcast(self):
        major = event(eid=2, grade='1')
        await self.store.save_events([major])
        await self.ingest(match(eid=2), event_info=major)
        await self.ingest(match(eid=2, status=1), event_info=major)
        self.assertEqual(await self.deliveries(), [])

    async def test_unsubscribe_cancels_queued_mentions_and_whitelists_are_separate(self):
        await self.store.follow("111", self.a, "200")
        await self.ingest(match())
        await self.ingest(match(status=2))
        await self.store.unfollow("111", self.a, group_only="200")
        self.assertIsNone(await self.store.claim_delivery({"100"}, {"200"}))
        self.assertEqual(len(await self.store.follows("111")), 1)

    async def test_correction_and_retry_states_do_not_regress_finished_match(self):
        await self.store.follow("111", self.a, "100")
        await self.ingest(match(status=2))
        claim = await self.store.claim_delivery(set(), {"100"})
        await self.store.delivery_result(claim["key"], "unknown")
        self.assertIsNone(await self.store.claim_delivery(set(), {"100"}))
        raw = match(status=2); raw["state"]["t1_score"] = "1"
        await self.ingest(raw)
        claim = await self.store.claim_delivery(set(), {"100"})
        self.assertIn("赛果更正", claim["text"])
        await self.ingest(match(status=0))
        async with self.sessions() as session:
            self.assertEqual((await session.get(ScheduleMatch, "m1")).status, "finished")

    async def test_live_priority_pagination_and_unknown_status(self):
        await self.store.follow("111", self.a, "100")
        for n in range(12):
            await self.ingest(match(mid=f"m{n:02}", status=1 if n == 11 else 0))
        first = await self.store.schedule([1], "111")
        self.assertEqual(first["total"], 12)
        self.assertEqual(first["matches"][0]["status"], "live")
        self.assertEqual(len((await self.store.schedule([1], "111", 2))["matches"]), 2)
        self.assertEqual(parse_schedule_matches(1, [match(status=99)])[0]["status"], "unknown")
        incomplete = match(status=2); del incomplete['state']['t1_score']
        self.assertEqual(parse_schedule_matches(1, [incomplete])[0]['status'], 'unknown')

    async def test_api_target_is_readable_but_writer_cannot_choose_qq(self):
        # Exercise real endpoint registration and SQL with auth represented by a fixture.
        schedule_api.schedule_store = self.store
        config = SimpleNamespace(get=lambda key: None)
        async def setting(key): return []
        config.get = setting
        async def identity(authorization: str | None = Header(None)):
            if authorization != "Bearer fixture": raise HTTPException(401)
            return SimpleNamespace(user_id="111", group_id="100")
        app = FastAPI()
        schedule_api.register_schedule_api(app, identity, identity, config)
        import httpx
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
            self.assertEqual((await client.post('/api/schedule', json={})).status_code, 401)
            headers = {"Authorization": "Bearer fixture"}
            response = await client.post('/api/schedule/follow', headers=headers,
                json={"teamId": self.a, "user_id": "999", "group_id": "999"})
            self.assertEqual(response.status_code, 200)
            self.assertEqual((await self.store.follows("111"))[0]["groups"], ["100"])
            self.assertEqual(await self.store.follows("999"), [])
            response = await client.post('/api/schedule', headers=headers, json={"uid": "222", "page": 1})
            self.assertEqual(response.status_code, 200)
            self.assertEqual(response.json()["uid"], "222")
            self.assertEqual((await client.post('/api/schedule', headers=headers, json={"uid": "bad"})).status_code, 400)

    async def test_explicit_migration_matches_models_and_reverses(self):
        from alembic.migration import MigrationContext
        from alembic.operations import Operations
        from alembic.autogenerate import compare_metadata
        from sqlalchemy import MetaData
        spec = importlib.util.spec_from_file_location('schedule_migration', ROOT/'mymigrations/versions/20261102_add_schedule.py')
        migration = importlib.util.module_from_spec(spec); spec.loader.exec_module(migration)
        engine = create_async_engine('sqlite+aiosqlite:///:memory:')
        def verify(connection):
            context = MigrationContext.configure(connection)
            migration.op = Operations(context)
            migration.upgrade()
            expected = MetaData()
            for table in self.tables: table.to_metadata(expected)
            self.assertEqual(compare_metadata(context, expected), [])
            migration.downgrade()
            self.assertEqual(compare_metadata(context, MetaData()), [])
        try:
            async with engine.begin() as connection: await connection.run_sync(verify)
        finally: await engine.dispose()

    async def test_schedule_archive_uses_same_route_and_keeps_page_filter(self):
        from ai_runtime.image_archive import page_source
        self.assertEqual(page_source('/schedule/user/111?page=2&eventId=1'),
                         {'page':'/schedule/user/111','params':{'page':'2','eventId':'1'}})
        self.assertIsNone(page_source('/schedule/user/111?token=secret'))
        self.assertIsNone(page_source('/schedule/user/not-a-qq'))


if __name__ == '__main__': unittest.main()
