"""Synthetic runtime tests for the event watcher (no network or database)."""

import ast
import asyncio
import importlib.util
import json
from pathlib import Path
import sys
import time
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock


PLUGIN = Path(__file__).parents[1] / "plugins" / "hltv_watcher"
LOGIC_SPEC = importlib.util.spec_from_file_location(
    "hltv_watcher_logic_runtime", PLUGIN / "logic.py"
)
assert LOGIC_SPEC and LOGIC_SPEC.loader
LOGIC = importlib.util.module_from_spec(LOGIC_SPEC)
sys.modules[LOGIC_SPEC.name] = LOGIC
LOGIC_SPEC.loader.exec_module(LOGIC)

FUNCTIONS = {
    "_load_int",
    "_save_int",
    "_load_metadata",
    "_get_bot_or_none",
    "_followed_teams",
    "refresh_watch_list",
    "_send_notification",
    "_poll_event",
    "_poll_due_events",
}


class FakeStorage:
    def __init__(self):
        self.data = {}

    async def set(self, key, value):
        self.data[key] = value

    async def get(self, key, default=None):
        return self.data.get(key, default)


class FakeRuntimeConfig:
    def __init__(self, values):
        self.values = dict(values)
        self.set_calls = []

    async def get(self, key):
        return self.values.get(key)

    async def set(self, key, value, updated_by):
        self.values[key] = value
        self.set_calls.append((key, value, updated_by))


def make_event(event_id, name, *, grade="5", status="live", start_ts=None,
               end_ts=None, teams=()):
    now = int(time.time())
    return LOGIC.EventInfo(
        event_id=event_id,
        name=name,
        grade=grade,
        grade_label="Major" if grade == "1" else ("S级赛事" if grade == "2" else "C级赛事"),
        special_grade_label="Major" if grade == "1" else ("S级赛事" if grade == "2" else ""),
        status=status,
        start_ts=now - 3600 if start_ts is None else start_ts,
        end_ts=now + 3600 if end_ts is None else end_ts,
        teams=tuple(teams),
    )


def load_runtime():
    tree = ast.parse((PLUGIN / "__init__.py").read_text())
    body = []
    for node in tree.body:
        if isinstance(node, ast.Assign) and all(
            isinstance(target, ast.Name) and target.id.isupper()
            for target in node.targets
        ):
            body.append(node)
        elif isinstance(node, ast.AsyncFunctionDef) and node.name in FUNCTIONS:
            node.decorator_list = []
            body.append(node)

    storage = FakeStorage()
    runtime = FakeRuntimeConfig(
        {
            "hltv_event_id_list": [],
            "hltv_followed_teams": ["TYLOO", "LVG", "FaZe", "BC.Game"],
        }
    )
    bot = SimpleNamespace(send_msg=AsyncMock())
    event_update = AsyncMock()
    fetch_events = AsyncMock()
    get_matches = AsyncMock()
    namespace = {
        "asyncio": asyncio,
        "json": json,
        "random": __import__("random"),
        "time": time,
        "logger": Mock(),
        "local_storage": storage,
        "runtime_config": runtime,
        "config": SimpleNamespace(cs_group_list=[10001]),
        "event_update": event_update,
        "fetch_events": fetch_events,
        "get_matches": get_matches,
        "get_bot": Mock(return_value=bot),
        "FiveEApiError": LOGIC_ERROR,
        "Bot": object,
        "DEFAULT_FOLLOWED_TEAMS": LOGIC.DEFAULT_FOLLOWED_TEAMS,
        "EventInfo": LOGIC.EventInfo,
        "build_message": LOGIC.build_message,
        "events_from_json": LOGIC.events_from_json,
        "events_to_json": LOGIC.events_to_json,
        "notify_matches": LOGIC.notify_matches,
        "select_watch_events": LOGIC.select_watch_events,
        "should_poll_event": LOGIC.should_poll_event,
        "split_message": LOGIC.split_message,
        "poll_schedule": {},
    }
    exec(compile(ast.Module(body=body, type_ignores=[]), str(PLUGIN / "__init__.py"), "exec"), namespace)
    return namespace, storage, runtime, bot, event_update, fetch_events, get_matches


class FiveEApiError(RuntimeError):
    pass


LOGIC_ERROR = FiveEApiError


class RefreshTest(unittest.TestCase):
    def test_refresh_adds_followed_and_major_and_removes_finished(self):
        runtime, storage, config, bot, event_update, fetch_events, _ = load_runtime()
        now = int(time.time())
        fetch_events.return_value = [
            make_event(1, "Major", grade="1"),
            make_event(2, "LVG 杯", teams=["LVG"], start_ts=now + 3600, end_ts=now + 7200, status="upcoming"),
            make_event(3, "无关 C 级", teams=["G2"]),
            make_event(4, "已结束", teams=["TYLOO"], status="past", start_ts=now - 14400, end_ts=now - 10800),
        ]

        asyncio.run(runtime["refresh_watch_list"](force=True))

        self.assertEqual(config.values["hltv_event_id_list"], [1, 2])
        self.assertEqual(config.set_calls[0][2], "auto")
        cached = LOGIC.events_from_json(storage.data["hltv_event_meta"])
        self.assertEqual(set(cached), {1, 2})

    def test_refresh_keeps_previous_list_on_api_failure(self):
        runtime, storage, config, bot, event_update, fetch_events, _ = load_runtime()
        config.values["hltv_event_id_list"] = [111]
        fetch_events.side_effect = FiveEApiError("blocked")

        asyncio.run(runtime["refresh_watch_list"](force=True))

        self.assertEqual(config.values["hltv_event_id_list"], [111])
        self.assertEqual(config.set_calls, [])
        self.assertGreater(int(storage.data["hltv_next_discovery"]), int(time.time()))
        self.assertEqual(storage.data["hltv_discovery_failures"], "1")


class PollTest(unittest.TestCase):
    def _run_poll(self, event, matches, *, second_call=False):
        runtime, storage, config, bot, event_update, fetch_events, get_matches = load_runtime()
        config.values["hltv_event_id_list"] = [event.event_id]
        get_matches.return_value = (event.name, matches)
        asyncio.run(runtime["_poll_event"](event, ["TYLOO", "bcg"], bot))
        if second_call:
            asyncio.run(runtime["_poll_event"](event, ["TYLOO", "bcg"], bot))
        return runtime, storage, bot, event_update, get_matches

    def test_non_major_only_notifies_followed_team(self):
        event = make_event(10, "C 级杯", teams=["TYLOO"])
        matches = [
            ("TYLOO", "G2", "2:0", "m1"),
            ("A", "B", "2:1", "m2"),
        ]

        _, storage, bot, event_update, _ = self._run_poll(event, matches)

        self.assertEqual(bot.send_msg.await_count, 1)
        text = bot.send_msg.await_args.kwargs["message"]
        self.assertIn("TYLOO vs G2 2:0", text)
        self.assertNotIn("A vs B", text)
        stored = json.loads(storage.data["hltvresult10"])
        self.assertEqual([row[3] for row in stored], ["m2", "m1"])
        self.assertEqual(event_update.await_count, 2)

    def test_major_notifies_all_and_is_idempotent(self):
        event = make_event(11, "Major", grade="1")
        matches = [
            ("A", "B", "2:0", "m1"),
            ("C", "D", "2:1", "m2"),
        ]

        _, storage, bot, event_update, _ = self._run_poll(event, matches, second_call=True)

        self.assertEqual(bot.send_msg.await_count, 1)
        text = bot.send_msg.await_args.kwargs["message"]
        self.assertIn("A vs B 2:0", text)
        self.assertIn("C vs D 2:1", text)
        self.assertEqual(event_update.await_count, 2)
        stored = json.loads(storage.data["hltvresult11"])
        self.assertEqual([row[3] for row in stored], ["m2", "m1"])

    def test_upcoming_events_are_not_polled(self):
        runtime, storage, config, bot, event_update, fetch_events, get_matches = load_runtime()
        now = int(time.time())
        live = make_event(20, "live", status="live")
        upcoming = make_event(
            21, "upcoming", status="upcoming",
            start_ts=now + 3600, end_ts=now + 7200,
        )
        config.values["hltv_event_id_list"] = [20, 21]
        storage.data["hltv_event_meta"] = LOGIC.events_to_json([live, upcoming])
        get_matches.return_value = ("live", [])

        asyncio.run(runtime["_poll_due_events"](bot, now_ts=now))
        asyncio.run(runtime["_poll_due_events"](bot, now_ts=now + 31))

        self.assertEqual(get_matches.await_count, 1)
        self.assertEqual(get_matches.await_args.args[0], 20)

    def test_live_events_are_staggered(self):
        runtime, storage, config, bot, event_update, fetch_events, get_matches = load_runtime()
        now = int(time.time())
        events = [make_event(100 + index, f"live-{index}") for index in range(4)]
        config.values["hltv_event_id_list"] = [event.event_id for event in events]
        storage.data["hltv_event_meta"] = LOGIC.events_to_json(events)
        get_matches.return_value = ("live", [])

        asyncio.run(runtime["_poll_due_events"](bot, now_ts=now))
        self.assertEqual(get_matches.await_count, 0)
        asyncio.run(runtime["_poll_due_events"](bot, now_ts=now + 31))
        self.assertEqual(get_matches.await_count, 1)
        asyncio.run(runtime["_poll_due_events"](bot, now_ts=now + 98))
        self.assertEqual(get_matches.await_count, 2)
        asyncio.run(runtime["_poll_due_events"](bot, now_ts=now + 170))
        self.assertEqual(get_matches.await_count, 3)
        asyncio.run(runtime["_poll_due_events"](bot, now_ts=now + 240))
        self.assertEqual(get_matches.await_count, 4)

    def test_rate_limit_cooldown_skips_polling(self):
        runtime, storage, config, bot, event_update, fetch_events, get_matches = load_runtime()
        now = int(time.time())
        live = make_event(22, "live", status="live")
        config.values["hltv_event_id_list"] = [22]
        storage.data["hltv_event_meta"] = LOGIC.events_to_json([live])
        storage.data["hltv_poll_cooldown"] = str(now + 600)

        asyncio.run(runtime["_poll_due_events"](bot, now_ts=now))

        self.assertEqual(get_matches.await_count, 0)


if __name__ == "__main__":
    unittest.main()
