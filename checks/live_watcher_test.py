"""Synthetic room responses and mocked storage/OneBot; no network or database."""

import ast
import asyncio
import importlib.util
import json
from pathlib import Path
import sys
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, Mock, patch

import aiohttp


PLUGIN = Path(__file__).parents[1] / "plugins" / "live_watcher"
SPEC = importlib.util.spec_from_file_location("douyu_status", PLUGIN / "douyu.py")
DOUYU = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = DOUYU
SPEC.loader.exec_module(DOUYU)


def room_response(**overrides):
    return {"room": {"room_id": 6979222, "nickname": "玩机器丶Machine",
                     "room_name": "一起打游戏", "show_status": 1, "videoLoop": 0} | overrides}


def mobile_page(rid=6979222, vip_id=6657, **overrides):
    data = {"pageProps": {"room": {"roomInfo": {"roomInfo": {
        "rid": rid, "vipId": vip_id, "isLive": 1} | overrides}}}}
    return '<script type="application/json" id="vike_pageContext">' + json.dumps(data) + '</script>'


class Response:
    def __init__(self, data=None, text="", error=None):
        self.data, self.body, self.error = data, text, error

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return False

    def raise_for_status(self):
        if self.error:
            raise self.error

    async def json(self):
        if self.data is None:
            raise ValueError("not JSON")
        return self.data

    async def text(self):
        return self.body


def load_poll():
    tree = ast.parse((PLUGIN / "__init__.py").read_text())
    nodes = [node for node in tree.body if isinstance(node, ast.AsyncFunctionDef)
             and node.name in ("get_live_status", "live_watcher")]
    for node in nodes:
        node.decorator_list = []
    bot = SimpleNamespace(send_msg=AsyncMock())
    db = SimpleNamespace(get_live_status=AsyncMock(return_value=0), set_live_status=AsyncMock())
    namespace = {"asyncio": asyncio, "fetch_douyu_status": DOUYU.fetch_douyu_status,
                 "get_session": Mock(), "get_bot": Mock(return_value=bot), "db": db,
                 "config": SimpleNamespace(cs_group_list=[10001]), "logger": Mock(),
                 "runtime_config": SimpleNamespace(get=AsyncMock(return_value=["dy_6979222"])),
                 "now_live_state": "无数据"}
    exec(compile(ast.Module(body=nodes, type_ignores=[]), str(PLUGIN / "__init__.py"), "exec"), namespace)
    return namespace, bot, db


class DouyuParsingTest(unittest.TestCase):
    def test_live_offline_and_platform_replay(self):
        for show_status, video_loop, expected, replay in [(1, 0, 1, False), (2, 0, 0, False),
                                                         (1, 1, 0, True), (2, 1, 0, False)]:
            with self.subTest(show=show_status, replay=video_loop):
                result = DOUYU.parse_room(room_response(show_status=show_status, videoLoop=video_loop), "6979222")
                self.assertEqual((result.islive, result.is_replay), (expected, replay))

    def test_explicit_rebroadcast_title_even_without_platform_flag(self):
        for title in ["【PUBG鱼乐赏金赛】DAY3重播", "比赛回放", "今晚录播", "精彩轮播"]:
            with self.subTest(title=title):
                result = DOUYU.parse_room(room_response(room_name=title), "6979222")
                self.assertEqual((result.islive, result.is_replay), (0, True))

    def test_string_flags_supported(self):
        result = DOUYU.parse_room(room_response(show_status="1", videoLoop="1", room_id="6979222"), "6979222")
        self.assertEqual(result.islive, 0)

    def test_incomplete_unknown_or_wrong_room_is_not_offline(self):
        for field, value in [("show_status", None), ("show_status", 0), ("show_status", True),
                             ("videoLoop", None), ("videoLoop", 2), ("videoLoop", False),
                             ("nickname", ""), ("room_name", None), ("room_id", 6657)]:
            with self.subTest(field=field, value=value), self.assertRaises(ValueError):
                DOUYU.parse_room(room_response(**{field: value}), "6979222")
        for data in [{}, {"room": []}, [], None]:
            with self.subTest(data=data), self.assertRaises(ValueError):
                DOUYU.parse_room(data, "6979222")

    def test_vanity_identity_checked(self):
        self.assertEqual(DOUYU.resolve_room_id(mobile_page(), "6657"), "6979222")
        self.assertEqual(DOUYU.resolve_room_id(mobile_page(), "6979222"), "6979222")
        for html in ["<title>停止服务公告</title>", mobile_page(rid=123, vip_id=456),
                     mobile_page(rid=0), mobile_page(rid=True)]:
            with self.subTest(html=html), self.assertRaises(ValueError):
                DOUYU.resolve_room_id(html, "6657")


class DouyuFetchingTest(unittest.IsolatedAsyncioTestCase):
    async def test_canonical_room_needs_one_bounded_request(self):
        session = SimpleNamespace(get=Mock(return_value=Response(room_response())))
        result = await DOUYU.fetch_douyu_status(session, "dy_6979222")
        self.assertEqual(result.islive, 1)
        session.get.assert_called_once()
        self.assertEqual(session.get.call_args.kwargs["timeout"].total, 10)

    async def test_vanity_resolution_checks_replay_in_real_room(self):
        session = SimpleNamespace(get=Mock(side_effect=[Response(text="error page"),
            Response(text=mobile_page()), Response(room_response(videoLoop=1))]))
        result = await DOUYU.fetch_douyu_status(session, "dy_6657")
        self.assertEqual((result.islive, result.is_replay), (0, True))
        self.assertEqual(session.get.call_args_list[-1].args[0], "https://www.douyu.com/betard/6979222")
        self.assertTrue(all("douyucdn.cn" not in call.args[0] for call in session.get.call_args_list))

    async def test_mobile_is_live_is_never_used_as_status(self):
        session = SimpleNamespace(get=Mock(side_effect=[Response(text="error page"),
            Response(text=mobile_page()), Response(room_response(videoLoop=None))]))
        with self.assertRaises(ValueError):
            await DOUYU.fetch_douyu_status(session, "dy_6657")

    async def test_failure_does_not_retry_same_room_or_assume_offline(self):
        for error in [TimeoutError(), aiohttp.ClientConnectionError("unavailable")]:
            session = SimpleNamespace(get=Mock(side_effect=[Response(error=error), Response(text=mobile_page())]))
            with self.subTest(error=error), self.assertRaises(ValueError):
                await DOUYU.fetch_douyu_status(session, "dy_6979222")
            self.assertEqual(session.get.call_count, 2)

    async def test_bad_id_does_not_send_request(self):
        session = SimpleNamespace(get=Mock())
        for liveid in ["dy_0", "dy_", "dy_6657/other", "bili_6657"]:
            with self.subTest(liveid=liveid), self.assertRaises(ValueError):
                await DOUYU.fetch_douyu_status(session, liveid)
        session.get.assert_not_called()


class LivePollTest(unittest.IsolatedAsyncioTestCase):
    async def test_replay_updates_non_live_status_without_alert(self):
        for fields in [{"videoLoop": 1}, {"room_name": "赛事重播"}]:
            namespace, bot, db = load_poll()
            namespace["get_session"].return_value = SimpleNamespace(get=Mock(return_value=Response(room_response(**fields))))
            db.get_live_status.return_value = 1
            with patch.object(asyncio, "sleep", AsyncMock()):
                await namespace["live_watcher"]()
            self.assertIn("玩机器丶Machine（回放） 0", namespace["now_live_state"])
            db.set_live_status.assert_awaited_once_with("dy_6979222", 0)
            bot.send_msg.assert_not_awaited()

    async def test_only_offline_to_live_transition_alerts(self):
        namespace, bot, db = load_poll()
        namespace["get_session"].return_value = SimpleNamespace(get=Mock(return_value=Response(room_response())))
        with patch.object(asyncio, "sleep", AsyncMock()):
            await namespace["live_watcher"]()
            db.get_live_status.return_value = 1
            await namespace["live_watcher"]()
        bot.send_msg.assert_awaited_once_with(message_type="group", group_id=10001, message="玩机器丶Machine 开播了")

    async def test_failure_stays_visible_preserves_status_and_continues(self):
        for failure in [ValueError("upstream failure"), None]:
            namespace, bot, db = load_poll()
            namespace["runtime_config"].get.return_value = ["dy_6979222", "bili_123"]
            namespace["get_live_status"] = AsyncMock(side_effect=[failure, (0, "另一主播")])
            await namespace["live_watcher"]()
            self.assertEqual(namespace["now_live_state"], "dy_6979222 查询失败\n另一主播 0")
            db.set_live_status.assert_awaited_once_with("bili_123", 0)
            bot.send_msg.assert_not_awaited()

    async def test_cancellation_preserves_last_snapshot(self):
        namespace, bot, db = load_poll()
        namespace["get_live_status"] = AsyncMock(side_effect=asyncio.CancelledError())
        await namespace["live_watcher"]()
        self.assertEqual(namespace["now_live_state"], "无数据")
        db.set_live_status.assert_not_awaited()
        bot.send_msg.assert_not_awaited()


if __name__ == "__main__":
    unittest.main()
