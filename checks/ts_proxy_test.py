"""Offline tests for the TeamSpeak edge HTTP proxy and the plugin client path.

Everything here is hermetic: no TeamSpeak server, no Tailnet, no network. The
proxy is started on a random loopback port against mocked query functions, and
the HTTP queries mock their TS3 connections.
"""
from __future__ import annotations

import asyncio
import http.client
import importlib.util
import json
import os
from pathlib import Path
import sys
import tempfile
import threading
import types
import unittest
from unittest import mock

import aiohttp

ROOT = Path(__file__).resolve().parents[1]
PLUGIN = ROOT / "plugins" / "ts"
PROXY_PATH = ROOT / "scripts" / "ts_query_proxy.py"


def _load(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def _load_plugin_modules():
    package = types.ModuleType("ts_plugin_under_test")
    package.__path__ = [str(PLUGIN)]
    sys.modules[package.__name__] = package
    modules = {
        "tree": _load("ts_plugin_under_test.tree", PLUGIN / "tree.py"),
        "client": _load("ts_plugin_under_test.client", PLUGIN / "client.py"),
        "direct": _load("ts_plugin_under_test.direct", PLUGIN / "direct.py"),
        "service": _load("ts_plugin_under_test.service", PLUGIN / "service.py"),
        "config": _load("ts_plugin_under_test.config", PLUGIN / "config.py"),
    }
    return modules


TREE = _load_plugin_modules()
PROXY = _load("ts_query_proxy_under_test", PROXY_PATH)


class FakeResult:
    def __init__(self, rows):
        self._rows = [dict(row) for row in rows]

    def first(self):
        return dict(self._rows[0]) if self._rows else {}

    def all(self):
        return [dict(row) for row in self._rows]

    def __iter__(self):
        return iter(self._rows)


class FakeConnection:
    """Records query commands; serves pre-baked serverinfo/channellist/clientlist."""

    def __init__(self, serverinfo, channels, clients):
        self.serverinfo = dict(serverinfo)
        self.channels = [dict(row) for row in channels]
        self.clients = [dict(row) for row in clients]
        self.calls = []
        self.open_args = None
        self.closed = False

    def open(self, host, port, timeout=None, protocol="telnet", tp_args=None):
        self.open_args = {
            "host": host, "port": port, "timeout": timeout,
            "tp_args": tp_args,
        }

    def exec_(self, command, **params):
        self.calls.append(("exec:" + command, params))
        if command == "clientlist":
            return FakeResult(self.clients)
        return FakeResult([])

    def query(self, command, **params):
        self.calls.append(("query:" + command, params))
        data = {
            "serverinfo": [self.serverinfo],
            "channellist": self.channels,
            "clientlist": self.clients,
        }[command]
        return FakeResult(data)

    def close(self):
        self.closed = True


def sample_tree_data():
    serverinfo = {"virtualserver_name": "测试服"}
    channels = [
        # Deliberate ordering: two orphans first, then their parent.
        {"cid": "5", "pid": "99", "channel_name": "孤儿甲"},
        {"cid": "6", "pid": "98", "channel_name": "孤儿乙"},
        {"cid": "1", "pid": "0", "channel_name": "大厅"},
        {"cid": "2", "pid": "1", "channel_name": "子频道"},
        {"cid": "3", "pid": "2", "channel_name": "孙频道"},
    ]
    clients = [
        {"cid": "1", "clid": "10", "client_type": "0", "client_nickname": "Alice"},
        {"cid": "1", "clid": "11", "client_type": "1", "client_nickname": "QueryBot"},
        {"cid": "2", "clid": "12", "client_type": "0", "client_nickname": "Bob"},
        {"cid": "3", "clid": "13", "client_type": "0", "client_nickname": "Carol"},
        # cid 4 is not in channellist; the old code dropped it too.
        {"cid": "4", "clid": "14", "client_type": "0", "client_nickname": "Ghost"},
    ]
    return serverinfo, channels, clients


def expected_sample_text():
    return (
        "|- 测试服\n"
        "   |- 孤儿甲\n"
        "   |- 孤儿乙\n"
        "   |- 大厅\n"
        "      -> Alice\n"
        "      |- 子频道\n"
        "         -> Bob\n"
        "         |- 孙频道\n"
        "            -> Carol\n"
    )


class ChannelTreeTest(unittest.TestCase):
    def test_build_tree_one_use_and_zero_channelinfo(self):
        serverinfo, channels, clients = sample_tree_data()
        conn = FakeConnection(serverinfo, channels, clients)
        tree = TREE["tree"].ChannelTreeNode.build_tree(conn, 1)

        commands = [call[0] for call in conn.calls]
        self.assertEqual(
            commands,
            ["exec:use", "query:serverinfo", "query:channellist", "query:clientlist"],
        )
        self.assertEqual(commands.count("exec:use"), 1)
        self.assertNotIn("query:channelinfo", commands)
        self.assertNotIn("channelinfo", commands)
        self.assertEqual(conn.calls[0][1], {"sid": 1, "virtual": True})
        self.assertEqual(tree.print(), expected_sample_text())

    def test_from_lists_matches_previous_layout_and_orphan_order(self):
        serverinfo, channels, clients = sample_tree_data()
        text = TREE["tree"].format_tree(serverinfo, channels, clients)
        self.assertEqual(text, expected_sample_text())

        # Orphans are attached under the virtual root in insertion order, before
        # their real parent (the historical algorithm's behaviour).
        positions = [text.index(name) for name in ("孤儿甲", "孤儿乙", "大厅")]
        self.assertEqual(positions, sorted(positions))

    def test_deep_indentation_and_query_client_filter(self):
        serverinfo = {"virtualserver_name": "S"}
        channels = [
            {"cid": "1", "pid": "0", "channel_name": "A"},
            {"cid": "2", "pid": "1", "channel_name": "B"},
        ]
        clients = [
            {"cid": "1", "client_type": "1", "client_nickname": "Q"},
            {"cid": "2", "client_type": "0", "client_nickname": "N"},
        ]
        self.assertEqual(
            TREE["tree"].format_tree(serverinfo, channels, clients),
            "|- S\n"
            "   |- A\n"
            "      |- B\n"
            "         -> N\n",
        )

    def test_from_lists_does_not_mutate_rows(self):
        serverinfo, channels, clients = sample_tree_data()
        snapshot = json.loads(json.dumps([serverinfo, channels, clients]))
        TREE["tree"].format_tree(serverinfo, channels, clients)
        self.assertEqual([serverinfo, channels, clients], snapshot)


class PluginConfigTest(unittest.TestCase):
    def test_proxy_defaults_and_overrides(self):
        config = TREE["config"].Config(ts_ip="127.0.0.1", ts_pswd="pw")
        self.assertEqual(config.ts_proxy_url, "")
        self.assertEqual(config.ts_proxy_token, "")
        self.assertEqual(config.ts_proxy_timeout, 10.0)
        self.assertEqual(config.ts_delete_delay, 300)

        proxied = TREE["config"].Config(
            ts_proxy_url="http://100.101.30.74:18011",
            ts_proxy_token="token",
            ts_proxy_timeout=3.5,
        )
        self.assertEqual(proxied.ts_ip, "")
        self.assertEqual(proxied.ts_pswd, "")
        self.assertEqual(proxied.ts_proxy_timeout, 3.5)


class FakeResponse:
    def __init__(self, status, payload):
        self.status = status
        self.payload = payload

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def json(self, content_type=None):
        if isinstance(self.payload, Exception):
            raise self.payload
        return self.payload


class FakeSession:
    def __init__(self, response=None, error=None):
        self.response = response
        self.error = error
        self.requests = []

    def get(self, url, headers=None, timeout=None):
        self.requests.append(("GET", url, headers, timeout, None))
        if self.error is not None:
            raise self.error
        return self.response

    def post(self, url, headers=None, json=None, timeout=None):
        self.requests.append(("POST", url, headers, timeout, json))
        if self.error is not None:
            raise self.error
        return self.response


class ClientHelperTest(unittest.TestCase):
    def test_fetch_tree_uses_single_get_with_bearer(self):
        session = FakeSession(FakeResponse(200, {"ok": True, "text": "|- S\n"}))
        text = asyncio.run(TREE["client"].fetch_tree(
            session, "http://100.101.30.74:18011/", "tok-1", 3.5))
        self.assertEqual(text, "|- S\n")
        method, url, headers, timeout, payload = session.requests[0]
        self.assertEqual(method, "GET")
        self.assertEqual(url, "http://100.101.30.74:18011/tree")
        self.assertEqual(headers, {"Authorization": "Bearer tok-1"})
        self.assertEqual(timeout, 3.5)
        self.assertIsNone(payload)

    def test_poke_posts_json_and_returns_found(self):
        session = FakeSession(FakeResponse(200, {"ok": True, "found": True}))
        found = asyncio.run(TREE["client"].poke_client(
            session, "http://100.101.30.74:18011", "tok-2", "Alice",
            "123 Poked you!", 4.0))
        self.assertTrue(found)
        method, url, headers, timeout, payload = session.requests[0]
        self.assertEqual(method, "POST")
        self.assertEqual(url, "http://100.101.30.74:18011/poke")
        self.assertEqual(headers, {"Authorization": "Bearer tok-2"})
        self.assertEqual(timeout, 4.0)
        self.assertEqual(payload, {"nickname": "Alice", "msg": "123 Poked you!"})

    def test_not_found_is_false(self):
        session = FakeSession(FakeResponse(200, {"ok": True, "found": False}))
        self.assertFalse(asyncio.run(TREE["client"].poke_client(
            session, "http://x", "tok", "Nobody", "msg", 1.0)))

    def test_http_error_raises_without_token(self):
        session = FakeSession(FakeResponse(401, {"ok": False}))
        with self.assertRaises(TREE["client"].TsProxyError) as ctx:
            asyncio.run(TREE["client"].fetch_tree(
                session, "http://x", "super-secret-token", 1.0))
        message = str(ctx.exception)
        self.assertIn("401", message)
        self.assertNotIn("super-secret-token", message)

    def test_missing_token_fails_before_network(self):
        session = FakeSession(FakeResponse(200, {"ok": True, "text": "x"}))
        with self.assertRaises(TREE["client"].TsProxyError):
            asyncio.run(TREE["client"].fetch_tree(session, "http://x", "", 1.0))
        self.assertEqual(session.requests, [])

    def test_bad_payload_and_transport_error_are_wrapped(self):
        session = FakeSession(FakeResponse(200, {"ok": True}))
        with self.assertRaises(TREE["client"].TsProxyError):
            asyncio.run(TREE["client"].fetch_tree(session, "http://x", "tok", 1.0))

        broken = FakeSession(FakeResponse(200, {"ok": True, "found": "yes"}))
        with self.assertRaises(TREE["client"].TsProxyError):
            asyncio.run(TREE["client"].poke_client(
                broken, "http://x", "tok", "A", "m", 1.0))

        offline = FakeSession(error=ConnectionResetError("reset"))
        with self.assertRaises(TREE["client"].TsProxyError) as ctx:
            asyncio.run(TREE["client"].fetch_tree(
                offline, "http://x", "tok", 1.0))
        self.assertIn("ConnectionResetError", str(ctx.exception))


class FakeConfig:
    def __init__(self, **kwargs):
        self.ts_ip = "127.0.0.1"
        self.ts_pswd = "pw"
        self.ts_delete_delay = 0
        self.ts_proxy_url = ""
        self.ts_proxy_token = ""
        self.ts_proxy_timeout = 10.0
        self.__dict__.update(kwargs)


class PluginServiceTest(unittest.TestCase):
    def setUp(self):
        self.service = TREE["service"]
        self._orig = {
            "client_fetch": self.service.client.fetch_tree,
            "client_poke": self.service.client.poke_client,
            "direct_fetch": self.service.direct_query_tree_text,
            "direct_poke": self.service.direct_poke_client,
        }
        self.service._session = None

    def tearDown(self):
        self.service.client.fetch_tree = self._orig["client_fetch"]
        self.service.client.poke_client = self._orig["client_poke"]
        self.service.direct_query_tree_text = self._orig["direct_fetch"]
        self.service.direct_poke_client = self._orig["direct_poke"]
        self.service._session = None

    def test_proxy_branch_uses_single_http_call(self):
        observed = {}

        async def fake_fetch(session, base_url, token, timeout):
            observed.update(base_url=base_url, token=token, timeout=timeout)
            return "|- proxy\n"

        self.service.client.fetch_tree = fake_fetch
        self.service.direct_query_tree_text = mock.Mock(
            side_effect=AssertionError("direct must not be used"))
        config = FakeConfig(
            ts_proxy_url="http://100.101.30.74:18011",
            ts_proxy_token="tok", ts_proxy_timeout=2.5)

        async def scenario():
            try:
                return await self.service.fetch_tree(config)
            finally:
                await self.service.close_session()

        self.assertEqual(asyncio.run(scenario()), "|- proxy\n")
        self.assertEqual(observed["base_url"], "http://100.101.30.74:18011")
        self.assertEqual(observed["token"], "tok")
        self.assertEqual(observed["timeout"], 2.5)

    def test_proxy_branch_poke_returns_found(self):
        observed = {}

        async def fake_poke(session, base_url, token, nickname, msg, timeout):
            observed.update(
                base_url=base_url, token=token, nickname=nickname,
                msg=msg, timeout=timeout)
            return True

        self.service.client.poke_client = fake_poke
        self.service.direct_poke_client = mock.Mock(
            side_effect=AssertionError("direct must not be used"))
        config = FakeConfig(
            ts_proxy_url="http://proxy", ts_proxy_token="tok",
            ts_proxy_timeout=3.0)

        async def scenario():
            try:
                return await self.service.poke(config, "Alice", "9 Poked you!")
            finally:
                await self.service.close_session()

        self.assertTrue(asyncio.run(scenario()))
        self.assertEqual(observed["nickname"], "Alice")
        self.assertEqual(observed["msg"], "9 Poked you!")
        self.assertEqual(observed["timeout"], 3.0)

    def test_fallback_runs_direct_query_in_worker_thread(self):
        observed = {}

        def fake_direct(host, password, sid=1, **kwargs):
            observed.update(
                host=host, password=password, sid=sid,
                thread=threading.current_thread(), kwargs=kwargs)
            return "|- direct\n"

        self.service.direct_query_tree_text = fake_direct
        self.service.client.fetch_tree = mock.AsyncMock(
            side_effect=AssertionError("proxy must not be used"))
        config = FakeConfig(ts_proxy_url="", ts_ip="100.101.30.74",
                            ts_pswd="pw")

        self.assertEqual(
            asyncio.run(self.service.fetch_tree(config)), "|- direct\n")
        self.assertIsNot(observed["thread"], threading.current_thread())
        self.assertEqual(observed["host"], "100.101.30.74")
        self.assertEqual(observed["password"], "pw")
        self.assertEqual(observed["sid"], 1)
        self.assertEqual(observed["kwargs"], {"timeout": 5.0})

    def test_fallback_poke_runs_direct_in_worker_thread(self):
        observed = {}

        def fake_direct(host, password, nickname, msg, sid=1, **kwargs):
            observed.update(
                nickname=nickname, msg=msg,
                thread=threading.current_thread())
            return False

        self.service.direct_poke_client = fake_direct
        self.service.client.poke_client = mock.AsyncMock(
            side_effect=AssertionError("proxy must not be used"))
        config = FakeConfig(ts_proxy_url="", ts_ip="127.0.0.1", ts_pswd="pw")

        self.assertFalse(
            asyncio.run(self.service.poke(config, "Bob", "1 Poked you!")))
        self.assertIsNot(observed["thread"], threading.current_thread())
        self.assertEqual(observed["nickname"], "Bob")

    def test_direct_mode_without_credentials_is_refused(self):
        self.service.direct_query_tree_text = mock.Mock(
            side_effect=AssertionError("must not connect"))
        config = FakeConfig(ts_proxy_url="", ts_ip="", ts_pswd="")
        with self.assertRaises(self.service.TsQueryError) as ctx:
            asyncio.run(self.service.fetch_tree(config))
        self.assertIn("not configured", str(ctx.exception))

    def test_direct_failure_is_redacted(self):
        def explode(*args, **kwargs):
            raise ValueError("hunter2 password")

        self.service.direct_query_tree_text = explode
        config = FakeConfig(ts_proxy_url="", ts_ip="127.0.0.1", ts_pswd="pw")
        with self.assertRaises(self.service.TsQueryError) as ctx:
            asyncio.run(self.service.fetch_tree(config))
        message = str(ctx.exception)
        self.assertIn("ValueError", message)
        self.assertNotIn("hunter2", message)


class DirectFallbackTest(unittest.TestCase):
    def setUp(self):
        self.direct = TREE["direct"]
        self._orig = self.direct.ts3.query.TS3ServerConnection

    def tearDown(self):
        self.direct.ts3.query.TS3ServerConnection = self._orig

    def _patch_connection(self, conn):
        self.direct.ts3.query.TS3ServerConnection = lambda: conn

    def test_query_tree_text_connects_logs_in_once_and_closes(self):
        serverinfo, channels, clients = sample_tree_data()
        conn = FakeConnection(serverinfo, channels, clients)
        self._patch_connection(conn)

        text = self.direct.query_tree_text(
            "100.101.30.74", "pw", sid=1, port=10011,
            user="serveradmin", timeout=2.0)
        self.assertEqual(text, expected_sample_text())
        self.assertEqual(conn.open_args, {
            "host": "100.101.30.74",
            "port": 10011,
            "timeout": 2.0,
            "tp_args": {"username": "serveradmin", "password": "pw"},
        })
        commands = [call[0] for call in conn.calls]
        self.assertEqual(commands.count("exec:use"), 1)
        self.assertNotIn("query:channelinfo", commands)
        self.assertTrue(conn.closed)

    def test_poke_client_matches_and_closes(self):
        serverinfo, channels, clients = sample_tree_data()
        conn = FakeConnection(serverinfo, channels, clients)
        self._patch_connection(conn)

        self.assertTrue(self.direct.poke_client(
            "127.0.0.1", "pw", "Alice", "42 Poked you!"))
        self.assertIn(
            ("exec:clientpoke", {"clid": "10", "msg": "42 Poked you!"}),
            conn.calls)
        self.assertTrue(conn.closed)

        conn2 = FakeConnection(serverinfo, channels, clients)
        self._patch_connection(conn2)
        self.assertFalse(self.direct.poke_client(
            "127.0.0.1", "pw", "Nobody", "x"))
        self.assertNotIn(
            "exec:clientpoke", [call[0] for call in conn2.calls])

    def test_direct_connect_refuses_missing_password(self):
        self.direct.ts3.query.TS3ServerConnection = mock.Mock(
            side_effect=AssertionError("must not connect"))
        with self.assertRaises(RuntimeError):
            self.direct.query_tree_text("127.0.0.1", "")


TOKEN = "unit-test-token-9f3a"


class ProxyServerTest(unittest.TestCase):
    def setUp(self):
        self._orig_ts_connection = PROXY._ts_connection
        self._orig_tree = PROXY.query_tree_text
        self._orig_poke = PROXY.poke_client
        self.tree_text = "|- 测试服\n   |- 大厅\n      -> Alice\n"
        self.poke_calls = []
        self.poke_result = True

        def fake_tree(settings):
            return self.tree_text

        def fake_poke(settings, nickname, msg):
            self.poke_calls.append((nickname, msg))
            return self.poke_result

        PROXY.query_tree_text = fake_tree
        PROXY.poke_client = fake_poke
        self.settings = PROXY.Settings(
            bind_host="127.0.0.1",
            bind_port=0,
            token=TOKEN,
            ts_host="127.0.0.1",
            ts_port=10011,
            ts_user="serveradmin",
            ts_pswd="ts-password",
            ts_sid=1,
            ts_timeout=1.0,
        )
        self.server = PROXY.build_server(self.settings)
        self.port = self.server.server_address[1]
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)
        PROXY._ts_connection = self._orig_ts_connection
        PROXY.query_tree_text = self._orig_tree
        PROXY.poke_client = self._orig_poke

    def _request(self, method, path, token=None, payload=None, raw_body=None):
        headers = {}
        body = None
        if raw_body is not None:
            body = raw_body
            headers["Content-Type"] = "application/json"
        elif payload is not None:
            body = json.dumps(payload).encode("utf-8")
            headers["Content-Type"] = "application/json"
        if token is not None:
            headers["Authorization"] = f"Bearer {token}"
        connection = http.client.HTTPConnection(
            "127.0.0.1", self.port, timeout=5)
        try:
            connection.request(method, path, body=body, headers=headers)
            response = connection.getresponse()
            raw = response.read()
        finally:
            connection.close()
        parsed = json.loads(raw) if raw else None
        return response.status, parsed, raw

    def test_health_is_public(self):
        status, payload, _ = self._request("GET", "/health")
        self.assertEqual(status, 200)
        self.assertEqual(payload, {"ok": True})

    def test_tree_requires_bearer(self):
        status, payload, _ = self._request("GET", "/tree")
        self.assertEqual(status, 401)
        self.assertFalse(payload["ok"])

        status, _, _ = self._request("GET", "/tree", token="wrong")
        self.assertEqual(status, 401)

        status, payload, raw = self._request("GET", "/tree", token=TOKEN)
        self.assertEqual(status, 200)
        self.assertEqual(payload, {"ok": True, "text": self.tree_text})
        self.assertNotIn(TOKEN.encode(), raw)

    def test_poke_found_and_not_found(self):
        status, payload, _ = self._request(
            "POST", "/poke", token=TOKEN,
            payload={"nickname": " Alice ", "msg": "42 Poked you!"})
        self.assertEqual(status, 200)
        self.assertEqual(payload, {"ok": True, "found": True})
        self.assertEqual(self.poke_calls, [("Alice", "42 Poked you!")])

        self.poke_result = False
        status, payload, _ = self._request(
            "POST", "/poke", token=TOKEN,
            payload={"nickname": "Nobody", "msg": "42 Poked you!"})
        self.assertEqual(status, 200)
        self.assertEqual(payload, {"ok": True, "found": False})

    def test_poke_requires_bearer(self):
        status, _, _ = self._request(
            "POST", "/poke", payload={"nickname": "Alice", "msg": "hi"})
        self.assertEqual(status, 401)
        self.assertEqual(self.poke_calls, [])

    def test_poke_rejects_missing_parameters(self):
        for payload in ({}, {"nickname": "Alice"}, {"msg": "hi"},
                        {"nickname": "  ", "msg": "hi"},
                        {"nickname": "Alice", "msg": ""}):
            status, _, _ = self._request(
                "POST", "/poke", token=TOKEN, payload=payload)
            self.assertEqual(status, 400, payload)

        status, _, _ = self._request(
            "POST", "/poke", token=TOKEN, raw_body=b"not-json")
        self.assertEqual(status, 400)
        self.assertEqual(self.poke_calls, [])

    def test_ts_errors_are_502_and_do_not_leak_secrets(self):
        def explode(settings):
            raise RuntimeError("login failed for ts-password")

        PROXY.query_tree_text = explode
        status, payload, raw = self._request("GET", "/tree", token=TOKEN)
        self.assertEqual(status, 502)
        self.assertEqual(payload, {"ok": False, "error": "teamspeak query failed"})
        self.assertNotIn(b"ts-password", raw)

        def poke_explode(settings, nickname, msg):
            raise RuntimeError("login failed for ts-password")

        PROXY.poke_client = poke_explode
        status, payload, raw = self._request(
            "POST", "/poke", token=TOKEN,
            payload={"nickname": "Alice", "msg": "hi"})
        self.assertEqual(status, 502)
        self.assertNotIn(b"ts-password", raw)

    def test_unknown_paths_and_methods(self):
        status, _, _ = self._request("GET", "/nope")
        self.assertEqual(status, 404)

        status, _, _ = self._request("GET", "/poke", token=TOKEN)
        self.assertEqual(status, 405)

        status, _, _ = self._request(
            "POST", "/tree", token=TOKEN, payload={})
        self.assertEqual(status, 405)

    def test_aiohttp_client_end_to_end_against_local_server(self):
        """Uses the real aiohttp client stack (loopback only) for both endpoints."""

        async def scenario():
            session = aiohttp.ClientSession()
            try:
                text = await TREE["client"].fetch_tree(
                    session, f"http://127.0.0.1:{self.port}", TOKEN, 5.0)
                found = await TREE["client"].poke_client(
                    session, f"http://127.0.0.1:{self.port}", TOKEN,
                    "Alice", "42 Poked you!", 5.0)
                return text, found
            finally:
                await session.close()

        text, found = asyncio.run(scenario())
        self.assertEqual(text, self.tree_text)
        self.assertTrue(found)
        self.assertEqual(self.poke_calls, [("Alice", "42 Poked you!")])

    def test_proxy_tree_query_uses_shared_builder(self):
        serverinfo, channels, clients = sample_tree_data()
        conn = FakeConnection(serverinfo, channels, clients)
        PROXY._ts_connection = lambda settings: conn
        PROXY.query_tree_text = self._orig_tree
        text = PROXY.query_tree_text(self.settings)
        self.assertEqual(text, expected_sample_text())
        commands = [call[0] for call in conn.calls]
        self.assertEqual(commands.count("exec:use"), 1)
        self.assertNotIn("query:channelinfo", commands)

    def test_proxy_poke_query_calls_clientpoke_only_on_match(self):
        serverinfo, channels, clients = sample_tree_data()
        PROXY.poke_client = self._orig_poke

        conn = FakeConnection(serverinfo, channels, clients)
        PROXY._ts_connection = lambda settings: conn
        self.assertTrue(PROXY.poke_client(
            self.settings, "Alice", "42 Poked you!"))
        self.assertIn(
            ("exec:clientpoke", {"clid": "10", "msg": "42 Poked you!"}),
            conn.calls)
        self.assertIn(("exec:use", {"sid": 1}), conn.calls)

        conn2 = FakeConnection(serverinfo, channels, clients)
        PROXY._ts_connection = lambda settings: conn2
        self.assertFalse(PROXY.poke_client(self.settings, "Nobody", "x"))
        self.assertNotIn(
            "exec:clientpoke", [call[0] for call in conn2.calls])


class ProxySettingsTest(unittest.TestCase):
    def test_defaults_and_env_parsing(self):
        settings = PROXY.load_settings({"TS_PROXY_TOKEN": "tok"})
        self.assertEqual(settings.bind_host, "127.0.0.1")
        self.assertEqual(settings.bind_port, 18011)
        self.assertEqual(settings.ts_host, "127.0.0.1")
        self.assertEqual(settings.ts_port, 10011)
        self.assertEqual(settings.ts_user, "serveradmin")
        self.assertEqual(settings.ts_sid, 1)
        self.assertEqual(settings.ts_timeout, 5.0)

        custom = PROXY.load_settings({
            "TS_PROXY_TOKEN": "tok",
            "TS_PROXY_BIND_HOST": "100.101.30.74",
            "TS_PROXY_PORT": "18011",
            "TS_IP": "127.0.0.1",
            "TS_PORT": "10011",
            "TS_USER": "serveradmin",
            "TS_PSWD": "pw",
            "TS_SID": "2",
            "TS_TIMEOUT": "2.5",
        })
        self.assertEqual(custom.bind_host, "100.101.30.74")
        self.assertEqual(custom.ts_sid, 2)
        self.assertEqual(custom.ts_timeout, 2.5)

    def test_token_is_required_fail_closed(self):
        with self.assertRaises(PROXY.ConfigError):
            PROXY.load_settings({})
        with self.assertRaises(PROXY.ConfigError):
            PROXY.load_settings({"TS_PROXY_TOKEN": "   "})
        with self.assertRaises(PROXY.ConfigError):
            PROXY.load_settings({"TS_PROXY_TOKEN_FILE": ""})

    def test_token_file_and_empty_file(self):
        with tempfile.TemporaryDirectory() as directory:
            token_path = Path(directory) / "token"
            token_path.write_text("file-token\n", encoding="utf-8")
            settings = PROXY.load_settings({
                "TS_PROXY_TOKEN_FILE": str(token_path)})
            self.assertEqual(settings.token, "file-token")

            empty_path = Path(directory) / "empty"
            empty_path.write_text("\n", encoding="utf-8")
            with self.assertRaises(PROXY.ConfigError):
                PROXY.load_settings({"TS_PROXY_TOKEN_FILE": str(empty_path)})

            with self.assertRaises(PROXY.ConfigError):
                PROXY.load_settings({
                    "TS_PROXY_TOKEN_FILE": str(Path(directory) / "missing")})

    def test_wildcard_bind_is_rejected(self):
        for host in ("0.0.0.0", "::", "[::]", "*", ""):
            with self.assertRaises(PROXY.ConfigError, msg=host):
                PROXY.load_settings({
                    "TS_PROXY_TOKEN": "tok",
                    "TS_PROXY_BIND_HOST": host,
                })

    def test_env_file_does_not_override_existing_values(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "ts-proxy.env"
            path.write_text(
                "# comment\n"
                "export TS_PSWD='file-pass'\n"
                "TS_PROXY_PORT=19999\n"
                'TS_USER="admin"\n'
                "TS_PROXY_TOKEN=file-token\n",
                encoding="utf-8",
            )
            environ = {"TS_PROXY_TOKEN": "env-token", "TS_PSWD": "env-pass"}
            PROXY.load_env_file(path, environ)
            self.assertEqual(environ["TS_PROXY_TOKEN"], "env-token")
            self.assertEqual(environ["TS_PSWD"], "env-pass")
            self.assertEqual(environ["TS_PROXY_PORT"], "19999")
            self.assertEqual(environ["TS_USER"], "admin")

            settings = PROXY.load_settings(environ)
            self.assertEqual(settings.bind_port, 19999)
            self.assertEqual(settings.token, "env-token")

    def test_main_refuses_to_start_without_token(self):
        with mock.patch.dict(os.environ, {}, clear=True):
            self.assertEqual(PROXY.main([]), 2)


if __name__ == "__main__":
    unittest.main()
