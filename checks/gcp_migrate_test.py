#!/usr/bin/env python3
"""Offline checks for the cross-host GCP migration (module B).

No network, no database, no QQ message, no service restart. Everything here is
standard library only, so the checks run on a bare Windows Python as well as on
the Linux host.
"""
from __future__ import annotations

import ast
from contextlib import contextmanager
import hashlib
import importlib.util
import json
import shutil
import subprocess
import math
import os
from pathlib import Path
import sys
import tempfile
import time
import types
import unittest
from unittest.mock import Mock, patch

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))


@contextmanager
def env(**values):
    """Set/clear environment variables and restore them afterwards."""
    previous = {key: os.environ.get(key) for key in values}
    try:
        for key, value in values.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
        yield
    finally:
        for key, value in previous.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


def load_module(name, path, *, register=True):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    if register:
        sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


class _FakeHttpResponse:
    def __init__(self, body: bytes = b"", status: int = 200):
        self._body = body
        self.status = status

    def read(self, _limit: int = -1) -> bytes:
        return self._body

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


# --------------------------------------------------------------- outgoing.py
def _install_nonebot_stubs():
    if "nonebot" in sys.modules:
        return
    nonebot = types.ModuleType("nonebot")
    nonebot.get_driver = lambda: types.SimpleNamespace(on_bot_connect=lambda fn: fn)
    nonebot.logger = types.SimpleNamespace(exception=lambda *a, **k: None)
    adapters = types.ModuleType("nonebot.adapters")
    onebot = types.ModuleType("nonebot.adapters.onebot")
    v11 = types.ModuleType("nonebot.adapters.onebot.v11")
    exception = types.ModuleType("nonebot.adapters.onebot.v11.exception")

    class MessageSegment:
        def __init__(self, segment_type="text", data=None):
            self.type = segment_type
            self.data = data or {}

    class Message(list):
        pass

    class Bot:
        @staticmethod
        async def call_api(*args, **kwargs):
            raise AssertionError("the offline checks never touch the network")

    class ActionFailed(Exception):
        pass

    v11.Bot = Bot
    v11.Message = Message
    v11.MessageSegment = MessageSegment
    exception.ActionFailed = ActionFailed
    nonebot.adapters = adapters
    adapters.onebot = onebot
    onebot.v11 = v11
    v11.exception = exception
    store = types.ModuleType("ai_runtime.store")
    store.state_store = lambda: None
    sys.modules.update({
        "nonebot": nonebot,
        "nonebot.adapters": adapters,
        "nonebot.adapters.onebot": onebot,
        "nonebot.adapters.onebot.v11": v11,
        "nonebot.adapters.onebot.v11.exception": exception,
        "ai_runtime.store": store,
    })


def load_outgoing():
    if "csbot_test_outgoing" in sys.modules:
        return sys.modules["csbot_test_outgoing"]
    _install_nonebot_stubs()
    return load_module("csbot_test_outgoing", REPO / "plugins/allmsg/outgoing.py")


def resource_marker(library="pic", filename="cat.png"):
    source = {"library": library, "filename": filename, "sha256": "a" * 64}
    item = {
        "schema": "csbot-image-v1",
        "mode": "resource",
        "title": "图库图片",
        "captured_at": "2026-01-01T00:00:00+00:00",
        "source": source,
    }
    item["image_id"] = hashlib.sha256(
        json.dumps(item, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    return item


def snapshot_marker():
    item = {
        "schema": "csbot-image-v1",
        "mode": "snapshot",
        "title": "快照",
        "captured_at": "2026-01-01T00:00:00+00:00",
        "source": {},
        "snapshot": {"score": 1},
    }
    item["image_id"] = hashlib.sha256(
        json.dumps(item, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    return item


class OutgoingWireTests(unittest.TestCase):
    def setUp(self):
        self.outgoing = load_outgoing()

    def test_unset_local_root_keeps_original_payload(self):
        marker = resource_marker()
        payload = [
            {"type": "text", "data": {"text": "hi"}},
            {"type": "image", "data": {"file": "base64://QUJD", "sub_type": 0,
                                       self.outgoing.MARKER: marker}},
            {"type": "node", "data": {"name": "n", "content": [
                {"type": "image", "data": {"file": "http://example/a.png",
                                           self.outgoing.MARKER: marker}},
            ]}},
        ]
        with env(CS_NAPCAT_LOCAL_IMGS_ROOT=None):
            result = self.outgoing.wire_message(payload)
        self.assertEqual(result[0], {"type": "text", "data": {"text": "hi"}})
        self.assertEqual(result[1], {"type": "image", "data": {"file": "base64://QUJD", "sub_type": 0}})
        self.assertEqual(
            result[2]["data"]["content"],
            [{"type": "image", "data": {"file": "http://example/a.png"}}],
        )
        self.assertEqual(self.outgoing.wire_message("plain"), "plain")

    def test_local_root_rewrites_pic_and_mgz_to_file_url(self):
        payload = [
            {"type": "image", "data": {"file": "base64://QUJD",
                                       self.outgoing.MARKER: resource_marker("pic", "cat.png")}},
            {"type": "image", "data": {"file": "base64://REVG",
                                       self.outgoing.MARKER: resource_marker("mgz", "map.webp")}},
        ]
        with env(CS_NAPCAT_LOCAL_IMGS_ROOT=" /app/imgs/ "):
            result = self.outgoing.wire_message(payload)
        self.assertEqual(result[0]["data"]["file"], "file:///app/imgs/pic/cat.png")
        self.assertEqual(result[1]["data"]["file"], "file:///app/imgs/mgz/map.webp")
        for item in result:
            self.assertNotIn(self.outgoing.MARKER, item["data"])
            self.assertEqual(item["type"], "image")

    def test_path_traversal_and_unknown_library_are_rejected(self):
        for marker in (
            resource_marker("pic", "../secret.png"),
            resource_marker("pic", "/etc/passwd"),
            resource_marker("avatars", "cat.png"),
        ):
            payload = [{"type": "image", "data": {"file": "base64://ORIGINAL",
                                                  self.outgoing.MARKER: marker}}]
            with env(CS_NAPCAT_LOCAL_IMGS_ROOT="/app/imgs"):
                result = self.outgoing.wire_message(payload)
            self.assertEqual(result[0]["data"]["file"], "base64://ORIGINAL")
            self.assertNotIn(self.outgoing.MARKER, result[0]["data"])
            self.assertNotIn("file://", json.dumps(result))
        # The durable outbox still refuses an invalid descriptor outright.
        with self.assertRaises(ValueError):
            self.outgoing.freeze_message([
                {"type": "image", "data": {"file": "base64://ORIGINAL",
                                           self.outgoing.MARKER: resource_marker("pic", "../x.png")}},
            ])

    def test_snapshot_markers_are_never_rewritten(self):
        payload = [{"type": "image", "data": {"file": "base64://SNAP",
                                              self.outgoing.MARKER: snapshot_marker()}}]
        with env(CS_NAPCAT_LOCAL_IMGS_ROOT="/app/imgs"):
            result = self.outgoing.wire_message(payload)
        self.assertEqual(result[0]["data"], {"file": "base64://SNAP"})


# --------------------------------------------------------------- _lookup_timeout
ALLMSG_INIT = REPO / "plugins/allmsg/__init__.py"


def load_lookup_timeout():
    raw = ALLMSG_INIT.read_bytes()
    assert b"\r\n" not in raw, "plugins/allmsg/__init__.py must stay LF for the manifest"
    tree = ast.parse(raw.decode("utf-8"))
    function = next(
        node for node in tree.body
        if isinstance(node, ast.FunctionDef) and node.name == "_lookup_timeout"
    )
    module = ast.Module(body=[function], type_ignores=[])
    ast.fix_missing_locations(module)
    namespace = {"os": os, "math": math}
    exec(compile(module, str(ALLMSG_INIT), "exec"), namespace)
    return namespace["_lookup_timeout"]


class OneBotLookupTimeoutTests(unittest.TestCase):
    def setUp(self):
        self.timeout = load_lookup_timeout()

    def test_default_and_valid_values(self):
        with env(CS_ONEBOT_LOOKUP_TIMEOUT=None):
            self.assertEqual(self.timeout(), 4.0)
        with env(CS_ONEBOT_LOOKUP_TIMEOUT="2.5"):
            self.assertEqual(self.timeout(), 2.5)
        with env(CS_ONEBOT_LOOKUP_TIMEOUT=" 6 "):
            self.assertEqual(self.timeout(), 6.0)

    def test_clamping_and_invalid_fallbacks(self):
        cases = {
            "0.1": 1.0,
            "-5": 1.0,
            "60": 10.0,
            "inf": 10.0,
            "abc": 4.0,
            "": 4.0,
            "nan": 4.0,
            "--2": 4.0,
        }
        for raw, expected in cases.items():
            with env(CS_ONEBOT_LOOKUP_TIMEOUT=raw):
                self.assertEqual(self.timeout(), expected, raw)

    def test_call_site_uses_the_helper_and_file_stays_lf(self):
        source = ALLMSG_INIT.read_text(encoding="utf-8")
        self.assertIn("timeout=_lookup_timeout()", source)
        self.assertNotIn("timeout=2)", source)
        raw = ALLMSG_INIT.read_bytes()
        self.assertNotIn(b"\r\n", raw)


# --------------------------------------------------------------- openai proxy
def load_openai_support():
    if "httpx" not in sys.modules:
        httpx = types.ModuleType("httpx")

        class _HTTPError(Exception):
            pass

        httpx.ConnectError = _HTTPError
        httpx.ConnectTimeout = _HTTPError
        httpx.RemoteProtocolError = _HTTPError
        httpx.ReadError = _HTTPError
        httpx.Response = object
        httpx.Timeout = object
        httpx.AsyncClient = object
        sys.modules["httpx"] = httpx
    if "PIL" not in sys.modules:
        pil = types.ModuleType("PIL")

        class _DecompressionBombError(Exception):
            pass

        pil.Image = types.SimpleNamespace(DecompressionBombError=_DecompressionBombError)
        pil.UnidentifiedImageError = type("UnidentifiedImageError", (Exception,), {})
        sys.modules["PIL"] = pil
    if "ai_runtime.openai_support" in sys.modules:
        return sys.modules["ai_runtime.openai_support"]
    return load_module("ai_runtime.openai_support", REPO / "ai_runtime/openai_support.py")


class OpenAIProxyTests(unittest.TestCase):
    def setUp(self):
        self.support = load_openai_support()

    def test_default_proxy_is_preserved(self):
        with env(CS_AI_OPENAI_PROXY_URL=None, CS_AI_PROVIDER=None,
                 CS_AI_FORCE_LOOPBACK_LLM_PROXY=None):
            self.assertEqual(
                self.support.resolve_openai_proxy("https://api.openai.com/v1"),
                "http://127.0.0.1:7890",
            )

    def test_direct_markers_disable_the_proxy(self):
        for value in ("", "none", "direct", "off", "false", " DIRECT ", "NONE"):
            with env(CS_AI_OPENAI_PROXY_URL=value, CS_AI_PROVIDER=None,
                     CS_AI_FORCE_LOOPBACK_LLM_PROXY=None):
                self.assertEqual(
                    self.support.resolve_openai_proxy("https://api.openai.com/v1"), "", value
                )
        with env(CS_AI_OPENAI_PROXY_URL="http://127.0.0.1:7890", CS_AI_PROVIDER=None):
            self.assertEqual(
                self.support.resolve_openai_proxy("https://api.openai.com/v1", "direct"), ""
            )

    def test_other_endpoints_and_invalid_proxies(self):
        with env(CS_AI_OPENAI_PROXY_URL="http://127.0.0.1:7890", CS_AI_PROVIDER=None):
            self.assertEqual(
                self.support.resolve_openai_proxy("https://api.deepseek.com/v1"), ""
            )
            self.assertEqual(
                self.support.resolve_openai_proxy(
                    "https://api.openai.com/v1", "http://127.0.0.1:7891"
                ),
                "http://127.0.0.1:7891",
            )
            with self.assertRaises(ValueError):
                self.support.resolve_openai_proxy(
                    "https://api.openai.com/v1", "http://proxy.example:7890"
                )

    def test_loopback_endpoint_skips_the_proxy(self):
        with env(CS_AI_OPENAI_PROXY_URL="http://127.0.0.1:7890", CS_AI_PROVIDER="openai",
                 CS_AI_FORCE_LOOPBACK_LLM_PROXY=None):
            self.assertEqual(
                self.support.resolve_openai_proxy("http://127.0.0.1:9999/v1"), ""
            )


# --------------------------------------------------------------- push proxy
def load_push_proxy():
    if "csbot_matrix_push_proxy" in sys.modules:
        return sys.modules["csbot_matrix_push_proxy"]
    return load_module("csbot_matrix_push_proxy", REPO / "scripts/matrix_push_proxy.py")


class MatrixPushProxyTests(unittest.TestCase):
    def setUp(self):
        self.proxy = load_push_proxy()

    def test_default_allowlist(self):
        patterns = self.proxy.DEFAULT_ALLOW_DOMAINS
        for host in ("matrix.org", "push.matrix.org", "sygnal.matrix.org",
                     "api.push.apple.com", "fcm.googleapis.com",
                     "firebaseinstallations.googleapis.com"):
            self.assertTrue(self.proxy.domain_allowed(host, patterns), host)
        for host in ("evil.com", "matrix.org.evil.com", "notmatrix.org",
                     "apple.com", "fcm.googleapis.com.evil.net", ""):
            self.assertFalse(self.proxy.domain_allowed(host, patterns), host)
        self.assertTrue(self.proxy.domain_allowed("a.b.push.apple.com", ("*.push.apple.com",)))
        self.assertFalse(self.proxy.domain_allowed("push.apple.com", ("*.push.apple.com",)))

    def test_connect_target_parsing(self):
        self.assertEqual(self.proxy.parse_connect_target("matrix.org:443"), ("matrix.org", 443))
        self.assertEqual(self.proxy.parse_connect_target("MATRIX.ORG.:443"), ("matrix.org", 443))
        self.assertEqual(self.proxy.parse_connect_target("[::1]:443"), ("::1", 443))
        for bad in ("", "matrix.org", "matrix.org:", "matrix.org:abc",
                    "matrix.org:0", "matrix.org:70000", "a:b:443", "matrix.org/443"):
            with self.assertRaises(ValueError, msg=bad):
                self.proxy.parse_connect_target(bad)

    def test_forbidden_ip_ranges(self):
        for address in ("127.0.0.1", "127.1.2.3", "10.1.2.3", "172.16.0.1",
                        "192.168.1.1", "169.254.169.254", "100.64.0.1",
                        "100.127.255.254", "::1", "fc00::1", "fd00::1", "fe80::1",
                        "::ffff:127.0.0.1", "not-an-ip", ""):
            self.assertTrue(self.proxy.is_forbidden_ip(address), address)
        for address in ("93.184.216.34", "1.1.1.1", "2606:4700:4700::1111"):
            self.assertFalse(self.proxy.is_forbidden_ip(address), address)

    def test_connect_decision_status_codes(self):
        public = [(2, 1, 6, "", ("93.184.216.34", 443))]
        resolve_public = lambda host, port, type=None: public  # noqa: E731
        resolve_loopback = lambda host, port, type=None: [(2, 1, 6, "", ("127.0.0.1", 443))]  # noqa: E731
        resolve_metadata = lambda host, port, type=None: [(2, 1, 6, "", ("169.254.169.254", 443))]  # noqa: E731
        resolve_empty = lambda host, port, type=None: []  # noqa: E731
        self.assertEqual(self.proxy.handle_connect_target("matrix.org:443", resolver=resolve_public), 200)
        self.assertEqual(self.proxy.handle_connect_target("evil.com:443", resolver=resolve_public), 403)
        self.assertEqual(self.proxy.handle_connect_target("matrix.org:80", resolver=resolve_public), 403)
        self.assertEqual(self.proxy.handle_connect_target("matrix.org", resolver=resolve_public), 400)
        self.assertEqual(self.proxy.handle_connect_target("matrix.org:443", resolver=resolve_loopback), 403)
        self.assertEqual(self.proxy.handle_connect_target("matrix.org:443", resolver=resolve_metadata), 403)
        self.assertEqual(self.proxy.handle_connect_target("matrix.org:443", resolver=resolve_empty), 502)
        # A whitelisted host that resolves to a Tailnet/CGNAT address is refused too.
        resolve_tailnet = lambda host, port, type=None: [(2, 1, 6, "", ("100.64.0.9", 443))]  # noqa: E731
        self.assertEqual(self.proxy.handle_connect_target("sygnal.matrix.org:443", resolver=resolve_tailnet), 403)

    def test_client_cidr_allowlist(self):
        self.assertTrue(self.proxy.client_allowed("127.0.0.1"))
        self.assertTrue(self.proxy.client_allowed("100.64.1.2"))
        self.assertTrue(self.proxy.client_allowed("::1"))
        self.assertFalse(self.proxy.client_allowed("8.8.8.8"))
        self.assertFalse(self.proxy.client_allowed("192.168.1.10"))
        self.assertFalse(self.proxy.client_allowed("garbage"))

    def test_absolute_http_request_and_wildcard_bind_refusal(self):
        request = self.proxy.parse_proxy_request("get", "http://fcm.googleapis.com/x?y=1")
        self.assertEqual((request.method, request.host, request.port, request.path),
                         ("GET", "fcm.googleapis.com", 80, "/x?y=1"))
        with self.assertRaises(ValueError):
            self.proxy.parse_proxy_request("GET", "https://fcm.googleapis.com/x")
        with self.assertRaises(ValueError):
            self.proxy.parse_proxy_request("GET", "/relative")
        for wildcard in ("0.0.0.0", "::", "[::]", "*"):
            with self.assertRaises(SystemExit):
                with patch("sys.stderr"):
                    self.proxy.main(["--host", wildcard])


# --------------------------------------------------------------- health alert
def load_health_alert():
    if "csbot_health_alert" in sys.modules:
        return sys.modules["csbot_health_alert"]
    return load_module("csbot_health_alert", REPO / "scripts/health_alert.py")


def vnstat_dump(year, month, tx_by_interface):
    return json.dumps({
        "vnstatversion": "2.9",
        "jsonversion": "2",
        "interfaces": [
            {
                "name": name,
                "traffic": {
                    "months": [
                        {"id": 1, "date": {"year": year - 1, "month": 12},
                         "tx": 123456, "rx": 654321},
                        {"id": 2, "date": {"year": year, "month": month},
                         "tx": tx, "rx": 999999},
                    ]
                },
            }
            for name, tx in tx_by_interface.items()
        ],
    })


NOVEMBER_2026 = time.mktime((2026, 11, 15, 12, 0, 0, 0, 0, -1))


class HealthAlertProfileTests(unittest.TestCase):
    def setUp(self):
        self.health = load_health_alert()

    def test_profile_plan_matrix(self):
        full = self.health.profile_plan("full")
        self.assertEqual(full["probes"], (
            "host_resources", "systemd", "containers", "postgresql", "backend_http",
            "steam_api", "napcat_qq", "matrix_egress", "openai_egress", "logs", "outbox",
        ))
        self.assertEqual(full["services"], self.health.SERVICES)
        self.assertEqual(full["containers"], self.health.CONTAINERS)

        edge = self.health.profile_plan("EDGE")
        self.assertEqual(edge["probes"], (
            "host_resources", "systemd", "containers", "napcat_qq", "matrix_egress",
            "remote_backend_http",
        ))
        self.assertIn("tuwunel.service", edge["services"])
        self.assertIn("teamspeak_server", edge["containers"])
        self.assertNotIn("postgresql", edge["probes"])
        self.assertNotIn("vnstat_egress", edge["probes"])

        gcp = self.health.profile_plan("gcp")
        self.assertIn("vnstat_egress", gcp["probes"])
        self.assertIn("dsh_daemon", gcp["probes"])
        self.assertIn("csbot-dsh.service", gcp["services"])
        self.assertIn("steam_monitor", gcp["containers"])
        self.assertNotIn("matrix_egress", gcp["probes"])

        with self.assertRaises(ValueError):
            self.health.profile_plan("nope")

    def test_cli_profile_defaults(self):
        health = self.health
        with tempfile.TemporaryDirectory() as tmp:
            missing = Path(tmp) / "absent.json"
            cases = (
                ("full", "http://127.0.0.1:7890", ""),
                ("edge", "http://127.0.0.1:7890", ""),
                ("gcp", "", "http://127.0.0.1:32151"),
            )
            for profile, expected_openai, expected_dsh in cases:
                argv = ["health_alert.py", "--dry-run", "--profile", profile,
                        "--root", tmp, "--config", str(missing)]
                with patch.object(health, "run") as run_mock, \
                     patch.object(health, "emit"), patch.object(sys, "argv", argv):
                    code = health.main()
                self.assertEqual(code, 0)
                options = run_mock.call_args.kwargs["probe_options"]
                self.assertEqual(options["openai_proxy"], expected_openai, profile)
                self.assertEqual(options["dsh_daemon_url"], expected_dsh, profile)

            for value, expected in (("direct", ""), ("http://127.0.0.1:7891", "http://127.0.0.1:7891")):
                argv = ["health_alert.py", "--dry-run", "--profile", "gcp",
                        "--openai-proxy", value, "--root", tmp, "--config", str(missing)]
                with patch.object(health, "run") as run_mock, \
                     patch.object(health, "emit"), patch.object(sys, "argv", argv):
                    health.main()
                self.assertEqual(
                    run_mock.call_args.kwargs["probe_options"]["openai_proxy"], expected, value
                )

    def test_dsh_daemon_probe_refuses_non_loopback_and_hides_token(self):
        health = self.health
        with tempfile.TemporaryDirectory() as tmp:
            token = Path(tmp) / "token"
            token.write_text("SECRET-DAEMON-TOKEN", encoding="utf-8")
            for url in ("http://10.1.2.3:32151", "https://127.0.0.1:32151",
                        "http://127.0.0.1", "http://127.0.0.1:32151/path"):
                with self.assertRaises(ValueError, msg=url) as error:
                    health.dsh_daemon_ok(url, str(token))
                self.assertNotIn("SECRET-DAEMON-TOKEN", str(error.exception))

    def test_tailnet_homeserver_validation(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "config.json"

            def accepts(homeserver):
                path.write_text(json.dumps({
                    "room_id": "!room:example",
                    "access_token": "fake-token-for-tests",
                    "homeserver": homeserver,
                }), encoding="utf-8")
                try:
                    self.health.load_config(path)
                except ValueError:
                    return False
                return True

            self.assertTrue(accepts("http://100.64.0.9:6167"))
            self.assertTrue(accepts("http://100.127.255.254:6167"))
            self.assertTrue(accepts("http://127.0.0.1:6167"))
            self.assertTrue(accepts("https://matrix.example.org"))
            self.assertFalse(accepts("http://100.128.0.1:6167"))
            self.assertFalse(accepts("http://192.168.1.10:6167"))
            self.assertFalse(accepts("http://10.0.0.5:6167"))
            self.assertFalse(accepts("http://100.64.0.9.evil.com:6167"))
            self.assertFalse(accepts("http://100.64.0.9"))
            self.assertFalse(accepts("http://8.8.8.8:6167"))
            self.assertFalse(accepts("ftp://100.64.0.9:6167"))
            self.assertFalse(accepts("http://user:pass@100.64.0.9:6167"))

    def test_vnstat_monthly_tx_parsing(self):
        dump = vnstat_dump(2026, 11, {"ens4": 2_000_000, "lo": 5_000})
        self.assertEqual(
            self.health.parse_vnstat_monthly_tx(dump, now=NOVEMBER_2026), 2_005_000
        )
        self.assertEqual(
            self.health.parse_vnstat_monthly_tx(dump, now=NOVEMBER_2026, iface="ens4"),
            2_000_000,
        )
        with self.assertRaises(ValueError):
            self.health.parse_vnstat_monthly_tx(dump, now=NOVEMBER_2026, iface="eth9")
        with self.assertRaises(ValueError):
            self.health.parse_vnstat_monthly_tx("not json", now=NOVEMBER_2026)
        with self.assertRaises(ValueError):
            self.health.parse_vnstat_monthly_tx(json.dumps({"interfaces": []}), now=NOVEMBER_2026)
        # A month with no vnstat row yet is a zero, not a crash.
        empty_month = vnstat_dump(2025, 1, {"ens4": 7})
        self.assertEqual(
            self.health.parse_vnstat_monthly_tx(empty_month, now=NOVEMBER_2026), 0
        )
        # RX bytes never count towards the egress allowance.
        self.assertEqual(
            self.health.parse_vnstat_monthly_tx(
                vnstat_dump(2026, 11, {"ens4": 42}), now=NOVEMBER_2026, iface="ens4"
            ),
            42,
        )

    def test_egress_grading(self):
        GIB = 1024 ** 3
        self.assertEqual(self.health.egress_issues(17 * GIB), {})
        warning = self.health.egress_issues(18 * GIB)["egress"]
        self.assertEqual(warning.level, self.health.WARNING)
        self.assertEqual(self.health.egress_issues(int(23.9 * GIB))["egress"].level,
                         self.health.WARNING)
        critical = self.health.egress_issues(24 * GIB)["egress"]
        self.assertEqual((critical.level, critical.samples), (self.health.CRITICAL, 1))
        self.assertEqual(self.health.egress_issues(30 * GIB)["egress"].level,
                         self.health.CRITICAL)
        custom = self.health.egress_issues(2 * GIB, warn_gib=1.0, crit_gib=2.0)["egress"]
        self.assertEqual(custom.level, self.health.CRITICAL)

    def test_gcp_collect_wires_vnstat_and_dsh_probe(self):
        health = self.health
        calls = []
        tx = 25 * 1024 ** 3

        def fake_command(args, timeout=5):
            calls.append(list(args))
            if args[0] == "systemctl":
                return "\n\n".join(
                    f"Id={service}\nActiveState=active\nMemoryCurrent=104857600\nNRestarts=0"
                    for service in health.PROFILE_SERVICES["gcp"]
                )
            if args[:2] == ["docker", "inspect"]:
                return "\n".join(
                    f'"/{name}" true false healthy'
                    for name in health.PROFILE_CONTAINERS["gcp"]
                )
            if args[0] == "vnstat":
                return vnstat_dump(2026, 11, {"ens4": tx})
            return ""

        opener = Mock()
        opener.open.side_effect = [
            _FakeHttpResponse(b"", status=404),
            _FakeHttpResponse(json.dumps({"ok": True, "loggedOn": True,
                                          "friendStatusReady": True}).encode()),
        ]
        with patch.object(health, "command", side_effect=fake_command), \
             patch.object(health, "napcat_online", return_value=True), \
             patch.object(health, "matrix_egress_ok", return_value=True), \
             patch.object(health, "openai_egress_ok", return_value=True), \
             patch.object(health, "dsh_daemon_ok", return_value=True), \
             patch.object(health, "build_opener", return_value=opener):
            issues, metrics = health.collect(
                Path("/unused"), {}, NOVEMBER_2026, profile="gcp", vnstat_iface="ens4"
            )

        self.assertEqual(issues["egress"].level, health.CRITICAL)
        self.assertEqual(metrics["egress_tx_bytes"], tx)
        for skipped in ("matrix_egress", "openai_egress", "dsh_daemon", "qq_online"):
            self.assertNotIn(skipped, issues)
        systemctl = next(call for call in calls if call[0] == "systemctl")
        self.assertIn("csbot-dsh.service", systemctl)
        self.assertNotIn("mihomo.service", systemctl)
        self.assertTrue(any(call[0] == "vnstat" for call in calls))
        docker_inspect = next(call for call in calls if call[:2] == ["docker", "inspect"])
        self.assertIn("csbot-database", docker_inspect)
        self.assertNotIn("csbot-napcat", docker_inspect)

    def test_edge_collect_skips_backend_and_uses_matrix_proxy(self):
        health = self.health
        calls = []

        def fake_command(args, timeout=5):
            calls.append(list(args))
            if args[0] == "systemctl":
                return "\n\n".join(
                    f"Id={service}\nActiveState=active\nMemoryCurrent=104857600\nNRestarts=0"
                    for service in health.PROFILE_SERVICES["edge"]
                )
            if args[:2] == ["docker", "inspect"]:
                return "\n".join(
                    f'"/{name}" true false healthy'
                    for name in health.PROFILE_CONTAINERS["edge"]
                )
            return ""

        opener = Mock()
        opener.open.side_effect = [_FakeHttpResponse(b"", status=404)]
        matrix_ok = Mock(return_value=True)
        with patch.object(health, "command", side_effect=fake_command), \
             patch.object(health, "napcat_online", return_value=True), \
             patch.object(health, "matrix_egress_ok", matrix_ok), \
             patch.object(health, "build_opener", return_value=opener):
            issues, _ = health.collect(
                Path("/unused"), {}, NOVEMBER_2026, profile="edge",
                matrix_proxy="http://100.64.0.9:18080",
                remote_backend_url="http://100.64.0.9:8888/__health_alert_probe__",
            )

        matrix_ok.assert_called_once_with("http://100.64.0.9:18080")
        self.assertNotIn("matrix_egress", issues)
        self.assertFalse(any(call[0] == "vnstat" for call in calls))
        self.assertFalse(any(call[:2] == ["docker", "exec"] for call in calls))
        systemctl = next(call for call in calls if call[0] == "systemctl")
        self.assertIn("tuwunel-push-sync.service", systemctl)
        self.assertNotIn("csbot.service", systemctl)

    def test_edge_collect_without_remote_url_skips_the_optional_probe(self):
        health = self.health

        def fake_command(args, timeout=5):
            if args[0] == "systemctl":
                return "\n\n".join(
                    f"Id={service}\nActiveState=active\nMemoryCurrent=104857600\nNRestarts=0"
                    for service in health.PROFILE_SERVICES["edge"]
                )
            if args[:2] == ["docker", "inspect"]:
                return "\n".join(
                    f'"/{name}" true false healthy'
                    for name in health.PROFILE_CONTAINERS["edge"]
                )
            return ""

        opener = Mock()
        with patch.object(health, "command", side_effect=fake_command), \
             patch.object(health, "napcat_online", return_value=True), \
             patch.object(health, "matrix_egress_ok", return_value=True), \
             patch.object(health, "build_opener", return_value=opener):
            issues, _ = health.collect(Path("/unused"), {}, NOVEMBER_2026, profile="edge")
        self.assertNotIn("remote_backend_http", issues)
        opener.assert_not_called()


# --------------------------------------------------------------- ai_preflight
class PreflightDaemonTests(unittest.TestCase):
    def setUp(self):
        if "csbot_ai_preflight" in sys.modules:
            self.preflight = sys.modules["csbot_ai_preflight"]
        else:
            self.preflight = load_module("csbot_ai_preflight", REPO / "scripts/ai_preflight.py")

    def test_token_sources_never_leak(self):
        self.assertEqual(self.preflight._daemon_token({"CS_AI_DSH_DAEMON_TOKEN": "abc"}), "abc")
        self.assertEqual(self.preflight._daemon_token({}), "")
        with tempfile.TemporaryDirectory() as tmp:
            token = Path(tmp) / "token"
            token.write_text("  file-token\n", encoding="utf-8")
            self.assertEqual(
                self.preflight._daemon_token({"CS_AI_DSH_DAEMON_TOKEN_FILE": str(token)}),
                "file-token",
            )
            self.assertEqual(
                self.preflight._daemon_token({"CS_AI_DSH_DAEMON_TOKEN_FILE": str(Path(tmp) / "missing")}),
                "",
            )

    def test_daemon_url_must_be_loopback_and_never_echoes_token(self):
        values = {
            "CS_AI_DSH_DAEMON_TOKEN": "SECRET-TOKEN-VALUE",
            "CS_AI_DSH_DAEMON_URL": "https://127.0.0.1:32151",
        }
        with self.assertRaises(ValueError) as error:
            self.preflight.check_dsh_daemon(values)
        self.assertNotIn("SECRET-TOKEN-VALUE", str(error.exception))
        for url in ("http://10.0.0.5:32151", "http://127.0.0.1", "http://example.com:32151"):
            with self.assertRaises(ValueError, msg=url):
                self.preflight.check_dsh_daemon({"CS_AI_DSH_DAEMON_URL": url})


# --------------------------------------------------------------- systemd files
class SystemdTemplateTests(unittest.TestCase):
    def test_dsh_service_template(self):
        text = (REPO / "deploy/systemd/csbot-dsh.service").read_text(encoding="utf-8")
        self.assertIn("Type=simple", text)
        self.assertIn(
            "ExecStart=/home/ubuntu/csbot/.tools/node/bin/node /home/ubuntu/csbot/ai_runtime/dsh/daemon-launch.mjs",
            text,
        )
        self.assertIn(
            "Environment=PATH=/home/ubuntu/csbot/.tools/node/bin:/usr/local/bin:/usr/bin:/bin",
            text,
        )
        self.assertIn("EnvironmentFile=-/home/ubuntu/csbot/.env.prod", text)
        self.assertIn("EnvironmentFile=-/home/ubuntu/csbot/deploy/gcp-direct.env", text)
        self.assertIn("StateDirectory=csbot-dsh", text)
        self.assertIn("Environment=DSH_HOME=/var/lib/csbot-dsh", text)
        self.assertIn("Restart=on-failure", text)
        self.assertIn("RestartSec=5", text)
        self.assertIn("User=ubuntu", text)
        self.assertIn("NoNewPrivileges=true", text)
        self.assertIn("PrivateTmp=true", text)
        self.assertIn("ProtectSystem=strict", text)
        self.assertIn("ReadWritePaths=", text)
        self.assertIn("LimitNOFILE=65536", text)

    def test_push_proxy_service_template(self):
        text = (REPO / "deploy/systemd/csbot-matrix-push-proxy.service").read_text(encoding="utf-8")
        self.assertIn(
            "ExecStart=/usr/bin/python3 /home/ubuntu/csbot/scripts/matrix_push_proxy.py "
            "--host 127.0.0.1 --port 18080",
            text,
        )
        self.assertIn("--host 100.x.y.z", text)
        self.assertIn("Restart=on-failure", text)
        self.assertIn("RestartSec=5", text)
        self.assertIn("User=ubuntu", text)

    def test_gcp_daemon_dropin_has_both_sections(self):
        text = (REPO / "deploy/systemd/csbot.service.d/gcp-daemon.conf").read_text(encoding="utf-8")
        self.assertIn("[Unit]", text)
        self.assertIn("[Service]", text)
        self.assertIn("Requires=csbot-dsh.service", text)
        self.assertIn("After=csbot-dsh.service", text)
        self.assertIn("Environment=CS_AI_DSH_MODE=daemon", text)
        self.assertIn("Environment=CS_AI_DSH_DAEMON_URL=http://127.0.0.1:32151", text)
        self.assertIn(
            "Environment=CS_AI_DSH_DAEMON_TOKEN_FILE=/var/lib/csbot-dsh/bridge-token", text
        )
        self.assertIn("EnvironmentFile=-/home/ubuntu/csbot/deploy/gcp-direct.env", text)

    def test_direct_egress_env_disables_the_old_proxy(self):
        text = (REPO / "deploy/gcp-direct.env").read_text(encoding="utf-8")
        self.assertIn("CSBOT_DIRECT_EGRESS=1", text)
        self.assertIn("CS_AI_OPENAI_PROXY_URL=direct", text)
        self.assertIn("CS_AI_FETCH_PROXY_URL=", text)
        self.assertNotIn("CS_AI_OPENAI_PROXY_URL=http", text)
        self.assertNotIn("CS_AI_FETCH_PROXY_URL=http", text)

    def test_templates_are_lf(self):
        for name in (
            "deploy/systemd/csbot-dsh.service",
            "deploy/systemd/csbot-matrix-push-proxy.service",
            "deploy/systemd/csbot.service.d/gcp-daemon.conf",
        ):
            self.assertNotIn(b"\r\n", (REPO / name).read_bytes(), name)


# --------------------------------------------------------------- module A: daemon
class DaemonModeTests(unittest.TestCase):
    def test_resolve_dsh_mode_default_and_invalid(self):
        from ai_runtime import runner

        with env(CS_AI_DSH_MODE=None):
            self.assertEqual(runner.resolve_dsh_mode(), "subprocess")
        with env(CS_AI_DSH_MODE=" Daemon "):
            self.assertEqual(runner.resolve_dsh_mode(), "daemon")
        with env(CS_AI_DSH_MODE="oneshot"):
            with self.assertRaises(ValueError):
                runner.resolve_dsh_mode()

    @unittest.skipIf(os.name == "nt", "ai_runtime.store requires POSIX fcntl")
    def test_session_titles_are_readable(self):
        from ai_runtime.service import session_title

        self.assertEqual(session_title("qq", "832126798", ""), "QQ群 832126798")
        self.assertEqual(session_title("birthday", "832126798", ""), "生日祝福 832126798")
        self.assertEqual(session_title("report", "832126798", ""), "群报告 832126798")
        self.assertEqual(session_title("web", "832126798", "42"), "网页 832126798/42")
        self.assertEqual(session_title("other", "1", "2"), "")


class DaemonSourceContractTests(unittest.TestCase):
    """Static review of the launcher/bridge safety contract."""

    def test_launcher_disables_dangerous_host_rows(self):
        text = (REPO / "ai_runtime/dsh/daemon-launch.mjs").read_text(encoding="utf-8")
        for name in (
            "tool-bash", "tool-pwsh", "tool-jobs", "tool-subagent", "tool-subagent-control",
            "tool-goal", "tool-ralph", "tool-workflow", "tool-ask-user", "pty",
            "session-log-deepseek", "plugin-package-inventory-deepseek", "playwright-mcp",
            "windows-mcp", "hmr", "session-telemetry-otel",
        ):
            self.assertIn(f"'{name}'", text, name)
        self.assertIn("'--profile', 'web'", text)
        self.assertIn("'--host', '127.0.0.1'", text)
        self.assertNotIn("0.0.0.0", text)
        # The authenticated deep link never reaches stdout/syslog.
        self.assertIn("dsh web:", text)
        self.assertIn("writePrivate(LOGIN_URL_FILE", text)
        # Ambient proxy variables must not leak into the DSH child process.
        for name in ("'HTTP_PROXY'", "'HTTPS_PROXY'", "'ALL_PROXY'"):
            self.assertNotIn(name, text, name)
        self.assertIn("resolveDaemonConfig", text)
        self.assertIn("invokedDirectly", text)

    def test_bridge_is_loopback_bearer_protected(self):
        text = (REPO / "ai_runtime/dsh/daemon-bridge.mjs").read_text(encoding="utf-8")
        self.assertIn("timingSafeEqual", text)
        self.assertIn("server.listen(bridgePort, '127.0.0.1'", text)
        self.assertIn("export function isWebWriteRequest", text)
        self.assertIn("READ_ONLY_NOTICE", text)
        # Per-turn state must be keyed, never a module-level singleton.
        self.assertIn("class TurnState", text)
        self.assertIn("const turns = new Map()", text)


def _mneme_schema() -> str:
    return """
    CREATE TABLE memories (
      id TEXT PRIMARY KEY, type TEXT NOT NULL, title TEXT NOT NULL, content TEXT NOT NULL,
      tags TEXT NOT NULL DEFAULT '[]', importance INTEGER NOT NULL DEFAULT 3,
      forgotten INTEGER NOT NULL DEFAULT 0, archived INTEGER NOT NULL DEFAULT 0,
      source TEXT, content_history TEXT, content_hash TEXT, embedding TEXT,
      epistemic_status TEXT NOT NULL DEFAULT 'subjective', last_accessed_at TEXT,
      _full_content TEXT, evidence TEXT, doc_path TEXT,
      created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
      workspace_scope TEXT, workspace_scope_source TEXT
    );
    """


def _build_memory_db(path: Path, rows: list[dict]) -> None:
    import sqlite3

    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(path)
    try:
        connection.executescript(_mneme_schema())
        for row in rows:
            connection.execute(
                "INSERT INTO memories (id,type,title,content,tags,importance,created_at,updated_at,"
                "workspace_scope,workspace_scope_source) VALUES (?,?,?,?,?,?,?,?,?,?)",
                (row["id"], row.get("type", "project"), row["title"], row["content"], "[]", 3,
                 "2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z",
                 row.get("workspace_scope"), row.get("workspace_scope_source")),
            )
        connection.commit()
    finally:
        connection.close()


try:  # runner (and therefore memory_browser) needs httpx, present in the venv.
    from ai_runtime import memory_browser as _memory_browser
except Exception:  # pragma: no cover - bare Python without runtime deps
    _memory_browser = None


@unittest.skipUnless(_memory_browser is not None, "ai_runtime dependencies are not installed")
class MemoryBrowserUnifiedTests(unittest.TestCase):
    def setUp(self):
        from ai_runtime.runner import scope_key

        self.scope_key = scope_key
        self.tmp = tempfile.TemporaryDirectory(prefix="csbot-mem-browser-")
        self.root = Path(self.tmp.name)
        self.scope = scope_key("web", "832126798", "42", "default")
        self.own_ws = str((self.root / "scopes" / self.scope / "workspace").resolve())
        self.other_ws = str((self.root / "scopes" / ("f" * 64) / "workspace").resolve())
        self.db = self.root / "web-memory" / "memory.db"
        _build_memory_db(self.db, [
            {"id": "a1", "title": "本群记忆", "content": "本群内容",
             "workspace_scope": self.own_ws, "workspace_scope_source": "explicit"},
            {"id": "b1", "title": "别群记忆", "content": "别群内容",
             "workspace_scope": self.other_ws, "workspace_scope_source": "explicit"},
            {"id": "u1", "title": "未标注记忆", "content": "未标注内容",
             "workspace_scope": None, "workspace_scope_source": None},
        ])
        self.store = Mock()
        self.store.rows = Mock(return_value=[{"ok": 1}])

    def tearDown(self):
        self.tmp.cleanup()

    def test_unified_database_filters_by_workspace_scope(self):
        with env(CS_AI_DSH_MEMORY_DB=str(self.db), CS_AI_DSH_MODE="daemon"):
            page = _memory_browser.browse(self.store, "832126798", "42", self.scope, root=self.root)
        ids = [item["id"] for item in page["items"]]
        self.assertEqual(ids, ["a1"])
        self.assertEqual(page["total"], 1)

    def test_other_workspace_row_is_not_readable(self):
        with env(CS_AI_DSH_MEMORY_DB=str(self.db), CS_AI_DSH_MODE="daemon"):
            with self.assertRaises(PermissionError):
                _memory_browser.detail(self.store, "832126798", "42", self.scope, "b1", root=self.root)
            with self.assertRaises(PermissionError):
                _memory_browser.detail(self.store, "832126798", "42", self.scope, "u1", root=self.root)

    def test_legacy_layout_still_works(self):
        legacy = self.root / "scopes" / self.scope / "memory" / "memory.db"
        _build_memory_db(legacy, [{"id": "l1", "title": "旧库", "content": "旧内容"}])
        with env(CS_AI_DSH_MEMORY_DB=None, CS_AI_DSH_MODE="subprocess"):
            page = _memory_browser.browse(self.store, "832126798", "42", self.scope, root=self.root)
        self.assertEqual([item["id"] for item in page["items"]], ["l1"])


class MigrateMnemeTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="csbot-migrate-")
        self.state = Path(self.tmp.name)
        self.spec = importlib.util.spec_from_file_location(
            "migrate_mneme_to_daemon", REPO / "scripts/migrate_mneme_to_daemon.py")
        self.module = importlib.util.module_from_spec(self.spec)
        self.spec.loader.exec_module(self.module)
        self.scope_a = "a" * 64
        self.scope_b = "b" * 64
        _build_memory_db(self.state / "scopes" / self.scope_a / "memory" / "memory.db", [
            {"id": "m1", "title": "一号", "content": "内容一"}])
        _build_memory_db(self.state / "scopes" / self.scope_b / "memory" / "memory.db", [
            {"id": "m2", "title": "二号", "content": "内容二"}])
        sessions = self.state / "scopes" / self.scope_a / "harness" / "proj" / "sessions"
        sessions.mkdir(parents=True)
        (sessions / "s1.jsonl").write_text('{"type":"start"}\n', encoding="utf-8")

    def tearDown(self):
        self.tmp.cleanup()

    def _target_rows(self):
        import sqlite3

        connection = sqlite3.connect(self.state / "web-memory" / "memory.db")
        try:
            return connection.execute(
                "SELECT id,content,workspace_scope,workspace_scope_source FROM memories ORDER BY id"
            ).fetchall()
        finally:
            connection.close()

    def test_dry_run_writes_nothing(self):
        code = self.module.main(["--state-dir", str(self.state)])
        self.assertEqual(code, 0)
        self.assertFalse((self.state / "web-memory" / "memory.db").exists())
        self.assertFalse((self.state / "web-sessions").exists())

    def test_apply_is_idempotent_and_sets_explicit_scope(self):
        backup = self.state / "backup-1"
        code = self.module.main(["--state-dir", str(self.state), "--apply", "--backup-dir", str(backup)])
        self.assertEqual(code, 0)
        rows = self._target_rows()
        self.assertEqual([row[0] for row in rows], ["m1", "m2"])
        for memory_id, _content, workspace, source in rows:
            self.assertTrue(workspace and workspace.endswith("workspace"), (memory_id, workspace))
            self.assertEqual(source, "explicit")
        self.assertTrue(any(path.name.endswith("-memory.db") for path in backup.iterdir()))
        self.assertTrue((self.state / "web-sessions" / "s1.jsonl").is_file())
        # Second run: everything already migrated, still success.
        code = self.module.main(["--state-dir", str(self.state), "--apply", "--backup-dir", str(self.state / "backup-2")])
        self.assertEqual(code, 0)
        self.assertEqual(self._target_rows(), rows)

    def test_conflict_aborts_without_overwriting(self):
        import sqlite3

        self.assertEqual(self.module.main(
            ["--state-dir", str(self.state), "--apply", "--backup-dir", str(self.state / "backup-1")]), 0)
        before = self._target_rows()
        connection = sqlite3.connect(self.state / "scopes" / self.scope_a / "memory" / "memory.db")
        try:
            connection.execute("UPDATE memories SET content='被篡改的内容' WHERE id='m1'")
            connection.commit()
        finally:
            connection.close()
        code = self.module.main(["--state-dir", str(self.state), "--apply",
                                 "--backup-dir", str(self.state / "backup-3")])
        self.assertEqual(code, 1)
        self.assertEqual(self._target_rows(), before)


class DaemonConfigDerivationTests(unittest.TestCase):
    """`.env.prod`-style `CS_AI_*` derivation for the resident daemon."""

    @classmethod
    def setUpClass(cls):
        cls.node = shutil.which("node")
        if not cls.node:
            raise unittest.SkipTest("node is not available")

    def resolve(self, env):
        script = (
            "import {readFileSync} from 'node:fs';"
            "import {resolveDaemonConfig} from "
            + json.dumps((REPO / "ai_runtime/dsh/daemon-launch.mjs").as_uri()) + ";"
            "const env=JSON.parse(readFileSync(0,'utf8')||'{}');"
            "try{process.stdout.write(JSON.stringify(resolveDaemonConfig(env)))}"
            "catch(e){process.stdout.write(JSON.stringify({error:String(e.message)}))}"
        )
        completed = subprocess.run(
            [self.node, "--input-type=module", "-e", script],
            input=json.dumps(env), text=True, capture_output=True, timeout=60,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr[-1200:])
        return json.loads(completed.stdout)

    def test_openai_with_direct_proxy_and_official_search(self):
        config = self.resolve({
            "CS_AI_URL": "https://api.openai.com",
            "CS_AI_API_KEY": "sk-openai-fixture",
            "CS_AI_MODEL": "gpt-6.2-sol",
            "CS_AI_OPENAI_PROXY_URL": "direct",
            "CS_AI_SEARCH_URL": "https://api.deepseek.com/anthropic/v1",
            "CS_AI_SEARCH_API_KEY": "sk-deepseek-fixture",
            "CS_AI_FETCH_PROXY_URL": "http://127.0.0.1:7890",
        })
        self.assertEqual(config["provider"], "openai")
        self.assertEqual(config["model"], "gpt-6.2-sol")
        self.assertEqual(config["openaiApiKey"], "sk-openai-fixture")
        self.assertEqual(config["openaiBaseUrl"], "https://api.openai.com/v1")
        self.assertEqual(config["llmProxyUrl"], "")
        self.assertTrue(config["mountPiAi"])
        # Search key is same-origin with api.deepseek.com, so aux/fallback reuse it.
        self.assertEqual(config["deepseekApiKey"], "sk-deepseek-fixture")
        self.assertEqual(config["deepseekBaseUrl"], "https://api.deepseek.com")
        self.assertEqual(config["auxProvider"], "deepseek-official")
        self.assertEqual(config["auxModel"], "deepseek-flash")
        self.assertEqual(config["fallbackProvider"], "deepseek-official")
        self.assertEqual(config["fallbackModel"], "deepseek-flash")
        self.assertTrue(config["webEnabled"])
        self.assertEqual(config["searchUrl"], "https://api.deepseek.com/anthropic/v1")
        self.assertEqual(config["fetchProxyUrl"], "http://127.0.0.1:7890")

    def test_third_party_search_never_reuses_credential(self):
        config = self.resolve({
            "CS_AI_URL": "https://api.openai.com/v1",
            "CS_AI_API_KEY": "sk-openai-fixture",
            "CS_AI_SEARCH_URL": "https://search.example/v1",
            "CS_AI_SEARCH_API_KEY": "sk-third-party",
        })
        self.assertEqual(config["searchApiKey"], "sk-third-party")
        self.assertEqual(config["webEnabled"], True)
        # The third-party key is never replayed against api.deepseek.com.
        self.assertEqual(config["deepseekApiKey"], "")
        self.assertEqual(config["auxProvider"], "")
        self.assertEqual(config["auxApiKey"], "")
        self.assertEqual(config["fallbackProvider"], "")
        self.assertEqual(config["fallbackApiKey"], "")

    def test_native_deepseek_primary(self):
        config = self.resolve({
            "CS_AI_URL": "https://api.deepseek.com",
            "CS_AI_API_KEY": "ds-fixture",
            "CS_AI_MODEL": "deepseek-chat",
        })
        self.assertEqual(config["provider"], "deepseek-official")
        self.assertEqual(config["model"], "deepseek-chat")
        self.assertEqual(config["deepseekApiKey"], "ds-fixture")
        self.assertEqual(config["deepseekBaseUrl"], "https://api.deepseek.com")
        self.assertFalse(config["mountPiAi"])
        self.assertEqual(config["llmProxyUrl"], "")
        self.assertFalse(config["webEnabled"])

    def test_explicit_csbot_values_win(self):
        config = self.resolve({
            "CS_AI_URL": "https://api.openai.com/v1",
            "CS_AI_API_KEY": "sk-openai-fixture",
            "CSBOT_PROVIDER": "deepseek-official",
            "CSBOT_MODEL": "explicit-model",
            "DEEPSEEK_API_KEY": "explicit-deepseek-key",
            "CSBOT_LLM_PROXY_URL": "direct",
        })
        self.assertEqual(config["provider"], "deepseek-official")
        self.assertEqual(config["model"], "explicit-model")
        self.assertEqual(config["deepseekApiKey"], "explicit-deepseek-key")
        # An explicit provider switch must not inherit the OpenAI base URL.
        self.assertEqual(config["deepseekBaseUrl"], "https://api.deepseek.com")

    def test_aux_switch_is_orthogonal_to_fallback(self):
        config = self.resolve({
            "CS_AI_URL": "https://api.openai.com/v1",
            "CS_AI_API_KEY": "sk-openai-fixture",
            "CS_AI_SEARCH_API_KEY": "sk-deepseek-fixture",
            "CS_AI_AUX_ENABLE": "0",
        })
        self.assertEqual(config["auxProvider"], "")
        self.assertEqual(config["auxApiKey"], "")
        self.assertEqual(config["fallbackProvider"], "deepseek-official")
        self.assertEqual(config["fallbackModel"], "deepseek-flash")

    def test_proxy_direct_by_default_and_explicit_values(self):
        default = self.resolve({
            "CS_AI_URL": "https://api.openai.com/v1",
            "CS_AI_API_KEY": "sk-openai-fixture",
        })
        # GCP dials directly: an unset proxy must not resurrect :7890.
        self.assertEqual(default["llmProxyUrl"], "")
        explicit = self.resolve({
            "CS_AI_URL": "https://api.openai.com/v1",
            "CS_AI_API_KEY": "sk-openai-fixture",
            "CS_AI_OPENAI_PROXY_URL": "http://127.0.0.1:7890",
        })
        self.assertEqual(explicit["llmProxyUrl"], "http://127.0.0.1:7890")
        self.assertEqual(explicit["llmProxyHost"], "api.openai.com")
        for off in ("", "none", "direct", "off", "false"):
            config = self.resolve({
                "CS_AI_URL": "https://api.openai.com/v1",
                "CS_AI_API_KEY": "sk-openai-fixture",
                "CS_AI_OPENAI_PROXY_URL": off,
            })
            self.assertEqual(config["llmProxyUrl"], "", off)
        # The GCP direct-egress switch wins over a stale .env.prod value.
        forced = self.resolve({
            "CS_AI_URL": "https://api.openai.com/v1",
            "CS_AI_API_KEY": "sk-openai-fixture",
            "CS_AI_OPENAI_PROXY_URL": "http://127.0.0.1:7890",
            "CSBOT_DIRECT_EGRESS": "1",
        })
        self.assertEqual(forced["llmProxyUrl"], "")
        invalid = self.resolve({
            "CS_AI_URL": "https://api.openai.com/v1",
            "CS_AI_API_KEY": "sk-openai-fixture",
            "CS_AI_OPENAI_PROXY_URL": "http://10.0.0.5:7890",
        })
        self.assertIn("error", invalid)
        self.assertIn("loopback", invalid["error"])


@unittest.skipUnless(_memory_browser is not None, "ai_runtime dependencies are not installed")
class DirectEgressTests(unittest.TestCase):
    def test_direct_flag_disables_llm_and_fetch_proxies(self):
        from ai_runtime import openai_support
        from ai_runtime.runner import web_environment

        endpoint = "https://api.openai.com/v1"
        with env(CSBOT_DIRECT_EGRESS="1", CS_AI_OPENAI_PROXY_URL="http://127.0.0.1:7890"):
            self.assertEqual(openai_support.resolve_openai_proxy(endpoint), "")
        with env(CSBOT_DIRECT_EGRESS=None, CS_AI_OPENAI_PROXY_URL="http://127.0.0.1:7890"):
            self.assertEqual(openai_support.resolve_openai_proxy(endpoint), "http://127.0.0.1:7890")
        with env(
            CSBOT_DIRECT_EGRESS="1",
            CS_AI_FETCH_PROXY_URL="http://127.0.0.1:7890",
            CS_AI_SEARCH_URL="https://api.deepseek.com/anthropic/v1",
            CS_AI_SEARCH_API_KEY="sk-deepseek-fixture",
        ):
            values = web_environment("https://api.deepseek.com", "ds-fixture")
        self.assertNotIn("CSBOT_FETCH_PROXY_URL", values)


class ReportingResult(unittest.TextTestResult):
    def addSuccess(self, test):
        super().addSuccess(test)
        self.stream.write(f"PASS {test.id()}\n")

    def addFailure(self, test, err):
        super().addFailure(test, err)
        self.stream.write(f"FAIL {test.id()}\n")

    def addError(self, test, err):
        super().addError(test, err)
        self.stream.write(f"FAIL {test.id()}\n")

    def addSkip(self, test, reason):
        super().addSkip(test, reason)
        self.stream.write(f"SKIP {test.id()}: {reason}\n")


def main() -> int:
    suite = unittest.TestLoader().loadTestsFromModule(sys.modules[__name__])
    result = unittest.TextTestRunner(
        stream=sys.stdout, verbosity=0, resultclass=ReportingResult
    ).run(suite)
    failed = len(result.failures) + len(result.errors)
    passed = result.testsRun - failed - len(result.skipped)
    print(f"\nPASS: {passed}  FAIL: {failed}  TOTAL: {result.testsRun}")
    return 0 if result.wasSuccessful() else 1


if __name__ == "__main__":
    raise SystemExit(main())
