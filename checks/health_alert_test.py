#!/usr/bin/env python3
"""Health alert policy and Matrix delivery checks; no production services or messages."""
import importlib.util
from io import BytesIO
import json
import os
from pathlib import Path
import stat
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch
from urllib.error import HTTPError

spec = importlib.util.spec_from_file_location("health_alert", Path(__file__).parents[1] / "scripts/health_alert.py")
health = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = health
spec.loader.exec_module(health)


class _FakeHttpResponse:
    def __init__(self, body: bytes, status: int = 200):
        self._body = body
        self.status = status

    def read(self, _limit: int = -1) -> bytes:
        return self._body

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class AlertTests(unittest.TestCase):
    def setUp(self):
        self.state = {"version": health.STATE_VERSION}
        self.sender = Mock()
        self.config = health.MatrixConfig(
            homeserver="http://127.0.0.1:6167",
            room_id="!room123:ave-mujica.top",
            access_token="secret-token-value",
            mention_user_id="@csbot:ave-mujica.top",
            mention_label="@csbot",
        )

    def cycle(self, issues, at):
        events = health.notifications(issues, self.state, at)
        with patch.object(health, "emit"):
            health.deliver(events, self.state, at, self.sender, host="cgserver")
        return events

    def test_resources_warn_before_full_and_critical_immediately(self):
        disk = {"available": 4 * health.GIB, "used_ratio": .89, "inode_ratio": .5}
        memory = {"available": 1024 * health.MIB}
        self.assertEqual(health.resource_issues(disk, memory), {})
        disk["used_ratio"] = .90
        self.assertEqual(health.resource_issues(disk, memory)["disk"].level, health.WARNING)
        disk["available"] = 799 * health.MIB
        issue = health.resource_issues(disk, memory)["disk"]
        self.assertEqual((issue.level, issue.samples), (health.CRITICAL, 1))
        memory["available"] = 256 * health.MIB
        disk["inode_ratio"] = .01
        found = health.resource_issues(disk, memory)
        self.assertEqual(found["host_memory"].level, health.CRITICAL)
        self.assertEqual(found["inodes"].level, health.CRITICAL)

    def test_confirmation_cooldown_and_escalation_wakes_supervisor(self):
        issues = {"disk": health.Issue(health.WARNING, "disk warning")}
        self.cycle(issues, 10000)
        self.sender.assert_not_called()

        self.cycle(issues, 10060)
        self.assertEqual(self.sender.call_count, 1)
        body, _, kwargs = self.sender.call_args.args[0], self.sender.call_args.args[1], self.sender.call_args.kwargs
        self.assertFalse(kwargs["wake_supervisor"])
        self.assertNotIn("@", body)

        # Within 6h warning cooldown -> suppressed
        self.cycle(issues, 10120)
        self.assertEqual(self.sender.call_count, 1)

        # Escalates to CRITICAL -> immediate notification with @csbot wake
        self.cycle({"disk": health.Issue(health.CRITICAL, "disk full", 1)}, 10180)
        self.assertEqual(self.sender.call_count, 2)
        self.assertTrue(self.sender.call_args.kwargs["wake_supervisor"])
        self.assertIn("@csbot", self.sender.call_args.args[0])

        # Still within 1h critical cooldown -> suppressed; after 1h -> re-alerts & wakes
        self.cycle({"disk": health.Issue(health.CRITICAL, "disk full", 1)}, 13700)
        self.assertEqual(self.sender.call_count, 2)
        self.cycle({"disk": health.Issue(health.CRITICAL, "disk full", 1)}, 13780)
        self.assertEqual(self.sender.call_count, 3)
        self.assertTrue(self.sender.call_args.kwargs["wake_supervisor"])

    def test_wake_throttle_10m_suppresses_repeat_mention_without_dropping_alert(self):
        self.cycle({"a": health.Issue(health.CRITICAL, "svc a down", 1)}, 10000)
        self.assertEqual(self.sender.call_count, 1)
        self.assertTrue(self.sender.call_args.kwargs["wake_supervisor"])
        self.assertIn("@csbot", self.sender.call_args.args[0])

        # Within 10m global wake throttle: alert still delivered, wake_supervisor=False, no @ in body
        self.cycle({"a": health.Issue(health.CRITICAL, "svc a down", 1), "b": health.Issue(health.CRITICAL, "svc b down @admin", 1)}, 10120)
        self.assertEqual(self.sender.call_count, 2)
        self.assertFalse(self.sender.call_args.kwargs["wake_supervisor"])
        self.assertNotIn("@", self.sender.call_args.args[0])
        self.assertIn("10m", self.sender.call_args.args[0])
        self.assertIn("＠admin", self.sender.call_args.args[0])

        # After 10m: new critical issue can wake again
        self.cycle({"a": health.Issue(health.CRITICAL, "svc a down", 1), "b": health.Issue(health.CRITICAL, "svc b down", 1), "c": health.Issue(health.CRITICAL, "svc c down", 1)}, 10660)
        self.assertEqual(self.sender.call_count, 3)
        self.assertTrue(self.sender.call_args.kwargs["wake_supervisor"])
        self.assertIn("@csbot", self.sender.call_args.args[0])

    def test_transient_warning_does_not_send_recovery(self):
        self.cycle({"memory": health.Issue(health.WARNING, "low")}, 10000)
        self.cycle({}, 10060)
        self.cycle({}, 10120)
        self.sender.assert_not_called()
        self.assertEqual(self.state["issues"], {})

    def test_two_healthy_samples_send_non_wake_recovery(self):
        self.cycle({"disk": health.Issue(health.CRITICAL, "full", 1)}, 10000)
        self.cycle({}, 10060)
        self.assertEqual(self.sender.call_count, 1)
        self.cycle({}, 10120)
        self.assertEqual(self.sender.call_count, 2)
        self.assertIn("[恢复]", self.sender.call_args.args[0])
        self.assertFalse(self.sender.call_args.kwargs["wake_supervisor"])
        self.assertNotIn("@", self.sender.call_args.args[0])
        self.assertEqual(self.state["issues"], {})
        self.cycle({}, 10180)
        self.assertEqual(self.sender.call_count, 2)

    def test_unreachable_channel_retains_incident_until_recovered_notice_and_never_leaks_exception(self):
        self.sender.side_effect = TimeoutError("sensitive http token must not leak")
        with patch.object(health, "emit") as emit:
            events = health.notifications({"qq_online": health.Issue(health.CRITICAL, "offline", 1)}, self.state, 10000)
            self.assertTrue(events)
            health.deliver(events, self.state, 10000, self.sender, host="cgserver")
            self.assertNotIn("sensitive http token", str(emit.call_args_list))
            self.assertIn("qq_online", self.state["issues"])
            self.assertIsNone(self.state["issues"]["qq_online"]["notified"])

        self.sender.side_effect = None
        self.cycle({}, 10060)
        self.cycle({}, 10360)
        self.assertEqual(self.sender.call_count, 2)
        self.assertIn("异常期间未能送达", self.sender.call_args.args[0])
        self.assertFalse(self.sender.call_args.kwargs["wake_supervisor"])
        self.assertEqual(self.state["issues"], {})

    def test_notification_length_does_not_acknowledge_unsent_items(self):
        issues = {f"item-{i:02}": health.Issue(health.CRITICAL, "x" * 200, 1) for i in range(30)}
        self.cycle(issues, 10000)
        first = sum(bool(v["notified"]) for v in self.state["issues"].values())
        self.assertGreater(first, 0)
        self.assertLess(first, 30)
        self.assertLessEqual(len(self.sender.call_args.args[0]), 3000)
        self.cycle(issues, 10060)
        second = sum(bool(v["notified"]) for v in self.state["issues"].values())
        self.assertGreater(second, first)
        self.assertLess(second, 30)
        self.cycle(issues, 10120)
        self.assertEqual(sum(bool(v["notified"]) for v in self.state["issues"].values()), 30)

    def test_steam_503_grading_and_container_dedup(self):
        docker_output = "\n".join([
            '"/csbot-database" true false healthy',
            '"/csbot-napcat" true false healthy',
            '"/steam_monitor" true false unhealthy',
            '"/csbot-homepage" true false healthy',
            '"/csbot-nginx" true false unhealthy',
        ])

        def fake_command(args, timeout=5):
            if args[0] == "systemctl":
                return "\n\n".join(
                    f"Id={svc}\nActiveState=active\nMemoryCurrent=104857600\nNRestarts=0"
                    for svc in health.SERVICES
                )
            if args[0] == "docker" and args[1] == "inspect":
                return docker_output
            if args[0] == "docker" and args[1] == "exec":
                return ""
            raise AssertionError(f"unexpected command: {args}")

        def steam_503(payload: dict):
            raw = json.dumps(payload).encode("utf-8")
            return HTTPError("http://127.0.0.1:5555/api/health", 503, "Service Unavailable", {}, BytesIO(raw))

        with patch.object(health, "command", side_effect=fake_command), \
             patch.object(health, "napcat_online", return_value=True), \
             patch.object(health, "matrix_egress_ok", return_value=True):
            # Case 1: recoveryAttempt=1 -> WARNING (not duplicated by container:steam_monitor), csbot-nginx still flagged
            opener = Mock()
            opener.open.side_effect = [
                _FakeHttpResponse(b"", status=404),
                steam_503({"ok": False, "recoveryMode": True, "recoveryAttempt": 1, "consecutiveLoginFailures": 2, "reconnectStopped": False, "lastLoginError": "timeout"}),
            ]
            with patch.object(health, "build_opener", return_value=opener):
                issues, _ = health.collect(Path("/unused"), {}, 10000.0)
            self.assertNotIn("container:steam_monitor", issues)
            self.assertIn("container:csbot-nginx", issues)
            self.assertEqual(issues["steam_ready"].level, health.WARNING)
            self.assertEqual(issues["steam_ready"].samples, 2)

            # Case 2: recoveryAttempt=3 -> CRITICAL (samples=2)
            opener.open.side_effect = [
                _FakeHttpResponse(b"", status=404),
                steam_503({"ok": False, "recoveryMode": True, "recoveryAttempt": 3, "consecutiveLoginFailures": 4, "reconnectStopped": False, "lastLoginError": "login-timeout-reset"}),
            ]
            with patch.object(health, "build_opener", return_value=opener):
                issues, _ = health.collect(Path("/unused"), {}, 10060.0)
            self.assertEqual(issues["steam_ready"].level, health.CRITICAL)
            self.assertEqual(issues["steam_ready"].samples, 2)

            # Case 3: reconnectStopped=True -> immediate CRITICAL (samples=1)
            opener.open.side_effect = [
                _FakeHttpResponse(b"", status=404),
                steam_503({"ok": False, "recoveryMode": False, "recoveryAttempt": 0, "consecutiveLoginFailures": 1, "reconnectStopped": True, "lastLoginError": "AccessDenied"}),
            ]
            with patch.object(health, "build_opener", return_value=opener):
                issues, _ = health.collect(Path("/unused"), {}, 10120.0)
            self.assertEqual(issues["steam_ready"].level, health.CRITICAL)
            self.assertEqual(issues["steam_ready"].samples, 1)

            # Case 4: steam_monitor container stopped -> container:steam_monitor IS flagged
            stopped_docker = docker_output.replace('"/steam_monitor" true false unhealthy', '"/steam_monitor" false false unhealthy')
            with patch.object(health, "command", side_effect=lambda args, timeout=5: stopped_docker if args[1] == "inspect" else fake_command(args, timeout)):
                opener.open.side_effect = [
                    _FakeHttpResponse(b"", status=404),
                    _FakeHttpResponse(json.dumps({"ok": True, "loggedOn": True, "friendStatusReady": True}).encode("utf-8")),
                ]
                with patch.object(health, "build_opener", return_value=opener):
                    issues, _ = health.collect(Path("/unused"), {}, 10180.0)
            self.assertIn("container:steam_monitor", issues)
            self.assertNotIn("steam_ready", issues)

    def test_matrix_notifier_put_request_mentions_and_ack_validation(self):
        notifier = health.MatrixNotifier(self.config)
        captured = []

        def fake_open(req, timeout=10):
            captured.append(req)
            return _FakeHttpResponse(json.dumps({"event_id": "$ev123"}).encode("utf-8"))

        notifier.opener = Mock(open=Mock(side_effect=fake_open))

        # Non-wake send: empty m.mentions and rejects accidental @ in body
        ev1 = notifier.send("normal alert", "<p>normal alert</p>", wake_supervisor=False)
        self.assertEqual(ev1, "$ev123")
        req1 = captured[-1]
        self.assertEqual(req1.get_method(), "PUT")
        self.assertIn("%21room123%3Aave-mujica.top", req1.full_url)
        self.assertEqual(req1.headers.get("Authorization"), "Bearer secret-token-value")
        payload1 = json.loads(req1.data.decode("utf-8"))
        self.assertEqual(payload1["m.mentions"], {"user_ids": []})

        with self.assertRaises(ValueError):
            notifier.send("bad @csbot leak", "<p>bad</p>", wake_supervisor=False)

        # Wake send: attaches m.mentions user_ids
        ev2 = notifier.send("alert @csbot", "<p>alert</p>", wake_supervisor=True)
        self.assertEqual(ev2, "$ev123")
        payload2 = json.loads(captured[-1].data.decode("utf-8"))
        self.assertEqual(payload2["m.mentions"], {"user_ids": ["@csbot:ave-mujica.top"]})

        # Rejects missing event_id acknowledgement
        notifier.opener = Mock(open=Mock(return_value=_FakeHttpResponse(b"{}")))
        with self.assertRaises(RuntimeError):
            notifier.send("normal alert", "<p>normal alert</p>", wake_supervisor=False)

    def test_dry_run_never_delivers_or_updates_state(self):
        state = {"existing": "unchanged"}
        with patch.object(health, "collect", return_value=({"disk": health.Issue(health.CRITICAL, "full", 1)}, {})), \
             patch.object(health, "MatrixNotifier") as notifier, patch.object(health, "emit"):
            health.run(Path("/unused"), self.config, state, True)
            notifier.return_value.send.assert_not_called()
            self.assertEqual(state, {"existing": "unchanged"})

    def test_state_version_reset_and_private_atomic_save(self):
        legacy_state = {"issues": {"disk": {"notified": {"10001": {"at": 1, "level": 2}}}}, "attempts": {"10001": 1}}
        events = health.notifications({}, legacy_state, 10000)
        self.assertEqual(events, [])
        self.assertEqual(legacy_state["version"], health.STATE_VERSION)
        self.assertEqual(legacy_state["issues"], {})

        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "state.json"
            health.save_state(path, {"version": 1})
            health.save_state(path, {"version": 2})
            self.assertEqual(json.loads(path.read_text(encoding="utf-8")), {"version": 2})
            if os.name != "nt":
                self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o600)
            self.assertFalse(path.with_suffix(".tmp").exists())

    def test_matrix_egress_probe_and_critical_alert(self):
        captured_proxy = []
        captured_req = []
        orig_proxy_handler = health.ProxyHandler

        def fake_proxy_handler(proxies):
            captured_proxy.append(proxies)
            return orig_proxy_handler(proxies)

        opener = Mock()

        def fake_open(req, timeout=8):
            captured_req.append((req, timeout))
            return _FakeHttpResponse(json.dumps({"versions": ["r0.0.1", "v1.11"]}).encode("utf-8"))

        opener.open.side_effect = fake_open
        with patch.object(health, "ProxyHandler", side_effect=fake_proxy_handler), \
             patch.object(health, "build_opener", return_value=opener):
            self.assertTrue(health.matrix_egress_ok())

        self.assertEqual(captured_proxy[-1], {"http": "http://127.0.0.1:7890", "https": "http://127.0.0.1:7890"})
        self.assertEqual(captured_req[-1][0].full_url, "https://matrix.org/_matrix/client/versions")
        self.assertEqual(captured_req[-1][1], 8)

        opener.open.side_effect = lambda req, timeout=8: _FakeHttpResponse(b'{"versions":[]}')
        with patch.object(health, "build_opener", return_value=opener):
            self.assertFalse(health.matrix_egress_ok())

        docker_output = "\n".join(f'"/{c}" true false healthy' for c in health.CONTAINERS)

        def fake_command(args, timeout=5):
            if args[0] == "systemctl":
                return "\n\n".join(
                    f"Id={svc}\nActiveState=active\nMemoryCurrent=104857600\nNRestarts=0"
                    for svc in health.SERVICES
                )
            if args[0] == "docker" and args[1] == "inspect":
                return docker_output
            return ""

        base_opener = Mock()
        base_opener.open.side_effect = [
            _FakeHttpResponse(b"", status=404),
            _FakeHttpResponse(json.dumps({"ok": True, "loggedOn": True, "friendStatusReady": True}).encode("utf-8")),
        ]
        with patch.object(health, "command", side_effect=fake_command), \
             patch.object(health, "napcat_online", return_value=True), \
             patch.object(health, "build_opener", return_value=base_opener), \
             patch.object(health, "matrix_egress_ok", side_effect=TimeoutError("secret proxy detail")):
            issues, _ = health.collect(Path("/unused"), {}, 10000.0)

        self.assertIn("matrix_egress", issues)
        issue = issues["matrix_egress"]
        self.assertEqual((issue.level, issue.samples), (health.CRITICAL, 2))
        self.assertIn("TimeoutError", issue.message)
        self.assertNotIn("secret proxy detail", issue.message)

        # Two consecutive failures wake @csbot
        self.cycle({"matrix_egress": issue}, 10000)
        self.sender.assert_not_called()
        self.cycle({"matrix_egress": issue}, 10060)
        self.assertEqual(self.sender.call_count, 1)
        self.assertTrue(self.sender.call_args.kwargs["wake_supervisor"])
        self.assertIn("@csbot", self.sender.call_args.args[0])


if __name__ == "__main__":
    unittest.main()
