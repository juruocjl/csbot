#!/usr/bin/env python3
"""Health alert policy and delivery checks; no production services or messages."""
import importlib.util
import json
from pathlib import Path
import stat
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch

spec = importlib.util.spec_from_file_location("health_alert", Path(__file__).parents[1] / "scripts/health_alert.py")
health = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = health
spec.loader.exec_module(health)


class AlertTests(unittest.TestCase):
    def setUp(self):
        self.state = {}
        self.people = ["10001"]
        self.sender = Mock()

    def cycle(self, issues, at, people=None):
        people = people or self.people
        events = health.notifications(issues, self.state, people, at)
        health.deliver(events, self.state, people, at, self.sender)
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

    def test_confirmation_cooldown_and_escalation(self):
        issues = {"disk": health.Issue(health.WARNING, "disk warning")}
        self.cycle(issues, 10000)
        self.sender.assert_not_called()
        self.cycle(issues, 10060)
        self.assertEqual(self.sender.call_count, 1)
        self.cycle(issues, 10120)
        self.assertEqual(self.sender.call_count, 1)
        self.cycle({"disk": health.Issue(health.CRITICAL, "disk full", 1)}, 10180)
        self.assertEqual(self.sender.call_count, 2)
        self.cycle({"disk": health.Issue(health.CRITICAL, "disk full", 1)}, 13780)
        self.assertEqual(self.sender.call_count, 3)

    def test_transient_warning_does_not_send_recovery(self):
        self.cycle({"memory": health.Issue(health.WARNING, "low")}, 10000)
        self.cycle({}, 10060)
        self.cycle({}, 10120)
        self.sender.assert_not_called()
        self.assertEqual(self.state["issues"], {})

    def test_two_healthy_samples_send_one_recovery(self):
        self.cycle({"disk": health.Issue(health.CRITICAL, "full", 1)}, 10000)
        self.cycle({}, 10060)
        self.assertEqual(self.sender.call_count, 1)
        self.cycle({}, 10120)
        self.assertEqual(self.sender.call_count, 2)
        self.assertIn("[恢复]", self.sender.call_args.args[1])
        self.cycle({}, 10180)
        self.assertEqual(self.sender.call_count, 2)

    def test_unreachable_channel_retains_incident_until_recovered_notice(self):
        events = health.notifications({"qq": health.Issue(health.CRITICAL, "offline", 1)}, self.state, self.people, 10000)
        self.assertTrue(events[self.people[0]])
        health.notifications({}, self.state, self.people, 10060)
        self.cycle({}, 10120)
        self.assertIn("异常期间未能送达", self.sender.call_args.args[1])
        self.assertEqual(self.state["issues"], {})

    def test_failed_recipient_does_not_repeat_successful_delivery(self):
        people = ["10001", "10002"]
        def send(recipient, message):
            if recipient == "10002":
                raise TimeoutError("sensitive response must not be logged")
        self.sender.side_effect = send
        issue = {"disk": health.Issue(health.CRITICAL, "full", 1)}
        with patch.object(health, "emit") as emit:
            self.cycle(issue, 10000, people)
            self.cycle(issue, 10060, people)
            self.assertEqual(self.sender.call_count, 2)
            self.cycle(issue, 10300, people)
            self.assertEqual(self.sender.call_count, 3)
            self.assertEqual(self.sender.call_args.args[0], "10002")
            self.assertNotIn("sensitive response", str(emit.call_args_list))

    def test_notification_length_does_not_acknowledge_unsent_items(self):
        issues = {f"item-{i:02}": health.Issue(health.CRITICAL, "x" * 200, 1) for i in range(30)}
        self.cycle(issues, 10000)
        first = sum(bool(v["notified"]) for v in self.state["issues"].values())
        self.assertLess(first, 30)
        self.assertLessEqual(len(self.sender.call_args.args[1]), 3500)
        self.cycle(issues, 10060)
        self.assertEqual(sum(bool(v["notified"]) for v in self.state["issues"].values()), 30)

    def test_napcat_send_only_explicit_recipient_and_require_ack(self):
        client = object.__new__(health.NapCat)
        client.post = Mock(return_value={"status": "ok", "retcode": 0, "data": {"message_id": 42}})
        client.send("10001", "test")
        args = client.post.call_args.args
        self.assertEqual(args[0], "Debug/call")
        self.assertEqual(args[1]["params"]["user_id"], 10001)
        for response in ({}, {"retcode": 1}, {"status": "ok", "retcode": 0, "data": {}}):
            client.post.return_value = response
            with self.assertRaises(RuntimeError):
                client.send("10001", "test")

    def test_dry_run_never_delivers_or_updates_state(self):
        state = {"existing": "unchanged"}
        with patch.object(health, "collect", return_value=({"disk": health.Issue(health.CRITICAL, "full", 1)}, {})), \
             patch.object(health, "NapCat") as client, patch.object(health, "emit"):
            health.run(Path("/unused"), self.people, state, True)
            client.return_value.send.assert_not_called()
            self.assertEqual(state, {"existing": "unchanged"})

    def test_state_is_private_and_replaces_atomically(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "state.json"
            health.save_state(path, {"version": 1})
            health.save_state(path, {"version": 2})
            self.assertEqual(json.loads(path.read_text()), {"version": 2})
            self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o600)
            self.assertFalse(path.with_suffix(".tmp").exists())


if __name__ == "__main__":
    unittest.main()
