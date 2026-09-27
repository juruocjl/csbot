"""Safety checks for the production guard; never touches systemd or a database."""

from dataclasses import replace
from datetime import datetime, timezone
import importlib.util
from io import BytesIO
from pathlib import Path
import subprocess
import sys
import unittest
from unittest.mock import patch
from urllib.error import HTTPError
from zoneinfo import ZoneInfo

spec = importlib.util.spec_from_file_location("memory_guard", Path(__file__).resolve().parents[1] / "scripts/memory_guard.py")
guard = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = guard
spec.loader.exec_module(guard)


class MemoryGuardTest(unittest.TestCase):
    def setUp(self):
        self.sample = guard.Sample("active", 800 * guard.MIB, 600 * guard.MIB, 86400,
                                   datetime(2026, 9, 27, 12, tzinfo=ZoneInfo("Asia/Shanghai")))

    def reason(self, **changes):
        return guard.restart_reason(replace(self.sample, **changes))

    def test_memory_threshold_and_host_pressure(self):
        self.assertIsNone(self.reason(memory=1599 * guard.MIB))
        self.assertEqual(self.reason(memory=1600 * guard.MIB), "backend_memory")
        self.assertEqual(self.reason(available=384 * guard.MIB, memory=1024 * guard.MIB), "host_memory_pressure")
        self.assertIsNone(self.reason(available=385 * guard.MIB, memory=1024 * guard.MIB))
        self.assertIsNone(self.reason(available=100 * guard.MIB, memory=1023 * guard.MIB))

    def test_stopped_service_and_cooldown(self):
        self.assertIsNone(self.reason(active="inactive", memory=1800 * guard.MIB))
        self.assertIsNone(self.reason(active="activating", memory=1800 * guard.MIB))
        self.assertIsNone(self.reason(uptime=599, memory=1800 * guard.MIB))
        self.assertEqual(self.reason(uptime=600, memory=1800 * guard.MIB), "backend_memory")

    def test_daily_window_uses_shanghai_and_skips_recent_restarts(self):
        for minute, expected in [(9, None), (10, "daily_maintenance"), (49, "daily_maintenance"), (50, None)]:
            now = self.sample.now.replace(hour=5, minute=minute)
            self.assertEqual(self.reason(now=now), expected)
        utc = datetime(2026, 9, 26, 21, 10, tzinfo=timezone.utc)
        self.assertEqual(self.reason(now=utc), "daily_maintenance")
        self.assertIsNone(self.reason(now=utc, uptime=21599))
        self.assertEqual(self.reason(now=utc, uptime=21600), "daily_maintenance")

    def test_dry_run_cannot_restart(self):
        with patch.object(guard, "sample", return_value=replace(self.sample, memory=1800 * guard.MIB)), \
             patch.object(guard, "emit"), patch.object(guard, "systemctl") as ctl:
            guard.run(True)
            ctl.assert_not_called()

    def test_operator_stop_between_samples_is_respected(self):
        with patch.object(guard, "sample", side_effect=[replace(self.sample, memory=1800 * guard.MIB),
                                                       replace(self.sample, active="inactive")]), \
             patch.object(guard, "emit"), patch.object(guard, "systemctl") as ctl:
            guard.run(False)
            ctl.assert_not_called()

    def test_single_restart_then_health_check_and_after_sample(self):
        before = replace(self.sample, memory=1700 * guard.MIB)
        after = replace(self.sample, uptime=10, memory=700 * guard.MIB)
        with patch.object(guard, "sample", side_effect=[before, before, after]), \
             patch.object(guard, "emit") as emit, patch.object(guard, "systemctl") as ctl, \
             patch.object(guard, "wait_for_backend") as health:
            guard.run(False)
            ctl.assert_called_once_with("try-restart", "csbot.service", timeout=65)
            health.assert_called_once_with()
            self.assertEqual(emit.call_args.args[0], "restart_completed")

    def test_failed_restart_is_not_reported_as_success_or_retried(self):
        before = replace(self.sample, memory=1700 * guard.MIB)
        with patch.object(guard, "sample", return_value=before), patch.object(guard, "emit") as emit, \
             patch.object(guard, "systemctl", side_effect=subprocess.TimeoutExpired("systemctl", 65)) as ctl, \
             patch.object(guard, "wait_for_backend") as health:
            with self.assertRaises(subprocess.TimeoutExpired):
                guard.run(False)
            self.assertEqual(ctl.call_count, 1)
            health.assert_not_called()
            self.assertNotIn("restart_completed", [call.args[0] for call in emit.call_args_list])

    def test_http_404_is_responsive_but_500_is_not(self):
        with patch.object(guard, "systemctl", return_value="active\n"), \
             patch.object(guard, "build_opener") as build, patch.object(guard.time, "sleep"):
            build.return_value.open.side_effect = HTTPError("http://local", 404, "Not found", {}, BytesIO())
            guard.wait_for_backend()
            build.return_value.open.side_effect = HTTPError("http://local", 500, "Error", {}, BytesIO())
            with patch.object(guard.time, "monotonic", side_effect=[0, 0, 46]):
                with self.assertRaises(RuntimeError):
                    guard.wait_for_backend()

    def test_inactive_sample_does_not_require_memory_accounting(self):
        props = "ActiveState=inactive\nMemoryCurrent=[not set]\nActiveEnterTimestampMonotonic=0\n"
        with patch.object(guard, "systemctl", return_value=props):
            self.assertEqual(guard.sample().active, "inactive")

    def test_unknown_memory_accounting_fails_closed(self):
        props = "ActiveState=active\nMemoryCurrent=18446744073709551615\nActiveEnterTimestampMonotonic=1\n"
        with patch.object(guard, "systemctl", return_value=props):
            with self.assertRaises(ValueError):
                guard.sample()


if __name__ == "__main__":
    unittest.main()
