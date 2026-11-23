#!/usr/bin/env python3
"""Local checks for the OpenAI -> deepseek-flash fallback and circuit breaker.

Pure standard library + repository modules; no network egress, no real keys,
no bot startup, no database. Windows and Linux both run this file.
"""
from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile
import threading
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from ai_runtime.openai_support import (
    DEFAULT_OPENAI_CONNECT_TIMEOUT_MS,
    DEFAULT_OPENAI_FALLBACK_CONTEXT_WINDOW,
    DEFAULT_OPENAI_FALLBACK_COOLDOWN_SECONDS,
    OpenAICircuitBreaker,
    is_openai_fallback_eligible,
    openai_circuit_breaker,
    resolve_openai_connect_timeout_ms,
    resolve_openai_fallback,
    resolve_openai_fallback_context_window,
    resolve_openai_fallback_cooldown_seconds,
    resolve_openai_fallback_fail_threshold,
)
from ai_runtime.runner import render_profile, run_dsh
from ai_runtime.usage import normalize_dsh_usage

REPO = Path(__file__).resolve().parents[1]
BRIDGE = REPO / "ai_runtime/dsh/bridge.mjs"


class _EnvMixin(unittest.TestCase):
    BASE_ENV = {
        "CS_AI_PROVIDER": "",
        "CS_AI_OPENAI_FALLBACK_ENABLE": "1",
        "CS_AI_OPENAI_FALLBACK_MODEL": "",
        "CS_AI_OPENAI_FALLBACK_CONTEXT_WINDOW": "",
        "CS_AI_OPENAI_FALLBACK_COOLDOWN_SECONDS": "",
        "CS_AI_OPENAI_FALLBACK_FAIL_THRESHOLD": "",
        "CS_AI_OPENAI_CONNECT_TIMEOUT_MS": "",
        "CS_AI_FALLBACK_URL": "",
        "CS_AI_FALLBACK_API_KEY": "",
        "CS_AI_AUX_ENABLE": "1",
        "CS_AI_AUX_URL": "",
        "CS_AI_AUX_API_KEY": "",
        "CS_AI_AUX_MODEL": "",
        "CS_AI_AUX_CONTEXT_WINDOW": "",
        "CS_AI_SEARCH_URL": "",
        "CS_AI_SEARCH_API_KEY": "",
    }

    def env(self, **overrides):
        values = dict(self.BASE_ENV)
        values.update(overrides)
        return patch.dict(os.environ, values, clear=False)


class FallbackRouteTest(_EnvMixin):
    def test_only_openai_endpoints_are_armed(self):
        with self.env(CS_AI_SEARCH_API_KEY="ds-search"):
            self.assertIsNone(resolve_openai_fallback("https://api.deepseek.com"))
            self.assertIsNone(
                resolve_openai_fallback("https://api.deepseek.com", provider_override="deepseek-official")
            )
            self.assertIsNotNone(resolve_openai_fallback("https://api.openai.com/v1"))

    def test_disable_switch_and_missing_credentials(self):
        with self.env(CS_AI_SEARCH_API_KEY="ds-search", CS_AI_OPENAI_FALLBACK_ENABLE="0"):
            self.assertIsNone(resolve_openai_fallback("https://api.openai.com/v1"))
        with self.env():
            self.assertIsNone(resolve_openai_fallback("https://api.openai.com/v1"))

    def test_official_search_key_is_reused_only_on_its_own_origin(self):
        for search_url in ("", "https://api.deepseek.com/anthropic/v1"):
            with self.subTest(search_url=search_url), self.env(
                CS_AI_SEARCH_API_KEY="ds-search", CS_AI_SEARCH_URL=search_url
            ):
                route = resolve_openai_fallback("https://api.openai.com/v1")
                self.assertIsNotNone(route)
                self.assertEqual(route["provider"], "deepseek-official")
                self.assertEqual(route["ledger_provider"], "deepseek")
                self.assertEqual(route["base_url"], "https://api.deepseek.com")
                self.assertEqual(route["api_key"], "ds-search")
                self.assertEqual(route["model"], "deepseek-flash")
                self.assertEqual(route["context_window"], DEFAULT_OPENAI_FALLBACK_CONTEXT_WINDOW)
        with self.env(
            CS_AI_SEARCH_API_KEY="ds-search",
            CS_AI_SEARCH_URL="https://other.example/anthropic/v1",
        ):
            self.assertIsNone(resolve_openai_fallback("https://api.openai.com/v1"))
        for bad_url in ("http://127.0.0.1:18080", "https://gateway.example.com/v1"):
            with self.subTest(bad_url=bad_url), self.env(
                CS_AI_SEARCH_API_KEY="ds-search", CS_AI_FALLBACK_URL=bad_url
            ):
                with self.assertRaises(ValueError):
                    resolve_openai_fallback("https://api.openai.com/v1")
        with self.env(
            CS_AI_SEARCH_API_KEY="ds-search", CS_AI_AUX_URL="https://gateway.example.com/v1"
        ):
            with self.assertRaises(ValueError):
                resolve_openai_fallback("https://api.openai.com/v1")

    def test_explicit_fallback_key_wins_over_aux(self):
        with self.env(
            CS_AI_FALLBACK_API_KEY="ds-fallback",
            CS_AI_FALLBACK_URL="https://api.deepseek.com/v1",
            CS_AI_AUX_API_KEY="ds-aux",
            CS_AI_AUX_URL="http://127.0.0.1:18080",
        ):
            route = resolve_openai_fallback("https://api.openai.com/v1")
        self.assertEqual(route["api_key"], "ds-fallback")
        self.assertEqual(route["base_url"], "https://api.deepseek.com")

    def test_aux_key_and_url_precedence(self):
        with self.env(CS_AI_AUX_API_KEY="ds-aux", CS_AI_AUX_URL="http://127.0.0.1:18080"):
            route = resolve_openai_fallback("https://api.openai.com/v1")
            self.assertEqual(route["base_url"], "http://127.0.0.1:18080")
            self.assertEqual(route["api_key"], "ds-aux")
        with self.env(
            CS_AI_AUX_API_KEY="ds-aux",
            CS_AI_AUX_URL="http://127.0.0.1:18080",
            CS_AI_FALLBACK_URL="http://127.0.0.1:18081",
        ):
            route = resolve_openai_fallback("https://api.openai.com/v1")
            self.assertEqual(route["base_url"], "http://127.0.0.1:18081")

    def test_fallback_stays_enabled_when_aux_channel_is_off(self):
        with self.env(
            CS_AI_SEARCH_API_KEY="ds-search",
            CS_AI_AUX_ENABLE="0",
        ):
            self.assertIsNotNone(resolve_openai_fallback("https://api.openai.com/v1"))

    def test_model_and_context_overrides(self):
        with self.env(CS_AI_SEARCH_API_KEY="ds-search", CS_AI_OPENAI_FALLBACK_MODEL="deepseek-pro"):
            self.assertEqual(
                resolve_openai_fallback("https://api.openai.com/v1")["model"], "deepseek-pro"
            )
        with self.env(
            CS_AI_SEARCH_API_KEY="ds-search", CS_AI_AUX_MODEL="deepseek-aux-model"
        ):
            self.assertEqual(
                resolve_openai_fallback("https://api.openai.com/v1")["model"],
                "deepseek-aux-model",
            )
        with self.env(
            CS_AI_SEARCH_API_KEY="ds-search",
            CS_AI_OPENAI_FALLBACK_CONTEXT_WINDOW="131072",
        ):
            self.assertEqual(
                resolve_openai_fallback("https://api.openai.com/v1")["context_window"], 131072
            )
        with self.env(
            CS_AI_SEARCH_API_KEY="ds-search",
            CS_AI_OPENAI_FALLBACK_CONTEXT_WINDOW="not-a-number",
        ):
            self.assertEqual(resolve_openai_fallback_context_window(), 65536)
        with self.env(
            CS_AI_SEARCH_API_KEY="ds-search", CS_AI_AUX_CONTEXT_WINDOW="32768"
        ):
            self.assertEqual(resolve_openai_fallback_context_window(), 32768)

    def test_route_dict_never_carries_extra_secrets(self):
        with self.env(CS_AI_FALLBACK_API_KEY="ds-fallback"):
            route = resolve_openai_fallback("https://api.openai.com/v1")
        self.assertEqual(
            set(route),
            {"provider", "ledger_provider", "base_url", "api_key", "model", "context_window"},
        )


class EligibilityTest(unittest.TestCase):
    ALLOW = (
        "TRANSPORT", "TIMEOUT", "SERVER", "RATE_LIMIT", "QUOTA", "AUTH",
        "EMPTY_RESPONSE", "STREAM_CLOSED",
    )
    DENY = (
        "INVALID_REQUEST", "CONTEXT_WINDOW_EXCEEDED", "ABORTED",
        "UNSUPPORTED_CONTENT", "UNSUPPORTED_REASONING_EFFORT", "NO_ADAPTER",
        "INVALID_CREDENTIAL", "INVALID_PREPARED_CALL",
    )

    def test_allow_and_deny_matrices(self):
        for code in self.ALLOW:
            with self.subTest(code=code):
                self.assertTrue(is_openai_fallback_eligible(code))
                self.assertTrue(is_openai_fallback_eligible(code.lower()))
        for code in self.DENY:
            with self.subTest(code=code):
                self.assertFalse(is_openai_fallback_eligible(code))
                self.assertFalse(
                    is_openai_fallback_eligible(code, "proxy ECONNREFUSED 503")
                )

    def test_unknown_codes_need_transport_evidence(self):
        for message in (
            "Proxy connection refused",
            "connect ECONNREFUSED 127.0.0.1:7890",
            "request timed out after 15s",
            "socket hang up",
            "TLS reset by peer",
            "fetch failed",
            "upstream returned 503",
            "429 Too Many Requests",
            "403 Forbidden from Cloudflare",
            "Bad Gateway",
            "service unavailable",
            "gateway timeout",
            "overloaded",
            "rate-limit exceeded",
        ):
            with self.subTest(message=message):
                self.assertTrue(is_openai_fallback_eligible("PI_AI_ERROR", message))
                self.assertTrue(is_openai_fallback_eligible("", message))
                self.assertTrue(is_openai_fallback_eligible("UNKNOWN", message))
        for message in (
            "invalid value for temperature",
            "unknown model gpt-5",
            "tool schema rejected",
            "content policy violation",
            "",
        ):
            with self.subTest(message=message):
                self.assertFalse(is_openai_fallback_eligible("PI_AI_ERROR", message))
                self.assertFalse(is_openai_fallback_eligible("", message))


class _FakeClock:
    def __init__(self, start: float = 0.0) -> None:
        self.now = start

    def monotonic(self) -> float:
        return self.now

    def wall(self) -> float:
        return 1_700_000_000.0 + self.now


class CircuitBreakerTest(unittest.TestCase):
    def make(self, **kwargs) -> tuple[OpenAICircuitBreaker, _FakeClock]:
        clock = _FakeClock()
        breaker = OpenAICircuitBreaker(
            monotonic_fn=clock.monotonic, wall_fn=clock.wall, **kwargs
        )
        return breaker, clock

    def test_closed_state_primary(self):
        breaker, _ = self.make()
        decision = breaker.acquire()
        self.assertEqual(decision, {
            "mode": "primary", "state": "closed", "lease_id": None,
            "remaining_cooldown_seconds": 0.0, "reason_code": "",
        })
        self.assertEqual(breaker.snapshot()["state"], "closed")

    def test_trips_at_threshold_one_and_two(self):
        breaker, _ = self.make(cooldown_seconds=60)
        result = breaker.record_failure(code="TRANSPORT", reason="proxy down")
        self.assertTrue(result["tripped"])
        self.assertEqual(result["state"], "open")
        self.assertEqual(breaker.snapshot()["trip_count"], 1)

        breaker, _ = self.make(cooldown_seconds=60, fail_threshold=2)
        first = breaker.record_failure(code="SERVER")
        self.assertFalse(first["tripped"])
        self.assertEqual(breaker.snapshot()["state"], "closed")
        second = breaker.record_failure(code="SERVER")
        self.assertTrue(second["tripped"])
        self.assertEqual(breaker.snapshot()["consecutive_failures"], 2)

    def test_open_window_short_circuits_and_counts_down(self):
        breaker, clock = self.make(cooldown_seconds=30)
        breaker.record_failure(code="TIMEOUT")
        first = breaker.acquire()
        self.assertEqual(first["mode"], "fallback")
        self.assertEqual(first["state"], "open")
        self.assertEqual(first["reason_code"], "TIMEOUT")
        self.assertGreater(first["remaining_cooldown_seconds"], 29.0)
        clock.now = 10.0
        second = breaker.acquire()
        self.assertAlmostEqual(second["remaining_cooldown_seconds"], 20.0)
        self.assertEqual(breaker.snapshot()["fallback_count"], 3)

    def test_single_half_open_probe(self):
        breaker, clock = self.make(cooldown_seconds=30)
        breaker.record_failure(code="SERVER")
        clock.now = 30.0
        probe = breaker.acquire()
        self.assertEqual(probe["mode"], "probe")
        self.assertEqual(probe["state"], "half_open")
        self.assertTrue(probe["lease_id"])
        concurrent = breaker.acquire()
        self.assertEqual(concurrent["mode"], "fallback")
        self.assertEqual(concurrent["state"], "half_open")
        self.assertEqual(concurrent["reason_code"], "SERVER")
        self.assertIsNone(concurrent["lease_id"])

    def test_probe_success_closes_the_breaker(self):
        breaker, clock = self.make(cooldown_seconds=30)
        breaker.record_failure(code="SERVER")
        clock.now = 30.0
        lease = breaker.acquire()["lease_id"]
        self.assertFalse(breaker.record_success(lease_id="someone-else")["recovered"])
        transition = breaker.record_success(lease_id=lease)
        self.assertTrue(transition["recovered"])
        snapshot = breaker.snapshot()
        self.assertEqual(snapshot["state"], "closed")
        self.assertEqual(snapshot["consecutive_failures"], 0)
        self.assertFalse(snapshot["probe_in_flight"])
        self.assertEqual(snapshot["recovery_count"], 1)
        self.assertEqual(breaker.acquire()["mode"], "primary")

    def test_probe_failure_reopens_even_below_threshold(self):
        breaker, clock = self.make(cooldown_seconds=30, fail_threshold=5)
        breaker.record_failure(code="SERVER")
        breaker.record_failure(code="SERVER")
        breaker.record_failure(code="SERVER")
        breaker.record_failure(code="SERVER")
        breaker.record_failure(code="SERVER")
        clock.now = 30.0
        lease = breaker.acquire()["lease_id"]
        result = breaker.record_failure(code="RATE_LIMIT", lease_id=lease)
        self.assertTrue(result["tripped"])
        self.assertEqual(result["state"], "open")
        self.assertEqual(breaker.snapshot()["trip_count"], 2)
        self.assertEqual(breaker.snapshot()["remaining_cooldown_seconds"], 30.0)

    def test_release_probe_never_deadlocks_half_open(self):
        breaker, clock = self.make(cooldown_seconds=30)
        breaker.record_failure(code="SERVER")
        clock.now = 30.0
        lease = breaker.acquire()["lease_id"]
        breaker.release_probe("stale-lease")
        self.assertTrue(breaker.snapshot()["probe_in_flight"])
        breaker.release_probe(lease)
        snapshot = breaker.snapshot()
        self.assertFalse(snapshot["probe_in_flight"])
        self.assertEqual(snapshot["state"], "half_open")
        self.assertEqual(snapshot["trip_count"], 1)
        self.assertEqual(breaker.acquire()["mode"], "probe")

    def test_reason_is_redacted_whitespace_collapsed_and_truncated(self):
        breaker, _ = self.make()
        result = breaker.record_failure(
            code="SERVER",
            reason="  upstream   failed  with sk-secret-key\nand ds-secret  ",
            secrets=("sk-secret-key", "ds-secret"),
        )
        self.assertEqual(result["reason"], "upstream failed with [REDACTED] and [REDACTED]")
        snapshot = breaker.snapshot()
        self.assertNotIn("sk-secret-key", json.dumps(snapshot))
        self.assertNotIn("ds-secret", json.dumps(snapshot))
        long_reason = breaker.record_failure(code="SERVER", reason="x" * 500)["reason"]
        self.assertEqual(len(long_reason), 160)

    def test_cooldown_zero_keeps_turns_primary(self):
        breaker, _ = self.make(cooldown_seconds=0)
        result = breaker.record_failure(code="SERVER")
        self.assertFalse(result["tripped"])
        self.assertEqual(breaker.snapshot()["state"], "closed")
        self.assertEqual(breaker.acquire()["mode"], "primary")
        self.assertEqual(breaker.snapshot()["trip_count"], 0)

    def test_snapshot_has_no_secret_fields(self):
        breaker, _ = self.make()
        breaker.record_failure(code="SERVER", reason="Bearer sk-abc", secrets=("sk-abc",))
        snapshot = breaker.snapshot()
        self.assertEqual(
            set(snapshot),
            {
                "state", "cooldown_seconds", "fail_threshold", "consecutive_failures",
                "probe_in_flight", "remaining_cooldown_seconds", "last_failure_code",
                "last_failure_reason", "last_failure_at", "last_recovery_at",
                "trip_count", "fallback_count", "recovery_count",
            },
        )
        self.assertNotIn("sk-abc", json.dumps(snapshot))

    def test_reset_returns_to_clean_closed(self):
        breaker, _ = self.make(cooldown_seconds=30)
        breaker.record_failure(code="SERVER")
        breaker.acquire()
        breaker.reset()
        snapshot = breaker.snapshot()
        self.assertEqual(snapshot["state"], "closed")
        self.assertEqual(snapshot["trip_count"], 0)
        self.assertEqual(snapshot["fallback_count"], 0)
        self.assertEqual(snapshot["last_failure_code"], "")
        self.assertEqual(breaker.acquire()["mode"], "primary")

    def test_concurrent_acquire_and_record_are_consistent(self):
        breaker, clock = self.make(cooldown_seconds=5, fail_threshold=2)
        errors: list[BaseException] = []
        barrier = threading.Barrier(8)

        def worker(index: int) -> None:
            try:
                barrier.wait(timeout=5)
                for step in range(200):
                    decision = breaker.acquire()
                    if decision["mode"] == "primary":
                        if step % 3 == 0:
                            breaker.record_failure(code="SERVER", reason="boom")
                        else:
                            breaker.record_success(lease_id=decision["lease_id"])
                    elif decision["mode"] == "probe":
                        if index % 2:
                            breaker.record_failure(code="TIMEOUT", lease_id=decision["lease_id"])
                        else:
                            breaker.record_success(lease_id=decision["lease_id"])
                    else:
                        breaker.release_probe(decision["lease_id"])
                    if step % 50 == 0:
                        clock.now += 1.0
            except BaseException as exc:  # pragma: no cover - failure reporting
                errors.append(exc)

        threads = [threading.Thread(target=worker, args=(index,)) for index in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=30)
        self.assertEqual(errors, [])
        snapshot = breaker.snapshot()
        self.assertIn(snapshot["state"], {"closed", "open", "half_open"})
        if snapshot["state"] != "half_open":
            self.assertFalse(snapshot["probe_in_flight"])

    def test_singleton_is_shared(self):
        self.assertIs(openai_circuit_breaker(), openai_circuit_breaker())

    def test_env_configuration_resolvers(self):
        with patch.dict(os.environ, {"CS_AI_OPENAI_FALLBACK_COOLDOWN_SECONDS": ""}, clear=False):
            self.assertEqual(
                resolve_openai_fallback_cooldown_seconds(),
                DEFAULT_OPENAI_FALLBACK_COOLDOWN_SECONDS,
            )
        with patch.dict(os.environ, {"CS_AI_OPENAI_FALLBACK_COOLDOWN_SECONDS": "-5"}):
            self.assertEqual(resolve_openai_fallback_cooldown_seconds(), 0.0)
        with patch.dict(os.environ, {"CS_AI_OPENAI_FALLBACK_COOLDOWN_SECONDS": "abc"}):
            self.assertEqual(
                resolve_openai_fallback_cooldown_seconds(),
                DEFAULT_OPENAI_FALLBACK_COOLDOWN_SECONDS,
            )
        with patch.dict(os.environ, {"CS_AI_OPENAI_FALLBACK_FAIL_THRESHOLD": "0"}):
            self.assertEqual(resolve_openai_fallback_fail_threshold(), 1)
        with patch.dict(os.environ, {"CS_AI_OPENAI_FALLBACK_FAIL_THRESHOLD": "3"}):
            self.assertEqual(resolve_openai_fallback_fail_threshold(), 3)
        with patch.dict(os.environ, {"CS_AI_OPENAI_CONNECT_TIMEOUT_MS": "10"}):
            self.assertEqual(resolve_openai_connect_timeout_ms(), 1_000)
        with patch.dict(os.environ, {"CS_AI_OPENAI_CONNECT_TIMEOUT_MS": "999999"}):
            self.assertEqual(resolve_openai_connect_timeout_ms(), 60_000)
        with patch.dict(os.environ, {"CS_AI_OPENAI_CONNECT_TIMEOUT_MS": ""}):
            self.assertEqual(resolve_openai_connect_timeout_ms(), DEFAULT_OPENAI_CONNECT_TIMEOUT_MS)


class ProbeLeaseReleaseTest(unittest.TestCase):
    """A failure before the live-process slot must still free the probe lease."""

    @staticmethod
    async def _no_rpc(request):
        raise AssertionError(f"unexpected host RPC: {request.get('method')}")

    def test_pre_slot_failure_releases_probe_lease(self):
        clock = _FakeClock()
        breaker = OpenAICircuitBreaker(cooldown_seconds=10, monotonic_fn=clock.monotonic)
        breaker.record_failure(code="SERVER")
        clock.now = 10.0  # cooldown elapsed: the next acquire() hands out the probe
        env = {
            "CS_AI_PROVIDER": "openai",
            "CS_AI_OPENAI_FALLBACK_ENABLE": "1",
            "CS_AI_FALLBACK_API_KEY": "ds-probe-fixture",
            "CS_AI_AUX_ENABLE": "0",
        }
        with (
            patch.dict(os.environ, env, clear=False),
            patch("ai_runtime.runner.source_snapshot", side_effect=ValueError("fixture abort")),
            tempfile.TemporaryDirectory() as tmp,
        ):
            with self.assertRaisesRegex(ValueError, "fixture abort"):
                asyncio.run(run_dsh(
                    scope="probe-release", text="hi", context="", model="gpt-6.2-sol",
                    endpoint="https://api.openai.com/v1", api_key="sk-probe-fixture",
                    dispatch=self._no_rpc, remember=False, state_root=Path(tmp),
                    circuit_breaker=breaker,
                ))
        snapshot = breaker.snapshot()
        self.assertFalse(snapshot["probe_in_flight"])
        self.assertEqual(snapshot["state"], "half_open")
        # The lease is free again, so the following turn can still probe.
        self.assertEqual(breaker.acquire()["mode"], "probe")


class RenderProfileFallbackTest(unittest.TestCase):
    BASE = {
        "CSBOT_PROVIDER": "openai",
        "CSBOT_REMEMBER": "0",
        "CSBOT_MODEL": "gpt-6.2-sol",
        "DSH_CONTEXT_WINDOW": "131072",
        "CSBOT_VISION": "1",
        "CSBOT_THINKING": "1",
        "CSBOT_WEB_ENABLED": "0",
        "CSBOT_OPENAI_BASE_URL": "https://api.openai.com/v1",
        "DEEPSEEK_API_KEY": "ds-test",
    }

    def profile(self, **overrides):
        env = dict(self.BASE)
        env.update(overrides)
        return render_profile(env)

    @staticmethod
    def plugin(profile, ident):
        for entry in profile:
            if not isinstance(entry, dict):
                continue
            if entry.get("id") == ident:
                return entry
            for item in entry.get("insert") or []:
                if item.get("id") == ident:
                    return item
        return None

    def pi_ai(self, profile):
        item = self.plugin(profile, "llm-pi-ai")
        return None if item is None else item["config"]["providers"]["openai"]

    def test_armed_closed_state_sets_one_fast_retry(self):
        profile = self.profile(
            CSBOT_FALLBACK_PROVIDER="deepseek-official",
            CSBOT_FALLBACK_MODEL="deepseek-flash",
            CSBOT_FALLBACK_CONTEXT_WINDOW="65536",
        )
        provider = self.pi_ai(profile)
        self.assertEqual(
            provider["retryPolicy"],
            {
                "mode": "normal",
                "maxRetries": 1,
                "retryableCodes": ["EMPTY_RESPONSE", "SERVER"],
                "backoff": {"initialDelayMs": 500, "maxDelayMs": 1000, "jitterRatio": 0.1},
            },
        )
        llm = self.plugin(profile, "llm-deepseek")
        self.assertNotIn("disabled", llm)
        self.assertEqual(
            llm["config"]["models"],
            [{
                "id": "deepseek-flash",
                "contextWindow": 65536,
                "maxTokens": 8192,
                "inputModalities": ["text"],
            }],
        )
        self.assertNotIn("summarizeProvider", self.plugin(profile, "mneme")["config"])

    def test_probe_state_disables_native_retries(self):
        profile = self.profile(
            CSBOT_FALLBACK_PROVIDER="deepseek-official",
            CSBOT_FALLBACK_MODEL="deepseek-flash",
            CSBOT_OPENAI_PROBE="1",
        )
        self.assertEqual(self.pi_ai(profile)["retryPolicy"], {"mode": "normal", "maxRetries": 0})

    def test_aux_and_fallback_models_are_deduplicated(self):
        profile = self.profile(
            CSBOT_AUX_PROVIDER="deepseek-official",
            CSBOT_AUX_MODEL="deepseek-flash",
            CSBOT_AUX_CONTEXT_WINDOW="32768",
            CSBOT_FALLBACK_PROVIDER="deepseek-official",
            CSBOT_FALLBACK_MODEL="deepseek-flash",
            CSBOT_FALLBACK_CONTEXT_WINDOW="65536",
        )
        llm = self.plugin(profile, "llm-deepseek")
        self.assertEqual(len(llm["config"]["models"]), 1)
        self.assertEqual(llm["config"]["models"][0]["contextWindow"], 32768)
        mneme = self.plugin(profile, "mneme")["config"]
        self.assertEqual(mneme["summarizeProvider"], "deepseek-official")
        self.assertEqual(mneme["summarizeModel"], "deepseek-flash")
        self.assertEqual(self.pi_ai(profile)["retryPolicy"]["maxRetries"], 1)

    def test_fallback_only_does_not_change_mneme(self):
        profile = self.profile(
            CSBOT_AUX_PROVIDER="", CSBOT_AUX_MODEL="",
            CSBOT_FALLBACK_PROVIDER="deepseek-official",
            CSBOT_FALLBACK_MODEL="deepseek-flash",
        )
        mneme = self.plugin(profile, "mneme")["config"]
        self.assertNotIn("summarizeProvider", mneme)
        self.assertNotIn("summarizeModel", mneme)
        self.assertFalse(self.plugin(profile, "llm-deepseek").get("disabled", False))

    def test_without_arming_nothing_changes(self):
        profile = self.profile()
        self.assertTrue(self.plugin(profile, "llm-deepseek")["disabled"])
        self.assertNotIn("retryPolicy", self.pi_ai(profile))

    def test_circuit_open_state_has_no_openai_provider(self):
        env = {
            "CSBOT_PROVIDER": "deepseek-official",
            "CSBOT_REMEMBER": "0",
            "CSBOT_MODEL": "deepseek-flash",
            "DSH_CONTEXT_WINDOW": "65536",
            "CSBOT_VISION": "0",
            "CSBOT_THINKING": "1",
            "CSBOT_WEB_ENABLED": "0",
            "CSBOT_FALLBACK_ACTIVE": "1",
            "CSBOT_PRIMARY_MODEL": "gpt-6.2-sol",
            "DEEPSEEK_API_KEY": "ds-test",
        }
        profile = render_profile(env)
        self.assertIsNone(self.plugin(profile, "llm-pi-ai"))
        llm = self.plugin(profile, "llm-deepseek")
        self.assertNotIn("disabled", llm)
        self.assertEqual(
            llm["config"]["models"],
            [{
                "id": "deepseek-flash",
                "contextWindow": 65536,
                "maxTokens": 8192,
                "inputModalities": ["text"],
            }],
        )


class BridgeStaticTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.source = BRIDGE.read_text(encoding="utf-8")

    def test_syntax_and_usage_tap_count(self):
        node = shutil.which("node")
        if not node:  # pragma: no cover - delivery machines ship Node
            self.skipTest("node is not installed")
        result = subprocess.run(
            [node, "--check", str(BRIDGE)], capture_output=True, text=True
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.source.count("tapUsage(next())"), 3)

    def test_fallback_markers_are_wired(self):
        for marker in (
            "isFallbackEligibleFailure",
            "activateFallback",
            "agent/request-error",
            "'agent/request'",
            "CSBOT_FALLBACK_ACTIVE",
            "CSBOT_FALLBACK_PROVIDER",
            "CSBOT_FALLBACK_MODEL",
            "csbot-fallback-status",
            "type: 'fallback'",
        ):
            with self.subTest(marker=marker):
                self.assertIn(marker, self.source)

    def test_connect_timeout_comes_from_the_host(self):
        self.assertIn("CSBOT_OPENAI_CONNECT_TIMEOUT_MS", self.source)
        self.assertIsNotNone(re.search(r"connectTimeout\s*[,}]", self.source))
        self.assertNotIn("connectTimeout:15000", self.source)

    def test_eligibility_matrix_matches_python(self):
        deny = re.search(r"FALLBACK_DENY_CODES = new Set\(\[(.*?)\]\)", self.source, re.S)
        allow = re.search(r"FALLBACK_ALLOW_CODES = new Set\(\[(.*?)\]\)", self.source, re.S)
        self.assertIsNotNone(deny)
        self.assertIsNotNone(allow)
        for code in ("INVALID_REQUEST", "CONTEXT_WINDOW_EXCEEDED", "ABORTED",
                     "UNSUPPORTED_CONTENT", "UNSUPPORTED_REASONING_EFFORT", "NO_ADAPTER",
                     "INVALID_CREDENTIAL", "INVALID_PREPARED_CALL"):
            self.assertIn(f"'{code}'", deny.group(1))
        for code in ("TRANSPORT", "TIMEOUT", "SERVER", "RATE_LIMIT", "QUOTA", "AUTH",
                     "EMPTY_RESPONSE", "STREAM_CLOSED"):
            self.assertIn(f"'{code}'", allow.group(1))

    def test_no_hardcoded_secret_material(self):
        self.assertNotRegex(self.source, r"sk-[A-Za-z0-9]{16,}")
        self.assertNotIn("Bearer ", self.source)


class BillingAlignmentTest(unittest.TestCase):
    def item(self, *, scene: str, peak: bool) -> dict:
        event = {
            "provider": "deepseek",
            "model": "deepseek-flash",
            "scene": scene,
            "uncached_input_tokens": 1000,
            "cached_tokens": 500,
            "cache_write_tokens": 0,
            "output_tokens": 200,
            "reasoning_tokens": 0,
        }
        with patch("ai_runtime.usage._beijing_peak", return_value=peak):
            return normalize_dsh_usage(event)

    def test_all_scenes_are_billed_as_deepseek(self):
        for scene in ("chat", "birthday_wish", "compaction", "memory_extract"):
            for peak in (True, False):
                with self.subTest(scene=scene, peak=peak):
                    item = self.item(scene=scene, peak=peak)
                    self.assertEqual(item["provider"], "deepseek")
                    self.assertEqual(item["model"], "deepseek-flash")
                    self.assertEqual(item["scene"], scene)
                    self.assertEqual(item["input_tokens"], 1500)
                    self.assertEqual(
                        item["input_tokens"],
                        item["uncached_input_tokens"]
                        + item["cached_tokens"]
                        + item["cache_write_tokens"],
                    )
                    expected_cost = 0.000543 if peak else 0.0002715
                    expected_nocache = 0.00069 if peak else 0.000345
                    self.assertAlmostEqual(item["cost_usd"], expected_cost, places=8)
                    self.assertAlmostEqual(item["nocache_cost_usd"], expected_nocache, places=8)

    def test_deepseek_official_alias_is_folded(self):
        with patch("ai_runtime.usage._beijing_peak", return_value=True):
            item = normalize_dsh_usage({
                "provider": "deepseek-official",
                "model": "deepseek-flash",
                "scene": "chat",
                "uncached_input_tokens": 1000,
                "cached_tokens": 500,
                "output_tokens": 200,
            })
        self.assertEqual(item["provider"], "deepseek")
        self.assertAlmostEqual(item["cost_usd"], 0.000543, places=8)


if __name__ == "__main__":
    unittest.main()
