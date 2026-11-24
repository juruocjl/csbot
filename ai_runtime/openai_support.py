"""OpenAI endpoint helpers and the gpt-image-2.6-sunburst image tool.

Kept stateless so both the DSH runner and the QQ output guard can share the same
endpoint/proxy decisions. Only explicit loopback HTTP proxies are accepted, and
no function here ever logs or returns the API key.
"""
from __future__ import annotations

import asyncio
import base64
from io import BytesIO
import math
import os
from pathlib import Path
import re
import threading
import time
from typing import Any
import uuid
from urllib.parse import urlsplit

import httpx
from PIL import Image, UnidentifiedImageError

from .usage import normalize_image_usage

DEFAULT_OPENAI_PROXY = "http://127.0.0.1:7890"
DEFAULT_OPENAI_MODEL = "gpt-6.2-sol"
DEFAULT_IMAGE_MODEL = "gpt-image-2.6-sunburst"
# Domestic direct-dial DeepSeek side channels (guard / memory distill / verify).
# They never traverse the cross-border loopback proxy.
DEFAULT_DEEPSEEK_BASE_URL = "https://api.deepseek.com"
DEFAULT_DEEPSEEK_AUX_MODEL = "deepseek-flash"
# Automatic primary-channel fallback (OpenAI outage -> domestic deepseek-flash).
DEFAULT_OPENAI_FALLBACK_COOLDOWN_SECONDS = 240.0
DEFAULT_OPENAI_FALLBACK_FAIL_THRESHOLD = 1
DEFAULT_OPENAI_CONNECT_TIMEOUT_MS = 10_000
DEFAULT_OPENAI_FALLBACK_CONTEXT_WINDOW = 65_536
_OPENAI_FALLBACK_DENY_CODES = frozenset({
    "INVALID_REQUEST",
    "CONTEXT_WINDOW_EXCEEDED",
    "ABORTED",
    "UNSUPPORTED_CONTENT",
    "UNSUPPORTED_REASONING_EFFORT",
    "NO_ADAPTER",
    "INVALID_CREDENTIAL",
    "INVALID_PREPARED_CALL",
})
_OPENAI_FALLBACK_ALLOW_CODES = frozenset({
    "TRANSPORT",
    "TIMEOUT",
    "SERVER",
    "RATE_LIMIT",
    "QUOTA",
    "AUTH",
    "EMPTY_RESPONSE",
    "STREAM_CLOSED",
})
# Only transport/proxy/5xx/429/403/timeout evidence may promote an unknown code.
_OPENAI_FALLBACK_MESSAGE_RE = re.compile(
    r"(?:proxy|tunnel|econn[a-z]*|etimedout|enotfound|eai_again|ehostunreach|"
    r"enetunreach|epipe|tls|ssl|socket|fetch failed|und_err|timed?\s*out|"
    r"bad gateway|service unavailable|gateway timeout|overloaded|rate.?limit|"
    r"\b5\d\d\b|\b429\b|\b403\b)",
    re.IGNORECASE,
)

IMAGE_SIZES = {"1024x1024", "1536x1024", "1024x1536", "auto"}
IMAGE_QUALITIES = {"low", "medium", "high", "auto"}
IMAGE_BACKGROUNDS = {"auto", "opaque", "transparent"}
_RETRYABLE_STATUS = {502, 503, 504, 429}
_RETRYABLE_ERRORS = (
    httpx.ConnectError,
    httpx.ConnectTimeout,
    httpx.RemoteProtocolError,
    httpx.ReadError,
)
_IMAGE_MAX_BYTES = 16 * 1024 * 1024
_IMAGE_MAX_PIXELS = 16_000_000
_PROXY_ERROR = "Proxy URL must be a loopback HTTP endpoint without credentials or a path"

MAX_REFERENCE_IMAGES = 8
_REF_IMAGE_MAX_RAW_BYTES = 32 * 1024 * 1024
_REF_IMAGE_MAX_PIXELS = 64_000_000
_REF_UPLOAD_PASSTHROUGH_BYTES = 8 * 1024 * 1024
_REF_UPLOAD_MAX_EDGE = 2048
_REFERENCE_FORMATS = {"PNG", "JPEG", "WEBP", "GIF"}
_REFERENCE_MIME = {".png": "image/png", ".jpg": "image/jpeg", ".webp": "image/webp"}


def validate_loopback_http_proxy(proxy_url: str) -> str:
    raw = str(proxy_url or "").strip()
    if not raw:
        return ""
    parsed = urlsplit(raw)
    try:
        port = parsed.port
    except ValueError:
        raise ValueError(_PROXY_ERROR) from None
    if (
        parsed.scheme != "http"
        or parsed.hostname not in ("127.0.0.1", "::1")
        or not port
        or not 1 <= port <= 65535
        or parsed.username
        or parsed.password
        or parsed.path not in ("", "/")
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError(_PROXY_ERROR)
    host = "[::1]" if parsed.hostname == "::1" else "127.0.0.1"
    return f"http://{host}:{port}"


def is_openai_endpoint(endpoint: str, provider_override: str = "") -> bool:
    override = str(provider_override or "").strip().lower()
    if override == "openai" or os.getenv("CS_AI_PROVIDER", "").strip().lower() == "openai":
        return True
    if override in {"deepseek", "deepseek-official"}:
        return False
    parsed = urlsplit(str(endpoint or "").strip())
    return (parsed.hostname or "").lower() == "api.openai.com"


def normalize_openai_base_url(endpoint: str) -> str:
    raw = str(endpoint or "").strip().rstrip("/")
    parsed = urlsplit(raw)
    if (parsed.hostname or "").lower() == "api.openai.com" and parsed.path in ("", "/"):
        return raw + "/v1"
    return raw


def resolve_openai_proxy(endpoint: str, configured_proxy: str | None = None) -> str:
    if not is_openai_endpoint(endpoint):
        return ""
    # GCP has direct egress: a deployment may hard-disable every loopback proxy
    # so a stale :7890 left in .env.prod can never resurface.
    if os.getenv("CSBOT_DIRECT_EGRESS", "").strip() == "1":
        return ""
    raw = (
        configured_proxy
        if configured_proxy is not None
        else os.getenv("CS_AI_OPENAI_PROXY_URL", DEFAULT_OPENAI_PROXY)
    )
    raw = str(raw or "").strip()
    if raw.lower() in {"", "none", "direct", "off", "false"}:
        return ""
    hostname = (urlsplit(str(endpoint or "").strip()).hostname or "").lower()
    if hostname in {"127.0.0.1", "localhost", "::1"} and os.getenv(
        "CS_AI_FORCE_LOOPBACK_LLM_PROXY", "0"
    ) != "1":
        return ""
    return validate_loopback_http_proxy(raw)


def is_openai_non_reasoning_model(model: str) -> bool:
    name = str(model or "").strip().lower()
    return name.startswith(("gpt-4", "gpt-3.5"))


def validate_llm_base_url(url: str, *, name: str = "LLM URL") -> str:
    """Accept only a credential-free HTTPS base, or loopback HTTP for local checks."""
    raw = str(url or "").strip().rstrip("/")
    parsed = urlsplit(raw)
    try:
        parsed.port
    except ValueError:
        raise ValueError(
            f"{name} must be a trusted HTTPS endpoint (or loopback HTTP for tests) "
            "without credentials or query"
        ) from None
    loopback_http = parsed.scheme == "http" and (parsed.hostname or "").lower() in {
        "127.0.0.1",
        "::1",
        "localhost",
    }
    if (
        not parsed.hostname
        or parsed.username
        or parsed.password
        or parsed.query
        or parsed.fragment
        or (parsed.scheme != "https" and not loopback_http)
    ):
        raise ValueError(
            f"{name} must be a trusted HTTPS endpoint (or loopback HTTP for tests) "
            "without credentials or query"
        )
    return raw


def normalize_deepseek_base_url(url: str) -> str:
    """Normalize the official DeepSeek origin so both SDK shapes agree."""
    raw = validate_llm_base_url(url, name="DeepSeek URL")
    parsed = urlsplit(raw)
    if (parsed.hostname or "").lower() == "api.deepseek.com" and parsed.path in ("", "/", "/v1"):
        return f"{parsed.scheme}://{parsed.netloc}"
    return raw


def is_official_deepseek_search_url(search_url: str) -> bool:
    """True only for the official DeepSeek Messages origin (or the empty default).

    Same-origin credential rule: a third-party search gateway or a local
    `search-fixture` key must never be replayed against `api.deepseek.com`.
    """
    raw = str(search_url or "").strip()
    if not raw:
        return True
    parsed = urlsplit(raw)
    try:
        port = parsed.port
    except ValueError:
        return False
    return (
        parsed.scheme == "https"
        and (parsed.hostname or "").lower() == "api.deepseek.com"
        and port in (None, 443)
        and not parsed.username
        and not parsed.password
        and not parsed.query
        and not parsed.fragment
    )


def _origin(url: str) -> tuple[str, str, int | None]:
    parsed = urlsplit(str(url or "").strip())
    try:
        port = parsed.port
    except ValueError:
        port = None
    return parsed.scheme, (parsed.hostname or "").lower(), port


def resolve_deepseek_aux(
    endpoint: str,
    *,
    aux_url: str = "",
    aux_api_key: str = "",
    aux_model: str = "",
    search_url: str = "",
    search_api_key: str = "",
    provider_override: str = "",
) -> dict[str, str] | None:
    """Resolve the domestic direct `deepseek-flash` side channel, or None.

    None is a graceful fallback to the primary model channel: a pure-OpenAI
    deployment (or a native DeepSeek primary) is untouched. Secrets are only
    ever returned to the caller, never logged or echoed.
    """
    if not is_openai_endpoint(endpoint, provider_override):
        return None
    if os.getenv("CS_AI_AUX_ENABLE", "1").strip() == "0":
        return None
    explicit_key = (aux_api_key or os.getenv("CS_AI_AUX_API_KEY", "")).strip()
    explicit_url = (aux_url or os.getenv("CS_AI_AUX_URL", "")).strip()
    model = (aux_model or os.getenv("CS_AI_AUX_MODEL", "")).strip() or DEFAULT_DEEPSEEK_AUX_MODEL
    if explicit_key:
        base_url = normalize_deepseek_base_url(explicit_url or DEFAULT_DEEPSEEK_BASE_URL)
        return {
            "provider": "deepseek-official",
            "ledger_provider": "deepseek",
            "base_url": base_url,
            "api_key": explicit_key,
            "model": model,
        }
    cand_search_key = (search_api_key or os.getenv("CS_AI_SEARCH_API_KEY", "")).strip()
    cand_search_url = (search_url or os.getenv("CS_AI_SEARCH_URL", "")).strip()
    if cand_search_key and is_official_deepseek_search_url(cand_search_url):
        if explicit_url:
            scheme, hostname, port = _origin(explicit_url)
            if scheme != "https" or hostname != "api.deepseek.com" or port not in (None, 443):
                raise ValueError(
                    "CS_AI_AUX_URL outside api.deepseek.com requires CS_AI_AUX_API_KEY"
                )
        base_url = normalize_deepseek_base_url(explicit_url or DEFAULT_DEEPSEEK_BASE_URL)
        return {
            "provider": "deepseek-official",
            "ledger_provider": "deepseek",
            "base_url": base_url,
            "api_key": cand_search_key,
            "model": model,
        }
    return None


def _positive_int(raw: Any, default: int) -> int:
    try:
        value = int(str(raw).strip())
    except (TypeError, ValueError):
        return default
    return value if value > 0 else default


def resolve_openai_fallback_cooldown_seconds() -> float:
    """Cooldown window for a tripped breaker; 0.0 disables the shared cache."""
    raw = (os.getenv("CS_AI_OPENAI_FALLBACK_COOLDOWN_SECONDS") or "").strip()
    if not raw:
        return DEFAULT_OPENAI_FALLBACK_COOLDOWN_SECONDS
    try:
        value = float(raw)
    except (TypeError, ValueError):
        return DEFAULT_OPENAI_FALLBACK_COOLDOWN_SECONDS
    if not math.isfinite(value):
        return DEFAULT_OPENAI_FALLBACK_COOLDOWN_SECONDS
    return max(0.0, value)


def resolve_openai_fallback_fail_threshold() -> int:
    return max(
        1,
        _positive_int(
            os.getenv("CS_AI_OPENAI_FALLBACK_FAIL_THRESHOLD"),
            DEFAULT_OPENAI_FALLBACK_FAIL_THRESHOLD,
        ),
    )


def resolve_openai_connect_timeout_ms() -> int:
    value = _positive_int(
        os.getenv("CS_AI_OPENAI_CONNECT_TIMEOUT_MS"), DEFAULT_OPENAI_CONNECT_TIMEOUT_MS
    )
    return min(60_000, max(1_000, value))


def resolve_openai_fallback_context_window() -> int:
    for name in ("CS_AI_OPENAI_FALLBACK_CONTEXT_WINDOW", "CS_AI_AUX_CONTEXT_WINDOW"):
        raw = (os.getenv(name) or "").strip()
        if raw:
            return _positive_int(raw, DEFAULT_OPENAI_FALLBACK_CONTEXT_WINDOW)
    return DEFAULT_OPENAI_FALLBACK_CONTEXT_WINDOW


def _same_official_deepseek_origin(url: str) -> bool:
    scheme, hostname, port = _origin(url)
    return scheme == "https" and hostname == "api.deepseek.com" and port in (None, 443)


def resolve_openai_fallback(
    endpoint: str,
    *,
    fallback_url: str = "",
    fallback_api_key: str = "",
    fallback_model: str = "",
    aux_url: str = "",
    aux_api_key: str = "",
    aux_model: str = "",
    search_url: str = "",
    search_api_key: str = "",
    provider_override: str = "",
) -> dict[str, Any] | None:
    """Resolve the target that replaces the OpenAI primary channel on failure.

    Stateless and independent of `CS_AI_AUX_ENABLE`: the primary channel may be
    armed for fallback even when the memory side channel is switched off. The
    same-origin rule is absolute, so a third-party gateway credential is never
    replayed against `api.deepseek.com` (and vice versa).
    """
    if not is_openai_endpoint(endpoint, provider_override):
        return None
    if (os.getenv("CS_AI_OPENAI_FALLBACK_ENABLE", "1") or "").strip() == "0":
        return None
    model = (
        fallback_model
        or os.getenv("CS_AI_OPENAI_FALLBACK_MODEL", "")
        or aux_model
        or os.getenv("CS_AI_AUX_MODEL", "")
    ).strip() or DEFAULT_DEEPSEEK_AUX_MODEL
    context_window = resolve_openai_fallback_context_window()

    explicit_fallback_key = (fallback_api_key or os.getenv("CS_AI_FALLBACK_API_KEY", "")).strip()
    explicit_fallback_url = (fallback_url or os.getenv("CS_AI_FALLBACK_URL", "")).strip()
    if explicit_fallback_key:
        return {
            "provider": "deepseek-official",
            "ledger_provider": "deepseek",
            "base_url": normalize_deepseek_base_url(
                explicit_fallback_url or DEFAULT_DEEPSEEK_BASE_URL
            ),
            "api_key": explicit_fallback_key,
            "model": model,
            "context_window": context_window,
        }

    explicit_aux_key = (aux_api_key or os.getenv("CS_AI_AUX_API_KEY", "")).strip()
    explicit_aux_url = (aux_url or os.getenv("CS_AI_AUX_URL", "")).strip()
    if explicit_aux_key:
        return {
            "provider": "deepseek-official",
            "ledger_provider": "deepseek",
            "base_url": normalize_deepseek_base_url(
                explicit_fallback_url or explicit_aux_url or DEFAULT_DEEPSEEK_BASE_URL
            ),
            "api_key": explicit_aux_key,
            "model": model,
            "context_window": context_window,
        }

    cand_search_key = (search_api_key or os.getenv("CS_AI_SEARCH_API_KEY", "")).strip()
    cand_search_url = (search_url or os.getenv("CS_AI_SEARCH_URL", "")).strip()
    if cand_search_key and is_official_deepseek_search_url(cand_search_url):
        # A shared official key may only ever address the official DeepSeek origin.
        for candidate in (explicit_fallback_url, explicit_aux_url):
            if candidate and not _same_official_deepseek_origin(candidate):
                raise ValueError(
                    "CS_AI_FALLBACK_URL / CS_AI_AUX_URL outside api.deepseek.com "
                    "requires a dedicated CS_AI_FALLBACK_API_KEY or CS_AI_AUX_API_KEY"
                )
        return {
            "provider": "deepseek-official",
            "ledger_provider": "deepseek",
            "base_url": normalize_deepseek_base_url(
                explicit_fallback_url or explicit_aux_url or DEFAULT_DEEPSEEK_BASE_URL
            ),
            "api_key": cand_search_key,
            "model": model,
            "context_window": context_window,
        }
    return None


def is_openai_fallback_eligible(code: str, message: str = "") -> bool:
    """Whether a primary-channel failure justifies switching to deepseek-flash.

    Kept byte-for-byte consistent with `isFallbackEligibleFailure` in
    `dsh/bridge.mjs`: business/protocol/local-config errors stay on OpenAI,
    while transport and upstream-availability failures move the step.
    """
    value = str(code or "").strip().upper()
    if value in _OPENAI_FALLBACK_DENY_CODES:
        return False
    if value in _OPENAI_FALLBACK_ALLOW_CODES:
        return True
    return bool(_OPENAI_FALLBACK_MESSAGE_RE.search(str(message or "")))


def _sanitize_fallback_reason(
    reason: Any, secrets: tuple[str, ...] | list[str] = ()
) -> str:
    text = str(reason or "")
    for secret in secrets or ():
        value = str(secret or "").strip()
        if value:
            text = text.replace(value, "[REDACTED]")
    return re.sub(r"\s+", " ", text).strip()[:160]


class OpenAICircuitBreaker:
    """Thread-safe CLOSED/OPEN/HALF_OPEN breaker for the OpenAI primary channel.

    OPEN short-circuits every new turn to the fallback route without touching
    the proxy; after the cooldown exactly one probe is admitted, and concurrent
    turns go straight to fallback so a recovering upstream is not stampeded.
    `monotonic_fn`/`wall_fn` are injectable so the state machine is testable
    offline without sleeping.
    """

    def __init__(
        self,
        *,
        cooldown_seconds: float | None = None,
        fail_threshold: int | None = None,
        monotonic_fn=time.monotonic,
        wall_fn=time.time,
    ) -> None:
        self._lock = threading.Lock()
        self._cooldown_override = (
            None if cooldown_seconds is None else max(0.0, float(cooldown_seconds))
        )
        self._threshold_override = (
            None if fail_threshold is None else max(1, int(fail_threshold))
        )
        self._monotonic = monotonic_fn
        self._wall = wall_fn
        self._state = "closed"
        self._consecutive_failures = 0
        self._open_until = 0.0
        self._probe_in_flight = False
        self._active_lease_id: str | None = None
        self._last_failure_code = ""
        self._last_failure_reason = ""
        self._last_failure_at = 0.0
        self._last_recovery_at = 0.0
        self._trip_count = 0
        self._fallback_count = 0
        self._recovery_count = 0

    @property
    def cooldown_seconds(self) -> float:
        if self._cooldown_override is not None:
            return self._cooldown_override
        return resolve_openai_fallback_cooldown_seconds()

    @property
    def fail_threshold(self) -> int:
        if self._threshold_override is not None:
            return self._threshold_override
        return resolve_openai_fallback_fail_threshold()

    def acquire(self) -> dict[str, Any]:
        with self._lock:
            now = self._monotonic()
            if self._state == "open":
                if now < self._open_until:
                    self._fallback_count += 1
                    return {
                        "mode": "fallback",
                        "state": "open",
                        "lease_id": None,
                        "remaining_cooldown_seconds": round(self._open_until - now, 1),
                        "reason_code": self._last_failure_code or "CIRCUIT_OPEN",
                    }
                # The cooldown elapsed: admit at most one half-open probe.
                self._state = "half_open"
                self._open_until = 0.0
            if self._state == "half_open":
                if self._probe_in_flight:
                    self._fallback_count += 1
                    return {
                        "mode": "fallback",
                        "state": "half_open",
                        "lease_id": None,
                        "remaining_cooldown_seconds": 0.0,
                        "reason_code": self._last_failure_code or "PROBE_IN_FLIGHT",
                    }
                lease_id = uuid.uuid4().hex
                self._probe_in_flight = True
                self._active_lease_id = lease_id
                return {
                    "mode": "probe",
                    "state": "half_open",
                    "lease_id": lease_id,
                    "remaining_cooldown_seconds": 0.0,
                    "reason_code": "",
                }
            return {
                "mode": "primary",
                "state": "closed",
                "lease_id": None,
                "remaining_cooldown_seconds": 0.0,
                "reason_code": "",
            }

    def record_success(self, lease_id: str | None = None) -> dict:
        with self._lock:
            previous = self._state
            if (
                self._state == "half_open"
                and lease_id is not None
                and lease_id == self._active_lease_id
            ):
                self._state = "closed"
                self._consecutive_failures = 0
                self._open_until = 0.0
                self._probe_in_flight = False
                self._active_lease_id = None
                self._recovery_count += 1
                self._last_recovery_at = self._wall()
                return {"previous_state": previous, "state": self._state, "recovered": True}
            if self._state == "closed":
                self._consecutive_failures = 0
            return {"previous_state": previous, "state": self._state, "recovered": False}

    def record_failure(
        self,
        *,
        code: str,
        reason: str = "",
        lease_id: str | None = None,
        secrets: tuple[str, ...] | list[str] = (),
    ) -> dict:
        safe_code = str(code or "UNKNOWN").strip()[:48] or "UNKNOWN"
        safe_reason = _sanitize_fallback_reason(reason, secrets)
        with self._lock:
            now = self._monotonic()
            previous = self._state
            if lease_id is not None and lease_id == self._active_lease_id:
                self._probe_in_flight = False
                self._active_lease_id = None
            self._consecutive_failures += 1
            self._last_failure_code = safe_code
            self._last_failure_reason = safe_reason
            self._last_failure_at = self._wall()
            self._fallback_count += 1
            cooldown = self.cooldown_seconds
            tripped = False
            if cooldown > 0 and (
                previous == "half_open"
                or self._consecutive_failures >= self.fail_threshold
            ):
                self._state = "open"
                self._open_until = now + cooldown
                self._trip_count += 1
                tripped = True
            elif self._state == "half_open":
                # Cooldown caching disabled at runtime: never strand half-open.
                self._state = "closed"
                self._open_until = 0.0
                self._probe_in_flight = False
                self._active_lease_id = None
            return {
                "tripped": tripped,
                "previous_state": previous,
                "state": self._state,
                "cooldown_seconds": cooldown if tripped else 0.0,
                "code": safe_code,
                "reason": safe_reason,
            }

    def release_probe(self, lease_id: str | None) -> None:
        """Drop a half-open probe lease without closing or cooling the breaker.

        A cancelled turn or a non-breaker error (400 / CancelledError) must not
        leave `probe_in_flight` stuck, which would short-circuit every later
        turn into fallback forever.
        """
        if not lease_id:
            return
        with self._lock:
            if self._probe_in_flight and lease_id == self._active_lease_id:
                self._probe_in_flight = False
                self._active_lease_id = None

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            now = self._monotonic()
            remaining = (
                round(self._open_until - now, 1)
                if self._state == "open" and self._open_until > now
                else 0.0
            )
            return {
                "state": self._state,
                "cooldown_seconds": self.cooldown_seconds,
                "fail_threshold": self.fail_threshold,
                "consecutive_failures": self._consecutive_failures,
                "probe_in_flight": self._probe_in_flight,
                "remaining_cooldown_seconds": remaining,
                "last_failure_code": self._last_failure_code,
                "last_failure_reason": self._last_failure_reason,
                "last_failure_at": self._last_failure_at,
                "last_recovery_at": self._last_recovery_at,
                "trip_count": self._trip_count,
                "fallback_count": self._fallback_count,
                "recovery_count": self._recovery_count,
            }

    def reset(self) -> None:
        with self._lock:
            self._state = "closed"
            self._consecutive_failures = 0
            self._open_until = 0.0
            self._probe_in_flight = False
            self._active_lease_id = None
            self._last_failure_code = ""
            self._last_failure_reason = ""
            self._last_failure_at = 0.0
            self._last_recovery_at = 0.0
            self._trip_count = 0
            self._fallback_count = 0
            self._recovery_count = 0


_DEFAULT_OPENAI_CIRCUIT_BREAKER: OpenAICircuitBreaker | None = None
_OPENAI_CIRCUIT_BREAKER_LOCK = threading.Lock()


def openai_circuit_breaker() -> OpenAICircuitBreaker:
    """Process-wide breaker shared by every turn of this bot process."""
    global _DEFAULT_OPENAI_CIRCUIT_BREAKER
    with _OPENAI_CIRCUIT_BREAKER_LOCK:
        if _DEFAULT_OPENAI_CIRCUIT_BREAKER is None:
            _DEFAULT_OPENAI_CIRCUIT_BREAKER = OpenAICircuitBreaker()
        return _DEFAULT_OPENAI_CIRCUIT_BREAKER


def resolve_guard_route(
    *,
    primary_url: str,
    primary_api_key: str,
    primary_model: str,
    openai_proxy_url: str | None = None,
    guard_url: str = "",
    guard_api_key: str = "",
    guard_model: str = "",
    aux_url: str = "",
    aux_api_key: str = "",
    aux_model: str = "",
    search_url: str = "",
    search_api_key: str = "",
) -> dict[str, Any]:
    """Pick the QQ output-guard route: explicit guard, then DeepSeek aux, then primary."""
    g_url = (guard_url or os.getenv("CS_AI_GUARD_URL", "")).strip()
    g_key = (guard_api_key or os.getenv("CS_AI_GUARD_API_KEY", "")).strip()
    g_model = (guard_model or os.getenv("CS_AI_GUARD_MODEL", "")).strip()
    g_provider_override = os.getenv("CS_AI_GUARD_PROVIDER", "").strip()
    if g_key or g_url:
        if g_url and not g_key and _origin(g_url) != _origin(primary_url):
            # A credential is only ever replayed on its own HTTPS origin.
            raise ValueError("CS_AI_GUARD_URL requires CS_AI_GUARD_API_KEY")
        raw_target = g_url or DEFAULT_DEEPSEEK_BASE_URL
        if is_openai_endpoint(raw_target, g_provider_override):
            return {
                "base_url": normalize_openai_base_url(
                    validate_llm_base_url(raw_target, name="CS_AI_GUARD_URL")
                ),
                "api_key": g_key or primary_api_key,
                "model": g_model or primary_model,
                "provider": "openai",
                "use_responses_api": True,
                "proxy_url": resolve_openai_proxy(raw_target, openai_proxy_url),
            }
        return {
            "base_url": normalize_deepseek_base_url(raw_target),
            "api_key": g_key or primary_api_key,
            "model": g_model or DEFAULT_DEEPSEEK_AUX_MODEL,
            "provider": "deepseek",
            "use_responses_api": False,
            "proxy_url": "",
        }
    if g_provider_override.lower() != "openai":
        aux = resolve_deepseek_aux(
            primary_url,
            aux_url=aux_url,
            aux_api_key=aux_api_key,
            aux_model=aux_model,
            search_url=search_url,
            search_api_key=search_api_key,
        )
        if aux is not None:
            return {
                "base_url": aux["base_url"],
                "api_key": aux["api_key"],
                "model": g_model or aux["model"],
                "provider": "deepseek",
                "use_responses_api": False,
                "proxy_url": "",
            }
    if is_openai_endpoint(primary_url):
        return {
            "base_url": normalize_openai_base_url(primary_url),
            "api_key": primary_api_key,
            "model": g_model or primary_model,
            "provider": "openai",
            "use_responses_api": True,
            "proxy_url": resolve_openai_proxy(primary_url, openai_proxy_url),
        }
    return {
        "base_url": str(primary_url or "").strip().rstrip("/"),
        "api_key": primary_api_key,
        "model": g_model or primary_model,
        "provider": "deepseek",
        "use_responses_api": False,
        "proxy_url": "",
    }


def _safe_error_text(response: httpx.Response, api_key: str) -> str:
    detail = ""
    try:
        payload = response.json()
        if isinstance(payload, dict):
            error = payload.get("error")
            if isinstance(error, dict):
                detail = str(error.get("message") or error.get("code") or "")
            elif error:
                detail = str(error)
    except ValueError:
        detail = ""
    if not detail:
        detail = str(response.text or "")[:400]
    detail = str(detail).replace(api_key or "", "[REDACTED]")
    detail = re.sub(r"\s+", " ", detail).strip()
    return detail[:160]


def _decode_image(b64_json: str) -> tuple[bytes, str, int, int]:
    try:
        raw = base64.b64decode(b64_json, validate=True)
    except (ValueError, TypeError):
        raise ValueError("OpenAI image generation returned invalid base64") from None
    if not raw or len(raw) > _IMAGE_MAX_BYTES:
        raise ValueError("OpenAI image generation returned an empty or oversized image")
    try:
        with Image.open(BytesIO(raw)) as image:
            image_format = str(image.format or "").upper()
            width, height = image.size
            image.verify()
    except (UnidentifiedImageError, OSError, ValueError):
        raise ValueError("OpenAI image generation returned an unreadable image") from None
    if image_format not in {"PNG", "JPEG", "WEBP"}:
        raise ValueError("OpenAI image generation returned an unsupported image format")
    if width * height > _IMAGE_MAX_PIXELS:
        raise ValueError("OpenAI image generation returned an oversized image")
    return raw, image_format, width, height


def _reject_symlink_chain(base: Path, candidate: Path) -> None:
    """Reject a symlink at any component between the workspace and the file."""
    try:
        relative = candidate.relative_to(base)
    except ValueError:
        return
    current = base
    for part in relative.parts:
        current = current / part
        if current.is_symlink():
            raise ValueError("reference image symlink is not allowed")


def resolve_authorized_image_paths(
    image_paths: list[str] | tuple[str, ...],
    *,
    workspace: Path | None = None,
    allowed_image_paths: set[str | Path] | frozenset[str | Path] | None = None,
) -> list[Path]:
    """Resolve reference images strictly inside this turn's authorized set.

    Fail-closed: an empty/absent whitelist authorizes nothing, so a caller that
    forgets to pass the turn's paths can never read an arbitrary local file.
    """
    if not isinstance(image_paths, (list, tuple)):
        raise ValueError("image_paths must be a list of authorized local image paths")
    if len(image_paths) > MAX_REFERENCE_IMAGES:
        raise ValueError(
            f"at most {MAX_REFERENCE_IMAGES} reference images are supported per call"
        )
    if not image_paths:
        return []

    allowed_resolved: set[Path] = set()
    for allowed in allowed_image_paths or ():
        if not allowed:
            continue
        entry = Path(str(allowed))
        try:
            if entry.is_symlink() or not entry.exists():
                continue
            resolved_allowed = entry.resolve(strict=True)
        except (OSError, RuntimeError, ValueError):
            continue
        if resolved_allowed.is_file():
            allowed_resolved.add(resolved_allowed)

    resolved_paths: list[Path] = []
    for raw_item in image_paths:
        if not isinstance(raw_item, str) or not raw_item or "\x00" in raw_item:
            raise ValueError("image_paths must be a list of authorized local image paths")
        text = raw_item.strip()
        if not text:
            raise ValueError("image_paths must be a list of authorized local image paths")
        if re.match(r"^https?://", text, re.IGNORECASE):
            raise ValueError(
                "image_paths accepts authorized local file paths, not URLs; "
                "call csdata.call('member_avatar', uid='QQ号') or csdata.image(id) "
                "first and pass full_path"
            )
        candidate = Path(text)
        if not candidate.is_absolute():
            if workspace is None:
                raise ValueError(
                    "image_paths must be a list of authorized local image paths"
                )
            candidate = Path(workspace) / candidate
        if candidate.is_symlink():
            raise ValueError("reference image symlink is not allowed")
        if workspace is not None:
            _reject_symlink_chain(Path(workspace).resolve(), candidate)
        try:
            resolved = candidate.resolve(strict=True)
        except (OSError, RuntimeError):
            raise ValueError(
                "reference image file is unavailable; original image may have been evicted"
            ) from None
        if resolved.is_symlink() or not resolved.is_file():
            raise ValueError("reference image file is unavailable; original image may have been evicted")
        if resolved not in allowed_resolved:
            raise ValueError(
                "reference image path is outside the authorized readable images for this turn"
            )
        try:
            size_bytes = resolved.stat().st_size
        except OSError:
            raise ValueError(
                "reference image file is unavailable; original image may have been evicted"
            ) from None
        if not 1 <= size_bytes <= _REF_IMAGE_MAX_RAW_BYTES:
            raise ValueError("reference image is empty or exceeds size limit")
        resolved_paths.append(resolved)
    return resolved_paths


def _prepare_reference_image(path: Path, index: int) -> tuple[str, bytes, str]:
    """Return (filename, bytes, mime) normalized for the /images/edits multipart."""
    try:
        raw = Path(path).read_bytes()
    except OSError:
        raise ValueError(
            "reference image file is unavailable; original image may have been evicted"
        ) from None
    if not 1 <= len(raw) <= _REF_IMAGE_MAX_RAW_BYTES:
        raise ValueError("reference image is empty or exceeds size limit")
    try:
        with Image.open(BytesIO(raw)) as probe:
            width, height = probe.size
            fmt = str(probe.format or "").upper()
            probe.verify()
    except (UnidentifiedImageError, OSError, ValueError, SyntaxError,
            Image.DecompressionBombError):
        raise ValueError("unsupported reference image format") from None
    if not 1 <= width * height <= _REF_IMAGE_MAX_PIXELS:
        raise ValueError("reference image is too large to decode")
    if fmt not in _REFERENCE_FORMATS:
        raise ValueError("unsupported reference image format")
    if (
        fmt in {"PNG", "JPEG", "WEBP"}
        and len(raw) <= _REF_UPLOAD_PASSTHROUGH_BYTES
        and max(width, height) <= _REF_UPLOAD_MAX_EDGE
    ):
        extension = {"PNG": ".png", "JPEG": ".jpg", "WEBP": ".webp"}[fmt]
        return f"ref_{index}{extension}", raw, _REFERENCE_MIME[extension]
    # Animated GIF (first frame), oversized pixels, or oversized bytes: re-encode
    # a bounded PNG so the upload stays stable and predictable.
    try:
        with Image.open(BytesIO(raw)) as image:
            image.seek(0)
            normalized = image.convert("RGBA")
            if max(normalized.size) > _REF_UPLOAD_MAX_EDGE:
                normalized.thumbnail(
                    (_REF_UPLOAD_MAX_EDGE, _REF_UPLOAD_MAX_EDGE), Image.Resampling.LANCZOS
                )
            buffer = BytesIO()
            normalized.save(buffer, format="PNG")
            png_bytes = buffer.getvalue()
    except (UnidentifiedImageError, OSError, ValueError, Image.DecompressionBombError):
        raise ValueError("unsupported reference image format") from None
    if not 1 <= len(png_bytes) <= _IMAGE_MAX_BYTES:
        raise ValueError("reference image is empty or exceeds size limit")
    return f"ref_{index}.png", png_bytes, "image/png"


async def generate_openai_image(
    *,
    prompt: str,
    api_key: str,
    endpoint: str,
    artifacts_dir: Path,
    model: str = DEFAULT_IMAGE_MODEL,
    proxy_url: str | None = None,
    size: str = "1024x1024",
    quality: str = "medium",
    background: str = "auto",
    caption: str = "",
    timeout: float = 120.0,
    image_paths: list[str] | tuple[str, ...] | None = None,
    allowed_image_paths: set[str | Path] | frozenset[str | Path] | None = None,
    workspace: Path | None = None,
) -> dict:
    prompt = str(prompt or "").strip()
    if not 1 <= len(prompt) <= 4000:
        raise ValueError("prompt must be 1-4000 characters")
    if size not in IMAGE_SIZES:
        raise ValueError("unsupported image size")
    if quality not in IMAGE_QUALITIES:
        raise ValueError("unsupported image quality")
    if background not in IMAGE_BACKGROUNDS:
        raise ValueError("unsupported image background")
    safe_caption = str(caption or "").strip()[:120]

    prepared_refs: list[tuple[str, bytes, str]] = []
    if image_paths is not None:
        resolved_refs = resolve_authorized_image_paths(
            image_paths, workspace=workspace, allowed_image_paths=allowed_image_paths
        )
        prepared_refs = [
            _prepare_reference_image(path, index) for index, path in enumerate(resolved_refs)
        ]

    effective_model = str(model or "").strip() or DEFAULT_IMAGE_MODEL
    effective_proxy = resolve_openai_proxy(endpoint, proxy_url)
    base_url = normalize_openai_base_url(endpoint)
    payload: dict = {}
    files: list[tuple[str, tuple[str, bytes, str]]] = []
    form_data: dict[str, str] = {}
    if prepared_refs:
        url = f"{base_url}/images/edits"
        field_name = "image" if len(prepared_refs) == 1 else "image[]"
        form_data = {
            "model": effective_model,
            "prompt": prompt,
            "n": "1",
            "size": size,
            "quality": quality,
            "background": background,
            "output_format": "png",
        }
        files = [(field_name, item) for item in prepared_refs]
        headers = {"Authorization": f"Bearer {api_key}"}
    else:
        url = f"{base_url}/images/generations"
        payload = {
            "model": effective_model,
            "prompt": prompt,
            "n": 1,
            "size": size,
            "quality": quality,
            "background": background,
            "output_format": "png",
        }
        headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}

    response: httpx.Response | None = None
    for attempt in range(3):
        try:
            async with httpx.AsyncClient(
                proxy=effective_proxy or None,
                timeout=httpx.Timeout(timeout, connect=15.0),
                trust_env=False,
            ) as client:
                if prepared_refs:
                    response = await client.post(
                        url, data=form_data, files=files, headers=headers
                    )
                else:
                    response = await client.post(url, json=payload, headers=headers)
        except _RETRYABLE_ERRORS as exc:
            if attempt < 2:
                await asyncio.sleep(0.8 * (2**attempt))
                continue
            raise ValueError(
                f"OpenAI image generation failed after retries ({type(exc).__name__})"
            ) from None

        if response.status_code == 200:
            break
        if response.status_code in _RETRYABLE_STATUS and attempt < 2:
            await asyncio.sleep(0.8 * (2**attempt))
            continue
        raise ValueError(
            f"OpenAI image generation failed (HTTP {response.status_code}): "
            f"{_safe_error_text(response, api_key)}"
        )

    if response is None or response.status_code != 200:
        raise ValueError("OpenAI image generation failed")

    try:
        body = response.json()
    except ValueError:
        raise ValueError("OpenAI image generation returned invalid JSON") from None
    items = body.get("data") if isinstance(body, dict) else None
    if not isinstance(items, list) or not items or not isinstance(items[0], dict):
        raise ValueError("OpenAI image generation returned no image data")
    item = items[0]
    b64_json = item.get("b64_json")
    if not isinstance(b64_json, str) or not b64_json:
        raise ValueError("OpenAI image generation returned no image bytes")
    revised_prompt = item.get("revised_prompt")

    raw, image_format, width, height = _decode_image(b64_json)
    extension = {"PNG": ".png", "JPEG": ".jpg", "WEBP": ".webp"}[image_format]
    artifacts_dir = Path(artifacts_dir)
    artifacts_dir.mkdir(parents=True, exist_ok=True)
    path = artifacts_dir / f"{uuid.uuid4().hex[:8]}-generated{extension}"
    path.write_bytes(raw)

    result = {
        "ok": True,
        "delivered": True,
        "model": effective_model,
        "mode": "edit" if prepared_refs else "generate",
        "reference_count": len(prepared_refs),
        "size": f"{width}x{height}",
        "quality": quality,
        "background": background,
        "bytes": len(raw),
        "file_path": str(path.resolve()),
        "caption": safe_caption,
    }
    if revised_prompt:
        result["revised_prompt"] = str(revised_prompt)[:500]
    # Billing contract: expose the normalized usage next to the image so the
    # runner can record it and local checks can assert it directly.
    result["usage"] = normalize_image_usage(
        body.get("usage") if isinstance(body, dict) else None,
        model=effective_model,
        mode=result["mode"],
        size=result["size"],
        quality=quality,
    )
    return result
