"""OpenAI token accounting for every CSBot call path.

Deliberately pure standard library: this module is imported by Windows delivery
checks and by the Linux production host, so it must never pull in `fcntl`,
NoneBot or SQLAlchemy. It also centralizes the Tailnet-only access rule for the
read-only incremental ledger endpoint.
"""
from __future__ import annotations

import ipaddress
import json
import os
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Mapping

USD_PER_MILLION = 1_000_000.0
DEFAULT_TEXT_MODEL = "gpt-6.2-sol"
DEFAULT_IMAGE_MODEL = "gpt-image-2.6-sunburst"
DEFAULT_LONG_CONTEXT_THRESHOLD = 272_000

# (environment variable, official public USD / 1M tokens)
_GPT_SOL_RATES = {
    "input": ("CS_AI_PRICE_GPT62_SOL_INPUT", 2.0),
    "cached": ("CS_AI_PRICE_GPT62_SOL_CACHED", 0.10),
    "cache_write": ("CS_AI_PRICE_GPT62_SOL_CACHE_WRITE", 2.50),
    "output": ("CS_AI_PRICE_GPT62_SOL_OUTPUT", 10.0),
}
_IMAGE_RATES = {
    "text_input": ("CS_AI_PRICE_IMAGE_TEXT_INPUT", 5.0),
    "cached_text_input": ("CS_AI_PRICE_IMAGE_CACHED_TEXT_INPUT", 1.25),
    "image_input": ("CS_AI_PRICE_IMAGE_INPUT", 8.0),
    "cached_image_input": ("CS_AI_PRICE_IMAGE_CACHED_INPUT", 2.0),
    "image_output": ("CS_AI_PRICE_IMAGE_OUTPUT", 30.0),
}
# (input, cached input, output) fallback for DeepSeek models, USD / 1M tokens.
_DEEPSEEK_PEAK = (0.30, 0.006, 1.20)
_DEEPSEEK_IDLE = (0.15, 0.003, 0.60)
_DEEPSEEK_OFFSET = timezone(timedelta(hours=8))  # Asia/Shanghai has no DST.

_FORWARD_HEADERS = frozenset({
    "x-forwarded-for",
    "x-real-ip",
    "forwarded",
    "x-forwarded-host",
    "x-forwarded-proto",
    "cf-connecting-ip",
    "true-client-ip",
})
# Loopback plus Tailnet only: Docker bridge, RFC1918 and public addresses never
# reach the read-only ledger, with or without a reverse proxy in front.
_INTERNAL_NETWORKS = tuple(
    ipaddress.ip_network(value)
    for value in ("127.0.0.0/8", "::1/128", "100.64.0.0/10", "fd7a:115c:a1e0::/48")
)


def _as_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _as_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _get(source: Any, name: str, default: Any = None) -> Any:
    """Read a field from a mapping or from an OpenAI SDK / pydantic object."""
    if source is None:
        return default
    if isinstance(source, Mapping):
        return source.get(name, default)
    return getattr(source, name, default)


def _rate(spec: tuple[str, float]) -> float:
    raw = os.getenv(spec[0])
    if raw is None or not str(raw).strip():
        return spec[1]
    return _as_float(raw, spec[1])


def _is_deepseek(provider: str, model: str) -> bool:
    return "deepseek" in str(provider or "").lower() or "deepseek" in str(model or "").lower()


def _canonical_provider(provider: str, model: str = "") -> str:
    """Fold provider aliases so the ledger always shows `deepseek` or `openai`."""
    name = str(provider or "").strip().lower()
    if name in {"deepseek", "deepseek-official"}:
        return "deepseek"
    if name == "openai":
        return "openai"
    if not name and "deepseek" in str(model or "").lower():
        return "deepseek"
    return str(provider or "").strip()


def _beijing_peak(now: datetime | None = None) -> bool:
    moment = now or datetime.now(_DEEPSEEK_OFFSET)
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=_DEEPSEEK_OFFSET)
    moment = moment.astimezone(_DEEPSEEK_OFFSET)
    if moment.weekday() >= 5:
        return False
    return 9 <= moment.hour <= 11 or 14 <= moment.hour <= 17


def _pricing_override(model: str) -> dict | None:
    raw = (os.getenv("CS_AI_MODEL_PRICING_JSON") or "").strip()
    if not raw:
        return None
    try:
        table = json.loads(raw)
    except ValueError:
        return None
    if not isinstance(table, dict):
        return None
    entry = table.get(str(model or "").strip().lower())
    if not isinstance(entry, dict):
        return None
    if "input" not in entry and "output" not in entry:
        return None
    return {
        "input": _as_float(entry.get("input")),
        "cached": _as_float(entry.get("cached")),
        "cache_write": _as_float(entry.get("cache_write")),
        "output": _as_float(entry.get("output")),
        "long_context_threshold": max(
            0, _as_int(entry.get("long_context_threshold"), DEFAULT_LONG_CONTEXT_THRESHOLD)
        ),
        "scale_long_context": True,
    }


def _text_rates(model: str, provider: str) -> dict:
    """Resolve USD / 1M rates; unknown models fall back instead of raising."""
    name = str(model or "").strip().lower()
    if name == "fixture":
        return {
            "input": 0.0,
            "cached": 0.0,
            "cache_write": 0.0,
            "output": 0.0,
            "long_context_threshold": DEFAULT_LONG_CONTEXT_THRESHOLD,
            "scale_long_context": False,
        }
    override = _pricing_override(name)
    if override is not None:
        return override
    if _is_deepseek(provider, name):
        peak_input, peak_cached, peak_output = (
            _DEEPSEEK_PEAK if _beijing_peak() else _DEEPSEEK_IDLE
        )
        return {
            "input": peak_input,
            "cached": peak_cached,
            "cache_write": 0.0,
            "output": peak_output,
            "long_context_threshold": DEFAULT_LONG_CONTEXT_THRESHOLD,
            "scale_long_context": False,
        }
    return {
        "input": _rate(_GPT_SOL_RATES["input"]),
        "cached": _rate(_GPT_SOL_RATES["cached"]),
        "cache_write": _rate(_GPT_SOL_RATES["cache_write"]),
        "output": _rate(_GPT_SOL_RATES["output"]),
        "long_context_threshold": DEFAULT_LONG_CONTEXT_THRESHOLD,
        "scale_long_context": True,
    }


def _image_rates() -> dict:
    return {key: _rate(spec) for key, spec in _IMAGE_RATES.items()}


def _text_costs(
    *,
    input_tokens: int,
    uncached_input_tokens: int,
    cached_tokens: int,
    cache_write_tokens: int,
    output_tokens: int,
    rates: dict,
) -> tuple[float, float]:
    r_input = rates["input"]
    r_cached = rates["cached"]
    r_cache_write = rates["cache_write"]
    r_output = rates["output"]
    threshold = rates.get("long_context_threshold") or 0
    if (
        rates.get("scale_long_context")
        and threshold > 0
        and input_tokens > threshold
    ):
        # Official OpenAI long-context rule: input/cache x2, output x1.5.
        r_input *= 2.0
        r_cached *= 2.0
        r_cache_write *= 2.0
        r_output *= 1.5
    cost = (
        uncached_input_tokens * r_input
        + cached_tokens * r_cached
        + cache_write_tokens * r_cache_write
        + output_tokens * r_output
    ) / USD_PER_MILLION
    nocache = (input_tokens * r_input + output_tokens * r_output) / USD_PER_MILLION
    return round(cost, 8), round(nocache, 8)


def _image_costs(
    *,
    text_tokens: int,
    image_tokens: int,
    cached_text_tokens: int,
    cached_image_tokens: int,
    output_tokens: int,
    rates: dict,
) -> tuple[float, float]:
    uncached_text = max(0, text_tokens - cached_text_tokens)
    uncached_image = max(0, image_tokens - cached_image_tokens)
    cost = (
        uncached_text * rates["text_input"]
        + cached_text_tokens * rates["cached_text_input"]
        + uncached_image * rates["image_input"]
        + cached_image_tokens * rates["cached_image_input"]
        + output_tokens * rates["image_output"]
    ) / USD_PER_MILLION
    nocache = (
        text_tokens * rates["text_input"]
        + image_tokens * rates["image_input"]
        + output_tokens * rates["image_output"]
    ) / USD_PER_MILLION
    return round(cost, 8), round(nocache, 8)


def _usage_item(
    *,
    provider: str,
    model: str,
    scene: str,
    uncached_input_tokens: int,
    cached_tokens: int,
    cache_write_tokens: int,
    output_tokens: int,
    reasoning_tokens: int,
    image_input_tokens: int,
    image_output_tokens: int,
    cost_usd: float,
    nocache_cost_usd: float,
    size: str = "",
    quality: str = "",
) -> dict:
    """Always satisfy input == uncached + cached + cache_write."""
    uncached = max(0, _as_int(uncached_input_tokens))
    cached = max(0, _as_int(cached_tokens))
    cache_write = max(0, _as_int(cache_write_tokens))
    return {
        "provider": str(provider or ""),
        "model": str(model or ""),
        "scene": str(scene or ""),
        "input_tokens": uncached + cached + cache_write,
        "uncached_input_tokens": uncached,
        "cached_tokens": cached,
        "cache_write_tokens": cache_write,
        "output_tokens": max(0, _as_int(output_tokens)),
        "reasoning_tokens": max(0, _as_int(reasoning_tokens)),
        "image_input_tokens": max(0, _as_int(image_input_tokens)),
        "image_output_tokens": max(0, _as_int(image_output_tokens)),
        "cost_usd": round(_as_float(cost_usd), 8),
        "nocache_cost_usd": round(_as_float(nocache_cost_usd), 8),
        "size": str(size or ""),
        "quality": str(quality or ""),
    }


def normalize_dsh_usage(event: Mapping[str, Any]) -> dict:
    """Normalize a `usage` event emitted by `bridge.mjs`.

    DSH `TokenUsage.inputTokens` is already the uncached input amount in both
    `@earendil-works/pi-ai` and `@deepseek-ai/dsh-llm-deepseek`, so it is never
    reduced again here.
    """
    data = event if isinstance(event, Mapping) else {}
    provider = str(data.get("provider") or "")
    model = str(data.get("model") or "")
    provider = _canonical_provider(provider, model)
    uncached = max(0, _as_int(data.get("uncached_input_tokens")))
    cached = max(0, _as_int(data.get("cached_tokens")))
    cache_write = max(0, _as_int(data.get("cache_write_tokens")))
    output = max(0, _as_int(data.get("output_tokens")))
    reasoning = max(0, _as_int(data.get("reasoning_tokens")))
    input_tokens = uncached + cached + cache_write
    cost, nocache = _text_costs(
        input_tokens=input_tokens,
        uncached_input_tokens=uncached,
        cached_tokens=cached,
        cache_write_tokens=cache_write,
        output_tokens=output,
        rates=_text_rates(model, provider),
    )
    return _usage_item(
        provider=provider,
        model=model,
        scene=str(data.get("scene") or "chat"),
        uncached_input_tokens=uncached,
        cached_tokens=cached,
        cache_write_tokens=cache_write,
        output_tokens=output,
        reasoning_tokens=reasoning,
        image_input_tokens=0,
        image_output_tokens=0,
        cost_usd=cost,
        nocache_cost_usd=nocache,
    )


def _image_token_split(raw_usage: Any) -> dict:
    raw = raw_usage if raw_usage is not None else {}
    raw_input = max(
        0, _as_int(_get(raw, "input_tokens", _get(raw, "prompt_tokens", 0)))
    )
    input_details = _get(raw, "input_tokens_details") or _get(raw, "prompt_tokens_details") or {}
    image_tokens = max(0, _as_int(_get(input_details, "image_tokens", 0)))
    raw_text_tokens = _get(input_details, "text_tokens", None)
    text_tokens = (
        max(0, _as_int(raw_text_tokens))
        if raw_text_tokens is not None
        else max(0, raw_input - image_tokens)
    )
    output_tokens = max(
        0, _as_int(_get(raw, "output_tokens", _get(raw, "completion_tokens", 0)))
    )
    output_details = _get(raw, "output_tokens_details") or {}
    raw_image_output = _get(output_details, "image_tokens", None)
    image_output_tokens = (
        max(0, _as_int(raw_image_output)) if raw_image_output is not None else output_tokens
    )
    cached_total = max(
        0,
        _as_int(
            _get(raw, "cached_tokens", _get(input_details, "cached_tokens", 0))
        ),
    )
    cached_details = _get(raw, "cached_tokens_details") or {}
    raw_cached_image = _get(cached_details, "image_tokens", None)
    raw_cached_text = _get(cached_details, "text_tokens", None)
    if raw_cached_image is None and raw_cached_text is None:
        # Only a total is available: offset the image side first, then text.
        cached_image = min(image_tokens, cached_total)
        cached_text = min(text_tokens, max(0, cached_total - cached_image))
    else:
        cached_image = min(image_tokens, max(0, _as_int(raw_cached_image)))
        if raw_cached_text is not None:
            cached_text = min(text_tokens, max(0, _as_int(raw_cached_text)))
        else:
            cached_text = min(text_tokens, max(0, cached_total - cached_image))
    reasoning_tokens = max(
        0, _as_int(_get(_get(raw, "output_tokens_details") or {}, "reasoning_tokens", 0))
    )
    return {
        "text_tokens": text_tokens,
        "image_tokens": image_tokens,
        "cached_text_tokens": cached_text,
        "cached_image_tokens": cached_image,
        "output_tokens": output_tokens,
        "image_output_tokens": image_output_tokens,
        "reasoning_tokens": reasoning_tokens,
    }


def normalize_image_usage(
    raw_usage: dict | None,
    *,
    model: str,
    mode: str,
    size: str,
    quality: str,
) -> dict:
    """Normalize `/v1/images/generations` and `/v1/images/edits` usage."""
    scene = "image_edit" if str(mode) == "edit" else "image_gen"
    effective_model = str(model or "").strip() or DEFAULT_IMAGE_MODEL
    if not raw_usage:
        return _usage_item(
            provider="openai",
            model=effective_model,
            scene=scene,
            uncached_input_tokens=0,
            cached_tokens=0,
            cache_write_tokens=0,
            output_tokens=0,
            reasoning_tokens=0,
            image_input_tokens=0,
            image_output_tokens=0,
            cost_usd=0.0,
            nocache_cost_usd=0.0,
            size=size,
            quality=quality,
        )
    split = _image_token_split(raw_usage)
    cost, nocache = _image_costs(
        text_tokens=split["text_tokens"],
        image_tokens=split["image_tokens"],
        cached_text_tokens=split["cached_text_tokens"],
        cached_image_tokens=split["cached_image_tokens"],
        output_tokens=split["output_tokens"],
        rates=_image_rates(),
    )
    uncached_text = max(0, split["text_tokens"] - split["cached_text_tokens"])
    uncached_image = max(0, split["image_tokens"] - split["cached_image_tokens"])
    return _usage_item(
        provider="openai",
        model=effective_model,
        scene=scene,
        uncached_input_tokens=uncached_text + uncached_image,
        cached_tokens=split["cached_text_tokens"] + split["cached_image_tokens"],
        cache_write_tokens=0,
        output_tokens=split["output_tokens"],
        reasoning_tokens=split["reasoning_tokens"],
        image_input_tokens=split["image_tokens"],
        image_output_tokens=split["image_output_tokens"],
        cost_usd=cost,
        nocache_cost_usd=nocache,
        size=size,
        quality=quality,
    )


def normalize_guard_usage(usage_obj: Any, *, provider: str, model: str) -> dict:
    """Normalize QQ output-guard usage from `/v1/responses` or chat completions."""
    provider = _canonical_provider(provider, model)
    raw_input = max(
        0, _as_int(_get(usage_obj, "input_tokens", _get(usage_obj, "prompt_tokens", 0)))
    )
    details = (
        _get(usage_obj, "input_tokens_details")
        or _get(usage_obj, "prompt_tokens_details")
        or {}
    )
    raw_cached = _get(details, "cached_tokens", 0)
    # DeepSeek /chat/completions reports cache hits on the top-level usage object.
    cached = max(
        0,
        _as_int(raw_cached),
        _as_int(_get(usage_obj, "prompt_cache_hit_tokens", 0)),
    )
    cache_write = max(
        0,
        _as_int(
            _get(details, "cache_write_tokens", _get(usage_obj, "cache_write_tokens", 0))
        ),
    )
    output = max(
        0, _as_int(_get(usage_obj, "output_tokens", _get(usage_obj, "completion_tokens", 0)))
    )
    output_details = _get(usage_obj, "output_tokens_details") or _get(
        usage_obj, "completion_tokens_details"
    ) or {}
    reasoning = max(0, _as_int(_get(output_details, "reasoning_tokens", 0)))
    image_input = max(0, _as_int(_get(details, "image_tokens", 0)))
    # The Responses API reports `input_tokens` including cached prompt tokens.
    if raw_input > 0:
        uncached = max(0, raw_input - cached - cache_write)
    else:
        # DeepSeek may report only the miss count when the prompt side is split.
        uncached = max(0, _as_int(_get(usage_obj, "prompt_cache_miss_tokens", 0)))
    input_tokens = uncached + cached + cache_write
    cost, nocache = _text_costs(
        input_tokens=input_tokens,
        uncached_input_tokens=uncached,
        cached_tokens=cached,
        cache_write_tokens=cache_write,
        output_tokens=output,
        rates=_text_rates(model, provider),
    )
    return _usage_item(
        provider=provider,
        model=model,
        scene="output_guard",
        uncached_input_tokens=uncached,
        cached_tokens=cached,
        cache_write_tokens=cache_write,
        output_tokens=output,
        reasoning_tokens=reasoning,
        image_input_tokens=image_input,
        image_output_tokens=0,
        cost_usd=cost,
        nocache_cost_usd=nocache,
    )


_LEDGER_FIELDS = (
    "provider",
    "model",
    "scene",
    "input_tokens",
    "uncached_input_tokens",
    "cached_tokens",
    "cache_write_tokens",
    "output_tokens",
    "reasoning_tokens",
    "image_input_tokens",
    "image_output_tokens",
    "cost_usd",
    "nocache_cost_usd",
    "size",
    "quality",
)
_LEDGER_TEXT_FIELDS = {"provider", "model", "scene", "size", "quality"}
_LEDGER_FLOAT_FIELDS = {"cost_usd", "nocache_cost_usd"}


def record_usage_for_run(
    store: Any,
    *,
    run_id: str,
    channel: str,
    group_id: str,
    item: dict,
    publish_fn: Any = None,
) -> int:
    """Persist one normalized usage item and optionally refresh the run summary."""
    now = time.time()
    payload = {key: (item or {}).get(key) for key in _LEDGER_FIELDS}
    for key in _LEDGER_FIELDS:
        if key in _LEDGER_TEXT_FIELDS:
            payload[key] = str(payload[key] or "")
        elif key in _LEDGER_FLOAT_FIELDS:
            payload[key] = _as_float(payload[key])
        else:
            payload[key] = max(0, _as_int(payload[key]))
    payload.update({
        "run_id": str(run_id or ""),
        "channel": str(channel or ""),
        "group_id": str(group_id or ""),
        "created_at": int(now),
        "created_at_ms": int(now * 1000),
    })
    row_id = int(store.record_usage(payload))
    if publish_fn is not None:
        try:
            publish_fn(run_id, {
                "type": "usage",
                "usage": store.run_usage_summary(run_id),
            })
        except Exception:
            # Billing telemetry must never interrupt the user-visible reply.
            pass
    return row_id


def _header_items(headers: Any) -> list[tuple[str, str]]:
    if headers is None:
        return []
    items = getattr(headers, "items", None)
    if not callable(items):
        return []
    try:
        raw = list(items())
    except Exception:
        return []
    return [
        (str(name or "").strip().lower(), "" if value is None else str(value).strip())
        for name, value in raw
    ]


def is_internal_usage_request_allowed(
    client_host: str | None, headers: Mapping[str, str] | Any
) -> bool:
    """Allow the ledger only from loopback/Tailnet direct connections.

    Any non-empty forwarding header is rejected, so a public request proxied by
    `csbot-homepage` (Docker bridge source IP + `X-Real-IP`) can never read it.
    """
    for name, value in _header_items(headers):
        if name in _FORWARD_HEADERS and value:
            return False
    host = str(client_host or "").strip()
    if not host:
        return False
    host = host.split("%", 1)[0].strip()
    if host.startswith("[") and host.endswith("]"):
        host = host[1:-1]
    try:
        address = ipaddress.ip_address(host)
    except ValueError:
        return False
    if address.version == 6 and address.ipv4_mapped is not None:
        address = address.ipv4_mapped
    return any(address in network for network in _INTERNAL_NETWORKS)
