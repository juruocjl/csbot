"""Async client helpers for the ``cgserver`` TeamSpeak query proxy.

The helpers take an ``aiohttp``-compatible session so the plugin can reuse a
single connection pool, and they are deliberately free of NoneBot imports so
they can be unit tested in isolation. Error messages never include the proxy
token (or any header value); only method/status/exception class names surface.
"""
from __future__ import annotations

from collections.abc import Mapping
from typing import Any

DEFAULT_TIMEOUT = 10.0


class TsProxyError(RuntimeError):
    """A proxy request failed; the message never contains the bearer token."""


def _endpoint(base_url: str, path: str) -> str:
    base = str(base_url or "").strip().rstrip("/")
    if not base:
        raise TsProxyError("TS_PROXY_URL is not configured")
    return base + path


def auth_headers(token: str) -> dict[str, str]:
    """Bearer header for the proxy, refusing an empty token before any I/O."""
    value = str(token or "").strip()
    if not value:
        raise TsProxyError("TS_PROXY_TOKEN is not configured")
    return {"Authorization": f"Bearer {value}"}


async def _request_json(session, method: str, url: str, headers: dict[str, str],
                        timeout: float, payload: Any = None) -> Mapping:
    try:
        if method == "GET":
            context = session.get(url, headers=headers, timeout=timeout)
        else:
            context = session.post(url, headers=headers, json=payload, timeout=timeout)
        async with context as response:
            status = response.status
            if status != 200:
                raise TsProxyError(f"proxy request failed with HTTP {status}")
            data = await response.json(content_type=None)
    except TsProxyError:
        raise
    except Exception as exc:  # timeouts, connection resets, bad JSON, ...
        raise TsProxyError(f"proxy request failed: {type(exc).__name__}") from None
    if not isinstance(data, Mapping) or data.get("ok") is not True:
        raise TsProxyError("proxy returned an invalid payload")
    return data


async def fetch_tree(session, base_url: str, token: str,
                     timeout: float = DEFAULT_TIMEOUT) -> str:
    """GET ``/tree`` once and return the rendered channel-tree text."""
    data = await _request_json(
        session, "GET", _endpoint(base_url, "/tree"),
        auth_headers(token), timeout)
    text = data.get("text")
    if not isinstance(text, str):
        raise TsProxyError("proxy returned an invalid tree payload")
    return text


async def poke_client(session, base_url: str, token: str, nickname: str,
                      msg: str, timeout: float = DEFAULT_TIMEOUT) -> bool:
    """POST ``/poke`` once and report whether an online client matched."""
    data = await _request_json(
        session, "POST", _endpoint(base_url, "/poke"),
        auth_headers(token), timeout,
        payload={"nickname": nickname, "msg": msg})
    found = data.get("found")
    if not isinstance(found, bool):
        raise TsProxyError("proxy returned an invalid poke payload")
    return found
