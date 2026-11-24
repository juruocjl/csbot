"""Dispatch TeamSpeak queries to the edge HTTP proxy or the direct fallback.

``TS_PROXY_URL`` non-empty -> one async HTTP request to ``cgserver`` (the
default after the split migration). Empty -> the legacy direct Telnet path,
still available as an explicit rollback switch, executed in a worker thread so
the event loop keeps running.
"""
from __future__ import annotations

import asyncio

import aiohttp

from . import client
from .direct import poke_client as direct_poke_client
from .direct import query_tree_text as direct_query_tree_text

SID = 1
DIRECT_TIMEOUT = 5.0

_session: aiohttp.ClientSession | None = None


class TsQueryError(RuntimeError):
    """A TeamSpeak query could not be completed; messages never hold secrets."""


def _proxy_timeout(cfg) -> float:
    try:
        value = float(cfg.ts_proxy_timeout)
    except (TypeError, ValueError, AttributeError):
        return client.DEFAULT_TIMEOUT
    return value if value > 0 else client.DEFAULT_TIMEOUT


def _http_session() -> aiohttp.ClientSession:
    global _session
    if _session is None or _session.closed:
        _session = aiohttp.ClientSession()
    return _session


async def close_session() -> None:
    global _session
    session, _session = _session, None
    if session is not None and not session.closed:
        await session.close()


async def fetch_tree(cfg) -> str:
    """Returns the rendered channel tree for the configured backend."""
    try:
        if cfg.ts_proxy_url:
            return await client.fetch_tree(
                _http_session(), cfg.ts_proxy_url,
                cfg.ts_proxy_token, _proxy_timeout(cfg))
        if not cfg.ts_ip or not cfg.ts_pswd:
            raise TsQueryError(
                "direct TeamSpeak host/password are not configured")
        return await asyncio.to_thread(
            direct_query_tree_text, cfg.ts_ip, cfg.ts_pswd, SID,
            timeout=DIRECT_TIMEOUT)
    except TsQueryError:
        raise
    except client.TsProxyError as exc:
        raise TsQueryError(str(exc)) from None
    except Exception as exc:
        raise TsQueryError(
            f"TeamSpeak query failed: {type(exc).__name__}") from None


async def poke(cfg, nickname: str, msg: str) -> bool:
    """Pokes *nickname* and reports whether an online client matched."""
    try:
        if cfg.ts_proxy_url:
            return await client.poke_client(
                _http_session(), cfg.ts_proxy_url,
                cfg.ts_proxy_token, nickname, msg, _proxy_timeout(cfg))
        if not cfg.ts_ip or not cfg.ts_pswd:
            raise TsQueryError(
                "direct TeamSpeak host/password are not configured")
        return await asyncio.to_thread(
            direct_poke_client, cfg.ts_ip, cfg.ts_pswd, nickname, msg, SID,
            timeout=DIRECT_TIMEOUT)
    except TsQueryError:
        raise
    except client.TsProxyError as exc:
        raise TsQueryError(str(exc)) from None
    except Exception as exc:
        raise TsQueryError(
            f"TeamSpeak query failed: {type(exc).__name__}") from None
