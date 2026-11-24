"""Blocking TeamSpeak query helpers for the direct (no proxy) fallback.

The plugin runs these through ``asyncio.to_thread`` so the event loop is never
blocked by the cross-ocean Telnet round trips. Each call opens one connection
and issues a single ``use`` command; the redundant per-channel ``channelinfo``
loop was removed from :mod:`plugins.ts.tree`.
"""
from __future__ import annotations

import ts3

from .tree import ChannelTreeNode

DEFAULT_QUERY_PORT = 10011
DEFAULT_QUERY_USER = "serveradmin"
DEFAULT_QUERY_TIMEOUT = 5.0


def _close_quietly(conn) -> None:
    try:
        conn.close()
    except Exception:
        pass


def _connect(host: str, password: str, port: int = DEFAULT_QUERY_PORT,
             user: str = DEFAULT_QUERY_USER,
             timeout: float = DEFAULT_QUERY_TIMEOUT):
    if not host or not password:
        raise RuntimeError("direct TeamSpeak host/password are not configured")
    conn = ts3.query.TS3ServerConnection()
    try:
        conn.open(
            host, int(port), timeout=float(timeout),
            tp_args={"username": user, "password": password},
        )
    except BaseException:
        _close_quietly(conn)
        raise
    return conn


def query_tree_text(host: str, password: str, sid: int = 1,
                    port: int = DEFAULT_QUERY_PORT,
                    user: str = DEFAULT_QUERY_USER,
                    timeout: float = DEFAULT_QUERY_TIMEOUT) -> str:
    """Connects locally, fetches the channel tree and returns the rendered text."""
    conn = _connect(host, password, port=port, user=user, timeout=timeout)
    try:
        return ChannelTreeNode.build_tree(conn, sid).print()
    finally:
        _close_quietly(conn)


def poke_client(host: str, password: str, nickname: str, msg: str,
                sid: int = 1, port: int = DEFAULT_QUERY_PORT,
                user: str = DEFAULT_QUERY_USER,
                timeout: float = DEFAULT_QUERY_TIMEOUT) -> bool:
    """Pokes the first online client with an exact nickname match."""
    conn = _connect(host, password, port=port, user=user, timeout=timeout)
    try:
        conn.exec_("use", sid=sid)
        for client in conn.exec_("clientlist"):
            if nickname == client["client_nickname"]:
                conn.exec_("clientpoke", clid=client["clid"], msg=msg)
                return True
        return False
    finally:
        _close_quietly(conn)
