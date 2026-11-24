#!/usr/bin/env python3
"""Minimal Tailnet-only HTTP proxy for the local TeamSpeak query service.

Runs on ``cgserver`` next to TeamSpeak (``127.0.0.1:10011``) so the NoneBot
process on ``gcp`` can fetch ``/getts`` and ``/poke`` with one async HTTP round
trip instead of a chain of cross-ocean Telnet commands.

Security model:

* binds ``127.0.0.1`` by default; wildcard bind addresses are refused. The
  systemd unit explicitly sets the Tailnet IPv4.
* every endpoint except ``GET /health`` requires
  ``Authorization: Bearer <TS_PROXY_TOKEN>``, compared in constant time.
  Without a configured token the process refuses to start (fail closed).
* the token, ``TS_PSWD``, request headers and request bodies are never logged.

Only ``http.server`` / ``hmac`` / ``json`` and the existing ``ts3`` dependency
are used; no new packages are required.
"""
from __future__ import annotations

import argparse
from dataclasses import dataclass, field
import hmac
import importlib.util
import json
import logging
import os
from pathlib import Path
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlsplit

import ts3

DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 18011
DEFAULT_TS_HOST = "127.0.0.1"
DEFAULT_TS_PORT = 10011
DEFAULT_TS_USER = "serveradmin"
DEFAULT_TS_SID = 1
DEFAULT_TS_TIMEOUT = 5.0
MAX_BODY_BYTES = 64 * 1024
TREE_MODULE_PATH = Path(__file__).resolve().parents[1] / "plugins" / "ts" / "tree.py"

LOG = logging.getLogger("csbot.ts_query_proxy")


class ConfigError(RuntimeError):
    """Invalid or unsafe startup configuration."""


@dataclass(frozen=True)
class Settings:
    bind_host: str
    bind_port: int
    token: str = field(repr=False)
    ts_host: str = DEFAULT_TS_HOST
    ts_port: int = DEFAULT_TS_PORT
    ts_user: str = DEFAULT_TS_USER
    ts_pswd: str = field(default="", repr=False)
    ts_sid: int = DEFAULT_TS_SID
    ts_timeout: float = DEFAULT_TS_TIMEOUT


# --- shared tree module (loaded by path, never through the NoneBot package) ---

_TREE_MODULE = None
_TREE_LOCK = threading.Lock()


def _load_tree_module(path: Path | None = None):
    module_path = Path(path) if path is not None else TREE_MODULE_PATH
    if not module_path.is_file():
        raise ConfigError(f"shared tree module not found at {module_path}")
    spec = importlib.util.spec_from_file_location("csbot_ts_tree", module_path)
    if spec is None or spec.loader is None:
        raise ConfigError(f"cannot load shared tree module at {module_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def tree_module():
    """Returns the cached shared tree module, loading it on first use."""
    global _TREE_MODULE
    with _TREE_LOCK:
        if _TREE_MODULE is None:
            _TREE_MODULE = _load_tree_module()
    return _TREE_MODULE


# --- settings ----------------------------------------------------------------


def load_env_file(path, environ=None) -> dict:
    """Loads a dotenv-style file without overriding existing variables."""
    target = os.environ if environ is None else environ
    env_path = Path(path)
    try:
        text = env_path.read_text(encoding="utf-8")
    except OSError as exc:
        raise ConfigError(
            f"cannot read env file {env_path}: {exc.strerror or type(exc).__name__}"
        ) from None
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export "):].lstrip()
        key, separator, value = line.partition("=")
        key = key.strip()
        if not separator or not key:
            continue
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
            value = value[1:-1]
        target.setdefault(key, value)
    return target


def resolve_token(environ) -> str:
    """Reads TS_PROXY_TOKEN or TS_PROXY_TOKEN_FILE; empty is a hard failure."""
    raw = environ.get("TS_PROXY_TOKEN")
    if raw is not None and str(raw).strip():
        return str(raw).strip()
    token_file = str(environ.get("TS_PROXY_TOKEN_FILE") or "").strip()
    if token_file:
        try:
            content = Path(token_file).read_text(encoding="utf-8")
        except OSError as exc:
            raise ConfigError(
                "cannot read TS_PROXY_TOKEN_FILE: "
                f"{exc.strerror or type(exc).__name__}"
            ) from None
        token = content.strip()
        if token:
            return token
    raise ConfigError(
        "TS_PROXY_TOKEN or TS_PROXY_TOKEN_FILE is required; "
        "refusing to start an unauthenticated proxy"
    )


def _int_setting(environ, key: str, default: int) -> int:
    raw = environ.get(key)
    if raw is None or str(raw).strip() == "":
        return default
    try:
        return int(str(raw).strip())
    except ValueError:
        raise ConfigError(f"{key} must be an integer") from None


def _float_setting(environ, key: str, default: float) -> float:
    raw = environ.get(key)
    if raw is None or str(raw).strip() == "":
        return default
    try:
        value = float(str(raw).strip())
    except ValueError:
        raise ConfigError(f"{key} must be a number") from None
    if value <= 0:
        raise ConfigError(f"{key} must be positive")
    return value


def _validate_bind_host(value: str) -> str:
    host = str(value or "").strip()
    if not host or host in {"*", "0.0.0.0", "::", "[::]"}:
        raise ConfigError(
            "refusing to bind a wildcard address; "
            "set TS_PROXY_BIND_HOST to 127.0.0.1 or the Tailnet address"
        )
    return host


def load_settings(environ=None, host: str | None = None,
                  port: int | None = None) -> Settings:
    """Builds and validates settings from a mapping (defaults to os.environ)."""
    env = os.environ if environ is None else environ

    bind_host = _validate_bind_host(
        host if host is not None
        else env.get("TS_PROXY_BIND_HOST", DEFAULT_HOST))
    bind_port = port if port is not None else _int_setting(
        env, "TS_PROXY_PORT", DEFAULT_PORT)
    if not 1 <= int(bind_port) <= 65535:
        raise ConfigError("TS_PROXY_PORT must be between 1 and 65535")

    ts_host = str(env.get("TS_IP") or DEFAULT_TS_HOST).strip() or DEFAULT_TS_HOST
    ts_port = _int_setting(env, "TS_PORT", DEFAULT_TS_PORT)
    if not 1 <= ts_port <= 65535:
        raise ConfigError("TS_PORT must be between 1 and 65535")
    ts_sid = _int_setting(env, "TS_SID", DEFAULT_TS_SID)
    if ts_sid < 1:
        raise ConfigError("TS_SID must be a positive integer")
    ts_timeout = _float_setting(env, "TS_TIMEOUT", DEFAULT_TS_TIMEOUT)

    return Settings(
        bind_host=bind_host,
        bind_port=int(bind_port),
        token=resolve_token(env),
        ts_host=ts_host,
        ts_port=ts_port,
        ts_user=str(env.get("TS_USER") or DEFAULT_TS_USER),
        ts_pswd=str(env.get("TS_PSWD") or ""),
        ts_sid=ts_sid,
        ts_timeout=ts_timeout,
    )


# --- TeamSpeak queries (blocking; ThreadingHTTPServer gives each request a thread)


def _close_quietly(conn) -> None:
    try:
        conn.close()
    except Exception:
        pass


def _ts_connection(settings: Settings):
    if not settings.ts_pswd:
        raise RuntimeError("TS_PSWD is not configured")
    conn = ts3.query.TS3ServerConnection()
    try:
        conn.open(
            settings.ts_host, settings.ts_port,
            timeout=settings.ts_timeout,
            tp_args={
                "username": settings.ts_user,
                "password": settings.ts_pswd,
            },
        )
    except BaseException:
        _close_quietly(conn)
        raise
    return conn


def query_tree_text(settings: Settings) -> str:
    """Connects to the local TS query port and renders the channel tree."""
    conn = _ts_connection(settings)
    try:
        return tree_module().ChannelTreeNode.build_tree(
            conn, settings.ts_sid).print()
    finally:
        _close_quietly(conn)


def poke_client(settings: Settings, nickname: str, msg: str) -> bool:
    """Pokes the first online client whose nickname matches exactly."""
    conn = _ts_connection(settings)
    try:
        conn.exec_("use", sid=settings.ts_sid)
        for client in conn.exec_("clientlist"):
            if nickname == client["client_nickname"]:
                conn.exec_("clientpoke", clid=client["clid"], msg=msg)
                return True
        return False
    finally:
        _close_quietly(conn)


# --- HTTP layer --------------------------------------------------------------


class ProxyHandler(BaseHTTPRequestHandler):
    server_version = "csbot-ts-proxy/1.0"
    protocol_version = "HTTP/1.1"

    @property
    def settings(self) -> Settings:
        return self.server.settings  # type: ignore[attr-defined]

    def log_request(self, code="-", size="-"):
        # Never log headers or query strings (they may carry the bearer token).
        LOG.info(
            "client=%s method=%s path=%s status=%s",
            self.client_address[0], self.command,
            urlsplit(self.path).path, code,
        )

    def log_message(self, format, *args):
        LOG.debug("%s %s", self.client_address[0], format % args)

    def _send_json(self, status: int, payload: dict, headers=None) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        if status >= 400:
            # An unread request body must not be parsed as the next request.
            self.close_connection = True
            self.send_header("Connection", "close")
        for key, value in (headers or {}).items():
            self.send_header(key, value)
        self.end_headers()
        self.wfile.write(body)

    def _authorized(self) -> bool:
        header = self.headers.get("Authorization", "")
        if not header.startswith("Bearer "):
            return False
        provided = header[len("Bearer "):]
        expected = str(self.settings.token)
        return hmac.compare_digest(
            provided.encode("utf-8"), expected.encode("utf-8"))

    def _read_json_body(self):
        try:
            length = int(self.headers.get("Content-Length", "0"))
        except (TypeError, ValueError):
            return None
        if length <= 0 or length > MAX_BODY_BYTES:
            return None
        try:
            raw = self.rfile.read(length)
            data = json.loads(raw.decode("utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError):
            return None
        return data if isinstance(data, dict) else None

    def _unauthorized(self) -> None:
        self._send_json(401, {"ok": False, "error": "unauthorized"})

    def _query_failed(self, operation: str, exc: Exception) -> None:
        LOG.warning("%s failed: %s", operation, type(exc).__name__)
        self._send_json(502, {"ok": False, "error": "teamspeak query failed"})

    def do_GET(self):
        path = urlsplit(self.path).path
        if path == "/health":
            self._send_json(200, {"ok": True})
            return
        if path == "/tree":
            if not self._authorized():
                self._unauthorized()
                return
            try:
                text = query_tree_text(self.settings)
            except Exception as exc:
                self._query_failed("tree", exc)
                return
            self._send_json(200, {"ok": True, "text": text})
            return
        if path == "/poke":
            self._send_json(
                405, {"ok": False, "error": "method not allowed"},
                headers={"Allow": "POST"})
            return
        self._send_json(404, {"ok": False, "error": "not found"})

    def do_POST(self):
        path = urlsplit(self.path).path
        if path == "/tree":
            self._send_json(
                405, {"ok": False, "error": "method not allowed"},
                headers={"Allow": "GET"})
            return
        if path != "/poke":
            self._send_json(404, {"ok": False, "error": "not found"})
            return
        if not self._authorized():
            self._unauthorized()
            return
        data = self._read_json_body()
        if data is None:
            self._send_json(400, {"ok": False, "error": "invalid json body"})
            return
        nickname = data.get("nickname")
        msg = data.get("msg")
        if (not isinstance(nickname, str) or not nickname.strip()
                or not isinstance(msg, str) or not msg.strip()):
            self._send_json(
                400, {"ok": False, "error": "nickname and msg are required"})
            return
        try:
            found = poke_client(self.settings, nickname.strip(), msg)
        except Exception as exc:
            self._query_failed("poke", exc)
            return
        self._send_json(200, {"ok": True, "found": bool(found)})


def build_server(settings: Settings) -> ThreadingHTTPServer:
    """Creates (but does not start) the threaded HTTP server."""
    tree_module()  # fail at startup if the shared tree module is missing
    server = ThreadingHTTPServer((settings.bind_host, settings.bind_port), ProxyHandler)
    server.settings = settings  # type: ignore[attr-defined]
    server.daemon_threads = True
    return server


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default=None,
                        help="bind address (default 127.0.0.1; wildcard refused)")
    parser.add_argument("--port", type=int, default=None,
                        help="bind port (default 18011)")
    parser.add_argument("--env-file", default=None,
                        help="dotenv-style file loaded before environment lookup")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        stream=sys.stdout,
    )
    # The ts3 library logs the full outgoing query at DEBUG, which includes the
    # login password. Never let that level reach the journal.
    logging.getLogger("ts3").setLevel(logging.INFO)
    try:
        if args.env_file:
            load_env_file(args.env_file)
        settings = load_settings(os.environ, host=args.host, port=args.port)
        server = build_server(settings)
    except ConfigError as exc:
        LOG.error("refusing to start: %s", exc)
        return 2
    except OSError as exc:
        LOG.error("cannot start server: %s", exc.strerror or type(exc).__name__)
        return 1

    if not settings.ts_pswd:
        LOG.warning("TS_PSWD is empty; TeamSpeak queries will fail until configured")
    LOG.info("listen=%s:%s", settings.bind_host, settings.bind_port)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
