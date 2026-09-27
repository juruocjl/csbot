"""Trusted, bounded query broker; imported by the host, never by agent scripts."""
import asyncio
import datetime
import decimal
import json
from pathlib import Path
import sqlite3
import time
import urllib.request
import urllib.error

from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from .queries import compile_query, template, catalog

MAX_BYTES = 256 * 1024
QUERY_SLOTS = asyncio.Semaphore(2)


def json_value(value):
    if isinstance(value, (datetime.datetime, datetime.date)):
        return value.isoformat()
    if isinstance(value, decimal.Decimal):
        return float(value)
    if isinstance(value, bytes):
        raise ValueError("binary data is not exposed")
    return value


def bounded(rows):
    output, size = [], 0
    truncated = False
    for row in rows:
        item = {str(k): json_value(v) for k,v in dict(row).items()}
        size += len(json.dumps(item,ensure_ascii=False,allow_nan=False).encode())
        if len(output) >= 500 or size > MAX_BYTES:
            truncated = True
            break
        output.append(item)
    return {"rows":output, "truncated":truncated, "row_limit":500, "byte_limit":MAX_BYTES}


class DataBroker:
    def __init__(self, session_factory, group_id: str, game_db: Path | None = None):
        self.session_factory, self.group_id, self.game_db = session_factory, group_id, game_db

    @staticmethod
    def monitor_health():
        checked=datetime.datetime.now(datetime.timezone.utc).isoformat()
        try:
            try:
                response=urllib.request.urlopen('http://127.0.0.1:5555/api/health',timeout=2)
            except urllib.error.HTTPError as exc:
                response=exc
            with response:
                value=json.loads(response.read(16384))
            return {key:value.get(key) is True for key in ('ok','loggedOn','friendStatusReady','reconnectStopped')} | {'checked_at':checked}
        except (OSError,ValueError):
            return {'ok':False,'available':False,'checked_at':checked}

    async def query(self, sql: str, params=None, source="main"):
        params = params or {}
        async with QUERY_SLOTS:
            async with asyncio.timeout(12):
                if source == "game":
                    members = await self._main(compile_query("SELECT steamid FROM members", {}, self.group_id))
                    if members["truncated"]:
                        raise ValueError("too many group bindings to scope game query safely")
                    statement = compile_query(sql, params, self.group_id, source,
                                              tuple(row["steamid"] for row in members["rows"]))
                    result = await asyncio.to_thread(self._game, statement)
                else:
                    statement = compile_query(sql, params, self.group_id, source)
                    result = await self._main(statement)
                result.update(source=source, fetched_at=datetime.datetime.now(datetime.timezone.utc).isoformat())
                return result

    async def _main(self, statement):
        async with self.session_factory() as session:
            async with session.begin():
                await session.execute(text("SET TRANSACTION READ ONLY"))
                await session.execute(text("SET LOCAL statement_timeout = '8000ms'"))
                await session.execute(text("SET LOCAL lock_timeout = '1000ms'"))
                await session.execute(text("SET LOCAL work_mem = '4MB'"))
                # Restrict object resolution too: public sources are explicitly
                # schema-qualified by the compiler; only builtins resolve here.
                await session.execute(text("SET LOCAL search_path = pg_catalog"))
                result = await session.stream(text(statement).execution_options(yield_per=32))
                rows, size, truncated = [], 0, False
                try:
                    async for row in result.mappings():
                        item = {str(k):json_value(v) for k,v in row.items()}
                        size += len(json.dumps(item,ensure_ascii=False,allow_nan=False).encode())
                        if len(rows) >= 500 or size > MAX_BYTES:
                            truncated = True
                            break
                        rows.append(item)
                finally:
                    await result.close()
                return {"rows":rows,"truncated":truncated,"row_limit":500,"byte_limit":MAX_BYTES}

    def _game(self, statement):
        if self.game_db is None or not self.game_db.is_file():
            raise ValueError("game database is unavailable; do not infer current status")
        db = sqlite3.connect(self.game_db.resolve().as_uri()+"?mode=ro", uri=True, timeout=1)
        db.row_factory = sqlite3.Row
        deadline = time.monotonic()+8
        try:
            db.execute("PRAGMA query_only=ON")
            db.execute("PRAGMA trusted_schema=OFF")
            db.set_progress_handler(lambda: int(time.monotonic()>deadline),1000)
            def authorize(op,a,b,*_):
                if op == sqlite3.SQLITE_READ:
                    return sqlite3.SQLITE_OK if a in {"friend_game_history","game_name_map"} else sqlite3.SQLITE_DENY
                if op in {sqlite3.SQLITE_SELECT,sqlite3.SQLITE_FUNCTION}:
                    return sqlite3.SQLITE_OK
                return sqlite3.SQLITE_DENY
            db.set_authorizer(authorize)
            return bounded(db.execute(statement))
        finally:
            db.close()

    async def call(self, name: str, params=None):
        source,sql,values = template(name, params or {})
        return await self.query(sql,values,source)

    async def dispatch(self, request):
        try:
            method = request.get("method")
            if method == "catalog":
                return catalog()
            if method == "status":
                return await asyncio.to_thread(self.monitor_health)
            if method == "call":
                return await self.call(request["name"],request.get("params"))
            if method == "query":
                return await self.query(request["sql"],request.get("params"),request.get("source","main"))
            raise ValueError("unknown csdata method")
        except DBAPIError as exc:
            code=getattr(exc.orig,"sqlstate",None)
            messages={"42703":"Unknown column. Consult catalog; quote camelCase column names.",
                      "42P01":"Data view unavailable: schema preflight or data setup is required.",
                      "57014":"Query timed out. Reduce the time range or aggregate before returning rows.",
                      "22012":"Division by zero. Use NULLIF(denominator,0).",
                      "42501":"Database access was denied."}
            raise ValueError(messages.get(code,"Read-only database query failed; no data was changed.")) from None
