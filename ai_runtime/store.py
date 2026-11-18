"""Durable operational state. This file contains no provider credentials."""
from __future__ import annotations

import json
import hashlib
import fcntl
import os
import re
import sqlite3
import threading
import time
import uuid
from pathlib import Path


USAGE_COLUMNS = (
    'created_at', 'created_at_ms', 'run_id', 'channel', 'group_id', 'provider',
    'model', 'scene', 'input_tokens', 'uncached_input_tokens', 'cached_tokens',
    'cache_write_tokens', 'output_tokens', 'reasoning_tokens',
    'image_input_tokens', 'image_output_tokens', 'cost_usd', 'nocache_cost_usd',
    'size', 'quality',
)
_USAGE_TEXT = {'run_id', 'channel', 'group_id', 'provider', 'model', 'scene', 'size', 'quality'}
_USAGE_COST = {'cost_usd', 'nocache_cost_usd'}


class StateStore:
    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        os.chmod(path.parent, 0o700)
        self.owner = open(path.with_suffix('.lock'), 'a')
        try:
            fcntl.flock(self.owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            self.owner.close()
            raise RuntimeError('AI state is already owned by another backend process') from None
        self.lock = threading.RLock()
        self.db = sqlite3.connect(path, check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA busy_timeout=10000")
        self.db.executescript("""
        CREATE TABLE IF NOT EXISTS outbound (
          id TEXT PRIMARY KEY, bot_id TEXT NOT NULL, group_id TEXT NOT NULL,
          api TEXT NOT NULL, payload TEXT NOT NULL, created_at INTEGER NOT NULL,
          status TEXT NOT NULL, message_id INTEGER, record_id INTEGER,
          run_id TEXT, error TEXT, recalled_at INTEGER);
        CREATE UNIQUE INDEX IF NOT EXISTS outbound_platform
          ON outbound(bot_id,group_id,message_id) WHERE message_id IS NOT NULL;
        CREATE INDEX IF NOT EXISTS outbound_pending ON outbound(status,bot_id,created_at);
        CREATE TABLE IF NOT EXISTS runs (
          id TEXT PRIMARY KEY, scope TEXT NOT NULL, group_id TEXT NOT NULL,
          user_id TEXT NOT NULL, channel TEXT NOT NULL, created_at INTEGER NOT NULL,
          status TEXT NOT NULL, request TEXT NOT NULL, response TEXT, error TEXT,
          cutoff INTEGER NOT NULL DEFAULT 0);
        CREATE INDEX IF NOT EXISTS runs_scope_time ON runs(scope,created_at);
        CREATE TABLE IF NOT EXISTS run_aliases (
          short_id TEXT PRIMARY KEY, run_id TEXT NOT NULL UNIQUE REFERENCES runs(id));
        CREATE TABLE IF NOT EXISTS scopes (
          id TEXT PRIMARY KEY, cursor INTEGER NOT NULL DEFAULT 0);
        CREATE TABLE IF NOT EXISTS blocks (
          id TEXT PRIMARY KEY, group_id TEXT NOT NULL, message_ids TEXT NOT NULL,
          created_at INTEGER NOT NULL);
        CREATE TABLE IF NOT EXISTS migrations (
          scope TEXT NOT NULL, name TEXT NOT NULL, completed_at INTEGER NOT NULL,
          PRIMARY KEY(scope,name));
        CREATE TABLE IF NOT EXISTS run_events (
          seq INTEGER PRIMARY KEY AUTOINCREMENT, run_id TEXT NOT NULL,
          payload TEXT NOT NULL, created_at REAL NOT NULL, bytes INTEGER NOT NULL);
        CREATE INDEX IF NOT EXISTS run_events_run ON run_events(run_id,seq);
        CREATE TABLE IF NOT EXISTS run_images (
          id TEXT PRIMARY KEY,run_id TEXT NOT NULL,digest TEXT NOT NULL,caption TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS ai_usage (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          created_at INTEGER NOT NULL,
          created_at_ms INTEGER NOT NULL,
          run_id TEXT NOT NULL,
          channel TEXT NOT NULL,
          group_id TEXT NOT NULL,
          provider TEXT NOT NULL,
          model TEXT NOT NULL,
          scene TEXT NOT NULL,
          input_tokens INTEGER NOT NULL DEFAULT 0,
          uncached_input_tokens INTEGER NOT NULL DEFAULT 0,
          cached_tokens INTEGER NOT NULL DEFAULT 0,
          cache_write_tokens INTEGER NOT NULL DEFAULT 0,
          output_tokens INTEGER NOT NULL DEFAULT 0,
          reasoning_tokens INTEGER NOT NULL DEFAULT 0,
          image_input_tokens INTEGER NOT NULL DEFAULT 0,
          image_output_tokens INTEGER NOT NULL DEFAULT 0,
          cost_usd REAL NOT NULL DEFAULT 0.0,
          nocache_cost_usd REAL NOT NULL DEFAULT 0.0,
          size TEXT NOT NULL DEFAULT '',
          quality TEXT NOT NULL DEFAULT ''
        );
        CREATE INDEX IF NOT EXISTS ai_usage_run ON ai_usage(run_id, id);
        CREATE INDEX IF NOT EXISTS ai_usage_created ON ai_usage(created_at);
        """)
        if 'recalled_at' not in {row[1] for row in self.db.execute('PRAGMA table_info(outbound)')}:
            self.db.execute('ALTER TABLE outbound ADD COLUMN recalled_at INTEGER')
        # Assign old records in insertion order once. Published aliases never
        # change when a newer run has the same UUID suffix.
        for row in self.db.execute('SELECT r.id FROM runs r LEFT JOIN run_aliases a ON a.run_id=r.id WHERE a.run_id IS NULL ORDER BY r.rowid').fetchall():
            self._assign_run_alias(row['id'])
        # No blind resend/replay after a crash: transport outcome may be unknown.
        self.db.execute("UPDATE runs SET status='interrupted',error='HostRestarted' WHERE status IN ('queued','running')")
        self.db.execute("UPDATE outbound SET status='unknown',error='HostRestarted' WHERE status='pending'")
        self.db.commit()
        os.chmod(path, 0o600)

    def execute(self, sql: str, args=()):
        with self.lock:
            cur = self.db.execute(sql, args)
            self.db.commit()
            return cur.rowcount

    def close(self):
        self.db.close()
        self.owner.close()

    def rows(self, sql: str, args=()) -> list[dict]:
        with self.lock:
            return [dict(row) for row in self.db.execute(sql, args).fetchall()]

    def stage(self, bot_id: str, group_id: str, api: str, payload: list, run_id: str | None) -> str:
        key = str(uuid.uuid4())
        encoded=json.dumps(payload, ensure_ascii=False)
        with self.lock:
            staged=self.db.execute("SELECT coalesce(sum(length(CAST(payload AS BLOB))),0) FROM outbound WHERE status IN ('pending','sent','unknown')").fetchone()[0]
            if staged+len(encoded.encode())>64*1024**2:
                raise ValueError('发送归档暂存已满，需要管理员处理未确认记录；本条尚未发送')
            self.execute("INSERT INTO outbound(id,bot_id,group_id,api,payload,created_at,status,run_id) VALUES(?,?,?,?,?,?,'pending',?)",
                         (key, bot_id, group_id, api, encoded, int(time.time()), run_id))
        return key

    def outbound_result(self, key: str, mid: int | None, error: str | None = None):
        self.execute("UPDATE outbound SET status=?,message_id=?,error=? WHERE id=?",
                     ("failed" if error else "sent" if mid is not None else "unknown", mid, error, key))

    def register_run(self, key: str, scope: str, gid: str, uid: str, channel: str, request: str):
        with self.lock, self.db:
            self.db.execute("INSERT INTO runs(id,scope,group_id,user_id,channel,created_at,status,request) VALUES(?,?,?,?,?,?,'queued',?)",
                            (key, scope, gid, uid, channel, int(time.time()), request))
            self._assign_run_alias(key)

    def _assign_run_alias(self, key: str):
        try: compact = uuid.UUID(key).hex
        except ValueError: compact = hashlib.sha256(key.encode()).hexdigest()
        for length in range(12, len(compact)+1, 4):
            candidate = compact[-length:]
            if not self.db.execute('SELECT 1 FROM run_aliases WHERE short_id=?', (candidate,)).fetchone():
                self.db.execute('INSERT INTO run_aliases(short_id,run_id) VALUES(?,?)', (candidate, key))
                return
        raise ValueError('AI short ID could not be allocated')

    def short_run_id(self, key: str) -> str | None:
        rows = self.rows('SELECT short_id FROM run_aliases WHERE run_id=?', (key,))
        return rows[0]['short_id'] if rows else None

    def resolve_run_id(self, key: str, gid: str, uid: str) -> str | None:
        if not gid or not uid or not isinstance(key, str): return None
        if re.fullmatch(r'[0-9a-fA-F]{12,32}', key):
            rows = self.rows('SELECT run_id FROM run_aliases WHERE short_id=?', (key.lower(),))
            target = rows[0]['run_id'] if rows else None
        elif re.fullmatch(r'[0-9a-fA-F]{8}(?:-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}', key):
            target = key.lower()
        else: return None
        return target if target and self.can_read(target, gid, uid) else None

    def get_run(self, key: str) -> dict | None:
        rows = self.rows("SELECT * FROM runs WHERE id=?", (key,))
        return rows[0] if rows else None

    def conversations(self, gid: str, uid: str, before: int | None = None):
        if not gid or not uid:
            raise ValueError('group and user identity required')
        if before is not None and (type(before) is not int or not 0 < before < 2**63):
            raise ValueError('invalid list cursor')
        rows = self.rows("""SELECT rowid AS cursor,id,substr(request,1,300) AS request,status,channel,created_at
            FROM runs WHERE group_id=? AND (channel IN ('qq','report') OR (channel='web' AND user_id=?))
            AND (? IS NULL OR rowid<?) ORDER BY rowid DESC LIMIT 31""", (gid, uid, before, before))
        page = rows[:30]
        cursor = page[-1]['cursor'] if len(rows)>30 else None
        for row in page: row.pop('cursor')
        return {'records': page, 'nextCursor': cursor}

    def can_read(self, key: str, gid: str, uid: str) -> bool:
        row = self.get_run(key)
        return bool(gid and uid and row and row["group_id"] == gid and
                    (row["channel"] in ('qq', 'report') or (row["channel"] == 'web' and row["user_id"] == uid)))

    def record_usage(self, item: dict) -> int:
        """Append one immutable billing row and return its autoincrement id."""
        source = dict(item or {})
        now = time.time()
        source.setdefault('created_at', int(now))
        source.setdefault('created_at_ms', int(now * 1000))
        values = {}
        for name in USAGE_COLUMNS:
            if name in _USAGE_TEXT:
                values[name] = str(source.get(name) or '')
            elif name in _USAGE_COST:
                try:
                    values[name] = float(source.get(name) or 0.0)
                except (TypeError, ValueError):
                    values[name] = 0.0
            else:
                try:
                    values[name] = int(source.get(name) or 0)
                except (TypeError, ValueError):
                    values[name] = 0
        columns = ','.join(USAGE_COLUMNS)
        placeholders = ','.join('?' for _ in USAGE_COLUMNS)
        with self.lock:
            cur = self.db.execute(
                f'INSERT INTO ai_usage({columns}) VALUES({placeholders})',
                tuple(values[name] for name in USAGE_COLUMNS),
            )
            self.db.commit()
            return int(cur.lastrowid)

    def list_usage_after(self, after_id: int = 0, limit: int = 500) -> dict:
        """Cursor pagination for the read-only Tailnet ledger endpoint."""
        if not isinstance(after_id, int) or isinstance(after_id, bool) or after_id < 0:
            raise ValueError('after_id must be a non-negative integer')
        if not isinstance(limit, int) or isinstance(limit, bool) or not 1 <= limit <= 2000:
            raise ValueError('limit must be between 1 and 2000')
        columns = 'id,' + ','.join(USAGE_COLUMNS)
        with self.lock:
            max_id = int(self.db.execute(
                'SELECT coalesce(max(id),0) FROM ai_usage').fetchone()[0])
            rows = [dict(row) for row in self.db.execute(
                f'SELECT {columns} FROM ai_usage WHERE id > ? ORDER BY id ASC LIMIT ?',
                (after_id, limit + 1),
            ).fetchall()]
        has_more = len(rows) > limit
        items = rows[:limit]
        return {
            'schema_version': 1,
            'after_id': after_id,
            'limit': limit,
            'count': len(items),
            'has_more': has_more,
            'next_after_id': items[-1]['id'] if items else after_id,
            'max_id': max_id,
            'items': items,
        }

    def run_usage_summary(self, run_id: str) -> dict:
        with self.lock:
            row = self.db.execute(
                """SELECT count(*) AS calls,
                       coalesce(sum(input_tokens),0) AS input_tokens,
                       coalesce(sum(uncached_input_tokens),0) AS uncached_input_tokens,
                       coalesce(sum(cached_tokens),0) AS cached_tokens,
                       coalesce(sum(cache_write_tokens),0) AS cache_write_tokens,
                       coalesce(sum(output_tokens),0) AS output_tokens,
                       coalesce(sum(reasoning_tokens),0) AS reasoning_tokens,
                       coalesce(sum(image_input_tokens),0) AS image_input_tokens,
                       coalesce(sum(image_output_tokens),0) AS image_output_tokens,
                       coalesce(sum(cost_usd),0) AS cost_usd,
                       coalesce(sum(nocache_cost_usd),0) AS nocache_cost_usd
                FROM ai_usage WHERE run_id=?""",
                (str(run_id or ''),),
            ).fetchone()
        input_tokens = int(row['input_tokens'])
        cached_tokens = int(row['cached_tokens'])
        return {
            'calls': int(row['calls']),
            'input_tokens': input_tokens,
            'uncached_input_tokens': int(row['uncached_input_tokens']),
            'cached_tokens': cached_tokens,
            'cache_write_tokens': int(row['cache_write_tokens']),
            'output_tokens': int(row['output_tokens']),
            'reasoning_tokens': int(row['reasoning_tokens']),
            'image_input_tokens': int(row['image_input_tokens']),
            'image_output_tokens': int(row['image_output_tokens']),
            'cost_usd': round(float(row['cost_usd']), 8),
            'nocache_cost_usd': round(float(row['nocache_cost_usd']), 8),
            'cache_hit_pct': round(cached_tokens / input_tokens * 100.0, 1) if input_tokens > 0 else 0.0,
        }


_store: StateStore | None = None


def state_store() -> StateStore:
    global _store
    if _store is None:
        _store = StateStore(Path(os.getenv("CS_AI_STATE_DIR", "data/ai")) / "state.sqlite3")
    return _store
