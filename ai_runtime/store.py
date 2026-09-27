"""Durable operational state. This file contains no provider credentials."""
from __future__ import annotations

import json
import fcntl
import os
import sqlite3
import threading
import time
import uuid
from pathlib import Path


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
        """)
        if 'recalled_at' not in {row[1] for row in self.db.execute('PRAGMA table_info(outbound)')}:
            self.db.execute('ALTER TABLE outbound ADD COLUMN recalled_at INTEGER')
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
        self.execute("INSERT INTO runs(id,scope,group_id,user_id,channel,created_at,status,request) VALUES(?,?,?,?,?,?,'queued',?)",
                     (key, scope, gid, uid, channel, int(time.time()), request))

    def get_run(self, key: str) -> dict | None:
        rows = self.rows("SELECT * FROM runs WHERE id=?", (key,))
        return rows[0] if rows else None

    def can_read(self, key: str, gid: str, uid: str) -> bool:
        row = self.get_run(key)
        return bool(row and row["group_id"] == gid and (row["channel"] != "web" or row["user_id"] == uid))


_store: StateStore | None = None


def state_store() -> StateStore:
    global _store
    if _store is None:
        _store = StateStore(Path(os.getenv("CS_AI_STATE_DIR", "data/ai")) / "state.sqlite3")
    return _store
