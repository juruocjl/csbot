#!/usr/bin/env python3
"""Offline, idempotent Mneme migration for the resident DSH Web host.

The one-shot model kept one SQLite database per scope under
``data/ai/scopes/<scope>/memory/memory.db``. The resident daemon keeps a single
database (``data/ai/web-memory/memory.db``) and isolates conversations with
Mneme's ``workspace_scope`` column instead. This script copies every scope's
rows into the unified database, stamps the canonical workspace path, and moves
the historical session JSONL logs into ``data/ai/web-sessions``.

Safety contract:
* ``--dry-run`` is the default and writes nothing (it only reads counts).
* ``--apply`` requires a brand-new ``--backup-dir``; every source and the target
  are snapshotted there first with the SQLite online backup API (0600).
* Rows are matched by ``id``: identical content is skipped, conflicting content
  aborts the whole transaction without overwriting anything.
* Memory bodies are never printed; only scope hash prefixes, counts and the
  integrity-check result leave the process.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import sqlite3
import sys


def _private(path: Path, mode: int = 0o600) -> None:
    try:
        os.chmod(path, mode)
    except OSError:
        pass


def scope_prefix(scope: str) -> str:
    return hashlib.sha256(scope.encode()).hexdigest()[:8]


def discover_sources(state_dir: Path):
    """Return ``[(scope, db_path)]`` for regular per-scope databases only."""
    scopes_dir = state_dir / 'scopes'
    found = []
    if not scopes_dir.is_dir() or scopes_dir.is_symlink():
        return found
    for entry in sorted(scopes_dir.iterdir()):
        path = entry / 'memory' / 'memory.db'
        if entry.is_symlink() or path.is_symlink() or not path.is_file():
            continue
        found.append((entry.name, path))
    return found


def connect_readonly(path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(path.as_uri() + '?mode=ro', uri=True, timeout=10)
    connection.row_factory = sqlite3.Row
    return connection


def snapshot(source: Path, destination: Path) -> None:
    """Consistent copy via the SQLite online backup API."""
    destination.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    if destination.exists() or destination.is_symlink():
        destination.unlink()
    src = connect_readonly(source) if source.exists() else None
    dst = sqlite3.connect(destination)
    try:
        if src is None:
            dst.execute('PRAGMA user_version=0')
            dst.commit()
        else:
            src.backup(dst)
            dst.commit()
    finally:
        dst.close()
        if src is not None:
            src.close()
    _private(destination)


def table_columns(connection: sqlite3.Connection) -> list[str]:
    return [row[1] for row in connection.execute('PRAGMA table_info(memories)')]


def create_schema(source: Path, target: Path) -> None:
    """Create an empty unified database with Mneme's exact schema."""
    src = connect_readonly(source)
    try:
        statements = [row[0] for row in src.execute(
            "SELECT sql FROM sqlite_master WHERE sql IS NOT NULL AND name NOT LIKE 'sqlite_%'")]
    finally:
        src.close()
    target.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    dst = sqlite3.connect(target)
    try:
        for statement in statements:
            dst.execute(statement)
        dst.commit()
    finally:
        dst.close()
    _private(target)


def row_digest(row: sqlite3.Row) -> str:
    """Stable digest of a row's user-visible payload (never printed)."""
    payload = [row['type'], row['title'], row['content'], row['tags'], row['source']]
    return hashlib.sha256(json.dumps(payload, ensure_ascii=False).encode()).hexdigest()


def existing_index(connection: sqlite3.Connection, columns: list[str]) -> dict[str, str]:
    if 'content' not in columns:
        return {}
    index = {}
    for row in connection.execute('SELECT id,type,title,content,tags,source FROM memories'):
        index[row['id']] = row_digest(row)
    return index


def inspect(sources, target: Path, state_dir: Path) -> dict:
    """Read-only plan: per-scope counts, duplicates and conflicts."""
    existing = {}
    target_columns: list[str] = []
    if target.is_file() and not target.is_symlink():
        connection = connect_readonly(target)
        try:
            target_columns = table_columns(connection)
            existing = existing_index(connection, target_columns)
        finally:
            connection.close()
    plan = []
    for scope, path in sources:
        connection = connect_readonly(path)
        try:
            columns = table_columns(connection)
            total = connection.execute('SELECT count(*) FROM memories').fetchone()[0]
            duplicate = conflict = 0
            for row in connection.execute('SELECT id,type,title,content,tags,source FROM memories'):
                current = existing.get(row['id'])
                if current is None:
                    continue
                if current == row_digest(row):
                    duplicate += 1
                else:
                    conflict += 1
        finally:
            connection.close()
        plan.append({
            'scope': scope,
            'prefix': scope_prefix(scope),
            'total': total,
            'duplicate': duplicate,
            'conflict': conflict,
            'columns': len(columns),
            'workspace': str((state_dir / 'scopes' / scope / 'workspace').resolve()),
            'file': str(path),
        })
    return {'plan': plan, 'target': str(target), 'targetExists': target.is_file(),
            'targetColumns': len(target_columns)}


def apply(sources, target: Path, sessions_target: Path, backup_dir: Path, state_dir: Path) -> dict:
    if backup_dir.exists() or backup_dir.is_symlink():
        raise SystemExit(f'backup directory already exists: {backup_dir}')
    backup_dir.mkdir(parents=True, mode=0o700)
    _private(backup_dir, 0o700)

    if not target.exists():
        create_schema(sources[0][1], target)
    elif target.is_symlink():
        raise SystemExit('refusing to write a symlinked unified database')

    dst = sqlite3.connect(target)
    dst.row_factory = sqlite3.Row
    stats = {'scopes': 0, 'inserted': 0, 'duplicate': 0, 'conflict': 0, 'sessions': 0}
    try:
        # Snapshot everything before the first write.
        snapshot(target, backup_dir / 'target-memory.db')
        for scope, path in sources:
            snapshot(path, backup_dir / f'{scope_prefix(scope)}-memory.db')
        dst.execute('PRAGMA foreign_keys=OFF')
        target_columns = table_columns(dst)
        for column in ('workspace_scope', 'workspace_scope_source'):
            if column not in target_columns:
                dst.execute(f'ALTER TABLE memories ADD COLUMN {column} TEXT')
        target_columns = table_columns(dst)
        if 'content' not in target_columns:
            raise SystemExit('unified database has no memories table')
        dst.execute('BEGIN IMMEDIATE')
        for scope, path in sources:
            src = connect_readonly(path)
            try:
                source_columns = table_columns(src)
                columns = [name for name in source_columns if name in target_columns]
                if 'id' not in columns:
                    raise SystemExit('source database has no id column')
                workspace = str((state_dir / 'scopes' / scope / 'workspace').resolve())
                inserted = duplicate = 0
                for row in src.execute('SELECT * FROM memories'):
                    values = {name: row[name] for name in columns}
                    current = dst.execute('SELECT type,title,content,tags,source FROM memories WHERE id=?',
                                          (row['id'],)).fetchone()
                    if current is not None:
                        same = all((current[name] or '') == (row[name] or '')
                                   for name in ('type', 'title', 'content', 'tags', 'source'))
                        if same:
                            duplicate += 1
                            continue
                        stats['conflict'] += 1
                        dst.execute('ROLLBACK')
                        raise SystemExit(
                            f"conflicting memory id in scope {scope_prefix(scope)}: {row['id']} "
                            '(content differs; nothing was overwritten)')
                    values['workspace_scope'] = workspace
                    # Mneme's `strictScope` only hard-filters EXPLICIT scopes.
                    # Auto-stamped rows would stay visible in every group, so the
                    # migrated row must declare its scope explicitly.
                    values['workspace_scope_source'] = 'explicit'
                    names = list(values)
                    dst.execute(
                        'INSERT INTO memories (' + ','.join(names) + ') VALUES ('
                        + ','.join('?' for _ in names) + ')',
                        [values[name] for name in names])
                    inserted += 1
                stats['scopes'] += 1
                stats['inserted'] += inserted
                stats['duplicate'] += duplicate
                # Count verification inside the same transaction.
                expected = src.execute('SELECT count(*) FROM memories').fetchone()[0]
                actual = dst.execute('SELECT count(*) FROM memories WHERE workspace_scope=?',
                                     (workspace,)).fetchone()[0]
                if actual != expected:
                    dst.execute('ROLLBACK')
                    raise SystemExit(
                        f'count mismatch in scope {scope_prefix(scope)}: expected {expected}, got {actual}')
            finally:
                src.close()
        dst.execute('COMMIT')
        integrity = dst.execute('PRAGMA integrity_check').fetchone()[0]
        if integrity != 'ok':
            raise SystemExit(f'unified database integrity check failed: {integrity}')
    except BaseException:
        try:
            dst.execute('ROLLBACK')
        except sqlite3.Error:
            pass
        raise
    finally:
        dst.close()

    # Historical session logs: append-only files, copied only when absent or
    # byte-identical, and never overwritten with different content.
    if sessions_target.exists():
        if sessions_target.is_symlink() or not sessions_target.is_dir():
            raise SystemExit('session target is not a plain directory')
    else:
        sessions_target.mkdir(parents=True, mode=0o700)
    _private(sessions_target, 0o700)
    for scope, _ in sources:
        harness = state_dir / 'scopes' / scope / 'harness'
        if not harness.is_dir():
            continue
        for log in sorted(harness.rglob('sessions/*.jsonl*')):
            if log.is_symlink() or not log.is_file():
                continue
            destination = sessions_target / log.name
            data = log.read_bytes()
            if destination.exists():
                if destination.read_bytes() == data:
                    continue
                raise SystemExit(f'conflicting session log: {log.name}')
            destination.write_bytes(data)
            _private(destination)
            stats['sessions'] += 1
    return stats


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description='Migrate per-scope Mneme databases into the daemon layout.')
    parser.add_argument('--state-dir', default=os.getenv('CS_AI_STATE_DIR', 'data/ai'))
    parser.add_argument('--target', default='')
    parser.add_argument('--sessions-target', default='')
    parser.add_argument('--backup-dir', default='')
    parser.add_argument('--apply', action='store_true', help='write changes (default: dry run)')
    args = parser.parse_args(argv)

    state_dir = Path(args.state_dir).resolve()
    if not state_dir.is_dir():
        print(f'FAIL: state directory not found: {state_dir}')
        return 1
    target = Path(args.target).resolve() if args.target else state_dir / 'web-memory' / 'memory.db'
    sessions_target = (Path(args.sessions_target).resolve() if args.sessions_target
                       else state_dir / 'web-sessions')
    sources = [item for item in discover_sources(state_dir) if item[1].resolve() != target]
    if not sources:
        print('OK: no per-scope memory databases to migrate')
        return 0
    plan = inspect(sources, target, state_dir)
    for entry in plan['plan']:
        print(f"scope {entry['prefix']}: rows={entry['total']} existing={entry['duplicate']} "
              f"conflict={entry['conflict']} columns={entry['columns']}")
    conflicts = sum(entry['conflict'] for entry in plan['plan'])
    if conflicts:
        print(f'FAIL: {conflicts} conflicting rows; nothing was written')
        return 1
    if not args.apply:
        print(f"DRY-RUN: would migrate {len(sources)} scopes into {target} "
              f"(pass --apply --backup-dir <new-dir> to write)")
        return 0
    if not args.backup_dir:
        print('FAIL: --apply requires --backup-dir')
        return 1
    stats = apply(sources, target, sessions_target, Path(args.backup_dir).resolve(), state_dir)
    print(f"APPLIED: scopes={stats['scopes']} inserted={stats['inserted']} "
          f"duplicate={stats['duplicate']} sessions={stats['sessions']} integrity=ok")
    return 0


if __name__ == '__main__':
    try:
        raise SystemExit(main())
    except SystemExit:
        raise
    except Exception as exc:  # noqa: BLE001 - CLI boundary
        print(f'FAIL: {type(exc).__name__}: {exc}')
        raise SystemExit(1)
