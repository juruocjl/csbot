"""Read-only Mneme 0.8.8 projection; never starts DSH or changes recall state."""
from contextlib import contextmanager
import base64
import json
import os
from pathlib import Path
import re
import sqlite3
import time
from .runner import scope_key


class MemoryUnavailable(Exception):
    pass


def scopes(store, gid, uid):
    if not gid or not uid:
        raise ValueError('identity required')
    rows = store.rows("""SELECT scope,max(created_at) AS updated_at FROM runs
        WHERE group_id=? AND user_id=? AND channel='web' GROUP BY scope
        ORDER BY updated_at DESC,scope LIMIT 201""", (gid, uid))
    return {'scopes': [{'id': scope_key('qq', gid, uid), 'kind': 'group', 'updated_at': None}]
            + [{'id': r['scope'], 'kind': 'personal', 'updated_at': r['updated_at']}
               for r in rows[:200] if re.fullmatch(r'[a-f0-9]{64}', r['scope'])],
            'truncated': len(rows) > 200}


def authorize(store, gid, uid, scope):
    if not gid or not uid:
        raise ValueError('identity required')
    if not re.fullmatch(r'[a-f0-9]{64}', scope):
        raise PermissionError('unknown scope')
    if scope != scope_key('qq', gid, uid) and not store.rows(
        "SELECT 1 FROM runs WHERE scope=? AND group_id=? AND user_id=? AND channel='web' LIMIT 1",
        (scope, gid, uid)):
        raise PermissionError('unknown scope')


@contextmanager
def database(scope, root=None):
    base = Path(root or os.getenv('CS_AI_STATE_DIR', 'data/ai')).resolve()
    path = base / 'scopes' / scope / 'memory' / 'memory.db'
    # The caller has authorized an opaque scope, never a user-supplied path.
    if any(p.is_symlink() for p in (base/'scopes', path.parent.parent, path.parent, path)):
        raise MemoryUnavailable('unsafe memory path')
    if not path.is_file():
        yield None
        return
    db = None
    try:
        db = sqlite3.connect(path.as_uri()+'?mode=ro', uri=True, timeout=1)
        db.row_factory = sqlite3.Row
        db.execute('PRAGMA query_only=ON')
        db.execute('PRAGMA trusted_schema=OFF')
        deadline = time.monotonic()+2
        db.set_progress_handler(lambda: int(time.monotonic() > deadline), 1000)
        yield db
    except sqlite3.Error as exc:
        raise MemoryUnavailable('memory read unavailable') from exc
    finally:
        if db is not None:
            db.close()


def tags(value):
    try:
        parsed = json.loads(value)
        return [v[:100] for v in parsed[:30] if isinstance(v, str)] if isinstance(parsed, list) else []
    except (ValueError, TypeError):
        return []


def browse(store, gid, uid, scope, query='', kind='', archived=False, cursor=None, root=None):
    authorize(store, gid, uid, scope)
    if len(query)>200 or len(kind)>80 or type(archived) is not bool:
        raise ValueError('invalid filter')
    position = None
    if cursor is not None:
        try:
            if not isinstance(cursor,str) or len(cursor)>600:
                raise ValueError()
            position = json.loads(base64.urlsafe_b64decode(cursor.encode()))
            if not isinstance(position,list) or len(position)!=2 or not all(isinstance(s,str) and 0<len(s)<=128 for s in position):
                raise ValueError()
        except (ValueError, UnicodeError):
            raise ValueError('invalid cursor') from None
    with database(scope, root) as db:
        if db is None:
            return {'items': [], 'types': [], 'nextCursor': None, 'total': 0}
        filters = ['forgotten=0', 'archived=?']; args = [int(archived)]
        if kind:
            filters.append('type=?'); args.append(kind)
        if query.strip():
            # Literal substring search: SQL wildcard characters have no special meaning.
            filters.append('(instr(lower(title),lower(?))>0 OR instr(lower(content),lower(?))>0 OR instr(lower(tags),lower(?))>0)')
            args.extend([query.strip()]*3)
        where = ' AND '.join(filters)
        db.execute('BEGIN')
        total = db.execute('SELECT count(*) FROM memories WHERE '+where,args).fetchone()[0]
        types = [r[0] for r in db.execute('SELECT DISTINCT type FROM memories WHERE forgotten=0 AND archived=? ORDER BY type LIMIT 100',(int(archived),))]
        if position:
            where += ' AND (updated_at<? OR (updated_at=? AND id<?))'
            args.extend([position[0],position[0],position[1]])
        rows = [dict(r) for r in db.execute('''SELECT id,type,substr(title,1,300) AS title,
            substr(content,1,300) AS preview,substr(tags,1,8192) AS tags,importance,archived,created_at,updated_at
            FROM memories WHERE '''+where+' ORDER BY updated_at DESC,id DESC LIMIT 21',args)]
        page = rows[:20]
        next_cursor = base64.urlsafe_b64encode(json.dumps([page[-1]['updated_at'],page[-1]['id']]).encode()).decode() if len(rows)>20 else None
        for row in page:
            row['tags'] = tags(row['tags']); row['archived'] = bool(row['archived'])
        return {'items': page, 'types': types, 'nextCursor': next_cursor, 'total': total}


def detail(store, gid, uid, scope, memory_id, root=None):
    authorize(store, gid, uid, scope)
    if not memory_id or len(memory_id)>128:
        raise ValueError('invalid memory id')
    with database(scope, root) as db:
        row = db.execute('''SELECT id,type,substr(title,1,300) AS title,
            substr(content,1,131072) AS content,length(content)>131072 AS truncated,
            substr(tags,1,8192) AS tags,importance,archived,created_at,updated_at
            FROM memories WHERE id=? AND forgotten=0''',(memory_id,)).fetchone() if db else None
        if row is None:
            raise PermissionError('unknown memory')
        result = dict(row)
        result['tags'] = tags(result['tags'])
        result['archived'] = bool(result['archived']); result['truncated'] = bool(result['truncated'])
        return result
