"""Exercise the real lexicon update/ORM without starting the bot.

Defaults to in-memory SQLite; --postgres uses only CS_DATABASE=csbot_backup.
Synthetic tokenization isolates query batching from jieba and chat contents.
"""
import argparse
import ast
import asyncio
from collections import Counter
import logging
import math
import os
from pathlib import Path
from types import SimpleNamespace
import uuid

from sqlalchemy import BigInteger, Boolean, Float, Index, Integer, String, Text, delete, event, select, text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, MappedAsDataclass, mapped_column

ROOT = Path(__file__).resolve().parents[1]


class Base(MappedAsDataclass, DeclarativeBase):
    pass


namespace = dict(globals())
models = ast.parse((ROOT / "plugins/models/__init__.py").read_text())
classes = [node for node in models.body if isinstance(node, ast.ClassDef)
           and node.name in {"ChatMessageIndex", "ChatTokenLexicon"}]
exec(compile(ast.Module(body=classes, type_ignores=[]), "models", "exec"), namespace)
ChatMessageIndex = namespace["ChatMessageIndex"]
ChatTokenLexicon = namespace["ChatTokenLexicon"]
source = ast.parse((ROOT / "plugins/chat_history/__init__.py").read_text())
manager = next(node for node in source.body if isinstance(node, ast.ClassDef) and node.name == "DataManager")
method = next(node for node in manager.body if isinstance(node, ast.AsyncFunctionDef)
              and node.name == "update_group_lexicon")
constants = [node for node in source.body if isinstance(node, ast.Assign)
             and any(isinstance(t, ast.Name) and t.id.startswith("LEXICON_") for t in node.targets)]
scorer = next(node for node in source.body if isinstance(node, ast.FunctionDef)
              and node.name == "_score_lexicon_term")
namespace.update(_lexicon_candidates_from_text=lambda value: set(value.split()),
                 normalize_text=lambda value: value.lower(),
                 _is_good_lexicon_term=lambda value: True,
                 logger=logging.getLogger(__name__))
exec(compile(ast.Module(body=constants + [scorer, method], type_ignores=[]), "chat_history", "exec"), namespace)


async def main(postgres=False):
    url = os.environ["CS_DATABASE"] if postgres else "sqlite+aiosqlite:///:memory:"
    if postgres:
        assert make_url(url).database == "csbot_backup", "only the existing test database is allowed"
    engine = create_async_engine(url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    namespace["async_session_factory"] = sessions
    bind_counts = []

    @event.listens_for(engine.sync_engine, "before_cursor_execute")
    def check_query(conn, cursor, statement, parameters, context, executemany):
        if statement.lstrip().upper().startswith("SELECT") and "chat_token_lexicon.term IN (" in statement:
            count = len(parameters)
            bind_counts.append(count)
            assert count <= 1001, f"unbounded lexicon query: {count} parameters"

    gid = "lex" + uuid.uuid4().hex[:16]
    other = "lex" + uuid.uuid4().hex[:16]
    words = [f"word{i:06d}" for i in range(54843)]
    base_id = 1_500_000_000 + (uuid.uuid4().int % 100_000_000)

    async def refresh(*args, **kwargs):
        return set()

    instance = SimpleNamespace(_lexicon_cache={gid: {"stale"}}, _enabled_lexicon_terms=refresh)
    try:
        async with engine.begin() as conn:
            if postgres:
                assert await conn.scalar(text("SELECT current_database()")) == "csbot_backup"
            else:
                await conn.run_sync(Base.metadata.create_all)
        async with sessions.begin() as session:
            for i, offset in enumerate(range(0, len(words), 128)):
                plain = " ".join(words[offset:offset + 128])
                session.add(ChatMessageIndex(record_id=base_id + i, mid=base_id + i,
                    group_id=gid, user_id="fixture", timestamp=10,
                    plain_text=plain, normalized_text=plain))
            for term in (words[0], words[-1], "untouched"):
                session.add(ChatTokenLexicon(group_id=gid, term=term, freq=1000,
                    doc_freq=7, score=1, last_seen=20, enabled=True))
            session.add(ChatTokenLexicon(group_id=other, term=words[0], freq=999999,
                doc_freq=999999, score=1, last_seen=99, enabled=True))
        enabled = await namespace["update_group_lexicon"](instance, gid, start_time=10, replace=False)
        assert enabled == 2
        assert len(bind_counts) == math.ceil(len(words) / 1000)
        assert max(bind_counts) == 1001 and bind_counts[-1] == 844
        assert gid not in instance._lexicon_cache
        async with sessions() as session:
            for term in (words[0], words[-1]):
                row = await session.get(ChatTokenLexicon, (gid, term))
                assert (row.freq, row.doc_freq, row.last_seen, row.enabled) == (1001, 8, 20, True)
            assert (await session.get(ChatTokenLexicon, (other, words[0]))).freq == 999999
            assert (await session.get(ChatTokenLexicon, (gid, "untouched"))).freq == 1000
        assert await namespace["update_group_lexicon"](instance, gid, start_time=11, replace=False) == 0
        bind_counts.clear()
        assert await namespace["update_group_lexicon"](instance, gid, replace=True) == 0
        assert not bind_counts, "replacement must not read/merge old counts"
        async with sessions() as session:
            rows = list((await session.scalars(select(ChatTokenLexicon).where(ChatTokenLexicon.group_id == gid))).all())
            assert len(rows) == 4000
            assert all(row.freq == 1 and row.doc_freq == 1 and row.last_seen == 10 for row in rows)
            assert not any(row.term == "untouched" for row in rows)
        print("PASS: 54843 candidates use 55 bounded queries; existing counts/timestamps, group isolation, empty window and replacement preserved")
    finally:
        async with sessions.begin() as session:
            await session.execute(delete(ChatTokenLexicon).where(ChatTokenLexicon.group_id.in_([gid, other])))
            await session.execute(delete(ChatMessageIndex).where(ChatMessageIndex.group_id == gid))
        await engine.dispose()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--postgres", action="store_true")
    asyncio.run(main(parser.parse_args().postgres))
