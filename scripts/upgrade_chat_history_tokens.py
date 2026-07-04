from __future__ import annotations

import asyncio
from pathlib import Path
import sys

import nonebot
from sqlalchemy import inspect, text
from sqlalchemy.ext.asyncio import create_async_engine


REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

nonebot.init()
nonebot.load_plugin(Path("plugins") / "models")

from plugins.models import ChatRetrievalSpan, ChatTokenLexicon


async def main() -> None:
    database_url = nonebot.get_driver().config.cs_database
    engine = create_async_engine(database_url, pool_pre_ping=True, pool_recycle=3600)
    async with engine.begin() as connection:
        await connection.run_sync(ChatTokenLexicon.__table__.create, checkfirst=True)

        def existing_columns(sync_connection):
            inspector = inspect(sync_connection)
            return {column["name"] for column in inspector.get_columns(ChatRetrievalSpan.__tablename__)}

        columns = await connection.run_sync(existing_columns)
        if "token_text" not in columns:
            await connection.execute(text("ALTER TABLE chat_retrieval_span ADD COLUMN token_text TEXT NOT NULL DEFAULT '[]'"))
        if "token_count" not in columns:
            await connection.execute(text("ALTER TABLE chat_retrieval_span ADD COLUMN token_count INTEGER NOT NULL DEFAULT 0"))

        await connection.run_sync(ChatTokenLexicon.__table__.create, checkfirst=True)
    await engine.dispose()
    print("chat history token schema is ready")


if __name__ == "__main__":
    asyncio.run(main())
