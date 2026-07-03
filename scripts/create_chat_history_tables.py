from __future__ import annotations

import asyncio
from pathlib import Path
import sys

import nonebot
from sqlalchemy.ext.asyncio import create_async_engine


REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

nonebot.init()
nonebot.load_plugin(Path("plugins") / "models")

from plugins.models import (
    ChatChunkIndex,
    ChatChunkMessage,
    ChatMessageIndex,
    ChatReplyEdge,
    ChatRetrievalSpan,
)


async def main() -> None:
    database_url = nonebot.get_driver().config.cs_database
    engine = create_async_engine(database_url, pool_pre_ping=True, pool_recycle=3600)
    async with engine.begin() as connection:
        for table in [
            ChatMessageIndex.__table__,
            ChatChunkIndex.__table__,
            ChatChunkMessage.__table__,
            ChatRetrievalSpan.__table__,
            ChatReplyEdge.__table__,
        ]:
            await connection.run_sync(table.create, checkfirst=True)
    await engine.dispose()
    print("chat history tables are ready")


if __name__ == "__main__":
    asyncio.run(main())
