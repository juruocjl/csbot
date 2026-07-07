from __future__ import annotations

import asyncio
import os
from pathlib import Path

import nonebot
from nonebot.adapters.onebot.v11 import Adapter as OneBotAdapter
from sqlalchemy import select


def _set_default_env() -> None:
    defaults = {
        "DRIVER": "~fastapi+~httpx",
        "HOST": "127.0.0.1",
        "PORT": "1234",
        "SUPERUSERS": "[]",
        "CS_DATABASE": "postgresql+asyncpg://postgres:password@127.0.0.1:5432/csbot_backup",
        "CS_SEASON_ID": "S21",
        "CS_LAST_SEASON_ID": "S20",
        "CS_AI_URL": "https://example.invalid",
        "CS_AI_API_KEY": "test-key",
        "CS_AI_MODEL": "deepseek-v4-flash",
        "CS_DOMAIN": "http://127.0.0.1:1234",
        "CS_SERVER_SKIP_STARTUP_CACHE": "1",
        "CS_BOTID": "0",
        "CS_MYSTEAM_ID": "0",
        "CS_WMTOKEN": "test-token",
        "MAJOR_NAME": "test-major",
        "MAJOR_STAGE": "stage1",
        "MAJOR_EVENT_ID": "0",
        "CS_GROUP_LIST": "[]",
    }
    for key, value in defaults.items():
        os.environ.setdefault(key, value)


async def _run_once_check() -> None:
    from plugins.chat_history import db as chat_history_db
    from plugins.models import (
        ChatChunkIndex,
        ChatChunkMessage,
        ChatMessageIndex,
        ChatReplyEdge,
        ChatRetrievalSpan,
        GroupMsg,
    )
    from plugins.utils import async_session_factory, engine

    async with engine.begin() as connection:
        for table in [
            ChatMessageIndex.__table__,
            ChatChunkIndex.__table__,
            ChatChunkMessage.__table__,
            ChatRetrievalSpan.__table__,
            ChatReplyEdge.__table__,
        ]:
            await connection.run_sync(table.create, checkfirst=True)

    async with async_session_factory() as session:
        groupmsg_count = (await session.execute(select(GroupMsg.id).limit(1))).first()

    # Exercise the chat-history API with an empty query against a likely group.
    # The call may return no rows, but it verifies SQL construction and DB access.
    await chat_history_db.search_chat_spans("0", limit=1)
    print(f"minimal bot check passed; groupmsg_has_rows={groupmsg_count is not None}")


def main() -> None:
    _set_default_env()
    nonebot.init(
        driver=os.environ["DRIVER"],
        host=os.environ["HOST"],
        port=int(os.environ["PORT"]),
        superusers=[],
        cs_database=os.environ["CS_DATABASE"],
        cs_season_id=os.environ["CS_SEASON_ID"],
        cs_last_season_id=os.environ["CS_LAST_SEASON_ID"],
        cs_ai_url=os.environ["CS_AI_URL"],
        cs_ai_api_key=os.environ["CS_AI_API_KEY"],
        cs_ai_model=os.environ["CS_AI_MODEL"],
        cs_domain=os.environ["CS_DOMAIN"],
        cs_botid=int(os.environ["CS_BOTID"]),
        cs_mysteam_id=int(os.environ["CS_MYSTEAM_ID"]),
        cs_wmtoken=os.environ["CS_WMTOKEN"],
        major_name=os.environ["MAJOR_NAME"],
        major_stage=os.environ["MAJOR_STAGE"],
        major_event_id=int(os.environ["MAJOR_EVENT_ID"]),
        cs_group_list=[],
    )
    driver = nonebot.get_driver()
    driver.register_adapter(OneBotAdapter)

    for plugin in [
        "blocker",
        "models",
        "utils",
        "major_hw",
        "cs_db_val",
        "cs_db_upd",
        "chat_history",
        "allmsg",
        "pic",
        "cs_ai",
        "cs_server",
    ]:
        nonebot.load_plugin(Path("plugins") / plugin)

    if os.getenv("MINIMAL_RUN_ONCE") == "1":
        asyncio.run(_run_once_check())
    else:
        nonebot.run()


if __name__ == "__main__":
    main()
