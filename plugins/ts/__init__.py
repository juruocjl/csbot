from nonebot import get_driver, get_plugin_config, on_command, require
from nonebot.adapters.onebot.v11 import GroupMessageEvent, Message, Bot
from nonebot.params import CommandArg
from nonebot.plugin import PluginMetadata

import asyncio

require("fudu")
from ..fudu import addpoint

from . import service
from .config import Config

__plugin_meta__ = PluginMetadata(
    name="ts",
    description="",
    usage="",
    config=Config,
)

config = get_plugin_config(Config)

QUERY_FAILED_TEXT = "TeamSpeak 查询失败，请稍后再试。"


getts = on_command("getts", priority=10, block=True)

tspoke = on_command("poke", priority=10, block=True)


@get_driver().on_shutdown
async def _close_ts_proxy_session() -> None:
    """Release the shared aiohttp pool on NoneBot shutdown."""
    await service.close_session()


@getts.handle()
async def getts_function(bot: Bot):
    """
    Prints the channel tree of the virtual server, including all clients.
    """
    try:
        text = await service.fetch_tree(config)
    except service.TsQueryError:
        await getts.finish(QUERY_FAILED_TEXT)
        return
    msg = await getts.send(text)
    await asyncio.sleep(config.ts_delete_delay)
    await bot.delete_msg(message_id=msg["message_id"])


@tspoke.handle()
async def tspoke_function(message: GroupMessageEvent, arg: Message = CommandArg()):
    """
    Pokes a user with a given nickname.

    Usage: poke <nickname>
    """
    nickname = arg.extract_plain_text().strip()
    if not nickname:
        await tspoke.finish("Please provide a nickname to poke.")
    uid = message.get_user_id()
    gid = message.get_session_id().split('_')[1]
    try:
        found = await service.poke(config, nickname, f"{uid} Poked you!")
    except service.TsQueryError:
        await tspoke.finish(QUERY_FAILED_TEXT)
        return
    if not found:
        await tspoke.finish(f"User {nickname} not found.")
    if not await addpoint(gid, uid, 5):
        await tspoke.finish(f"Poked {nickname}!")
    return
