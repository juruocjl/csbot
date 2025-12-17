from nonebot import get_plugin_config
from nonebot.plugin import PluginMetadata
from nonebot.adapters.onebot.v11 import Message, MessageEvent, MessageSegment
from nonebot import on_command
from nonebot import require
from nonebot import get_bot
from nonebot import logger
require("nonebot_plugin_apscheduler")
from nonebot_plugin_apscheduler import scheduler

require("utils")
from ..utils import local_storage

event_update = require("major_hw").event_update


from .get5e import get_matches

import json
import asyncio

from .config import Config

__plugin_meta__ = PluginMetadata(
    name="hltv_watcher",
    description="",
    usage="",
    config=Config,
)

config = get_plugin_config(Config)

updategame = on_command("更新比赛", priority=10, block=True)

@scheduler.scheduled_job("cron", minute="*/5", id="hltv")
@updategame.handle()
async def update_events() -> None:
    bot = get_bot()
    for event in config.hltv_event_id_list:
        logger.info(f"start get {event}")
        title, newres = await get_matches(event)
        res: list[tuple[str, str, str, str]] = json.loads(await local_storage.get(f"hltvresult{event}", default="[]"))
        res.reverse()
        newres.reverse()
        ids = set([match[3] for match in res])
        if len(newres) != len(res):
            text = title + " 结果有更新"
            for match in newres:
                if match[3] not in ids:
                    text += f"\n{match[0]} vs {match[1]} {match[2]}"
                    res.append(match)
                    ids.add(match[3])
            for groupid in config.cs_group_list:
                await bot.send_msg(
                    message_type="group",
                    group_id=groupid,
                    message=text
                )
            res.reverse()
            await local_storage.set(f"hltvresult{event}", json.dumps(res))
            await event_update(event)
        await asyncio.sleep(2)
