from nonebot import get_plugin_config
from nonebot.plugin import PluginMetadata
from nonebot.adapters.onebot.v11 import Message, MessageEvent, MessageSegment
from nonebot import on_command
from nonebot import require
from nonebot import get_bot
from nonebot import logger

from .get5e import get_matches

import json
import asyncio

scheduler = require("nonebot_plugin_apscheduler").scheduler

localstorage = require("utils").localstorage
event_update = require("major_hw").event_update

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
async def update_events():
    bot = get_bot()
    for event in config.hltv_event_id_list:
        logger.info(f"start get {event}")
        title, res = get_matches(event)
        oldres = json.loads(localstorage.get(f"hltvresult{event}", default="[]"))
        if len(res) != len(oldres):
            text = title + " 结果有更新"
            for i in range(len(res) - len(oldres)):
                text += f"\n{res[i][0]} {res[i][2]} {res[i][1]}"
            for groupid in config.cs_group_list:
                await bot.send_msg(
                    message_type="group",
                    group_id=groupid,
                    message=text
                )
            localstorage.set(f"hltvresult{event}", json.dumps(res))
            await event_update(event)
        await asyncio.sleep(2)
