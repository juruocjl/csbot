from nonebot import get_plugin_config
from nonebot.plugin import PluginMetadata
from nonebot import on_command
from nonebot import require
from nonebot import get_bot
from nonebot import logger

import asyncio
import re
from sqlalchemy import String, Integer
from sqlalchemy.orm import Mapped, mapped_column

scheduler = require("nonebot_plugin_apscheduler").scheduler

require("utils")

from ..utils import async_session_factory, Base

get_session = require("utils").get_session

from .config import Config

__plugin_meta__ = PluginMetadata(
    name="live_watcher",
    description="",
    usage="",
    config=Config,
)

config = get_plugin_config(Config)

class LiveStatus(Base):
    __tablename__ = "live_status"

    liveid: Mapped[str] = mapped_column(String, primary_key=True)
    islive: Mapped[int] = mapped_column(Integer)

class DataManager:
    async def get_live_status(self, liveid: str) -> int:
        async with async_session_factory() as session:
            status_obj = await session.get(LiveStatus, liveid)
            
            if status_obj:
                return status_obj.islive
            return 0

    async def set_live_status(self, liveid: str, status: int):
        async with async_session_factory() as session:
            async with session.begin():
                new_status = LiveStatus(liveid=liveid, islive=status)
                await session.merge(new_status)
        
db = DataManager()

livestate = on_command("直播状态", priority=10, block=True)

async def get_live_status(liveid):
    await asyncio.sleep(1)
    if liveid.startswith("dy_"):
        async with get_session().get("https://www.doseeing.com/room/"+liveid.split('_')[1]) as res:
            data = await res.text() 
            islive = int('<span>直播中</span>' in data)
            nickname = re.findall(r'<title>(.*?)</title>', data, re.IGNORECASE)[1][:-10]
            return islive, nickname
    if liveid.startswith("bili_"):
        headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.111 Safari/537.36"}
        async with get_session().get("https://api.live.bilibili.com/room/v1/Room/room_init?id="+liveid.split('_')[1], headers=headers) as res:
            await asyncio.sleep(1)
            data = await res.json()
            islive = int(data['data']['live_status'] == 1)
            uid = data['data']['uid']
            async with get_session().get("https://api.live.bilibili.com/live_user/v1/Master/info?uid="+str(uid), headers=headers) as res:
                data = await res.json()
                nickname = data['data']['info']['uname']
                return islive, nickname


now_live_state = "无数据"
@scheduler.scheduled_job("cron", minute="*/2", id="livewatcher")
async def live_watcher():
    bot = get_bot()
    new_live_state = ""
    for liveid in config.live_watch_list:
        islive, nickname = await get_live_status(liveid)
        logger.info(f"[live_watcher] {nickname} {islive}")
        new_live_state += f"{nickname} {islive}\n"
        if islive == 1 and await db.get_live_status(liveid) == 0:
            for groupid in config.cs_group_list:
                await bot.send_msg(
                    message_type="group",
                    group_id=groupid,
                    message=f"{nickname} 开播了"
                )
        await db.set_live_status(liveid, islive)
    global now_live_state
    now_live_state = new_live_state.strip()


@livestate.handle()
async def livestate_function():
    await livestate.finish(now_live_state)