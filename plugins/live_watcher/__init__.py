from nonebot import get_plugin_config
from nonebot.plugin import PluginMetadata
from nonebot import on_command
from nonebot import require
from nonebot import get_bot
from nonebot import logger

import asyncio
import requests
import re

scheduler = require("nonebot_plugin_apscheduler").scheduler

get_cursor = require("utils").get_cursor

from .config import Config

__plugin_meta__ = PluginMetadata(
    name="live_watcher",
    description="",
    usage="",
    config=Config,
)

config = get_plugin_config(Config)


class DataManager:
    def __init__(self):
        cursor = get_cursor()

        cursor.execute('''
        CREATE TABLE IF NOT EXISTS live_status (
            liveid TEXT,
            islive INT,
            PRIMARY KEY (liveid)
        )
        ''')
   
    def get_live_status(self, liveid):
        cursor = get_cursor()
        cursor.execute(
            'SELECT islive FROM live_status WHERE liveid = ?',
            (liveid, )
        )
        if result := cursor.fetchone():
            return result[0]
        return 0

    def set_live_status(self, liveid, status):
        cursor = get_cursor()
        cursor.execute(
            'INSERT OR REPLACE INTO live_status (liveid, islive) VALUES (?, ?)',
            (liveid, status)
        )
        
db = DataManager()

livestate = on_command("直播状态", priority=10, block=True)

async def get_live_status(liveid):
    await asyncio.sleep(1)
    if liveid.startswith("dy_"):
        res = requests.get("https://www.doseeing.com/room/"+liveid.split('_')[1])
        islive = int('<span>直播中</span>' in res.text)
        nickname = re.findall(r'<title>(.*?)</title>', res.text, re.IGNORECASE)[1][:-10]
        return islive, nickname
    if liveid.startswith("bili_"):
        headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.111 Safari/537.36"}
        res = requests.get("https://api.live.bilibili.com/room/v1/Room/room_init?id="+liveid.split('_')[1], headers=headers)
        await asyncio.sleep(1)
        islive = int(res.json()['data']['live_status'] == 1)
        uid = res.json()['data']['uid']
        res = requests.get("https://api.live.bilibili.com/live_user/v1/Master/info?uid="+str(uid), headers=headers)
        nickname = res.json()['data']['info']['uname']
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
        if islive == 1 and db.get_live_status(liveid) == 0:
            for groupid in config.cs_group_list:
                await bot.send_msg(
                    message_type="group",
                    group_id=groupid,
                    message=f"{nickname} 开播了"
                )
        db.set_live_status(liveid, islive)
    global now_live_state
    now_live_state = new_live_state.strip()


@livestate.handle()
async def livestate_function():
    await livestate.finish(now_live_state)