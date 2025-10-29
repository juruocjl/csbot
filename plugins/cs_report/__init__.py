from nonebot import get_plugin_config
from nonebot.plugin import PluginMetadata
from nonebot.adapters.onebot.v11 import Message, MessageEvent, MessageSegment
from nonebot.params import CommandArg
from nonebot import on_command
from nonebot import get_bot
from nonebot import logger
from nonebot import require

scheduler = require("nonebot_plugin_apscheduler").scheduler

output = require("utils").output

db_upd = require("cs_db_upd").db
db_val = require("cs_db_val").db

from .config import Config

__plugin_meta__ = PluginMetadata(
    name="cs_report",
    description="",
    usage="",
    config=Config,
)

config = get_plugin_config(Config)


weekreport = on_command("周报", priority=10, block=True)

dayreport = on_command("日报", priority=10, block=True)



def get_report_part(rank_type, time_type, steamids, reverse, fmt, n=3, filter = lambda x: True):
    prize_name = "🥇🥈🥉456789"
    datas = []
    for steamid in steamids:
        try:
            val = db_val.get_value(steamid, rank_type, time_type)
            if filter(val[0]):
                datas.append((steamid, val))
        except ValueError as e:
            pass
    datas = sorted(datas, key=lambda x: x[1][0], reverse=reverse)
    if len(datas) == 0:
        return "没有人类了\n"
    rk = [0] * len(datas)
    for i in range(1, len(datas)):
        if datas[i][1][0] == datas[i-1][1][0]:
            rk[i] = rk[i-1]
        else:
            rk[i] = i
    result = ""
    for i in range(len(datas)):
        if rk[i] < n:
            result += prize_name[rk[i]] + ". " + db_val.get_stats(datas[i][0])[2] + " " + output(datas[i][1][0], fmt) + "\n"
    return result

def get_report(time_type, steamids):
    result = ""
    result += "= 场次榜 =\n" + get_report_part("场次", time_type, steamids, True, "d0")
    result += "= 高手榜 =\n" + get_report_part("rt", time_type, steamids, True, "d2", filter = lambda x: x > 1)
    result += "= 菜逼榜 =\n" + get_report_part("rt", time_type, steamids, False, "d2", filter = lambda x: x < 1)
    result += "= 演员榜 =\n" + get_report_part("演员", time_type, steamids, False, "d2", filter = lambda x: x < 1)
    result += "= 上分榜 =\n" + get_report_part("上分", time_type, steamids, True, "d0", filter = lambda x: x > 0)
    result += "= 掉分榜 =\n" + get_report_part("上分", time_type, steamids, False, "d0", filter = lambda x: x < 0)
    result += "= 本周受益者 = " + get_report_part("受益", "本周", steamids, True, "p2", n=1, filter = lambda x: x > 0)
    result += "= 本周受害者 = " + get_report_part("受益", "本周", steamids, False, "p2", n=1, filter = lambda x: x < 0)

    return result

@weekreport.handle()
async def weekreport_function(message: MessageEvent):
    sid = message.get_session_id()
    steamids = db_val.get_member_steamid(sid)
    await weekreport.finish("== 周报 ==\n" + get_report("本周", steamids))
    
@dayreport.handle()
async def dayreport_function(message: MessageEvent):
    sid = message.get_session_id()
    steamids = db_val.get_member_steamid(sid)
    await weekreport.finish("== 日报 ==\n" + get_report("今日", steamids))

@scheduler.scheduled_job("cron", hour="23", minute="30", id="dayreport")
async def send_day_report():
    for steamid in db_val.get_all_steamid():
        result = db_upd.update_stats(steamid)
    bot = get_bot()
    for groupid in config.cs_group_list:
        steamids = db_val.get_member_steamid(f"group_{groupid}")
        await bot.send_msg(
            message_type="group",
            group_id=groupid,
            message="== 23:30自动日报 ==\n" + get_report("今日", steamids)
        )

@scheduler.scheduled_job("cron", day_of_week="sun", hour="23", minute="45", id="weekreport")
async def send_week_report():
    bot = get_bot()
    for groupid in config.cs_group_list:
        steamids = db_val.get_member_steamid(f"group_{groupid}")
        await bot.send_msg(
            message_type="group",
            group_id=groupid,
            message="== 周日23:45自动周报 ==\n" + get_report("本周", steamids)
        )
