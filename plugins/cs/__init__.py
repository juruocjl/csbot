from nonebot import get_plugin_config
from nonebot.plugin import PluginMetadata
from nonebot.adapters.onebot.v11 import Message, MessageEvent, MessageSegment
from nonebot.params import CommandArg
from nonebot import on_command
from nonebot.permission import SUPERUSER
from nonebot import get_bot
from nonebot import require
from nonebot import logger

scheduler = require("nonebot_plugin_apscheduler").scheduler

get_cursor = require("utils").get_cursor
output = require("utils").output
get_today_start_timestamp = require("utils").get_today_start_timestamp

gen_rank_image1 = require("cs_img").gen_rank_image1
gen_rank_image2 = require("cs_img").gen_rank_image2
gen_matches_image = require("cs_img").gen_matches_image
gen_stats_image = require("cs_img").gen_stats_image

db_upd = require("cs_db_upd").db
db_val = require("cs_db_val").db
valid_time = require("cs_db_val").valid_time
valid_rank = require("cs_db_val").valid_rank
rank_config = require("cs_db_val").rank_config


from .config import Config
config = get_plugin_config(Config)

import re
import os


if not os.path.exists("avatar"):
    os.makedirs("avatar", exist_ok=True)


__plugin_meta__ = PluginMetadata(
    name="cs",
    description="",
    usage="",
    config=Config,
)


bind = on_command("绑定", priority=10, block=True)

unbind = on_command("解绑", priority=10, block=True)

update = on_command("更新数据", priority=10, block=True)

show = on_command("查看数据", priority=10, block=True)

rank = on_command("排名", priority=10, block=True)

updateall = on_command("全部更新", priority=10, block=True, permission=SUPERUSER)

matches = on_command("记录", priority=10, block=True)

weekreport = on_command("周报", priority=10, block=True)

dayreport = on_command("日报", priority=10, block=True)




@bind.handle()
async def bind_function(message: MessageEvent, args: Message = CommandArg()):
    uid = message.get_user_id()
    sid = message.get_session_id()
    print("user: %s\nsession: %s\n" % (uid, sid))
    if (steamid := args.extract_plain_text()) and re.match(r'^\d{17}$', steamid):
        db_upd.bind(uid, steamid)
        await bind.finish(f"成功绑定{steamid}。你可以使用 /更新数据 获取战绩。")
    else:
        await bind.finish("请输入steamid64，应该是一个17位整数。你可以使用steamidfinder等工具找到此值。")

@unbind.handle()
async def unbind_function(message: MessageEvent):
    uid = message.get_user_id()
    sid = message.get_session_id()
    print("user: %s\nsession: %s\n" % (uid, sid))
    db_upd.unbind(uid)
    await unbind.finish(f"解绑成功。")

@update.handle()
async def update_function(message: MessageEvent):
    uid = message.get_user_id()
    sid = message.get_session_id()

    db_upd.add_member(sid, uid)

    print("user: %s\nsession: %s\n" % (uid, sid))
    steamid = db_val.get_steamid(uid)
    if steamid != None:
        print(f"更新{steamid}战绩")
        result = db_upd.update_stats(steamid)
        if result[0]:
            await update.send(f"{result[1]} 成功更新 {result[2]} 场数据")
            result = db_val.get_stats(steamid)
            image = await gen_stats_image(result)
            await update.finish(MessageSegment.image(image))
        else:
            await update.finish(result[1])
    else:
        await update.finish("请先使用 /绑定 steamid64 绑定")

@show.handle()
async def show_function(message: MessageEvent, args: Message = CommandArg()):
    uid = message.get_user_id()
    sid = message.get_session_id()
    
    db_upd.add_member(sid, uid)
    print("user: %s\nsession: %s\n" % (uid, sid))
    steamid = db_val.get_steamid(uid)
    if user := db_val.work_msg(args):
        print(user)
        if result := db_val.search_user(user):
            await show.send(f"找到用户 {result[1]}")
            steamid = result[0]
        else:
            await show.finish(f"未找到用户")
    if steamid != None:
        print(f"查询{steamid}战绩")
        result = db_val.get_stats(steamid)
        if result:
            image = await gen_stats_image(result)
            await show.finish(MessageSegment.image(image))
        else:
            await show.finish("请先使用 /更新数据 更新战绩")
    else:
        await show.finish("请先使用 /绑定 steamid64 绑定或者指定用户")

@rank.handle()
async def rank_function(message: MessageEvent, args: Message = CommandArg()):
    uid = message.get_user_id()
    sid = message.get_session_id()

    text = args.extract_plain_text()
    steamids = db_val.get_member_steamid(sid)

    if text:
        cmd = text.split()
        if len(cmd) > 0:
            rank_type = cmd[0]
            if rank_type in valid_rank:
                index = valid_rank.index(rank_type)
                config = rank_config[index]
                time_type = config[2]
                if len(cmd) >= 2:
                    time_type = cmd[1]
                if time_type in valid_time:
                    if config[3] and time_type != config[2]:
                        await rank.finish(f"{rank_type} 仅支持 {config[2]}")
                    datas = []
                    for steamid in steamids:
                        try:
                            val = db_val.get_value(steamid, rank_type, time_type)
                            print(val)
                            datas.append((steamid, val))
                        except ValueError as e:
                            print(e)
                            pass
                    print(datas)
                    datas = sorted(datas, key=lambda x: x[1][0], reverse=config[4])
                    if len(datas) == 0:
                        await rank.finish("没有人类了")
                    max_value = datas[0][1][0] if config[4] else datas[-1][1][0]
                    min_value = datas[-1][1][0] if config[4] else datas[0][1][0]
                    if max_value == 0 and rank_type == "胜率":
                        await rank.finish("啊😰device😱啊这是人类啊😩哦，bro也没杀人😩这局...这局没有人类了😭只有🐍只有🐭，只有沟槽的野榜😭只有...啊！！！😭我在看什么😭我🌿你的😫🖐🏻️🎧")
                    min_value, max_value = config[5].getval(min_value, max_value)
                    print(min_value, max_value)
                    image = None
                    if config[7] == 1:
                        image = await gen_rank_image1(datas, min_value, max_value, f"{time_type} {config[1]}", config[6])
                    elif config[7] == 2:
                        image = await gen_rank_image2(datas, min_value, max_value, f"{time_type} {config[1]}", config[6])
                    await rank.finish(MessageSegment.image(image))

    await rank.finish(f"请使用 /排名 [选项] (时间) 生成排名。\n可选 [选项]：{valid_rank}\n可用 (时间)：{valid_time}")
        
@matches.handle()
async def matches_function(message: MessageEvent, args: Message = CommandArg()):
    uid = message.get_user_id()
    sid = message.get_session_id()

    text = db_val.work_msg(args)

    steamid = db_val.get_steamid(uid)
    time_type = "全部"

    if text:
        cmd = text.split()
        if len(cmd) == 1:
            if cmd[0] not in valid_time:
                if result := db_val.search_user(cmd[0]):
                    await matches.send(f"找到用户 {result[1]}")
                    steamid = result[0]
                else:
                    await matches.finish(f"未找到用户")
            else:
                time_type = cmd[0]
        elif len(cmd) > 1:
            if result := db_val.search_user(cmd[0]):
                await matches.send(f"找到用户 {result[1]}")
                steamid = result[0]
            else:
                await matches.finish(f"未找到用户")
            if cmd[1] not in valid_time:
                await matches.finish(f"非法的时间")
            else:
                time_type = cmd[1]
    if steamid != None:
        print(steamid, time_type)
        result = db_val.get_matches(steamid, time_type)
        if result:
            image = await gen_matches_image(result, steamid, db_val.get_stats(steamid)[2])
            await matches.finish(MessageSegment.image(image))
        else:
            await matches.finish("未找到比赛")
    else:
        await matches.finish("请先使用 /绑定 steamid64 绑定或者指定用户")

@updateall.handle()
async def updateall_function():
    await updateall.send("开始更新所有数据")
    qwq = []
    for steamid in db_val.get_all_steamid():
        result = db_upd.update_stats(steamid)
        if result[0] and result[2] != 0:
            qwq.append(result[1:])
    await updateall.finish(f"更新完成 {qwq}")


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
