from nonebot import get_plugin_config
from nonebot.plugin import PluginMetadata
from nonebot.adapters.onebot.v11 import Message, MessageEvent, MessageSegment
from nonebot.params import CommandArg
from nonebot import on_command
from nonebot.permission import SUPERUSER
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
        result = await db_upd.update_stats(steamid)
        if result[0]:
            await update.send(f"{result[1]} 成功更新 {result[2]} 场完美数据, {result[3]} 场官匹数据")
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
                time_type = config.default_time
                if len(cmd) >= 2:
                    time_type = cmd[1]
                if time_type not in config.allowed_time:
                    await rank.finish(f"{rank_type} 仅支持 {config.allowed_time}")
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
                datas = sorted(datas, key=lambda x: x[1][0], reverse=config.reversed)
                if len(datas) == 0:
                    await rank.finish("没有人类了")
                max_value = datas[0][1][0] if config.reversed else datas[-1][1][0]
                min_value = datas[-1][1][0] if config.reversed else datas[0][1][0]
                if max_value == 0 and rank_type == "胜率":
                    await rank.finish("啊😰device😱啊这是人类啊😩哦，bro也没杀人😩这局...这局没有人类了😭只有🐍只有🐭，只有沟槽的野榜😭只有...啊！！！😭我在看什么😭我🌿你的😫🖐🏻️🎧")
                min_value, max_value = config.range_gen.getval(min_value, max_value)
                print(min_value, max_value)
                image = None
                if config.template == 1:
                    image = await gen_rank_image1(datas, min_value, max_value, f"{time_type} {config.title}", config.outputfmt)
                elif config.template == 2:
                    image = await gen_rank_image2(datas, min_value, max_value, f"{time_type} {config.title}", config.outputfmt)
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
    cntwm = 0
    cntgp = 0
    for steamid in db_val.get_all_steamid():
        result = await db_upd.update_stats(steamid)
        cntwm += result[2]
        cntgp += result[3]
    await updateall.finish(f"更新完成 {cntwm} 场完美数据 {cntgp} 场官匹数据")

