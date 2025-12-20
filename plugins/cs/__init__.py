from nonebot import get_plugin_config
from nonebot.plugin import PluginMetadata
from nonebot.adapters.onebot.v11 import Message, MessageEvent, MessageSegment
from nonebot.params import CommandArg
from nonebot import on_command
from nonebot.permission import SUPERUSER
from nonebot import require
from nonebot import logger

require("nonebot_plugin_apscheduler")
from nonebot_plugin_apscheduler import scheduler

require("cs_img")
from ..cs_img import gen_rank_image2, gen_matches_image, gen_stats_image, gen_teammate_image

require("cs_db_val")
from ..cs_db_val import db as db_val
from ..cs_db_val import valid_time,valid_rank
from ..cs_db_val import NoValueError

require("cs_db_upd")
from ..cs_db_upd import db as db_upd


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

matchteammate = on_command("缘分", priority=10, block=True)

@bind.handle()
async def bind_function(message: MessageEvent, args: Message = CommandArg()):
    uid = message.get_user_id()
    sid = message.get_session_id()
    print("user: %s\nsession: %s\n" % (uid, sid))
    if (steamid := args.extract_plain_text()) and re.match(r'^\d{17}$', steamid):
        await db_upd.bind(uid, steamid)
        await bind.finish(f"成功绑定{steamid}。你可以使用 /更新数据 获取战绩。")
    else:
        await bind.finish("请输入steamid64，应该是一个17位整数。你可以使用steamidfinder等工具找到此值。")

@unbind.handle()
async def unbind_function(message: MessageEvent):
    uid = message.get_user_id()
    sid = message.get_session_id()
    print("user: %s\nsession: %s\n" % (uid, sid))
    await db_upd.unbind(uid)
    await unbind.finish(f"解绑成功。")

@update.handle()
async def update_function(message: MessageEvent):
    uid = message.get_user_id()
    sid = message.get_session_id()

    await db_upd.add_member(sid, uid)

    print("user: %s\nsession: %s\n" % (uid, sid))
    steamid = await db_val.get_steamid(uid)
    if steamid != None:
        print(f"更新{steamid}战绩")
        result = await db_upd.update_stats(steamid)
        if result[0]:
            await update.send(f"{result[1]} 成功更新 {result[2]} 场完美数据, {result[3]} 场官匹数据")
            baseinfo = await db_val.get_base_info(steamid)
            detailinfo = await db_val.get_detail_info(steamid)
            if baseinfo is None or detailinfo is None:
                await update.finish("数据获取失败，请稍后再试")
            image = await gen_stats_image(baseinfo, detailinfo)
            await update.finish(MessageSegment.image(image))
        else:
            await update.finish(result[1])
    else:
        await update.finish("请先使用 /绑定 steamid64 绑定")

@show.handle()
async def show_function(message: MessageEvent, args: Message = CommandArg()):
    uid = message.get_user_id()
    sid = message.get_session_id()
    
    await db_upd.add_member(sid, uid)
    print("user: %s\nsession: %s\n" % (uid, sid))
    steamid = await db_val.get_steamid(uid)
    if user := await db_val.work_msg(args):
        print(user)
        if result := await db_val.search_user(user):
            await show.send(f"找到用户 {result.name}")
            steamid = result.steamid
        else:
            await show.finish(f"未找到用户")
    if steamid != None:
        print(f"查询{steamid}战绩")
        baseinfo = await db_val.get_base_info(steamid)
        detailinfo = await db_val.get_detail_info(steamid)
        if baseinfo is not None and detailinfo is not None:
            image = await gen_stats_image(baseinfo, detailinfo)
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
    steamids = await db_val.get_member_steamid(sid)

    if text:
        cmd = text.split()
        if len(cmd) > 0:
            rank_type = cmd[0]
            time_type = None
            if len(cmd) >= 2:
                time_type = cmd[1]
            try:
                config = db_val.get_value_config(rank_type)
                if time_type is None:
                    time_type = config.default_time
                if time_type not in config.allowed_time:
                    raise ValueError(f"无效的时间范围，支持的有 {config.allowed_time}")
                datas = []
                for steamid in steamids:
                    try:
                        val = await config.func(steamid, time_type)
                        print(val)
                        datas.append((steamid, val))
                    except NoValueError as e:
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
                image = await gen_rank_image2(datas, min_value, max_value, f"{time_type} {config.title}", config.outputfmt)
                await rank.finish(MessageSegment.image(image))
            except ValueError as e:
                await rank.finish(str(e))

    await rank.finish(f"请使用 /排名 [选项] (时间) 生成排名。\n可选 [选项]：{valid_rank}\n可用 (时间)：{valid_time}")
        
@matches.handle()
async def matches_function(message: MessageEvent, args: Message = CommandArg()):
    uid = message.get_user_id()
    sid = message.get_session_id()

    text = await db_val.work_msg(args)

    steamid = await db_val.get_steamid(uid)
    time_type = "全部"

    if text:
        cmd = text.split()
        if len(cmd) == 1:
            if cmd[0] not in valid_time:
                if result := await db_val.search_user(cmd[0]):
                    await matches.send(f"找到用户 {result.name}")
                    steamid = result.steamid
                else:
                    await matches.finish(f"未找到用户")
            else:
                time_type = cmd[0]
        elif len(cmd) > 1:
            if result := await db_val.search_user(cmd[0]):
                await matches.send(f"找到用户 {result.name}")
                steamid = result.steamid
            else:
                await matches.finish(f"未找到用户")
            if cmd[1] not in valid_time:
                await matches.finish(f"非法的时间")
            else:
                time_type = cmd[1]
    if steamid != None:
        print(steamid, time_type)
        matches_data = await db_val.get_matches(steamid, time_type)
        baseinfo = await db_val.get_base_info(steamid)
        if matches_data is not None and baseinfo is not None:
            image = await gen_matches_image(matches_data, steamid, baseinfo.name)
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
    for steamid in await db_val.get_all_steamid():
        result = await db_upd.update_stats(steamid)
        cntwm += result[2]
        cntgp += result[3]
    await updateall.finish(f"更新完成 {cntwm} 场完美数据 {cntgp} 场官匹数据")

@matchteammate.handle()
async def matchteammate_function(message: MessageEvent, args: Message = CommandArg()):
    uid = message.get_user_id()
    for seg in message.get_message():
        if seg.type == "at":
            uid = seg.data["qq"]
    steamid = await db_val.get_steamid(uid)
    if not steamid:
        await matchteammate.finish("该用户未绑定")
    time_type = args.extract_plain_text().strip()
    if not time_type:
        time_type = "本赛季"
    if time_type not in valid_time:
        await matchteammate.finish(f"非法的时间范围，可用时间范围：{valid_time}")
    results = await db_val.get_match_teammate(steamid, time_type, ["场次", "上分", "_上分", "上分2", "_上分2", "WE2", "rt2", "_WE2", "_rt2"])
    # print(results)
    data: list[tuple[str, str, str, str]] = []
    info = [("最爱队友", "一起打了{count}场", lambda x: True),
            ("最佳上分队友", "你上分 {value}（{count}场）", lambda x: x > 0),
            ("最佳掉分队友", "你掉分 {value}（{count}场）", lambda x: x < 0),
            ("最带飞队友", "与你组排上分 {value}（{count}场）", lambda x: x > 0),
            ("最坑飞队友", "与你组排掉分 {value}（{count}场）", lambda x: x < 0),
            ("最强队友(WE)", "一起时WE {value:.2f}（{count}场）", lambda x: x > 8),
            ("最强队友(rt)", "一起时rt {value:.2f}（{count}场）", lambda x: x > 1),
            ("最菜队友(WE)", "一起时WE {value:.2f}（{count}场）", lambda x: x <= 8),
            ("最菜队友(rt)", "一起时rt {value:.2f}（{count}场）", lambda x: x <= 1),
            ]
    for i, (title, fmt, cond) in enumerate(info):
        result = results[i]
        if result is not None and cond(result[1]):
            baseinfo = await db_val.get_base_info(result[0])
            assert baseinfo is not None
            data.append((title, result[0], baseinfo.name, fmt.format(value=result[1], count=result[2])))
        else:
            data.append((title, "", "虚位以待", fmt.format(value=float("nan"), count=float("nan"))))
    image = await gen_teammate_image(steamid, time_type, data)
    await matchteammate.finish(MessageSegment.image(image))
    