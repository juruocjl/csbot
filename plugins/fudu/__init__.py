from nonebot import get_plugin_config
from nonebot.plugin import PluginMetadata
from nonebot.adapters.onebot.v11 import GroupMessageEvent, NoticeEvent, Message, MessageSegment, Bot
from nonebot import logger
from nonebot import on_command, on_notice, on_message
from nonebot import require
from nonebot.permission import SUPERUSER
from nonebot import get_bot

require("nonebot_plugin_apscheduler")
from nonebot_plugin_apscheduler import scheduler

require("allmsg")
from ..allmsg import db as db_all
from ..allmsg import myallmsg

require("cs_db_val")
from ..cs_db_val import db as db_val
from ..cs_db_val import NoValueError

require("utils")
from ..utils import local_storage
from ..utils import Base, async_session_factory
from ..utils import get_today_start_timestamp, get_session, getcard

import math
import random
import time
from pathlib import Path
from sqlalchemy import String, Integer, LargeBinary, select, func, desc, text
from sqlalchemy.orm import Mapped, mapped_column
import json
import asyncio
from .config import Config


class FuduPoint(Base):
    __tablename__ = "fudu_points"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True, init=False)
    uid: Mapped[str] = mapped_column(String(50))
    timestamp: Mapped[int] = mapped_column(Integer, name="timeStamp")
    point: Mapped[int] = mapped_column(Integer)


class DataManager:
    async def add_point(self, uid: str, point: int):
        timestamp = int(time.time())
        async with async_session_factory() as session:
            async with session.begin():
                new_point = FuduPoint(uid=uid, timestamp=timestamp, point=point)
                session.add(new_point)

    async def get_point(self, uid: str, day: int = 0) -> int:
        starttime = get_today_start_timestamp(refreshtime=86100) - day * 86400
        endtime = starttime + 86400
        
        async with async_session_factory() as session:
            stmt = select(func.sum(FuduPoint.point)).where(
                FuduPoint.uid == uid,
                FuduPoint.timestamp >= starttime,
                FuduPoint.timestamp < endtime
            )
            result = await session.execute(stmt)
            total = result.scalar() # scalar() 获取第一行第一列
            return total if total is not None else 0

    async def get_zero_point(self, uid: str, day: int = 0) -> int:
        starttime = get_today_start_timestamp(refreshtime=86100) - day * 86400
        endtime = starttime + 86400

        async with async_session_factory() as session:
            # 对应 SQL: SELECT COUNT(point) ... WHERE point == 0
            stmt = select(func.count(FuduPoint.point)).where(
                FuduPoint.uid == uid,
                FuduPoint.timestamp >= starttime,
                FuduPoint.timestamp < endtime,
                FuduPoint.point == 0
            )
            result = await session.execute(stmt)
            count = result.scalar()
            return count if count is not None else 0

db = DataManager()

__plugin_meta__ = PluginMetadata(
    name="fudu",
    description="",
    usage="",
    config=Config,
)

config = get_plugin_config(Config)

fudupoint = on_command("复读点数", priority=10, block=True)

fuduhelp = on_command("复读帮助", priority=10, block=True)

roll = on_command("roll", priority=10, block=True, permission=SUPERUSER)

pointrank = on_command("点数排行", priority=10, block=True)


def bancheck(event: NoticeEvent):
    return event.get_event_name().startswith("notice.group_ban.")

admincheck = on_notice(bancheck, priority=100, block=False)

@fuduhelp.handle()
async def fuduhelp_function():
    await fuduhelp.finish("""禁言概率公式：max(0.02,tanh((本句点数*累计点数-50)/500))
管理员被撤销概率公式：max(0,tanh((本句点数*累计点数/100-50)/500))
复读自己/使用poke5，第一遍复读1，二遍复读2，之后复读3，禁言点数为禁言时长（单位秒），取消禁言为50""")

def sigmoid_step(x, admin = False):
    if admin:
        t = (x / 100 - 50) / 500.0
        return max(0, math.tanh(t))
    else:
        t = (x - 50) / 500.0
        return max(0.02, math.tanh(t))

@fudupoint.handle()
async def fudupoint_function(message: GroupMessageEvent):
    uid = message.get_user_id()
    sid = message.get_session_id()
    assert(sid.startswith("group"))
    gid = sid.split('_')[1]
    for seg in message.get_message():
        if seg.type == "at":
            uid = seg.data["qq"]
    admin = False
    if uid == await local_storage.get(f'adminqq{gid}') and int(await local_storage.get(f'adminqqalive{gid}', "0")):
        admin = True
    point = await db.get_point(f"group_{gid}_{uid}")
    prob1 = sigmoid_step((point + 1), admin=admin)
    prob2 = sigmoid_step((point + 2) * 2, admin=admin)
    prob3 = sigmoid_step((point + 3) * 3, admin=admin)
    prob5 = sigmoid_step((point + 5) * 5, admin=admin)
    tm = await db.get_zero_point(f"group_{gid}_{uid}") + 1
    if admin:
        await fudupoint.finish(f"[管理员]当前点数：{point}  下一次被下放\n点数：复读自己5({prob5:.2f})，第一遍复读1({prob1:.2f})，二遍复读2({prob2:.2f})，之后复读3({prob3:.2f})")
    else:
        await fudupoint.finish(f"当前点数：{point}  下一次禁言时间：{tm}min\n点数：复读自己5({prob5:.2f})，第一遍复读1({prob1:.2f})，二遍复读2({prob2:.2f})，之后复读3({prob3:.2f})")

async def addpoint(gid: str, uid: str, nowpoint: int) -> bool:
    bot = get_bot()
    assert isinstance(bot, Bot)
    
    sid = f"group_{gid}_{uid}"
    if uid == await local_storage.get(f'adminqq{gid}') and int(await local_storage.get(f'adminqqalive{gid}', "0")):
        await db.add_point(sid, nowpoint)
        prob = sigmoid_step(nowpoint * await db.get_point(sid), admin=True)
        if random.random() < prob:
            await local_storage.set(f'adminqqalive{gid}', '0')
            await bot.set_group_admin(group_id=int(gid), user_id=int(uid), enable=False)
            await bot.send_group_msg(group_id=int(gid), message=f"管理员" + MessageSegment.at(uid) + f" 以概率{prob:.2f}被下放")
            return True
    else:
        await db.add_point(sid, nowpoint)
        prob = sigmoid_step(nowpoint * await db.get_point(sid), admin=False)
        if random.random() < prob:
            await db.add_point(sid, 0)
            tm = await db.get_zero_point(sid)
            await bot.set_group_ban(group_id=int(gid), user_id=int(uid), duration=60 * tm)
            await bot.send_group_msg(group_id=int(gid), message="恭喜" + MessageSegment.at(uid) + f" 以概率{prob:.2f}被禁言{tm}分钟")
            return True
    return False

lastmsg: dict[str, list[tuple[str, Message, str]]] = {}

def checksb(message: Message):
    text = message.extract_plain_text().strip().lower()
    if text == "sb" or text == "傻逼" or text == "艾斯比":
        atset = set()
        for seg in message:
            if seg.type == "at":
                atset.add(seg.data["qq"])
        if len(atset) == 1:
            return (True, list(atset)[0])
    return (False, None)

msg_lock = asyncio.Lock()

@myallmsg.handle()
async def fuducheck_function(bot: Bot, message: GroupMessageEvent, mhs: str) -> None:
    uid = message.get_user_id()
    sid = message.get_session_id()
    assert(sid.startswith("group"))
    msg = message.original_message
    nowpoint = 0
    logger.info(f"{uid} send {msg} with {mhs}")
    gid = sid.split('_')[1]
    text = msg.extract_plain_text().lower().strip()
    if text == "gsm" or text == "干什么":
        await bot.send_group_msg(group_id=int(gid), message=Message(MessageSegment.record(Path("assets") / "gsm.mp3")))
    if text == "mbf" or text == "没办法":
        await bot.send_group_msg(group_id=int(gid), message=Message(MessageSegment.record(Path("assets") / "mbf.mp3")))
    need_send = False
    issb, whosb = checksb(msg)
    async with msg_lock:
        msglst = []
        if gid in lastmsg:
            msglst = lastmsg[gid]
        if issb:
            nowpoint += 5

        if len(msglst) > 0 and msglst[0][2] == mhs and (uid in [a[0] for a in msglst]):
            nowpoint += 5
        else:
            msglst.append((uid, msg, mhs))
            if len(msglst) > 1 and msglst[-1][2] != msglst[-2][2]:
                msglst = msglst[-1:]
            nowpoint += min(3, len(msglst) - 1)
            if len(msglst) == 3:
                need_send = True
        lastmsg[gid] = msglst
    if need_send:
        await bot.send_group_msg(group_id=int(gid), message=msg)
        if issb:
            tm = await db.get_zero_point(f"group_{gid}_{whosb}") + 1
            await bot.set_group_ban(group_id=int(gid), user_id=int(whosb), duration=60 * tm)
    if nowpoint > 0:
        await addpoint(gid, uid, nowpoint)

@admincheck.handle()
async def admincheck_function(bot: Bot, notice: NoticeEvent):
    logger.info(notice.get_event_description())
    # print(notice.get_event_name(), notice.get_event_description())
    myid = str((await bot.get_login_info())['user_id'])
    data = json.loads(notice.get_event_description().replace("'", '"'))
    uid = str(data['user_id'])
    gid = str(data['group_id'])
    o_uid = str(data['operator_id'])
    duration = data['duration']
    if myid == o_uid:
        return
    # print(uid, gid, duration, data['sub_type'])
    if duration:
        if await addpoint(gid, o_uid, duration):
            await bot.set_group_ban(group_id=int(gid), user_id=int(uid), duration=0)
    else:
        await addpoint(gid, o_uid, 50)

async def calc_roll_point(groupid: str, time_type: str, day:int = 1) -> list[tuple[int, str, float]]:
    adminuid = None
    if await local_storage.get(f'adminqq{groupid}'):
        adminuid = int(await local_storage.get(f'adminqq{groupid}', "0"))
    users = []
    
    sid_list = await db_all.get_active_user(groupid)

    ttconfig = db_val.get_value_config("场次")
    gpconfig = db_val.get_value_config("gp场次")
    nzconfig = db_val.get_value_config("内战场次")


    for sid in sid_list:
        sum_point = await db.get_point(sid, day = day)
        cnt_ban = await db.get_zero_point(sid, day = day)
        userid = int(sid.split('_')[2])
        if userid != adminuid:
            if steamid := await db_val.get_steamid(str(userid)):
                try:
                    ttcount = (await ttconfig.func(steamid, time_type))[0]
                except NoValueError:
                    ttcount = 0
                try:
                    gpcount = (await gpconfig.func(steamid, time_type))[0]
                except NoValueError:
                    gpcount = 0
                try:
                    nzcount = (await nzconfig.func(steamid, time_type))[0]
                except NoValueError:
                    nzcount = 0
                point = (sum_point / (cnt_ban + 1) + 1) * (math.log(1 + ttcount + 0.6 * gpcount + 0.3 * nzcount))
                users.append((userid, f"({sum_point}/{cnt_ban+1}+1)*log({1 + ttcount + 0.6 * gpcount + 0.3 * nzcount})", point))
    return users

async def get_roll_point_text(bot: Bot, groupid: str, users: list[tuple[int, str, float]]) -> str:
    text = "得分：\n"
    users.sort(key=lambda x: x[2], reverse=True)
    for uid, expr, point in users:
        text += f"{await getcard(bot, groupid, str(uid))}: {expr}={point:.2f}\n"
    return text.strip()

async def roll_admin(groupid: str):
    bot = get_bot()
    assert isinstance(bot, Bot)
    
    if time.time() - get_today_start_timestamp() < 86100:
        time_type = "昨日"
    else:
        time_type = "今日"
    
    if int(await local_storage.get(f'adminqqalive{groupid}', "0")):
        await bot.set_group_admin(group_id=int(groupid), user_id=int(await local_storage.get(f'adminqq{groupid}', '0')), enable=False)
    
    users = await calc_roll_point(groupid, time_type, 1)
    text = await get_roll_point_text(bot, groupid, users)
    weights = [point for _, _, point in users]

    newadmin, pointmsg, point = random.choices(users, weights=weights, k=1)[0]
    totsum = sum(weights)
    
    await bot.send_group_msg(group_id=int(groupid), message='恭喜' + MessageSegment.at(newadmin) + f" 以{point:.2f}/{totsum:.2f}选为管理员")
    await bot.send_group_msg(group_id=int(groupid), message=text)
    await local_storage.set(f'adminqq{groupid}', str(newadmin))
    await local_storage.set(f'adminqqalive{groupid}', '1')
    await bot.set_group_admin(group_id=int(groupid), user_id=int(newadmin), enable=True)

@pointrank.handle()
async def pointrank_function(bot: Bot, message: GroupMessageEvent):
    sid = message.get_session_id()
    assert(sid.startswith("group"))
    gid = sid.split('_')[1]
    users = await calc_roll_point(gid, "今日", 0)
    text = await get_roll_point_text(bot, gid, users)
    await pointrank.finish(text.strip())

@roll.handle()
async def roll_function(message: GroupMessageEvent):
    sid = message.get_session_id()
    assert(sid.startswith("group"))
    await roll_admin(sid.split('_')[1])

@scheduler.scheduled_job("cron", hour="23", minute="55", id="roll")
async def autoroll():
    logger.info("start roll")
    for group in config.cs_group_list:
        await roll_admin(str(group))