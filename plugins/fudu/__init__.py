from nonebot import get_plugin_config
from nonebot.plugin import PluginMetadata
from nonebot.adapters.onebot.v11 import GroupMessageEvent, NoticeEvent, Message, MessageSegment, Bot
from nonebot import logger
from nonebot import on_command, on_notice, on_message
from nonebot import require
from nonebot.params import CommandArg
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

require("models")
from ..models import FuduPoint

require("utils")
from ..utils import local_storage
from ..utils import async_session_factory
from ..utils import get_today_start_timestamp, getcard

import math
import random
import time
from pathlib import Path
from sqlalchemy import select, func
import json
import asyncio
from typing import Optional
from pydantic import BaseModel
from .config import Config




class DataManager:
    async def add_point(self, uid: str, point: int, pointType: int = 0) -> None:
        timestamp = int(time.time())
        async with async_session_factory() as session:
            async with session.begin():
                new_point = FuduPoint(uid=uid, timestamp=timestamp, point=point, pointType=pointType)
                session.add(new_point)

    async def get_point(self, uid: str, day: int = 0) -> int:
        starttime = get_today_start_timestamp(refreshtime=86100) - day * 86400
        endtime = starttime + 86400
        
        async with async_session_factory() as session:
            stmt = select(func.sum(FuduPoint.point)).where(
                FuduPoint.uid == uid,
                FuduPoint.timestamp >= starttime,
                FuduPoint.timestamp < endtime,
                FuduPoint.pointType == 0,
            )
            result = await session.execute(stmt)
            total = result.scalar() # scalar() 获取第一行第一列
            return int(total) if total else 0

    async def get_zero_point(self, uid: str, day: int = 0) -> int:
        starttime = get_today_start_timestamp(refreshtime=86100) - day * 86400
        endtime = starttime + 86400

        async with async_session_factory() as session:
            # 对应 SQL: SELECT COUNT(point) ... WHERE point == 0
            stmt = select(func.count(FuduPoint.point)).where(
                FuduPoint.uid == uid,
                FuduPoint.timestamp >= starttime,
                FuduPoint.timestamp < endtime,
                FuduPoint.point == 0,
                FuduPoint.pointType == 0,
            )
            result = await session.execute(stmt)
            count = result.scalar()
            return count if count is not None else 0
    
    async def get_award_point(self, uid: str, day: int = 0) -> int:
        starttime = get_today_start_timestamp(refreshtime=86100) - day * 86400
        endtime = starttime + 86400

        async with async_session_factory() as session:
            stmt = select(func.sum(FuduPoint.point)).where(
                FuduPoint.uid == uid,
                FuduPoint.timestamp >= starttime,
                FuduPoint.timestamp < endtime,
                FuduPoint.pointType == 1,
            )
            result = await session.execute(stmt)
            total = result.scalar()
            return int(total) if total else 0

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

setcard = on_command("设置昵称", priority=10, block=True)

def bancheck(event: NoticeEvent):
    return event.get_event_name().startswith("notice.group_ban.")

admincheck = on_notice(bancheck, priority=100, block=False)

@fuduhelp.handle()
async def fuduhelp_function():
    await fuduhelp.finish(f"""禁言概率公式：max(0.02,tanh((本句点数*累计点数-50)/500))
管理员被撤销概率公式：max(0,tanh((本句点数*累计点数/100-50)/500))
复读自己/使用poke5，第一遍复读1，二遍复读2，之后复读3，禁言点数为禁言时长（单位秒），取消禁言为50。
复读时会给第一个发消息的人加一点奖励点数，奖励点数不参与概率计算。骂sb额外增加5点，3条相同会给@对象禁言。
管理员可以使用/设置昵称 @人 名称 来给群成员设置昵称，管理员被撤销后设置的昵称会失效，点数为 20 点。
若两条相同消息间隔不超过{config.cs_fudu_delay}s，即使中间有其他消息，也会被认为是复读。
管理员roll点权重为 (常规点数/(禁言次数+1)+奖励点数+1)*log(1+天梯场次+0.6*官匹场次+0.3*内战场次)
""")

def sigmoid_step(x, admin = False):
    x = float(x)  # 确保 x 是 float 类型，处理 Decimal 等其他数值类型
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
    award_point = await db.get_award_point(f"group_{gid}_{uid}")
    prob1 = sigmoid_step((point + 1), admin=admin)
    prob2 = sigmoid_step((point + 2) * 2, admin=admin)
    prob3 = sigmoid_step((point + 3) * 3, admin=admin)
    prob5 = sigmoid_step((point + 5) * 5, admin=admin)
    tm = await db.get_zero_point(f"group_{gid}_{uid}") + 1
    if admin:
        await fudupoint.finish(f"[管理员]当前点数：{point} + {award_point}  下一次被下放\n点数：复读自己5({prob5:.2f})，第一遍复读1({prob1:.2f})，二遍复读2({prob2:.2f})，之后复读3({prob3:.2f})")
    else:
        await fudupoint.finish(f"当前点数：{point} + {award_point}  下一次禁言时间：{tm}min\n点数：复读自己5({prob5:.2f})，第一遍复读1({prob1:.2f})，二遍复读2({prob2:.2f})，之后复读3({prob3:.2f})")

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

async def add_award_point(gid: str, uid: str, point: int) -> None:
    sid = f"group_{gid}_{uid}"
    await db.add_point(sid, point, pointType=1)

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

class MessageClass:
    def __init__(self, msg: Message, uid: str, mid: int, time: int):
        self.uids: list[str] = [uid]
        self.msg: Message = msg
        self.last_mid: int = mid
        self.last_time: int = time
    
    def add_uid(self, uid: str, mid: int, time: int) -> tuple[Optional[str], bool, int]:
        self.last_mid = mid
        self.last_time = time
        if uid not in self.uids:
            self.uids.append(uid)
            return self.uids[0], len(self.uids) == 3, len(self.uids) - 1
        else:
            return None, False, 5

    def print(self) -> None:
        print(f"msg: {self.msg}, uids: {self.uids}, last_mid: {self.last_mid}, last_time: {self.last_time}")

class GroupMessageManager:
    def __init__(self):
        self.last_mid: int = 0
        self.msg_dict: dict[str, MessageClass] = {}
    
    def flush(self):
        nowtime = int(time.time())
        self.msg_dict = {
            k: v for k, v in self.msg_dict.items() 
            if nowtime - v.last_time < config.cs_fudu_delay or v.last_mid == self.last_mid
        }

    def __call__(self, mhs: str, msg: Message, uid: str,  mid: int, time_stamp: int) -> tuple[Optional[str], bool, int]:
        self.flush()
        self.last_mid = mid
        if mhs not in self.msg_dict:
            self.msg_dict[mhs] = MessageClass(msg, uid, mid, time_stamp)
            return None, False, 0
        else:
            return self.msg_dict[mhs].add_uid(uid, mid, time_stamp)
    
    def print(self) -> None:
        print(f"last_mid: {self.last_mid}")
        for k, v in self.msg_dict.items():
            print(f"mhs: {k}")
            v.print()

group_message_managers: dict[str, GroupMessageManager] = {}

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
        if issb:
            nowpoint += 5
        if gid not in group_message_managers:
            group_message_managers[gid] = GroupMessageManager()
        addpointuid, need_send, addpointnum = group_message_managers[gid](
            mhs, msg, uid, message.message_id, int(time.time()))
        # group_message_managers[gid].print()
        nowpoint += addpointnum
        if addpointuid:
            await add_award_point(gid, addpointuid, 1)
        if nowpoint > 0:
            await addpoint(gid, uid, nowpoint)
    if need_send:
        await bot.send_group_msg(group_id=int(gid), message=msg)
        if issb:
            await db.add_point(f"group_{gid}_{whosb}", 0)
            tm = await db.get_zero_point(f"group_{gid}_{whosb}")
            await bot.set_group_ban(group_id=int(gid), user_id=int(whosb), duration=60 * tm)
            await bot.send_group_msg(group_id=int(gid), message="恭喜 sb" + MessageSegment.at(uid) + f"被禁言{tm}分钟")


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
        award_point = await db.get_award_point(sid, day = day)
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
                point = ((sum_point)/ (cnt_ban + 1) + award_point + 1) * (math.log(1 + ttcount + 0.6 * gpcount + 0.3 * nzcount))
                users.append((userid, f"({sum_point}/{cnt_ban+1}+{award_point}+1)*log({1+ttcount+0.6*gpcount+0.3*nzcount:.1f})", point))
    return users

async def get_roll_point_text(bot: Bot, groupid: str, users: list[tuple[int, str, float]]) -> str:
    text = "得分：\n"
    users.sort(key=lambda x: x[2], reverse=True)
    for uid, expr, point in users:
        text += f"{await getcard(bot, groupid, str(uid))}\n  > {expr}={point:.2f}\n"
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
    msg = await pointrank.send(text.strip())
    await asyncio.sleep(config.cs_fudu_rank_delete_delay)
    await bot.delete_msg(message_id=msg['message_id'])

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


class CardSet(BaseModel):
    group_id: str
    user_id: str
    card: str
    admin_uid: str

class CardManager(BaseModel):

    nickname_sets: list[CardSet] = []
    
    def flush(self, group_id: str, admin_uid: Optional[str] = None):
        self.nickname_sets = [s for s in self.nickname_sets if s.group_id != group_id or s.admin_uid == admin_uid]
    
    def set_card(self, group_id: str, user_id: str, card: str, admin_uid: str) -> None:
        self.flush(group_id, admin_uid)
        
        if card == "":
            self.nickname_sets = [s for s in self.nickname_sets if s.group_id != group_id or s.user_id != user_id]
            return

        for s in self.nickname_sets:
            if s.group_id == group_id and s.user_id == user_id:
                s.card = card
                return
            
        self.nickname_sets.append(CardSet(group_id=group_id, user_id=user_id, card=card, admin_uid=admin_uid))

INIT_CARD_MANAGER = CardManager().model_dump_json()

card_lock = asyncio.Lock()

@setcard.handle()
async def setcard_function(bot: Bot, message: GroupMessageEvent, args: Message = CommandArg()):
    uid = message.get_user_id()
    sid = message.get_session_id()
    assert(sid.startswith("group"))
    gid = sid.split('_')[1]
    admin = False
    if uid == await local_storage.get(f'adminqq{gid}') and int(await local_storage.get(f'adminqqalive{gid}', "0")):
        admin = True
    if not admin:
        await setcard.finish("只有管理员可以设置昵称")
    text = args.extract_plain_text().strip()
    for seg in args:
        if seg.type == "at":
            setuid = seg.data["qq"]
    if len(text.encode('utf-8')) > 60:
        await setcard.finish("昵称过长")
    async with card_lock:
        card_manager = CardManager.model_validate_json(await local_storage.get("card_manager", INIT_CARD_MANAGER))
        card_manager.set_card(gid, setuid, text, uid)
        await local_storage.set("card_manager", card_manager.model_dump_json())
    await bot.set_group_card(group_id=int(gid), user_id=int(setuid), card=text)
    await setcard.finish("设置成功 " + MessageSegment.at(setuid) + f" 的昵称为 {text}")
    await addpoint(gid, setuid, 20)

@scheduler.scheduled_job("cron", minute="*/5", id="flush_card")
async def flush_card():
    bot = get_bot()
    assert isinstance(bot, Bot)
    card_manager = CardManager.model_validate_json(await local_storage.get("card_manager", INIT_CARD_MANAGER))
    for gid in config.cs_group_list:
        if await local_storage.get(f'adminqq{gid}') and int(await local_storage.get(f'adminqqalive{gid}', "0")):
            admin_uid = await local_storage.get(f'adminqq{gid}')
            card_manager.flush(str(gid), admin_uid)
    for s in card_manager.nickname_sets:
        if await getcard(bot, s.group_id, s.user_id) != s.card:
            await bot.set_group_card(group_id=int(s.group_id), user_id=int(s.user_id), card=s.card)
    