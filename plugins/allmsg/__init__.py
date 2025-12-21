from nonebot import get_plugin_config
from nonebot.plugin import PluginMetadata
from nonebot.adapters.onebot.v11 import Message, MessageEvent, GroupMessageEvent, MessageSegment, NoticeEvent, Event
from nonebot import on_command, on_message, on_notice
from nonebot import logger
from nonebot import require
from nonebot.adapters import Bot
from nonebot.params import CommandArg
from nonebot import get_bot



require("utils")
from ..utils import getcard
from ..utils import async_session_factory, Base
from ..utils import get_session, get_today_start_timestamp

from .config import Config

from collections import defaultdict
import msgpack
import time
import math
import hashlib
from pathlib import Path
import random
import json
from sqlalchemy import String, Integer, LargeBinary, select, func, desc, text
from sqlalchemy.orm import Mapped, mapped_column

__plugin_meta__ = PluginMetadata(
    name="allmsg",
    description="",
    usage="",
    config=Config,
)


config = get_plugin_config(Config)


class GroupMsg(Base):
    __tablename__ = "groupmsg"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True, init=False)
    mid: Mapped[int] = mapped_column(Integer)
    sid: Mapped[str] = mapped_column(String)
    timestamp: Mapped[int] = mapped_column(Integer, name="timeStamp")
    data: Mapped[bytes] = mapped_column(LargeBinary) # 对应 BLOB 类型

class DataManager:
    async def insert_groupmsg(self, mid: int, sid: str, timestamp: int, data_bytes: bytes):
        async with async_session_factory() as session:
            async with session.begin():
                msg = GroupMsg(mid=mid, sid=sid, timestamp=timestamp, data=data_bytes)
                session.add(msg)

    async def get_id_by_mid(self, mid: int) -> int:
        async with async_session_factory() as session:
            stmt = select(GroupMsg.id).where(GroupMsg.mid == mid)\
                .order_by(desc(GroupMsg.timestamp))\
                .limit(1)
            
            result = await session.execute(stmt)
            row_id = result.scalar()
            return row_id if row_id is not None else -1

    async def get_all_msg(self, groupid, userid="%", tmrange=(0, 1e10)) -> dict:
        async with async_session_factory() as session:
            # 构造 LIKE 字符串: group_{groupid}_{userid}
            like_str = f"group_{groupid}_{userid}"
            
            stmt = select(GroupMsg).where(
                GroupMsg.sid.like(like_str),
                GroupMsg.timestamp >= int(tmrange[0]),
                GroupMsg.timestamp <= int(tmrange[1])
            )
            
            result = await session.execute(stmt)
            msgs = result.scalars().all() # 获取所有对象列表
            
            msgdict = {}
            for msg in msgs:
                # 保持原本的返回格式
                msgdict[msg.id] = (msg.sid, msg.timestamp, msgpack.loads(msg.data))
            return msgdict

    async def get_active_user(self, groupid) -> list[str]:
        async with async_session_factory() as session:
            like_str = f"group_{groupid}_%"
            today_start = get_today_start_timestamp()
            
            stmt = select(GroupMsg.sid).distinct().where(
                GroupMsg.sid.like(like_str),
                GroupMsg.timestamp >= today_start
            )
            
            result = await session.execute(stmt)
            return list(result.scalars().all())

    async def get_msg_count(self, groupid) -> int:
        async with async_session_factory() as session:
            like_str = f"group_{groupid}_%"
            
            stmt = select(func.count()).select_from(GroupMsg).where(
                GroupMsg.sid.like(like_str)
            )
            
            result = await session.execute(stmt)
            count = result.scalar()
            return count if count is not None else 0


db = DataManager()

allmsg = on_message(priority=0, block=False)

report = on_command("统计", priority=10, block=True)

async def get_msg_status(groupid):
    count = await db.get_msg_count(groupid)
    return f"本群已记录消息数：{count}"



async def insert_message(mid: int, sid: str, timestamp: int, message: Message):
    msglist = []
    for seg in message:
        if seg.type == "text":
            msglist.append(["text", seg.data["text"]])
        elif seg.type == "reply":
            msglist.append(["reply", await db.get_id_by_mid(seg.data["id"])])
        elif seg.type == "at":
            msglist.append(["at", seg.data["qq"]])
        elif seg.type == "face":
            msglist.append(["face", seg.data["id"]])
        else:
            msglist.append([seg.type, ])
    await db.insert_groupmsg(
        mid, sid, timestamp,
        msgpack.dumps(msglist)
    )

@allmsg.handle()
async def allmsg_function(bot: Bot, message: GroupMessageEvent):
    assert(message.get_session_id().startswith("group"))
    await insert_message(
        message.message_id,
        message.get_session_id(),
        message.time,
        message.original_message
    )

@report.handle()
async def report_function(bot: Bot, message: GroupMessageEvent):
    sid = message.get_session_id()
    assert(sid.startswith("group"))
    gid = sid.split('_')[1]
    myid = (await bot.get_login_info())['user_id']
    attocnt: defaultdict[int, int] = defaultdict(int)
    atfromcnt: defaultdict[int, int] = defaultdict(int)
    atpaircnt: defaultdict[tuple[int, int], int] = defaultdict(int)
    msgdict = await db.get_all_msg(gid)
    # print(len(msgdict))
    for sid, _, msg in msgdict.values():
        atset = set()
        atall = False
        for seg in msg:
            if seg[0] == "at":
                if seg[1] == "all":
                    atall = True
                else:
                    atset.add(int(seg[1]))
        uid = int(sid.split('_')[2])
        for toid in atset:
            attocnt[toid] += 1
            atpaircnt[(uid, toid)] += 1
        if len(atset) or atall:
            atfromcnt[uid] += 1
    result = Message()

    maxatfrom = sorted(atfromcnt.items(), key=lambda x: x[1])[-1]
    result += "最多 at 次数：" 
    result += await getcard(bot, gid, str(maxatfrom[0]))
    result += f" {maxatfrom[1]}次\n"

    maxatto = sorted(attocnt.items(), key=lambda x: x[1])[-1]
    result += "最多被 at 次数：" 
    result += await getcard(bot, gid, str(maxatto[0]))
    result += f" {maxatto[1]}次\n"

    maxatpair = sorted(atpaircnt.items(), key=lambda x: x[1])[-1]
    result += "最多 at 对次数：" 
    result += await getcard(bot, gid, str(maxatpair[0][0]))
    result += " -> "
    result += await getcard(bot, gid, str(maxatpair[0][1]))
    result += f" {maxatpair[1]}次\n"
    
    await report.send(result)

    lastattime: dict[int, int] = {}
    waittime: dict[int, list[int]] = {}
    for sid, tm, msg in msgdict.values():
        uid = int(sid.split('_')[2])
        if uid in lastattime:
            if uid not in waittime:
                waittime[uid] = [0, 0]
            waittime[uid][0] += min(600, tm - lastattime[uid])
            waittime[uid][1] += 1
            del lastattime[uid]
        atset = set()
        for seg in msg:
            if seg[0] == "at" and seg[1] != "all" and int(seg[1]) != uid:
                atset.add(int(seg[1]))
        for toid in atset:
            if toid not in lastattime:
                lastattime[toid] = tm
            elif tm - lastattime[toid] >= 600:
                if toid not in waittime:
                    waittime[toid] = [0, 0]
                waittime[toid][0] += 600
                waittime[toid][1] += 1
                lastattime[toid] = tm
    for uid in lastattime:
        if uid not in waittime:
            waittime[uid] = [0, 0]
        waittime[uid][0] += min(600, int(time.time()) - lastattime[uid])
        waittime[uid][1] += 1
    result = Message("平均at回复时间")
    waittime_list = sorted(waittime.items(), key=lambda x: x[1][0] / x[1][1], reverse=True)
    for data in waittime_list:
        if data[0] != myid:
            result += "\n"
            result += await getcard(bot, gid, str(data[0]))
            result += f" {data[1][0]}/{data[1][1]}={data[1][0]/data[1][1]:.0f}"
    await report.send(result)

def extra_plain_text(msg):
    result = ""
    for seg in msg:
        if seg[0] == "text":
            result += seg[1]
    return result
