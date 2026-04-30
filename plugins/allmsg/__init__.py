from nonebot import get_plugin_config
from nonebot.plugin import PluginMetadata
from nonebot.adapters.onebot.v11 import Message, GroupMessageEvent, MessageSegment
from nonebot.params import CommandArg
from nonebot import on_command, on_message
from nonebot import logger
from nonebot import require
from nonebot.adapters.onebot.v11 import Bot

require("utils")
from ..utils import getcard
from ..utils import async_session_factory
from ..utils import get_session, get_today_start_timestamp
from ..utils import local_storage

require("models")
from ..models import GroupMsg, ImgCacheInfo


from .config import Config

from collections import defaultdict
import msgpack
import time
import hashlib
from typing import Awaitable, Callable
from pathlib import Path
import random
import json
import asyncio
from io import BytesIO
from datetime import date, datetime, timedelta
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from sqlalchemy import String, Integer, LargeBinary, select, func, desc, text, Boolean
from sqlalchemy.orm import Mapped, mapped_column

__plugin_meta__ = PluginMetadata(
    name="allmsg",
    description="",
    usage="",
    config=Config,
)


config = get_plugin_config(Config)

plt.rcParams["font.sans-serif"] = [
    "WenQuanYi Micro Hei",
    "Noto Sans CJK SC",
    "SimHei",
    "Arial Unicode MS",
    "DejaVu Sans",
]
plt.rcParams["axes.unicode_minus"] = False


class DataManager:
    async def insert_groupmsg(self, mid: int, sid: str, timestamp: int, data_bytes: bytes):
        async with async_session_factory() as session:
            async with session.begin():
                msg = GroupMsg(mid=mid, sid=sid, timestamp=timestamp, data=data_bytes)
                session.add(msg)

    async def touch_img_cache(self, hashval: str) -> tuple[bool, bool]:
        async with async_session_factory() as session:
            async with session.begin():
                row = await session.get(ImgCacheInfo, hashval)
                if row is None:
                    row = ImgCacheInfo(hash=hashval, count=1, valid=True)
                    session.add(row)
                    return False, False
                valid = row.valid
                row.valid = True
                row.count = (row.count or 0) + 1
                return True, valid
    

    async def get_id_by_mid(self, mid: int) -> int:
        async with async_session_factory() as session:
            stmt = select(GroupMsg.id).where(GroupMsg.mid == mid)\
                .order_by(GroupMsg.timestamp.desc())\
                .limit(1)
            
            result = await session.execute(stmt)
            row_id = result.scalar()
            return row_id if row_id is not None else -1

    async def get_all_msg(self, groupid: str, userid: str="%", tmrange: tuple[int, int]=(0, int(1e10))) -> dict:
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

    async def get_user_daily_msg_count(self, groupid: str, userid: str, days: int = 30) -> list[tuple[datetime, int]]:
        async with async_session_factory() as session:
            sid = f"group_{groupid}_{userid}"
            now = datetime.now()
            today = datetime(now.year, now.month, now.day)
            start_date = today - timedelta(days=days - 1)
            start_ts = int(start_date.timestamp())
            end_ts = int((today + timedelta(days=1)).timestamp()) - 1

            stmt = select(GroupMsg.timestamp).where(
                GroupMsg.sid == sid,
                GroupMsg.timestamp >= start_ts,
                GroupMsg.timestamp <= end_ts
            )
            result = await session.execute(stmt)
            rows = result.scalars().all()

            counter: defaultdict[datetime, int] = defaultdict(int)
            for ts in rows:
                day = datetime.fromtimestamp(ts)
                day_key = datetime(day.year, day.month, day.day)
                counter[day_key] += 1

            return [
                (start_date + timedelta(days=i), counter[start_date + timedelta(days=i)])
                for i in range(days)
            ]

    

full_dir = Path("imgs") / "history" / "full"
small_dir = Path("imgs") / "history" / "small"

cache_dir = Path("imgs") / "cache"

img_lock = asyncio.Lock()
img_cache_lock = asyncio.Lock()

if not full_dir.exists():
    full_dir.mkdir(parents=True)
if not small_dir.exists():
    small_dir.mkdir(parents=True)
if not cache_dir.exists():
    cache_dir.mkdir(parents=True)

db = DataManager()

allmsg = on_message(priority=0, block=False)

report = on_command("统计", priority=10, block=True)
talk_trend = on_command("发言", priority=10, block=True)

async def get_msg_status(groupid) -> str:
    count = await db.get_msg_count(groupid)
    return f"本群已记录消息数：{count}"

async def get_image(file: str, url: str) -> bytes:
    async with img_cache_lock:
        filepath = cache_dir / file
        cachc_dict: dict[str, float] = json.loads(await local_storage.get("img_cache_dict", "{}"))
        content = None
        if file in cachc_dict and filepath.exists():
            logger.info(f"使用缓存图片 {file}")
            with open(filepath, "rb") as f:
                content = f.read()
            cachc_dict[file] = time.time()
        else:
            logger.info(f"下载图片 {file} 从 {url}")
            async with get_session().get(url) as res:
                assert res.status == 200
                content = await res.read()
            with open(filepath, "wb") as f:
                f.write(content)
            cachc_dict[file] = time.time()
        # 清理缓存
        if len(cachc_dict) > 300:
            sorted_items = sorted(cachc_dict.items(), key=lambda item: item[1])
            for i in range(len(sorted_items) - 250):
                fpath = cache_dir / sorted_items[i][0]
                if fpath.exists():
                    fpath.unlink()
                del cachc_dict[sorted_items[i][0]]
        await local_storage.set("img_cache_dict", json.dumps(cachc_dict))
        return content

async def insert_message(bot: Bot, mid: int, sid: str, timestamp: int, message: Message) -> str:
    msglist = []
    mhs: str | None = None
    for seg in message:
        if seg.type == "text":
            msglist.append(["text", seg.data["text"]])
        elif seg.type == "reply":
            msglist.append(["reply", await db.get_id_by_mid(int(seg.data["id"]))])
        elif seg.type == "at":
            msglist.append(["at", seg.data["qq"]])
        elif seg.type == "face":
            msglist.append(["face", seg.data["id"]])
        elif seg.type == "image":
            # print(seg.data)
            try:
                content = await get_image(seg.data["file"], seg.data["url"])
                filehash = hashlib.sha256(content).hexdigest()
                filename = filehash + ".png"
                # print(filehash)
                async with img_lock:
                    has_small, has_full = await db.touch_img_cache(filehash)
                    if not has_full:
                        with open(full_dir / filename, "wb") as fullf:
                            fullf.write(content)
                    if not has_small:
                        from PIL import Image
                        img = Image.open(BytesIO(content))
                        img.thumbnail((128, 128))
                        with open(small_dir / filename, "wb") as smallf:
                            img.save(smallf, format="PNG")
            except Exception as e:
                logger.error(f"图片处理失败: {e}")
                filehash = "error" + random.randbytes(32).hex()

            msglist.append(["imagev2", filehash, seg.data.get("sub_type", ""), seg.data.get("summary", "")])
        else:
            msglist.append([seg.type, ])
            mhs = random.randbytes(32).hex()
    await db.insert_groupmsg(
        mid, sid, timestamp,
        msgpack.dumps(msglist)
    )
    if mhs is not None:
        return mhs
    return hashlib.sha256(msgpack.dumps(msglist)).hexdigest()

FuduType = Callable[[Bot, GroupMessageEvent, str], Awaitable[None]]

class MyDecorator:
    def __init__(self):
        self.registered_funcs: list[FuduType] = []

    def handle(self) -> Callable[[FuduType], FuduType]:
        def decorator(func: FuduType) -> FuduType:
            self.registered_funcs.append(func)
            return func
        return decorator

    async def run(self, bot: Bot, message: GroupMessageEvent, mhs: str) -> None:
        for func in self.registered_funcs:
            await func(bot, message, mhs)

myallmsg = MyDecorator()

@allmsg.handle()
async def allmsg_function(bot: Bot, message: GroupMessageEvent) -> None:
    assert(message.get_session_id().startswith("group"))
    mhs = await insert_message(
        bot,
        message.message_id,
        message.get_session_id(),
        message.time,
        message.original_message
    )
    await myallmsg.run(bot, message, mhs)

@talk_trend.handle()
async def talk_trend_function(event: GroupMessageEvent, args: Message = CommandArg()):
    target_uids: list[str] = []
    for seg in args:
        if seg.type == "at":
            uid = str(seg.data["qq"])
            if uid not in target_uids:
                target_uids.append(uid)
    if len(target_uids) == 0:
        target_uids.append(event.get_user_id())

    display_names: dict[str, str] = {}
    for uid in target_uids:
        display_names[uid] = await getcard(event.bot, str(event.group_id), uid)

    uid_histories: list[tuple[str, list[tuple[date, int]]]] = []
    for target_uid in target_uids:
        history = await db.get_user_daily_msg_count(str(event.group_id), target_uid, 30)
        uid_histories.append((target_uid, history))

    fig, ax = plt.subplots(figsize=(11, 5.5))
    try:
        for target_uid, history in uid_histories:
            xs = [day for day, _ in history]
            ys = [count for _, count in history]
            ax.plot(xs, ys, marker="o", linewidth=1.8, markersize=3.5, label=display_names[target_uid])

        if len(target_uids) == 1:
            ax.set_title(f"用户 {display_names[target_uids[0]]} 最近30天发言条数")
        else:
            ax.set_title("多用户最近30天发言条数")
        ax.set_xlabel("日期")
        ax.set_ylabel("发言条数")
        ax.grid(True, linestyle="--", alpha=0.4)
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%m-%d"))
        if len(target_uids) > 1:
            ax.legend(title="用户")
        fig.autofmt_xdate()
        fig.tight_layout()

        buffer = BytesIO()
        fig.savefig(buffer, format="png", dpi=160)
        buffer.seek(0)
        image = buffer.getvalue()
    finally:
        plt.close(fig)

    await talk_trend.finish(MessageSegment.image(image))

@report.handle()
async def report_function(bot: Bot, message: GroupMessageEvent) -> None:
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
