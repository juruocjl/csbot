from nonebot import get_plugin_config
from nonebot.plugin import PluginMetadata
from nonebot.adapters.onebot.v11 import Message, MessageEvent, GroupMessageEvent, MessageSegment
from nonebot import on_command, on_message
from nonebot import logger
from nonebot import require
from nonebot.adapters import Bot
from nonebot.permission import SUPERUSER
from nonebot import get_bot

scheduler = require("nonebot_plugin_apscheduler").scheduler

localstorage = require("utils").localstorage

from .config import Config

from collections import defaultdict
import msgpack
import time
import math
import hashlib
import urllib
from pathlib import Path
import random
from meme_generator import Image, get_meme
import jieba
from wordcloud import WordCloud
from io import BytesIO

__plugin_meta__ = PluginMetadata(
    name="allmsg",
    description="",
    usage="",
    config=Config,
)

get_cursor = require("utils").get_cursor

get_today_start_timestamp = require("utils").get_today_start_timestamp

config = get_plugin_config(Config)

class DataManager:
    def __init__(self):
        cursor = get_cursor()

        cursor.execute('''
        CREATE TABLE IF NOT EXISTS fudu_points (
            uid TEXT,
            timeStamp INT,
            point INT
        )
        ''')
        
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS groupmsg (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            mid INTEGER NOT NULL,
            sid TEXT NOT NULL,
            timeStamp INTEGER NOT NULL,
            data BLOB NOT NULL
        )
        ''')

    def add_point(self, uid, point):
        timestamp = int(time.time())
        cursor = get_cursor()
        cursor.execute(
            "INSERT INTO fudu_points (uid, timeStamp, point) VALUES (?, ?, ?)",
            (uid, timestamp, point)
        )
         
    def get_point(self, uid):
        cursor = get_cursor()
        cursor.execute(
            "SELECT SUM(point) FROM fudu_points WHERE uid = ? AND timeStamp >= ?",
            (uid, get_today_start_timestamp())
        )
        result = cursor.fetchone()
        return result[0] if result[0] is not None else 0

    def get_zero_point(self, uid):
        cursor = get_cursor()
        cursor.execute(
            "SELECT COUNT(point) FROM fudu_points WHERE uid = ? AND timeStamp >= ? AND point == 0",
            (uid, get_today_start_timestamp())
        )
        result = cursor.fetchone()
        return result[0] if result[0] is not None else 0

    def insert_groupmsg(self, mid: int, sid: str, timestamp: int, data_bytes: bytes):
        cursor = get_cursor()
        cursor.execute('''
            INSERT INTO groupmsg (mid, sid, timeStamp, data)
            VALUES (?, ?, ?, ?)
        ''', (mid, sid, timestamp, data_bytes))
    
    def get_id_by_mid(self, mid: int):
        cursor = get_cursor()
        cursor.execute('''
            SELECT id FROM groupmsg 
            WHERE mid = ? 
            ORDER BY timeStamp DESC 
            LIMIT 1
        ''', (mid,))
        result = cursor.fetchone()
        return result[0] if result else -1

    def get_all_msg(self, groupid, userid = "%"):
        cursor = get_cursor()
        cursor.execute("SELECT * from groupmsg WHERE sid LIKE ?", (f"group_{groupid}_{userid}",))
        result = cursor.fetchall()
        msgdict = {}
        for id, _, sid, tm, msg in result:
            msgdict[id] = (sid, tm, msgpack.loads(msg))
        return msgdict


db = DataManager()

fudupoint = on_command("复读点数", priority=10, block=True)

roll = on_command("roll", priority=10, block=True, permission=SUPERUSER)

allmsg = on_message(priority=0, block=False)

fuducheck = on_message(priority=100, block=True)

debug_updmsg = on_command("updmsg", priority=10, block=True, permission=SUPERUSER)

report = on_command("统计", priority=10, block=True)

wordcloud = on_command("词云", priority=10, block=True)

mywordcloud = on_command("我的词云", priority=10, block=True)

def get_bytes_hash(data, algorithm='sha256'):
    hash_obj = hashlib.new(algorithm)
    hash_obj.update(data)
    return hash_obj.hexdigest()

def process_message_segments(segments):
    """处理消息段，提取信息并计算哈希"""
    hash_source = b""
    
    for seg in segments:
        if seg.type == "text":
            text = get_bytes_hash(seg.data["text"].encode("utf-8"))
            hash_source += f"text:{text}".encode("utf-8") + b"|"
            
        elif seg.type == "at":
            user_id = seg.data["qq"]
            hash_source += f"at:{user_id}".encode("utf-8") + b"|"
            
        elif seg.type == "face":
            face_id = seg.data["id"]
            hash_source += f"face:{face_id}".encode("utf-8") + b"|"
            
        elif seg.type == "image":
            url = seg.data["url"]
            with urllib.request.urlopen(url) as response:
                data = get_bytes_hash(response.read())
                hash_source += f"image:{data}".encode("utf-8") + b"|"

    return get_bytes_hash(hash_source)

def encode_msg(segments):
    msglist = []
    for seg in segments:
        if seg["type"] == "text":
            msglist.append(("text", seg["data"]["text"]))
        if seg["type"] == "reply":
            msglist.append(("reply", db.get_id_by_mid(seg["data"]["id"])))
        elif seg["type"] == "at":
            msglist.append(("at", seg["data"]["qq"]))
        elif seg["type"] == "face":
            msglist.append(("face", seg["data"]["id"]))
        elif seg["type"] == "image":
            msglist.append(("image", ))
    return msgpack.dumps(msglist)

def insert_msg(message):
    db.insert_groupmsg(
        message["message_id"],
        "group_{}_{}".format(message["group_id"], message["user_id"]),
        message["time"],
        encode_msg(message["message"])
    )

@allmsg.handle()
async def allmsg_function(bot: Bot, message: GroupMessageEvent):
    sid = message.get_session_id()
    assert(sid.startswith("group"))
    # logger.info(await bot.get_msg(message_id=message.message_id))
    insert_msg(await bot.get_msg(message_id=message.message_id))

async def getcard(bot, gid, uid):
    info = await bot.get_group_member_info(group_id=gid, user_id=uid, no_cache=False)
    if info["card"]:
        return info["card"]
    return info["nickname"]

@report.handle()
async def report_function(bot: Bot, message: GroupMessageEvent):
    sid = message.get_session_id()
    assert(sid.startswith("group"))
    gid = sid.split('_')[1]
    myid = (await bot.get_login_info())['user_id']
    attocnt = defaultdict(int)
    atfromcnt = defaultdict(int)
    atpaircnt = defaultdict(int)
    msgdict = db.get_all_msg(sid.split('_')[1])
    for sid, _, msg in msgdict.values():
        atset = set()
        for seg in msg:
            if seg[0] == "at":
                atset.add(int(seg[1]))
        uid = int(sid.split('_')[2])
        for toid in atset:
            attocnt[toid] += 1
            atpaircnt[(uid, toid)] += 1
        if len(atset):
            atfromcnt[uid] += 1
    result = Message()

    maxatfrom = sorted(atfromcnt.items(), key=lambda x: x[1])[-1]
    result += "最多 at 次数：" 
    result += await getcard(bot, gid, maxatfrom[0])
    result += f" {maxatfrom[1]}次\n"

    maxatto = sorted(attocnt.items(), key=lambda x: x[1])[-1]
    result += "最多被 at 次数：" 
    result += await getcard(bot, gid, maxatto[0])
    result += f" {maxatto[1]}次\n"

    maxatpair = sorted(atpaircnt.items(), key=lambda x: x[1])[-1]
    result += "最多 at 对次数：" 
    result += await getcard(bot, gid, maxatpair[0][0])
    result += " -> "
    result += await getcard(bot, gid, maxatpair[0][1])
    result += f" {maxatpair[1]}次\n"
    
    await report.send(result)

    lastattime = {}
    waittime = {}
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
            if seg[0] == "at" and int(seg[1]) != uid:
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
    result = "平均at回复时间\n"
    waittime = sorted(waittime.items(), key=lambda x: x[1][0] / x[1][1], reverse=True)
    for data in waittime:
        if data[0] != myid:
            result += await getcard(bot, gid, data[0])
            result += f" {data[1][0]}/{data[1][1]}={data[1][0]/data[1][1]:.0f}"
            result += "\n"
    await report.send(result.strip())

def extra_plain_text(msg):
    result = ""
    for seg in msg:
        if seg[0] == "text":
            result += seg[1]
    return result

def get_wordcloud(msgdict):
    stopwords = {
        "怎么", "感觉", "什么", "真是", "不是", "一个", "可以", "没有", "你们", "但是", "现在"
    }
    raw_text = " ".join(map(lambda x: extra_plain_text(x[2]),msgdict.values()))
    seg_list = list(jieba.cut(raw_text, cut_all=False))
    text = " ".join([word for word in seg_list if word not in stopwords and len(word) > 1])
    
    buffer = BytesIO()
    WordCloud(
        width=800,
        height=600,
        background_color='white',
        font_path=Path("./assets") / "SimHei.ttf",
        max_words=200,
        colormap='viridis',
        collocations=False
    ).generate(text).to_image().save(buffer, format='PNG') 
    return buffer

@wordcloud.handle()
async def wordcloud_function(message: GroupMessageEvent):
    sid = message.get_session_id()
    assert(sid.startswith("group"))
    msgdict = db.get_all_msg(sid.split('_')[1])
    await wordcloud.finish(MessageSegment.image(get_wordcloud(msgdict)))

@mywordcloud.handle()
async def wordcloud_function(message: GroupMessageEvent):
    sid = message.get_session_id()
    uid = message.get_user_id()
    assert(sid.startswith("group"))
    msgdict = db.get_all_msg(sid.split('_')[1], userid=uid)
    await mywordcloud.finish(MessageSegment.image(get_wordcloud(msgdict)))

@debug_updmsg.handle()
async def qwqwqwwqq(bot: Bot, message: GroupMessageEvent):
    sid = message.get_session_id()
    assert(sid.startswith("group"))
    data = await bot.call_api("get_group_msg_history", group_id=sid.split('_')[1], count=5000)
    myid = (await bot.get_login_info())['user_id']
    for msg in data["messages"]:
        if msg['user_id'] != myid:
            insert_msg(msg)


def sigmoid_step(x):
    t = (x - 50) / 500.0
    return max(0.02, math.tanh(t))



@fudupoint.handle()
async def fudupoint_function(message: GroupMessageEvent):
    uid = message.get_user_id()
    sid = message.get_session_id()
    assert(sid.startswith("group"))
    gid = sid.split('_')[1]
    point = db.get_point(sid)
    admincoef = 2 if uid == localstorage.get(f'adminqq{gid}') else 1
    prob1 = sigmoid_step(admincoef * (point + 1))
    prob2 = sigmoid_step(admincoef * (point + 2) * 2)
    prob3 = sigmoid_step(admincoef * (point + 3) * 3)
    prob5 = sigmoid_step(admincoef * (point + 5) * 5)
    tm = db.get_zero_point(sid) + 1
    await fudupoint.finish(f"当前点数：{point}  下一次禁言时间：{tm}min\n点数：复读自己5({prob5:.2f})，第一遍复读1({prob1:.2f})，二遍复读2({prob2:.2f})，之后复读3({prob3:.2f})")


lastmsg = {}

@fuducheck.handle()
async def fuducheck_function(bot: Bot, message: GroupMessageEvent):
    global lastpic
    uid = message.get_user_id()
    sid = message.get_session_id()
    assert(sid.startswith("group"))
    msg = message.get_message()
    mhs = process_message_segments(msg)
    nowpoint = 0
    logger.info(f"{uid} send {msg} with {mhs}")
    gid = sid.split('_')[1]
    text = msg.extract_plain_text().lower().strip()
    if text == "wlp" and lastpic:
        meme = get_meme("my_wife")
        with open(lastpic, "rb") as f:
            data = f.read()
        result = meme.generate([Image("test", data)], [], {})
        lastpic = None
        if isinstance(result, bytes):
            await fuducheck.send(MessageSegment.image(result))
    if text == "nlg" and lastpic:
        meme = get_meme("dog_dislike")
        with open(lastpic, "rb") as f:
            data = f.read()
        result = meme.generate([Image("test", data)], [], {})
        lastpic = None
        if isinstance(result, bytes):
            await fuducheck.send(MessageSegment.image(result))
    if text == "gsm" or text == "干什么":
        await fuducheck.send(MessageSegment.record(Path("assets") / "gsm.mp3"))
    if text == "mbf" or text == "没办法":
        await fuducheck.send(MessageSegment.record(Path("assets") / "mbf.mp3"))
    msglst = []
    if gid in lastmsg:
        msglst = lastmsg[gid]
    if len(msglst) == 1 and msglst[0][0] == -1 and msglst[0][2] == mhs:
        nowpoint = 3
    elif len(msglst) > 0 and msglst[0][2] == mhs and (uid in [a[0] for a in msglst]):
        nowpoint = 5
    else:
        msglst.append((uid, msg, mhs))
        if len(msglst) > 1 and msglst[-1][2] != msglst[-2][2]:
            msglst = msglst[-1:]
        nowpoint = len(msglst) - 1
        if len(msglst) > 2:
            await fuducheck.send(msglst[0][1])
            msglst = [(-1, msglst[0][1], mhs)]
    lastmsg[gid] = msglst
    if nowpoint > 0:
        db.add_point(sid, nowpoint)
        admincoef = 2 if uid == localstorage.get(f'adminqq{gid}') else 1
        prob = sigmoid_step(admincoef * nowpoint * db.get_point(sid))
        if random.random() < prob:
            db.add_point(sid, 0)
            tm = db.get_zero_point(sid)
            await bot.set_group_ban(group_id=gid, user_id=uid, duration=60 * db.get_zero_point(uid))
            await fuducheck.send(Message(["恭喜", MessageSegment.at(uid), f" 以概率{prob:.2f}被禁言{tm}分钟"]))


async def roll_admin(groupid: str):
    bot = get_bot()
    keyname = f'adminqq{groupid}'
    if localstorage.get(keyname):
        await bot.set_group_admin(group_id=groupid, user_id=localstorage.get(keyname), enable=False)
    member_list = await bot.get_group_member_list(group_id=groupid)
    myid = (await bot.get_login_info())['user_id']
    users = []
    weights = []
    for user in member_list:
        if not user['is_robot'] and user['user_id'] != myid:
            sid = "group_" + groupid + "_" + str(user['user_id'])
            point = db.get_point(sid) / (db.get_zero_point(sid) + 1) + 1
            users.append((user['user_id'], point))
            weights.append(point)
    print(users)
    newadmin, point = random.choices(users, weights=weights, k=1)[0]
    totsum = sum(weights)
    await bot.send_group_msg(group_id=groupid, message=Message(['恭喜', MessageSegment.at(newadmin), f" 以{point}/{totsum}选为管理员"]))
    localstorage.set(keyname, newadmin)
    await bot.set_group_admin(group_id=groupid, user_id=localstorage.get(keyname), enable=True)

@roll.handle()
async def roll_function(message: GroupMessageEvent):
    sid = message.get_session_id()
    assert(sid.startswith("group"))
    await roll_admin(sid.split('_')[1])
    

@scheduler.scheduled_job("cron", hour="23", minute="55", id="roll")
async def autoroll():
    logger.info("start roll")
    for group in config.cs_group_list:
        await roll_admin(group)