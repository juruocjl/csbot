from nonebot import get_plugin_config
from nonebot.plugin import PluginMetadata
from nonebot.adapters.onebot.v11 import Message, MessageEvent, GroupMessageEvent, MessageSegment, NoticeEvent, Event
from nonebot import on_command, on_message, on_notice
from nonebot import logger
from nonebot import require
from nonebot.adapters import Bot
from nonebot.permission import SUPERUSER
from nonebot.params import CommandArg
from nonebot import get_bot

scheduler = require("nonebot_plugin_apscheduler").scheduler

localstorage = require("utils").localstorage
get_today_start_timestamp = require("utils").get_today_start_timestamp

from .config import Config

from collections import defaultdict
import msgpack
import time
import math
import hashlib
from pathlib import Path
import random
from meme_generator import Image, get_meme
import jieba
from wordcloud import WordCloud
from io import BytesIO
import json

__plugin_meta__ = PluginMetadata(
    name="allmsg",
    description="",
    usage="",
    config=Config,
)

get_cursor = require("utils").get_cursor
get_session = require("utils").get_session

get_today_start_timestamp = require("utils").get_today_start_timestamp

config = get_plugin_config(Config)

jieba.load_userdict(str(Path("assets") / "dict.txt"))

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

    def get_all_msg(self, groupid, userid = "%", tmrange = (0, 1e9)):
        cursor = get_cursor()
        cursor.execute("SELECT * from groupmsg WHERE sid LIKE ? and ? <= timeStamp and timeStamp <= ?",
                       (f"group_{groupid}_{userid}", int(tmrange[0]), int(tmrange[1])))
        result = cursor.fetchall()
        msgdict = {}
        for id, _, sid, tm, msg in result:
            msgdict[id] = (sid, tm, msgpack.loads(msg))
        return msgdict

    def get_active_user(self, groupid):
        cursor = get_cursor()
        cursor.execute('SELECT DISTINCT sid FROM groupmsg WHERE sid LIKE ? and timeStamp >= ?',
                       (f"group_{groupid}_%", get_today_start_timestamp(), ))
        res = cursor.fetchall()
        return [a[0] for a in res]


db = DataManager()

fudupoint = on_command("复读点数", priority=10, block=True)

fuduhelp = on_command("复读帮助", priority=10, block=True)

roll = on_command("roll", priority=10, block=True, permission=SUPERUSER)

allmsg = on_message(priority=0, block=False)

fuducheck = on_message(priority=100, block=True)

def bancheck(event: NoticeEvent):
    return event.get_event_name().startswith("notice.group_ban.")

admincheck = on_notice(bancheck, priority=100, block=False)

debug_updmsg = on_command("updmsg", priority=10, block=True, permission=SUPERUSER)

report = on_command("统计", priority=10, block=True)

wordcloud = on_command("词云", priority=10, block=True)

mywordcloud = on_command("我的词云", priority=10, block=True)

def get_bytes_hash(data, algorithm='sha256'):
    hash_obj = hashlib.new(algorithm)
    hash_obj.update(data)
    return hash_obj.hexdigest()

async def process_message_segments(segments):
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
            async with get_session().get(url) as response:
                data = get_bytes_hash(await response.read())
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

valid_time = ["今日", "昨日", "本周", "全部"]
def get_time_range(time_type):
    if time_type == "今日":
        return get_today_start_timestamp(), 1e10
    if time_type == "昨日":
        return get_today_start_timestamp() - 24 * 3600, get_today_start_timestamp()
    if time_type == "本周":
        return time.time() - 7 * 24 * 3600, 1e10
    if time_type == "全部":
        return 0, 1e10
    raise RuntimeError("no time type")

def get_wordcloud(groud_id, user_id = "%", time_type = "全部"):
    if time_type not in valid_time:
        time_type = "全部"
    msgdict = db.get_all_msg(groud_id, userid=user_id, tmrange=get_time_range(time_type))
    stopwords = {
        "怎么", "感觉", "什么", "真是", "不是", "一个", "可以", "没有", "你们", "但是", "现在", "这个",
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
async def wordcloud_function(message: GroupMessageEvent, args: Message = CommandArg()):
    sid = message.get_session_id()
    assert(sid.startswith("group"))
    gid = sid.split('_')[1]
    msg = args.extract_plain_text().strip()
    uid = "%"
    for seg in message.get_message():
        if seg.type == "at":
            uid = seg.data["qq"]
    image = get_wordcloud(gid, user_id=uid, time_type=msg)
    await wordcloud.finish(MessageSegment.image(image))

@mywordcloud.handle()
async def wordcloud_function(message: GroupMessageEvent, args: Message = CommandArg()):
    sid = message.get_session_id()
    assert(sid.startswith("group"))
    gid = sid.split('_')[1]
    msg = args.extract_plain_text().strip()
    uid = message.get_user_id()
    image = get_wordcloud(gid, user_id=uid, time_type=msg)
    await mywordcloud.finish(MessageSegment.image(image))

@scheduler.scheduled_job("cron", hour="23", minute="50", id="todaywc")
async def todaywc():
    bot = get_bot()
    for group in config.cs_group_list:
        image = get_wordcloud(group, time_type="今日")
        await bot.send_group_msg(group_id=group, message=Message([MessageSegment.image(image)]))

@debug_updmsg.handle()
async def qwqwqwwqq(bot: Bot, message: GroupMessageEvent):
    sid = message.get_session_id()
    assert(sid.startswith("group"))
    data = await bot.call_api("get_group_msg_history", group_id=sid.split('_')[1], count=5000)
    myid = (await bot.get_login_info())['user_id']
    for msg in data["messages"]:
        if msg['user_id'] != myid:
            insert_msg(msg)

@fuduhelp.handle()
async def fuduhelp_function():
    await fuduhelp.finish("""禁言概率公式：max(0.02,tanh((本句点数*累计点数-50)/500))
管理员被撤销概率公式：max(0,tanh((本句点数*累计点数/100-50)/500))
第一遍复读1，二遍复读2，之后复读3，禁言点数为禁言时长（单位秒），取消禁言为100""")

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
    if uid == localstorage.get(f'adminqq{gid}') and int(localstorage.get(f'adminqqalive{gid}')):
        admin = True
    point = db.get_point(f"group_{gid}_{uid}")
    prob1 = sigmoid_step((point + 1), admin=admin)
    prob2 = sigmoid_step((point + 2) * 2, admin=admin)
    prob3 = sigmoid_step((point + 3) * 3, admin=admin)
    prob5 = sigmoid_step((point + 5) * 5, admin=admin)
    tm = db.get_zero_point(f"group_{gid}_{uid}") + 1
    if admin:
        await fudupoint.finish(f"[管理员]当前点数：{point}  下一次被下放\n点数：复读自己5({prob5:.2f})，第一遍复读1({prob1:.2f})，二遍复读2({prob2:.2f})，之后复读3({prob3:.2f})")
    else:
        await fudupoint.finish(f"当前点数：{point}  下一次禁言时间：{tm}min\n点数：复读自己5({prob5:.2f})，第一遍复读1({prob1:.2f})，二遍复读2({prob2:.2f})，之后复读3({prob3:.2f})")

async def addpoint(gid, uid, nowpoint):
    bot = get_bot()
    sid = f"group_{gid}_{uid}"
    if uid == localstorage.get(f'adminqq{gid}') and int(localstorage.get(f'adminqqalive{gid}')):
        db.add_point(sid, nowpoint)
        prob = sigmoid_step(nowpoint * db.get_point(sid), admin=True)
        if random.random() < prob:
            localstorage.set(f'adminqqalive{gid}', '0')
            await bot.set_group_admin(group_id=gid, user_id=uid, enable=False)
            await fuducheck.send(Message(["恭喜", MessageSegment.at(uid), f" 以概率{prob:.2f}被下放"]))
            return True
    else:
        db.add_point(sid, nowpoint)
        prob = sigmoid_step(nowpoint * db.get_point(sid), admin=False)
        if random.random() < prob:
            db.add_point(sid, 0)
            tm = db.get_zero_point(sid)
            await bot.set_group_ban(group_id=gid, user_id=uid, duration=60 * tm)
            await fuducheck.send(Message(["恭喜", MessageSegment.at(uid), f" 以概率{prob:.2f}被禁言{tm}分钟"]))
            return True
    return False

lastmsg = {}

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

@fuducheck.handle()
async def fuducheck_function(bot: Bot, message: GroupMessageEvent):
    global lastpic
    uid = message.get_user_id()
    sid = message.get_session_id()
    assert(sid.startswith("group"))
    msg = message.get_message()
    mhs = await process_message_segments(msg)
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
    issb, whosb = checksb(msg)
    print(issb, whosb)
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
            await fuducheck.send(msglst[0][1])
            if issb:
                tm = db.get_zero_point(f"group_{gid}_{whosb}") + 1
                await bot.set_group_ban(group_id=gid, user_id=whosb, duration=60 * tm)
    lastmsg[gid] = msglst
    if nowpoint > 0:
        addpoint(gid, uid, nowpoint)

@admincheck.handle()
async def admincheck_function(bot: Bot, notice: NoticeEvent):
    logger.info(notice.get_event_description())
    # print(notice.get_event_name(), notice.get_event_description())
    data = json.loads(notice.get_event_description().replace("'", '"'))
    uid = data['user_id']
    gid = data['group_id']
    o_uid = data['operator_id']
    duration = data['duration']
    # print(uid, gid, duration, data['sub_type'])
    if duration:
        if await addpoint(gid, o_uid, duration):
            await bot.set_group_ban(group_id=gid, user_id=uid, duration=0)
    else:
        await addpoint(gid, o_uid, 100)


async def roll_admin(groupid: str):
    bot = get_bot()
    if localstorage.get(f'adminqq{groupid}') and int(localstorage.get(f'adminqqalive{groupid}')):
        await bot.set_group_admin(group_id=groupid, user_id=localstorage.get(f'adminqq{groupid}'), enable=False)
    users = []
    weights = []
    sid_list = db.get_active_user(groupid)
    for sid in sid_list:
        point = db.get_point(sid) / (db.get_zero_point(sid) + 1) + 1
        userid = int(sid.split('_')[2])
        users.append((userid, point))
        weights.append(point)
    print(users)
    newadmin, point = random.choices(users, weights=weights, k=1)[0]
    totsum = sum(weights)
    await bot.send_group_msg(group_id=groupid, message=Message(['恭喜', MessageSegment.at(newadmin), f" 以{point}/{totsum}选为管理员"]))
    localstorage.set(f'adminqq{groupid}', newadmin)
    localstorage.set(f'adminqqalive{groupid}', '1')
    await bot.set_group_admin(group_id=groupid, user_id=newadmin, enable=True)

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