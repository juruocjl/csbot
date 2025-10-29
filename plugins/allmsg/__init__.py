from nonebot import get_plugin_config
from nonebot.plugin import PluginMetadata
from nonebot.adapters.onebot.v11 import Message, MessageEvent, MessageSegment
from nonebot import on_command, on
from nonebot import logger
from nonebot import require
from nonebot.adapters import Bot
from nonebot.permission import SUPERUSER
from nonebot import get_bot

scheduler = require("nonebot_plugin_apscheduler").scheduler

localstorage = require("utils").localstorage

from .config import Config

import time
import math
import hashlib
import urllib
from pathlib import Path
import random
from meme_generator import Image, get_meme

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

db = DataManager()

fudupoint = on_command("复读点数", priority=10, block=True)

roll = on_command("roll", priority=10, block=True, permission=SUPERUSER)

allmsg = on(priority=100, block=True)

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


def sigmoid_step(x):
    t = (x - 50) / 500.0
    return max(0.02, math.tanh(t))

@fudupoint.handle()
async def fudupoint_function(message: MessageEvent):
    uid = message.get_user_id()
    point = db.get_point(uid)
    prob1 = sigmoid_step(point + 1)
    prob2 = sigmoid_step((point + 2) * 2)
    prob3 = sigmoid_step((point + 3) * 3)
    prob5 = sigmoid_step((point + 5) * 5)
    tm = db.get_zero_point(uid) + 1
    await fudupoint.finish(f"当前点数：{point}  下一次禁言时间：{tm}min\n点数：复读自己5({prob5:.2f})，第一遍复读1({prob1:.2f})，二遍复读2({prob2:.2f})，之后复读3({prob3:.2f})")


lastmsg = {}

@allmsg.handle()
async def allmsg_function(bot: Bot, message: MessageEvent):
    global lastpic
    uid = message.get_user_id()
    sid = message.get_session_id()
    msg = message.get_message()
    mhs = process_message_segments(msg)
    nowpoint = 0
    logger.info(f"{uid} send {msg} with {mhs}")
    if not sid.startswith("group"):
        return
    gid = sid.split('_')[1]
    text = msg.extract_plain_text().lower().strip()
    if text == "wlp" and lastpic:
        meme = get_meme("my_wife")
        with open(lastpic, "rb") as f:
            data = f.read()
        result = meme.generate([Image("test", data)], [], {})
        lastpic = None
        if isinstance(result, bytes):
            await allmsg.send(MessageSegment.image(result))
    if text == "nlg" and lastpic:
        meme = get_meme("dog_dislike")
        with open(lastpic, "rb") as f:
            data = f.read()
        result = meme.generate([Image("test", data)], [], {})
        lastpic = None
        if isinstance(result, bytes):
            await allmsg.send(MessageSegment.image(result))
    if text == "gsm" or text == "干什么":
        await allmsg.send(MessageSegment.record(Path("assets") / "gsm.mp3"))
    if text == "mbf" or text == "没办法":
        await allmsg.send(MessageSegment.record(Path("assets") / "mbf.mp3"))
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
            await allmsg.send(msglst[0][1])
            msglst = [(-1, msglst[0][1], mhs)]
    lastmsg[gid] = msglst
    if nowpoint > 0:
        db.add_point(uid, nowpoint)
        prob = sigmoid_step(nowpoint * db.get_point(uid))
        if random.random() < prob:
            db.add_point(uid, 0)
            tm = db.get_zero_point(uid)
            await bot.set_group_ban(group_id=gid, user_id=uid, duration=60 * db.get_zero_point(uid))
            await allmsg.send(Message(["恭喜", MessageSegment.at(uid), f" 以概率{prob:.2f}被禁言{tm}分钟"]))


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
            point = db.get_point(user['user_id']) + 1
            users.append((user['user_id'], point))
            weights.append(point)
    print(users)
    newadmin, point = random.choices(user, weights=weights, k=1)
    totsum = sum(weights)
    bot.send_group_msg(group_id=groupid, message=['恭喜', MessageSegment.at(newadmin), f" 以{point}/{totsum}选为管理员"])
    localstorage.set(keyname, newadmin)
    await bot.set_group_admin(group_id=groupid, user_id=localstorage.get(keyname), enable=True)

@roll.handle()
async def roll_function(message: MessageEvent):
    sid = message.get_session_id()
    if sid.startswith("group"):
        await roll_admin(sid.split('_')[1])
    