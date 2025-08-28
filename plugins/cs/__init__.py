from nonebot import get_plugin_config
from nonebot.plugin import PluginMetadata
from nonebot.adapters.qq import MessageEvent, MessageSegment
from nonebot.adapters import Message
from nonebot.params import CommandArg
from nonebot import on_command
from nonebot.rule import to_me
from nonebot.permission import SUPERUSER
from dotenv import load_dotenv

from .config import Config

from pathlib import Path
import re
import sqlite3
import requests
import urllib.request
import os
import asyncio
from pyppeteer import launch
from io import BytesIO
import tempfile

def path_to_file_url(path):
    absolute_path = os.path.abspath(path)
    
    if os.name == 'nt':
        absolute_path = '/' + absolute_path.replace('\\', '/')
    return 'file://' + absolute_path

async def screenshot_html_to_png(url, width, height):
    browser = await launch(headless=True, args=['--no-sandbox', '--disable-setuid-sandbox'])
    page = await browser.newPage()
    await page.setViewport({'width': width, 'height': height})
    await page.goto(url)
    await asyncio.sleep(1)
    image = await page.screenshot()
    await browser.close()

    return image

load_dotenv(".env.prod")

with open("data.html", 'r', encoding='utf-8') as file:
    data_content = file.read()

with open("rank.html", 'r', encoding='utf-8') as file:
    rank_content = file.read().split("<!--SPLIT--->")

def red_to_green_color(score):
    red = 1.0 - score
    green = score
    blue = 0.2
    
    return f"rgb({round(red*255)},{round(green*255)},{round(blue*255)})"

class DataManager:
    def __init__(self, db_name: str = "groups.db"):
        self.conn = sqlite3.connect(db_name)
        self._create_tables()

    def _create_tables(self):
        cursor = self.conn.cursor()
        
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS group_members (
            gid TEXT,
            uid TEXT,
            PRIMARY KEY (gid, uid)
        )
        ''')

        cursor.execute('''
        CREATE TABLE IF NOT EXISTS members_steamid (
            uid TEXT,
            steamid TEXT,
            PRIMARY KEY (uid)
        )
        ''')

        cursor.execute('''
        CREATE TABLE IF NOT EXISTS steamid_detail (
            steamid TEXT,
            avatarlink TEXT,
            name TEXT,
            pvpScore INT,
            cnt INT,
            kd FLOAT,
            winRate FLOAT,
            pwRating FLOAT,
            avgWe FLOAT,
            kills INT,
            deaths INT,
            assists INT,
            rws FLOAT,
            adr FLOAT,
            headShotRatio FLOAT,
            entryKillRatio FLOAT,
            vs1WinRate FLOAT,
            lasttime TEXT,
            PRIMARY KEY (steamid)
        )
        ''')
        self.conn.commit()
    
    def bind(self, uid, steamid):
        cursor = self.conn.cursor()
        cursor.execute('''
        INSERT OR REPLACE INTO members_steamid (uid, steamid) VALUES (?, ?)
        ''', (uid, steamid))
        self.conn.commit()

    def get_steamid(self, uid):
        cursor = self.conn.cursor()
        cursor.execute('''
        SELECT steamid FROM members_steamid WHERE uid = ?
        ''', (uid,))
        result = cursor.fetchone()
        return result[0] if result else None

    def update_stats(self, steamid):
        url = "https://api.wmpvp.com/api/csgo/home/pvp/detailStats"
        payload = {
            "mySteamId": os.getenv("mySteamId"),
            "toSteamId": steamid
        }
        header = {
            "appversion": "3.5.4.172",
            "token":os.getenv("wmtoken")
        }
        result = requests.post(url,headers=header,json=payload, verify=False)
        data = result.json()
        if data["statusCode"] != 0:
            return "爬取失败：" + data["errorMessage"]
        cursor = self.conn.cursor()
        cursor.execute('''
        SELECT avatarlink FROM steamid_detail WHERE steamid = ?
        ''', (steamid,))
        result = cursor.fetchone()
        if not result or result != data["data"]["avatar"]:
            urllib.request.urlretrieve(data["data"]["avatar"], Path(f"./avatar/{steamid}.png"))
        lasttime = "none"
        if len(data["data"]["historyDates"]) != 0:
            lasttime = data["data"]["historyDates"][0]
        cursor.execute('''
        INSERT OR REPLACE INTO steamid_detail 
            (steamid,
            avatarlink,
            name,
            pvpScore,
            cnt,
            kd,
            winRate,
            pwRating,
            avgWe,
            kills,
            deaths,
            assists,
            rws,
            adr,
            headShotRatio,
            entryKillRatio,
            vs1WinRate,
            lasttime) 
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (steamid, 
              data["data"]["avatar"],
              data["data"]["name"],
              data["data"]["pvpScore"],
              data["data"]["cnt"],
              data["data"]["kd"],
              data["data"]["winRate"],
              data["data"]["pwRating"],
              data["data"]["avgWe"],
              data["data"]["kills"],
              data["data"]["deaths"],
              data["data"]["assists"],
              data["data"]["rws"],
              data["data"]["adr"],
              data["data"]["headShotRatio"],
              data["data"]["entryKillRatio"],
              data["data"]["vs1WinRate"],
              lasttime
              ))
        self.conn.commit()
    
    def get_stats(self, steamid):
        cursor = self.conn.cursor()
        cursor.execute('''
        SELECT * FROM steamid_detail WHERE steamid = ?
        ''', (steamid,))
        return cursor.fetchone()

    async def get_stats_image(self, steamid):
        result = self.get_stats(steamid)
        if result:
            (steamid, _, name, pvpScore, cnt, kd, winRate, pwRating, avgWe, kills, deaths, assists, rws, adr, headShotRatio, entryKillRatio, vs1WinRate, lasttime) = result
            html = data_content
            html = html.replace("_avatar_", path_to_file_url(os.path.join("avatar", f"{steamid}.png")))
            html = html.replace("_name_", name)
            html = html.replace("_WE_", f"{avgWe: .1f}")
            html = html.replace("_Rating_",f"{pwRating: .2f}")
            html = html.replace("_ELO_", f"{pvpScore}")
            html = html.replace("_cnt_", f"{cnt}")
            html = html.replace("_winRate_", f"{winRate * 100 : .0f}%")
            html = html.replace("_RWS_", f"{rws: .2f}")
            html = html.replace("_ADR_", f"{adr: .2f}")
            html = html.replace("_KD_", f"{kd: .2f}")
            html = html.replace("_headShotRatio_", f"{headShotRatio * 100 : .0f}%")
            html = html.replace("_entryKillRatio_", f"{entryKillRatio * 100 : .0f}%")
            html = html.replace("_vs1WinRate_", f"{vs1WinRate * 100 : .0f}%")
            html = html.replace("_avgK_", "nan" if cnt == 0 else f"{kills / cnt : .2f}")
            html = html.replace("_avgD_", "nan" if cnt == 0 else f"{deaths / cnt : .2f}")
            html = html.replace("_avgA_", "nan" if cnt == 0 else f"{assists / cnt : .2f}")
            html = html.replace("_LastTime_", lasttime)

            pool = "S"
            color = "#87CEFA"
            arc = 0
            if pvpScore <= 1000:
                pool = "D"
                arc = pvpScore / 1000
            elif pvpScore <= 1150:
                pool = "C"
                arc = (pvpScore - 1000) / 150
            elif pvpScore <= 1300:
                pool = "C+"
                arc = (pvpScore - 1150) / 150
            elif pvpScore <= 1450:
                pool = "C+"
                arc = (pvpScore - 1300) / 150
                color = "#FFDF00"
            elif pvpScore <= 1600:
                pool = "B"
                arc = (pvpScore - 1450) / 150
            elif pvpScore <= 1750:
                pool = "B+"
                arc = (pvpScore - 1600) / 150
            elif pvpScore <= 1900:
                pool = "B+"
                arc = (pvpScore - 1750) / 150
                color = "#FFDF00"
            elif pvpScore <= 2050:
                pool = "A"
                arc = (pvpScore - 1900) / 150
            elif pvpScore <= 2200:
                pool = "A+"
                arc = (pvpScore - 2050) / 150
            elif pvpScore <= 2400:
                pool = "A+"
                arc = (pvpScore - 2200) / 200
                color = "#FFDF00"
            
            html = html.replace("_COLOR_", color)
            html = html.replace("_ARC_", f"{226.2 * (1 - arc)}")
            html = html.replace("_POOL_", pool)
            with tempfile.NamedTemporaryFile(mode='w', encoding='utf-8', suffix=".html", dir="temp", delete=False) as temp_file:
                temp_file.write(html)
                temp_file.close()
                img = await screenshot_html_to_png(path_to_file_url(temp_file.name), 480, 560)
                os.remove(temp_file.name)
            return BytesIO(img)
        else:
            return None
    
    def add_member(self, gid, uid):
        if gid.startswith("group_"):
            gid = gid.split("_")[1]
            cursor = self.conn.cursor()
            cursor.execute(
                'INSERT OR IGNORE INTO group_members (gid, uid) VALUES (?, ?)',
                (gid, uid)
            )
            self.conn.commit()
    
    def get_member(self, gid):
        if gid.startswith("group_"):
            gid = gid.split("_")[1]
            cursor = self.conn.cursor()
            cursor.execute(
                'SELECT uid FROM group_members WHERE gid = ?',
                (gid, )
            )
            members = [row[0] for row in cursor.fetchall()]
            return members
        return []

    async def get_elo_rk_image(self, uids):
        datas = []
        for uid in uids:
            steamid = self.get_steamid(uid)
            if steamid != None:
                result = self.get_stats(steamid)
                if result:
                    datas.append((steamid, result[3]))
        sorted(datas, key=lambda x: x[1], reverse=True)
        if len(datas) == 0:
            return None
        html = rank_content[0]
        max_value = datas[0][1]
        min_value = datas[-1][1] - 10
        sum = 0
        for (steamid, elo) in datas:
            score = (elo - min_value) / (max_value - min_value)
            temp_html = rank_content[1]
            temp_html = temp_html.replace('_AVATAR_', path_to_file_url(os.path.join("avatar", f"{steamid}.png")))
            temp_html = temp_html.replace('_COLOR_', red_to_green_color(score))
            temp_html = temp_html.replace('_LEN_', f"{round(500 * score)}")
            temp_html = temp_html.replace('_VALUE_', f"{elo}")
            html += temp_html
            sum += elo
        html += rank_content[2]
        avg = sum / len(datas)
        score = (avg - min_value) / (max_value - min_value)
        html = html.replace("_AVG_", f"{round(avg)}")
        html = html.replace("_AVGPOS_", f"{round(score * 500) + 98}")
        html = html.replace("_AVGLEN_", f"{round(len(datas) * 80) + 90}")
        html = html.replace("_TITLE_", "ELO排名")
        with tempfile.NamedTemporaryFile(mode='w', encoding='utf-8', suffix=".html", dir="temp", delete=False) as temp_file:
            temp_file.write(html)
            temp_file.close()
            img = await screenshot_html_to_png(path_to_file_url(temp_file.name), 700, 200 + len(datas) * 80)
            os.remove(temp_file.name)
        return BytesIO(img)

        
if not os.path.exists("avatar"):
    os.makedirs("avatar", exist_ok=True)

if not os.path.exists("temp"):
    os.makedirs("temp", exist_ok=True)

__plugin_meta__ = PluginMetadata(
    name="cs",
    description="",
    usage="",
    config=Config,
)

config = get_plugin_config(Config)

db = DataManager()

help = on_command("帮助", rule=to_me(), priority=20, block=True)

bind = on_command("绑定", rule=to_me(), priority=10, block=True)

update = on_command("更新数据", rule=to_me(), priority=10, block=True)

show = on_command("查看数据", rule=to_me(), priority=10, block=True)

rank = on_command("排名", rule=to_me(), priority=10, block=True)

updateall = on_command("更新所有", rule=to_me(), priority=10, block=True, permission=SUPERUSER)

@help.handle()
async def help_function():
    await help.finish("可用指令：\n/绑定 steamid64\n/更新数据\n/查看数据")

@bind.handle()
async def bind_function(message: MessageEvent, args: Message = CommandArg()):
    uid = message.get_user_id()
    sid = message.get_session_id()
    print("user: %s\nsession: %s\n" % (uid, sid))
    if (steamid := args.extract_plain_text()) and re.match(r'^\d{17}$', steamid):
        db.bind(uid, steamid)
        await bind.finish(f"成功绑定{steamid}。你可以使用 /更新数据 获取战绩。")
    else:
        await bind.finish("请输入steamid64，应该是一个17位整数。你可以使用steamidfinder等工具找到此值。")

@update.handle()
async def update_function(message: MessageEvent):
    uid = message.get_user_id()
    sid = message.get_session_id()

    db.add_member(sid, uid)

    print("user: %s\nsession: %s\n" % (uid, sid))
    steamid = db.get_steamid(uid)
    if steamid != None:
        print(f"更新{steamid}战绩")
        result = db.update_stats(steamid)
        if result == None:
            image = await db.get_stats_image(steamid)
            assert(image != None)
            await update.finish(MessageSegment.file_image(image))
        else:
            await update.finish(result)
    else:
        await update.finish("请先使用 /绑定 steamid64 绑定")

@show.handle()
async def show_function(message: MessageEvent):
    uid = message.get_user_id()
    sid = message.get_session_id()
    
    db.add_member(sid, uid)

    print("user: %s\nsession: %s\n" % (uid, sid))
    steamid = db.get_steamid(uid)
    if steamid != None:
        print(f"查询{steamid}战绩")
        image = await db.get_stats_image(steamid)
        if image:
            await show.finish(MessageSegment.file_image(image))
        else:
            await show.finish("请先使用 /更新数据 更新战绩")
    else:
        await show.finish("请先使用 /绑定 steamid64 绑定")

@rank.handle()
async def rank_function(message: MessageEvent):
    uid = message.get_user_id()
    sid = message.get_session_id()

    image = await db.get_elo_rk_image(db.get_member(sid))
    if image:
        await show.finish(MessageSegment.file_image(image))
    else:
        await rank.finish("没有人类了")

@updateall.handle()
async def updateall_function():
    await updateall.finish("开始更新所有数据")

