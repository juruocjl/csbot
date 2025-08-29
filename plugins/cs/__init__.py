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
import time

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
async def gen_rank_html(datas, min_value, max_value, ndigits, title, suf=''):
    html = rank_content[0]
    sum = 0
    for (steamid, value) in datas:
        score = (value - min_value) / (max_value - min_value)
        temp_html = rank_content[1]
        temp_html = temp_html.replace('_AVATAR_', path_to_file_url(os.path.join("avatar", f"{steamid}.png")))
        temp_html = temp_html.replace('_COLOR_', red_to_green_color(score))
        temp_html = temp_html.replace('_LEN_', f"{round(500 * score)}")
        temp_html = temp_html.replace('_VALUE_', f"{value: .{ndigits}f}{suf}")
        html += temp_html
        sum += value
    html += rank_content[2]
    avg = sum / len(datas)
    score = (avg - min_value) / (max_value - min_value)
    html = html.replace("_AVG_", f"{avg: .{ndigits}f}{suf}")
    html = html.replace("_AVGPOS_", f"{round(score * 500) + 98}")
    html = html.replace("_AVGLEN_", f"{round(len(datas) * 90) + 40}")
    html = html.replace("_TITLE_", title)
    with tempfile.NamedTemporaryFile(mode='w', encoding='utf-8', suffix=".html", dir="temp", delete=False) as temp_file:
        temp_file.write(html)
        temp_file.close()
        img = await screenshot_html_to_png(path_to_file_url(temp_file.name), 750, 200 + len(datas) * 90)
        os.remove(temp_file.name)
    return BytesIO(img)

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
            seasonId TEXT,
            PRIMARY KEY (steamid)
        )
        ''')

        cursor.execute('''
        CREATE TABLE IF NOT EXISTS matches (
            mid TEXT,
            steamid TEXT,
            seasonId TEXT,
            mapName TEXT,
            team INT,
            winTeam INT,
            score1 INT,
            score2 INT,
            pwRating FLOAT,
            we FLOAT,
            timeStamp INT,
            kill INT,
            death INT,
            assist INT,
            duration INT,
            mode TEXT,
            pvpScore INT,
            pvpScoreChange INT,
            pvpMvp INT,
            isgroup INT,
            greenMatch INT,
            PRIMARY KEY (mid, steamid)
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
            lasttime,
            seasonId) 
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
              lasttime,
              data["data"]["seasonId"],
              ))
        self.conn.commit()
        for SeasonID in [os.getenv("SeasonId"), os.getenv("lastSeasonId")]:
            page = 1
            while True:
                url = "https://api.wmpvp.com/api/csgo/home/match/list"  

                headers = {
                    "appversion": "3.5.4.172",
                    "token": os.getenv("wmtoken")
                }

                payload = {
                    "csgoSeasonId": SeasonID,
                    "dataSource": 3,
                    "mySteamId": os.getenv("mySteamId"),
                    "page": page,
                    "pageSize": 20,
                    "pvpType": -1,
                    "toSteamId": steamid
                }

                result = requests.post(url, json=payload, headers=headers,verify=False)
                data = result.json()
                if data["statusCode"] != 0:
                    return "爬取失败：" + data["errorMessage"]
                failed = 0
                success = 0
                for match in data['data']['matchList']:
                    cursor = self.conn.cursor()
                    cursor.execute('''
                        INSERT OR IGNORE INTO matches
                        (mid,
                        steamid,
                        seasonId,
                        mapName,
                        team,
                        winTeam,
                        score1,
                        score2,
                        pwRating,
                        we,
                        timeStamp,
                        kill,
                        death,
                        assist,
                        duration,
                        mode,
                        pvpScore,
                        pvpScoreChange,
                        pvpMvp,
                        isgroup,
                        greenMatch)
                        VALUES
                        (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    ''',(match["matchId"],match["playerId"],SeasonID,
                        match["mapName"],
                        match["team"],
                        match["winTeam"],
                        match["score1"],
                        match["score2"],
                        match["pwRating"],
                        match["we"],
                        match["timeStamp"],
                        match["kill"],
                        match["death"],
                        match["assist"],
                        match["duration"],
                        match["mode"],
                        match["pvpScore"],
                        match["pvpScoreChange"],
                        int(match["pvpMvp"]),
                        int(match["group"]),
                        match["greenMatch"]
                    ))
                    if cursor.rowcount > 0:
                        success += 1
                    else:
                        failed += 1
                print(f"success {success}, fail {failed}")
                if success == 0 or failed >= 2:
                    break
                page += 1
        self.conn.commit()
        return None
    
    def get_stats(self, steamid):
        cursor = self.conn.cursor()
        cursor.execute('''
        SELECT * FROM steamid_detail WHERE steamid = ?
        ''', (steamid,))
        return cursor.fetchone()

    async def get_stats_image(self, steamid):
        result = self.get_stats(steamid)
        if result:
            (steamid, _, name, pvpScore, cnt, kd, winRate, pwRating, avgWe, kills, deaths, assists, rws, adr, headShotRatio, entryKillRatio, vs1WinRate, lasttime, _) = result
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
            if pvpScore == 0:
                pool = "?"
            elif pvpScore <= 1000:
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
    
    
    def get_all_steamid(self):
        cursor = self.conn.cursor()
        cursor.execute('SELECT steamid FROM members_steamid',)
        return [row[0] for row in cursor.fetchall()]
    
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
                if result and result[18] == os.getenv("SeasonId"):
                    datas.append((steamid, result[3]))
        datas = sorted(datas, key=lambda x: x[1], reverse=True)
        if len(datas) == 0:
            return None
        return await gen_rank_html(datas, datas[-1][1] - 10, datas[0][1], 0, "天梯分排名")

    async def get_rt_rk_image(self, uids):
        datas = []
        for uid in uids:
            steamid = self.get_steamid(uid)
            if steamid != None:
                result = self.get_stats(steamid)
                if result and result[18] == os.getenv("SeasonId"):
                    datas.append((steamid, result[7]))
        datas = sorted(datas, key=lambda x: x[1], reverse=True)
        if len(datas) == 0:
            return None
        return await gen_rank_html(datas, max(0, datas[-1][1] - 0.1), datas[0][1], 2, "rating")
    
    async def get_we_rk_image(self, uids):
        datas = []
        for uid in uids:
            steamid = self.get_steamid(uid)
            if steamid != None:
                result = self.get_stats(steamid)
                if result and result[18] == os.getenv("SeasonId"):
                    datas.append((steamid, result[8]))
        datas = sorted(datas, key=lambda x: x[1], reverse=True)
        if len(datas) == 0:
            return None
        return await gen_rank_html(datas, max(0, datas[-1][1] - 1), datas[0][1], 1, "WE")
    
    async def get_adr_rk_image(self, uids):
        datas = []
        for uid in uids:
            steamid = self.get_steamid(uid)
            if steamid != None:
                result = self.get_stats(steamid)
                if result and result[18] == os.getenv("SeasonId"):
                    datas.append((steamid, result[13]))
        datas = sorted(datas, key=lambda x: x[1], reverse=True)
        if len(datas) == 0:
            return None
        return await gen_rank_html(datas, max(0, datas[-1][1] - 10), datas[0][1], 1, "ADR")

    async def get_cnt_rk_image(self, uids):
        datas = []
        for uid in uids:
            steamid = self.get_steamid(uid)
            if steamid != None:
                result = self.get_stats(steamid)
                if result and result[18] == os.getenv("SeasonId"):
                    datas.append((steamid, result[4]))
        datas = sorted(datas, key=lambda x: x[1], reverse=True)
        if len(datas) == 0:
            return None
        return await gen_rank_html(datas, max(0, datas[-1][1] - 1), datas[0][1], 1, "场次")
    
    async def get_wr_rk_image(self, uids):
        datas = []
        for uid in uids:
            steamid = self.get_steamid(uid)
            if steamid != None:
                result = self.get_stats(steamid)
                if result and result[18] == os.getenv("SeasonId"):
                    datas.append((steamid, round(result[6] * 100)))
        datas = sorted(datas, key=lambda x: x[1], reverse=True)
        if len(datas) == 0:
            return None
        return await gen_rank_html(datas, max(0, datas[-1][1] - 10), datas[0][1], 1, "胜率", suf='%')

    async def get_ek_rk_image(self, uids):
        datas = []
        for uid in uids:
            steamid = self.get_steamid(uid)
            if steamid != None:
                result = self.get_stats(steamid)
                if result and result[18] == os.getenv("SeasonId"):
                    datas.append((steamid, round(result[15] * 100)))
        datas = sorted(datas, key=lambda x: x[1], reverse=True)
        if len(datas) == 0:
            return None
        return await gen_rank_html(datas, max(0, datas[-1][1] - 10), datas[0][1], 1, "首杀率", suf='%')

    async def get_hs_rk_image(self, uids):
        datas = []
        for uid in uids:
            steamid = self.get_steamid(uid)
            if steamid != None:
                result = self.get_stats(steamid)
                if result and result[18] == os.getenv("SeasonId"):
                    datas.append((steamid, round(result[14] * 100)))
        datas = sorted(datas, key=lambda x: x[1], reverse=True)
        if len(datas) == 0:
            return None
        return await gen_rank_html(datas, max(0, datas[-1][1] - 10), datas[0][1], 1, "爆头率", suf='%')
    
    async def get_1v1_rk_image(self, uids):
        datas = []
        for uid in uids:
            steamid = self.get_steamid(uid)
            if steamid != None:
                result = self.get_stats(steamid)
                if result and result[18] == os.getenv("SeasonId"):
                    datas.append((steamid, round(result[16] * 100)))
        datas = sorted(datas, key=lambda x: x[1], reverse=True)
        if len(datas) == 0:
            return None
        return await gen_rank_html(datas, max(0, datas[-1][1] - 10), datas[0][1], 1, "1v1胜率", suf='%')

    async def get_k_rk_image(self, uids):
        datas = []
        for uid in uids:
            steamid = self.get_steamid(uid)
            if steamid != None:
                result = self.get_stats(steamid)
                if result and result[18] == os.getenv("SeasonId") and result[4] != 0:
                    datas.append((steamid, result[9] / result[4]))
        datas = sorted(datas, key=lambda x: x[1], reverse=True)
        if len(datas) == 0:
            return None
        return await gen_rank_html(datas, max(0, datas[-1][1] - 1), datas[0][1], 2, "场均击杀")

    async def get_d_rk_image(self, uids):
        datas = []
        for uid in uids:
            steamid = self.get_steamid(uid)
            if steamid != None:
                result = self.get_stats(steamid)
                if result and result[18] == os.getenv("SeasonId") and result[4] != 0:
                    datas.append((steamid, result[10] / result[4]))
        datas = sorted(datas, key=lambda x: x[1])
        if len(datas) == 0:
            return None
        return await gen_rank_html(datas, max(0, datas[0][1] - 1), datas[-1][1], 2, "场均死亡")

    async def get_a_rk_image(self, uids):
        datas = []
        for uid in uids:
            steamid = self.get_steamid(uid)
            if steamid != None:
                result = self.get_stats(steamid)
                if result and result[18] == os.getenv("SeasonId") and result[4] != 0:
                    datas.append((steamid, result[11] / result[4]))
        datas = sorted(datas, key=lambda x: x[1], reverse=True)
        if len(datas) == 0:
            return None
        return await gen_rank_html(datas, max(0, datas[-1][1] - 1), datas[0][1], 2, "场均助攻")

    async def get_jinli_rk_image(self, uids):
        datas = []
        for uid in uids:
            steamid = self.get_steamid(uid)
            if steamid != None:
                cursor = self.conn.cursor()
                cursor.execute('''SELECT AVG(pwRating) as avgRating
                                FROM 'matches'
                                WHERE 
                                (mode =="天梯组排对局" or mode == "天梯单排对局" or mode == "PVP周末联赛")
                                and winTeam != team 
                                and (seasonId == ? or seasonId == ?) and steamid == ?
                            ''', (os.getenv("SeasonId"), os.getenv("lastSeasonId"), steamid))
                result = cursor.fetchone()[0]
                print(result)
                if result:
                    datas.append((steamid, result))
        datas = sorted(datas, key=lambda x: x[1], reverse=True)
        if len(datas) == 0:
            return None
        return await gen_rank_html(datas, max(0, datas[-1][1] - 0.1), datas[0][1], 2, "两赛季 失败场次rating")

    async def get_zhayu_rk_image(self, uids):
        datas = []
        for uid in uids:
            steamid = self.get_steamid(uid)
            if steamid != None:
                cursor = self.conn.cursor()
                cursor.execute('''SELECT AVG(pwRating) as avgRating
                                FROM 'matches'
                                WHERE 
                                (mode =="天梯组排对局" or mode == "天梯单排对局" or mode == "PVP周末联赛")
                                and winTeam == team 
                                and min(score1, score2) <= 6
                                and (seasonId == ? or seasonId == ?) and steamid == ?
                            ''', (os.getenv("SeasonId"), os.getenv("lastSeasonId"), steamid))
                result = cursor.fetchone()[0]
                print(result)
                if result:
                    datas.append((steamid, result))
        datas = sorted(datas, key=lambda x: x[1], reverse=True)
        if len(datas) == 0:
            return None
        return await gen_rank_html(datas, max(0, datas[-1][1] - 0.1), datas[0][1], 2, "两赛季 小分拿下rating")

    async def get_yanyuan_rk_image(self, uids):
        datas = []
        for uid in uids:
            steamid = self.get_steamid(uid)
            if steamid != None:
                cursor = self.conn.cursor()
                cursor.execute('''SELECT AVG(pwRating) as avgRating
                                FROM 'matches'
                                WHERE 
                                (mode =="天梯组排对局" or mode == "天梯单排对局" or mode == "PVP周末联赛")
                                and isgroup == 1
                                and (seasonId == ? or seasonId == ?) and steamid == ?
                            ''', (os.getenv("SeasonId"), os.getenv("lastSeasonId"), steamid))
                result = cursor.fetchone()[0]
                print(result)
                if result:
                    datas.append((steamid, result))
        datas = sorted(datas, key=lambda x: x[1])
        if len(datas) == 0:
            return None
        return await gen_rank_html(datas, max(0, datas[0][1] - 0.1), datas[-1][1], 2, "两赛季 组排rating")

    async def get_guli_rk_image(self, uids):
        datas = []
        for uid in uids:
            steamid = self.get_steamid(uid)
            if steamid != None:
                cursor = self.conn.cursor()
                cursor.execute('''SELECT COUNT(*) AS total_count
                                FROM 'matches'
                                WHERE 
                                (mode =="天梯组排对局" or mode == "天梯单排对局")
                                and isgroup == 0
                                and (seasonId == ? or seasonId == ?) and steamid == ?
                            ''', (os.getenv("SeasonId"), os.getenv("lastSeasonId"), steamid))
                result = cursor.fetchone()[0]
                print(result)
                if result:
                    datas.append((steamid, result))
        datas = sorted(datas, key=lambda x: x[1], reverse=True)
        if len(datas) == 0:
            return None
        return await gen_rank_html(datas, max(0, datas[-1][1] - 0.1), datas[0][1], 0, "两赛季 单排场数")
    
    async def get_shangfen_rk_image(self, uids):
        datas = []
        for uid in uids:
            steamid = self.get_steamid(uid)
            if steamid != None:
                cursor = self.conn.cursor()
                cursor.execute('''SELECT SUM(pvpScoreChange) as ScoreDelta
                                FROM 'matches'
                                WHERE 
                                (mode =="天梯组排对局" or mode == "天梯单排对局" or mode == "PVP周末联赛")
                                and timeStamp >= ?
                                and (seasonId == ? or seasonId == ?) and steamid == ?
                            ''', (int(time.time()) - 7 * 24 * 3600, os.getenv("SeasonId"), os.getenv("lastSeasonId"), steamid))
                result = cursor.fetchone()[0]
                print(result)
                if result:
                    datas.append((steamid, result))
        datas = sorted(datas, key=lambda x: x[1], reverse=True)
        if len(datas) == 0:
            return None
        return await gen_rank_html(datas, max(0, datas[-1][1] - 10), datas[0][1], 0, "7天内 上分")

    async def get_neizhan_rk_image(self, uids):
        datas = []
        for uid in uids:
            steamid = self.get_steamid(uid)
            if steamid != None:
                cursor = self.conn.cursor()
                cursor.execute('''SELECT AVG(pwRating) as avgRating
                                FROM 'matches'
                                WHERE 
                                mode == "PVP自定义"
                                and (seasonId == ? or seasonId == ?) and steamid == ?
                            ''', (os.getenv("SeasonId"), os.getenv("lastSeasonId"), steamid))
                result = cursor.fetchone()[0]
                print(result)
                if result:
                    datas.append((steamid, result))
        datas = sorted(datas, key=lambda x: x[1], reverse=True)
        if len(datas) == 0:
            return None
        return await gen_rank_html(datas, max(0, datas[-1][1] - 0.1), datas[0][1], 2, "两赛季 馁站rating")

    async def get_var_rk_image(self, uids):
        datas = []
        for uid in uids:
            steamid = self.get_steamid(uid)
            if steamid != None:
                cursor = self.conn.cursor()
                cursor.execute("""
                                WITH filtered_matches AS (
                                    SELECT pwRating FROM 'matches'
                                    WHERE 
                                    (mode =="天梯组排对局" or mode == "天梯单排对局" or mode == "PVP周末联赛")
                                    and (seasonId == ? or seasonId == ?) and steamid == ?
                                )
                                SELECT SUM((pwRating - avg_val) * (pwRating - avg_val)) AS SUM2, COUNT(pwRating) AS CNT
                                FROM filtered_matches, 
                                    (SELECT AVG(pwRating) AS avg_val FROM filtered_matches) AS sub
                            """, (os.getenv("SeasonId"), os.getenv("lastSeasonId"), steamid))
                result = cursor.fetchone()
                print(result)
                if result[1] != 0:
                    datas.append((steamid, result[0] / (result[1] - 1)))
        datas = sorted(datas, key=lambda x: x[1], reverse=True)
        if len(datas) == 0:
            return None
        return await gen_rank_html(datas, max(0, datas[-1][1] - 0.1), datas[0][1], 2, "两赛季 rating方差")

    def query(self, sql):
        cursor = self.conn.cursor()
        cursor.execute(sql)
        self.conn.commit()
        url_pattern = re.compile(
            r'\'https?://\S+?\''
        )
        return url_pattern.sub('url deleted', str(cursor.fetchall()))


        
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

updateall = on_command("全部更新", rule=to_me(), priority=10, block=True, permission=SUPERUSER)

sql = on_command("sql", rule=to_me(), priority=10, block=True, permission=SUPERUSER)

@help.handle()
async def help_function():
    await help.finish("可用指令：\n/绑定 steamid64\n/更新数据\n/查看数据\n/排名")

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
async def rank_function(message: MessageEvent, args: Message = CommandArg()):
    uid = message.get_user_id()
    sid = message.get_session_id()

    wrong_arg = False
    image = None
    rk_type = args.extract_plain_text()
    if rk_type == "ELO":
        image = await db.get_elo_rk_image(db.get_member(sid))
    elif rk_type == "rt":
        image = await db.get_rt_rk_image(db.get_member(sid))
    elif rk_type == "WE":
        image = await db.get_we_rk_image(db.get_member(sid))
    elif rk_type == "ADR":
        image = await db.get_adr_rk_image(db.get_member(sid))
    elif rk_type == "场次":
        image = await db.get_cnt_rk_image(db.get_member(sid))
    elif rk_type == "胜率":
        image = await db.get_wr_rk_image(db.get_member(sid))
    elif rk_type == "首杀":
        image = await db.get_ek_rk_image(db.get_member(sid))
    elif rk_type == "爆头":
        image = await db.get_hs_rk_image(db.get_member(sid))
    elif rk_type == "1v1":
        image = await db.get_1v1_rk_image(db.get_member(sid))
    elif rk_type == "击杀":
        image = await db.get_k_rk_image(db.get_member(sid))
    elif rk_type == "死亡":
        image = await db.get_d_rk_image(db.get_member(sid))
    elif rk_type == "助攻":
        image = await db.get_a_rk_image(db.get_member(sid))
    elif rk_type == "尽力":
        image = await db.get_jinli_rk_image(db.get_member(sid))
    elif rk_type == "炸鱼":
        image = await db.get_zhayu_rk_image(db.get_member(sid))
    elif rk_type == "演员":
        image = await db.get_yanyuan_rk_image(db.get_member(sid))
    elif rk_type == "鼓励":
        image = await db.get_guli_rk_image(db.get_member(sid))
    elif rk_type == "馁站":
        image = await db.get_neizhan_rk_image(db.get_member(sid))
    elif rk_type == "上分":
        image = await db.get_shangfen_rk_image(db.get_member(sid))
    elif rk_type == "方差":
        image = await db.get_var_rk_image(db.get_member(sid))
    else:
        wrong_arg = True
    
    if wrong_arg:
        await rank.finish("请使用 /排名 [选项] 生成排名。目前可用选项：ELO，rt，WE，ADR，场次，胜率，首杀，爆头，1v1，击杀，死亡，助攻，尽力，炸鱼，演员，鼓励，馁站，上分，方差")
    elif image:
        await rank.finish(MessageSegment.file_image(image))
    else:
        await rank.finish("没有人类了")

@updateall.handle()
async def updateall_function():
    await updateall.send("开始更新所有数据")
    for steamid in db.get_all_steamid():
        db.update_stats(steamid)
    await updateall.finish("更新完成")

@sql.handle()
async def sql_function(args: Message = CommandArg()):
    await sql.finish(db.query(args.extract_plain_text()))