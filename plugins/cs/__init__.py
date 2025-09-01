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
import datetime
import logging

logging.basicConfig(
    filename='app.log',  # 日志文件
    level=logging.INFO,  # 只记录 ERROR 及以上级别
    format='%(asctime)s - %(levelname)s - %(message)s'  # 格式
)


load_dotenv(".env.prod")
SeasonId = os.getenv("SeasonId")
lastSeasonId = os.getenv("lastSeasonId")

with open("data.html", 'r', encoding='utf-8') as file:
    data_content = file.read()

with open("rank.html", 'r', encoding='utf-8') as file:
    rank_content = file.read().split("<!--SPLIT--->")

with open("matches.html", 'r', encoding='utf-8') as file:
    matches_content = file.read().split("<!--SPLIT--->")


def get_today_start_timestamp():
    today = datetime.date.today()
    today_start = datetime.datetime.combine(today, datetime.time.min)
    timestamp = int(time.mktime(today_start.timetuple()))
    return timestamp

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


def get_elo_info(pvpScore, seasonId = SeasonId):
    if int(seasonId[1:]) >= 21:
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
        
        return pool, color, arc
    else:
        pool = "S"
        color = "#87CEFA"
        arc = 0
        if pvpScore == 0:
            pool = "?"
        elif pvpScore <= 1000:
            pool = "D"
            arc = pvpScore / 1000
        elif pvpScore <= 1200:
            pool = "D+"
            arc = (pvpScore - 1000) / 200
        elif pvpScore <= 1400:
            pool = "C"
            arc = (pvpScore - 1200) / 200
        elif pvpScore <= 1600:
            pool = "C+"
            arc = (pvpScore - 1400) / 200
        elif pvpScore <= 1800:
            pool = "B"
            arc = (pvpScore - 1600) / 200
        elif pvpScore <= 2000:
            pool = "B+"
            arc = (pvpScore - 1800) / 200
        elif pvpScore <= 2200:
            pool = "A"
            arc = (pvpScore - 2000) / 200
        elif pvpScore <= 2400:
            pool = "A+"
            arc = (pvpScore - 2200) / 200
        
        return pool, color, arc

def output(val, format):
    if format.startswith("d"):
        return f"{val: .{int(format[1:])}f}"
    elif format.startswith("p"):
        return f"{val * 100: .{int(format[1:])}f}%"

async def gen_rank_html(datas, min_value, max_value, title, format):
    html = rank_content[0]
    sum = 0
    for (steamid, value) in datas:
        score = (value[0] - min_value) / (max_value - min_value)
        temp_html = rank_content[1]
        temp_html = temp_html.replace('_AVATAR_', path_to_file_url(os.path.join("avatar", f"{steamid}.png")))
        temp_html = temp_html.replace('_COLOR_', red_to_green_color(score))
        temp_html = temp_html.replace('_LEN_', f"{round(500 * score)}")
        if len(value) == 1:
            temp_html = temp_html.replace('_VALUE_', output(value[0], format))
        else:
            temp_html = temp_html.replace('_VALUE_', f"{output(value[0], format)},{value[1]}场")
        html += temp_html
        sum += value[0]
    html += rank_content[2]
    avg = sum / len(datas)
    score = (avg - min_value) / (max_value - min_value)
    html = html.replace("_AVG_", output(avg, format))
    html = html.replace("_AVGPOS_", f"{round(score * 500) + 98}")
    html = html.replace("_AVGLEN_", f"{round(len(datas) * 90) + 40}")
    html = html.replace("_TITLE_", title)
    with tempfile.NamedTemporaryFile(mode='w', encoding='utf-8', suffix=".html", dir="temp", delete=False) as temp_file:
        temp_file.write(html)
        temp_file.close()
        img = await screenshot_html_to_png(path_to_file_url(temp_file.name), 850, 200 + len(datas) * 90)
        os.remove(temp_file.name)
    return BytesIO(img)

async def gen_matches_html(datas, steamid, name):
    green = "#4CAF50"
    red = "#F44336"
    gray = "#9E9E9E"
    html = matches_content[0]
    html = html.replace("_avatar_", path_to_file_url(os.path.join("avatar", f"{steamid}.png")))
    html = html.replace("_name_", name)
    for match in datas:
        (_,_,seasonId,mapName,team,winTeam,score1,score2,pwRating,we,timeStamp,kill,death,assist,_,mode,pvpScore,pvpScoreChange,pvpMvp,isgroup,greenMatch) = match
        temp_html = matches_content[1]
        myScore = score1 if team == 1 else score2
        opScore = score2 if team == 1 else score1
        Result = 2 if team == winTeam else (1 if winTeam == -1 else 0)
        temp_html = temp_html.replace("_SCORERESULT_", ["负", "平", "胜"][Result])
        temp_html = temp_html.replace("_TIME_", datetime.datetime.fromtimestamp(timeStamp).strftime("%m-%d %H:%M"))
        temp_html = temp_html.replace("_SCORE1_", f"{myScore}")
        temp_html = temp_html.replace("_SCORE2_", f"{opScore}")
        temp_html = temp_html.replace("_SCORECOLOR_", [red, gray, green][Result])
        temp_html = temp_html.replace("_MAP_", mapName)
        temp_html = temp_html.replace("_TYPE_", mode)
        temp_html = temp_html.replace("_RT_",f"{pwRating: .2f}")
        temp_html = temp_html.replace("_RTCOLOR_", green if pwRating > 1 else red)
        temp_html = temp_html.replace("_K_", f"{kill}")
        temp_html = temp_html.replace("_D_", f"{death}")
        temp_html = temp_html.replace("_A_", f"{assist}")
        temp_html = temp_html.replace("_WE_", f"{we: .1f}")
        temp_html = temp_html.replace("_WECOLOR_", green if we > 8 else red)
        temp_html = temp_html.replace("_GROUPDISPLAY_", "inline" if isgroup else "none")
        pool, color, arc = get_elo_info(pvpScore, seasonId)
        temp_html = temp_html.replace("_POOLCOLOR_", color)
        temp_html = temp_html.replace("_ARC_", f"{113.1 * (1 - arc)}")
        temp_html = temp_html.replace("_POOL_", pool)
        temp_html = temp_html.replace("_DELTA_", f"{pvpScoreChange:+}")

        html += temp_html
    html += matches_content[2]

    with tempfile.NamedTemporaryFile(mode='w', encoding='utf-8', suffix=".html", dir="temp", delete=False) as temp_file:
        temp_file.write(html)
        temp_file.close()
        img = await screenshot_html_to_png(path_to_file_url(temp_file.name), 570, 100 + len(datas) * 80)
        os.remove(temp_file.name)
    return BytesIO(img)

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
            lasttime INT,
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
            pvpStars INT,
            pvpScoreChange INT,
            pvpMvp INT,
            isgroup INT,
            greenMatch INT,
            entryKill INT,
            headShot INT,
            headShotRatio FLOAT,
            flashTeammate INT,
            flashSuccess mvpValue,
            twoKill INT,
            threeKill INT,
            fourKill INT,
            fiveKill INT,
            vs1 INT,
            vs2 INT,
            vs3 INT,
            vs4 INT,
            vs5 INT,
            dmgArmor INT,
            dmgHealth INT,
            adpr INT,
            rws FLOAT,
            teamId INT,
            throwsCnt INT,
            snipeNum INT,
            firstDeath INT,
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

    def unbind(self, uid):
        cursor = self.conn.cursor()
        cursor.execute('''
        DELETE FROM members_steamid WHERE uid == ?;
        ''', (uid,))
        self.conn.commit()

    def get_steamid(self, uid):
        cursor = self.conn.cursor()
        cursor.execute('''
        SELECT steamid FROM members_steamid WHERE uid = ?
        ''', (uid,))
        result = cursor.fetchone()
        return result[0] if result else None

    def update_match(self, mid, timeStamp, season):
        logging.info(f"[update_match] start {mid}")
        cursor = self.conn.cursor()
        cursor.execute('''SELECT COUNT(mid) as cnt FROM matches WHERE mid == ?
        ''',(mid, ))
        result = cursor.fetchone()
        if result[0] > 0:
            logging.info(f"[update_match] {mid} in db")
            return 0
        url = "https://api.wmpvp.com/api/v1/csgo/match"
        payload = {
            "matchId": mid,
        }
        header = {
            "appversion": "3.5.4.172",
            "token":os.getenv("wmtoken")
        }
        result = requests.post(url,headers=header,json=payload, verify=False)
        data = result.json()
        if data["statusCode"] != 0:
            logging.error(f"爬取失败  {mid} {data}")
            self.conn.rollback()
            raise RuntimeError("爬取失败：" + data["errorMessage"])
        base = data['data']['base']
        count = {}
        for player in data['data']['players']:
            if player['teamId'] not in count:
                count[player['teamId']] = 0
            count[player['teamId']] += 1
        for player in data['data']['players']:
            cursor.execute('''INSERT OR REPLACE INTO matches
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
                pvpStars,
                pvpScoreChange,
                pvpMvp,
                isgroup,
                greenMatch,
                entryKill,
                headShot,
                headShotRatio,
                flashTeammate,
                flashSuccess,
                twoKill,
                threeKill,
                fourKill,
                fiveKill,
                vs1,
                vs2,
                vs3,
                vs4,
                vs5,
                dmgArmor,
                dmgHealth,
                adpr,
                rws,
                teamId,
                throwsCnt,
                snipeNum,
                firstDeath
                ) VALUES
                (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)  
            ''', 
                (mid, player['playerId'], season, base['map'], player['team'],
                base['winTeam'], base['score1'], base['score2'], player['pwRating'], player['we'],
                timeStamp, player['kill'], player['death'], player['assist'], base['duration'],
                base['mode'], player['pvpScore'], player['pvpStars'], player['pvpScoreChange'], int(player['mvp']),
                bool(count[player['teamId']] > 1), base['greenMatch'], player['entryKill'], player['headShot'], player['headShotRatio'],
                player['flashTeammate'], player['flashSuccess'], player['twoKill'], player['threeKill'], player['fourKill'],
                player['fiveKill'], player['vs1'], player['vs2'], player['vs3'], player['vs4'],
                player['vs5'], player['dmgArmor'], player['dmgHealth'], player['adpr'], player['rws'],
                player['teamId'], player['throwsCnt'], player['snipeNum'], player['firstDeath'])
            )
        return 1

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
            logging.error(f"爬取失败 {steamid} {data}")
            return (False, "爬取失败：" + data["errorMessage"])
        cursor = self.conn.cursor()
        cursor.execute('''
        SELECT avatarlink, lasttime FROM steamid_detail WHERE steamid = ?
        ''', (steamid,))
        result = cursor.fetchone()
        if not result or result[0] != data["data"]["avatar"]:
            urllib.request.urlretrieve(data["data"]["avatar"], Path(f"./avatar/{steamid}.png"))
        LastTime = 0
        if result:
            LastTime = result[1]
        newLastTime = LastTime
        name = data["data"]["name"]
        addMatches = 0
        def work():
            nonlocal newLastTime
            nonlocal addMatches
            for SeasonID in [SeasonId, lastSeasonId]:
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
                        "pageSize": 50,
                        "pvpType": -1,
                        "toSteamId": steamid
                    }

                    result = requests.post(url, json=payload, headers=headers,verify=False)
                    ddata = result.json()
                    if ddata["statusCode"] != 0:
                        logging.error(f"爬取失败 {steamid} {SeasonID} {page} {data}")
                        self.conn.rollback()
                        return (False, "爬取失败：" + data["errorMessage"])
                    time.sleep(0.1)
                    for match in ddata['data']['matchList']:
                        newLastTime = max(newLastTime, match["timeStamp"])
                        if match["timeStamp"] > LastTime:
                            try:
                                self.update_match(match["matchId"], match["timeStamp"], SeasonID)
                                addMatches += 1
                            except RuntimeError as e:
                                return (False, f"爬取失败 {e}")
                        else:
                            return
                    if len(ddata['data']['matchList']) == 0:
                        break
                    page += 1
        work()
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
              newLastTime,
              data["data"]["seasonId"],
              ))
        self.conn.commit()
        return (True, name, addMatches)
    
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
            html = html.replace("_LastTime_", datetime.datetime.fromtimestamp(lasttime).strftime("%y-%m-%d %H:%M"))

            pool, color, arc = get_elo_info(pvpScore)
            
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

    def search_user(self, name, id = 1):
        cursor = self.conn.cursor()
        cursor.execute('''SELECT steamid, name FROM steamid_detail 
            WHERE name LIKE ? 
            ORDER BY steamid
            LIMIT 1 OFFSET ?''', (name, id - 1))
        return cursor.fetchone()

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

    def get_time_sql(self, time_type):
        if time_type == "今日":
            return f"(timeStamp >= {get_today_start_timestamp()})"
        elif time_type == "昨日":
            return f"({get_today_start_timestamp() - 24 * 3600} <= timeStamp and timeStamp < {get_today_start_timestamp()})"
        elif time_type == "本周":
            return f"({int(time.time()) - 7 * 24 * 3600} <= timeStamp)"
        elif time_type == "本赛季":
            return f"(seasonId == '{SeasonId}')"
        elif time_type == "两赛季":
            return f"(seasonId == '{SeasonId}' or seasonId == '{lastSeasonId}')"
        elif time_type == "上赛季":
            return f"(seasonId == '{lastSeasonId}')"
        elif time_type == "全部":
            return f"( 1 == 1 )"
        else:
            raise ValueError("err time")
    
    def get_value(self, steamid, query_type, time_type):
        time_sql = self.get_time_sql(time_type)
        steamid_sql = f"steamid == '{steamid}'"
        cursor = self.conn.cursor()
        
        if query_type == "ELO":
            assert(time_type == "本赛季")
            result = self.get_stats(steamid)
            if result and result[18] == SeasonId and result[3] != 0:
                return result[3], result[4]
            raise ValueError(f"no {query_type}")
        if query_type == "rt":
            cursor.execute(f'''SELECT AVG(pwRating) as avgRating, COUNT(mid) as cnt
                                FROM 'matches'
                                WHERE 
                                (mode =="天梯组排对局" or mode == "天梯单排对局" or mode == "PVP周末联赛")
                                and {time_sql} and {steamid_sql}
                            ''')
            result = cursor.fetchone()
            if result[1] > 0:
                return result
            raise ValueError(f"no {query_type}")
        if query_type == "WE":
            cursor.execute(f'''SELECT AVG(we) as avgwe, COUNT(mid) as cnt
                                FROM 'matches'
                                WHERE 
                                (mode =="天梯组排对局" or mode == "天梯单排对局" or mode == "PVP周末联赛")
                                and {time_sql} and {steamid_sql}
                            ''')
            result = cursor.fetchone()
            if result[1] > 0:
                return result
            raise ValueError(f"no {query_type}")
        if query_type == "ADR":
            assert(time_type == "本赛季")
            result = self.get_stats(steamid)
            if result and result[18] == SeasonId and result[4] != 0:
                return result[13], result[4]
            raise ValueError(f"no {query_type}")
        if query_type == "场次":
            cursor.execute(f'''SELECT COUNT(mid) as cnt
                                FROM 'matches'
                                WHERE 
                                (mode =="天梯组排对局" or mode == "天梯单排对局" or mode == "PVP周末联赛")
                                and {time_sql} and {steamid_sql}
                            ''')
            result = cursor.fetchone()
            if result[0] > 0:
                return result
            raise ValueError(f"no {query_type}")
        if query_type == "胜率":
            cursor.execute(f'''SELECT AVG(winTeam == team) as wr, COUNT(mid) as cnt
                                FROM 'matches'
                                WHERE 
                                (mode =="天梯组排对局" or mode == "天梯单排对局" or mode == "PVP周末联赛")
                                and {time_sql} and {steamid_sql}
                            ''')
            result = cursor.fetchone()
            if result[1] > 0:
                return result
            raise ValueError(f"no {query_type}")
        if query_type == "首杀":
            assert(time_type == "本赛季")
            result = self.get_stats(steamid)
            if result and result[18] == SeasonId and result[4] != 0:
                return result[15], result[4]
            raise ValueError(f"no {query_type}")
        if query_type == "爆头":
            assert(time_type == "本赛季")
            result = self.get_stats(steamid)
            if result and result[18] == SeasonId and result[4] != 0:
                return result[14], result[4]
            raise ValueError(f"no {query_type}")
        if query_type == "1v1":
            assert(time_type == "本赛季")
            result = self.get_stats(steamid)
            if result and result[18] == SeasonId and result[4] != 0:
                return result[16], result[4]
            raise ValueError(f"no {query_type}")
        if query_type == "击杀":
            cursor.execute(f'''SELECT AVG(kill) as avgkill, COUNT(mid) as cnt
                                FROM 'matches'
                                WHERE 
                                (mode =="天梯组排对局" or mode == "天梯单排对局" or mode == "PVP周末联赛")
                                and {time_sql} and {steamid_sql}
                            ''')
            result = cursor.fetchone()
            if result[1] > 0:
                return result
            raise ValueError(f"no {query_type}")
        if query_type == "死亡":
            cursor.execute(f'''SELECT AVG(death) as avgdeath, COUNT(mid) as cnt
                                FROM 'matches'
                                WHERE 
                                (mode =="天梯组排对局" or mode == "天梯单排对局" or mode == "PVP周末联赛")
                                and {time_sql} and {steamid_sql}
                            ''')
            result = cursor.fetchone()
            if result[1] > 0:
                return result
            raise ValueError(f"no {query_type}")
        if query_type == "助攻":
            cursor.execute(f'''SELECT AVG(assist) as avgassist, COUNT(mid) as cnt
                                FROM 'matches'
                                WHERE 
                                (mode =="天梯组排对局" or mode == "天梯单排对局" or mode == "PVP周末联赛")
                                and {time_sql} and {steamid_sql}
                            ''')
            result = cursor.fetchone()
            if result[1] > 0:
                return result
            raise ValueError(f"no {query_type}")
        if query_type == "尽力":
            cursor.execute(f'''SELECT AVG(pwRating) as avgRating, COUNT(mid) as cnt
                                FROM 'matches'
                                WHERE 
                                (mode =="天梯组排对局" or mode == "天梯单排对局" or mode == "PVP周末联赛")
                                and winTeam != team  
                                and {time_sql} and {steamid_sql}
                            ''')
            result = cursor.fetchone()
            if result[1] > 0:
                return result
            raise ValueError(f"no {query_type}")
        if query_type == "带飞":
            cursor.execute(f'''SELECT AVG(pwRating) as avgRating, COUNT(mid) as cnt
                                FROM 'matches'
                                WHERE 
                                (mode =="天梯组排对局" or mode == "天梯单排对局" or mode == "PVP周末联赛")
                                and winTeam == team  
                                and {time_sql} and {steamid_sql}
                            ''')
            result = cursor.fetchone()
            if result[1] > 0:
                return result
            raise ValueError(f"no {query_type}")
        if query_type == "炸鱼":
            cursor.execute(f'''SELECT AVG(pwRating) as avgRating, COUNT(mid) as cnt
                                FROM 'matches'
                                WHERE 
                                (mode =="天梯组排对局" or mode == "天梯单排对局" or mode == "PVP周末联赛")
                                and winTeam == team  
                                and min(score1, score2) <= 6
                                and {time_sql} and {steamid_sql}
                            ''')
            result = cursor.fetchone()
            if result[1] > 0:
                return result
            raise ValueError(f"no {query_type}")
        if query_type == "演员":
            cursor.execute(f'''SELECT AVG(pwRating) as avgRating, COUNT(mid) as cnt
                                FROM 'matches'
                                WHERE 
                                (mode =="天梯组排对局" or mode == "天梯单排对局" or mode == "PVP周末联赛")
                                and isgroup == 1  
                                and {time_sql} and {steamid_sql}
                            ''')
            result = cursor.fetchone()
            if result[1] > 0:
                return result
            raise ValueError(f"no {query_type}")
        if query_type == "鼓励":
            cursor.execute(f'''SELECT COUNT(*) AS total_count, COUNT(mid) as cnt
                                FROM 'matches'
                                WHERE 
                                (mode =="天梯组排对局" or mode == "天梯单排对局")
                                and isgroup == 0
                                and {time_sql} and {steamid_sql}
                            ''')
            result = cursor.fetchone()
            if result[1] > 0:
               return result
            raise ValueError(f"no {query_type}")
        if query_type == "悲情":
            cursor.execute(f'''SELECT COUNT(mid) AS total_count
                                FROM 'matches'
                                WHERE 
                                (mode =="天梯组排对局" or mode == "天梯单排对局" or mode == "PVP周末联赛")
                                and winTeam != team  
                                and pwRating > 1.2
                                and {time_sql} and {steamid_sql}
                            ''')
            result = cursor.fetchone()
            if result[0] > 0:
               return result
            raise ValueError(f"no {query_type}")
        if query_type == "馁站":
            cursor.execute(f'''SELECT AVG(pwRating) as avgRating, COUNT(mid) as cnt
                                FROM 'matches'
                                WHERE 
                                mode == "PVP自定义"
                                and {time_sql} and {steamid_sql}
                            ''')
            result = cursor.fetchone()
            if result[1] > 0:
                return result
            raise ValueError(f"no {query_type}")        
        if query_type == "上分":
            cursor.execute(f'''SELECT SUM(pvpScoreChange) as ScoreDelta, COUNT(mid) as cnt
                            FROM 'matches'
                            WHERE 
                            (mode =="天梯组排对局" or mode == "天梯单排对局" or mode == "PVP周末联赛")
                            and {time_sql} and {steamid_sql}
                            ''')
            result = cursor.fetchone()
            if result[1] > 0:
                return result
            raise ValueError(f"no {query_type}")
        if query_type == "回均首杀":
            cursor.execute(f'''SELECT SUM(entryKill) as totEK, SUM(score1 + score2) as totR, COUNT(mid) as cnt
                            FROM 'matches'
                            WHERE 
                            (mode =="天梯组排对局" or mode == "天梯单排对局" or mode == "PVP周末联赛")
                            and {time_sql} and {steamid_sql}
                            ''')
            result = cursor.fetchone()
            if result[2] > 0:
                return (result[0] / result[1], result[2])
            raise ValueError(f"no {query_type}")
        if query_type == "回均狙杀":
            cursor.execute(f'''SELECT SUM(snipeNum) as totEK, SUM(score1 + score2) as totR, COUNT(mid) as cnt
                            FROM 'matches'
                            WHERE 
                            (mode =="天梯组排对局" or mode == "天梯单排对局" or mode == "PVP周末联赛")
                            and {time_sql} and {steamid_sql}
                            ''')
            result = cursor.fetchone()
            if result[2] > 0:
                return (result[0] / result[1], result[2])
            raise ValueError(f"no {query_type}")
        if query_type == "内鬼":
            cursor.execute(f'''SELECT AVG(flashTeammate) as avgFT, COUNT(mid) as cnt
                            FROM 'matches'
                            WHERE 
                            (mode =="天梯组排对局" or mode == "天梯单排对局" or mode == "PVP周末联赛")
                            and {time_sql} and {steamid_sql}
                            ''')
            result = cursor.fetchone()
            if result[1] > 0:
                return result
            raise ValueError(f"no {query_type}")
        if query_type == "投掷":
            cursor.execute(f'''SELECT AVG(throwsCnt) as avgFT, COUNT(mid) as cnt
                            FROM 'matches'
                            WHERE 
                            (mode =="天梯组排对局" or mode == "天梯单排对局" or mode == "PVP周末联赛")
                            and {time_sql} and {steamid_sql}
                            ''')
            result = cursor.fetchone()
            if result[1] > 0:
                return result
            raise ValueError(f"no {query_type}")
        if query_type == "白给":
            cursor.execute(f'''SELECT SUM(entryKill - firstDeath) as totEKD, SUM(score1 + score2) as totR, COUNT(mid) as cnt
                            FROM 'matches'
                            WHERE 
                            (mode =="天梯组排对局" or mode == "天梯单排对局" or mode == "PVP周末联赛")
                            and {time_sql} and {steamid_sql}
                            ''')
            result = cursor.fetchone()
            if result[2] > 0:
                return (result[0] / result[1], result[2])
            raise ValueError(f"no {query_type}")
        if query_type == "方差":
            cursor.execute(f"""
                                WITH filtered_matches AS (
                                    SELECT pwRating FROM 'matches'
                                    WHERE 
                                    (mode =="天梯组排对局" or mode == "天梯单排对局" or mode == "PVP周末联赛")
                                    and {time_sql} and {steamid_sql}
                                )
                                SELECT SUM((pwRating - avg_val) * (pwRating - avg_val)) AS SUM2, COUNT(pwRating) AS CNT
                                FROM filtered_matches, 
                                    (SELECT AVG(pwRating) AS avg_val FROM filtered_matches) AS sub
                            """)
            result = cursor.fetchone()
            if result[1] > 1:
                return (result[0] / (result[1] - 1), result[1])
            raise ValueError(f"no {query_type}")
        raise ValueError(f"unknown {query_type}")
    
    async def get_matches_image(self, steamid, time_type, LIMIT = 20):
        cursor = self.conn.cursor()
        cursor.execute(f'''SELECT * FROM 'matches'
                            WHERE 
                            {self.get_time_sql(time_type)} and steamid == ?
                            ORDER BY timeStamp DESC
                            LIMIT ?
                        ''', (steamid, LIMIT, ))
        result = cursor.fetchall()
        if len(result):
            return await gen_matches_html(result, steamid, self.get_stats(steamid)[2])
        else:
            return None

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

unbind = on_command("解绑", rule=to_me(), priority=10, block=True)

update = on_command("更新数据", rule=to_me(), priority=10, block=True)

show = on_command("查看数据", rule=to_me(), priority=10, block=True)

rank = on_command("排名", rule=to_me(), priority=10, block=True)

updateall = on_command("全部更新", rule=to_me(), priority=10, block=True, permission=SUPERUSER)

matches = on_command("记录", rule=to_me(), priority=10, block=True)

sql = on_command("sql", rule=to_me(), priority=10, block=True, permission=SUPERUSER)



valid_time = ["今日", "昨日", "本周", "本赛季", "两赛季", "上赛季", "全部"]
# (指令名，标题，默认时间，是否唯一时间，排序是否reversed，最小值，输出格式)
rank_config = [
    ("ELO", "ELO", 3, True, True, "m-10", "d0"),
    ("rt", "rating", 3, False, True, "m-0.05", "d2"),
    ("WE", "WE", 3, False, True, "m-1", "d2"),
    ("ADR", "ADR", 3, True, True, "m-10", "d2"),
    ("场次", "场次", 3, False, True, "v0", "d0"),
    ("胜率", "胜率", 3, False, True, "v0", "p2"),
    ("首杀", "首杀率", 3, True, True, "v0", "p0"),
    ("爆头", "爆头率", 3, True, True, "v0", "p0"),
    ("1v1", "1v1胜率", 3, True, True, "v0", "p0"),
    ("击杀", "场均击杀", 3, False, True, "m-0.1", "d2"),
    ("死亡", "场均死亡", 3, False, True, "m-0.1", "d2"),
    ("助攻", "场均助攻", 3, False, True, "m-0.1", "d2"),
    ("尽力", "未胜利平均rt", 4, False, True, "m-0.05", "d2"),
    ("带飞", "胜利平均rt", 4, False, True, "m-0.05", "d2"),
    ("炸鱼", "小分平均rt", 4, False, True, "m-0.05", "d2"),
    ("演员", "组排平均rt", 4, False, False, "m-0.05", "d2"),
    ("鼓励", "单排场次", 4, False, True, "v0", "d0"),
    ("悲情", ">1.2rt未胜利场次", 4, False, True, "v0", "d0"),
    ("馁站", "馁站平均rt", 4, False, True, "m-0.05", "d2"),
    ("上分", "上分", 2, False, True, "m-1", "d0"),
    ("回均首杀", "平均每回合首杀", 3, False, True, "m-0.5", "d2"),
    ("回均狙杀", "平均每回合狙杀", 3, False, True, "m-0.5", "d2"),
    ("内鬼", "场均闪白队友", 3, False, True, "m-0.5", "d1"),
    ("投掷", "场均道具投掷数", 3, False, True, "m-0.5", "d1"),
    ("白给", "平均每回合首杀-首死", 3, False, False, "m-0.01", "d2"),
    ("方差", "rt方差", 4, False, True, "v0" , "d2"),
]

valid_rank = [a[0] for a in rank_config]


@help.handle()
async def help_function():
    await help.finish(f"""可用指令：
/绑定 steamid64
/解绑
/更新数据
/查看数据 (用户名匹配)
默认查看自己数据。你可以使用用户名匹配查看第一个匹配到用户的数据。
/记录 (用户名匹配) (时间)
默认查看自己记录。最多 20 条。如果只有一个参数，会优先判断是否为时间。默认时间为全部。
/排名 [选项] (时间)
查看指定时间指定排名，具体可选项可以使用 /排名 查看。

(用户名匹配) 使用语法为 % 匹配任意长度串，_ 匹配长度为 1 串。
可选 (时间)：{valid_time}
""")

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

@unbind.handle()
async def unbind_function(message: MessageEvent):
    uid = message.get_user_id()
    sid = message.get_session_id()
    print("user: %s\nsession: %s\n" % (uid, sid))
    db.unbind(uid)
    await unbind.finish(f"解绑成功。")

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
        if result[0]:
            await update.send(f"{result[1]} 成功更新 {result[2]} 场数据")
            image = await db.get_stats_image(steamid)
            assert(image != None)
            await update.finish(MessageSegment.file_image(image))
        else:
            await update.finish(result[1])
    else:
        await update.finish("请先使用 /绑定 steamid64 绑定")

@show.handle()
async def show_function(message: MessageEvent, args: Message = CommandArg()):
    uid = message.get_user_id()
    sid = message.get_session_id()
    
    db.add_member(sid, uid)
    print("user: %s\nsession: %s\n" % (uid, sid))
    steamid = db.get_steamid(uid)
    if user := args.extract_plain_text():
        if result := db.search_user(user):
            await show.send(f"找到用户 {result[1]}")
            steamid = result[0]
        else:
            await show.finish(f"未找到用户")
    if steamid != None:
        print(f"查询{steamid}战绩")
        image = await db.get_stats_image(steamid)
        if image:
            await show.finish(MessageSegment.file_image(image))
        else:
            await show.finish("请先使用 /更新数据 更新战绩")
    else:
        await show.finish("请先使用 /绑定 steamid64 绑定或者指定用户")

@rank.handle()
async def rank_function(message: MessageEvent, args: Message = CommandArg()):
    uid = message.get_user_id()
    sid = message.get_session_id()

    text = args.extract_plain_text()
    uids = db.get_member(sid)


    if text:
        cmd = text.split()
        if len(cmd) > 0:
            rank_type = cmd[0]
            if rank_type in valid_rank:
                index = valid_rank.index(rank_type)
                config = rank_config[index]
                time_type = valid_time[config[2]]
                if len(cmd) >= 2:
                    time_type = cmd[1]
                if time_type in valid_time:
                    if config[3] and time_type != valid_time[config[2]]:
                        await rank.finish(f"{rank_type} 仅支持 {valid_time[config[2]]}")
                    datas = []
                    for uid in uids:
                        steamid = db.get_steamid(uid)
                        if steamid != None:
                            try:
                                val = db.get_value(steamid, rank_type, time_type)
                                print(val)
                                datas.append((steamid, val))
                            except ValueError as e:
                                print(e)
                                pass
                    print(datas)
                    datas = sorted(datas, key=lambda x: x[1][0], reverse=config[4])
                    if len(datas) == 0:
                        await rank.finish("没有人类了")
                    max_value = datas[0][1][0] if config[4] else datas[-1][1][0]
                    min_value = datas[-1][1][0] if config[4] else datas[0][1][0]
                    if max_value == 0 and rank_type == "胜率":
                        await rank.finish("啊😰device😱啊这是人类啊😩哦，bro也没杀人😩这局...这局没有人类了😭只有🐍只有🐭，只有沟槽的野榜😭只有...啊！！！😭我在看什么😭我🌿你的😫🖐🏻️🎧")
                    if config[5].startswith("m"):
                        min_value += float(config[5][1:])
                    else:
                        min_value = float(config[5][1:])
                    await rank.finish(MessageSegment.file_image(await gen_rank_html(datas, min_value, max_value, f"{time_type} {config[1]}", config[6])))

    await rank.finish(f"请使用 /排名 [选项] (时间) 生成排名。\n可用选项：{valid_rank}\n可用时间：{valid_time}")
        

@matches.handle()
async def matches_function(message: MessageEvent, args: Message = CommandArg()):
    uid = message.get_user_id()
    sid = message.get_session_id()

    text = args.extract_plain_text()

    steamid = db.get_steamid(uid)
    time_type = "全部"

    if text:
        cmd = text.split()
        if len(cmd) == 1:
            if cmd[0] not in valid_time:
                if result := db.search_user(cmd[0]):
                    await matches.send(f"找到用户 {result[1]}")
                    steamid = result[0]
                else:
                    await matches.finish(f"未找到用户")
            else:
                time_type = cmd[0]
        elif len(cmd) > 1:
            if result := db.search_user(cmd[0]):
                await matches.send(f"找到用户 {result[1]}")
                steamid = result[0]
            else:
                await matches.finish(f"未找到用户")
            if cmd[1] not in valid_time:
                await matches.finish(f"非法的时间")
            else:
                time_type = cmd[1]
    if steamid != None:
        print(steamid, time_type)
        image = await db.get_matches_image(steamid, time_type)
        if image:
            await matches.finish(MessageSegment.file_image(image))
        else:
            await matches.finish("未找到比赛")
    else:
        await matches.finish("请先使用 /绑定 steamid64 绑定或者指定用户")

@updateall.handle()
async def updateall_function():
    await updateall.send("开始更新所有数据")
    qwq = []
    for steamid in db.get_all_steamid():
        result = db.update_stats(steamid)
        if result[0] and result[2] != 0:
            qwq.append(result[1:])
    await updateall.finish(f"更新完成 {qwq}")

@sql.handle()
async def sql_function(args: Message = CommandArg()):
    await sql.finish(db.query(args.extract_plain_text()))