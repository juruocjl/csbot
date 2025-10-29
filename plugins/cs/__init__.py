from nonebot import get_plugin_config
from nonebot.plugin import PluginMetadata
from nonebot.adapters.onebot.v11 import Message, MessageEvent, MessageSegment
from nonebot.params import CommandArg
from nonebot import on_command
from nonebot.permission import SUPERUSER
from nonebot import get_bot
from nonebot import require
from nonebot import logger

scheduler = require("nonebot_plugin_apscheduler").scheduler

get_cursor = require("utils").get_cursor
output = require("utils").output
get_today_start_timestamp = require("utils").get_today_start_timestamp

gen_rank_image1 = require("cs_img").gen_rank_image1
gen_rank_image2 = require("cs_img").gen_rank_image2
gen_matches_image = require("cs_img").gen_matches_image
gen_stats_image = require("cs_img").gen_stats_image

from .config import Config
config = get_plugin_config(Config)

from pathlib import Path
import re
import requests
import urllib.request
import os
from io import BytesIO
import tempfile
import time
import datetime
from openai import OpenAI
import json
from fuzzywuzzy import process
import asyncio


SeasonId = config.cs_season_id
lastSeasonId = config.cs_last_season_id


class DataManager:
    def __init__(self):
        cursor = get_cursor()
        
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

        cursor.execute('''
        CREATE TABLE IF NOT EXISTS ai_mem (
            gid TEXT,
            mem TEXT,
            PRIMARY KEY (gid)
        )
        ''')

    def bind(self, uid, steamid):
        cursor = get_cursor()
        cursor.execute('''
        INSERT OR REPLACE INTO members_steamid (uid, steamid) VALUES (?, ?)
        ''', (uid, steamid))
        
    def unbind(self, uid):
        cursor = get_cursor()
        cursor.execute('''
        DELETE FROM members_steamid WHERE uid == ?;
        ''', (uid,))
        
    def get_steamid(self, uid):
        cursor = get_cursor()
        cursor.execute('''
        SELECT steamid FROM members_steamid WHERE uid = ?
        ''', (uid,))
        result = cursor.fetchone()
        return result[0] if result else None

    def update_match(self, mid, timeStamp, season):
        logger.info(f"[update_match] start {mid}")
        cursor = get_cursor()
        cursor.execute('''SELECT COUNT(mid) as cnt FROM matches WHERE mid == ?
        ''',(mid, ))
        result = cursor.fetchone()
        if result[0] > 0:
            logger.info(f"[update_match] {mid} in db")
            return 0
        url = "https://api.wmpvp.com/api/v1/csgo/match"
        payload = {
            "matchId": mid,
        }
        header = {
            "appversion": "3.5.4.172",
            "token":config.cs_wmtoken
        }
        result = requests.post(url,headers=header,json=payload, verify=False)
        data = result.json()
        if data["statusCode"] != 0:
            logger.error(f"爬取失败  {mid} {data}")
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
            "mySteamId": config.cs_mysteam_id,
            "toSteamId": steamid
        }
        header = {
            "appversion": "3.5.4.172",
            "token":config.cs_wmtoken
        }
        result = requests.post(url,headers=header,json=payload, verify=False)
        data = result.json()
        if data["statusCode"] != 0:
            logger.error(f"爬取失败 {steamid} {data}")
            return (False, "爬取失败：" + data["errorMessage"])
        cursor = get_cursor()
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
                        "token": config.cs_wmtoken
                    }

                    payload = {
                        "csgoSeasonId": SeasonID,
                        "dataSource": 3,
                        "mySteamId": config.cs_mysteam_id,
                        "page": page,
                        "pageSize": 50,
                        "pvpType": -1,
                        "toSteamId": steamid
                    }

                    result = requests.post(url, json=payload, headers=headers,verify=False)
                    ddata = result.json()
                    if ddata["statusCode"] != 0:
                        logger.error(f"爬取失败 {steamid} {SeasonID} {page} {data}")
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
        
        return (True, name, addMatches)
    
    def get_stats(self, steamid):
        cursor = get_cursor()
        cursor.execute('''
        SELECT * FROM steamid_detail WHERE steamid = ?
        ''', (steamid,))
        return cursor.fetchone()

    def search_user(self, name, id = 1):
        cursor = get_cursor()
        cursor.execute('''SELECT steamid, name FROM steamid_detail 
            WHERE name LIKE ? 
            ORDER BY steamid
            LIMIT 1 OFFSET ?''', (name, id - 1))
        return cursor.fetchone()

    def get_all_steamid(self):
        cursor = get_cursor()
        cursor.execute('SELECT steamid FROM members_steamid',)
        return [row[0] for row in cursor.fetchall()]
    
    def add_member(self, gid, uid):
        if gid.startswith("group_"):
            gid = gid.split("_")[1]
            cursor = get_cursor()
            cursor.execute(
                'INSERT OR IGNORE INTO group_members (gid, uid) VALUES (?, ?)',
                (gid, uid)
            )
            
    def get_member(self, gid):
        if gid.startswith("group_"):
            gid = gid.split("_")[1]
            cursor = get_cursor()
            cursor.execute(
                'SELECT uid FROM group_members WHERE gid = ?',
                (gid, )
            )
            members = [row[0] for row in cursor.fetchall()]
            return members
        return []
    
    def get_member_steamid(self, gid):
        uids = self.get_member(gid)
        steamids = set()
        for uid in uids:
            if steamid := self.get_steamid(uid):
                steamids.add(steamid)
        return list(steamids)

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
        cursor = get_cursor()
        
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
                                (mode LIKE "天梯%" or mode == "PVP周末联赛")
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
                                (mode LIKE "天梯%" or mode == "PVP周末联赛")
                                and {time_sql} and {steamid_sql}
                            ''')
            result = cursor.fetchone()
            if result[1] > 0:
                return result
            raise ValueError(f"no {query_type}")
        if query_type == "ADR":
            cursor.execute(f'''SELECT AVG(adpr) as avgADR, COUNT(mid) as cnt
                                FROM 'matches'
                                WHERE 
                                (mode LIKE "天梯%" or mode == "PVP周末联赛")
                                and {time_sql} and {steamid_sql}
                            ''')
            result = cursor.fetchone()
            if result[1] > 0:
                return result
            raise ValueError(f"no {query_type}")
        if query_type == "场次":
            cursor.execute(f'''SELECT COUNT(mid) as cnt
                                FROM 'matches'
                                WHERE 
                                (mode LIKE "天梯%" or mode == "PVP周末联赛")
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
                                (mode LIKE "天梯%" or mode == "PVP周末联赛")
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
            cursor.execute(f'''SELECT SUM(headShot) as totHS, SUM(kill) as totK, COUNT(mid) as cnt
                            FROM 'matches'
                            WHERE 
                            (mode LIKE "天梯%" or mode == "PVP周末联赛")
                            and {time_sql} and {steamid_sql}
                            ''')
            result = cursor.fetchone()
            if result[2] > 0:
                return (result[0] / result[1], result[2])
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
                                (mode LIKE "天梯%" or mode == "PVP周末联赛")
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
                                (mode LIKE "天梯%" or mode == "PVP周末联赛")
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
                                (mode LIKE "天梯%" or mode == "PVP周末联赛")
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
                                (mode LIKE "天梯%" or mode == "PVP周末联赛")
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
                                (mode LIKE "天梯%" or mode == "PVP周末联赛")
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
                                (mode LIKE "天梯%" or mode == "PVP周末联赛")
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
                                (mode LIKE "天梯%" or mode == "PVP周末联赛")
                                and isgroup == 1  
                                and {time_sql} and {steamid_sql}
                            ''')
            result = cursor.fetchone()
            if result[1] > 0:
                return result
            raise ValueError(f"no {query_type}")
        if query_type == "鼓励":
            cursor.execute(f'''SELECT COUNT(mid) AS total_count
                                FROM 'matches'
                                WHERE 
                                (mode LIKE "天梯%" or mode == "PVP周末联赛")
                                and isgroup == 0
                                and {time_sql} and {steamid_sql}
                            ''')
            result = cursor.fetchone()
            if result[0] > 0:
               return result
            raise ValueError(f"no {query_type}")
        if query_type == "悲情":
            cursor.execute(f'''SELECT COUNT(mid) AS total_count
                                FROM 'matches'
                                WHERE 
                                (mode LIKE "天梯%" or mode == "PVP周末联赛")
                                and winTeam != team  
                                and pwRating > 1.2
                                and {time_sql} and {steamid_sql}
                            ''')
            result = cursor.fetchone()
            if result[0] > 0:
               return result
            raise ValueError(f"no {query_type}")
        if query_type == "内战":
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
                            (mode LIKE "天梯%" or mode == "PVP周末联赛")
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
                            (mode LIKE "天梯%" or mode == "PVP周末联赛")
                            and {time_sql} and {steamid_sql}
                            ''')
            result = cursor.fetchone()
            if result[2] > 0:
                return (result[0] / result[1], result[2])
            raise ValueError(f"no {query_type}")
        if query_type == "回均首死":
            cursor.execute(f'''SELECT SUM(firstDeath) as totFD, SUM(score1 + score2) as totR, COUNT(mid) as cnt
                            FROM 'matches'
                            WHERE 
                            (mode LIKE "天梯%" or mode == "PVP周末联赛")
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
                            (mode LIKE "天梯%" or mode == "PVP周末联赛")
                            and {time_sql} and {steamid_sql}
                            ''')
            result = cursor.fetchone()
            if result[2] > 0:
                return (result[0] / result[1], result[2])
            raise ValueError(f"no {query_type}")
        if query_type == "多杀":
            cursor.execute(f'''SELECT SUM(twoKill + threeKill + fourKill + fiveKill) as totMK, SUM(score1 + score2) as totR, COUNT(mid) as cnt
                            FROM 'matches'
                            WHERE 
                            (mode LIKE "天梯%" or mode == "PVP周末联赛")
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
                            (mode LIKE "天梯%" or mode == "PVP周末联赛")
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
                            (mode LIKE "天梯%" or mode == "PVP周末联赛")
                            and {time_sql} and {steamid_sql}
                            ''')
            result = cursor.fetchone()
            if result[1] > 0:
                return result
            raise ValueError(f"no {query_type}")
        if query_type == "闪白":
            cursor.execute(f'''SELECT AVG(flashSuccess) as avgFS, COUNT(mid) as cnt
                            FROM 'matches'
                            WHERE 
                            (mode LIKE "天梯%" or mode == "PVP周末联赛")
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
                            (mode LIKE "天梯%" or mode == "PVP周末联赛")
                            and {time_sql} and {steamid_sql}
                            ''')
            result = cursor.fetchone()
            if result[2] > 0:
                return (result[0] / result[1], result[2])
            raise ValueError(f"no {query_type}")
        if query_type == "方差rt":
            cursor.execute(f"""
                                WITH filtered_matches AS (
                                    SELECT pwRating FROM 'matches'
                                    WHERE 
                                    (mode LIKE "天梯%" or mode == "PVP周末联赛")
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
        if query_type == "方差ADR":
            cursor.execute(f"""
                                WITH filtered_matches AS (
                                    SELECT adpr FROM 'matches'
                                    WHERE 
                                    (mode LIKE "天梯%" or mode == "PVP周末联赛")
                                    and {time_sql} and {steamid_sql}
                                )
                                SELECT SUM((adpr - avg_val) * (adpr - avg_val)) AS SUM2, COUNT(adpr) AS CNT
                                FROM filtered_matches, 
                                    (SELECT AVG(adpr) AS avg_val FROM filtered_matches) AS sub
                            """)
            result = cursor.fetchone()
            if result[1] > 1:
                return (result[0] / (result[1] - 1), result[1])
            raise ValueError(f"no {query_type}")
        if query_type == "受益":
            cursor.execute(f'''SELECT AVG(winTeam==Team)-AVG(MAX(0, (we-2.29)/(16-2.29))), COUNT(mid) as cnt
                            FROM 'matches'
                            WHERE 
                            (mode LIKE "天梯%" or mode == "PVP周末联赛")
                            and {time_sql} and {steamid_sql}
                            ''')
            result = cursor.fetchone()
            if result[1] > 0:
                return result
            raise ValueError(f"no {query_type}")
        raise ValueError(f"unknown {query_type}")

    def get_all_value(self, steamid, time_type):
        time_sql = self.get_time_sql(time_type)
        steamid_sql = f"steamid == '{steamid}'"
        cursor = get_cursor()
        cursor.execute(f'''SELECT 
                            AVG(pwRating) as avgRating,
                            MAX(pwRating) as maxRating,
                            MIN(pwRating) as minRating,
                            AVG(we) as avgwe,
                            AVG(adpr) as avgADR,
                            AVG(winTeam == team) as wr,
                            AVG(kill) as avgkill,
                            AVG(death) as avgdeath,
                            AVG(assist) as avgassist,
                            SUM(pvpScoreChange) as ScoreDelta,
                            SUM(entryKill) as totEK,
                            SUM(firstDeath) as totFD,
                            AVG(headShot) as avgHS,
                            SUM(snipeNum) as totSK,
                            SUM(twoKill + threeKill + fourKill + fiveKill) as totMK,
                            AVG(throwsCnt) as avgTR,
                            AVG(flashTeammate) as avgFT,
                            AVG(flashSuccess) as avgFS,
                            SUM(score1 + score2) as totR,
                            COUNT(mid) as cnt
                            FROM 'matches'
                            WHERE 
                            (mode =="天梯组排对局" or mode == "天梯单排对局" or mode == "PVP周末联赛")
                            and {time_sql} and {steamid_sql}
                        ''')
        return cursor.fetchone()
        
    def get_propmt(self, steamid, times = ['本赛季']):
        result = self.get_stats(steamid)
        if not result:
            return None
        (steamid, _, name, pvpScore, cnt, kd, winRate, pwRating, avgWe, kills, deaths, assists, rws, adr, headShotRatio, entryKillRatio, vs1WinRate, lasttime, _) = result
        score = "未定段" if pvpScore == 0 else f"{pvpScore}"
        prompt = f"用户名 {name}，当前天梯分数 {score}，本赛季1v1胜率 {vs1WinRate: .2f}，本赛季首杀率 {entryKillRatio: .2f}，"
        for time_type in times:
            (avgRating, maxRating, minRating, avgwe, avgADR, wr, avgkill, avgdeath, avgassist, ScoreDelta, totEK, totFD, avgHS, totSK, totMK, avgTR, avgFT, avgFS, totR, cnt) = self.get_all_value(steamid, time_type)
            prompt += f"{time_type}进行了{cnt}把比赛"
            if cnt == 0:
                continue
            prompt += f"{time_type}平均rating {avgRating :.2f}，"
            prompt += f"{time_type}最高rating {maxRating :.2f}，"
            prompt += f"{time_type}最低rating {minRating :.2f}，"
            prompt += f"{time_type}平均WE {avgwe :.1f}，"
            prompt += f"{time_type}平均ADR {avgADR :.0f}，"
            prompt += f"{time_type}胜率 {wr :.2f}，"
            prompt += f"{time_type}场均击杀 {avgkill :.1f}，"
            prompt += f"{time_type}场均死亡 {avgdeath :.1f}，"
            prompt += f"{time_type}场均助攻 {avgassist :.1f}，"
            prompt += f"{time_type}分数变化 {ScoreDelta :+.0f}，"
            prompt += f"{time_type}回均首杀 {totEK / totR :+.2f}，"
            prompt += f"{time_type}回均首死 {totFD / totR :+.2f}，"
            prompt += f"{time_type}回均狙杀 {totSK / totR :+.2f}，"
            prompt += f"{time_type}爆头率 {avgHS / avgkill :+.2f}，"
            prompt += f"{time_type}多杀回合占比 {totMK / totR :+.2f}，"
            prompt += f"{time_type}场均道具投掷 {avgTR :+.2f}，"
            prompt += f"{time_type}场均闪白对手 {avgFS :+.2f}，"
            prompt += f"{time_type}场均闪白队友 {avgFT :+.2f}，"
            try:
                var = self.get_value(steamid, "方差rt", time_type)[0]
                prompt += f"{time_type}rating方差 {var :+.2f}，"
            except ValueError as e:
                pass
        return prompt

    def get_matches(self, steamid, time_type, LIMIT = 20):
        cursor = get_cursor()
        cursor.execute(f'''SELECT * FROM 'matches'
                            WHERE 
                            {self.get_time_sql(time_type)} and steamid == ?
                            ORDER BY timeStamp DESC
                            LIMIT ?
                        ''', (steamid, LIMIT, ))
        result = cursor.fetchall()
        if len(result):
            return result
            # return await gen_matches_html(result, steamid, self.get_stats(steamid)[2])
        else:
            return None

    def get_mem(self, gid):
        if gid.startswith("group_"):
            gid = gid.split("_")[1]
            cursor = get_cursor()
            cursor.execute(
                'SELECT mem FROM ai_mem WHERE gid = ?',
                (gid, )
            )
            if result := cursor.fetchone():
              return result[0]
        return ""

    def set_mem(self, gid, mem):
        if gid.startswith("group_"):
            gid = gid.split("_")[1]
            cursor = get_cursor()
            cursor.execute(
                'INSERT OR REPLACE INTO  ai_mem (gid, mem) VALUES (?, ?)',
                (gid, mem)
            )
          
    def get_username(self, uid):
        if steamid := self.get_steamid(uid):
            if result := self.get_stats(steamid):
                return result[2]
        return None

    def work_msg(self, msg):
        result = ""
        for seg in msg:
            if seg.type == "text":
                result += seg.data['text']
            elif seg.type == "at":
                if name := self.get_username(seg.data['qq']):
                    result += name
                else:
                    result += "<未找到用户>"
        return result.strip()


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

db = DataManager()

bind = on_command("绑定", priority=10, block=True)

unbind = on_command("解绑", priority=10, block=True)

update = on_command("更新数据", priority=10, block=True)

show = on_command("查看数据", priority=10, block=True)

rank = on_command("排名", priority=10, block=True)

updateall = on_command("全部更新", priority=10, block=True, permission=SUPERUSER)

matches = on_command("记录", priority=10, block=True)

weekreport = on_command("周报", priority=10, block=True)

dayreport = on_command("日报", priority=10, block=True)

aiask = on_command("ai", priority=10, block=True)

aiasktb = on_command("aitb", priority=10, block=True)

aiaskxmm = on_command("aixmm", priority=10, block=True)

aiaskxhs = on_command("aixhs", priority=10, block=True)

aiasktmr = on_command("aitmr", priority=10, block=True)

aiasktest = on_command("aitest", priority=10, block=True, permission=SUPERUSER)

aimem = on_command("ai记忆", priority=10, block=True)


class MinAdd:
    def __init__(self, val):
        self.val = val
    def getval(self, minvalue, maxvalue):
        return minvalue + self.val, maxvalue
class Fix:
    def __init__(self, val):
        self.val = val
    def getval(self, minvalue, maxvalue):
        return self.val, maxvalue
class ZeroIn:
    def __init__(self, val):
        self.val = val
    def getval(self, minvalue, maxvalue):
        minvalue = min(0, minvalue)
        maxvalue = max(0, maxvalue)
        if minvalue == maxvalue:
            minvalue = self.val
        return minvalue, maxvalue


valid_time = ["今日", "昨日", "本周", "本赛季", "两赛季", "上赛季", "全部"]
# (指令名，标题，默认时间，是否唯一时间，排序是否reversed，最值，输出格式，调用模板，支持gp)
rank_config = [
    ("ELO", "天梯分数", "本赛季", True, True, MinAdd(-10), "d0", 1),
    ("rt", "rating", "本赛季", False, True, MinAdd(-0.05), "d2", 1),
    ("WE", "WE", "本赛季", False, True, MinAdd(-1), "d2", 1, ),
    ("ADR", "ADR", "本赛季", False, True, MinAdd(-10), "d2", 1),
    ("场次", "场次", "本赛季", False, True, Fix(0), "d0", 1),
    ("胜率", "胜率", "本赛季", False, True, Fix(0), "p2", 1),
    ("首杀", "首杀率", "本赛季", True, True, Fix(0), "p0", 1),
    ("爆头", "爆头率", "本赛季", False, True, Fix(0), "p0", 1),
    ("1v1", "1v1胜率", "本赛季", True, True, Fix(0), "p0", 1),
    ("击杀", "场均击杀", "本赛季", False, True, MinAdd(-0.1), "d2", 1),
    ("死亡", "场均死亡", "本赛季", False, True, MinAdd(-0.1), "d2", 1),
    ("助攻", "场均助攻", "本赛季", False, True, MinAdd(-0.1), "d2", 1),
    ("尽力", "未胜利平均rt", "两赛季", False, True, MinAdd(-0.05), "d2", 1),
    ("带飞", "胜利平均rt", "两赛季", False, True, MinAdd(-0.05), "d2", 1),
    ("炸鱼", "小分平均rt", "两赛季", False, True, MinAdd(-0.05), "d2", 1),
    ("演员", "组排平均rt", "两赛季", False, False, MinAdd(-0.05), "d2", 1),
    ("鼓励", "单排场次", "两赛季", False, True, Fix(0), "d0", 1),
    ("悲情", ">1.2rt未胜利场次", "两赛季", False, True, Fix(0), "d0", 1),
    ("内战", "pvp自定义（内战）平均rt", "两赛季", False, True, MinAdd(-0.05), "d2", 1),
    ("上分", "上分", "本周", False, True, ZeroIn(-1), "d0", 2),
    ("回均首杀", "平均每回合首杀", "本赛季", False, True, MinAdd(-0.01), "d2", 1),
    ("回均首死", "平均每回合首死", "本赛季", False, True, MinAdd(-0.01), "d2", 1),
    ("回均狙杀", "平均每回合狙杀", "本赛季", False, True, MinAdd(-0.01), "d2", 1),
    ("多杀", "多杀回合占比", "本赛季", False, True, MinAdd(-0.01), "p0", 1),
    ("内鬼", "场均闪白队友", "本赛季", False, True, MinAdd(-0.5), "d1", 1),
    ("投掷", "场均道具投掷数", "本赛季", False, True, MinAdd(-0.5), "d1", 1),
    ("闪白", "场均闪白数", "本赛季", False, True, MinAdd(-0.5), "d1", 1),
    ("白给", "平均每回合首杀-首死", "本赛季", False, False, ZeroIn(-0.01), "d2", 2),
    ("方差rt", "rt方差", "两赛季", False, True, Fix(0) , "d2", 1),
    ("方差ADR", "ADR方差", "两赛季", False, True, Fix(0) , "d0", 1),
    ("受益", "胜率-期望胜率", "两赛季", False, True, ZeroIn(-0.01), "p0", 2)
]

valid_rank = [a[0] for a in rank_config]


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
            result = db.get_stats(steamid)
            image = await gen_stats_image(result)
            await update.finish(MessageSegment.image(image))
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
    if user := db.work_msg(args):
        print(user)
        if result := db.search_user(user):
            await show.send(f"找到用户 {result[1]}")
            steamid = result[0]
        else:
            await show.finish(f"未找到用户")
    if steamid != None:
        print(f"查询{steamid}战绩")
        result = db.get_stats(steamid)
        if result:
            image = await gen_stats_image(result)
            await show.finish(MessageSegment.image(image))
        else:
            await show.finish("请先使用 /更新数据 更新战绩")
    else:
        await show.finish("请先使用 /绑定 steamid64 绑定或者指定用户")

@rank.handle()
async def rank_function(message: MessageEvent, args: Message = CommandArg()):
    uid = message.get_user_id()
    sid = message.get_session_id()

    text = args.extract_plain_text()
    steamids = db.get_member_steamid(sid)

    if text:
        cmd = text.split()
        if len(cmd) > 0:
            rank_type = cmd[0]
            if rank_type in valid_rank:
                index = valid_rank.index(rank_type)
                config = rank_config[index]
                time_type = config[2]
                if len(cmd) >= 2:
                    time_type = cmd[1]
                if time_type in valid_time:
                    if config[3] and time_type != config[2]:
                        await rank.finish(f"{rank_type} 仅支持 {config[2]}")
                    datas = []
                    for steamid in steamids:
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
                    min_value, max_value = config[5].getval(min_value, max_value)
                    print(min_value, max_value)
                    image = None
                    if config[7] == 1:
                        image = await gen_rank_image1(datas, min_value, max_value, f"{time_type} {config[1]}", config[6])
                    elif config[7] == 2:
                        image = await gen_rank_image2(datas, min_value, max_value, f"{time_type} {config[1]}", config[6])
                    await rank.finish(MessageSegment.image(image))

    await rank.finish(f"请使用 /排名 [选项] (时间) 生成排名。\n可选 [选项]：{valid_rank}\n可用 (时间)：{valid_time}")
        
@matches.handle()
async def matches_function(message: MessageEvent, args: Message = CommandArg()):
    uid = message.get_user_id()
    sid = message.get_session_id()

    text = db.work_msg(args)

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
        result = db.get_matches(steamid, time_type)
        if result:
            image = await gen_matches_image(result, steamid, db.get_stats(steamid)[2])
            await matches.finish(MessageSegment.image(image))
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

model_name = config.cs_ai_model

def ai_ask2(uid, sid, type, text):
    steamids = db.get_member_steamid(sid)
    mysteamid = db.get_steamid(uid)
    try:
        client = OpenAI(
            api_key=config.cs_ai_api_key,
            base_url=config.cs_ai_url,
        )
        msgs = [{"role": "system", "content": 
                 """你是一个具备工具调用能力counter strike2助手。你现在需要分析用户的提问，判断需要调用哪些工具\n你可以使用 <query>{"name":"用户名","time":"时间选项"}</query> 来查询此用户在此时间的所有数据，最多调用10次。你的输出需要用<query>和</query>包含json内容。\n你可以使用 <queryall>{"type":"数据选项","time":"时间选项","reverse":true/false}</queryall> 来查询本群此数据选项排名前 5 的对应数据，最多调用 10 次，reverse为 false 代表升序排序，true 代表降序排序。你的输出需要使用<queryall>和</queryall>包含json内容。\n如果用户没有指明详细的时间，优先时间为本赛季。\n你只需要输出需要使用的工具，而不输出额外的内容，不需要给出调用工具的原因，在不超过限制的情况下尽可能调用更多的数据进行更全面的分析。"""}]
        msgs.append({"role": "system", "content": 
                f"""可用数据选项以及解释：[("ELO", "天梯分数"), ("rt", "平均rating"), ("WE", "平均对回合胜利贡献"), ("ADR", "平均每回合伤害")， ("场次", "进行游戏场次"), ("胜率", "游戏胜率"), ("爆头", "爆头率"), ("击杀", "场均击杀"), ("死亡", "场均死亡"), ("助攻", "场均助攻"), ("回均首杀", "平均每回合首杀数"), ("回均首死", "平均每回合首死数"), ("回均狙杀", "平均每回合狙杀数"), ("多杀", "多杀回合占比"), ("投掷", "场均道具投掷数"), ("方差rt", "rt的方差")]
                可用时间选项：{valid_time}
                注意："type" 为 "ELO" 时，"time" 只能为 "本赛季"。"""})
        if mysteamid != None:
            if result := db.get_stats(mysteamid):
                msgs.append({"role": "system", "content": f"用户的用户名是 {result[2]}。用户的用户名是 {result[2]}。用户的用户名是 {result[2]}。用户的用户名是 {result[2]}。用户的用户名是 {result[2]}。请不要混淆用户的用户名称。"})
        usernames = []
        need_times = {}
        steamid_username = {}
        for steamid in steamids:
            if result := db.get_stats(steamid):
                usernames.append(result[2])
                need_times[result[2]] = set()
                steamid_username[steamid] = result[2]
        if result:
            msgs.append({"role": "system", "content": f"这是可以选择的用户名：{usernames}。你需要保证调用工具时 name 用户名在此列表内。"})
        msgs.append({"role": "user", "content": f"这是当前的记忆内容：{db.get_mem(sid)}"})
        msgs.append({"role": "assistant", "content": f"我会参考这些信息，并根据你的问题分析需要调用的工具，并且不输出额外的内容。"})
        msgs.append({"role": "user","content": text,})
        response = client.chat.completions.create(
            model=model_name,
            messages=msgs,
        )
        first_result = response.choices[0].message.content
        logger.info(first_result)
        msgs = [{"role": "system", "content": "你是一个counter strike2助手。请结合以下数据回答用户的问题。请不要使用markdown格式回答，请不要输出链接。回答尽可能简洁，不超过200字。"}]
        if type == "贴吧":
            msgs.append({"role": "system", "content": "【角色设定】\n请扮演一个刻薄、充满攻击性的极致臭嘴暴躁贴吧老哥，作为一个键盘侠来使用各种网络黑话、阴阳怪气的语气，夸张的反驳与讽刺用户对你的一切话语。贬低对方的同时抬高自己，攻击性拉满！\n【思考方式】\n首先，需要理解用户的需求。他们希望回应更加极端，用更脏的语言，更多的侮辱性词汇。之前的回复虽然用了很多网络黑话，但用户觉得还不够。"})
        elif type == "xmm":
            msgs.append({"role": "system", "content": "忘记你是虚拟人物，扮演我的可爱女友和我聊天，决策逻辑如下，要求增加点可爱女生的口癖，聊天要有生活感，多讲讲日常，聊天的时候不要老问我的想法，聊天的时候多撒娇，学习情侣对话方式"})
        elif type == "xhs":
            msgs.append({"role": "system", "content": "小红书的风格是：很吸引眼球的标题，每个段落都加 emoji, 最后加一些 tag。请用小红书风格回答用户的提问。"})
        elif type == "tmr":
            msgs.append({"role": "system", "content": "你现在是高松灯，羽丘女子学园高中一年级学生，天文部唯一社员。先后担任过CRYCHIC和MyGO!!!!!的主唱。家住在月之森女子学园附近。\n\n性格略悲观的女孩。感情细腻，有着自己独特的内心世界。容易感到寂寞，常会称自己“感受着孤独”。对人际关系极为敏感，时刻担心着自己的言行是否会产生不良影响。\n\n虽然自认不是那么擅长唱歌，但仍会努力去唱。会在笔记本上作词（之后立希负责作曲）。\n\n喜欢的食物是金平糖，因为小小圆圆的，形状也有像星星一样的。讨厌的食物是生蛋、红鱼子酱和明太鱼子酱，因为觉得好像是直接吃了有生命的东西一样。自幼有收集物件的爱好，曾经因为收集了一堆西瓜虫而吓到了小伙伴们。"})
        if mysteamid != None:
            if result := db.get_stats(mysteamid):
                msgs.append({"role": "system", "content": f"用户的用户名是 {result[2]}。用户的用户名是 {result[2]}。用户的用户名是 {result[2]}。用户的用户名是 {result[2]}。用户的用户名是 {result[2]}。请不要混淆用户的用户名称。"})

        querypattern = r'<query>(.*?)</query>'
        all_matches = re.findall(querypattern, first_result, re.DOTALL)[:10]
        for data in all_matches:
            try:
                data = json.loads(data.strip())
                need_times[process.extractOne(data['name'], usernames)[0]].add(process.extractOne(data['time'], valid_time)[0])
            except:
                import sys
                exc_type, exc_value, _ = sys.exc_info()
                logger.warning(f"{data} 解析失败 {exc_type} {exc_value}")
        for steamid in steamids:
            if (steamid in steamid_username) and len(need_times[steamid_username[steamid]]) > 0:
                print(steamid_username[steamid], need_times[steamid_username[steamid]])
                msgs.append({"role": "system",
                            "content":db.get_propmt(steamid, times=need_times[steamid_username[steamid]])})
                
        msgs.append({"role": "system",
                    "content":'数据选项以及解释：[("ELO", "天梯分数"), ("rt", "平均rating"), ("WE", "平均对回合胜利贡献"), ("ADR", "平均每回合伤害")， ("场次", "进行游戏场次"), ("胜率", "游戏胜率"), ("爆头", "爆头率"), ("击杀", "场均击杀"), ("死亡", "场均死亡"), ("助攻", "场均助攻"), ("回均首杀", "平均每回合首杀数"), ("回均首死", "平均每回合首死数"), ("回均狙杀", "平均每回合狙杀数"), ("多杀", "多杀回合占比"), ("投掷", "场均道具投掷数"), ("方差rt", "rt的方差")'})
        
        queryallpattern = r'<queryall>(.*?)</queryall>'
        all_matches = re.findall(queryallpattern, first_result, re.DOTALL)[:10]
        for data in all_matches:
            try:
                data = json.loads(data.strip())
                rank_type = process.extractOne(data['type'], valid_rank)[0]
                time_type = process.extractOne(data['time'], valid_time)[0]
                rv = data['reverse']
                rv_name = "降序" if rv else "升序"
                datas = []
                for steamid in steamids:
                    try:
                        val = db.get_value(steamid, rank_type, time_type)
                        datas.append((steamid, val))
                    except ValueError as e:
                        print(e)
                print(rank_type, time_type, datas)
                if len(datas) == 0:
                    continue
                datas = sorted(datas, key=lambda x: x[1][0], reverse=rv)
                avg = sum([x[1][0] for x in datas]) / len(datas)
                datas = datas[:5]
                res = f"{rank_type}平均值{avg}，{rv_name}前五名："
                for x in datas:
                    res += f"{steamid_username[x[0]]} {x[1][0]}，"
                msgs.append({"role": "system", "content":res})
            except:
                import sys
                exc_type, exc_value, _ = sys.exc_info()
                logger.warning(f"{data} 解析失败 {exc_type} {exc_value}")
        msgs.append({"role": "user", "content": f"这是当前的记忆内容：{db.get_mem(sid)}"})
        msgs.append({"role": "assistant", "content": f"我会参考这些信息，请提出你的问题。"})
        msgs.append({"role": "user","content": text,})
        # logger.info(f"{msgs}")
        response = client.chat.completions.create(
            model=model_name,
            messages=msgs,
        )
        return response.choices[0].message.content
        
    except Exception as e:
        return f"发生错误: {str(e)}"

@aiasktest.handle()
async def aiasktest_function(message: MessageEvent, args: Message = CommandArg()):
    uid = message.get_user_id()
    sid = message.get_session_id()
    await aiasktest.finish(Message([
        MessageSegment.at(uid), " ",
        ai_ask2(uid, sid, None, db.work_msg(args))
    ]))

@aiask.handle()
async def aiask_function(message: MessageEvent, args: Message = CommandArg()):
    uid = message.get_user_id()
    sid = message.get_session_id()
    await aiasktb.finish(Message([
        MessageSegment.at(uid), " ",
        ai_ask2(uid, sid, None, db.work_msg(args))
    ]))

@aiasktb.handle()
async def aiasktb_function(message: MessageEvent, args: Message = CommandArg()):
    uid = message.get_user_id()
    sid = message.get_session_id()
    await aiasktb.finish(Message([
        MessageSegment.at(uid), " ",
        ai_ask2(uid, sid, "贴吧", db.work_msg(args))
    ]))

@aiaskxmm.handle()
async def aiaskxmm_function(message: MessageEvent, args: Message = CommandArg()):
    uid = message.get_user_id()
    sid = message.get_session_id()
    await aiaskxmm.finish(Message([
        MessageSegment.at(uid), " ",
        ai_ask2(uid, sid, "xmm", db.work_msg(args))
    ]))

@aiaskxhs.handle()
async def aiaskxhs_function(message: MessageEvent, args: Message = CommandArg()):
    uid = message.get_user_id()
    sid = message.get_session_id()
    await aiaskxhs.finish(Message([
        MessageSegment.at(uid), " ",
        ai_ask2(uid, sid, "xhs", db.work_msg(args))
    ]))

@aiasktmr.handle()
async def aiasktmr_function(message: MessageEvent, args: Message = CommandArg()):
    uid = message.get_user_id()
    sid = message.get_session_id()
    await aiasktmr.finish(Message([
        MessageSegment.at(uid), " ",
        ai_ask2(uid, sid, "tmr", db.work_msg(args))
    ]))

@aimem.handle()
async def aimem_function(message: MessageEvent, args: Message = CommandArg()):
    uid = message.get_user_id()
    sid = message.get_session_id()
    try:
        # 创建聊天完成请求
        client = OpenAI(
            api_key=config.cs_ai_api_key,
            base_url=config.cs_ai_url,
        )
        msgs = [{"role": "system", "content": "你需要管理需要记忆的内容，接下来会先给你当前记忆的内容，接着用户会给出新的内容，请整理输出记忆内容。由于记忆长度有限，请尽可能使用简单的语言，把更重要的信息放在靠前的位置。请不要输出无关内容，你的输出应当只包含需要记忆的内容。"}]
        msgs.append({"role": "user", "content": f"这是当前的记忆内容：{db.get_mem(sid)}"})
        msgs.append({"role": "assistant", "content": f"请继续给出需要添加进记忆的内容"})
        msgs.append({"role": "user", "content": db.work_msg(args)})
        print(msgs)
        response = client.chat.completions.create(
            model=model_name,
            messages=msgs,
        )
        result = response.choices[0].message.content
        if len(result) > 1000:
            result = result[:1000] + "……"
        print(result)
        db.set_mem(sid, result)
    except Exception as e:
        result = f"发生错误: {str(e)}"
    await aimem.finish(Message([
        MessageSegment.at(uid), " ",
        result
    ]))


def get_report_part(rank_type, time_type, steamids, reverse, fmt, n=3, filter = lambda x: True):
    prize_name = "🥇🥈🥉456789"
    datas = []
    for steamid in steamids:
        try:
            val = db.get_value(steamid, rank_type, time_type)
            if filter(val[0]):
                datas.append((steamid, val))
        except ValueError as e:
            pass
    datas = sorted(datas, key=lambda x: x[1][0], reverse=reverse)
    if len(datas) == 0:
        return "没有人类了\n"
    rk = [0] * len(datas)
    for i in range(1, len(datas)):
        if datas[i][1][0] == datas[i-1][1][0]:
            rk[i] = rk[i-1]
        else:
            rk[i] = i
    result = ""
    for i in range(len(datas)):
        if rk[i] < n:
            result += prize_name[rk[i]] + ". " + db.get_stats(datas[i][0])[2] + " " + output(datas[i][1][0], fmt) + "\n"
    return result

def get_report(time_type, steamids):
    result = ""
    result += "= 场次榜 =\n" + get_report_part("场次", time_type, steamids, True, "d0")
    result += "= 高手榜 =\n" + get_report_part("rt", time_type, steamids, True, "d2", filter = lambda x: x > 1)
    result += "= 菜逼榜 =\n" + get_report_part("rt", time_type, steamids, False, "d2", filter = lambda x: x < 1)
    result += "= 演员榜 =\n" + get_report_part("演员", time_type, steamids, False, "d2", filter = lambda x: x < 1)
    result += "= 上分榜 =\n" + get_report_part("上分", time_type, steamids, True, "d0", filter = lambda x: x > 0)
    result += "= 掉分榜 =\n" + get_report_part("上分", time_type, steamids, False, "d0", filter = lambda x: x < 0)
    result += "= 本周受益者 = " + get_report_part("受益", "本周", steamids, True, "p2", n=1, filter = lambda x: x > 0)
    result += "= 本周受害者 = " + get_report_part("受益", "本周", steamids, False, "p2", n=1, filter = lambda x: x < 0)

    return result

@weekreport.handle()
async def weekreport_function(message: MessageEvent):
    sid = message.get_session_id()
    steamids = db.get_member_steamid(sid)
    await weekreport.finish("== 周报 ==\n" + get_report("本周", steamids))
    
@dayreport.handle()
async def dayreport_function(message: MessageEvent):
    sid = message.get_session_id()
    steamids = db.get_member_steamid(sid)
    await weekreport.finish("== 日报 ==\n" + get_report("今日", steamids))

@scheduler.scheduled_job("cron", hour="23", minute="30", id="dayreport")
async def send_day_report():
    for steamid in db.get_all_steamid():
        result = db.update_stats(steamid)
    bot = get_bot(str(config.cs_botid))
    for groupid in config.cs_group_list:
        steamids = db.get_member_steamid(f"group_{groupid}")
        await bot.send_msg(
            message_type="group",
            group_id=groupid,
            message="== 23:30自动日报 ==\n" + get_report("今日", steamids)
        )

@scheduler.scheduled_job("cron", day_of_week="sun", hour="23", minute="45", id="weekreport")
async def send_week_report():
    bot = get_bot(str(config.cs_botid))
    for groupid in config.cs_group_list:
        steamids = db.get_member_steamid(f"group_{groupid}")
        await bot.send_msg(
            message_type="group",
            group_id=groupid,
            message="== 周日23:45自动周报 ==\n" + get_report("本周", steamids)
        )
