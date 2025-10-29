from nonebot import get_plugin_config
from nonebot.plugin import PluginMetadata
from nonebot import require
from nonebot import logger

get_cursor = require("utils").get_cursor
get_today_start_timestamp = require("utils").get_today_start_timestamp

import requests
import urllib
from pathlib import Path
import time

from .config import Config

__plugin_meta__ = PluginMetadata(
    name="cs_db_upd",
    description="",
    usage="",
    config=Config,
)

config = get_plugin_config(Config)

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
    
    def add_member(self, gid, uid):
        if gid.startswith("group_"):
            gid = gid.split("_")[1]
            cursor = get_cursor()
            cursor.execute(
                'INSERT OR IGNORE INTO group_members (gid, uid) VALUES (?, ?)',
                (gid, uid)
            )
  
db = DataManager()