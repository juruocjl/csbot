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
    name="cs_db",
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

db = DataManager()