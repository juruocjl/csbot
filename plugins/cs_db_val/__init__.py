from nonebot import get_plugin_config
from nonebot.plugin import PluginMetadata
from nonebot import require
from nonebot import logger

get_cursor = require("utils").get_cursor
get_today_start_timestamp = require("utils").get_today_start_timestamp

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import List
import time

from .config import Config

__plugin_meta__ = PluginMetadata(
    name="cs_db_val",
    description="",
    usage="",
    config=Config,
)

config = get_plugin_config(Config)

SeasonId = config.cs_season_id
lastSeasonId = config.cs_last_season_id

class DataManager:
    def get_steamid(self, uid):
        cursor = get_cursor()
        cursor.execute('''
        SELECT steamid FROM members_steamid WHERE uid = ?
        ''', (uid,))
        result = cursor.fetchone()
        return result[0] if result else None

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
        if query_type == "gprt":
            cursor.execute(f'''SELECT AVG(rating) as avgRating, COUNT(mid) as cnt
                                FROM 'matches_gp'
                                WHERE 
                                {time_sql} and {steamid_sql}
                            ''')
            result = cursor.fetchone()
            if result[1] > 0:
                return result
            raise ValueError(f"no {query_type}")
        if query_type == "gp场次":
            cursor.execute(f'''SELECT COUNT(*) as cnt
                                FROM 'matches_gp'
                                WHERE 
                                {time_sql} and {steamid_sql}
                            ''')
            result = cursor.fetchone()
            if result[0] > 0:
                return result
            raise ValueError(f"no {query_type}")
        if query_type == "gp回均首杀":
            cursor.execute(f'''SELECT SUM(entryKill) as totEK, SUM(score1 + score2) as totR, COUNT(mid) as cnt FROM 'matches_gp'
                            WHERE 
                            {time_sql} and {steamid_sql}
                            ''')
            result = cursor.fetchone()
            if result[2] > 0:
                return (result[0] / result[1], result[2])
            raise ValueError(f"no {query_type}")
        if query_type == "gp回均首死":
            cursor.execute(f'''SELECT SUM(entryDeath) as totFD, SUM(score1 + score2) as totR, COUNT(mid) as cnt FROM 'matches_gp'
                            WHERE 
                            {time_sql} and {steamid_sql}
                            ''')
            result = cursor.fetchone()
            if result[2] > 0:
                return (result[0] / result[1], result[2])
            raise ValueError(f"no {query_type}")
        if query_type == "gp回均狙杀":
            cursor.execute(f'''SELECT SUM(awpKill) as totEK, SUM(score1 + score2) as totR, COUNT(mid) as cnt FROM 'matches_gp'
                            WHERE 
                            {time_sql} and {steamid_sql}
                            ''')
            result = cursor.fetchone()
            if result[2] > 0:
                return (result[0] / result[1], result[2])
            raise ValueError(f"no {query_type}")
        if query_type == "gp白给":
            cursor.execute(f'''SELECT SUM(entryKill - entryDeath) as totEKD, SUM(score1 + score2) as totR, COUNT(mid) as cnt  FROM 'matches_gp'
                            WHERE 
                            {time_sql} and {steamid_sql}
                            ''')
            result = cursor.fetchone()
            if result[2] > 0:
                return (result[0] / result[1], result[2])
            raise ValueError(f"no {query_type}")
        if query_type == "gp击杀":
            cursor.execute(f'''SELECT AVG(kill) as avgkill, COUNT(mid) as cnt FROM 'matches_gp'
                                WHERE 
                                {time_sql} and {steamid_sql}
                            ''')
            result = cursor.fetchone()
            if result[1] > 0:
                return result
            raise ValueError(f"no {query_type}")
        if query_type == "gp死亡":
            cursor.execute(f'''SELECT AVG(death) as avgdeath, COUNT(mid) as cnt FROM 'matches_gp'
                                WHERE 
                                {time_sql} and {steamid_sql}
                            ''')
            result = cursor.fetchone()
            if result[1] > 0:
                return result
            raise ValueError(f"no {query_type}")
        if query_type == "gp助攻":
            cursor.execute(f'''SELECT AVG(assist) as avgassist, COUNT(mid) as cnt FROM 'matches_gp'
                                WHERE 
                                {time_sql} and {steamid_sql}
                            ''')
            result = cursor.fetchone()
            if result[1] > 0:
                return result
            raise ValueError(f"no {query_type}")
        if query_type == "gp尽力":
            cursor.execute(f'''SELECT AVG(pwRating) as avgRating, COUNT(mid) as cnt FROM 'matches_gp'
                                WHERE 
                                winTeam != team  
                                and {time_sql} and {steamid_sql}
                            ''')
            result = cursor.fetchone()
            if result[1] > 0:
                return result
            raise ValueError(f"no {query_type}")
        if query_type == "gp带飞":
            cursor.execute(f'''SELECT AVG(pwRating) as avgRating, COUNT(mid) as cnt FROM 'matches_gp'
                                WHERE 
                                winTeam == team  
                                and {time_sql} and {steamid_sql}
                            ''')
            result = cursor.fetchone()
            if result[1] > 0:
                return result
            raise ValueError(f"no {query_type}")
        if query_type == "gp炸鱼":
            cursor.execute(f'''SELECT AVG(pwRating) as avgRating, COUNT(mid) as cnt FROM 'matches_gp'
                                WHERE 
                                winTeam == team  
                                and min(score1, score2) <= 6
                                and {time_sql} and {steamid_sql}
                            ''')
            result = cursor.fetchone()
            if result[1] > 0:
                return result
            raise ValueError(f"no {query_type}")
        if query_type == "皮蛋":
            cursor.execute(f'''SELECT AVG(bombPlanted) as avgBombPlanted, COUNT(mid) as cnt FROM 'matches_gp'
                                WHERE 
                                {time_sql} and {steamid_sql}
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


class RangeGen(ABC):
    @abstractmethod
    def getval(self, a: int, b: int) -> tuple[int, int]:
        pass

class MinAdd(RangeGen):
    def __init__(self, val):
        self.val = val
    def getval(self, minvalue, maxvalue):
        return minvalue + self.val, maxvalue
class Fix(RangeGen):
    def __init__(self, val):
        self.val = val
    def getval(self, minvalue, maxvalue):
        return self.val, maxvalue
class ZeroIn(RangeGen):
    def __init__(self, val):
        self.val = val
    def getval(self, minvalue, maxvalue):
        minvalue = min(0, minvalue)
        maxvalue = max(0, maxvalue)
        if minvalue == maxvalue:
            minvalue = self.val
        return minvalue, maxvalue


valid_time = ["今日", "昨日", "本周", "本赛季", "两赛季", "上赛季", "全部"]
gp_time = ["今日", "昨日", "本周", "全部"]

@dataclass
class RankConfig:
    name: str
    title: str
    default_time: str
    allowed_time: List[str]
    reversed: bool
    range_gen: RangeGen
    outputfmt: str
    template: int
    
    def __post_init__(self):
        if self.allowed_time is None:
            self.allowed_time = [self.default_time]

rank_config = [
    RankConfig("ELO", "天梯分数", "本赛季", None, True, MinAdd(-10), "d0", 1),
    RankConfig("rt", "rating", "本赛季", valid_time, True, MinAdd(-0.05), "d2", 1),
    RankConfig("WE", "WE", "本赛季", valid_time, True, MinAdd(-1), "d2", 1, ),
    RankConfig("ADR", "ADR", "本赛季", valid_time, True, MinAdd(-10), "d2", 1),
    RankConfig("场次", "场次", "本赛季", valid_time, True, Fix(0), "d0", 1),
    RankConfig("胜率", "胜率", "本赛季", valid_time, True, Fix(0), "p2", 1),
    RankConfig("首杀", "首杀率", "本赛季", None, True, Fix(0), "p0", 1),
    RankConfig("爆头", "爆头率", "本赛季", valid_time, True, Fix(0), "p0", 1),
    RankConfig("1v1", "1v1胜率", "本赛季", None, True, Fix(0), "p0", 1),
    RankConfig("击杀", "场均击杀", "本赛季", valid_time, True, MinAdd(-0.1), "d2", 1),
    RankConfig("死亡", "场均死亡", "本赛季", valid_time, True, MinAdd(-0.1), "d2", 1),
    RankConfig("助攻", "场均助攻", "本赛季", valid_time, True, MinAdd(-0.1), "d2", 1),
    RankConfig("尽力", "未胜利平均rt", "两赛季", valid_time, True, MinAdd(-0.05), "d2", 1),
    RankConfig("带飞", "胜利平均rt", "两赛季", valid_time, True, MinAdd(-0.05), "d2", 1),
    RankConfig("炸鱼", "小分平均rt", "两赛季", valid_time, True, MinAdd(-0.05), "d2", 1),
    RankConfig("演员", "组排平均rt", "两赛季", valid_time, False, MinAdd(-0.05), "d2", 1),
    RankConfig("鼓励", "单排场次", "两赛季", valid_time, True, Fix(0), "d0", 1),
    RankConfig("悲情", ">1.2rt未胜利场次", "两赛季", valid_time, True, Fix(0), "d0", 1),
    RankConfig("内战", "pvp自定义平均rt", "两赛季", valid_time, True, MinAdd(-0.05), "d2", 1),
    RankConfig("上分", "上分", "本周", valid_time, True, ZeroIn(-1), "d0", 2),
    RankConfig("回均首杀", "平均每回合首杀", "本赛季", valid_time, True, MinAdd(-0.01), "d2", 1),
    RankConfig("回均首死", "平均每回合首死", "本赛季", valid_time, True, MinAdd(-0.01), "d2", 1),
    RankConfig("回均狙杀", "平均每回合狙杀", "本赛季", valid_time, True, MinAdd(-0.01), "d2", 1),
    RankConfig("多杀", "多杀回合占比", "本赛季", valid_time, True, MinAdd(-0.01), "p0", 1),
    RankConfig("内鬼", "场均闪白队友", "本赛季", valid_time, True, MinAdd(-0.5), "d1", 1),
    RankConfig("投掷", "场均道具投掷数", "本赛季", valid_time, True, MinAdd(-0.5), "d1", 1),
    RankConfig("闪白", "场均闪白数", "本赛季", valid_time, True, MinAdd(-0.5), "d1", 1),
    RankConfig("白给", "平均每回合首杀-首死", "本赛季", valid_time, False, ZeroIn(-0.01), "d2", 2),
    RankConfig("方差rt", "rt方差", "两赛季", valid_time, True, Fix(0) , "d2", 1),
    RankConfig("方差ADR", "ADR方差", "两赛季", valid_time, True, Fix(0) , "d0", 1),
    RankConfig("受益", "胜率-期望胜率", "两赛季", valid_time, True, ZeroIn(-0.01), "p0", 2),

    RankConfig("gprt", "官匹rating", "全部", gp_time, True, ZeroIn(-0.01), "d2", 2),
    RankConfig("gp场次", "官匹场次", "全部", gp_time, True, Fix(0), "d0", 1),
    RankConfig("gp回均首杀", "官匹平均每回合首杀", "全部", gp_time, True, MinAdd(-0.01), "d2", 1),
    RankConfig("gp回均首死", "官匹平均每回合首死", "全部", gp_time, True, MinAdd(-0.01), "d2", 1),
    RankConfig("gp回均狙杀", "官匹平均每回合狙杀", "全部", gp_time, True, MinAdd(-0.01), "d2", 1),
    RankConfig("gp白给", "官匹平均每回合首杀-首死", "全部", gp_time, False, ZeroIn(-0.01), "d2", 2),
    RankConfig("gp击杀", "官匹场均击杀", "全部", gp_time, True, MinAdd(-0.1), "d2", 1),
    RankConfig("gp死亡", "官匹场均死亡", "全部", gp_time, True, MinAdd(-0.1), "d2", 1),
    RankConfig("gp助攻", "官匹场均助攻", "全部", gp_time, True, MinAdd(-0.1), "d2", 1),
    RankConfig("gp尽力", "官匹未胜利平均rt", "全部", gp_time, True, MinAdd(-0.05), "d2", 1),
    RankConfig("gp带飞", "官匹胜利平均rt", "全部", gp_time, True, MinAdd(-0.05), "d2", 1),
    RankConfig("gp炸鱼", "官匹小分平均rt", "全部", gp_time, True, MinAdd(-0.05), "d2", 1),
    RankConfig("皮蛋", "场均下包数", "全部", gp_time, True, Fix(0), "d2", 1),
]

valid_rank = [a.name for a in rank_config]
