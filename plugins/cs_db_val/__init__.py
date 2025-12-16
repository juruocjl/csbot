from nonebot import get_plugin_config
from nonebot.plugin import PluginMetadata
from nonebot import require
from nonebot import logger

require("cs_db_upd")
from ..cs_db_upd import GroupMember, MemberSteamID

require("utils")

from ..utils import async_session_factory
from ..utils import get_today_start_timestamp
from ..utils import get_cursor

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import List, Tuple, Dict, Callable, Awaitable
import time
from sqlalchemy import select

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


AsyncFloatFunc = Callable[[str, str], Awaitable[Tuple[float, int]]]

@dataclass
class RankConfig:
    title: str
    default_time: str
    allowed_time: List[str]
    reversed: bool
    range_gen: RangeGen
    outputfmt: str
    template: int
    func: AsyncFloatFunc

valid_rank: List[str] = []


def get_time_sql(time_type):
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


class DataManager:
    def __init__(self):
        self._registry: Dict[str, RankConfig] = {}

    def register(self, name: str, title: str, default_time: str, allowed_time: List[str], reversed: bool, range_gen: RangeGen, outputfmt: str, template: int):
        if allowed_time is None:
            allowed_time = [default_time]
        
        valid_rank.append(name)

        def decorator(func: Callable):
            self._registry[name] = RankConfig(title, default_time, allowed_time, reversed, range_gen, outputfmt, template, func)
            return func
        return decorator

    async def get_steamid(self, uid: str) -> str | None:
        async with async_session_factory() as session:
            record = await session.get(MemberSteamID, uid)
            
            return record.steamid if record else None

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

    async def get_all_steamid(self) -> list[str]:
        """
        获取所有绑定的 SteamID
        """
        async with async_session_factory() as session:
            stmt = select(MemberSteamID.steamid)
            result = await session.execute(stmt)
            return list(result.scalars().all())
    
    async def get_member(self, sid: str) -> list[str]:
        """
        获取群成员列表
        """
        if sid.startswith("group_"):
            gid = sid.split("_")[1]

            async with async_session_factory() as session:
                stmt = select(GroupMember.uid).where(GroupMember.gid == gid)
                
                result = await session.execute(stmt)
                
                return list(result.scalars().all())
        
        return []
    
    async def get_member_steamid(self, sid):
        uids = await self.get_member(sid)
        steamids = set()
        for uid in uids:
            if steamid := await self.get_steamid(uid):
                steamids.add(steamid)
        return list(steamids)

    def get_value_config(self, query_type: str) -> RankConfig:
        if query_type not in self._registry:
            raise ValueError(f"无效的查询类型，支持的有 {list(self._registry.keys())}")
        return self._registry[query_type]


    def get_all_value(self, steamid, time_type):
        time_sql = get_time_sql(time_type)
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
        
    async def get_propmt(self, steamid, times = ['本赛季']):
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
                var = await (self.get_value_config("方差rt").func(steamid, time_type))
                prompt += f"{time_type}rating方差 {var :+.2f}，"
            except ValueError as e:
                pass
        return prompt

    def get_matches(self, steamid, time_type, LIMIT = 20):
        cursor = get_cursor()
        cursor.execute(f'''SELECT * FROM 'matches'
                            WHERE 
                            {get_time_sql(time_type)} and steamid == ?
                            ORDER BY timeStamp DESC
                            LIMIT ?
                        ''', (steamid, LIMIT, ))
        result = cursor.fetchall()
        if len(result):
            return result
            # return await gen_matches_html(result, steamid, self.get_stats(steamid)[2])
        else:
            return None
         
    async def get_username(self, uid):
        if steamid := await self.get_steamid(uid):
            if result := self.get_stats(steamid):
                return result[2]
        return None

    async def work_msg(self, msg):
        result = ""
        for seg in msg:
            if seg.type == "text":
                result += seg.data['text']
            elif seg.type == "at":
                if name := await self.get_username(seg.data['qq']):
                    result += name
                else:
                    result += "<未找到用户>"
        return result.strip()

db = DataManager()



valid_time = ["今日", "昨日", "本周", "本赛季", "两赛季", "上赛季", "全部"]
gp_time = ["今日", "昨日", "本周", "全部"]


class NoValueError(Exception):
    pass

@db.register("ELO", "天梯分数", "本赛季", None, True, MinAdd(-10), "d0", 1)
async def get_elo(steamid: str, time_type: str) -> Tuple[float, int]:
    assert(time_type == "本赛季")
    result = db.get_stats(steamid)
    if result and result[18] == SeasonId and result[3] != 0:
        return result[3], result[4]
    raise NoValueError()

@db.register("rt", "rating", "本赛季", valid_time, True, MinAdd(-0.05), "d2", 1)
async def get_rt(steamid: str, time_type: str) -> Tuple[float, int]:
    time_sql = get_time_sql(time_type)
    steamid_sql = f"steamid == '{steamid}'"
    cursor = get_cursor()
    cursor.execute(f'''SELECT AVG(pwRating) as avgRating, COUNT(mid) as cnt
                        FROM 'matches'
                        WHERE 
                        (mode LIKE "天梯%" or mode == "PVP周末联赛")
                        and {time_sql} and {steamid_sql}
                    ''')
    result = cursor.fetchone()
    if result[1] > 0:
        return result
    raise NoValueError()

@db.register("WE", "WE", "本赛季", valid_time, True, MinAdd(-1), "d2", 1, )
async def get_we(steamid: str, time_type: str) -> Tuple[float, int]:
    time_sql = get_time_sql(time_type)
    steamid_sql = f"steamid == '{steamid}'"
    cursor = get_cursor()
    cursor.execute(f'''SELECT AVG(we) as avgwe, COUNT(mid) as cnt
                        FROM 'matches'
                        WHERE 
                        (mode LIKE "天梯%" or mode == "PVP周末联赛")
                        and {time_sql} and {steamid_sql}
                    ''')
    result = cursor.fetchone()
    if result[1] > 0:
        return result
    raise NoValueError()

@db.register("ADR", "ADR", "本赛季", valid_time, True, MinAdd(-10), "d2", 1)
async def get_adr(steamid: str, time_type: str) -> Tuple[float, int]:
    time_sql = get_time_sql(time_type)
    steamid_sql = f"steamid == '{steamid}'"
    cursor = get_cursor()
    cursor.execute(f'''SELECT AVG(adpr) as avgADR, COUNT(mid) as cnt
                        FROM 'matches'
                        WHERE 
                        (mode LIKE "天梯%" or mode == "PVP周末联赛")
                        and {time_sql} and {steamid_sql}
                    ''')
    result = cursor.fetchone()
    if result[1] > 0:
        return result
    raise NoValueError()

@db.register("场次", "场次", "本赛季", valid_time, True, Fix(0), "d0", 1)
async def get_matches_cnt(steamid: str, time_type: str) -> Tuple[float, int]:
    time_sql = get_time_sql(time_type)
    steamid_sql = f"steamid == '{steamid}'"
    cursor = get_cursor()
    cursor.execute(f'''SELECT COUNT(mid) as cnt
                        FROM 'matches'
                        WHERE 
                        (mode LIKE "天梯%" or mode == "PVP周末联赛")
                        and {time_sql} and {steamid_sql}
                    ''')
    result = cursor.fetchone()
    if result[0] > 0:
        return result[0], result[0]
    raise NoValueError()

@db.register("胜率", "胜率", "本赛季", valid_time, True, Fix(0), "p2", 1)
async def get_winrate(steamid: str, time_type: str) -> Tuple[float, int]:
    time_sql = get_time_sql(time_type)
    steamid_sql = f"steamid == '{steamid}'"
    cursor = get_cursor()
    cursor.execute(f'''SELECT AVG(winTeam == team) as wr, COUNT(mid) as cnt
                        FROM 'matches'
                        WHERE 
                        (mode LIKE "天梯%" or mode == "PVP周末联赛")
                        and {time_sql} and {steamid_sql}
                    ''')
    result = cursor.fetchone()
    if result[1] > 0:
        return result
    raise NoValueError()

@db.register("首杀", "首杀率", "本赛季", None, True, Fix(0), "p0", 1)
async def get_ekrate(steamid: str, time_type: str) -> Tuple[float, int]:
    time_sql = get_time_sql(time_type)
    steamid_sql = f"steamid == '{steamid}'"
    cursor = get_cursor()
    assert(time_type == "本赛季")
    result = db.get_stats(steamid)
    if result and result[18] == SeasonId and result[4] != 0:
        return result[15], result[4]
    raise NoValueError()
    
@db.register("爆头", "爆头率", "本赛季", valid_time, True, Fix(0), "p0", 1)
async def get_hsrate(steamid: str, time_type: str) -> Tuple[float, int]:
    time_sql = get_time_sql(time_type)
    steamid_sql = f"steamid == '{steamid}'"
    cursor = get_cursor()
    cursor.execute(f'''SELECT SUM(headShot) as totHS, SUM(kill) as totK, COUNT(mid) as cnt
                    FROM 'matches'
                    WHERE 
                    (mode LIKE "天梯%" or mode == "PVP周末联赛")
                    and {time_sql} and {steamid_sql}
                    ''')
    result = cursor.fetchone()
    if result[2] > 0 and result[1] > 0:
        return (result[0] / result[1], result[2])
    raise NoValueError()

@db.register("1v1", "1v1胜率", "本赛季", None, True, Fix(0), "p0", 1)
async def get_1v1wr(steamid: str, time_type: str) -> Tuple[float, int]:
    time_sql = get_time_sql(time_type)
    steamid_sql = f"steamid == '{steamid}'"
    cursor = get_cursor()
    assert(time_type == "本赛季")
    result = db.get_stats(steamid)
    if result and result[18] == SeasonId and result[4] != 0:
        return result[16], result[4]
    raise NoValueError()

@db.register("击杀", "场均击杀", "本赛季", valid_time, True, MinAdd(-0.1), "d2", 1)
async def get_kills(steamid: str, time_type: str) -> Tuple[float, int]:
    time_sql = get_time_sql(time_type)
    steamid_sql = f"steamid == '{steamid}'"
    cursor = get_cursor()
    cursor.execute(f'''SELECT AVG(kill) as avgkill, COUNT(mid) as cnt
                        FROM 'matches'
                        WHERE 
                        (mode LIKE "天梯%" or mode == "PVP周末联赛")
                        and {time_sql} and {steamid_sql}
                    ''')
    result = cursor.fetchone()
    if result[1] > 0:
        return result
    raise NoValueError()

@db.register("死亡", "场均死亡", "本赛季", valid_time, True, MinAdd(-0.1), "d2", 1)
async def get_deaths(steamid: str, time_type: str) -> Tuple[float, int]:
    time_sql = get_time_sql(time_type)
    steamid_sql = f"steamid == '{steamid}'"
    cursor = get_cursor()
    cursor.execute(f'''SELECT AVG(death) as avgdeath, COUNT(mid) as cnt
                        FROM 'matches'
                        WHERE 
                        (mode LIKE "天梯%" or mode == "PVP周末联赛")
                        and {time_sql} and {steamid_sql}
                    ''')
    result = cursor.fetchone()
    if result[1] > 0:
        return result
    raise NoValueError()

@db.register("助攻", "场均助攻", "本赛季", valid_time, True, MinAdd(-0.1), "d2", 1)
async def get_assists(steamid: str, time_type: str) -> Tuple[float, int]:
    time_sql = get_time_sql(time_type)
    steamid_sql = f"steamid == '{steamid}'"
    cursor = get_cursor()
    cursor.execute(f'''SELECT AVG(assist) as avgassist, COUNT(mid) as cnt
                        FROM 'matches'
                        WHERE 
                        (mode LIKE "天梯%" or mode == "PVP周末联赛")
                        and {time_sql} and {steamid_sql}
                    ''')
    result = cursor.fetchone()
    if result[1] > 0:
        return result
    raise NoValueError()

@db.register("尽力", "未胜利平均rt", "两赛季", valid_time, True, MinAdd(-0.05), "d2", 1)
async def get_tryhard(steamid: str, time_type: str) -> Tuple[float, int]:
    time_sql = get_time_sql(time_type)
    steamid_sql = f"steamid == '{steamid}'"
    cursor = get_cursor()
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
    raise NoValueError()

@db.register("带飞", "胜利平均rt", "两赛季", valid_time, True, MinAdd(-0.05), "d2", 1)
async def get_carry(steamid: str, time_type: str) -> Tuple[float, int]:
    time_sql = get_time_sql(time_type)
    steamid_sql = f"steamid == '{steamid}'"
    cursor = get_cursor()
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
    raise NoValueError()

@db.register("炸鱼", "小分平均rt", "两赛季", valid_time, True, MinAdd(-0.05), "d2", 1)
async def get_fish(steamid: str, time_type: str) -> Tuple[float, int]:
    time_sql = get_time_sql(time_type)
    steamid_sql = f"steamid == '{steamid}'"
    cursor = get_cursor()
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
    raise NoValueError()

@db.register("演员", "组排平均rt", "两赛季", valid_time, False, MinAdd(-0.05), "d2", 1)
async def get_duoqi(steamid: str, time_type: str) -> Tuple[float, int]:
    time_sql = get_time_sql(time_type)
    steamid_sql = f"steamid == '{steamid}'"
    cursor = get_cursor()
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
    raise NoValueError()

@db.register("鼓励", "单排场次", "两赛季", valid_time, True, Fix(0), "d0", 1)
async def get_solo_cnt(steamid: str, time_type: str) -> Tuple[float, int]:
    time_sql = get_time_sql(time_type)
    steamid_sql = f"steamid == '{steamid}'"
    cursor = get_cursor()
    cursor.execute(f'''SELECT COUNT(mid) AS total_count
                        FROM 'matches'
                        WHERE 
                        (mode LIKE "天梯%" or mode == "PVP周末联赛")
                        and isgroup == 0
                        and {time_sql} and {steamid_sql}
                    ''')
    result = cursor.fetchone()
    if result[0] > 0:
        return result[0], result[0]
    raise NoValueError()

@db.register("悲情", ">1.2rt未胜利场次", "两赛季", valid_time, True, Fix(0), "d0", 1)
async def get_sad_cnt(steamid: str, time_type: str) -> Tuple[float, int]:
    time_sql = get_time_sql(time_type)
    steamid_sql = f"steamid == '{steamid}'"
    cursor = get_cursor()
    cursor.execute(f'''SELECT COUNT(mid) AS total_count
                        FROM 'matches'
                        WHERE 
                        (mode LIKE "天梯%" or mode == "PVP周末联赛")
                        and pwRating > 1.2
                        and winTeam != team
                        and {time_sql} and {steamid_sql}
                    ''')
    result = cursor.fetchone()
    if result[0] > 0:
        return result[0], result[0]
    raise NoValueError()

@db.register("内战", "pvp自定义平均rt", "两赛季", valid_time, True, MinAdd(-0.05), "d2", 1)
async def get_pvp_rt(steamid: str, time_type: str) -> Tuple[float, int]:
    time_sql = get_time_sql(time_type)
    steamid_sql = f"steamid == '{steamid}'"
    cursor = get_cursor()
    cursor.execute(f'''SELECT AVG(pwRating) as avgRating, COUNT(mid) as cnt
                        FROM 'matches'
                        WHERE 
                        mode == "PVP自定义"
                        and {time_sql} and {steamid_sql}
                    ''')
    result = cursor.fetchone()
    if result[1] > 0:
        return result
    raise NoValueError()

@db.register("内战胜率", "pvp自定义胜率", "两赛季", valid_time, True, Fix(0), "p2", 1)
async def get_pvp_wr(steamid: str, time_type: str) -> Tuple[float, int]:
    time_sql = get_time_sql(time_type)
    steamid_sql = f"steamid == '{steamid}'"
    cursor = get_cursor()
    cursor.execute(f'''SELECT AVG(winTeam == team) as wr, COUNT(mid) as cnt
                        FROM 'matches'
                        WHERE 
                        mode == "PVP自定义"
                        and {time_sql} and {steamid_sql}
                    ''')
    result = cursor.fetchone()
    if result[1] > 0:
        return result
    raise NoValueError()

@db.register("上分", "上分", "本周", valid_time, True, ZeroIn(-1), "d0", 2)
async def get_upscore(steamid: str, time_type: str) -> Tuple[float, int]:
    time_sql = get_time_sql(time_type)
    steamid_sql = f"steamid == '{steamid}'"
    cursor = get_cursor()
    cursor.execute(f'''SELECT SUM(pvpScoreChange) as ScoreDelta, COUNT(mid) as cnt
                        FROM 'matches'
                        WHERE 
                        (mode LIKE "天梯%" or mode == "PVP周末联赛")
                        and {time_sql} and {steamid_sql}
                    ''')
    result = cursor.fetchone()
    if result[1] > 0:
        return result
    raise NoValueError()

@db.register("回均首杀", "平均每回合首杀", "本赛季", valid_time, True, MinAdd(-0.01), "d2", 1)
async def get_rpek(steamid: str, time_type: str) -> Tuple[float, int]:
    time_sql = get_time_sql(time_type)
    steamid_sql = f"steamid == '{steamid}'"
    cursor = get_cursor()
    cursor.execute(f'''SELECT SUM(entryKill) as totEK, SUM(score1 + score2) as totR, COUNT(mid) as cnt FROM 'matches'
                    WHERE 
                    (mode LIKE "天梯%" or mode == "PVP周末联赛")
                    and {time_sql} and {steamid_sql}
                    ''')
    result = cursor.fetchone()
    if result[2] > 0:
        return (result[0] / result[1], result[2])
    raise NoValueError()

@db.register("回均首死", "平均每回合首死", "本赛季", valid_time, True, MinAdd(-0.01), "d2", 1)
async def get_rpfd(steamid: str, time_type: str) -> Tuple[float, int]:
    time_sql = get_time_sql(time_type)
    steamid_sql = f"steamid == '{steamid}'"
    cursor = get_cursor()
    cursor.execute(f'''SELECT SUM(firstDeath) as totFD, SUM(score1 + score2) as totR, COUNT(mid) as cnt FROM 'matches'
                    WHERE 
                    (mode LIKE "天梯%" or mode == "PVP周末联赛")
                    and {time_sql} and {steamid_sql}
                    ''')
    result = cursor.fetchone()
    if result[2] > 0:
        return (result[0] / result[1], result[2])
    raise NoValueError()

@db.register("回均狙杀", "平均每回合狙杀", "本赛季", valid_time, True, MinAdd(-0.01), "d2", 1)
async def get_rpsn(steamid: str, time_type: str) -> Tuple[float, int]:
    time_sql = get_time_sql(time_type)
    steamid_sql = f"steamid == '{steamid}'"
    cursor = get_cursor()
    cursor.execute(f'''SELECT SUM(snipeNum) as totSK, SUM(score1 + score2) as totR, COUNT(mid) as cnt FROM 'matches'
                    WHERE 
                    (mode LIKE "天梯%" or mode == "PVP周末联赛")
                    and {time_sql} and {steamid_sql}
                    ''')
    result = cursor.fetchone()
    if result[2] > 0:
        return (result[0] / result[1], result[2])
    raise NoValueError()

@db.register("多杀", "多杀回合占比", "本赛季", valid_time, True, MinAdd(-0.01), "p0", 1)
async def get_rpmk(steamid: str, time_type: str) -> Tuple[float, int]:
    time_sql = get_time_sql(time_type)
    steamid_sql = f"steamid == '{steamid}'"
    cursor = get_cursor()
    cursor.execute(f'''SELECT SUM(twoKill + threeKill + fourKill + fiveKill) as totMK, SUM(score1 + score2) as totR, COUNT(mid) as cnt FROM 'matches'
                    WHERE 
                    (mode LIKE "天梯%" or mode == "PVP周末联赛")
                    and {time_sql} and {steamid_sql}
                    ''')
    result = cursor.fetchone()
    if result[2] > 0:
        return (result[0] / result[1], result[2])
    raise NoValueError()

@db.register("内鬼", "场均闪白队友", "本赛季", valid_time, True, MinAdd(-0.5), "d1", 1)
async def get_rpft(steamid: str, time_type: str) -> Tuple[float, int]:
    time_sql = get_time_sql(time_type)
    steamid_sql = f"steamid == '{steamid}'"
    cursor = get_cursor()
    cursor.execute(f'''SELECT AVG(flashTeammate) as avgFT, COUNT(mid) as cnt FROM 'matches'
                    WHERE 
                    (mode LIKE "天梯%" or mode == "PVP周末联赛")
                    and {time_sql} and {steamid_sql}
                    ''')
    result = cursor.fetchone()
    if result[1] > 0:
        return result
    raise NoValueError()

@db.register("投掷", "场均道具投掷数", "本赛季", valid_time, True, MinAdd(-0.5), "d1", 1)
async def get_rptr(steamid: str, time_type: str) -> Tuple[float, int]:
    time_sql = get_time_sql(time_type)
    steamid_sql = f"steamid == '{steamid}'"
    cursor = get_cursor()
    cursor.execute(f'''SELECT AVG(throwsCnt) as avgTR, COUNT(mid) as cnt FROM 'matches'
                    WHERE 
                    (mode LIKE "天梯%" or mode == "PVP周末联赛")
                    and {time_sql} and {steamid_sql}
                    ''')
    result = cursor.fetchone()
    if result[1] > 0:
        return result
    raise NoValueError()

@db.register("闪白", "场均闪白数", "本赛季", valid_time, True, MinAdd(-0.5), "d1", 1)
async def get_rpfs(steamid: str, time_type: str) -> Tuple[float, int]:
    time_sql = get_time_sql(time_type)
    steamid_sql = f"steamid == '{steamid}'"
    cursor = get_cursor()
    cursor.execute(f'''SELECT AVG(flashSuccess) as avgFS, COUNT(mid) as cnt FROM 'matches'
                    WHERE 
                    (mode LIKE "天梯%" or mode == "PVP周末联赛")
                    and {time_sql} and {steamid_sql}
                    ''')
    result = cursor.fetchone()
    if result[1] > 0:
        return result
    raise NoValueError()

@db.register("白给", "平均每回合首杀-首死", "本赛季", valid_time, False, ZeroIn(-0.01), "d2", 2)
async def get_rpbg(steamid: str, time_type: str) -> Tuple[float, int]:
    time_sql = get_time_sql(time_type)
    steamid_sql = f"steamid == '{steamid}'"
    cursor = get_cursor()
    cursor.execute(f'''SELECT 
                        SUM(entryKill) as totEK, 
                        SUM(firstDeath) as totFD, 
                        SUM(score1 + score2) as totR, 
                        COUNT(mid) as cnt 
                    FROM 'matches'
                    WHERE 
                    (mode LIKE "天梯%" or mode == "PVP周末联赛")
                    and {time_sql} and {steamid_sql}
                    ''')
    result = cursor.fetchone()
    if result[3] > 0:
        return ((result[0] - result[1]) / result[2], result[3])
    raise NoValueError()

@db.register("方差rt", "rt方差", "两赛季", valid_time, True, Fix(0) , "d2", 1)
async def get_var_rt(steamid: str, time_type: str) -> Tuple[float, int]:
    time_sql = get_time_sql(time_type)
    steamid_sql = f"steamid == '{steamid}'"
    cursor = get_cursor()
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
    raise NoValueError()

@db.register("方差ADR", "ADR方差", "两赛季", valid_time, True, Fix(0) , "d0", 1)
async def get_var_adr(steamid: str, time_type: str) -> Tuple[float, int]:
    time_sql = get_time_sql(time_type)
    steamid_sql = f"steamid == '{steamid}'"
    cursor = get_cursor()
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
    raise NoValueError()

@db.register("受益", "胜率-期望胜率", "两赛季", valid_time, True, ZeroIn(-0.01), "p0", 2)
async def get_benefit(steamid: str, time_type: str) -> Tuple[float, int]:
    time_sql = get_time_sql(time_type)
    steamid_sql = f"steamid == '{steamid}'"
    cursor = get_cursor()
    cursor.execute(f'''SELECT AVG(winTeam==Team)-AVG(MAX(0, (we-2.29)/(16-2.29))), COUNT(mid) as cnt
                    FROM 'matches'
                    WHERE 
                    (mode LIKE "天梯%" or mode == "PVP周末联赛")
                    and {time_sql} and {steamid_sql}
                    ''')
    result = cursor.fetchone()
    if result[1] > 0:
        return result
    raise NoValueError()

@db.register("gprt", "官匹rating", "全部", gp_time, True, ZeroIn(-0.01), "d2", 2)
async def get_gprt(steamid: str, time_type: str) -> Tuple[float, int]:
    time_sql = get_time_sql(time_type)
    steamid_sql = f"steamid == '{steamid}'"
    cursor = get_cursor()
    cursor.execute(f'''SELECT AVG(rating) as avgRating, COUNT(mid) as cnt
                        FROM 'matches_gp'
                        WHERE 
                        {time_sql} and {steamid_sql}
                    ''')
    result = cursor.fetchone()
    if result[1] > 0:
        return result
    raise NoValueError()

@db.register("gp场次", "官匹场次", "全部", gp_time, True, Fix(0), "d0", 1)
async def get_gp_matches_cnt(steamid: str, time_type: str) -> Tuple[float, int]:
    time_sql = get_time_sql(time_type)
    steamid_sql = f"steamid == '{steamid}'"
    cursor = get_cursor()
    cursor.execute(f'''SELECT COUNT(mid) as cnt
                        FROM 'matches_gp'
                        WHERE 
                        {time_sql} and {steamid_sql}
                    ''')
    result = cursor.fetchone()
    if result[0] > 0:
        return result[0], result[0]
    raise NoValueError()

@db.register("gp回均首杀", "官匹平均每回合首杀", "全部", gp_time, True, MinAdd(-0.01), "d2", 1)
async def get_gp_rpek(steamid: str, time_type: str) -> Tuple[float, int]:
    time_sql = get_time_sql(time_type)
    steamid_sql = f"steamid == '{steamid}'"
    cursor = get_cursor()
    cursor.execute(f'''SELECT SUM(entryKill) as totEK, SUM(score1 + score2) as totR, COUNT(mid) as cnt FROM 'matches_gp'
                    WHERE 
                    {time_sql} and {steamid_sql}
                    ''')
    result = cursor.fetchone()
    if result[2] > 0:
        return (result[0] / result[1], result[2])
    raise NoValueError()

@db.register("gp回均首死", "官匹平均每回合首死", "全部", gp_time, True, MinAdd(-0.01), "d2", 1)
async def get_gp_rpfd(steamid: str, time_type: str) -> Tuple[float, int]:
    time_sql = get_time_sql(time_type)
    steamid_sql = f"steamid == '{steamid}'"
    cursor = get_cursor()
    cursor.execute(f'''SELECT SUM(firstDeath) as totFD, SUM(score1 + score2) as totR, COUNT(mid) as cnt FROM 'matches_gp'
                    WHERE 
                    {time_sql} and {steamid_sql}
                    ''')
    result = cursor.fetchone()
    if result[2] > 0:
        return (result[0] / result[1], result[2])
    raise NoValueError()

@db.register("gp回均狙杀", "官匹平均每回合狙杀", "全部", gp_time, True, MinAdd(-0.01), "d2", 1)
async def get_gp_rpsn(steamid: str, time_type: str) -> Tuple[float, int]:
    time_sql = get_time_sql(time_type)
    steamid_sql = f"steamid == '{steamid}'"
    cursor = get_cursor()
    cursor.execute(f'''SELECT SUM(snipeNum) as totSK, SUM(score1 + score2) as totR, COUNT(mid) as cnt FROM 'matches_gp'
                    WHERE 
                    {time_sql} and {steamid_sql}
                    ''')
    result = cursor.fetchone()
    if result[2] > 0:
        return (result[0] / result[1], result[2])
    raise NoValueError()

@db.register("gp白给", "官匹平均每回合首杀-首死", "全部", gp_time, False, ZeroIn(-0.01), "d2", 2)
async def get_gp_rpbg(steamid: str, time_type: str) -> Tuple[float, int]:
    time_sql = get_time_sql(time_type)
    steamid_sql = f"steamid == '{steamid}'"
    cursor = get_cursor()
    cursor.execute(f'''SELECT SUM(entryKill - entryDeath) as totEKD, SUM(score1 + score2) as totR, COUNT(mid) as cnt  FROM 'matches_gp'
                    WHERE 
                    {time_sql} and {steamid_sql}
                    ''')
    result = cursor.fetchone()
    if result[2] > 0:
        return (result[0] / result[1], result[2])
    raise NoValueError()

@db.register("gp击杀", "官匹场均击杀", "全部", gp_time, True, MinAdd(-0.1), "d2", 1)
async def get_gp_kills(steamid: str, time_type: str) -> Tuple[float, int]:
    time_sql = get_time_sql(time_type)
    steamid_sql = f"steamid == '{steamid}'"
    cursor = get_cursor()
    cursor.execute(f'''SELECT AVG(kill) as avgkill, COUNT(mid) as cnt
                        FROM 'matches_gp'
                        WHERE 
                        {time_sql} and {steamid_sql}
                    ''')
    result = cursor.fetchone()
    if result[1] > 0:
        return result
    raise NoValueError()

@db.register("gp死亡", "官匹场均死亡", "全部", gp_time, True, MinAdd(-0.1), "d2", 1)
async def get_gp_deaths(steamid: str, time_type: str) -> Tuple[float, int]:
    time_sql = get_time_sql(time_type)
    steamid_sql = f"steamid == '{steamid}'"
    cursor = get_cursor()
    cursor.execute(f'''SELECT AVG(death) as avgdeath, COUNT(mid) as cnt
                        FROM 'matches_gp'
                        WHERE 
                        {time_sql} and {steamid_sql}
                    ''')
    result = cursor.fetchone()
    if result[1] > 0:
        return result
    raise NoValueError()

@db.register("gp助攻", "官匹场均助攻", "全部", gp_time, True, MinAdd(-0.1), "d2", 1)
async def get_gp_assists(steamid: str, time_type: str) -> Tuple[float, int]:
    time_sql = get_time_sql(time_type)
    steamid_sql = f"steamid == '{steamid}'"
    cursor = get_cursor()
    cursor.execute(f'''SELECT AVG(assist) as avgassist, COUNT(mid) as cnt
                        FROM 'matches_gp'
                        WHERE 
                        {time_sql} and {steamid_sql}
                    ''')
    result = cursor.fetchone()
    if result[1] > 0:
        return result
    raise NoValueError()

@db.register("gp尽力", "官匹未胜利平均rt", "全部", gp_time, True, MinAdd(-0.05), "d2", 1)
async def get_gp_tryhard(steamid: str, time_type: str) -> Tuple[float, int]:
    time_sql = get_time_sql(time_type)
    steamid_sql = f"steamid == '{steamid}'"
    cursor = get_cursor()
    cursor.execute(f'''SELECT AVG(rating) as avgRating, COUNT(mid) as cnt
                        FROM 'matches_gp'
                        WHERE 
                        winTeam != team  
                        and {time_sql} and {steamid_sql}
                    ''')
    result = cursor.fetchone()
    if result[1] > 0:
        return result
    raise NoValueError()

@db.register("gp带飞", "官匹胜利平均rt", "全部", gp_time, True, MinAdd(-0.05), "d2", 1)
async def get_gp_carry(steamid: str, time_type: str) -> Tuple[float, int]:
    time_sql = get_time_sql(time_type)
    steamid_sql = f"steamid == '{steamid}'"
    cursor = get_cursor()
    cursor.execute(f'''SELECT AVG(rating) as avgRating, COUNT(mid) as cnt
                        FROM 'matches_gp'
                        WHERE 
                        winTeam == team  
                        and {time_sql} and {steamid_sql}
                    ''')
    result = cursor.fetchone()
    if result[1] > 0:
        return result
    raise NoValueError()

@db.register("gp炸鱼", "官匹小分平均rt", "全部", gp_time, True, MinAdd(-0.05), "d2", 1)
async def get_gp_fish(steamid: str, time_type: str) -> Tuple[float, int]:
    time_sql = get_time_sql(time_type)
    steamid_sql = f"steamid == '{steamid}'"
    cursor = get_cursor()
    cursor.execute(f'''SELECT AVG(rating) as avgRating, COUNT(mid) as cnt
                        FROM 'matches_gp'
                        WHERE 
                        winTeam == team  
                        and min(score1, score2) <= 6
                        and {time_sql} and {steamid_sql}
                    ''')
    result = cursor.fetchone()
    if result[1] > 0:
        return result
    raise NoValueError()

@db.register("皮蛋", "官匹场均下包数", "全部", gp_time, True, Fix(0), "d2", 1)
async def get_gp_c4(steamid: str, time_type: str) -> Tuple[float, int]:
    time_sql = get_time_sql(time_type)
    steamid_sql = f"steamid == '{steamid}'"
    cursor = get_cursor()
    cursor.execute(f'''SELECT AVG(bombPlanted) as avgC4, COUNT(mid) as cnt
                        FROM 'matches_gp'
                        WHERE 
                        {time_sql} and {steamid_sql}
                    ''')
    result = cursor.fetchone()
    if result[1] > 0:
        return result
    raise NoValueError()










