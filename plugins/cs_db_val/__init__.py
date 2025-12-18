from nonebot import get_plugin_config
from nonebot.plugin import PluginMetadata
from nonebot import require
from nonebot import logger

require("cs_db_upd")
from ..cs_db_upd import GroupMember, MemberSteamID, MatchStatsGP, MatchStatsPW, SteamBaseInfo, SteamDetailInfo

require("utils")

from ..utils import async_session_factory
from ..utils import get_today_start_timestamp

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Callable, Awaitable
import time
from sqlalchemy import select, func, text, or_, case

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
    def getval(self, a: float, b: float) -> tuple[float, float]:
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


AsyncFloatFunc = Callable[[str, str], Awaitable[tuple[float, int]]]

@dataclass
class RankConfig:
    title: str
    default_time: str
    allowed_time: list[str]
    reversed: bool
    range_gen: RangeGen
    outputfmt: str
    func: AsyncFloatFunc

valid_rank: list[str] = []


def get_time_sql(time_type: str) -> str:
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
    def __init__(self) -> None:
        self._registry: dict[str, RankConfig] = {}

    def register(self, name: str, title: str, default_time: str, allowed_time: list[str] | None, reversed: bool, range_gen: RangeGen, outputfmt: str) -> Callable[[AsyncFloatFunc], AsyncFloatFunc]:
        if allowed_time is None:
            allowed_time = [default_time]
        
        valid_rank.append(name)

        def decorator(func: AsyncFloatFunc) -> AsyncFloatFunc:
            if name in self._registry:
                raise ValueError(f"重复注册排名类型: {name}")
            self._registry[name] = RankConfig(title, default_time, allowed_time, reversed, range_gen, outputfmt, func)
            return func
        return decorator

    async def get_steamid(self, uid: str) -> str | None:
        async with async_session_factory() as session:
            record = await session.get(MemberSteamID, uid)
            
            return record.steamid if record else None

    async def get_base_info(self, steamid) -> SteamBaseInfo | None:
        async with async_session_factory() as session:
            return await session.get(SteamBaseInfo, steamid)

    async def get_detail_info(self, steamid, seasonid = SeasonId) -> SteamDetailInfo | None:
        async with async_session_factory() as session:
            return await session.get(SteamDetailInfo, (steamid, seasonid))

    async def search_user(self, name, id = 1) -> SteamBaseInfo | None:
        async with async_session_factory() as session:
            stmt = select(SteamBaseInfo).where(SteamBaseInfo.name.like(f"%{name}%")).limit(id)
            result = await session.execute(stmt)
            records = result.scalars().all()
            if len(records) >= id:
                return records[id - 1]
            return None

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


    async def get_all_value(self, steamid, time_type):
        async with async_session_factory() as session:
            is_win = case((MatchStatsPW.winTeam == MatchStatsPW.team, 1), else_=0)
            stmt = (
                select(
                    func.avg(MatchStatsPW.pwRating),
                    func.max(MatchStatsPW.pwRating),
                    func.min(MatchStatsPW.pwRating),
                    func.avg(MatchStatsPW.we),
                    func.avg(MatchStatsPW.adpr),
                    func.avg(is_win),
                    func.avg(MatchStatsPW.kill),
                    func.avg(MatchStatsPW.death),
                    func.avg(MatchStatsPW.assist),
                    func.sum(MatchStatsPW.pvpScoreChange),
                    func.sum(MatchStatsPW.entryKill),
                    func.sum(MatchStatsPW.firstDeath),
                    func.avg(MatchStatsPW.headShot),
                    func.sum(MatchStatsPW.snipeNum),
                    func.sum(MatchStatsPW.twoKill + MatchStatsPW.threeKill + MatchStatsPW.fourKill + MatchStatsPW.fiveKill),
                    func.avg(MatchStatsPW.throwsCnt),
                    func.avg(MatchStatsPW.flashTeammate),
                    func.avg(MatchStatsPW.flashSuccess),
                    func.sum(MatchStatsPW.score1 + MatchStatsPW.score2),
                    func.count(MatchStatsPW.mid)
                )
                .where(*get_ladder_filter(steamid, time_type))
            )
            return (await session.execute(stmt)).one()
        
    async def get_propmt(self, steamid, times = ['本赛季']):
        base_info = await self.get_base_info(steamid)
        detail_info = await self.get_detail_info(steamid)
        
        if not base_info or not detail_info:
            return None
        
        score = "未定段" if detail_info.pvpScore == 0 else f"{detail_info.pvpScore}"
        prompt = f"用户名 {base_info.name}，当前天梯分数 {score}，本赛季1v1胜率 {detail_info.v1WinPercentage: .2f}，本赛季首杀率 {detail_info.firstRate: .2f}，"
        for time_type in times:
            (avgRating, maxRating, minRating, avgwe, avgADR, wr, avgkill, avgdeath, avgassist, ScoreDelta, totEK, totFD, avgHS, totSK, totMK, avgTR, avgFT, avgFS, totR, cnt) = await self.get_all_value(steamid, time_type)
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
                var, _ = await (self.get_value_config("方差rt").func(steamid, time_type))
                prompt += f"{time_type}rating方差 {var :+.2f}，"
            except NoValueError as e:
                pass
        return prompt

    async def get_matches(self, steamid: str, time_type: str, limit = 20) -> list[MatchStatsPW] | None:
        raw_time_sql = get_time_sql(time_type)

        async with async_session_factory() as session:
            stmt = (
                select(MatchStatsPW)
                .where(MatchStatsPW.steamid == steamid)
                .where(text(raw_time_sql))               # 兼容旧的时间字符串
                .order_by(MatchStatsPW.timeStamp.desc()) # 倒序排列
                .limit(limit)
            )

            result = await session.execute(stmt)
            matches = result.scalars().all()

            match_list = list(matches)

            return match_list if match_list else None
         
    async def get_username(self, uid):
        if steamid := await self.get_steamid(uid):
            if baseinfo := await self.get_base_info(steamid):
                return baseinfo.name
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

def get_ladder_filter(steamid: str, time_type: str) -> list:
    # 获取时间 SQL 片段
    time_sql_str = get_time_sql(time_type)
    
    return [
        MatchStatsPW.steamid == steamid,
        text(time_sql_str),
        # 使用 or_ 处理两种模式
        or_(
            MatchStatsPW.mode.like("天梯%"),
            MatchStatsPW.mode == "PVP周末联赛"
        )
    ]

def get_custom_filter(steamid: str, time_type: str) -> list:
    # 获取时间 SQL 片段
    time_sql_str = get_time_sql(time_type)
    
    return [
        MatchStatsPW.steamid == steamid,
        text(time_sql_str),
        MatchStatsPW.mode == "PVP自定义"
    ]

@db.register("ELO", "天梯分数", "本赛季", ["本赛季", "上赛季"], True, MinAdd(-10), "d0")
async def get_elo(steamid: str, time_type: str) -> tuple[float, int]:
    async with async_session_factory() as session:

        stmt_latest = (
            select(MatchStatsPW.pvpScore)
            .where(*get_ladder_filter(steamid, time_type))
            .order_by(MatchStatsPW.timeStamp.desc())
            .limit(1)
        )
        
        stmt_count = (
            select(func.count(MatchStatsPW.mid))
            .where(*get_ladder_filter(steamid, time_type))
        )

        latest_score_res = await session.execute(stmt_latest)
        current_elo = latest_score_res.scalar() 
        
        if current_elo is None or current_elo == 0:
            raise NoValueError()

        count_res = await session.execute(stmt_count)
        total_count = count_res.scalar()
        if total_count is None or total_count == 0:
            raise NoValueError()

        return float(current_elo), total_count

@db.register("rt", "rating", "本赛季", valid_time, True, MinAdd(-0.05), "d2")
async def get_rt(steamid: str, time_type: str) -> tuple[float, int]:
    async with async_session_factory() as session:
        stmt = (
            select(
                func.avg(MatchStatsPW.pwRating),
                func.count(MatchStatsPW.mid)
            )
            .where(*get_ladder_filter(steamid, time_type))
        )
        row = (await session.execute(stmt)).one()
        if row[1] > 0:
            return (float(row[0]), row[1])
            
    raise NoValueError()

@db.register("WE", "WE", "本赛季", valid_time, True, MinAdd(-1), "d2")
async def get_we(steamid: str, time_type: str) -> tuple[float, int]:
    async with async_session_factory() as session:
        stmt = (
            select(func.avg(MatchStatsPW.we), func.count(MatchStatsPW.mid))
            .where(*get_ladder_filter(steamid, time_type))
        )
        row = (await session.execute(stmt)).one()
        if row[1] > 0: return (float(row[0]), row[1])
    raise NoValueError()

@db.register("ADR", "ADR", "本赛季", valid_time, True, MinAdd(-10), "d2")
async def get_adr(steamid: str, time_type: str) -> tuple[float, int]:
    async with async_session_factory() as session:
        stmt = (
            select(func.avg(MatchStatsPW.adpr), func.count(MatchStatsPW.mid))
            .where(*get_ladder_filter(steamid, time_type))
        )
        row = (await session.execute(stmt)).one()
        if row[1] > 0:
            return (float(row[0]), row[1])
    raise NoValueError()

@db.register("场次", "场次", "本赛季", valid_time, True, Fix(0), "d0")
async def get_matches_cnt(steamid: str, time_type: str) -> tuple[float, int]:
    async with async_session_factory() as session:
        stmt = (
            select(func.count(MatchStatsPW.mid))
            .where(*get_ladder_filter(steamid, time_type))
        )
        result = (await session.execute(stmt)).scalar()
        if result is not None and result > 0:
            return (float(result), result)
    raise NoValueError()

@db.register("胜率", "胜率", "本赛季", valid_time, True, Fix(0), "p2")
async def get_winrate(steamid: str, time_type: str) -> tuple[float, int]:
    async with async_session_factory() as session:
        is_win = case((MatchStatsPW.winTeam == MatchStatsPW.team, 1), else_=0)
        
        stmt = (
            select(func.avg(is_win), func.count(MatchStatsPW.mid))
            .where(*get_ladder_filter(steamid, time_type))
        )
        row = (await session.execute(stmt)).one()
        if row[1] > 0: return (float(row[0]), row[1])
    raise NoValueError()

@db.register("首杀", "首杀率", "本赛季", None, True, Fix(0), "p0")
async def get_ekrate(steamid: str, time_type: str) -> tuple[float, int]:
    assert(time_type == "本赛季")
    result = await db.get_detail_info(steamid)
    if result and result.cnt != 0:
        return result.firstRate, result.cnt
    raise NoValueError()
    
@db.register("爆头", "爆头率", "本赛季", valid_time, True, Fix(0), "p0")
async def get_hsrate(steamid: str, time_type: str) -> tuple[float, int]:
    async with async_session_factory() as session:
        stmt = (
            select(
                func.sum(MatchStatsPW.headShot),
                func.sum(MatchStatsPW.kill),
                func.count(MatchStatsPW.mid)
            )
            .where(*get_ladder_filter(steamid, time_type))
        )
        row = (await session.execute(stmt)).one()
        # row: (总爆头, 总击杀, 场次)
        if row[2] > 0 and row[1] and row[1] > 0:
            return (row[0] / row[1], row[2])
    raise NoValueError()

@db.register("1v1", "1v1胜率", "本赛季", None, True, Fix(0), "p0")
async def get_1v1wr(steamid: str, time_type: str) -> tuple[float, int]:
    assert(time_type == "本赛季")
    result = await db.get_detail_info(steamid)
    if result and result.cnt != 0:
        return result.v1WinPercentage, result.cnt
    raise NoValueError()

@db.register("击杀", "场均击杀", "本赛季", valid_time, True, MinAdd(-0.1), "d2")
async def get_kills(steamid: str, time_type: str) -> tuple[float, int]:
    async with async_session_factory() as session:
        stmt = (
            select(func.avg(MatchStatsPW.kill), func.count(MatchStatsPW.mid))
            .where(*get_ladder_filter(steamid, time_type))
        )
        row = (await session.execute(stmt)).one()
        if row[1] > 0:
            return (float(row[0]), row[1])
    raise NoValueError()

@db.register("死亡", "场均死亡", "本赛季", valid_time, True, MinAdd(-0.1), "d2")
async def get_deaths(steamid: str, time_type: str) -> tuple[float, int]:
    async with async_session_factory() as session:
        stmt = (
            select(func.avg(MatchStatsPW.death), func.count(MatchStatsPW.mid))
            .where(*get_ladder_filter(steamid, time_type))
        )
        row = (await session.execute(stmt)).one()
        if row[1] > 0:
            return (float(row[0]), row[1])
    raise NoValueError()

@db.register("助攻", "场均助攻", "本赛季", valid_time, True, MinAdd(-0.1), "d2")
async def get_assists(steamid: str, time_type: str) -> tuple[float, int]:
    async with async_session_factory() as session:
        stmt = (
            select(func.avg(MatchStatsPW.assist), func.count(MatchStatsPW.mid))
            .where(*get_ladder_filter(steamid, time_type))
        )
        row = (await session.execute(stmt)).one()
        if row[1] > 0:
            return (float(row[0]), row[1])
    raise NoValueError()

@db.register("尽力", "未胜利平均rt", "两赛季", valid_time, True, MinAdd(-0.05), "d2")
async def get_tryhard(steamid: str, time_type: str) -> tuple[float, int]:
    async with async_session_factory() as session:
        stmt = (
            select(func.avg(MatchStatsPW.pwRating), func.count(MatchStatsPW.mid))
            .where(*get_ladder_filter(steamid, time_type))
            .where(MatchStatsPW.winTeam != MatchStatsPW.team)
        )
        row = (await session.execute(stmt)).one()
        if row[1] > 0:
            return (float(row[0]), row[1])
    raise NoValueError()

@db.register("带飞", "胜利平均rt", "两赛季", valid_time, True, MinAdd(-0.05), "d2")
async def get_carry(steamid: str, time_type: str) -> tuple[float, int]:
    async with async_session_factory() as session:
        stmt = (
            select(func.avg(MatchStatsPW.pwRating), func.count(MatchStatsPW.mid))
            .where(*get_ladder_filter(steamid, time_type))
            .where(MatchStatsPW.winTeam == MatchStatsPW.team)
        )
        row = (await session.execute(stmt)).one()
        if row[1] > 0:
            return (float(row[0]), row[1])
    raise NoValueError()

@db.register("炸鱼", "小分平均rt", "两赛季", valid_time, True, MinAdd(-0.05), "d2")
async def get_fish(steamid: str, time_type: str) -> tuple[float, int]:
    async with async_session_factory() as session:
        stmt = (
            select(func.avg(MatchStatsPW.pwRating), func.count(MatchStatsPW.mid))
            .where(*get_ladder_filter(steamid, time_type))
            .where(MatchStatsPW.winTeam == MatchStatsPW.team)
            .where(func.min(MatchStatsPW.score1, MatchStatsPW.score2) <= 6)
        )
        row = (await session.execute(stmt)).one()
        if row[1] > 0:
            return (float(row[0]), row[1])
    raise NoValueError()

@db.register("演员", "组排平均rt", "两赛季", valid_time, False, MinAdd(-0.05), "d2")
async def get_duoqi(steamid: str, time_type: str) -> tuple[float, int]:
    async with async_session_factory() as session:
        stmt = (
            select(func.avg(MatchStatsPW.pwRating), func.count(MatchStatsPW.mid))
            .where(*get_ladder_filter(steamid, time_type))
            .where(MatchStatsPW.isgroup == 1)
        )
        row = (await session.execute(stmt)).one()
        if row[1] > 0:
            return (float(row[0]), row[1])
    raise NoValueError()

@db.register("鼓励", "单排场次", "两赛季", valid_time, True, Fix(0), "d0")
async def get_solo_cnt(steamid: str, time_type: str) -> tuple[float, int]:
    async with async_session_factory() as session:
        stmt = (
            select(func.count(MatchStatsPW.mid))
            .where(*get_ladder_filter(steamid, time_type))
            .where(MatchStatsPW.isgroup == 0)
        )
        result = (await session.execute(stmt)).scalar()
        if result is not None and result > 0:
            return (float(result), result)
    raise NoValueError()

@db.register("悲情", ">1.2rt未胜利场次", "两赛季", valid_time, True, Fix(0), "d0")
async def get_sad_cnt(steamid: str, time_type: str) -> tuple[float, int]:
    async with async_session_factory() as session:
        stmt = (
            select(func.count(MatchStatsPW.mid))
            .where(*get_ladder_filter(steamid, time_type))
            .where(MatchStatsPW.pwRating > 1.2)
            .where(MatchStatsPW.winTeam != MatchStatsPW.team)
        )
        result = (await session.execute(stmt)).scalar()
        if result is not None and result > 0:
            return float(result), result
    raise NoValueError()

@db.register("内战", "pvp自定义平均rt", "两赛季", valid_time, True, MinAdd(-0.05), "d2")
async def get_pvp_rt(steamid: str, time_type: str) -> tuple[float, int]:
    async with async_session_factory() as session:
        stmt = (
            select(func.avg(MatchStatsPW.pwRating), func.count(MatchStatsPW.mid))
            .where(*get_custom_filter(steamid, time_type))
        )
        row = (await session.execute(stmt)).one()
        if row[1] > 0:
            return (float(row[0]), row[1])
    raise NoValueError()

@db.register("内战场次", "pvp自定义场次", "两赛季", valid_time, True, Fix(0), "d0")
async def get_pvp_cnt(steamid: str, time_type: str) -> tuple[float, int]:
    async with async_session_factory() as session:
        stmt = (
            select(func.count(MatchStatsPW.mid))
            .where(*get_custom_filter(steamid, time_type))
        )
        row = (await session.execute(stmt)).scalar()
        if row is not None and row > 0:
            return (float(row), row)
    raise NoValueError()

@db.register("内战胜率", "pvp自定义胜率", "两赛季", valid_time, True, Fix(0), "p2")
async def get_pvp_wr(steamid: str, time_type: str) -> tuple[float, int]:
    async with async_session_factory() as session:
        is_win = case((MatchStatsPW.winTeam == MatchStatsPW.team, 1), else_=0)
        stmt = (
            select(func.avg(is_win), func.count(MatchStatsPW.mid))
            .where(*get_custom_filter(steamid, time_type))
        )
        row = (await session.execute(stmt)).one()
        if row[1] > 0:
            return (float(row[0]), row[1])
    raise NoValueError()

@db.register("上分", "上分", "本周", valid_time, True, ZeroIn(-1), "d0")
async def get_upscore(steamid: str, time_type: str) -> tuple[float, int]:
    async with async_session_factory() as session:
        stmt = (
            select(func.sum(MatchStatsPW.pvpScoreChange), func.count(MatchStatsPW.mid))
            .where(*get_ladder_filter(steamid, time_type))
        )
        row = (await session.execute(stmt)).one()
        if row[1] > 0:
            return (float(row[0]) if row[0] else 0.0, row[1])
    raise NoValueError()

@db.register("回均首杀", "平均每回合首杀", "本赛季", valid_time, True, MinAdd(-0.01), "d2")
async def get_rpek(steamid: str, time_type: str) -> tuple[float, int]:
    async with async_session_factory() as session:
        stmt = (
            select(
                func.sum(MatchStatsPW.entryKill),
                func.sum(MatchStatsPW.score1 + MatchStatsPW.score2),
                func.count(MatchStatsPW.mid)
            )
            .where(*get_ladder_filter(steamid, time_type))
        )
        row = (await session.execute(stmt)).one()
        if row[2] > 0 and row[1] > 0:
            return (float(row[0]) / row[1], row[2])
    raise NoValueError()

@db.register("回均首死", "平均每回合首死", "本赛季", valid_time, True, MinAdd(-0.01), "d2")
async def get_rpfd(steamid: str, time_type: str) -> tuple[float, int]:
    async with async_session_factory() as session:
        stmt = (
            select(
                func.sum(MatchStatsPW.firstDeath),
                func.sum(MatchStatsPW.score1 + MatchStatsPW.score2),
                func.count(MatchStatsPW.mid)
            )
            .where(*get_ladder_filter(steamid, time_type))
        )
        row = (await session.execute(stmt)).one()
        if row[2] > 0 and row[1] > 0:
            return (float(row[0]) / row[1], row[2])
    raise NoValueError()

@db.register("回均狙杀", "平均每回合狙杀", "本赛季", valid_time, True, MinAdd(-0.01), "d2")
async def get_rpsn(steamid: str, time_type: str) -> tuple[float, int]:
    async with async_session_factory() as session:
        stmt = (
            select(
                func.sum(MatchStatsPW.snipeNum),
                func.sum(MatchStatsPW.score1 + MatchStatsPW.score2),
                func.count(MatchStatsPW.mid)
            )
            .where(*get_ladder_filter(steamid, time_type))
        )
        row = (await session.execute(stmt)).one()
        if row[2] > 0 and row[1] > 0:
            return (float(row[0]) / row[1], row[2])
    raise NoValueError()

@db.register("多杀", "多杀回合占比", "本赛季", valid_time, True, MinAdd(-0.01), "p0")
async def get_rpmk(steamid: str, time_type: str) -> tuple[float, int]:
    async with async_session_factory() as session:
        stmt = (
            select(
                func.sum(MatchStatsPW.twoKill + MatchStatsPW.threeKill + MatchStatsPW.fourKill + MatchStatsPW.fiveKill),
                func.sum(MatchStatsPW.score1 + MatchStatsPW.score2),
                func.count(MatchStatsPW.mid)
            )
            .where(*get_ladder_filter(steamid, time_type))
        )
        row = (await session.execute(stmt)).one()
        if row[2] > 0 and row[1] > 0:
            return (float(row[0]) / row[1], row[2])
    raise NoValueError()

@db.register("内鬼", "场均闪白队友", "本赛季", valid_time, True, MinAdd(-0.5), "d1")
async def get_rpft(steamid: str, time_type: str) -> tuple[float, int]:
    async with async_session_factory() as session:
        stmt = (
            select(func.avg(MatchStatsPW.flashTeammate), func.count(MatchStatsPW.mid))
            .where(*get_ladder_filter(steamid, time_type))
        )
        row = (await session.execute(stmt)).one()
        if row[1] > 0:
            return (float(row[0]), row[1])
    raise NoValueError()

@db.register("投掷", "场均道具投掷数", "本赛季", valid_time, True, MinAdd(-0.5), "d1")
async def get_rptr(steamid: str, time_type: str) -> tuple[float, int]:
    async with async_session_factory() as session:
        stmt = (
            select(func.avg(MatchStatsPW.throwsCnt), func.count(MatchStatsPW.mid))
            .where(*get_ladder_filter(steamid, time_type))
        )
        row = (await session.execute(stmt)).one()
        if row[1] > 0:
            return (float(row[0]), row[1])
    raise NoValueError()

@db.register("闪白", "场均闪白数", "本赛季", valid_time, True, MinAdd(-0.5), "d1")
async def get_rpfs(steamid: str, time_type: str) -> tuple[float, int]:
    async with async_session_factory() as session:
        stmt = (
            select(func.avg(MatchStatsPW.flashSuccess), func.count(MatchStatsPW.mid))
            .where(*get_ladder_filter(steamid, time_type))
        )
        row = (await session.execute(stmt)).one()
        if row[1] > 0:
            return (float(row[0]), row[1])
    raise NoValueError()

@db.register("白给", "平均每回合首杀-首死", "本赛季", valid_time, False, ZeroIn(-0.01), "d2")
async def get_rpbg(steamid: str, time_type: str) -> tuple[float, int]:
    async with async_session_factory() as session:
        stmt = (
            select(
                func.sum(MatchStatsPW.entryKill),
                func.sum(MatchStatsPW.firstDeath),
                func.sum(MatchStatsPW.score1 + MatchStatsPW.score2),
                func.count(MatchStatsPW.mid)
            )
            .where(*get_ladder_filter(steamid, time_type))
        )
        row = (await session.execute(stmt)).one()
        if row[3] > 0 and row[2] > 0:
            return ((float(row[0]) - float(row[1])) / row[2], row[3])
    raise NoValueError()

@db.register("方差rt", "rt方差", "两赛季", valid_time, True, Fix(0) , "d2")
async def get_var_rt(steamid: str, time_type: str) -> tuple[float, int]:
    async with async_session_factory() as session:
        avg_stmt = select(func.avg(MatchStatsPW.pwRating)).where(*get_ladder_filter(steamid, time_type))
        avg_val = (await session.execute(avg_stmt)).scalar()
        
        if avg_val is None:
            raise NoValueError()
            
        stmt = (
            select(
                func.sum((MatchStatsPW.pwRating - avg_val) * (MatchStatsPW.pwRating - avg_val)),
                func.count(MatchStatsPW.pwRating)
            )
            .where(*get_ladder_filter(steamid, time_type))
        )
        row = (await session.execute(stmt)).one()
        
        if row[1] > 1:
            return (float(row[0]) / (row[1] - 1), row[1])
    raise NoValueError()

@db.register("方差ADR", "ADR方差", "两赛季", valid_time, True, Fix(0) , "d0")
async def get_var_adr(steamid: str, time_type: str) -> tuple[float, int]:
    async with async_session_factory() as session:
        avg_stmt = select(func.avg(MatchStatsPW.adpr)).where(*get_ladder_filter(steamid, time_type))
        avg_val = (await session.execute(avg_stmt)).scalar()
        
        if avg_val is None:
            raise NoValueError()

        stmt = (
            select(
                func.sum((MatchStatsPW.adpr - avg_val) * (MatchStatsPW.adpr - avg_val)),
                func.count(MatchStatsPW.adpr)
            )
            .where(*get_ladder_filter(steamid, time_type))
        )
        row = (await session.execute(stmt)).one()
        
        if row[1] > 1:
            return (float(row[0]) / (row[1] - 1), row[1])
    raise NoValueError()

@db.register("受益", "胜率-期望胜率", "两赛季", valid_time, True, ZeroIn(-0.01), "p0")
async def get_benefit(steamid: str, time_type: str) -> tuple[float, int]:
    async with async_session_factory() as session:
        is_win = case((MatchStatsPW.winTeam == MatchStatsPW.team, 1), else_=0)
        expected_win = func.max(0, (MatchStatsPW.we - 2.29) / (16 - 2.29))
        
        stmt = (
            select(
                func.avg(is_win) - func.avg(expected_win),
                func.count(MatchStatsPW.mid)
            )
            .where(*get_ladder_filter(steamid, time_type))
        )
        row = (await session.execute(stmt)).one()
        if row[1] > 0:
            return (float(row[0]) if row[0] is not None else 0.0, row[1])
    raise NoValueError()

@db.register("火力", "火力分", "本赛季", None, True, Fix(0), "d0")
async def get_firepower(steamid: str, time_type: str) -> tuple[float, int]:
    assert(time_type == "本赛季")
    result = await db.get_detail_info(steamid)
    if result and result.cnt != 0:
        return result.firePowerScore, result.cnt
    raise NoValueError()

@db.register("枪法", "枪法分", "本赛季", None, True, Fix(0), "d0")
async def get_marksmanship(steamid: str, time_type: str) -> tuple[float, int]:
    assert(time_type == "本赛季")
    result = await db.get_detail_info(steamid)
    if result and result.cnt != 0:
        return result.marksmanshipScore, result.cnt
    raise NoValueError()

@db.register("补枪", "补枪分", "本赛季", None, True, Fix(0), "d0")
async def get_follow_up_shot_score(steamid: str, time_type: str) -> tuple[float, int]:
    assert(time_type == "本赛季")
    result = await db.get_detail_info(steamid)
    if result and result.cnt != 0:
        return result.followUpShotScore, result.cnt
    raise NoValueError()

@db.register("突破", "突破分", "本赛季", None, True, Fix(0), "d0")
async def get_breakthrough_score(steamid: str, time_type: str) -> tuple[float, int]:
    assert(time_type == "本赛季")
    result = await db.get_detail_info(steamid)
    if result and result.cnt != 0:
        return result.firstScore, result.cnt
    raise NoValueError()

@db.register("残局", "残局分", "本赛季", None, True, Fix(0), "d0")
async def get_endgame_score(steamid: str, time_type: str) -> tuple[float, int]:
    assert(time_type == "本赛季")
    result = await db.get_detail_info(steamid)
    if result and result.cnt != 0:
        return result.oneVnScore, result.cnt
    raise NoValueError()

@db.register("道具", "道具分", "本赛季", None, True, Fix(0), "d0")
async def get_utility_score(steamid: str, time_type: str) -> tuple[float, int]:
    assert(time_type == "本赛季")
    result = await db.get_detail_info(steamid)
    if result and result.cnt != 0:
        return result.itemScore, result.cnt
    raise NoValueError()

@db.register("狙击", "狙击分", "本赛季", None, True, Fix(0), "d0")
async def get_sniper_score(steamid: str, time_type: str) -> tuple[float, int]:
    assert(time_type == "本赛季")
    result = await db.get_detail_info(steamid)
    if result and result.cnt != 0:
        return result.sniperScore, result.cnt
    raise NoValueError()

@db.register("好人", "CTrt-Trt", "本赛季", None, True, ZeroIn(-0.01), "d0")
async def get_good_person(steamid: str, time_type: str) -> tuple[float, int]:
    assert(time_type == "本赛季")
    result = await db.get_detail_info(steamid)
    if result and result.cnt != 0:
        return result.pwRatingCtAvg - result.pwRatingTAvg, result.cnt
    raise NoValueError()

@db.register("gprt", "官匹rating", "全部", gp_time, True, ZeroIn(-0.01), "d2")
async def get_gprt(steamid: str, time_type: str) -> tuple[float, int]:
    time_sql = get_time_sql(time_type)
    async with async_session_factory() as session:
        stmt = (
            select(
                func.avg(MatchStatsGP.rating),
                func.count(MatchStatsGP.mid)
            )
            .where(MatchStatsGP.steamid == steamid)
            .where(text(time_sql))
        )
        result = (await session.execute(stmt)).one()
        if result is not None and result[1] > 0:
            return result[0], result[1]
    raise NoValueError()

@db.register("gp场次", "官匹场次", "全部", gp_time, True, Fix(0), "d0")
async def get_gp_matches_cnt(steamid: str, time_type: str) -> tuple[float, int]:
    time_sql = get_time_sql(time_type)
    async with async_session_factory() as session:
        stmt = (
            select(func.count(MatchStatsGP.mid))
            .where(MatchStatsGP.steamid == steamid)
            .where(text(time_sql))
        )
        result = (await session.execute(stmt)).scalar()
        
        if result is not None and result > 0:
            return result, result
    raise NoValueError()

@db.register("gp回均首杀", "官匹平均每回合首杀", "全部", gp_time, True, MinAdd(-0.01), "d2")
async def get_gp_rpek(steamid: str, time_type: str) -> tuple[float, int]:
    time_sql = get_time_sql(time_type)
    async with async_session_factory() as session:
        stmt = (
            select(
                func.sum(MatchStatsGP.entryKill),
                func.sum(MatchStatsGP.score1 + MatchStatsGP.score2), # 总回合数
                func.count(MatchStatsGP.mid)
            )
            .where(MatchStatsGP.steamid == steamid)
            .where(text(time_sql))
        )
        row = (await session.execute(stmt)).one()
        
        # row: (totEK, totR, cnt)
        if row[2] > 0 and row[1] > 0: # 确保场次>0 且 总回合数>0
            # SQL SUM 可能返回 None，转为 0.0 安全处理
            tot_ek = row[0] if row[0] else 0
            tot_rounds = row[1]
            return (tot_ek / tot_rounds, row[2])
    raise NoValueError()

@db.register("gp回均首死", "官匹平均每回合首死", "全部", gp_time, True, MinAdd(-0.01), "d2")
async def get_gp_rpfd(steamid: str, time_type: str) -> tuple[float, int]:
    time_sql = get_time_sql(time_type)
    async with async_session_factory() as session:
        stmt = (
            select(
                func.sum(MatchStatsGP.entryDeath), # 对应旧SQL的 firstDeath
                func.sum(MatchStatsGP.score1 + MatchStatsGP.score2),
                func.count(MatchStatsGP.mid)
            )
            .where(MatchStatsGP.steamid == steamid)
            .where(text(time_sql))
        )
        row = (await session.execute(stmt)).one()
        
        if row[2] > 0 and row[1] > 0:
            tot_fd = row[0] if row[0] else 0
            tot_rounds = row[1]
            return (tot_fd / tot_rounds, row[2])
    raise NoValueError()

@db.register("gp回均狙杀", "官匹平均每回合狙杀", "全部", gp_time, True, MinAdd(-0.01), "d2")
async def get_gp_rpsn(steamid: str, time_type: str) -> tuple[float, int]:
    time_sql = get_time_sql(time_type)
    async with async_session_factory() as session:
        stmt = (
            select(
                func.sum(MatchStatsGP.awpKill),
                func.sum(MatchStatsGP.score1 + MatchStatsGP.score2),
                func.count(MatchStatsGP.mid)
            )
            .where(MatchStatsGP.steamid == steamid)
            .where(text(time_sql))
        )
        row = (await session.execute(stmt)).one()
        
        if row[2] > 0 and row[1] > 0:
            tot_awp = row[0] if row[0] else 0
            tot_rounds = row[1]
            return (tot_awp / tot_rounds, row[2])
    raise NoValueError()

@db.register("gp白给", "官匹平均每回合首杀-首死", "全部", gp_time, False, ZeroIn(-0.01), "d2")
async def get_gp_rpbg(steamid: str, time_type: str) -> tuple[float, int]:
    time_sql = get_time_sql(time_type)
    async with async_session_factory() as session:
        stmt = (
            select(
                func.sum(MatchStatsGP.entryKill - MatchStatsGP.entryDeath),
                func.sum(MatchStatsGP.score1 + MatchStatsGP.score2),
                func.count(MatchStatsGP.mid)
            )
            .where(MatchStatsGP.steamid == steamid)
            .where(text(time_sql))
        )
        row = (await session.execute(stmt)).one()
        
        if row[2] > 0 and row[1] > 0:
            diff = row[0] if row[0] else 0
            tot_rounds = row[1]
            return (diff / tot_rounds, row[2])
    raise NoValueError()

@db.register("gp击杀", "官匹场均击杀", "全部", gp_time, True, MinAdd(-0.1), "d2")
async def get_gp_kills(steamid: str, time_type: str) -> tuple[float, int]:
    time_sql = get_time_sql(time_type)
    async with async_session_factory() as session:
        stmt = (
            select(func.avg(MatchStatsGP.kill), func.count(MatchStatsGP.mid))
            .where(MatchStatsGP.steamid == steamid)
            .where(text(time_sql))
        )
        row = (await session.execute(stmt)).one()
        if row is not None and row[1] > 0:
            return row[0], row[1]
    raise NoValueError()

@db.register("gp死亡", "官匹场均死亡", "全部", gp_time, True, MinAdd(-0.1), "d2")
async def get_gp_deaths(steamid: str, time_type: str) -> tuple[float, int]:
    time_sql = get_time_sql(time_type)
    async with async_session_factory() as session:
        stmt = (
            select(func.avg(MatchStatsGP.death), func.count(MatchStatsGP.mid))
            .where(MatchStatsGP.steamid == steamid)
            .where(text(time_sql))
        )
        row = (await session.execute(stmt)).one()
        if row is not None and row[1] > 0:
            return row[0], row[1]
    raise NoValueError()

@db.register("gp助攻", "官匹场均助攻", "全部", gp_time, True, MinAdd(-0.1), "d2")
async def get_gp_assists(steamid: str, time_type: str) -> tuple[float, int]:
    time_sql = get_time_sql(time_type)
    async with async_session_factory() as session:
        stmt = (
            select(func.avg(MatchStatsGP.assist), func.count(MatchStatsGP.mid))
            .where(MatchStatsGP.steamid == steamid)
            .where(text(time_sql))
        )
        row = (await session.execute(stmt)).one()
        if row is not None and row[1] > 0:
            return row[0], row[1]
    raise NoValueError()

@db.register("gp尽力", "官匹未胜利平均rt", "全部", gp_time, True, MinAdd(-0.05), "d2")
async def get_gp_tryhard(steamid: str, time_type: str) -> tuple[float, int]:
    time_sql = get_time_sql(time_type)
    async with async_session_factory() as session:
        stmt = (
            select(func.avg(MatchStatsGP.rating), func.count(MatchStatsGP.mid))
            .where(MatchStatsGP.steamid == steamid)
            .where(MatchStatsGP.winTeam != MatchStatsGP.team)
            .where(text(time_sql))
        )
        row = (await session.execute(stmt)).one()
        if row is not None and row[1] > 0:
            return row[0], row[1]
    raise NoValueError()

@db.register("gp带飞", "官匹胜利平均rt", "全部", gp_time, True, MinAdd(-0.05), "d2")
async def get_gp_carry(steamid: str, time_type: str) -> tuple[float, int]:
    time_sql = get_time_sql(time_type)
    async with async_session_factory() as session:
        stmt = (
            select(func.avg(MatchStatsGP.rating), func.count(MatchStatsGP.mid))
            .where(MatchStatsGP.steamid == steamid)
            .where(MatchStatsGP.winTeam == MatchStatsGP.team)
            .where(text(time_sql))
        )
        row = (await session.execute(stmt)).one()
        if row is not None and row[1] > 0:
            return row[0], row[1]
    raise NoValueError()

@db.register("gp炸鱼", "官匹小分平均rt", "全部", gp_time, True, MinAdd(-0.05), "d2")
async def get_gp_fish(steamid: str, time_type: str) -> tuple[float, int]:
    time_sql = get_time_sql(time_type)
    async with async_session_factory() as session:
        stmt = (
            select(func.avg(MatchStatsGP.rating), func.count(MatchStatsGP.mid))
            .where(MatchStatsGP.steamid == steamid)
            .where(MatchStatsGP.winTeam == MatchStatsGP.team)
            # 小分 <= 6
            .where(func.min(MatchStatsGP.score1, MatchStatsGP.score2) <= 6)
            .where(text(time_sql))
        )
        row = (await session.execute(stmt)).one()
        if row is not None and row[1] > 0:
            return row[0], row[1]
    raise NoValueError()

@db.register("皮蛋", "官匹场均下包数", "全部", gp_time, True, Fix(0), "d2")
async def get_gp_c4(steamid: str, time_type: str) -> tuple[float, int]:
    time_sql = get_time_sql(time_type)
    async with async_session_factory() as session:
        stmt = (
            select(func.avg(MatchStatsGP.bombPlanted), func.count(MatchStatsGP.mid))
            .where(MatchStatsGP.steamid == steamid)
            .where(text(time_sql))
        )
        row = (await session.execute(stmt)).one()
        if row is not None and row[1] > 0:
            return row[0], row[1]
    raise NoValueError()
