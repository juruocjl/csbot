from nonebot import get_plugin_config
from nonebot.plugin import PluginMetadata
from nonebot.adapters.onebot.v11.message import Message
from nonebot import require
from nonebot import logger


from sqlalchemy import select, func
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Callable, Awaitable
import time
import json
from sqlalchemy import select, select, union_all, literal, func, text, or_, case, desc
from itertools import groupby
from collections import defaultdict
from typing import Any

require("utils")
from ..utils import async_session_factory
from ..utils import get_today_start_timestamp
require("models")
from ..models import MemberSteamID, GroupMember, SteamBaseInfo, SteamDetailInfo, SteamExtraInfo, MatchStatsPW, MatchStatsPWExtra, MatchStatsGP, MatchStatsGPExtra

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
        return f"""("timeStamp" >= {get_today_start_timestamp()})"""
    elif time_type == "昨日":
        return f"""({get_today_start_timestamp() - 24 * 3600} <= "timeStamp" and "timeStamp" < {get_today_start_timestamp()})"""
    elif time_type == "本周":
        return f"""({int(time.time()) - 7 * 24 * 3600} <= "timeStamp")"""
    elif time_type == "本赛季":
        return f"""("seasonId" = '{SeasonId}')"""
    elif time_type == "两赛季":
        return f"""("seasonId" = '{SeasonId}' or "seasonId" = '{lastSeasonId}')"""
    elif time_type == "上赛季":
        return f"""("seasonId" = '{lastSeasonId}')"""
    elif time_type == "全部":
        return f"""( 1 = 1 )"""
    else:
        raise ValueError("err time")

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

    async def is_user(self, steamid: str) -> bool:
        """是否是群友"""
        async with async_session_factory() as session:
            stmt = select(func.count(MemberSteamID.steamid)).where(MemberSteamID.steamid == steamid)
            result = await session.execute(stmt)
            count = result.scalar_one()
            return count > 0

    async def get_base_info(self, steamid: str) -> SteamBaseInfo | None:
        async with async_session_factory() as session:
            return await session.get(SteamBaseInfo, steamid)

    async def get_detail_info(self, steamid: str, seasonid: str = SeasonId) -> SteamDetailInfo | None:
        async with async_session_factory() as session:
            return await session.get(SteamDetailInfo, (steamid, seasonid))

    async def _get_extra_info(self, steamid: str, session, timeStamp: int = 2000000000) -> SteamExtraInfo | None:
        """接受 session 参数的版本，用于事务内调用"""
        stmt = (
            select(SteamExtraInfo)
            .where(SteamExtraInfo.steamid == steamid)
            .where(SteamExtraInfo.timeStamp >= timeStamp)
            .order_by(SteamExtraInfo.timeStamp.asc())
            .limit(1)
        )
        result = await session.execute(stmt)
        record = result.scalar_one_or_none()
        if record is not None:
            return record
        stmt = (
            select(SteamExtraInfo)
            .where(SteamExtraInfo.steamid == steamid)
            .where(SteamExtraInfo.timeStamp < timeStamp)
            .order_by(SteamExtraInfo.timeStamp.desc())
            .limit(1)
        )
        result = await session.execute(stmt)
        return result.scalar_one_or_none()

    async def get_extra_info(self, steamid: str, timeStamp: int = 2000000000) -> SteamExtraInfo | None:
        async with async_session_factory() as session:
            return await self._get_extra_info(steamid, session, timeStamp)

    async def search_user(self, name: str, id: int = 1) -> SteamBaseInfo | None:
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
            stmt = select(MemberSteamID.steamid).distinct()
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
    
    async def get_member_steamid(self, sid: str) -> list[str]:
        uids = await self.get_member(sid)
        steamids = set()
        for uid in uids:
            if steamid := await self.get_steamid(uid):
                steamids.add(steamid)
        return list(steamids)
    
    async def get_group_member(self, gid: str) -> list[str]:
        """
        获取群成员列表
        """
        async with async_session_factory() as session:
            stmt = select(GroupMember.uid).where(GroupMember.gid == gid)
            result = await session.execute(stmt)
            return list(result.scalars().all())
        
        return []
    
    async def get_group_member_steamid(self, gid: str) -> list[str]:
        uids = await self.get_group_member(gid)
        steamids = set()
        for uid in uids:
            if steamid := await self.get_steamid(uid):
                steamids.add(steamid)
        return list(steamids)

    def get_value_config(self, query_type: str) -> RankConfig:
        if query_type not in self._registry:
            raise ValueError(f"无效的查询类型，支持的有 {list(self._registry.keys())}")
        return self._registry[query_type]

    async def get_matches(self, steamid: str, time_type: str, 
                          only_ladder: bool = False,
                          only_custom: bool = False,
                          limit: int = 20,
                          offset: int = 0
                        ) -> list[MatchStatsPW] | None:

        async with async_session_factory() as session:
            assert not (only_ladder and only_custom), "only_ladder 和 only_custom 不能同时为 True"
            if only_ladder:
                stmt = (
                    select(MatchStatsPW)
                    .where(*get_ladder_filter(steamid, time_type))
                )
            elif only_custom:
                stmt = (
                    select(MatchStatsPW)
                    .where(*get_custom_filter(steamid, time_type))
                )
            else:
                stmt = (
                    select(MatchStatsPW)
                    .where(MatchStatsPW.steamid == steamid)
                    .where(text(get_time_sql(time_type)))
                )
            stmt = (
                stmt
                .order_by(MatchStatsPW.timeStamp.desc()) # 倒序排列
                .limit(limit).offset(offset)
            )

            result = await session.execute(stmt)
            matches = result.scalars().all()

            match_list = list(matches)

            return match_list if match_list else None
    
    async def get_matches_count(self, steamid: str, time_type: str, 
                          only_ladder: bool = False,
                          only_custom: bool = False
                        ) -> int:
        async with async_session_factory() as session:
            assert not (only_ladder and only_custom), "only_ladder 和 only_custom 不能同时为 True"
            if only_ladder:
                stmt = (
                    select(func.count(MatchStatsPW.mid))
                    .where(*get_ladder_filter(steamid, time_type))
                )
            elif only_custom:
                stmt = (
                    select(func.count(MatchStatsPW.mid))
                    .where(*get_custom_filter(steamid, time_type))
                )
            else:
                stmt = (
                    select(func.count(MatchStatsPW.mid))
                    .where(MatchStatsPW.steamid == steamid)
                    .where(text(get_time_sql(time_type)))
                )

            result = await session.execute(stmt)

            return result.scalar_one()

    async def get_match_detail(self, mid: str) -> list[MatchStatsPW] | None:
        async with async_session_factory() as session:
            stmt = select(MatchStatsPW).where(MatchStatsPW.mid == mid)

            result = await session.execute(stmt)
            matches = list(result.scalars().all())

            return matches if matches else None

    async def get_match_extra(self, mid: str) -> MatchStatsPWExtra | None:
        async with async_session_factory() as session:
            return await session.get(MatchStatsPWExtra, mid)

    async def get_match_teammate(self, steamid: str, time_type: str, querys: list[str], top_k: int = 1) -> list[list[tuple[str, float, int]]]:
        """
        获取队友信息
        参数:
            steamid: 查询的 SteamID
            time_type: 时间类型
            querys: 需要查询的内容列表
                场次：一起打的场次
                上分：一起打自己上分
                上分2：一起打对方上分
                WE：一起打时自己 WE
                WE2：一起打时对方 WE
                rt：一起打时自己 rt
                rt2：一起打时对方 rt
                用 _ 开头表示选择最小值，否则选择最大值
        返回:
            包含元组 (steamid, value, count) 的列表，value 根据 querys 决定
        """
        matches = await self.get_matches(steamid, time_type, only_ladder=True, limit=1000000000)
        assert matches is not None, "无比赛数据"
        match_info: dict[str, list[MatchStatsPW]] = {}
        for match in matches:
            teammates = await self.get_match_detail(match.mid)
            assert teammates is not None, "无比赛详情数据"
            match_info[match.mid] = [match]
            for mate in teammates:
                if mate.steamid != steamid and await self.steamid_in_db(mate.steamid) and mate.team == match.team:
                    match_info[match.mid].append(mate)
        result: list[list[tuple[str, float, int]]] = []
        
        teammate_count: dict[str, int] = defaultdict(int)
        teammate_upscore: dict[str, int] = defaultdict(int)
        teammate_upscore2: dict[str, int] = defaultdict(int)
        teammate_we: dict[str, float] = defaultdict(float)
        teammate_we2: dict[str, float] = defaultdict(float)
        teammate_rt: dict[str, float] = defaultdict(float)
        teammate_rt2: dict[str, float] = defaultdict(float)
        for mates in match_info.values():
            base_match = mates[0]
            for mate in mates[1:]:
                teammate_count[mate.steamid] += 1
                teammate_upscore[mate.steamid] += base_match.pvpScoreChange
                teammate_upscore2[mate.steamid] += mate.pvpScoreChange
                teammate_we[mate.steamid] += base_match.we
                teammate_we2[mate.steamid] += mate.we
                teammate_rt[mate.steamid] += base_match.pwRating
                teammate_rt2[mate.steamid] += mate.pwRating

        
        for querytype in querys:
            reversed = not querytype.startswith("_")
            querytype = querytype.lstrip("_")
            
            if querytype == "场次":
                best_steamids = sorted(teammate_count, key=lambda x: teammate_count[x], reverse=reversed)[:top_k]
                result.append([(id, teammate_count[id], teammate_count[id]) for id in best_steamids])
            elif querytype == "上分":
                best_steamids = sorted(teammate_upscore, key=lambda x: teammate_upscore[x], reverse=reversed)[:top_k]
                result.append([(id, teammate_upscore[id], teammate_count[id]) for id in best_steamids])
            elif querytype == "上分2":
                best_steamids = sorted(teammate_upscore2, key=lambda x: teammate_upscore2[x], reverse=reversed)[:top_k]
                result.append([(id, teammate_upscore2[id], teammate_count[id]) for id in best_steamids])
            elif querytype == "WE":
                best_steamids = sorted(teammate_we, key=lambda x: teammate_we[x] / teammate_count[x], reverse=reversed)[:top_k]
                result.append([(id, teammate_we[id] / teammate_count[id], teammate_count[id]) for id in best_steamids])
            elif querytype == "WE2":
                best_steamids = sorted(teammate_we2, key=lambda x: teammate_we2[x] / teammate_count[x], reverse=reversed)[:top_k]
                result.append([(id, teammate_we2[id] / teammate_count[id], teammate_count[id]) for id in best_steamids])
            elif querytype == "rt":
                best_steamids = sorted(teammate_rt, key=lambda x: teammate_rt[x] / teammate_count[x], reverse=reversed)[:top_k]
                result.append([(id, teammate_rt[id] / teammate_count[id], teammate_count[id]) for id in best_steamids])
            elif querytype == "rt2":
                best_steamids = sorted(teammate_rt2, key=lambda x: teammate_rt2[x] / teammate_count[x], reverse=reversed)[:top_k]
                result.append([(id, teammate_rt2[id] / teammate_count[id], teammate_count[id]) for id in best_steamids])
            else:
                assert False, "未知的查询类型"
        return result

    async def get_matches_gp(self, steamid: str, time_type: str, limit: int = 20, offset: int = 0) -> list[MatchStatsGP] | None:
        assert time_type in gp_time, "时间类型错误"
        async with async_session_factory() as session:
            stmt = (
                select(MatchStatsGP)
                .where(MatchStatsGP.steamid == steamid)
                .where(text(get_time_sql(time_type)))
                .order_by(MatchStatsGP.timeStamp.desc()) # 倒序排列
                .limit(limit).offset(offset)
            )

            result = await session.execute(stmt)
            matches = result.scalars().all()

            match_list = list(matches)

            return match_list if match_list else None
    
    async def get_matches_gp_count(self, steamid: str, time_type: str) -> int:
        assert time_type in gp_time, "时间类型错误"
        async with async_session_factory() as session:
            stmt = (
                select(func.count(MatchStatsGP.mid))
                .where(MatchStatsGP.steamid == steamid)
                .where(text(get_time_sql(time_type)))
            )

            result = await session.execute(stmt)

            return result.scalar_one()
    
    async def get_match_gp_detail(self, mid: str) -> list[MatchStatsGP] | None:
        async with async_session_factory() as session:
            stmt = select(MatchStatsGP).where(MatchStatsGP.mid == mid)

            result = await session.execute(stmt)
            matches = list(result.scalars().all())

            return matches if matches else None
    
    async def get_match_gp_extra(self, mid: str) -> MatchStatsGPExtra | None:
        async with async_session_factory() as session:
            return await session.get(MatchStatsGPExtra, mid)

    async def get_all_matches_count(self) -> int:
        """
        获取包含群成员的比赛总场次（去重后的 mid 数量）。
        """
        async with async_session_factory() as session:
            # 1. 构建联合表 (CTE)
            # 为了性能，这里只需要查 mid 和 steamid，不需要查 map, time, team 等详细信息
            stats_union = union_all(
                select(MatchStatsPW.mid, MatchStatsPW.steamid),
                select(MatchStatsGP.mid, MatchStatsGP.steamid)
            ).cte("stats_union")

            # 2. 构建查询
            # 逻辑：在联合表中，连接 MemberSteamID 表，统计不重复的 mid 数量
            stmt = (
                select(func.count(func.distinct(stats_union.c.mid)))
                .join(MemberSteamID, stats_union.c.steamid == MemberSteamID.steamid)
            )

            # 3. 执行查询
            # scalar() 会返回查询结果的第一行第一列，即 count 的数值
            result = await session.scalar(stmt)
            
            # 如果结果为空（没数据），返回 0
            return result if result is not None else 0

    async def get_all_matches(self, limit: int = 20, offset: int = 0) -> list[dict[str, Any]]:
        async with async_session_factory() as session:
            unique_members_subq = (
                select(MemberSteamID.steamid)
                .distinct()
            ).subquery("unique_members")

            stats_union = union_all(
                select(
                    MatchStatsPW.mid, 
                    MatchStatsPW.steamid, 
                    MatchStatsPW.timeStamp, 
                    MatchStatsPW.mapName, 
                    MatchStatsPW.mode,
                    MatchStatsPW.team,
                    MatchStatsPW.winTeam,
                    MatchStatsPW.score1,
                    MatchStatsPW.score2,
                    literal(0).label("isGP")
                ),
                select(
                    MatchStatsGP.mid, 
                    MatchStatsGP.steamid, 
                    MatchStatsGP.timeStamp, 
                    MatchStatsGP.mapName, 
                    MatchStatsGP.mode,
                    MatchStatsGP.team,
                    MatchStatsGP.winTeam,
                    MatchStatsGP.score1,
                    MatchStatsGP.score2,
                    literal(1).label("isGP")
                )
            ).cte("stats_union")

            # 2. 分页子查询：获取目标页的 mid 列表
            # 逻辑：找出最近的 N 场比赛，且这些比赛里必须包含至少一个群成员
            paged_mids_subq = (
                select(stats_union.c.mid, stats_union.c.timeStamp)
                .join(unique_members_subq, stats_union.c.steamid == unique_members_subq.c.steamid) # 过滤：只看有群友的比赛
                .group_by(stats_union.c.mid, stats_union.c.timeStamp)
                .order_by(desc(stats_union.c.timeStamp)) # 按时间倒序
                .limit(limit)
                .offset(offset)
            ).subquery("paged_mids")

            # 3. 主查询：根据 mid 拉取详细数据
            stmt = (
                select(
                    stats_union.c.mid,
                    stats_union.c.timeStamp,
                    stats_union.c.mapName,
                    stats_union.c.mode,
                    stats_union.c.isGP,
                    stats_union.c.steamid,
                    stats_union.c.team,
                    stats_union.c.winTeam,
                    stats_union.c.score1,
                    stats_union.c.score2
                )
                .join(paged_mids_subq, stats_union.c.mid == paged_mids_subq.c.mid) # 锁定这几场比赛
                .join(unique_members_subq, stats_union.c.steamid == unique_members_subq.c.steamid) # 锁定只显示群友
                .order_by(desc(paged_mids_subq.c.timeStamp)) # 保持比赛间的时间顺序
            )

            result = await session.execute(stmt)
            rows = result.all()

            # 4. Python 侧聚合：按 mid 分组并拆分 team1/team2
            matches_list = []
            
            # groupby 需要数据已排序，SQL 中已经 order_by desc(timestamp)，也就是隐式按 mid 分组了(如果timestamp不重复)
            # 为了保险，通常建议 key 取 mid
            for mid, group in groupby(rows, key=lambda x: x.mid):
                group_rows = list(group)
                if not group_rows:
                    continue
                    
                first = group_rows[0]
                
                match_info = {
                    "mid": mid,
                    "timeStamp": first.timeStamp,
                    "mapName": first.mapName,
                    "mode": first.mode,
                    "isGP": first.isGP,
                    "winTeam": first.winTeam, # int: 1 或 2
                    "score1": first.score1,
                    "score2": first.score2,
                    "team1": [], # 存放 team=1 的 steamid
                    "team2": []  # 存放 team=2 的 steamid
                }

                for row in group_rows:
                    # 这里的 row.team 是 int 类型
                    if row.team == 1:
                        match_info["team1"].append(row.steamid)
                    elif row.team == 2:
                        match_info["team2"].append(row.steamid)
                    # 忽略 team 不是 1 或 2 的异常数据

                matches_list.append(match_info)

            return matches_list

    async def steamid_in_db(self, steamid: str) -> bool:
        async with async_session_factory() as session:
            stmt = select(MemberSteamID).where(MemberSteamID.steamid == steamid)
            result = await session.execute(stmt)
            record = result.scalar()
            return record is not None

    async def get_username(self, uid: str) -> str | None:
        if steamid := await self.get_steamid(uid):
            if baseinfo := await self.get_base_info(steamid):
                return baseinfo.name
        return None

    async def work_msg(self, msg: Message):
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

@db.register("底蕴", "天梯底蕴", "全部", None, True, MinAdd(-1), "d0")
async def get_legacy(steamid: str, time_type: str) -> tuple[float, int]:
    assert(time_type == "全部")
    extra_info = await db.get_extra_info(steamid)
    base_info = await db.get_base_info(steamid)
    if extra_info is not None and base_info is not None:
        ladderHistory = json.loads(base_info.ladderScore)
        TotCount = sum([d["matchCount"] for d in ladderHistory])
        return (extra_info.legacyScore, TotCount)
    raise NoValueError()

@db.register("底蕴差", "平均己方-对方底蕴", "本赛季", valid_time, True, ZeroIn(-1), "d0")
async def get_legacy_diff(steamid: str, time_type: str) -> tuple[float, int]:
    async with async_session_factory() as session:
        stmt = (
            select(
                func.sum(
                    case(
                        (MatchStatsPW.team == 1, 1),
                        else_=-1
                    ) * (MatchStatsPWExtra.team1Legacy - MatchStatsPWExtra.team2Legacy)
                ),
                func.count(MatchStatsPWExtra.mid)
            )
            .select_from(MatchStatsPW)
            .join(MatchStatsPWExtra, MatchStatsPW.mid == MatchStatsPWExtra.mid)
            .where(*get_ladder_filter(steamid, time_type))
        )
        res = (await session.execute(stmt)).one()
        print(res)
        if res[1]:
            return (res[0] / res[1], res[1])
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
            return (int(row[0]) / int(row[1]), row[2])
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
            return (float(row[0]) / int(row[1]), row[2])
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
            return (float(row[0]) / int(row[1]), row[2])
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
            return (float(row[0]) / int(row[1]), row[2])
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
            return (float(row[0]) / int(row[1]), row[2])
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
            return ((float(row[0]) - float(row[1])) / int(row[2]), row[3])
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
        expected_win = func.greatest(0, (MatchStatsPW.we - 2.29) / (16 - 2.29))
        
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

@db.register("好人", "CTrt-Trt", "本赛季", None, True, ZeroIn(-0.01), "d2")
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
            return (int(tot_ek) / int(tot_rounds), row[2])
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
            return (int(tot_fd) / int(tot_rounds), row[2])
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
            return (int(tot_awp) / int(tot_rounds), row[2])
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
            return (int(diff) / int(tot_rounds), row[2])
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
