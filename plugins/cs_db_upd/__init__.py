from nonebot import get_plugin_config
from nonebot.plugin import PluginMetadata
from nonebot import on_command
from nonebot.permission import SUPERUSER
from nonebot.adapters.onebot.v11 import Message
from nonebot.params import CommandArg
from nonebot import require
from nonebot import logger
from nonebot import get_driver

require("utils")
from ..utils import avatar_dir
from ..utils import async_session_factory
from ..utils import get_session

require("models")
from ..models import MemberSteamID, SteamBaseInfo, SteamDetailInfo, SteamExtraInfo, MatchStatsPW, MatchStatsPWExtra, MatchStatsGP, MatchStatsGPExtra, SteamFaceitID, MatchStatsFaceit
require("cs_db_val")
from ..cs_db_val import db as db_val

from sqlalchemy import select, delete, func
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession
import time
import json
import math
import random
import asyncio
from PIL import Image
from PIL import UnidentifiedImageError
from io import BytesIO

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

class TooFrequentError(Exception):
    def __init__(self, wait_time: int):
        self.wait_time = wait_time
        super().__init__(f"操作过于频繁，请等待 {wait_time} 秒后再试。")
class LockingError(Exception):
    def __init__(self):
        super().__init__("数据库正在使用中，请稍后再试。")

class DataManager:
    def __init__(self):
        self.lock = asyncio.Lock()

    async def bind(self, uid: str, steamid: str):
        """
        绑定 SteamID
        对应 SQL: INSERT OR REPLACE ...
        """
        async with async_session_factory() as session:
            async with session.begin():
                stmt = select(MemberSteamID).where(MemberSteamID.steamid == steamid)
                result = await session.execute(stmt)
                existed = result.scalar_one_or_none()
                if existed is not None and existed.uid != uid:
                    raise ValueError("该 SteamID 已被其他账号绑定。")
                # merge 会自动检查主键 uid
                # 1. 存在 -> 更新 steamid
                # 2. 不存在 -> 插入新记录
                record = MemberSteamID(uid=uid, steamid=steamid)
                await session.merge(record)

    async def unbind(self, uid: str):
        """
        解绑 SteamID
        对应 SQL: DELETE FROM ... WHERE uid == ?
        """
        async with async_session_factory() as session:
            async with session.begin():
                # 使用 delete 语句构造器
                stmt = delete(MemberSteamID).where(MemberSteamID.uid == uid)
                await session.execute(stmt)

    def _faceit_headers(self) -> dict[str, str]:
        if not config.faceit_api_key:
            raise RuntimeError("FACEIT API key 未配置")
        return {
            "Authorization": f"Bearer {config.faceit_api_key}",
            "Accept": "application/json",
        }

    async def _faceit_get(self, path: str, params: dict | None = None) -> dict:
        url = "https://open.faceit.com/data/v4" + path
        async with get_session().get(url, headers=self._faceit_headers(), params=params) as resp:
            if resp.status == 404:
                raise ValueError("FACEIT 数据不存在")
            if resp.status in (401, 403):
                raise RuntimeError("FACEIT API key 无效或无权限")
            if resp.status >= 400:
                text = await resp.text()
                raise RuntimeError(f"FACEIT API 请求失败: {resp.status} {text[:200]}")
            return await resp.json()

    def _faceit_cs2_info(self, player: dict) -> dict:
        games = player.get("games") if isinstance(player, dict) else None
        cs2 = games.get("cs2") if isinstance(games, dict) else None
        if not isinstance(cs2, dict):
            raise ValueError("该 FACEIT 用户没有 CS2 信息")
        return cs2

    def _faceit_player_record(self, steamid: str, player: dict) -> SteamFaceitID:
        cs2 = self._faceit_cs2_info(player)
        game_player_id = str(cs2.get("game_player_id") or "")
        if not game_player_id:
            raise ValueError("该 FACEIT 用户没有公开 CS2 SteamID")
        if game_player_id != steamid:
            nickname = str(player.get("nickname") or player.get("player_id") or "unknown")
            raise ValueError(f"FACEIT 用户 {nickname} 对应 SteamID 为 {game_player_id}，与当前 SteamID {steamid} 不一致")
        return SteamFaceitID(
            steamid=steamid,
            player_id=str(player["player_id"]),
            nickname=str(player.get("nickname") or player["player_id"]),
            skill_level=self._as_int(cs2.get("skill_level")),
            faceit_elo=self._as_int(cs2.get("faceit_elo")),
            updated_at=int(time.time()),
        )

    async def bind_faceit(self, steamid: str, player_id: str) -> SteamFaceitID:
        player = await self._faceit_get(f"/players/{player_id}")
        record = self._faceit_player_record(steamid, player)
        async with async_session_factory() as session:
            async with session.begin():
                stmt = select(SteamFaceitID).where(SteamFaceitID.player_id == record.player_id)
                existed = (await session.execute(stmt)).scalar_one_or_none()
                if existed is not None and existed.steamid != steamid:
                    raise ValueError("该 FACEIT ID 已被其他 SteamID 绑定")
                await session.merge(record)
        return record

    async def unbind_faceit(self, steamid: str):
        async with async_session_factory() as session:
            async with session.begin():
                stmt = delete(SteamFaceitID).where(SteamFaceitID.steamid == steamid)
                await session.execute(stmt)

    async def _refresh_faceit_bind(self, bind: SteamFaceitID, session: AsyncSession) -> SteamFaceitID:
        player = await self._faceit_get(f"/players/{bind.player_id}")
        record = self._faceit_player_record(bind.steamid, player)
        await session.merge(record)
        return record

    def _as_int(self, value, default: int = 0) -> int:
        try:
            return int(float(value))
        except (TypeError, ValueError):
            return default

    def _as_float(self, value, default: float = 0.0) -> float:
        try:
            return float(value)
        except (TypeError, ValueError):
            return default

    def _faceit_score(self, score: dict | None, team_key: str) -> int:
        if not isinstance(score, dict):
            return 0
        return self._as_int(score.get(team_key))

    def _faceit_roster(self, match_detail: dict) -> dict[str, dict]:
        roster: dict[str, dict] = {}
        teams = match_detail.get("teams") if isinstance(match_detail, dict) else None
        if not isinstance(teams, dict):
            return roster
        for team_key, team in teams.items():
            if not isinstance(team, dict):
                continue
            players = team.get("roster") or team.get("players") or []
            if not isinstance(players, list):
                continue
            for player in players:
                if not isinstance(player, dict):
                    continue
                pid = str(player.get("player_id") or "")
                if not pid:
                    continue
                roster[pid] = {
                    "team_key": team_key,
                    "steamid": str(player.get("game_player_id") or "") or None,
                    "nickname": str(player.get("nickname") or player.get("game_player_name") or pid),
                    "skill_level": self._as_int(player.get("skill_level") or player.get("game_skill_level")),
                    "faceit_elo": self._as_int(player.get("faceit_elo")),
                }
        return roster

    async def _update_match_faceit(self, mid: str, timeStamp: int, bind: SteamFaceitID, session: AsyncSession) -> int:
        count_stmt = select(func.count()).select_from(MatchStatsFaceit).where(MatchStatsFaceit.mid == mid)
        if (await session.execute(count_stmt)).scalar_one() > 0:
            return 0

        match_detail = await self._faceit_get(f"/matches/{mid}")
        match_stats = await self._faceit_get(f"/matches/{mid}/stats")
        await asyncio.sleep(0.2)

        results = match_detail.get("results") if isinstance(match_detail, dict) else {}
        score = results.get("score") if isinstance(results, dict) else {}
        winner_key = str(results.get("winner") or "") if isinstance(results, dict) else ""
        roster = self._faceit_roster(match_detail)
        rounds = match_stats.get("rounds") if isinstance(match_stats, dict) else []
        if not rounds:
            return 0
        first_round = rounds[0]
        round_stats = first_round.get("round_stats") if isinstance(first_round, dict) else {}
        voting = match_detail.get("voting") if isinstance(match_detail, dict) else {}
        voting_map = voting.get("map") if isinstance(voting, dict) else {}
        map_name = str(round_stats.get("Map") or voting_map.get("pick") or "unknown")
        region = str(round_stats.get("Region") or match_detail.get("region") or "")
        competition = str(match_detail.get("competition_name") or match_detail.get("organizer_name") or "FACEIT")
        mode = str(match_detail.get("game_mode") or "FACEIT")

        teams = first_round.get("teams") if isinstance(first_round, dict) else []
        if not isinstance(teams, list):
            return 0

        for team in teams:
            if not isinstance(team, dict):
                continue
            team_key = str(team.get("team_id") or "")
            players = team.get("players") or []
            if not isinstance(players, list):
                continue
            if team_key not in ("faction1", "faction2"):
                for roster_player in players:
                    if not isinstance(roster_player, dict):
                        continue
                    pid = str(roster_player.get("player_id") or "")
                    roster_team_key = roster.get(pid, {}).get("team_key")
                    if roster_team_key in ("faction1", "faction2"):
                        team_key = roster_team_key
                        break
            team_num = 1 if team_key == "faction1" else 2
            win_team = 1 if winner_key == "faction1" else 2 if winner_key == "faction2" else 0
            for player in players:
                if not isinstance(player, dict):
                    continue
                pid = str(player.get("player_id") or "")
                if not pid:
                    continue
                player_stats = player.get("player_stats") or {}
                info = roster.get(pid, {})
                steamid = info.get("steamid")
                if not steamid:
                    stmt = select(SteamFaceitID.steamid).where(SteamFaceitID.player_id == pid).limit(1)
                    steamid = (await session.execute(stmt)).scalar_one_or_none()
                skill_level = self._as_int(info.get("skill_level"))
                faceit_elo = self._as_int(info.get("faceit_elo"))
                if pid == bind.player_id:
                    skill_level = bind.skill_level
                    faceit_elo = bind.faceit_elo
                    steamid = bind.steamid

                entry = MatchStatsFaceit(
                    mid=mid,
                    player_id=pid,
                    steamid=steamid,
                    nickname=str(info.get("nickname") or player.get("nickname") or pid),
                    mapName=map_name,
                    team=team_num,
                    winTeam=win_team,
                    score1=self._faceit_score(score, "faction1"),
                    score2=self._faceit_score(score, "faction2"),
                    timeStamp=timeStamp,
                    mode=mode,
                    competitionName=competition,
                    region=region,
                    kill=self._as_int(player_stats.get("Kills")),
                    death=self._as_int(player_stats.get("Deaths")),
                    assist=self._as_int(player_stats.get("Assists")),
                    adr=self._as_float(player_stats.get("ADR")),
                    rating=self._as_float(player_stats.get("Rating")),
                    kdRatio=self._as_float(player_stats.get("K/D Ratio")),
                    headshots=self._as_int(player_stats.get("Headshots")),
                    headshotsPct=self._as_int(player_stats.get("Headshots %")),
                    mvp=self._as_int(player_stats.get("MVPs")),
                    skillLevel=skill_level,
                    faceitElo=faceit_elo,
                )
                await session.merge(entry)
        return 1

    async def _set_match_gp_extra(self, mid: str, nextUpdateTime: int, fetchCount: int, session: AsyncSession):
        record = await session.get(MatchStatsGPExtra, mid)
        assert record is not None
        record.nextUpdateTime = nextUpdateTime
        record.fetchCount = fetchCount
        await session.merge(record)

    async def _init_match_gp_extra(self, mid: str, session: AsyncSession):
        if (await session.get(MatchStatsGPExtra, mid)) is not None:
            return
        extra_info = MatchStatsGPExtra(
            mid=mid,
            nextUpdateTime=0,
            fetchCount=0,
            team1Legacy=None,
            team2Legacy=None
        )
        await session.merge(extra_info)

    async def _update_match_gp_extra(self, mid: str, session: AsyncSession):
        extra_info = await session.get(MatchStatsGPExtra, mid)
        assert extra_info is not None
        if extra_info.team1Legacy is not None or extra_info.team2Legacy is not None:
            return
        logger.info(f"计算 match_gp_extra_info")
        team1sum, team1cnt = .0, 0
        team2sum, team2cnt = .0, 0
        stmt = select(MatchStatsGP).where(MatchStatsGP.mid == mid)
        result = await session.execute(stmt)
        rows = result.scalars().all()
        for row in rows:
            if row.team == 1:
                if res := await db_val._get_extra_info(row.steamid, session, timeStamp=row.timeStamp):
                    team1sum += res.legacyScore
                    team1cnt += 1
            elif row.team == 2:
                if res := await db_val._get_extra_info(row.steamid, session, timeStamp=row.timeStamp):
                    team2sum += res.legacyScore
                    team2cnt += 1
        if team1cnt == 0 or team2cnt == 0:
            logger.warning(f"match_extra_gp_info 计算失败，队伍人数为0 {mid}")
            return
        extra_info.team1Legacy = team1sum / team1cnt
        extra_info.team2Legacy = team2sum / team2cnt
        await session.merge(extra_info)
    
    async def _check_match_gp_fetched_completed(self, mid: str, session: AsyncSession) -> tuple[bool, bool]:
        stmt = select(func.count(MatchStatsGP.mid), func.sum(MatchStatsGP.adpr)).where(MatchStatsGP.mid == mid)
        
        result = await session.execute(stmt)
        row = result.one_or_none()
        if row is None:
            return False, False
        return row[0] > 0, row[1] is not None and row[1] > 0

    async def _update_match_extra(self, mid: str, session: AsyncSession):
        extra_info = await session.get(MatchStatsPWExtra, mid)
        if extra_info is not None:
            return
        logger.info(f"计算 match_extra_info")
        team1sum, team1cnt = .0, 0
        team2sum, team2cnt = .0, 0
        stmt = select(MatchStatsPW).where(MatchStatsPW.mid == mid)
        result = await session.execute(stmt)
        rows = result.scalars().all()
        for row in rows:
            if row.team == 1:
                if res := await db_val._get_extra_info(row.steamid, session, timeStamp=row.timeStamp):
                    team1sum += res.legacyScore
                    team1cnt += 1
            elif row.team == 2:
                if res := await db_val._get_extra_info(row.steamid, session, timeStamp=row.timeStamp):
                    team2sum += res.legacyScore
                    team2cnt += 1
        if team1cnt == 0 or team2cnt == 0:
            logger.warning(f"match_extra_info 计算失败，队伍人数为0 {mid}")
            return
        extra_info = MatchStatsPWExtra(
            mid=mid,
            team1Legacy=team1sum / team1cnt,
            team2Legacy=team2sum / team2cnt
        )
        await session.merge(extra_info)

    async def _update_match(self, mid: str, timeStamp: int, season: str, session: AsyncSession):
        logger.info(f"_update_match start mid={mid} season={season} timestamp={timeStamp}")
        stmt = select(func.count()).select_from(MatchStatsPW).where(MatchStatsPW.mid == mid)
        
        result = await session.execute(stmt)
        row = result.scalar()
        if row is not None and row > 0:
            logger.info(f"update_matchpw {mid} in db")
            return 0
        logger.info(f"update_matchpw {mid} not in db, fetching...")
        url = "https://api.wmpvp.com/api/v1/csgo/match"
        payload = {
            "matchId": mid,
        }
        header = {
            "appversion": "3.5.4.172",
            "token":config.cs_wmtoken
        }
        async with get_session().post(url,headers=header,json=payload) as resp:
            data = await resp.json()
        await asyncio.sleep(0.2)
        if data["statusCode"] != 0:
            logger.error(f"爬取失败  {mid} {data}")
            raise RuntimeError("爬取失败：" + data.get("errorMessage", "未知错误"))
        base = data['data']['base']
        count = {}
        for player in data['data']['players']:
            if player['teamId'] not in count:
                count[player['teamId']] = 0
            count[player['teamId']] += 1
            
        for player in data['data']['players']:
            match_entry = MatchStatsPW(
                # --- 核心主键 ---
                mid=mid,
                steamid=player['playerId'],

                # --- 基础环境信息 ---
                seasonId=season,
                mapName=base['map'],
                timeStamp=timeStamp,
                duration=base['duration'],
                mode=base['mode'],
                greenMatch=base['greenMatch'],

                # --- 队伍与比分 ---
                team=player['team'],
                winTeam=base['winTeam'],
                score1=base['score1'],
                score2=base['score2'],
                teamId=player['teamId'],
                
                # --- 组排逻辑 (isgroup) ---
                # 这里的逻辑是：如果该队伍 ID 在本局出现的次数 > 1，则视为组排
                # SQLAlchemy 会自动处理 bool -> int (1/0) 的转换
                isgroup=bool(count[player['teamId']] > 1),

                # --- 评分数据 ---
                pwRating=player['pwRating'],
                we=player['we'],
                
                # --- PVP 特有数据 ---
                pvpScore=player['pvpScore'],
                pvpStars=player['pvpStars'],
                pvpScoreChange=player['pvpScoreChange'],
                pvpMvp=int(player['mvp']),  # 注意这里对应 SQL 中的 pvpMvp

                # --- 基础 KDA ---
                kill=player['kill'],
                death=player['death'],
                assist=player['assist'],

                # --- 详细战斗数据 ---
                entryKill=player['entryKill'],
                firstDeath=player['firstDeath'],
                headShot=player['headShot'],
                headShotRatio=player['headShotRatio'],
                dmgArmor=player['dmgArmor'],
                dmgHealth=player['dmgHealth'],
                snipeNum=player['snipeNum'],

                # --- 道具与投掷 ---
                flashTeammate=player['flashTeammate'],
                flashSuccess=player['flashSuccess'],
                throwsCnt=player['throwsCnt'],

                # --- 多杀统计 ---
                twoKill=player['twoKill'],
                threeKill=player['threeKill'],
                fourKill=player['fourKill'],
                fiveKill=player['fiveKill'],

                # --- 残局统计 ---
                vs1=player['vs1'],
                vs2=player['vs2'],
                vs3=player['vs3'],
                vs4=player['vs4'],
                vs5=player['vs5'],

                # --- 进阶数据 ---
                adpr=player['adpr'],
                rws=player['rws']
            )

            # 执行 Upsert 操作
            await session.merge(match_entry)

            try:
                await self._update_stats_card(player['playerId'], session)
                await self._update_extra_info(player['playerId'], session)
            except Exception as exc:
                logger.warning(f"_update_match player update skipped mid={mid} player={player['playerId']} error={exc}")
        await self._update_match_extra(mid, session)
        logger.info(f"update_match {mid} success")
        return 1


    async def _update_matchgp(self, mid: str, timeStamp: int, session: AsyncSession):
        logger.info(f"_update_matchgp start mid={mid} timestamp={timeStamp}")
        await self._init_match_gp_extra(mid, session)
        extra_info = await session.get(MatchStatsGPExtra, mid)
        assert extra_info is not None

        fetched, completed = await self._check_match_gp_fetched_completed(mid, session)

        already_in_db = False
        if fetched:
            if completed:
                logger.info(f"update_matchgp {mid} in db")
                return 0
            elif time.time() < extra_info.nextUpdateTime:
                logger.info(f"update_matchgp {mid} too frequent, skipped")
                return 0
            else:
                already_in_db = True
                logger.warning(f"update_matchgp {mid} incomplete data, refetching...")

        url = "https://api.wmpvp.com/api/v1/csgo/match"
        payload = {"matchId": mid}
        header = {
            "appversion": "3.5.4.172",
            "token": config.cs_wmtoken
        }
        
        async with get_session().post(url, headers=header, json=payload) as resp:
            data = await resp.json()
        await asyncio.sleep(0.2)

        if data["statusCode"] != 0:
            logger.error(f"爬取失败 {mid} {data}")
            raise RuntimeError("爬取失败：" + data.get("errorMessage", "未知错误"))

        base = data['data']['base']
        players = data['data']['players']
        for player in players:
            def as_int(value, default: int = 0) -> int:
                return int(value) if value is not None else default

            def as_float(value, default: float = 0.0) -> float:
                return float(value) if value is not None else default

            # 显式赋值，左边是数据库列名，右边是数据来源
            stats_entry = MatchStatsGP(
                # --- 主键 ---
                mid=mid,
                steamid=player['playerId'],
                
                # --- 基础信息 (来自 base) ---
                mapName=base['map'],
                team=player['team'],
                winTeam=base['winTeam'],
                score1=base['score1'],
                score2=base['score2'],
                timeStamp=timeStamp,
                mode=base['mode'],
                duration=base['duration'],
                
                # --- 玩家数据 (来自 player) ---
                kill=as_int(player.get('kill')),
                handGunKill=as_int(player.get('handGunKill')),
                entryKill=as_int(player.get('entryKill')),
                awpKill=as_int(player.get('awpKill')),
                death=as_int(player.get('death')),
                entryDeath=as_int(player.get('entryDeath')),
                assist=as_int(player.get('assist')),
                headShot=as_int(player.get('headShot')),
                rating=as_float(player.get('rating')),
                
                # --- 投掷物 ---
                itemThrow=as_int(player.get('itemThrow')),
                flash=as_int(player.get('flash')),
                flashTeammate=as_int(player.get('flashTeammate')),
                flashSuccess=as_int(player.get('flashSuccess')),
                
                # --- 多杀 ---
                twoKill=as_int(player.get('twoKill')),
                threeKill=as_int(player.get('threeKill')),
                fourKill=as_int(player.get('fourKill')),
                fiveKill=as_int(player.get('fiveKill')),
                
                # --- 残局 ---
                vs1=as_int(player.get('vs1')),
                vs2=as_int(player.get('vs2')),
                vs3=as_int(player.get('vs3')),
                vs4=as_int(player.get('vs4')),
                vs5=as_int(player.get('vs5')),
                
                # --- 进阶数据 ---
                adpr=as_float(player.get('adpr')),
                rws=as_float(player.get('rws')),
                kast=as_float(player.get('kast')),
                
                # --- 其他 ---
                rank=as_int(player.get('rank')),
                throwsCnt=as_int(player.get('throwsCnt')),
                bombPlanted=as_int(player.get('bombPlanted')),
                bombDefused=as_int(player.get('bombDefused')),
                smokeThrows=as_int(player.get('smokeThrows')),
                grenadeDamage=as_int(player.get('grenadeDamage')),
                infernoDamage=as_int(player.get('infernoDamage')),
                mvp=as_int(player.get('mvp')) # 类型转换
            )
            
            # 执行 Upsert (存在则更新，不存在则插入)
            await session.merge(stats_entry)
            if not already_in_db:
                try:
                    await self._update_stats_card(player['playerId'], session)
                    await self._update_extra_info(player['playerId'], session)
                except TooFrequentError as exc:
                    logger.warning(f"skip update_stats_card in update_matchgp: steamid={player['playerId']} reason={exc}")
                except RuntimeError as exc:
                    logger.warning(f"skip update_stats_card in update_matchgp: steamid={player['playerId']} reason={exc}")
                except SQLAlchemyError:
                    # 数据库事务异常必须上抛，避免在已失效事务上继续执行 SQL
                    logger.exception(f"sqlalchemy error in update_matchgp mid={mid} player={player['playerId']}")
                    raise
                except Exception as exc:
                    logger.exception(f"unexpected error in update_matchgp mid={mid} player={player['playerId']} error={exc}")
        await self._update_match_gp_extra(mid, session)
        await self._set_match_gp_extra(mid,
                int(time.time() + (random.random() + 1) * 80000 * math.pow(2, extra_info.fetchCount)),
                extra_info.fetchCount + 1, session)
        _, completed = await self._check_match_gp_fetched_completed(mid, session)
        if already_in_db:
            if not completed:
                logger.warning(f"update_matchgp {mid} incomplete data after fetch")
            return 0
        else:
            if not completed:
                logger.warning(f"update_matchgp {mid} fetch failed to get complete data")
            else:
                logger.info(f"update_matchgp {mid} success")
            return 1

    async def _insert_detail_info(self, data: dict, session: AsyncSession):
        logger.info(f"Inserting detail info: {data['steamId']}, {data['seasonId']}")

        # --- 准备引用变量 (简化后续写法) ---
        # 直接通过 key 访问，KeyError 由 Python 原生抛出
        radar = data['radar']
        fp = radar['firePower']
        fp_d = fp['detail']
        mk = radar['marksmanship']
        mk_d = mk['detail']
        fu = radar['followUpShot']
        fu_d = fu['detail']
        ft = radar['first']
        ft_d = ft['detail']
        it = radar['item']
        it_d = it['detail']
        ov = radar['oneVN']
        ov_d = ov['detail']
        sn = radar['sniper']
        sn_d = sn['detail']
        app = radar['app'] # 注意：app 可能没有 score，但一定有 detail
        app_d = app['detail']

        # --- 3. 显式实例化 SteamDetailInfo ---
        detail_info = SteamDetailInfo(
            # 主键
            steamid=data['steamId'],
            seasonId=data['seasonId'],

            # 基础综合
            pvpScore=int(data['pvpScore']),
            pvpStars=int(data['stars']),
            cnt=int(data['cnt']),
            winRate=float(data['winRate']),
            pwRating=float(data['pwRating']),

            # KDA 与 基础评分
            kills=int(data['kills']),
            rws=float(data['rws']),
            
            # APP / 综合评分细节
            pwRatingTAvg=float(app_d['pw_rating_t_avg_raw']),
            pwRatingCtAvg=float(app_d['pw_rating_ct_avg_raw']),
            kastPerRound=float(app_d['kast_per_round_raw']),

            # 火力 (FirePower)
            firePowerScore=fp['score'],
            killsPerRound=float(fp_d['kills_per_round_raw']),
            killsPerWinRound=float(fp_d['kills_per_win_round_raw']),
            damagePerRound=float(fp_d['damage_per_round_raw']),
            damagePerRoundWin=float(fp_d['damage_per_round_win_raw']),
            roundsWithAKill=float(fp_d['rounds_with_a_kill_raw']),
            multiKillRoundsPercentage=float(fp_d['multi_kill_rounds_percentage_raw']),
            we=float(fp_d['we_raw']),
            pistolRoundRating=float(fp_d['pistol_round_rating_raw']),

            # 枪法 (Marksmanship)
            marksmanshipScore=mk['score'],
            headshotRate=float(mk_d['headshot_rate_raw']),
            killTime=int(mk_d['kill_time_raw']), # 假设是整数字符串 '544'
            smHitRate=float(mk_d['sm_hit_rate_raw']),
            reactionTime=float(mk_d['reaction_time_raw']),
            rapidStopRate=float(mk_d['rapid_stop_rate_raw']),

            # 补枪与辅助 (FollowUp)
            followUpShotScore=fu['score'],
            savedTeammatePerRound=float(fu_d['saved_teammate_per_round_raw']),
            tradeKillsPerRound=float(fu_d['trade_kills_per_round_raw']),
            tradeKillsPercentage=float(fu_d['trade_kills_percentage_raw']),
            assistKillsPercentage=float(fu_d['assist_kills_percentage_raw']),
            damagePerKill=float(fu_d['damage_per_kill_raw']),

            # 首杀 (First Blood)
            firstScore=ft['score'],
            firstHurt=float(ft_d['first_hurt_raw']),
            winAfterOpeningKill=float(ft_d['win_after_opening_kill_raw']),
            firstSuccessRate=float(ft_d['first_success_rate_raw']),
            firstKill=float(ft_d['first_kill_raw']),
            firstRate=float(ft_d['first_rate_raw']),

            # 道具 (Item)
            itemScore=it['score'],
            itemRate=float(it_d['item_rate_raw']),
            utilityDamagePerRounds=float(it_d['utility_damage_per_rounds_raw']),
            flashAssistPerRound=float(it_d['flash_assist_per_round_raw']),
            flashbangFlashRate=float(it_d['flashbang_flash_rate_raw']),
            timeOpponentFlashedPerRound=float(it_d['time_opponent_flashed_per_round_raw']),

            # 残局 (OneVN)
            oneVnScore=ov['score'],
            v1WinPercentage=float(ov_d['v1_win_percentage_raw']),
            clutchPointsPerRound=float(ov_d['clutch_points_per_round_raw']),
            lastAlivePercentage=float(ov_d['last_alive_percentage_raw']),
            timeAlivePerRound=float(ov_d['time_alive_per_round_raw']),
            savesPerRoundLoss=float(ov_d['saves_per_round_loss_raw']),

            # 狙击 (Sniper)
            sniperScore=sn['score'],
            sniperFirstKillPercentage=float(sn_d['sniper_first_kill_percentage_raw']),
            sniperKillsPercentage=float(sn_d['sniper_kills_percentage_raw']),
            sniperKillPerRound=float(sn_d['sniper_kill_per_round_raw']),
            roundsWithSniperKillsPercentage=float(sn_d['rounds_with_sniper_kills_percentage_raw']),
            sniperMultipleKillRoundPercentage=float(sn_d['sniper_multiple_kill_round_percentage_raw'])
        )

        await session.merge(detail_info)

    async def _update_stats_card(self, steamid: str, session: AsyncSession, interval: int = 600):
        logger.info(f"获取具体信息 for SteamID: {steamid}")
        url = "https://api.wmpvp.com/api/csgo/home/pvp/detailStats/v2"
        payload = {
            "mySteamId": config.cs_mysteam_id,
            "toSteamId": steamid,
            "csgoSeasonId": SeasonId,
        }
        header = {
            "appversion": "3.5.4.172",
            "token":config.cs_wmtoken
        }
        result_info: SteamBaseInfo | None = await session.get(SteamBaseInfo, steamid)
        if result_info is not None:
            if time.time() - result_info.updateTime <= interval:
                logger.warning(f"数据更新过于频繁 for SteamID: {steamid}")
                raise TooFrequentError(int(interval - (time.time() - result_info.updateTime)))
            result_info.updateTime = int(time.time())
            await session.merge(result_info)
        else:
            record = SteamBaseInfo(
                steamid=steamid,
                name="",
                updateTime=int(time.time()),
                updateMatchTime=0,
                avatarlink="",
                lasttime=0,
                ladderScore="[]"
            )
            result_info = await session.merge(record)
        async with get_session().post(url,headers=header,json=payload) as result:
            data = await result.json()
        if data["statusCode"] != 0:
            raise RuntimeError("爬取失败：" + data["errorMessage"])
        if result_info.avatarlink != data["data"]["avatar"]:
            try:
                async with get_session().get(data["data"]["avatar"]) as resp:
                    image_data = await resp.read()
                # 缩小图片到128*128
                img = Image.open(BytesIO(image_data))
                img_small = img.resize((128, 128), Image.Resampling.LANCZOS)
                img_small.save(avatar_dir / f"{steamid}.png", "PNG")
            except UnidentifiedImageError:
                logger.warning(f"头像格式无法识别，跳过头像保存: {steamid} {data['data']['avatar']}")
            except Exception as exc:
                logger.warning(f"头像下载或保存失败，跳过头像保存: {steamid}, {exc}")
        result_info.name = data["data"]["name"]

        result_info.name = data["data"]["name"]
        result_info.updateTime = int(time.time())
        result_info.avatarlink = data["data"]["avatar"]
        result_info.ladderScore = json.dumps(data["data"]["ladderScoreList"])

        await session.merge(result_info)
        await self._insert_detail_info(data["data"], session)
        await asyncio.sleep(0.2)
        result_detail: SteamDetailInfo | None = await session.get(SteamDetailInfo, (steamid, lastSeasonId))
        if result_detail == None:
            payload = {
                "mySteamId": config.cs_mysteam_id,
                "toSteamId": steamid,
                "csgoSeasonId": lastSeasonId,
            }
            async with get_session().post(url,headers=header,json=payload) as result:
                data = await result.json()
            if data["statusCode"] != 0:
                raise RuntimeError("上赛季爬取失败：" + data["errorMessage"])
            await self._insert_detail_info(data["data"], session)
            await asyncio.sleep(0.2)
    
    async def _update_extra_info(self, steamid: str, session: AsyncSession):
        logger.info(f"计算 extra_info for SteamID: {steamid}")
        base_info = await session.get(SteamBaseInfo, steamid)
        detail_info = await session.get(SteamDetailInfo, (steamid, SeasonId))
        detail_info_last = await session.get(SteamDetailInfo, (steamid, lastSeasonId))
        if base_info is None or detail_info is None or detail_info_last is None:
            return
        ladderHistory = json.loads(base_info.ladderScore)
        MaxScore = max([d["score"] for d in ladderHistory])
        TotCount = sum([d["matchCount"] for d in ladderHistory])
        if detail_info.cnt + detail_info_last.cnt == 0:
            AvgRating = 0.6
            AvgWe = 4.0
        else:
            AvgRating = (
                detail_info.pwRating * detail_info.cnt 
                + detail_info_last.pwRating * detail_info_last.cnt
            ) / (detail_info.cnt + detail_info_last.cnt)
            AvgWe = (
                detail_info.we * detail_info.cnt
                + detail_info_last.we * detail_info_last.cnt
            ) / (detail_info.cnt + detail_info_last.cnt)

        legacyScore = MaxScore * math.log(TotCount + 1) * (AvgRating / 0.3 + AvgWe / 3)

        extra_info = SteamExtraInfo(
            steamid=steamid,
            timeStamp=int(time.time()),
            legacyScore=legacyScore
        )

        old_extra_info: SteamExtraInfo | None = await db_val._get_extra_info(steamid, session)
        if old_extra_info is None or abs(old_extra_info.legacyScore - extra_info.legacyScore) > 1:
            await session.merge(extra_info)
        else:
            logger.info(f"extra_info no change, skipped for SteamID: {steamid}")
    
    async def _update_faceit_matches(self, steamid: str, session: AsyncSession) -> list[str]:
        bind = await session.get(SteamFaceitID, steamid)
        if bind is None:
            return []
        bind = await self._refresh_faceit_bind(bind, session)
        latest_time = await db_val.get_latest_faceit_match_time(steamid)
        params = {
            "game": "cs2",
            "offset": 0,
            "limit": 20,
        }
        if latest_time > 0:
            params["from"] = latest_time + 1
        history = await self._faceit_get(f"/players/{bind.player_id}/history", params=params)
        await asyncio.sleep(0.2)
        items = history.get("items") if isinstance(history, dict) else []
        if not isinstance(items, list):
            return []
        added: list[str] = []
        for match in sorted(items, key=lambda item: int(item.get("started_at") or 0)):
            mid = str(match.get("match_id") or "")
            started_at = self._as_int(match.get("started_at"))
            if not mid or started_at <= latest_time:
                continue
            try:
                if await self._update_match_faceit(mid, started_at, bind, session):
                    added.append(mid)
            except Exception as exc:
                logger.warning(f"update faceit match skipped steamid={steamid} mid={mid} error={exc}")
        return added

    async def update_stats(self, steamid: str, interval: int=600) -> tuple[str, list[str], list[str], list[str]]:
        logger.info(f"update_stats start steamid={steamid} interval={interval}")
        # 尝试获取锁，失败则抛出 LockingError
        try:
            await asyncio.wait_for(self.lock.acquire(), timeout=0.1)
        except asyncio.TimeoutError:
            logger.warning(f"update_stats lock acquire timeout steamid={steamid}")
            raise LockingError()
        
        try:
            try:
                async with async_session_factory() as session:
                    async with session.begin():
                        logger.info(f"update_stats stage=stats_card steamid={steamid}")
                        await self._update_stats_card(steamid, session)
                    async with session.begin():
                        logger.info(f"update_stats stage=extra_info steamid={steamid}")
                        await self._update_extra_info(steamid, session)
            except TooFrequentError as e:
                logger.info(f"update_stats stage=stats_card skipped too frequent steamid={steamid} wait={e.wait_time}s")
            except RuntimeError as e:
                logger.warning(f"更新数据失败 {steamid} {e}")
            except Exception:
                logger.exception(f"update_stats unexpected error during profile refresh steamid={steamid}")
                raise
            base_info = await db_val.get_base_info(steamid)
            assert base_info is not None
            if base_info.updateMatchTime + interval > time.time():
                logger.info(f"update_stats stage=match_list skipped too frequent steamid={steamid}")
                raise TooFrequentError(int(base_info.updateMatchTime + interval - time.time()))
            async with async_session_factory() as session:
                async with session.begin():
                    logger.info(f"update_stats stage=mark_update_time steamid={steamid}")
                    base_info.updateMatchTime = int(time.time())
                    await session.merge(base_info)

            LastTime = base_info.lasttime
            newLastTime = LastTime
            addMatchesList: list[str] = []
            addMatchesGPList: list[str] = []
            addMatchesFaceitList: list[str] = []

            async def _work(session: AsyncSession) -> None:
                nonlocal newLastTime
                nonlocal addMatchesList
                for SeasonID in [SeasonId, lastSeasonId]:
                    logger.info(f"update_stats stage=match_list season={SeasonID} steamid={steamid}")
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
                        async with get_session().post(url, json=payload, headers=headers) as result:
                            ddata = await result.json()
                        if ddata["statusCode"] != 0:
                            logger.error(f"爬取失败 {steamid} {SeasonID} {page} {ddata}")
                            raise RuntimeError(ddata["errorMessage"])
                        await asyncio.sleep(0.2)
                        for match in ddata['data']['matchList']:
                            newLastTime = max(newLastTime, match["timeStamp"])
                            if match["timeStamp"] > LastTime:
                                if await self._update_match(match["matchId"], match["timeStamp"],SeasonID, session):
                                    addMatchesList.append(match["matchId"])
                            else:
                                return
                        if len(ddata['data']['matchList']) == 0:
                            break
                        page += 1
            async def _work_gp(session: AsyncSession) -> None:
                nonlocal addMatchesGPList
                logger.info(f"update_stats stage=match_list_gp steamid={steamid}")
                url = "https://api.wmpvp.com/api/csgo/home/match/list"  
                headers = {
                    "appversion": "3.5.4.172",
                    "token": config.cs_wmtoken
                }
                payload = {
                    "dataSource": 0,
                    "mySteamId": config.cs_mysteam_id,
                    "pageSize": 50,
                    "toSteamId": steamid
                }

                async with get_session().post(url, json=payload, headers=headers) as result:
                    ddata = await result.json()
                if ddata["statusCode"] != 0:
                    logger.error(f"gp爬取失败 {steamid} {ddata}")
                    raise RuntimeError(ddata["errorMessage"])
                await asyncio.sleep(0.2)
                if ddata['data']['dataPublic']:
                    for match in ddata['data']['matchList']:
                        if await self._update_matchgp(match["matchId"], match["timeStamp"], session):
                            addMatchesGPList.append(match["matchId"])
        
            async with async_session_factory() as session:
                async with session.begin():
                    logger.info(f"update_stats stage=write_match_list steamid={steamid}")
                    await _work(session)
                    base_info.lasttime = newLastTime
                    await session.merge(base_info)

                async with session.begin():
                    logger.info(f"update_stats stage=write_match_list_gp steamid={steamid}")
                    await _work_gp(session)

                async with session.begin():
                    logger.info(f"update_stats stage=write_match_list_faceit steamid={steamid}")
                    addMatchesFaceitList = await self._update_faceit_matches(steamid, session)
            logger.info(f"update_stats done steamid={steamid} pw_added={len(addMatchesList)} gp_added={len(addMatchesGPList)} faceit_added={len(addMatchesFaceitList)}")
            return base_info.name, addMatchesList, addMatchesGPList, addMatchesFaceitList
        finally:
            logger.info(f"update_stats release_lock steamid={steamid}")
            self.lock.release()
    
debug_update_stats_card = on_command("update_stats_card", priority=10, block=True, permission=SUPERUSER)
debug_update_extra_info = on_command("update_extra_info", priority=10, block=True, permission=SUPERUSER)
debug_update_matchgp = on_command("update_matchgp", priority=10, block=True, permission=SUPERUSER)
debug_update_match = on_command("update_match", priority=10, block=True, permission=SUPERUSER)


def _split_args(args: Message) -> list[str]:
    return [s for s in args.extract_plain_text().strip().split() if s]


@debug_update_stats_card.handle()
async def handle_debug_update_stats_card(args: Message = CommandArg()):
    argv = _split_args(args)
    if len(argv) != 1:
        await debug_update_stats_card.finish("用法: /update_stats_card <steamid>")
    steamid = argv[0]
    async with async_session_factory() as session:
        async with session.begin():
            await db._update_stats_card(steamid, session)
    await debug_update_stats_card.finish(f"update_stats_card 成功: {steamid}")


@debug_update_extra_info.handle()
async def handle_debug_update_extra_info(args: Message = CommandArg()):
    argv = _split_args(args)
    if len(argv) != 1:
        await debug_update_extra_info.finish("用法: /update_extra_info <steamid>")
    steamid = argv[0]
    async with async_session_factory() as session:
        async with session.begin():
            await db._update_extra_info(steamid, session)
    await debug_update_extra_info.finish(f"update_extra_info 成功: {steamid}")


@debug_update_matchgp.handle()
async def handle_debug_update_matchgp(args: Message = CommandArg()):
    argv = _split_args(args)
    if len(argv) < 1:
        await debug_update_matchgp.finish("用法: /update_matchgp <matchid> [timestamp]")
    mid = argv[0]
    try:
        timestamp = int(argv[1]) if len(argv) >= 2 else int(time.time())
    except ValueError:
        await debug_update_matchgp.finish("timestamp 必须为整数")
    async with async_session_factory() as session:
        async with session.begin():
            changed = await db._update_matchgp(mid, timestamp, session)
    await debug_update_matchgp.finish(f"update_matchgp 完成: mid={mid}, changed={changed}")


@debug_update_match.handle()
async def handle_debug_update_match(args: Message = CommandArg()):
    argv = _split_args(args)
    if len(argv) < 1:
        await debug_update_match.finish("用法: /update_match <matchid> [timestamp] [season]")
    mid = argv[0]
    try:
        timestamp = int(argv[1]) if len(argv) >= 2 else int(time.time())
    except ValueError:
        await debug_update_match.finish("timestamp 必须为整数")
    season = argv[2] if len(argv) >= 3 else SeasonId
    async with async_session_factory() as session:
        async with session.begin():
            changed = await db._update_match(mid, timestamp, season, session)
    await debug_update_match.finish(f"update_match 完成: mid={mid}, season={season}, changed={changed}")
  
db = DataManager()
