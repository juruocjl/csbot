from nonebot import get_plugin_config
from nonebot.plugin import PluginMetadata
from nonebot import on_command
from nonebot.permission import SUPERUSER
from nonebot import require
from nonebot import logger
from nonebot import get_driver

require("utils")
from ..utils import avatar_dir
from ..utils import async_session_factory
from ..utils import get_session

require("cs_db_val")
from ..cs_db_val import MemberSteamID, SteamBaseInfo, SteamDetailInfo, SteamExtraInfo, MatchStatsPW, MatchStatsPWExtra, MatchStatsGP, MatchStatsGPExtra, GroupMember
from ..cs_db_val import db as db_val

from sqlalchemy import select, delete, func
from sqlalchemy.ext.asyncio import AsyncSession
import time
import json
import math
import random
import asyncio
from PIL import Image
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

class DataManager:


    async def bind(self, uid: str, steamid: str):
        """
        绑定 SteamID
        对应 SQL: INSERT OR REPLACE ...
        """
        async with async_session_factory() as session:
            async with session.begin():
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

    async def _update_match_gp_extra(self, mid: str, session: AsyncSession):
        extra_info = await session.get(MatchStatsGPExtra, mid)
        if extra_info is not None:
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
        extra_info = MatchStatsGPExtra(
            mid=mid,
            team1Legacy=team1sum / team1cnt,
            team2Legacy=team2sum / team2cnt
        )
        await session.merge(extra_info)
    
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
            except:
                pass
        await self._update_match_extra(mid, session)
        logger.info(f"update_match {mid} success")
        return 1

    async def _update_matchgp(self, mid: str, timeStamp: int, session: AsyncSession):
        stmt = select(func.count()).select_from(MatchStatsGP).where(MatchStatsGP.mid == mid)
        
        result = await session.execute(stmt)
        row = result.scalar()
        if row is not None and row > 0:
            # logger.info(f"update_matchgp {mid} in db")
            return 0


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
                kill=player['kill'],
                handGunKill=player['handGunKill'],
                entryKill=player['entryKill'],
                awpKill=player['awpKill'],
                death=player['death'],
                entryDeath=player['entryDeath'],
                assist=player['assist'],
                headShot=player['headShot'],
                rating=player['rating'],
                
                # --- 投掷物 ---
                itemThrow=player['itemThrow'],
                flash=player['flash'],
                flashTeammate=player['flashTeammate'],
                flashSuccess=player['flashSuccess'],
                
                # --- 多杀 ---
                twoKill=player['twoKill'],
                threeKill=player['threeKill'],
                fourKill=player['fourKill'],
                fiveKill=player['fiveKill'],
                
                # --- 残局 ---
                vs1=player['vs1'],
                vs2=player['vs2'],
                vs3=player['vs3'],
                vs4=player['vs4'],
                vs5=player['vs5'],
                
                # --- 进阶数据 ---
                adpr=player['adpr'],
                rws=player['rws'],
                kast=player['kast'],
                
                # --- 其他 ---
                rank=player['rank'],
                throwsCnt=player['throwsCnt'],
                bombPlanted=player['bombPlanted'],
                bombDefused=player['bombDefused'],
                smokeThrows=player['smokeThrows'],
                grenadeDamage=player['grenadeDamage'],
                infernoDamage=player['infernoDamage'],
                mvp=int(player['mvp']) # 类型转换
            )
            
            # 执行 Upsert (存在则更新，不存在则插入)
            await session.merge(stats_entry)

            try:
                await self._update_stats_card(player['playerId'], session)
                await self._update_extra_info(player['playerId'], session)
            except:
                pass

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
            async with get_session().get(data["data"]["avatar"]) as resp:
                image_data = await resp.read()
            # 缩小图片到128*128
            img = Image.open(BytesIO(image_data))
            img_small = img.resize((128, 128), Image.Resampling.LANCZOS)
            img_small.save(avatar_dir / f"{steamid}.png", "PNG")
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
    
    async def update_stats(self, steamid: str, interval: int=600) -> tuple[str, int, int]:
        try:
            async with async_session_factory() as session:
                async with session.begin():
                    await self._update_stats_card(steamid, session)
                async with session.begin():
                    await self._update_extra_info(steamid, session)
        except RuntimeError as e:
            logger.warning(f"更新数据失败 {steamid} {e}")
        base_info = await db_val.get_base_info(steamid)
        assert base_info is not None
        if base_info.updateMatchTime + interval > time.time():
            raise TooFrequentError(int(base_info.updateMatchTime + interval - time.time()))
        async with async_session_factory() as session:
            async with session.begin():
                base_info.updateMatchTime = int(time.time())
                await session.merge(base_info)

        LastTime = base_info.lasttime
        newLastTime = LastTime
        addMatches = 0
        addMatchesGP = 0
        async def _work(session: AsyncSession) -> None:
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
                    async with get_session().post(url, json=payload, headers=headers) as result:
                        ddata = await result.json()
                    if ddata["statusCode"] != 0:
                        logger.error(f"爬取失败 {steamid} {SeasonID} {page} {ddata}")
                        raise RuntimeError(ddata["errorMessage"])
                    await asyncio.sleep(0.2)
                    for match in ddata['data']['matchList']:
                        newLastTime = max(newLastTime, match["timeStamp"])
                        if match["timeStamp"] > LastTime:
                            addMatches += await self._update_match(match["matchId"], match["timeStamp"],SeasonID, session)
                        else:
                            return
                    if len(ddata['data']['matchList']) == 0:
                        break
                    page += 1
        async def _work_gp(session: AsyncSession) -> None:
            nonlocal addMatchesGP
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
                    addMatchesGP += await self._update_matchgp(match["matchId"], match["timeStamp"], session)
    
        async with async_session_factory() as session:
            async with session.begin():
                await _work(session)
                base_info.lasttime = newLastTime
                await session.merge(base_info)

            async with session.begin():
                await _work_gp(session)
    
        return base_info.name, addMatches, addMatchesGP
    
    async def add_member(self, gid: str, uid: str):
        if gid.startswith("group_"):
            clean_gid = gid.split("_")[1]

            async with async_session_factory() as session:
                async with session.begin():
                    
                    new_member = GroupMember(gid=clean_gid, uid=uid)
                    await session.merge(new_member)
  
db = DataManager()

qwqqwq = on_command("qwqqwq", permission=SUPERUSER)

@qwqqwq.handle()
async def _():
    async with async_session_factory() as session:
        stmt = select(MatchStatsGP.mid).distinct()
        result = (await session.execute(stmt)).scalars().all()
    async with async_session_factory() as session:
        for mid in result:
            print(mid)
            async with session.begin():
                await db._update_match_gp_extra(mid, session)
