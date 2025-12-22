from nonebot import get_plugin_config
from nonebot.plugin import PluginMetadata
from nonebot import require
from nonebot import logger

require("utils")

from ..utils import Base, async_session_factory

get_session = require("utils").get_session
async_download = require("utils").async_download
get_today_start_timestamp = require("utils").get_today_start_timestamp

from sqlalchemy import String, Float, Integer, select, delete, func
from sqlalchemy.orm import Mapped, mapped_column
from pathlib import Path
import asyncio

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

class GroupMember(Base):
    __tablename__ = "group_members"

    gid: Mapped[str] = mapped_column(String, primary_key=True)
    uid: Mapped[str] = mapped_column(String, primary_key=True)

class MemberSteamID(Base):
    __tablename__ = "members_steamid"

    uid: Mapped[str] = mapped_column(String, primary_key=True)
    steamid: Mapped[str] = mapped_column(String)

class MatchStatsPW(Base):
    __tablename__ = "matches"

    # --- 复合主键 ---
    mid: Mapped[str] = mapped_column(String, primary_key=True)
    steamid: Mapped[str] = mapped_column(String, primary_key=True)

    # --- 赛季与地图 ---
    seasonId: Mapped[str] = mapped_column(String)
    mapName: Mapped[str] = mapped_column(String)
    
    # --- 队伍与比分 ---
    team: Mapped[int] = mapped_column(Integer)
    winTeam: Mapped[int] = mapped_column(Integer)
    score1: Mapped[int] = mapped_column(Integer)
    score2: Mapped[int] = mapped_column(Integer)
    
    # --- 评分数据 ---
    pwRating: Mapped[float] = mapped_column(Float)
    we: Mapped[float] = mapped_column(Float)
    
    # --- 基础数据 ---
    timeStamp: Mapped[int] = mapped_column(Integer)
    kill: Mapped[int] = mapped_column(Integer)
    death: Mapped[int] = mapped_column(Integer)
    assist: Mapped[int] = mapped_column(Integer)
    duration: Mapped[int] = mapped_column(Integer)
    mode: Mapped[str] = mapped_column(String)
    
    # --- PVP/完美特有数据 ---
    pvpScore: Mapped[int] = mapped_column(Integer)
    pvpStars: Mapped[int] = mapped_column(Integer)
    pvpScoreChange: Mapped[int] = mapped_column(Integer)
    pvpMvp: Mapped[int] = mapped_column(Integer)
    
    # --- 组队信息 (0/1) ---
    isgroup: Mapped[int] = mapped_column(Integer)
    greenMatch: Mapped[int] = mapped_column(Integer)
    
    # --- 详细击杀数据 ---
    entryKill: Mapped[int] = mapped_column(Integer)
    headShot: Mapped[int] = mapped_column(Integer)
    headShotRatio: Mapped[float] = mapped_column(Float)
    
    # --- 道具 ---
    flashTeammate: Mapped[int] = mapped_column(Integer)
    flashSuccess: Mapped[int] = mapped_column(Integer) # 修正了 mvpValue 类型
    
    # --- 多杀 ---
    twoKill: Mapped[int] = mapped_column(Integer)
    threeKill: Mapped[int] = mapped_column(Integer)
    fourKill: Mapped[int] = mapped_column(Integer)
    fiveKill: Mapped[int] = mapped_column(Integer)
    
    # --- 残局 ---
    vs1: Mapped[int] = mapped_column(Integer)
    vs2: Mapped[int] = mapped_column(Integer)
    vs3: Mapped[int] = mapped_column(Integer)
    vs4: Mapped[int] = mapped_column(Integer)
    vs5: Mapped[int] = mapped_column(Integer)
    
    # --- 伤害与其他 ---
    dmgArmor: Mapped[int] = mapped_column(Integer)
    dmgHealth: Mapped[int] = mapped_column(Integer)
    adpr: Mapped[float] = mapped_column(Float) 
    rws: Mapped[float] = mapped_column(Float)
    
    teamId: Mapped[int] = mapped_column(Integer)
    throwsCnt: Mapped[int] = mapped_column(Integer)
    snipeNum: Mapped[int] = mapped_column(Integer)
    firstDeath: Mapped[int] = mapped_column(Integer)

class MatchStatsGP(Base):
    __tablename__ = "matches_gp"

    # --- 复合主键 ---
    mid: Mapped[str] = mapped_column(String, primary_key=True)
    steamid: Mapped[str] = mapped_column(String, primary_key=True)

    # --- 基础信息 ---
    mapName: Mapped[str] = mapped_column(String)
    team: Mapped[int] = mapped_column(Integer)
    winTeam: Mapped[int] = mapped_column(Integer)
    score1: Mapped[int] = mapped_column(Integer)
    score2: Mapped[int] = mapped_column(Integer)
    timeStamp: Mapped[int] = mapped_column(Integer)
    mode: Mapped[str] = mapped_column(String)
    duration: Mapped[int] = mapped_column(Integer)
    
    # --- 击杀/死亡数据 ---
    kill: Mapped[int] = mapped_column(Integer)
    handGunKill: Mapped[int] = mapped_column(Integer)
    entryKill: Mapped[int] = mapped_column(Integer)
    awpKill: Mapped[int] = mapped_column(Integer)
    death: Mapped[int] = mapped_column(Integer)
    entryDeath: Mapped[int] = mapped_column(Integer)
    assist: Mapped[int] = mapped_column(Integer)
    headShot: Mapped[int] = mapped_column(Integer)
    
    # --- 评分 (Float) ---
    rating: Mapped[float] = mapped_column(Float)
    
    # --- 投掷物/战术 ---
    itemThrow: Mapped[int] = mapped_column(Integer)
    flash: Mapped[int] = mapped_column(Integer)
    flashTeammate: Mapped[int] = mapped_column(Integer)
    flashSuccess: Mapped[int] = mapped_column(Integer)
    
    # --- 多杀统计 ---
    twoKill: Mapped[int] = mapped_column(Integer)
    threeKill: Mapped[int] = mapped_column(Integer)
    fourKill: Mapped[int] = mapped_column(Integer)
    fiveKill: Mapped[int] = mapped_column(Integer)
    
    # --- 残局 (Clutch) ---
    vs1: Mapped[int] = mapped_column(Integer)
    vs2: Mapped[int] = mapped_column(Integer)
    vs3: Mapped[int] = mapped_column(Integer)
    vs4: Mapped[int] = mapped_column(Integer)
    vs5: Mapped[int] = mapped_column(Integer)
    
    # --- 进阶数据 (Float) ---
    adpr: Mapped[float] = mapped_column(Float)
    rws: Mapped[float] = mapped_column(Float)
    kast: Mapped[float] = mapped_column(Float)
    
    # --- 其他 ---
    rank: Mapped[int] = mapped_column(Integer)
    throwsCnt: Mapped[int] = mapped_column(Integer)
    bombPlanted: Mapped[int] = mapped_column(Integer)
    bombDefused: Mapped[int] = mapped_column(Integer)
    smokeThrows: Mapped[int] = mapped_column(Integer)
    grenadeDamage: Mapped[int] = mapped_column(Integer)
    infernoDamage: Mapped[int] = mapped_column(Integer)
    mvp: Mapped[int] = mapped_column(Integer)

class SteamBaseInfo(Base):
    __tablename__ = "steamid_baseinfo"

    # 主键
    steamid: Mapped[str] = mapped_column(String, primary_key=True)
    
    # 基础信息 (允许为空，以防某些字段抓取失败)
    avatarlink: Mapped[str] = mapped_column(String)
    name: Mapped[str] = mapped_column(String)
    lasttime: Mapped[int] = mapped_column(Integer)

class SteamDetailInfo(Base):
    __tablename__ = "steam_detail_info"

    # --- 复合主键 ---
    steamid: Mapped[str] = mapped_column(String, primary_key=True)
    seasonId: Mapped[str] = mapped_column(String, primary_key=True)

    # --- 基础综合数据 ---
    pvpScore: Mapped[int] = mapped_column(Integer)
    cnt: Mapped[int] = mapped_column(Integer)
    winRate: Mapped[float] = mapped_column(Float)
    pwRating: Mapped[float] = mapped_column(Float)

    # --- KDA 与 基础评分 ---
    kills: Mapped[int] = mapped_column(Integer)
    rws: Mapped[float] = mapped_column(Float)
    pwRatingTAvg: Mapped[float] = mapped_column(Float)
    pwRatingCtAvg: Mapped[float] = mapped_column(Float)
    kastPerRound: Mapped[float] = mapped_column(Float)

    # --- 火力 (FirePower) ---
    firePowerScore: Mapped[int] = mapped_column(Integer)
    killsPerRound: Mapped[float] = mapped_column(Float)
    killsPerWinRound: Mapped[float] = mapped_column(Float)
    damagePerRound: Mapped[float] = mapped_column(Float)
    damagePerRoundWin: Mapped[float] = mapped_column(Float)
    roundsWithAKill: Mapped[float] = mapped_column(Float)
    multiKillRoundsPercentage: Mapped[float] = mapped_column(Float)
    we: Mapped[float] = mapped_column(Float)
    pistolRoundRating: Mapped[float] = mapped_column(Float)

    # --- 枪法 (Marksmanship) ---
    marksmanshipScore: Mapped[int] = mapped_column(Integer)
    headshotRate: Mapped[float] = mapped_column(Float)
    killTime: Mapped[int] = mapped_column(Integer)
    smHitRate: Mapped[float] = mapped_column(Float)
    reactionTime: Mapped[float] = mapped_column(Float)
    rapidStopRate: Mapped[float] = mapped_column(Float)

    # --- 补枪与辅助 (FollowUp) ---
    followUpShotScore: Mapped[int] = mapped_column(Integer)
    savedTeammatePerRound: Mapped[float] = mapped_column(Float)
    tradeKillsPerRound: Mapped[float] = mapped_column(Float)
    tradeKillsPercentage: Mapped[float] = mapped_column(Float)
    assistKillsPercentage: Mapped[float] = mapped_column(Float)
    damagePerKill: Mapped[float] = mapped_column(Float)

    # --- 首杀 (First Blood) ---
    firstScore: Mapped[int] = mapped_column(Integer)
    firstHurt: Mapped[float] = mapped_column(Float)
    winAfterOpeningKill: Mapped[float] = mapped_column(Float)
    firstSuccessRate: Mapped[float] = mapped_column(Float)
    firstKill: Mapped[float] = mapped_column(Float)
    firstRate: Mapped[float] = mapped_column(Float)

    # --- 道具 (Item/Utility) ---
    itemScore: Mapped[int] = mapped_column(Integer)
    itemRate: Mapped[float] = mapped_column(Float)
    utilityDamagePerRounds: Mapped[float] = mapped_column(Float)
    flashAssistPerRound: Mapped[float] = mapped_column(Float)
    flashbangFlashRate: Mapped[float] = mapped_column(Float)
    timeOpponentFlashedPerRound: Mapped[float] = mapped_column(Float)

    # --- 残局 (Clutch / 1vN) ---
    oneVnScore: Mapped[int] = mapped_column(Integer)
    v1WinPercentage: Mapped[float] = mapped_column(Float)
    clutchPointsPerRound: Mapped[float] = mapped_column(Float)
    lastAlivePercentage: Mapped[float] = mapped_column(Float)
    timeAlivePerRound: Mapped[float] = mapped_column(Float)
    savesPerRoundLoss: Mapped[float] = mapped_column(Float)

    # --- 狙击 (Sniper) ---
    sniperScore: Mapped[int] = mapped_column(Integer)
    sniperFirstKillPercentage: Mapped[float] = mapped_column(Float)
    sniperKillsPercentage: Mapped[float] = mapped_column(Float)
    sniperKillPerRound: Mapped[float] = mapped_column(Float)
    roundsWithSniperKillsPercentage: Mapped[float] = mapped_column(Float)
    sniperMultipleKillRoundPercentage: Mapped[float] = mapped_column(Float)

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

    async def update_match(self, mid, timeStamp, season):
        async with async_session_factory() as session:
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
        async with get_session().post(url,headers=header,json=payload) as result:
            data = await result.json()
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
            
        async with async_session_factory() as session:
            async with session.begin():
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
        logger.info(f"update_match {mid} success")
        return 1

    async def update_matchgp(self, mid, timeStamp):
        async with async_session_factory() as session:
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
        
        async with get_session().post(url, headers=header, json=payload) as result:
            data = await result.json()
        await asyncio.sleep(0.2)

        if data["statusCode"] != 0:
            logger.error(f"爬取失败 {mid} {data}")
            raise RuntimeError("爬取失败：" + data.get("errorMessage", "未知错误"))

        base = data['data']['base']
        players = data['data']['players']
        async with async_session_factory() as session:
            for player in players:
                async with session.begin():
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

        logger.info(f"update_matchgp {mid} success")
        return 1

    async def insert_detail_info(self, data: dict):
        logger.info(f"Inserting detail info: {data['steamId']}, {data['seasonId']}")
        async with async_session_factory() as session:
            async with session.begin():

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

    async def update_stats(self, steamid: str) -> tuple[bool, str, int, int]:
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
        async with get_session().post(url,headers=header,json=payload) as result:
            data = await result.json()
        if data["statusCode"] != 0:
            raise RuntimeError("爬取失败：" + data["errorMessage"])
        async with async_session_factory() as session:
            async with session.begin():
                result_info: SteamBaseInfo | None = await session.get(SteamBaseInfo, steamid)
        if not result_info or result_info.avatarlink != data["data"]["avatar"]:
            await async_download(data["data"]["avatar"], Path(f"./avatar/{steamid}.png"))
        LastTime = 0
        if result_info:
            LastTime = result_info.lasttime
        newLastTime = LastTime
        name = data["data"]["name"]
        addMatches = 0
        addMatchesGP = 0
        async def work() -> None:
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
                            addMatches += await self.update_match(match["matchId"], match["timeStamp"],SeasonID)
                        else:
                            return
                    if len(ddata['data']['matchList']) == 0:
                        break
                    page += 1
        async def work_gp() -> None:
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
                logger.error(f"gp爬取失败 {steamid} {data}")
                raise RuntimeError(ddata["errorMessage"])
            await asyncio.sleep(0.2)
            if ddata['data']['dataPublic']:
                for match in ddata['data']['matchList']:
                    addMatchesGP += await self.update_matchgp(match["matchId"], match["timeStamp"])
        await work()
        await work_gp()
        
        async with async_session_factory() as session:
            async with session.begin():
                record = SteamBaseInfo(
                    steamid=steamid,
                    name=name,
                    avatarlink=data["data"]["avatar"],
                    lasttime=newLastTime
                )
                await session.merge(record)
        
        await self.insert_detail_info(data["data"])
        await asyncio.sleep(0.2)
        async with async_session_factory() as session:
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
                await self.insert_detail_info(data["data"])
                await asyncio.sleep(0.2)
        return (True, name, addMatches, addMatchesGP)
    
    async def add_member(self, gid, uid):
        if gid.startswith("group_"):
            clean_gid = gid.split("_")[1]

            async with async_session_factory() as session:
                async with session.begin():
                    
                    new_member = GroupMember(gid=clean_gid, uid=uid)
                    await session.merge(new_member)
  
db = DataManager()