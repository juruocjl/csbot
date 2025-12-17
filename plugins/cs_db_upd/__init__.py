from nonebot import get_plugin_config
from nonebot.plugin import PluginMetadata
from nonebot import require
from nonebot import logger

require("utils")

from ..utils import Base, async_session_factory

get_cursor = require("utils").get_cursor
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

class DataManager:
    def __init__(self):
        cursor = get_cursor()
        

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
        cursor = get_cursor()
        cursor.execute('''SELECT COUNT(*) as cnt FROM matches WHERE mid == ?
        ''',(mid, ))
        result = cursor.fetchone()
        if result[0] > 0:
            # logger.info(f"update_match {mid} in db")
            return 0
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
        for player in data['data']['players']:
            cursor.execute('''INSERT OR REPLACE INTO matches
                (mid, steamid, seasonId, mapName, team, winTeam, score1, score2,
                pwRating, we, timeStamp, kill, death, assist, duration, mode, pvpScore, pvpStars, pvpScoreChange, pvpMvp,
                isgroup, greenMatch, entryKill, headShot, headShotRatio,
                flashTeammate, flashSuccess,
                twoKill, threeKill, fourKill, fiveKill, vs1, vs2, vs3, vs4, vs5,
                dmgArmor, dmgHealth, adpr, rws, teamId, throwsCnt, snipeNum, firstDeath
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
        logger.info(f"update_match {mid} success")
        return 1

    async def update_matchgp(self, mid, timeStamp):
        async with async_session_factory() as session:
            stmt = select(func.count()).select_from(MatchStatsGP).where(MatchStatsGP.mid == mid)
            
            result = await session.execute(stmt)
            count = result.scalar()
            if count > 0:
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

    async def update_stats(self, steamid):
        url = "https://api.wmpvp.com/api/csgo/home/pvp/detailStats"
        payload = {
            "mySteamId": config.cs_mysteam_id,
            "toSteamId": steamid
        }
        header = {
            "appversion": "3.5.4.172",
            "token":config.cs_wmtoken
        }
        async with get_session().post(url,headers=header,json=payload) as result:
            data = await result.json()
        if data["statusCode"] != 0:
            logger.error(f"爬取失败 {steamid} {data}")
            return (False, "爬取失败：" + data["errorMessage"])
        cursor = get_cursor()
        cursor.execute('''
        SELECT avatarlink, lasttime FROM steamid_detail WHERE steamid = ?
        ''', (steamid,))
        result = cursor.fetchone()
        if not result or result[0] != data["data"]["avatar"]:
            await async_download(data["data"]["avatar"], Path(f"./avatar/{steamid}.png"))
        LastTime = 0
        if result:
            LastTime = result[1]
        newLastTime = LastTime
        name = data["data"]["name"]
        addMatches = 0
        addMatchesGP = 0
        async def work():
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
        async def work_gp():
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
        try:
            await work()
            await work_gp()
        except RuntimeError as e:
            return (False, "爬取失败：" + str(e))
        cursor.execute('''
        INSERT OR REPLACE INTO steamid_detail 
            (steamid, avatarlink, name, pvpScore, cnt, kd, winRate, pwRating, 
            avgWe, kills, deaths, assists, rws, adr, headShotRatio, entryKillRatio, vs1WinRate, lasttime, seasonId) 
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
        
        return (True, name, addMatches, addMatchesGP)
    
    async def add_member(self, gid, uid):
        if gid.startswith("group_"):
            clean_gid = gid.split("_")[1]

            async with async_session_factory() as session:
                async with session.begin():
                    
                    new_member = GroupMember(gid=clean_gid, uid=uid)
                    await session.merge(new_member)
  
db = DataManager()