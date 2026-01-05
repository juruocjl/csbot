from nonebot import get_plugin_config
from nonebot import get_app, get_bot
from nonebot import on_command
from nonebot.adapters.onebot.v11 import Message, MessageSegment, GroupMessageEvent, Bot
from nonebot.params import CommandArg
from nonebot.plugin import PluginMetadata
from nonebot import require

import secrets
import time
import psutil
from fastapi import FastAPI, Body, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from sqlalchemy import String, Integer, Boolean, select
from sqlalchemy.orm import Mapped, mapped_column
from pydantic import BaseModel, Field
from pyppeteer import launch

require("utils")
from ..utils import Base, async_session_factory
require("cs_db_val")
from ..cs_db_val import MatchStatsPW
from ..cs_db_val import db as db_val
from ..cs_db_val import valid_time, NoValueError

require("allmsg")
from ..allmsg import get_msg_status

require("pic")
from ..pic import get_pic_status

from .config import Config

__plugin_meta__ = PluginMetadata(
    name="cs_server",
    description="",
    usage="",
    config=Config,
)

security = HTTPBearer()

config = get_plugin_config(Config)

# 全局缓存配置
from typing import Any
SEASON_STATS_CACHE: dict[str, tuple[dict[str, tuple[float, float, float]], float]] = {}  # 格式: {seasonId: (global_stats, timestamp)}
# global_stats 格式: {field_name: (min, max, avg)}
CACHE_EXPIRE_TIME: int = 3600  # 缓存过期时间，单位为秒（1小时）

class AuthSession(Base):
    __tablename__ = "auth_sessions"
    
    # 长 Token，用于 API 鉴权 (主键)
    token: Mapped[str] = mapped_column(String(64), primary_key=True)
    # 短验证码，用于群内验证 (添加索引以加快查找)
    code: Mapped[str] = mapped_column(String(10), index=True)
    # 绑定的 QQ 号和群号 (验证后填写)
    user_id: Mapped[str | None] = mapped_column(String, nullable=True)
    group_id: Mapped[str | None] = mapped_column(String, nullable=True)
    # 创建时间 (用于计算过期)
    created_at: Mapped[int] = mapped_column(Integer)
    # 上一次使用时间
    last_used_at: Mapped[int] = mapped_column(Integer, default=0)
    # 是否已验证
    is_verified: Mapped[bool] = mapped_column(Boolean, default=False)

class UserInfo(Base):
    __tablename__ = "user_info"
    
    user_id: Mapped[str] = mapped_column(String, primary_key=True)
    nickname: Mapped[str] = mapped_column(String)
    
    last_send_time: Mapped[int] = mapped_column(Integer, default=0)
    last_update_time: Mapped[int] = mapped_column(Integer, default=0)


class DataMannager:
    async def generate_token(self) -> AuthSession:
        token = secrets.token_hex(32)
        code = str(secrets.randbelow(900000) + 100000)  # 生成6位验证码
        async with async_session_factory() as session:
            auth_session = AuthSession(
                token=token,
                code=code,
                user_id=None,
                group_id=None,
                created_at=int(time.time()),
                is_verified=False,
            )
            session.add(auth_session)
            await session.commit()
            return auth_session
    
    async def verify_code(self, code: str, user_id: str, group_id: str) -> bool:
        async with async_session_factory() as session:
            async with session.begin():
                stmt = (
                    select(AuthSession)
                    .where(AuthSession.code == code)
                    .where(AuthSession.created_at >= int(time.time()) - config.auth_code_valid_seconds)
                )
                result = await session.execute(stmt)
                auth_session = result.scalar_one_or_none()
                if auth_session and not auth_session.is_verified:
                    auth_session.user_id = user_id
                    auth_session.group_id = group_id
                    auth_session.is_verified = True
                    auth_session.last_used_at = int(time.time())
                    await session.merge(auth_session)
                    return True
                return False
    
    async def get_verified_user(self, token: str) -> AuthSession | None:
        async with async_session_factory() as session:
            async with session.begin():
                stmt = (
                    select(AuthSession)
                    .where(AuthSession.token == token)
                    .where(AuthSession.is_verified == True)
                )
                result = await session.execute(stmt)
                auth_session = result.scalar_one_or_none()
                if auth_session:
                    auth_session.last_used_at = int(time.time())
                    await session.merge(auth_session)
                    return auth_session
                return None

    async def get_user(self, uid: str) -> UserInfo | None:
        async with async_session_factory() as session:
            return await session.get(UserInfo, uid)

    async def set_user_name(self, uid: str, nickname: str):
        async with async_session_factory() as session:
            async with session.begin():
                user = await session.get(UserInfo, uid)
                if user != None:
                    user.nickname = nickname
                    user.last_update_time = int(time.time())
                else:
                    user = UserInfo(
                        user_id=uid,
                        nickname=nickname,
                        last_update_time=int(time.time())
                    )
                await session.merge(user)
    
    async def set_user_send(self, uid: str):
        async with async_session_factory() as session:
            async with session.begin():
                user = await session.get(UserInfo, uid)
                if user != None:
                    user.last_send_time = int(time.time())
                    await session.merge(user)
    

async def get_user_name(uid: str, interval: int = config.user_name_cache_expiration) -> str:
    user = await db.get_user(uid)
    if user != None and user.last_update_time and (int(time.time()) - user.last_update_time) < interval:
        return user.nickname
    bot = get_bot()
    assert isinstance(bot, Bot)
    username = "未知用户"
    try:
        username = (await bot.get_stranger_info(user_id=int(uid), no_cache=True))["nickname"]
    except:
        pass
    await db.set_user_name(uid, username)
    result = await db.get_user(uid)
    assert result is not None
    return result.nickname

db = DataMannager()

verify = on_command("验证", aliases={"verify"}, priority=10)

@verify.handle()
async def handle_verify(event: GroupMessageEvent, args: Message = CommandArg()):
    code = args.extract_plain_text().strip()
    if not code:
        await verify.finish("请提供验证码，例如：verify 123456")
    user_id = event.get_user_id()
    group_id = str(event.group_id)
    success = await db.verify_code(code, user_id, group_id)
    await get_user_name(user_id, interval=0)  # 强制更新昵称
    if success:
        await verify.finish("验证成功！")
    else:
        await verify.finish("验证失败！请检查验证码是否正确或已过期。")

app: FastAPI = get_app()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # 生产环境建议将 "*" 改为具体的网页域名
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

async def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(security)):
    token = credentials.credentials
    auth_session = await db.get_verified_user(token)
    if not auth_session:
        raise HTTPException(status_code=401, detail="Invalid token")
    return auth_session

class InitTokenResponse(BaseModel):
    token: str = Field(..., description="用于 API 认证的 Token")
    code: str = Field(..., description="用于群内验证的验证码")
    expires_in: int = Field(..., description="验证码有效期，单位为秒")

class StatusResponse(BaseModel):
    cpuUsage: float = Field(..., description="CPU 使用率百分比")
    memoryTotal: float = Field(..., description="内存总容量 GB")
    memoryUsed: float = Field(..., description="已使用内存 GB")
    memoryUsagePercent: float = Field(..., description="内存使用百分比")
    memoryAvailable: float = Field(..., description="可用内存 GB")
    pictureLibrary: str = Field(..., description="图库状态")
    messageCount: str = Field(..., description="消息计数信息")

@app.post(
    "/api/auth/init",
    response_model=InitTokenResponse,
    summary="申请认证 Token",
    description="申请一个新的认证 Token 和验证码。"
)
async def init_token():
    """
    申请token接口
    返回: {"token": "...", "code": "123456"}
    """
    auth_session = await db.generate_token()
    return InitTokenResponse(
        token=auth_session.token,
        code=auth_session.code,
        expires_in=config.auth_code_valid_seconds
    )

class VerifyTokenResponse(BaseModel):
    isVerified: bool = Field(..., description="Token 是否有效")

@app.post("/api/auth/verify",
    response_model=VerifyTokenResponse,
    summary="验证认证 Token",
    description="验证提供的 Token 是否有效。"
)
async def verify_token(_ = Depends(get_current_user)):
    return VerifyTokenResponse(isVerified=True)

class InfoNameResponse(BaseModel):
    showName: str = Field(..., description="显示名称")

@app.post("/api/auth/info/name",
    response_model=InfoNameResponse,
    summary="获取认证信息",
    description="获取绑定的用户 ID。"
)
async def get_token_info(info: AuthSession = Depends(get_current_user)):
    bot = get_bot()
    assert isinstance(bot, Bot)
    assert info.user_id is not None
    assert info.group_id is not None
    username = await get_user_name(info.user_id)
    groupname = (await bot.get_group_info(group_id=int(info.group_id), no_cache=False))["group_name"]
    return InfoNameResponse(
        showName=f"{username} ({groupname})"
    )

class InfoSteamIdResponse(BaseModel):
    steamId: str | None = Field(..., description="绑定的 Steam ID")

@app.post("/api/auth/info/steamid",
    response_model=InfoSteamIdResponse,
    summary="获取绑定的 Steam ID",
    description="获取绑定的 Steam ID（如果有）。"
)
async def get_token_steamid(info: AuthSession = Depends(get_current_user)):
    assert info.user_id is not None
    return InfoSteamIdResponse(
        steamId=await db_val.get_steamid(info.user_id)
    )

@app.post("/api/status",
    response_model=StatusResponse,
    summary="获取服务器状态",
    description="获取 CPU、内存、图库和消息统计等服务器状态信息。"
)
async def get_status(info: AuthSession = Depends(get_current_user)):
    cpu_usage = psutil.cpu_percent()
    
    # 获取内存信息
    memory = psutil.virtual_memory()
    total_mem = memory.total / (1024 ** 3)  # 转换为GB
    used_mem = memory.used / (1024 ** 3)
    available_mem = memory.available / (1024 ** 3)
    mem_usage = memory.percent
    
    # 获取图库状态
    tuku = get_pic_status()
    
    # 获取消息计数
    assert info.group_id is not None
    msgcount = await get_msg_status(int(info.group_id))
    
    return StatusResponse(
        cpuUsage=cpu_usage,
        memoryTotal=total_mem,
        memoryUsed=used_mem,
        memoryUsagePercent=mem_usage,
        memoryAvailable=available_mem,
        pictureLibrary=tuku,
        messageCount=msgcount
    )

class InfoQQResponse(BaseModel):
    qq: str = Field(..., description="绑定的 QQ 号")

@app.post("/api/auth/info/qq",
    response_model=InfoQQResponse,
    summary="获取绑定的 QQ 号",
    description="获取绑定的 QQ 号。"
)
async def get_token_qq(info: AuthSession = Depends(get_current_user)):
    assert info.user_id is not None
    return InfoQQResponse(
        qq=info.user_id
    )

@app.post("/api/auth/send",
    summary="发送指定页面图片",
    description="向绑定的 QQ 群发送指定页面的图片。")
async def send_page_image(path: str = Body(..., embed=True), info: AuthSession = Depends(get_current_user)):
    assert info.user_id is not None
    user_info = await db.get_user(info.user_id)
    assert user_info is not None
    if user_info.last_send_time and (int(time.time()) - user_info.last_send_time) < config.send_interval_seconds:
        raise HTTPException(status_code=429, detail=f"发送过于频繁，请 {config.send_interval_seconds - (int(time.time()) - user_info.last_send_time)}s 后再试")
    await db.set_user_send(info.user_id)
    bot = get_bot()
    assert isinstance(bot, Bot)
    assert info.group_id is not None
    
    browser = None
    try:
        # 启动浏览器
        browser = await launch(headless=True, args=['--no-sandbox'])
        page = await browser.newPage()
        
        # 访问页面
        await page.goto(f"http://localhost", waitUntil='networkidle2')
        
        # 设置 localStorage 中的 token
        await page.evaluate(f"""
            localStorage.setItem('token', '{info.token}');
        """)
        
        # 重新加载页面以应用 token
        await page.goto(f"http://localhost{path}", waitUntil='networkidle2')
        
        # 等待页面完全加载（可选：等待特定元素）
        await page.waitForNavigation(waitUntil='networkidle2')
        
        await page.setViewport({'width': 1000, 'height': 1000})

        # 获取.main-container的高度
        height = await page.evaluate('document.querySelector(".content").scrollHeight + 50')
        
        # 设置视口大小
        await page.setViewport({'width': 1000, 'height': int(height)})
        
        # 截图
        screenshot = await page.screenshot({'fullPage': True})
        
        # 发送消息
        await bot.send_group_msg(
            group_id=int(info.group_id),
            message=Message(MessageSegment.image(screenshot))
        )
        
    finally:
        # 确保浏览器被关闭
        if browser:
            await browser.close()
    return {}

class MatchPWPlayerInfo(BaseModel):
    steamId: str = Field(..., description="玩家的 Steam ID")
    nickname: str = Field(..., description="玩家昵称")
    team: int = Field(..., description="玩家所属队伍 (1 或 2)")
    rating: float = Field(..., description="玩家的比赛评分")
    we: float = Field(..., description="玩家的 WE 值")
    kills: int = Field(..., description="击杀数")
    deaths: int = Field(..., description="死亡数")
    assists: int = Field(..., description="助攻数")
    legacyScore: float | None = Field(..., description="玩家的底蕴分数")
    pvpScore: float = Field(..., description="玩家的天梯分数")
    pvpScoreChange: float = Field(..., description="玩家的天梯分数变化")
    pvpStars: int = Field(..., description="玩家的天梯星级")

class MatchPWInfo(BaseModel):
    matchId: str = Field(..., description="比赛 ID")
    timestamp: int = Field(..., description="比赛时间戳")
    season: str = Field(..., description="赛季（Sxx）")
    winTeam: int = Field(..., description="获胜队伍 (1 或 2)")
    mode: str = Field(..., description="比赛模式")
    mapName: str = Field(..., description="比赛地图")
    team1Score: int = Field(..., description="队伍 1 分数")
    team2Score: int = Field(..., description="队伍 2 分数")
    team1LegacyScore: float | None = Field(..., description="队伍 1 底蕴均分")
    team2LegacyScore: float | None = Field(..., description="队伍 2 底蕴均分")
    players: list[MatchPWPlayerInfo] = Field(..., description="参赛玩家信息列表")

class MatchHistoryItem(BaseModel):
    matchId: str = Field(..., description="比赛 ID")
    timeStamp: int = Field(..., description="比赛时间戳")
    season: str = Field(..., description="赛季")
    mode: str = Field(..., description="比赛模式")
    mapName: str = Field(..., description="比赛地图")
    team1Score: int = Field(..., description="队伍 1 分数")
    team2Score: int = Field(..., description="队伍 2 分数")
    team: int = Field(..., description="玩家所在队伍")
    winTeam: int = Field(..., description="获胜队伍")
    rating: float = Field(..., description="玩家评分")
    we: float = Field(..., description="玩家 WE 值")
    pvpScore: float = Field(..., description="天梯分数")
    pvpScoreChange: float = Field(..., description="天梯分数变化")
    legacyDiff: float | None = Field(..., description="底蕴分数变化")

class MatchHistoryResponse(BaseModel):
    totCount: int = Field(..., description="总比赛数")
    pageSize: int = Field(..., description="每页大小")
    matches: list[MatchHistoryItem] = Field(..., description="比赛列表")

@app.post("/api/match/info",
    response_model=MatchPWInfo,
    summary="获取比赛详细信息",
    description="根据比赛 ID 获取比赛的详细信息，包括参赛玩家的数据。需要提供有效的认证 Token。"
)
async def get_match_info(matchId: str = Body(..., embed=True), _ = Depends(get_current_user)):
    match_detail = await db_val.get_match_detail(matchId)
    if not match_detail:
        raise HTTPException(status_code=404, detail="Match not found")
    match_extra = await db_val.get_match_extra(matchId)
    async def get_nickname(player: MatchStatsPW) -> str:
        base_info = await db_val.get_base_info(player.steamid)
        return base_info.name if base_info else "未知玩家"
    async def get_legacy_score(player: MatchStatsPW) -> float | None:
        extra_info = await db_val.get_extra_info(player.steamid, timeStamp=player.timeStamp)
        return extra_info.legacyScore if extra_info else None
    return MatchPWInfo(
        matchId=matchId,
        timestamp=match_detail[0].timeStamp,
        season=match_detail[0].seasonId,
        winTeam=match_detail[0].winTeam,
        mode=match_detail[0].mode,
        mapName=match_detail[0].mapName,
        team1Score=match_detail[0].score1,
        team2Score=match_detail[0].score2,
        team1LegacyScore=match_extra.team1Legacy if match_extra else None,
        team2LegacyScore=match_extra.team2Legacy if match_extra else None,
        players=[
            MatchPWPlayerInfo(
                steamId=player.steamid,
                nickname=await get_nickname(player),
                team=player.team,
                rating=player.pwRating,
                we=player.we,
                kills=player.kill,
                deaths=player.death,
                assists=player.assist,
                legacyScore=await get_legacy_score(player),
                pvpScore=player.pvpScore,
                pvpScoreChange=player.pvpScoreChange,
                pvpStars=player.pvpStars
            ) for player in match_detail
        ]
    )

@app.post("/api/match/history",
    response_model=MatchHistoryResponse,
    summary="获取比赛历史",
    description="根据玩家 Steam ID 获取其比赛历史记录。需要提供有效的认证 Token。"
)
async def get_match_history(
    steamId: str = Body(..., embed=True),
    timeType: str = Body(..., embed=True),
    page: int = Body(..., embed=True, ge=1),
    _ = Depends(get_current_user)):
    # 获取玩家的比赛历史
    match_records = await db_val.get_matches(steamId, timeType, offset=(page - 1) * 20, limit=20)
    total_count = await db_val.get_matches_count(steamId, timeType)
    async def get_legacy_diff(player: MatchStatsPW) -> float | None:
        extra_info = await db_val.get_match_extra(player.mid)
        if not extra_info:
            return None
        if player.team == 1:
            return extra_info.team1Legacy - extra_info.team2Legacy
        else:
            return extra_info.team2Legacy - extra_info.team1Legacy
    if not total_count:
        raise HTTPException(status_code=404, detail="No match history found")
    
    return MatchHistoryResponse(
        totCount=total_count,
        pageSize=20,
        matches=[
            MatchHistoryItem(
                matchId=record.mid,
                timeStamp=record.timeStamp,
                season=record.seasonId,
                mode=record.mode,
                mapName=record.mapName,
                team=record.team,
                winTeam=record.winTeam,
                team1Score=record.score1,
                team2Score=record.score2,
                rating=record.pwRating,
                we=record.we,
                pvpScore=record.pvpScore,
                pvpScoreChange=record.pvpScoreChange,
                legacyDiff=await get_legacy_diff(record)
            ) for record in match_records
        ] if match_records else []
    )

class PlayerBaseResponse(BaseModel):
    nickname: str = Field(..., description="玩家昵称")
    lastUpdate: int = Field(..., description="最后比赛更新时间戳")

@app.post("/api/player/base",
    response_model=PlayerBaseResponse,
    summary="获取玩家基本信息",
    description="根据 Steam ID 获取玩家的基本信息，包括昵称和最后更新时间。"
)
async def get_player_base(steamId: str = Body(..., embed=True), _ = Depends(get_current_user)):
    base_info = await db_val.get_base_info(steamId)
    if not base_info:
        raise HTTPException(status_code=404, detail="Player not found")
    
    return PlayerBaseResponse(
        nickname=base_info.name,
        lastUpdate=base_info.updateMatchTime
    )

class PlayerDetailItem(BaseModel):
    value: float = Field(..., description="数值")
    minValue: float = Field(..., description="最小值")
    maxValue: float = Field(..., description="最大值")
    avgValue: float = Field(..., description="平均值")

# 火力 (FirePower)
class FirePowerDetail(BaseModel):
    score: int = Field(..., description="火力分")
    killsPerRound: PlayerDetailItem = Field(..., description="场均击杀")
    killsPerWinRound: PlayerDetailItem = Field(..., description="胜局场均击杀")
    damagePerRound: PlayerDetailItem = Field(..., description="场均伤害")
    damagePerRoundWin: PlayerDetailItem = Field(..., description="胜局场均伤害")
    roundsWithAKill: PlayerDetailItem = Field(..., description="有击杀的回合占比")
    multiKillRoundsPercentage: PlayerDetailItem = Field(..., description="多杀回合占比")
    we: PlayerDetailItem = Field(..., description="WE")
    pistolRoundRating: PlayerDetailItem = Field(..., description="手枪局Rating")

# 枪法 (Marksmanship)
class MarksmanshipDetail(BaseModel):
    score: int = Field(..., description="枪法分")
    headshotRate: PlayerDetailItem = Field(..., description="爆头率")
    killTime: PlayerDetailItem = Field(..., description="击杀时间")
    smHitRate: PlayerDetailItem = Field(..., description="副武器命中率")
    reactionTime: PlayerDetailItem = Field(..., description="反应时间")
    rapidStopRate: PlayerDetailItem = Field(..., description="急停率")

# 补枪与辅助 (FollowUp)
class FollowUpShotDetail(BaseModel):
    score: int = Field(..., description="补枪分")
    savedTeammatePerRound: PlayerDetailItem = Field(..., description="每回合拯救队友次数")
    tradeKillsPerRound: PlayerDetailItem = Field(..., description="每回合补枪击杀")
    tradeKillsPercentage: PlayerDetailItem = Field(..., description="补枪击杀占比")
    assistKillsPercentage: PlayerDetailItem = Field(..., description="助攻击杀占比")
    damagePerKill: PlayerDetailItem = Field(..., description="每次击杀的伤害")

# 首杀 (First Blood)
class FirstBloodDetail(BaseModel):
    score: int = Field(..., description="首杀分")
    firstHurt: PlayerDetailItem = Field(..., description="首杀伤害")
    winAfterOpeningKill: PlayerDetailItem = Field(..., description="开局杀后胜率")
    firstSuccessRate: PlayerDetailItem = Field(..., description="首杀成功率")
    firstKill: PlayerDetailItem = Field(..., description="首杀数")
    firstRate: PlayerDetailItem = Field(..., description="首杀率")

# 道具 (Item/Utility)
class ItemDetail(BaseModel):
    score: int = Field(..., description="道具分")
    itemRate: PlayerDetailItem = Field(..., description="道具使用率")
    utilityDamagePerRounds: PlayerDetailItem = Field(..., description="每回合道具伤害")
    flashAssistPerRound: PlayerDetailItem = Field(..., description="每回合闪白助攻")
    flashbangFlashRate: PlayerDetailItem = Field(..., description="闪光弹命中率")
    timeOpponentFlashedPerRound: PlayerDetailItem = Field(..., description="每回合敌人被闪白时间")

# 残局 (Clutch / 1vN)
class ClutchDetail(BaseModel):
    score: int = Field(..., description="残局分")
    v1WinPercentage: PlayerDetailItem = Field(..., description="1v1胜率")
    clutchPointsPerRound: PlayerDetailItem = Field(..., description="每回合残局点数")
    lastAlivePercentage: PlayerDetailItem = Field(..., description="最后活着占比")
    timeAlivePerRound: PlayerDetailItem = Field(..., description="每回合存活时间")
    savesPerRoundLoss: PlayerDetailItem = Field(..., description="失败回合保枪率")

# 狙击 (Sniper)
class SniperDetail(BaseModel):
    score: int = Field(..., description="狙击分")
    sniperFirstKillPercentage: PlayerDetailItem = Field(..., description="狙击首杀占比")
    sniperKillsPercentage: PlayerDetailItem = Field(..., description="狙击击杀占比")
    sniperKillPerRound: PlayerDetailItem = Field(..., description="每回合狙击击杀")
    roundsWithSniperKillsPercentage: PlayerDetailItem = Field(..., description="有狙击击杀的回合占比")
    sniperMultipleKillRoundPercentage: PlayerDetailItem = Field(..., description="狙击多杀回合占比")

# 基础评分
class BaseRatingDetail(BaseModel):
    pwRating: PlayerDetailItem = Field(..., description="rating")
    rws: PlayerDetailItem = Field(..., description="RWS")
    pwRatingTAvg: PlayerDetailItem = Field(..., description="T方平均Rating")
    pwRatingCtAvg: PlayerDetailItem = Field(..., description="CT方平均Rating")
    kastPerRound: PlayerDetailItem = Field(..., description="每回合KAST")

class PlayerDetailResponse(BaseModel):
    # 基础综合数据
    seasonId: str = Field(..., description="赛季")
    lastUpdate: int = Field(..., description="最后数据更新时间戳")
    pvpScore: int = Field(..., description="PVP分数")
    pvpStars: int = Field(..., description="PVP星级")
    cnt: int = Field(..., description="比赛场次")
    winRate: PlayerDetailItem = Field(..., description="胜率")
    
    # 基础评分
    baseRating: BaseRatingDetail = Field(..., description="基础评分")
    
    # 各能力分项
    firePower: FirePowerDetail = Field(..., description="火力")
    marksmanship: MarksmanshipDetail = Field(..., description="枪法")
    followUpShot: FollowUpShotDetail = Field(..., description="补枪与辅助")
    firstBlood: FirstBloodDetail = Field(..., description="首杀")
    item: ItemDetail = Field(..., description="道具")
    clutch: ClutchDetail = Field(..., description="残局")
    sniper: SniperDetail = Field(..., description="狙击")

@app.post("/api/player/detail",
    response_model=PlayerDetailResponse,
    summary="获取玩家详细信息",
    description="根据 Steam ID 获取玩家在当前赛季的详细统计信息。"
)
async def get_player_detail(steamId: str = Body(..., embed=True), _ = Depends(get_current_user)):
    from sqlalchemy import select, func
    from ..cs_db_val import SteamDetailInfo
    
    detail_info = await db_val.get_detail_info(steamId)
    base_info = await db_val.get_base_info(steamId)
    if not detail_info:
        raise HTTPException(status_code=404, detail="Player detail info not found")
    assert base_info is not None
    # 获取或更新全局统计数据缓存
    current_time = time.time()
    if detail_info.seasonId in SEASON_STATS_CACHE:
        global_stats, cached_time = SEASON_STATS_CACHE[detail_info.seasonId]
        if current_time - cached_time >= CACHE_EXPIRE_TIME:
            # 缓存已过期，重新计算
            global_stats = await _calculate_global_stats(detail_info.seasonId)
            SEASON_STATS_CACHE[detail_info.seasonId] = (global_stats, current_time)
    else:
        # 无缓存，计算并存储
        global_stats = await _calculate_global_stats(detail_info.seasonId)
        SEASON_STATS_CACHE[detail_info.seasonId] = (global_stats, current_time)
    
    # 使用全局统计数据计算个人的 PlayerDetailItem
    stats_data = _get_player_stats(detail_info, global_stats)
    
    return PlayerDetailResponse(
        seasonId=detail_info.seasonId,
        lastUpdate=base_info.updateTime,
        pvpScore=detail_info.pvpScore,
        pvpStars=detail_info.pvpStars,
        cnt=detail_info.cnt,
        winRate=stats_data['winRate'],
        baseRating=BaseRatingDetail(
            pwRating=stats_data['pwRating'],
            rws=stats_data['rws'],
            pwRatingTAvg=stats_data['pwRatingTAvg'],
            pwRatingCtAvg=stats_data['pwRatingCtAvg'],
            kastPerRound=stats_data['kastPerRound']
        ),
        firePower=FirePowerDetail(
            score=detail_info.firePowerScore,
            killsPerRound=stats_data['killsPerRound'],
            killsPerWinRound=stats_data['killsPerWinRound'],
            damagePerRound=stats_data['damagePerRound'],
            damagePerRoundWin=stats_data['damagePerRoundWin'],
            roundsWithAKill=stats_data['roundsWithAKill'],
            multiKillRoundsPercentage=stats_data['multiKillRoundsPercentage'],
            we=stats_data['we'],
            pistolRoundRating=stats_data['pistolRoundRating']
        ),
        marksmanship=MarksmanshipDetail(
            score=detail_info.marksmanshipScore,
            headshotRate=stats_data['headshotRate'],
            killTime=stats_data['killTime'],
            smHitRate=stats_data['smHitRate'],
            reactionTime=stats_data['reactionTime'],
            rapidStopRate=stats_data['rapidStopRate']
        ),
        followUpShot=FollowUpShotDetail(
            score=detail_info.followUpShotScore,
            savedTeammatePerRound=stats_data['savedTeammatePerRound'],
            tradeKillsPerRound=stats_data['tradeKillsPerRound'],
            tradeKillsPercentage=stats_data['tradeKillsPercentage'],
            assistKillsPercentage=stats_data['assistKillsPercentage'],
            damagePerKill=stats_data['damagePerKill']
        ),
        firstBlood=FirstBloodDetail(
            score=detail_info.firstScore,
            firstHurt=stats_data['firstHurt'],
            winAfterOpeningKill=stats_data['winAfterOpeningKill'],
            firstSuccessRate=stats_data['firstSuccessRate'],
            firstKill=stats_data['firstKill'],
            firstRate=stats_data['firstRate']
        ),
        item=ItemDetail(
            score=detail_info.itemScore,
            itemRate=stats_data['itemRate'],
            utilityDamagePerRounds=stats_data['utilityDamagePerRounds'],
            flashAssistPerRound=stats_data['flashAssistPerRound'],
            flashbangFlashRate=stats_data['flashbangFlashRate'],
            timeOpponentFlashedPerRound=stats_data['timeOpponentFlashedPerRound']
        ),
        clutch=ClutchDetail(
            score=detail_info.oneVnScore,
            v1WinPercentage=stats_data['v1WinPercentage'],
            clutchPointsPerRound=stats_data['clutchPointsPerRound'],
            lastAlivePercentage=stats_data['lastAlivePercentage'],
            timeAlivePerRound=stats_data['timeAlivePerRound'],
            savesPerRoundLoss=stats_data['savesPerRoundLoss']
        ),
        sniper=SniperDetail(
            score=detail_info.sniperScore,
            sniperFirstKillPercentage=stats_data['sniperFirstKillPercentage'],
            sniperKillsPercentage=stats_data['sniperKillsPercentage'],
            sniperKillPerRound=stats_data['sniperKillPerRound'],
            roundsWithSniperKillsPercentage=stats_data['roundsWithSniperKillsPercentage'],
            sniperMultipleKillRoundPercentage=stats_data['sniperMultipleKillRoundPercentage']
        )
    )

async def _calculate_global_stats(seasonId: str) -> dict[str, tuple[float, float, float]]:
    """计算整个赛季的全局统计数据"""
    from sqlalchemy import select
    from ..cs_db_val import SteamDetailInfo
    
    async with async_session_factory() as session:
        stmt = select(SteamDetailInfo).where(SteamDetailInfo.seasonId == seasonId)
        result = await session.execute(stmt)
        all_details = result.scalars().all()
    
    # 定义用于计算统计数据的字段
    float_fields = [
        'winRate', 'pwRating', 'rws', 'pwRatingTAvg', 'pwRatingCtAvg', 'kastPerRound',
        'killsPerRound', 'killsPerWinRound', 'damagePerRound', 'damagePerRoundWin',
        'roundsWithAKill', 'multiKillRoundsPercentage', 'we', 'pistolRoundRating',
        'headshotRate', 'killTime', 'smHitRate', 'reactionTime', 'rapidStopRate',
        'savedTeammatePerRound', 'tradeKillsPerRound', 'tradeKillsPercentage',
        'assistKillsPercentage', 'damagePerKill', 'firstHurt', 'winAfterOpeningKill',
        'firstSuccessRate', 'firstKill', 'firstRate', 'itemRate', 'utilityDamagePerRounds',
        'flashAssistPerRound', 'flashbangFlashRate', 'timeOpponentFlashedPerRound',
        'v1WinPercentage', 'clutchPointsPerRound', 'lastAlivePercentage',
        'timeAlivePerRound', 'savesPerRoundLoss', 'sniperFirstKillPercentage',
        'sniperKillsPercentage', 'sniperKillPerRound', 'roundsWithSniperKillsPercentage',
        'sniperMultipleKillRoundPercentage'
    ]
    
    # 计算全局统计数据：(min, max, avg)
    global_stats: dict[str, tuple[float, float, float]] = {}
    for field in float_fields:
        values = [getattr(d, field) for d in all_details if hasattr(d, field) and getattr(d, field) is not None]
        if values:
            global_stats[field] = (min(values), max(values), sum(values) / len(values))
        else:
            global_stats[field] = (0.0, 0.0, 0.0)
    
    return global_stats

def _get_player_stats(detail_info, global_stats: dict[str, tuple[float, float, float]]) -> dict[str, PlayerDetailItem]:
    """从全局统计数据中获取个人的 PlayerDetailItem"""
    stats_data: dict[str, PlayerDetailItem] = {}
    
    for field, (min_val, max_val, avg_val) in global_stats.items():
        field_value = getattr(detail_info, field, 0.0)
        
        stats_data[field] = PlayerDetailItem(
            value=field_value,
            minValue=min_val,
            maxValue=max_val,
            avgValue=avg_val
        )
    
    return stats_data


class TimeResponse(BaseModel):
    timeTypes: list[str] = Field(..., description="支持的时间范围类型列表")

@app.post("/api/config/time",
    response_model=TimeResponse,
    summary="获取支持的时间范围类型",
    description="获取所有支持的时间范围类型列表。"
)
async def get_time_types(_ = Depends(get_current_user)):
    return TimeResponse(
        timeTypes=valid_time
    )

class RankConfigItem(BaseModel):
    name: str = Field(..., description="排名选项名称")
    description: str = Field(..., description="排名选项描述")
    defaultTimeType: str = Field(..., description="默认时间范围类型")
    allowedTimeTypes: list[str] = Field(..., description="允许的时间范围类型列表")
    outputFormat: str = Field(..., description="输出格式")

class RankConfigResponse(BaseModel):
    rankOptions: list[RankConfigItem] = Field(..., description="排名选项列表")

@app.post("/api/config/rank",
    response_model=RankConfigResponse,
    summary="获取排名配置",
    description="获取可用的排名选项及其配置。"
)
async def get_rank_config(_ = Depends(get_current_user)):
    rank_options = []
    for name, config in db_val._registry.items():
        rank_options.append(RankConfigItem(
            name=name,
            description=config.title,
            defaultTimeType=config.default_time,
            allowedTimeTypes=config.allowed_time,
            outputFormat=config.outputfmt
        ))
    return RankConfigResponse(
        rankOptions=rank_options
    )

class UserQQItem(BaseModel):
    qq: str = Field(..., description="玩家的 QQ 号")
    qqNickname: str = Field(..., description="玩家的 QQ 昵称")
    steamId: str = Field(..., description="玩家的 Steam ID")
    nickname: str = Field(..., description="玩家昵称")

class UserResponse(BaseModel):
    users: list[UserQQItem] = Field(..., description="绑定用户列表")
@app.post("/api/config/users",
    response_model=UserResponse,
    summary="获取绑定用户列表",
    description="获取当前认证 Token 所在群组的绑定用户列表。"
)
async def get_bound_users(info: AuthSession = Depends(get_current_user)):
    assert info.group_id is not None
    qqs = await db_val.get_group_member(info.group_id)
    users: list[UserQQItem] = [] 
    for qq in qqs:
        if steamid := await db_val.get_steamid(qq):
            base_info = await db_val.get_base_info(steamid)
            if base_info is not None:
                users.append(UserQQItem(
                    qq=qq,
                    qqNickname=await get_user_name(qq),
                    steamId=steamid,
                    nickname=base_info.name
                ))
    return UserResponse(
        users=users
    )

class RankItem(BaseModel):
    steamId: str = Field(..., description="玩家的 Steam ID")
    nickname: str = Field(..., description="玩家昵称")
    value: float = Field(..., description="值")
    count: int = Field(..., description="场次")

class RankResponse(BaseModel):
    minValue: float = Field(..., description="最小值")
    maxValue: float = Field(..., description="最大值")
    players: list[RankItem] = Field(..., description="排名玩家列表")

@app.post("/api/rank",
    response_model=RankResponse,
    summary="获取排名列表",
    description="根据排名名称和时间范围类型获取排名列表。"
)
async def get_rank_list(
    rankName: str = Body(..., embed=True),
    timeType: str = Body(..., embed=True),
    info = Depends(get_current_user)):
    rank_config = db_val.get_value_config(rankName)
    if not rank_config:
        raise HTTPException(status_code=400, detail="Invalid rank name")
    if timeType not in rank_config.allowed_time:
        raise HTTPException(status_code=400, detail="Invalid time type")
    
    async def get_nickname(steamId: str) -> str:
        base_info = await db_val.get_base_info(steamId)
        return base_info.name if base_info else "未知玩家"
    steamids = await db_val.get_group_member_steamid(info.group_id)
    datas = []
    for steamid in steamids:
        try:
            val = await rank_config.func(steamid, timeType)
            print(val)
            datas.append((steamid, val))
        except NoValueError:
            pass
    print(datas)
    datas = sorted(datas, key=lambda x: x[1][0], reverse=rank_config.reversed)
    if len(datas) == 0:
        raise HTTPException(status_code=404, detail="No ranking data found")
    max_value = datas[0][1][0] if rank_config.reversed else datas[-1][1][0]
    min_value = datas[-1][1][0] if rank_config.reversed else datas[0][1][0]
    min_value, max_value = rank_config.range_gen.getval(min_value, max_value)
    return RankResponse(
        minValue=min_value,
        maxValue=max_value,
        players=[
            RankItem(
                steamId=steamid,
                nickname=await get_nickname(steamid),
                value=val[0],
                count=val[1]
            ) for steamid, val in datas
        ]
    )