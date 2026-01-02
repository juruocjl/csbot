from nonebot import get_plugin_config
from nonebot import get_app
from nonebot import on_command
from nonebot.adapters.onebot.v11 import Message, MessageEvent, GroupMessageEvent
from nonebot.params import CommandArg
from nonebot.plugin import PluginMetadata
from nonebot import require

import secrets
import time
from fastapi import FastAPI, Body, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from sqlalchemy import String, Integer, Boolean, select
from sqlalchemy.orm import Mapped, mapped_column
from pydantic import BaseModel, Field

require("utils")
from ..utils import Base, async_session_factory
require("cs_db_val")
from ..cs_db_val import MatchStatsPW
from ..cs_db_val import db as db_val

from .config import Config

__plugin_meta__ = PluginMetadata(
    name="cs_server",
    description="",
    usage="",
    config=Config,
)

security = HTTPBearer()

config = get_plugin_config(Config)

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

class DataMannager:
    async def generate_token(self) -> AuthSession:
        token = secrets.token_hex(32)
        code = str(secrets.randbelow(900000) + 100000)  # 生成6位验证码
        async with async_session_factory() as session:
            auth_session = AuthSession(
                token=token,
                code=code,
                user_id=None,
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

db = DataMannager()

verify = on_command("验证", aliases={"verify"}, priority=10)

@verify.handle()
async def handle_verify(event: GroupMessageEvent, args: Message = CommandArg()):
    code = args.extract_plain_text().strip()
    if not code:
        await verify.finish("请提供验证码，例如：verify 123456")
    user_id = str(event.get_user_id())
    group_id = str(event.get_group_id())
    success = await db.verify_code(code, user_id, group_id)
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

class BaseInfoResponse(BaseModel):
    verify: bool = Field(..., description="Token 是否有效")
    user_id: str = Field(..., description="绑定的用户 ID")
    group_id: str = Field(..., description="绑定的群 ID")

@app.post(
    "/api/auth/info",
    response_model=BaseInfoResponse,
    summary="获取认证信息",
    description="验证 Token 并获取绑定的用户 ID。"
)
async def verify_token(info = Depends(get_current_user)):
    return BaseInfoResponse(
        verify=True,
        user_id=info.user_id,
        group_id=info.group_id
    )

class MatchPWPlayerInfo(BaseModel):
    steamId: str = Field(..., description="玩家的 Steam ID")
    nickname: str = Field(..., description="玩家昵称")
    team: int = Field(..., description="玩家所属队伍 (1 或 2)")
    rating: float = Field(..., description="玩家的比赛评分")
    we: float = Field(..., description="玩家的 WE 值")
    kills: int = Field(..., description="击杀数")
    deaths: int = Field(..., description="死亡数")
    assists: int = Field(..., description="助攻数")
    legasyscore: float = Field(..., description="玩家的底蕴分数")
    pvpscore: float = Field(..., description="玩家的天梯分数")
    pvpstar: int = Field(..., description="玩家的天梯星级")

class MatchPWInfo(BaseModel):
    match_id: str = Field(..., description="比赛 ID")
    timestamp: int = Field(..., description="比赛时间戳")
    season: str = Field(..., description="赛季（Sxx）")
    winteam: int = Field(..., description="获胜队伍 (1 或 2)")
    mode: str = Field(..., description="比赛模式")
    mapname: str = Field(..., description="比赛地图")
    score1: int = Field(..., description="队伍 1 分数")
    score2: int = Field(..., description="队伍 2 分数")
    legasyscore1: float = Field(..., description="队伍 1 底蕴均分")
    legasyscore2: float = Field(..., description="队伍 2 底蕴均分")
    players: list[MatchPWPlayerInfo] = Field(..., description="参赛玩家信息列表")

@app.post("/api/data/match_info",
    response_model=MatchPWInfo,
    summary="获取比赛详细信息",
    description="根据比赛 ID 获取比赛的详细信息，包括参赛玩家的数据。需要提供有效的认证 Token。"
)
async def get_match_info(match_id: str = Body(..., embed=True), _ = Depends(get_current_user)):
    match_detail = await db_val.get_match_detail(match_id)
    if not match_detail:
        raise HTTPException(status_code=404, detail="Match not found")
    match_extra = await db_val.get_match_extra(match_id)
    assert match_extra is not None
    async def get_nickname(player: MatchStatsPW) -> str:
        base_info = await db_val.get_base_info(player.steamid)
        return base_info.name if base_info else "未知玩家"
    async def get_legacy_score(player: MatchStatsPW) -> float:
        extra_info = await db_val.get_extra_info(player.steamid, timeStamp=player.timeStamp)
        return extra_info.legacyScore if extra_info else float('nan')
    return MatchPWInfo(
        match_id=match_id,
        timestamp=match_detail[0].timeStamp,
        season=match_detail[0].seasonId,
        winteam=match_detail[0].winTeam,
        mode=match_detail[0].mode,
        mapname=match_detail[0].mapName,
        score1=match_detail[0].score1,
        score2=match_detail[0].score2,
        legasyscore1=match_extra.team1Legacy,
        legasyscore2=match_extra.team2Legacy,
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
                legasyscore=await get_legacy_score(player),
                pvpscore=player.pvpScore,
                pvpstar=player.pvpStars
            ) for player in match_detail
        ]
    )
    