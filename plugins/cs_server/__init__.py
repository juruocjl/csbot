from nonebot import get_plugin_config
from nonebot import get_app
from nonebot import on_command
from nonebot.adapters.onebot.v11 import Message, MessageEvent
from nonebot.params import CommandArg
from nonebot.plugin import PluginMetadata
from nonebot import require

import secrets
import time
from fastapi import FastAPI, Header, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from sqlalchemy import String, Integer, Boolean, select
from sqlalchemy.orm import Mapped, mapped_column

require("utils")
from ..utils import Base, async_session_factory

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
    # 绑定的 QQ 号 (验证前为空)
    user_id: Mapped[str | None] = mapped_column(String, nullable=True)
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
    
    async def verify_code(self, code: str, user_id: str) -> bool:
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
                    auth_session.is_verified = True
                    auth_session.last_used_at = int(time.time())
                    await session.merge(auth_session)
                    return True
                return False
    
    async def get_verified_user(self, token: str) -> str | None:
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
                    return auth_session.user_id
                return None

db = DataMannager()

verify = on_command("验证", aliases={"verify"}, priority=10)

@verify.handle()
async def handle_verify(event: MessageEvent, args: Message = CommandArg()):
    code = args.extract_plain_text().strip()
    if not code:
        await verify.finish("请提供验证码，例如：verify 123456")
    user_id = str(event.get_user_id())
    success = await db.verify_code(code, user_id)
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
    user_id = await db.get_verified_user(token)
    if not user_id:
        raise HTTPException(status_code=401, detail="Invalid token")
    return user_id

@app.post("/api/auth/init")
async def init_token():
    """
    申请token接口
    返回: {"token": "...", "code": "123456"}
    """
    auth_session = await db.generate_token()
    return {
        "token": auth_session.token,
        "code": auth_session.code,
        "expires_in": config.auth_code_valid_seconds
    }

@app.post("/api/auth/info")
async def verify_token(user_id = Depends(get_current_user)):
    """
    验证token接口
    参数: token (HTTP Bearer 认证)
    返回: {"verified": true, "user_id": "..."}
    """
    return {
        "verified": True,
        "user_id": user_id
    }
