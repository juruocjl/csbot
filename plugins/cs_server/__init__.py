from nonebot import get_driver, get_plugin_config
from nonebot import get_app, get_bot
from nonebot import on_command
from nonebot import logger
from nonebot.adapters.onebot.v11 import Message, MessageSegment, GroupMessageEvent, Bot
from nonebot.params import CommandArg
from nonebot.plugin import PluginMetadata
from nonebot import require
from nonebot.exception import ActionFailed
import secrets
import time
import re
import json
import math
import os
import aiohttp
import psutil
import asyncio
import uuid
from pathlib import Path
from fastapi import FastAPI, Body, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError
from pydantic import BaseModel, Field
from pyppeteer import launch
from ..major_hw.playoff_homework import (
    PLAYOFF_CATEGORIES,
    PLAYOFF_CATEGORY_SLOTS,
    PLAYOFF_STATUS_LABELS,
    playoff_category_status,
)

require("nonebot_plugin_apscheduler")
from nonebot_plugin_apscheduler import scheduler

require("utils")
from ..utils import async_session_factory, local_storage, get_session
require("models")
from ..models import AuthSession, GroupMember, MajorHWSnapshot, MemberSteamID, SteamBaseInfo, SteamExtraInfo, UserInfo
from ..models import MatchStatsPW, MatchStatsGP, MatchStatsFaceit
require("cs_db_val")
from ..cs_db_val import db as db_val
from ..cs_db_val import valid_time, gp_time, NoValueError
from ..cs_db_val import SteamDetailInfo
require("cs_db_upd")
from ..cs_db_upd import db as db_upd
from ..cs_db_upd import TooFrequentError, LockingError
require("cs_ai")
from ..cs_ai import db as db_ai
from ..cs_ai import ai_ask_main

require("major_hw")
from ..major_hw import config as major_hw_config
from ..major_hw import db as db_major_hw
from ..major_hw import get_name as get_major_team_name
from ..major_hw import major_teams, major_stage_name

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
if not config.mute_api_token:
    logger.warning("Mute API disabled: MUTE_API_TOKEN not configured")


def _env_flag(name: str) -> bool:
    return os.getenv(name, "").strip().lower() in {"1", "true", "yes", "on"}


def _background_jobs_disabled() -> bool:
    return _env_flag("CS_DISABLE_BACKGROUND_JOBS")


def _watch_stage_profile_refresh_enabled() -> bool:
    return _env_flag("CS_WATCH_STAGE_ENABLE_PROFILE_REFRESH")

# 全局缓存配置
from typing import Any
from sqlalchemy import select
SEASON_STATS_CACHE: dict[str, dict[str, tuple[float, float, float]]] = {}  # 格式: {seasonId: global_stats}
# global_stats 格式: {field_name: (min, max, avg)}

async def _calculate_global_stats(seasonId: str) -> dict[str, tuple[float, float, float]]:
    """计算整个赛季的全局统计数据"""
    
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
    
    # 计算全局统计数据：(min, max, weighted_avg)
    global_stats: dict[str, tuple[float, float, float]] = {}
    for field in float_fields:
        # 获取 (数值, 场次权重) 元组列表
        data_pairs = [
            (getattr(d, field), d.cnt) 
            for d in all_details 
            if hasattr(d, field) and getattr(d, field) is not None
        ]
        
        if data_pairs:
            values = [p[0] for p in data_pairs]
            weights = [p[1] for p in data_pairs]
            total_weight = sum(weights)
            
            # 使用带权平均数：sum(值 * 权重) / sum(权重)
            avg_val = sum(v * w for v, w in data_pairs) / total_weight if total_weight > 0 else sum(values) / len(values)
            global_stats[field] = (min(values), max(values), avg_val)
        else:
            global_stats[field] = (0.0, 0.0, 0.0)
    
    return global_stats

@scheduler.scheduled_job("interval", minutes=30, id="update_season_stats_cache")
async def update_season_stats_cache():
    """定时更新赛季统计缓存"""
    if _background_jobs_disabled():
        logger.info("skip season stats cache job: background jobs disabled")
        return
    global SEASON_STATS_CACHE
    for season_id in [config.cs_season_id, ]:
        SEASON_STATS_CACHE[season_id] = await _calculate_global_stats(season_id)

driver = get_driver()

@driver.on_startup
async def _():
    if _background_jobs_disabled():
        logger.info("skip season stats cache warmup: background jobs disabled")
        return
    if os.getenv("CS_SERVER_SKIP_STARTUP_CACHE") == "1":
        logger.info("skip season stats cache warmup")
        return
    await update_season_stats_cache()

class DataMannager:
    async def generate_token(self) -> AuthSession:
        token = secrets.token_hex(32)
        code = str(secrets.randbelow(900000) + 100000)  # 生成6位验证码
        async with async_session_factory() as session:
            async with session.begin():
                auth_session = AuthSession(
                    token=token,
                    code=code,
                    user_id=None,
                    group_id=None,
                    created_at=int(time.time()),
                    is_verified=False,
                )
                session.add(auth_session)
                return auth_session
    
    async def get_bot_token(self, gid: str) -> str:
        async with async_session_factory() as session:
            async with session.begin():
                stmt = (
                    select(AuthSession)
                    .where(AuthSession.group_id == gid)
                    .where(AuthSession.user_id == str(config.cs_botid))
                    .where(AuthSession.is_verified == True)
                )
                result = await session.execute(stmt)
                auth_session = result.scalar_one_or_none()
                if auth_session:
                    return auth_session.token
                new_session = AuthSession(
                    token=secrets.token_hex(32),
                    code="000000",
                    user_id=str(config.cs_botid),
                    group_id=gid,
                    created_at=int(time.time()),
                    last_used_at=int(time.time()),
                    is_verified=True,
                )
                session.add(new_session)
                return new_session.token

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
    username = user.nickname if user else "未知用户"
    try:
        bot = get_bot()
        assert isinstance(bot, Bot)
        username = (await bot.get_stranger_info(user_id=int(uid), no_cache=True))["nickname"]
    except:
        pass
    await db.set_user_name(uid, username)
    result = await db.get_user(uid)
    return result.nickname if result else username

LOCAL_URL = os.getenv("CS_SCREENSHOT_BASE_URL") or f"http://localhost:{os.getenv('PORT', '1234')}"

async def get_screenshot(path: str, token: str, width:int = 1000) -> bytes | None:
    browser = None
    screenshot = None
    try:
        # 启动浏览器
        browser = await launch(headless=True, args=['--no-sandbox'])
        page = await browser.newPage()
        
        # 使用 cookie 设置 token（替代 localStorage）
        await page.setCookie({'name': 'token', 'value': token, 'url': f'{LOCAL_URL}', 'path': '/', 'httpOnly': False, 'secure': False})

        # 访问目标页面并等待网络空闲
        final_path = path
        if "hideSidebar=" not in final_path:
            final_path = f"{final_path}&hideSidebar=True" if "?" in final_path else f"{final_path}?hideSidebar=True"
        await page.goto(f"{LOCAL_URL}{final_path}", waitUntil='networkidle0')

        await asyncio.sleep(0.2)
        
        await page.setViewport({'width': width, 'height': 100})

        # 获取.main-container的高度
        height = await page.evaluate('document.querySelector(".content").scrollHeight + 50')
        
        # 设置视口大小
        await page.setViewport({'width': width, 'height': int(height)})

        # Trigger lazy images and wait for them before taking a full-page screenshot.
        await page.evaluate("""async () => {
            const wait = (ms) => new Promise(resolve => setTimeout(resolve, ms));
            const imgs = Array.from(document.images);
            imgs.forEach(img => {
                img.loading = 'eager';
                img.decoding = 'sync';
            });
            window.scrollTo(0, document.body.scrollHeight);
            await wait(200);
            window.scrollTo(0, 0);
            await Promise.race([
                Promise.all(imgs.map(img => {
                    if (img.complete) {
                        return Promise.resolve();
                    }
                    return new Promise(resolve => {
                        img.addEventListener('load', resolve, { once: true });
                        img.addEventListener('error', resolve, { once: true });
                    });
                })),
                wait(5000),
            ]);
        }""")
        
        # 截图
        screenshot = await page.screenshot({'fullPage': True})
            
    finally:
        # 确保浏览器被关闭
        if browser:
            await browser.close()
    return screenshot

async def get_match_user_team(players: list[tuple[str, int]]) -> int | None:
    team1_users = False
    team2_users = False
    for steamid, team in players:
        is_user = await db_val.is_user(steamid)
        if is_user:
            if team == 1:
                team1_users = True
            elif team == 2:
                team2_users = True
    if team1_users and not team2_users:
        return 1
    elif team2_users and not team1_users:
        return 2
    else:
        return None

async def get_nickname(steamid: str) -> str:
    base_info = await db_val.get_base_info(steamid)
    return base_info.name if base_info else "未知玩家"

async def get_legacy_score(steamid: str, timeStamp: int) -> float | None:
    extra_info = await db_val.get_extra_info(steamid, timeStamp=timeStamp)
    return extra_info.legacyScore if extra_info else None

def is_ladder_mode(mode: str) -> bool:
    return mode.startswith("天梯") or mode == "PVP周末联赛"

def previous_season_id(season: str) -> str | None:
    match = re.fullmatch(r"S(\d+)", season)
    if not match:
        return None
    season_num = int(match.group(1))
    if season_num <= 1:
        return None
    return f"S{season_num - 1}"

def predict_reset_hidden_score(prev_end_score: int) -> float:
    if prev_end_score < 1600:
        return 0.504089 * prev_end_score + 594.34
    if prev_end_score < 2000:
        return 0.099434 * prev_end_score + 1351.14
    return 0.119768 * prev_end_score + 1584.41

async def get_display_pvp_score(player: MatchStatsPW) -> tuple[int | None, bool]:
    if player.pvpScore > 0:
        return int(player.pvpScore), False
    if not is_ladder_mode(player.mode):
        return None, False

    mode_filter = (MatchStatsPW.mode.like("天梯%")) | (MatchStatsPW.mode == "PVP周末联赛")
    async with async_session_factory() as session:
        season_stmt = (
            select(
                MatchStatsPW.mid,
                MatchStatsPW.pvpScore,
                MatchStatsPW.pvpScoreChange,
                MatchStatsPW.timeStamp,
            )
            .where(MatchStatsPW.steamid == player.steamid)
            .where(MatchStatsPW.seasonId == player.seasonId)
            .where(mode_filter)
            .order_by(MatchStatsPW.timeStamp.asc(), MatchStatsPW.mid.asc())
        )
        season_rows = list((await session.execute(season_stmt)).all())
        current_index = next(
            (
                index for index, row in enumerate(season_rows)
                if row.mid == player.mid and int(row.timeStamp) == int(player.timeStamp)
            ),
            None,
        )
        if current_index is None:
            return None, False

        visible_index = next(
            (index for index, row in enumerate(season_rows) if int(row.pvpScore or 0) > 0),
            None,
        )
        if visible_index is not None:
            hidden_start = int(season_rows[visible_index].pvpScore) - sum(
                int(row.pvpScoreChange or 0) for row in season_rows[: visible_index + 1]
            )
        else:
            prev_season = previous_season_id(player.seasonId)
            if not prev_season:
                return None, False
            prev_stmt = (
                select(MatchStatsPW.pvpScore)
                .where(MatchStatsPW.steamid == player.steamid)
                .where(MatchStatsPW.seasonId == prev_season)
                .where(mode_filter)
                .where(MatchStatsPW.pvpScore > 0)
                .order_by(MatchStatsPW.timeStamp.desc(), MatchStatsPW.mid.desc())
                .limit(1)
            )
            prev_end_score = (await session.execute(prev_stmt)).scalar_one_or_none()
            if not prev_end_score:
                return None, False
            hidden_start = predict_reset_hidden_score(int(prev_end_score))

        display_score = hidden_start + sum(
            int(row.pvpScoreChange or 0) for row in season_rows[: current_index + 1]
        )
        return max(1, int(round(display_score))), True


class WatchStageLivePlayer(BaseModel):
    steamId: str = Field(..., description="Steam ID")
    side: str = Field(..., description="CT/TERRORIST")
    kill: int = Field(0, description="本场击杀")
    death: int = Field(0, description="本场死亡")
    assist: int = Field(0, description="本场助攻")
    score: int = Field(0, description="本场分数")
    adr: float = Field(0.0, description="本场 ADR")
    headshot: str | None = Field(None, description="本场爆头率")
    alive: bool = Field(False, description="是否存活")


class WatchStagePlayerProfile(BaseModel):
    steamId: str = Field(..., description="Steam ID")
    nickname: str | None = Field(None, description="昵称")
    avatar: str | None = Field(None, description="头像")
    pvpScore: int | None = Field(None, description="完美天梯分")
    pvpStars: int | None = Field(None, description="完美天梯星数")
    legacyScore: float | None = Field(None, description="底蕴分")
    avgRt: float | None = Field(None, description="平均 rating")
    avgWe: float | None = Field(None, description="平均 WE")
    faceitElo: int | None = Field(None, description="FACEIT ELO")
    faceitLevel: int | None = Field(None, description="FACEIT Level")
    status: str = Field("missing", description="missing/updating/limited/ready/failed")
    message: str | None = Field(None, description="补充状态")
    updatedAt: int | None = Field(None, description="资料更新时间")


class WatchStageSnapshot(BaseModel):
    status: str = Field(..., description="pending/running/no_active_match/closed")
    requestedSteamId: str = Field(..., description="查询的 Steam ID")
    connectionId: str | None = Field(None, description="后端 WS 连接 ID")
    message: str | None = Field(None, description="状态说明")
    matchId: str | None = Field(None, description="比赛 ID")
    map: str | None = Field(None, description="地图")
    ctScore: int | None = Field(None, description="CT 分数")
    terroristScore: int | None = Field(None, description="T 分数")
    duration: str | None = Field(None, description="比赛时长")
    warmUpStatus: bool | None = Field(None, description="是否热身")
    updatedAt: int | None = Field(None, description="快照更新时间")
    players: list[WatchStageLivePlayer] = Field(default_factory=list, description="本场实时数据")
    profiles: dict[str, WatchStagePlayerProfile] = Field(default_factory=dict, description="玩家基础数据")


class WatchStageConnection:
    def __init__(self, manager: "WatchStageManager", connection_id: str, seed_steam_id: str, platform: str, websocket_url: str):
        self.manager = manager
        self.id = connection_id
        self.seed_steam_id = seed_steam_id
        self.platform = platform
        self.websocket_url = websocket_url
        self.player_ids: set[str] = {seed_steam_id}
        self.match_id: str | None = None
        self.created_at = int(time.time())
        self.last_message_at = self.created_at
        self.closed = False
        self.task: asyncio.Task | None = None
        self.ws: aiohttp.ClientWebSocketResponse | None = None
        self.first_update = asyncio.Event()
        self.no_data_since: int | None = None
        self.close_status = "closed"
        self.close_message = "观将台连接已关闭"

    async def start(self) -> None:
        self.task = asyncio.create_task(self.run())

    async def close(self) -> None:
        self.closed = True
        if self.ws and not self.ws.closed:
            await self.ws.close()

    async def subscribe(self) -> None:
        if self.ws is None or self.ws.closed:
            return
        await self.ws.send_str(json.dumps({
            "messageType": 10001,
            "messageData": {"steam_id": self.seed_steam_id}
        }))

    async def run(self) -> None:
        headers = {
            "Origin": "https://news.wmpvp.com",
            "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 18_7 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148 EsportsApp Version=4.1.0",
        }
        try:
            async with get_session().ws_connect(self.websocket_url, headers=headers, heartbeat=30) as ws:
                self.ws = ws
                await ws.send_str("ping")
                while not self.closed:
                    try:
                        msg = await ws.receive(timeout=60)
                    except asyncio.TimeoutError:
                        logger.info(f"watch stage ws timeout connection={self.id}")
                        break

                    if msg.type in (aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                        break

                    if msg.type != aiohttp.WSMsgType.TEXT:
                        continue

                    self.last_message_at = int(time.time())
                    text = msg.data
                    if text == "pong":
                        await self.subscribe()
                        continue

                    try:
                        payload = json.loads(text)
                    except json.JSONDecodeError:
                        continue

                    message_type = payload.get("messageType")
                    if message_type == 10002 and isinstance(payload.get("messageData"), dict):
                        self.no_data_since = None
                        await self.manager.handle_match_data(self, payload["messageData"])
                        self.first_update.set()
                    elif message_type == 10003:
                        self.close_status = "no_active_match"
                        self.close_message = "该玩家当前没有可展示的观将台比赛"
                        await self.manager.handle_no_active_match(self)
                        self.first_update.set()
                        break
        except Exception:
            logger.exception(f"watch stage ws failed connection={self.id} seed={self.seed_steam_id}")
        finally:
            await self.manager.drop_connection(self.id)


class WatchStageManager:
    def __init__(self):
        self.lock = asyncio.Lock()
        self.player_to_connection: dict[str, str] = {}
        self.connections: dict[str, WatchStageConnection] = {}
        self.latest_snapshots: dict[str, WatchStageSnapshot] = {}
        self.profile_cache: dict[str, WatchStagePlayerProfile] = {}
        self.external_profiles: dict[str, dict[str, Any]] = {}
        self.profile_update_tasks: set[str] = set()
        self.profile_retry_after: dict[str, int] = {}
        self.profile_update_lock = asyncio.Lock()

    async def subscribe(self, steam_id: str) -> WatchStageSnapshot:
        if not re.match(r"^\d{17}$", steam_id):
            raise HTTPException(status_code=400, detail="Invalid Steam ID")

        first_no_active: WatchStageSnapshot | None = None
        for platform in ("2", "1"):
            snapshot = await self._subscribe_platform(steam_id, platform)
            if snapshot.status != "no_active_match":
                return snapshot
            first_no_active = first_no_active or snapshot
        return first_no_active or WatchStageSnapshot(
            status="no_active_match",
            requestedSteamId=steam_id,
            message="该玩家当前没有可展示的观将台比赛",
        )

    async def _subscribe_platform(self, steam_id: str, platform: str) -> WatchStageSnapshot:
        async with self.lock:
            existing = self._get_connection_for_player(steam_id)
            if existing:
                if existing.platform != platform:
                    await existing.close()
                else:
                    snapshot = self._snapshot_for_player(steam_id)
                    if snapshot:
                        return snapshot
                    return self._pending_snapshot(steam_id, existing.id)

            snapshot = self.latest_snapshots.get(steam_id)
            if snapshot and snapshot.status == "running":
                return self._snapshot_for_player(steam_id) or snapshot
            if snapshot and snapshot.status == "no_active_match" and getattr(snapshot, "platform", None) == platform:
                return snapshot

            websocket_url = await self._get_websocket_url(steam_id, platform)
            connection_id = uuid.uuid4().hex
            connection = WatchStageConnection(self, connection_id, steam_id, platform, websocket_url)
            self.connections[connection_id] = connection
            self.player_to_connection[steam_id] = connection_id
            await connection.start()

        try:
            await asyncio.wait_for(connection.first_update.wait(), timeout=3)
        except asyncio.TimeoutError:
            pass

        return self._snapshot_for_player(steam_id) or self._pending_snapshot(steam_id, connection_id)

    async def snapshot(self, steam_id: str) -> WatchStageSnapshot:
        async with self.lock:
            connection = self._get_connection_for_player(steam_id)
            if connection:
                snapshot = self._snapshot_for_player(steam_id)
                if snapshot:
                    return snapshot
                return self._pending_snapshot(steam_id, connection.id)
            snapshot = self.latest_snapshots.get(steam_id)
            if snapshot and snapshot.updatedAt and int(time.time()) - snapshot.updatedAt < 30:
                return snapshot
        return WatchStageSnapshot(
            status="closed",
            requestedSteamId=steam_id,
            message="观将台连接未建立或已关闭",
        )

    async def handle_match_data(self, connection: WatchStageConnection, match_data: dict[str, Any]) -> None:
        player_rows = match_data.get("playerList") if isinstance(match_data.get("playerList"), list) else []
        player_ids = {str(player.get("steamId")) for player in player_rows if player.get("steamId")}
        if not player_ids:
            return

        connection.player_ids = player_ids
        connection.match_id = str(match_data.get("matchId") or "")
        stats_profiles = await self._fetch_team_statistics(match_data, connection.platform)
        now = int(time.time())

        players = [
            WatchStageLivePlayer(
                steamId=str(player.get("steamId")),
                side=str(player.get("side") or ""),
                kill=int(player.get("kill") or 0),
                death=int(player.get("death") or 0),
                assist=int(player.get("assist") or 0),
                score=int(player.get("score") or 0),
                adr=float(player.get("adr") or 0),
                headshot=player.get("headshot"),
                alive=bool(player.get("alive")),
            )
            for player in player_rows
            if player.get("steamId")
        ]

        for steam_id, external in stats_profiles.items():
            self.external_profiles[steam_id] = external

        profiles = await self._profiles_for_players(player_ids)
        for steam_id in player_ids:
            self._ensure_profile_update(steam_id)

        snapshot = WatchStageSnapshot(
            status="running",
            requestedSteamId=connection.seed_steam_id,
            connectionId=connection.id,
            matchId=str(match_data.get("matchId") or ""),
            map=match_data.get("map"),
            ctScore=self._optional_int(match_data.get("ctScore")),
            terroristScore=self._optional_int(match_data.get("terroristScore")),
            duration=str(match_data.get("duration")) if match_data.get("duration") is not None else None,
            warmUpStatus=bool(match_data.get("warmUpStatus")),
            updatedAt=now,
            players=players,
            profiles=profiles,
        )

        async with self.lock:
            duplicate_connections = {
                existing_id
                for steam_id in player_ids
                for existing_id in [self.player_to_connection.get(steam_id)]
                if existing_id and existing_id != connection.id
            }
            for existing_id in duplicate_connections:
                existing = self.connections.get(existing_id)
                if existing:
                    await existing.close()
            self.connections[connection.id] = connection
            for steam_id in player_ids:
                self.player_to_connection[steam_id] = connection.id
                self.latest_snapshots[steam_id] = snapshot.copy(update={"requestedSteamId": steam_id})

    async def handle_no_active_match(self, connection: WatchStageConnection) -> None:
        async with self.lock:
            self.latest_snapshots[connection.seed_steam_id] = WatchStageSnapshot(
                status="no_active_match",
                requestedSteamId=connection.seed_steam_id,
                connectionId=connection.id,
                message="该玩家当前没有可展示的观将台比赛",
            )

    async def drop_connection(self, connection_id: str) -> None:
        async with self.lock:
            connection = self.connections.pop(connection_id, None)
            if not connection:
                return
            connection.closed = True
            for steam_id, existing_id in list(self.player_to_connection.items()):
                if existing_id == connection_id:
                    self.player_to_connection.pop(steam_id, None)
                    snapshot = self.latest_snapshots.get(steam_id)
                    if snapshot:
                        self.latest_snapshots[steam_id] = snapshot.copy(update={
                            "status": connection.close_status,
                            "connectionId": None,
                            "message": connection.close_message,
                        })

    def _get_connection_for_player(self, steam_id: str) -> WatchStageConnection | None:
        connection_id = self.player_to_connection.get(steam_id)
        if not connection_id:
            return None
        connection = self.connections.get(connection_id)
        if not connection or connection.closed:
            self.player_to_connection.pop(steam_id, None)
            return None
        return connection

    def _snapshot_for_player(self, steam_id: str) -> WatchStageSnapshot | None:
        snapshot = self.latest_snapshots.get(steam_id)
        if not snapshot:
            return None
        return snapshot.copy(update={
            "requestedSteamId": steam_id,
            "profiles": {
                player.steamId: self.profile_cache.get(player.steamId)
                or WatchStagePlayerProfile(
                    steamId=player.steamId,
                    status="updating" if player.steamId in self.profile_update_tasks else "missing",
                )
                for player in snapshot.players
            }
        })

    def _pending_snapshot(self, steam_id: str, connection_id: str) -> WatchStageSnapshot:
        return WatchStageSnapshot(
            status="pending",
            requestedSteamId=steam_id,
            connectionId=connection_id,
            message="正在连接观将台",
        )

    async def _get_websocket_url(self, steam_id: str, platform: str) -> str:
        params = {"steamId": steam_id, "platform": platform}
        headers = {
            "Origin": "https://news.wmpvp.com",
            "Referer": "https://news.wmpvp.com/",
            "X-Requested-With": "XMLHttpRequest",
            "platform": "h5_web",
            "appversion": "4.1.0",
            "appTheme": "0",
            "Accept": "application/json, text/plain, */*",
        }
        async with get_session().get(
            "https://appactivity.wmpvp.com/steamcn/match/watchStage/getWebsocketInfo",
            params=params,
            headers=headers,
            timeout=aiohttp.ClientTimeout(total=10),
        ) as response:
            if response.status >= 400:
                raise HTTPException(status_code=502, detail="观将台地址获取失败")
            data = await response.json()
        websocket_url = data.get("result", {}).get("websocketUrl")
        if not websocket_url:
            raise HTTPException(status_code=502, detail="观将台未返回 WebSocket 地址")
        return websocket_url

    async def _fetch_team_statistics(self, match_data: dict[str, Any], platform: str) -> dict[str, dict[str, Any]]:
        player_rows = match_data.get("playerList") if isinstance(match_data.get("playerList"), list) else []
        ct_ids = [str(player.get("steamId")) for player in player_rows if player.get("side") == "CT" and player.get("steamId")]
        t_ids = [str(player.get("steamId")) for player in player_rows if player.get("side") == "TERRORIST" and player.get("steamId")]
        if not ct_ids and not t_ids:
            return {}
        headers = {
            "Origin": "https://news.wmpvp.com",
            "Referer": "https://news.wmpvp.com/",
            "X-Requested-With": "XMLHttpRequest",
            "platform": "h5_web",
            "appversion": "4.1.0",
            "appTheme": "0",
            "Content-Type": "application/json",
            "Accept": "application/json, text/plain, */*",
        }
        payload = {
            "ctTeamSteamIds": ct_ids,
            "teTeamSteamIds": t_ids,
            "map": match_data.get("map"),
        }
        try:
            statistics_url = (
                "https://appactivity.wmpvp.com/steamcn/match/watchStage/getMatchTeamStatisticsData"
                if platform == "1"
                else "https://appactivity.wmpvp.com/steamcn/match/watchStage/getPvPMatchTeamStatisticsData"
            )
            async with get_session().post(
                statistics_url,
                json=payload,
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=10),
            ) as response:
                if response.status >= 400:
                    return {}
                data = await response.json()
        except Exception:
            logger.exception("watch stage team statistics request failed")
            return {}
        result = data.get("result") if isinstance(data, dict) else None
        if not isinstance(result, dict):
            return {}
        profiles: dict[str, dict[str, Any]] = {}
        player_keys = (
            ("ctTeamPlayInfoList", "teTeamPlayInfoList")
            if platform == "1"
            else ("ctPlayerStatsDTOList", "tplayerStatsDTOList")
        )
        for key in player_keys:
            rows = result.get(key)
            if not isinstance(rows, list):
                continue
            for row in rows:
                steam_id = str(row.get("steamId") or "")
                if steam_id:
                    profiles[steam_id] = row
        return profiles

    def _ensure_profile_update(self, steam_id: str) -> None:
        if not _watch_stage_profile_refresh_enabled():
            profile = self.profile_cache.get(steam_id) or self._profile_from_external(steam_id)
            self.profile_cache[steam_id] = profile.copy(update={
                "status": "missing" if profile.status in {"missing", "updating"} else profile.status,
                "message": profile.message or "watch stage profile refresh disabled",
            })
            return
        if steam_id in self.profile_update_tasks:
            return
        retry_after = self.profile_retry_after.get(steam_id, 0)
        if retry_after > int(time.time()):
            return
        profile = self.profile_cache.get(steam_id) or self._profile_from_external(steam_id)
        if (
            profile
            and profile.status == "ready"
            and profile.legacyScore is not None
            and profile.updatedAt
            and int(time.time()) - profile.updatedAt < 3600
        ):
            return
        self.profile_update_tasks.add(steam_id)
        self.profile_cache[steam_id] = (profile or WatchStagePlayerProfile(steamId=steam_id)).copy(update={
            "status": "updating",
            "message": "基础数据抓取中",
        })
        asyncio.create_task(self._refresh_profile(steam_id))

    def _profile_from_external(self, steam_id: str) -> WatchStagePlayerProfile:
        cached = self.profile_cache.get(steam_id)
        if cached:
            return cached
        external = self.external_profiles.get(steam_id, {})
        profile = WatchStagePlayerProfile(
            steamId=steam_id,
            nickname=external.get("nickname"),
            avatar=external.get("avatar"),
            pvpScore=self._optional_int(external.get("pvpScore")),
            pvpStars=None,
            legacyScore=None,
            avgRt=self._optional_float(external.get("ratingPro")),
            avgWe=self._optional_float(external.get("we")),
            faceitElo=None,
            faceitLevel=None,
            status="missing",
            message="等待基础数据抓取",
            updatedAt=None,
        )
        self.profile_cache[steam_id] = profile
        return profile

    async def _profiles_for_players(self, steam_ids: set[str]) -> dict[str, WatchStagePlayerProfile]:
        profiles = {steam_id: self._profile_from_external(steam_id) for steam_id in steam_ids}
        try:
            async with async_session_factory() as session:
                base_rows = list((await session.execute(
                    select(SteamBaseInfo).where(SteamBaseInfo.steamid.in_(steam_ids))
                )).scalars().all())
                detail_rows = list((await session.execute(
                    select(SteamDetailInfo)
                    .where(SteamDetailInfo.steamid.in_(steam_ids))
                    .where(SteamDetailInfo.seasonId == config.cs_season_id)
                )).scalars().all())
                extra_rows = list((await session.execute(
                    select(SteamExtraInfo).where(SteamExtraInfo.steamid.in_(steam_ids))
                )).scalars().all())
        except SQLAlchemyError as exc:
            logger.warning(f"watch stage existing profile query skipped: {exc.__class__.__name__}")
            return profiles

        base_by_id = {row.steamid: row for row in base_rows}
        detail_by_id = {row.steamid: row for row in detail_rows}
        extra_by_id: dict[str, SteamExtraInfo] = {}
        for row in extra_rows:
            existing = extra_by_id.get(row.steamid)
            if existing is None or row.timeStamp > existing.timeStamp:
                extra_by_id[row.steamid] = row

        now = int(time.time())
        for steam_id in steam_ids:
            profile = profiles[steam_id]
            base_info = base_by_id.get(steam_id)
            detail_info = detail_by_id.get(steam_id)
            extra_info = extra_by_id.get(steam_id)
            ladder_score, ladder_stars = self._ladder_score_from_base(base_info)
            if not base_info and not detail_info and not extra_info:
                continue
            profiles[steam_id] = profile.copy(update={
                "nickname": profile.nickname or (base_info.name if base_info else None),
                "avatar": profile.avatar or (base_info.avatarlink if base_info else None),
                "pvpScore": self._optional_int((detail_info.pvpScore if detail_info else None) or profile.pvpScore or ladder_score),
                "pvpStars": self._optional_int((detail_info.pvpStars if detail_info else None) or profile.pvpStars or ladder_stars),
                "legacyScore": float(extra_info.legacyScore) if extra_info else profile.legacyScore,
                "avgRt": self._optional_float((detail_info.pwRating if detail_info else None) or profile.avgRt),
                "avgWe": self._optional_float((detail_info.we if detail_info else None) or profile.avgWe),
                "status": "ready" if extra_info else profile.status,
                "message": None if extra_info else profile.message,
                "updatedAt": now if extra_info else profile.updatedAt,
            })
            self.profile_cache[steam_id] = profiles[steam_id]
        return profiles

    def _ladder_score_from_base(self, base_info: SteamBaseInfo | None) -> tuple[int | None, int | None]:
        if not base_info or not base_info.ladderScore:
            return None, None
        try:
            rows = json.loads(base_info.ladderScore)
        except (TypeError, json.JSONDecodeError):
            return None, None
        if not isinstance(rows, list):
            return None, None
        selected = next((row for row in rows if isinstance(row, dict) and row.get("season") == config.cs_season_id), None)
        if not selected or not self._optional_int(selected.get("score")):
            return None, None
        return self._optional_int(selected.get("score")), self._optional_int(selected.get("currSStars") or 0)

    async def _refresh_profile(self, steam_id: str) -> None:
        try:
            async with self.profile_update_lock:
                try:
                    await asyncio.wait_for(self._refresh_profile_base_info(steam_id), timeout=10)
                except TooFrequentError as exc:
                    logger.info(f"watch stage profile limited steamid={steam_id} wait={exc.wait_time}s")
                    self.profile_retry_after[steam_id] = int(time.time()) + max(30, exc.wait_time)
                    self.profile_cache[steam_id] = (await self._get_profile(steam_id)).copy(update={
                        "status": "limited",
                        "message": f"基础数据限频，约 {exc.wait_time}s 后可继续更新",
                    })
                    return
                except LockingError as exc:
                    self.profile_retry_after[steam_id] = int(time.time()) + 60
                    self.profile_cache[steam_id] = (await self._get_profile(steam_id)).copy(update={
                        "status": "limited",
                        "message": str(exc),
                    })
                    return
                except asyncio.TimeoutError:
                    self.profile_retry_after[steam_id] = int(time.time()) + 300
                    self.profile_cache[steam_id] = (await self._get_profile(steam_id)).copy(update={
                        "status": "failed",
                        "message": "基础数据抓取超时，稍后重试",
                    })
                    return
            self.profile_cache[steam_id] = await self._get_profile(steam_id, force_ready=True)
        except Exception as exc:
            logger.exception(f"watch stage profile refresh failed steamid={steam_id}")
            self.profile_retry_after[steam_id] = int(time.time()) + 300
            self.profile_cache[steam_id] = (await self._get_profile(steam_id)).copy(update={
                "status": "failed",
                "message": str(exc),
            })
        finally:
            self.profile_update_tasks.discard(steam_id)

    async def _refresh_profile_base_info(self, steam_id: str) -> None:
        try:
            async with async_session_factory() as session:
                async with session.begin():
                    await db_upd._update_stats_card(steam_id, session)
        except TooFrequentError:
            try:
                async with async_session_factory() as session:
                    async with session.begin():
                        await db_upd._update_extra_info(steam_id, session)
            except Exception as exc:
                logger.info(f"watch stage extra info refresh after rate limit skipped steamid={steam_id} error={exc}")
            raise

        async with async_session_factory() as session:
            async with session.begin():
                await db_upd._update_extra_info(steam_id, session)

    async def _get_profile(self, steam_id: str, force_ready: bool = False) -> WatchStagePlayerProfile:
        external = self.external_profiles.get(steam_id, {})
        base_info = await self._safe_profile_query("base_info", steam_id, db_val.get_base_info)
        detail_info = await self._safe_profile_query("detail_info", steam_id, db_val.get_detail_info)
        extra_info = await self._safe_profile_query("extra_info", steam_id, db_val.get_extra_info)
        faceit_bind = await self._safe_profile_query("faceit_bind", steam_id, db_val.get_faceit_bind)
        ladder_score, ladder_stars = self._ladder_score_from_base(base_info)
        status = "ready" if force_ready or base_info or detail_info or extra_info else "missing"
        if steam_id in self.profile_update_tasks:
            status = "updating"
        profile = WatchStagePlayerProfile(
            steamId=steam_id,
            nickname=external.get("nickname") or (base_info.name if base_info else None),
            avatar=external.get("avatar") or (base_info.avatarlink if base_info else None),
            pvpScore=self._optional_int((detail_info.pvpScore if detail_info else None) or external.get("pvpScore") or ladder_score),
            pvpStars=self._optional_int((detail_info.pvpStars if detail_info else None) or ladder_stars),
            legacyScore=float(extra_info.legacyScore) if extra_info else None,
            avgRt=self._optional_float((detail_info.pwRating if detail_info else None) or external.get("ratingPro")),
            avgWe=self._optional_float((detail_info.we if detail_info else None) or external.get("we")),
            faceitElo=faceit_bind.faceit_elo if faceit_bind else None,
            faceitLevel=faceit_bind.skill_level if faceit_bind else None,
            status=status,
            message=None if status == "ready" else "等待基础数据抓取",
            updatedAt=int(time.time()) if status == "ready" else None,
        )
        self.profile_cache[steam_id] = profile
        return profile

    async def _safe_profile_query(self, label: str, steam_id: str, query_func):
        try:
            return await query_func(steam_id)
        except SQLAlchemyError as exc:
            logger.warning(f"watch stage profile query skipped label={label} steamid={steam_id}: {exc.__class__.__name__}")
            return None

    def _optional_int(self, value: Any) -> int | None:
        if value is None:
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    def _optional_float(self, value: Any) -> float | None:
        if value is None:
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None


watch_stage_manager = WatchStageManager()

db = DataMannager()

verify = on_command("验证", aliases={"verify"}, priority=10)
steam_status_cmd = on_command("游戏状态", aliases={"steam-status", "steamstatus", "steam状态", "gamestatus"}, priority=10, block=True)

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


class WatchStageRequest(BaseModel):
    steamId: str = Field(..., description="Steam ID")


@app.post("/api/watch-stage/snapshot", response_model=WatchStageSnapshot)
async def get_watch_stage_snapshot(request: WatchStageRequest, _ = Depends(get_current_user)):
    return await watch_stage_manager.subscribe(request.steamId)

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
    assert info.user_id is not None
    assert info.group_id is not None
    try:
        bot = get_bot()
    except ValueError:
        return InfoNameResponse(showName=info.user_id)
    assert isinstance(bot, Bot)
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

class InfoFaceitResponse(BaseModel):
    steamId: str | None = Field(..., description="绑定的 Steam ID")
    playerId: str | None = Field(..., description="绑定的 FACEIT player_id")
    nickname: str | None = Field(..., description="FACEIT 昵称")
    skillLevel: int | None = Field(..., description="FACEIT 等级")
    faceitElo: int | None = Field(..., description="FACEIT ELO")

@app.post("/api/auth/info/faceit",
    response_model=InfoFaceitResponse,
    summary="获取绑定的 FACEIT 信息",
    description="获取当前用户 SteamID 对应的 FACEIT 绑定信息。"
)
async def get_token_faceit(info: AuthSession = Depends(get_current_user)):
    assert info.user_id is not None
    steamid = await db_val.get_steamid(info.user_id)
    if steamid is None:
        return InfoFaceitResponse(steamId=None, playerId=None, nickname=None, skillLevel=None, faceitElo=None)
    bind = await db_val.get_faceit_bind(steamid)
    if bind is None:
        return InfoFaceitResponse(steamId=steamid, playerId=None, nickname=None, skillLevel=None, faceitElo=None)
    return InfoFaceitResponse(
        steamId=steamid,
        playerId=bind.player_id,
        nickname=bind.nickname,
        skillLevel=bind.skill_level,
        faceitElo=bind.faceit_elo,
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

class SteamStatusItem(BaseModel):
    uid: str = Field(..., description="绑定的 QQ 号")
    game_appid: str = Field(..., description="游戏 AppId，离线时为空")
    game_name: str = Field(..., description="游戏名称，离线时为空")
    game_icon: str = Field(..., description="游戏图标 URL")
    party_id: str = Field(..., description="组队 ID")
    party_size: str = Field(..., description="组队人数")
    rich_presence_string: str = Field(..., description="游戏状态文本")
    rich_presence: dict[str, str] = Field(..., description="游戏状态详情")
    state: str = Field(..., description="在线状态")


class SteamStatusResponse(BaseModel):
    status: str = Field(..., description="上游接口状态")
    data: list[SteamStatusItem] = Field(..., description="筛选后的群成员状态")


async def _fetch_steam_status_payload() -> dict:
    if not config.cs_steam_monitor_url:
        raise HTTPException(status_code=503, detail="cs_steam_monitor_url 未配置")

    timeout = aiohttp.ClientTimeout(total=10)
    try:
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(config.cs_steam_monitor_url) as resp:
                if resp.status != 200:
                    raise HTTPException(status_code=502, detail=f"上游状态接口异常: HTTP {resp.status}")
                payload = await resp.json(content_type=None)
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Fetch steam monitor failed", exc_info=exc)
        raise HTTPException(status_code=502, detail=f"拉取状态失败: {exc}")

    if not isinstance(payload, dict):
        raise HTTPException(status_code=502, detail="上游返回格式错误")
    raw_data = payload.get("data", [])
    if not isinstance(raw_data, list):
        raise HTTPException(status_code=502, detail="上游 data 字段格式错误")
    return payload


async def _get_filtered_steam_status(group_id: str) -> SteamStatusResponse:
    payload = await _fetch_steam_status_payload()
    raw_data = payload.get("data", [])

    group_uids = await db_val.get_group_member(group_id)
    steam_to_uid: dict[str, str] = {}
    for uid in group_uids:
        steam_id = await db_val.get_steamid(uid)
        if steam_id:
            steam_to_uid[steam_id] = uid

    filtered_data: list[SteamStatusItem] = []
    for item in raw_data:
        if not isinstance(item, dict):
            continue
        steam_id = str(item.get("steamId") or item.get("steam_id") or "")
        uid = steam_to_uid.get(steam_id)
        if not uid:
            continue
        rich_presence_raw = item.get("richPresence") or item.get("rich_display") or {}
        rich_presence: dict[str, str] = {}
        if isinstance(rich_presence_raw, dict):
            rich_presence = {str(k): str(v) for k, v in rich_presence_raw.items()}
        elif isinstance(rich_presence_raw, str) and rich_presence_raw:
            rich_presence = {"text": rich_presence_raw}

        rich_presence_string = str(item.get("richPresenceString") or item.get("rich_display") or "")
        game_icon = str(item.get("gameSmallIcon") or item.get("game_logo") or "")

        party_raw = item.get("party")
        if isinstance(party_raw, dict):
            party_id = str(party_raw.get("groupId") or item.get("party_id") or "")
            party_size = str(party_raw.get("groupSize") or item.get("party_size") or "")
        else:
            party_id = str(item.get("party_id", ""))
            party_size = str(item.get("party_size", ""))

        state_text = str(item.get("personaStateText") or item.get("state") or "")
        filtered_data.append(
            SteamStatusItem(
                uid=uid,
                game_appid=str(item.get("gameId") or item.get("game_appid") or ""),
                game_name=str(item.get("gameName") or item.get("game_name") or ""),
                game_icon=game_icon,
                party_id=party_id,
                party_size=party_size,
                rich_presence_string=rich_presence_string,
                rich_presence=rich_presence,
                state=state_text,
            )
        )

    return SteamStatusResponse(
        status=str(payload.get("status", "success")),
        data=filtered_data,
    )


@app.post(
    "/api/steam/status",
    response_model=SteamStatusResponse,
    summary="获取群内 Steam 在线状态",
    description="从监控接口拉取状态，并按当前 Token 所属群筛选后返回。"
)
async def get_steam_status(info: AuthSession = Depends(get_current_user)):
    assert info.group_id is not None
    return await _get_filtered_steam_status(info.group_id)


@steam_status_cmd.handle()
async def handle_steam_status(event: GroupMessageEvent):
    token = await db.get_bot_token(str(event.group_id))
    screenshot = await get_screenshot("/steam-status", token, width=600)
    if screenshot:
        await steam_status_cmd.finish(MessageSegment.image(screenshot))
    await steam_status_cmd.finish("生成 Steam 状态图片失败，请稍后再试")

class MuteRequest(BaseModel):
    authToken: str = Field(..., description="来自 .env 的管理员认证 token")
    groupId: int = Field(..., ge=0, description="目标群号")
    userId: int = Field(..., ge=0, description="需要禁言的 QQ")
    duration: int = Field(..., ge=0, description="禁言时长，单位秒")
    reason: str | None = Field(None, description="可选的禁言原因，用于审计")


class MuteResponse(BaseModel):
    success: bool = Field(..., description="禁言是否成功")
    message: str = Field(..., description="禁言结果说明")


@app.post(
    "/api/mod/mute",
    response_model=MuteResponse,
    summary="强制禁言群成员",
    description="使用 .env 中配置的管理 token 对指定群成员进行禁言。"
)
async def mute_group_member(payload: MuteRequest):
    if not config.mute_api_token:
        raise HTTPException(status_code=503, detail="Mute API token 未配置")
    if payload.authToken != config.mute_api_token:
        raise HTTPException(status_code=403, detail="Token 无效")
    bot = get_bot()
    assert isinstance(bot, Bot)
    logger.info(f"Mute API request group={payload.groupId} user={payload.userId} duration={payload.duration} reason={payload.reason}")
    try:
        await bot.set_group_ban(group_id=payload.groupId, user_id=payload.userId, duration=payload.duration)
    except ActionFailed as exc:
        detail = exc.info.get("message") if isinstance(exc.info, dict) else str(exc)
        raise HTTPException(status_code=400, detail=detail or "操作失败")
    except Exception as exc:
        logger.error("Mute API failed", exc_info=exc)
        raise HTTPException(status_code=500, detail=f"禁言失败: {exc}")
    return MuteResponse(
        success=True,
        message=f"已对 {payload.userId} 禁言 {payload.duration} 秒"
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

class SendResponse(BaseModel):
    success: bool = Field(..., description="发送是否成功")

@app.post("/api/auth/send",
    response_model=SendResponse,
    summary="发送指定页面图片",
    description="向绑定的 QQ 群发送指定页面的图片。")
async def send_page_image(path: str = Body(..., embed=True), info: AuthSession = Depends(get_current_user)) -> SendResponse:
    assert info.user_id is not None
    user_info = await db.get_user(info.user_id)
    assert user_info is not None
    if user_info.last_send_time and (int(time.time()) - user_info.last_send_time) < config.send_interval_seconds:
        raise HTTPException(status_code=429, detail=f"发送过于频繁，请 {config.send_interval_seconds - (int(time.time()) - user_info.last_send_time)}s 后再试")
    await db.set_user_send(info.user_id)
    bot = get_bot()
    assert isinstance(bot, Bot)
    assert info.group_id is not None
    
    screenshot = await get_screenshot(path, info.token)
    if screenshot:
        # 发送消息
        await bot.send_group_msg(
            group_id=int(info.group_id),
            message=Message(MessageSegment.image(screenshot))
        )
        
        await bot.send_group_msg(
            group_id=int(info.group_id),
            message=MessageSegment.at(info.user_id) + " 分享了 " + (config.cs_domain + path)
        )
        return SendResponse(success=True)
    else:
        return SendResponse(success=False)

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
    displayPvpScore: float | None = Field(None, description="用于展示段位的分数，定段赛可能为预测值")
    isPredictedPvpScore: bool = Field(False, description="展示分数是否为预测值")
    pvpScoreChange: float = Field(..., description="玩家的天梯分数变化")
    pvpStars: int = Field(..., description="玩家的天梯星级")

class MatchPWInfo(BaseModel):
    matchId: str = Field(..., description="比赛 ID")
    timestamp: int = Field(..., description="比赛时间戳")
    season: str = Field(..., description="赛季（Sxx）")
    winTeam: int = Field(..., description="获胜队伍 (1 或 2)")
    mode: str = Field(..., description="比赛模式")
    mapName: str = Field(..., description="比赛地图")
    userTeam: int | None = Field(..., description="群友所属队伍 (1 或 2)")
    team1Score: int = Field(..., description="队伍 1 分数")
    team2Score: int = Field(..., description="队伍 2 分数")
    team1LegacyScore: float | None = Field(..., description="队伍 1 底蕴均分")
    team2LegacyScore: float | None = Field(..., description="队伍 2 底蕴均分")
    players: list[MatchPWPlayerInfo] = Field(..., description="参赛玩家信息列表")

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
    async def build_player_info(player: MatchStatsPW) -> MatchPWPlayerInfo:
        display_score, is_predicted = await get_display_pvp_score(player)
        return MatchPWPlayerInfo(
            steamId=player.steamid,
            nickname=await get_nickname(player.steamid),
            team=player.team,
            rating=player.pwRating,
            we=player.we,
            kills=player.kill,
            deaths=player.death,
            assists=player.assist,
            legacyScore=await get_legacy_score(player.steamid, player.timeStamp),
            pvpScore=player.pvpScore,
            displayPvpScore=display_score,
            isPredictedPvpScore=is_predicted,
            pvpScoreChange=player.pvpScoreChange,
            pvpStars=player.pvpStars
        )
    return MatchPWInfo(
        matchId=matchId,
        timestamp=match_detail[0].timeStamp,
        season=match_detail[0].seasonId,
        winTeam=match_detail[0].winTeam,
        mode=match_detail[0].mode,
        mapName=match_detail[0].mapName,
        userTeam=await get_match_user_team([(player.steamid, player.team) for player in match_detail]),
        team1Score=match_detail[0].score1,
        team2Score=match_detail[0].score2,
        team1LegacyScore=match_extra.team1Legacy if match_extra else None,
        team2LegacyScore=match_extra.team2Legacy if match_extra else None,
        players=[await build_player_info(player) for player in match_detail]
    )

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
    displayPvpScore: float | None = Field(None, description="用于展示段位的分数，定段赛可能为预测值")
    isPredictedPvpScore: bool = Field(False, description="展示分数是否为预测值")
    pvpScoreChange: float = Field(..., description="天梯分数变化")
    pvpStars: int = Field(..., description="天梯星级")
    legacyDiff: float | None = Field(..., description="底蕴分数变化")

class MatchHistoryResponse(BaseModel):
    totCount: int = Field(..., description="总比赛数")
    pageSize: int = Field(..., description="每页大小")
    matches: list[MatchHistoryItem] = Field(..., description="比赛列表")

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

    async def build_history_item(record: MatchStatsPW) -> MatchHistoryItem:
        display_score, is_predicted = await get_display_pvp_score(record)
        return MatchHistoryItem(
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
            displayPvpScore=display_score,
            isPredictedPvpScore=is_predicted,
            pvpScoreChange=record.pvpScoreChange,
            pvpStars=record.pvpStars,
            legacyDiff=await get_legacy_diff(record)
        )
    
    return MatchHistoryResponse(
        totCount=total_count,
        pageSize=20,
        matches=[await build_history_item(record) for record in match_records] if match_records else []
    )

class MatchGPPlayerInfo(BaseModel):
    steamId: str = Field(..., description="玩家的 Steam ID")
    nickname: str = Field(..., description="玩家昵称")
    team: int = Field(..., description="玩家所属队伍 (1 或 2)")
    rating: float = Field(..., description="玩家的比赛评分")
    adr: float = Field(..., description="玩家的 ADR 值")
    kills: int = Field(..., description="击杀数")
    deaths: int = Field(..., description="死亡数")
    assists: int = Field(..., description="助攻数")
    legacyScore: float | None = Field(..., description="玩家的底蕴分数")
    rank: float = Field(..., description="玩家的段位")

class MatchGPInfo(BaseModel):
    matchId: str = Field(..., description="比赛 ID")
    timestamp: int = Field(..., description="比赛时间戳")
    winTeam: int = Field(..., description="获胜队伍 (1 或 2)")
    mode: str = Field(..., description="比赛模式")
    mapName: str = Field(..., description="比赛地图")
    userTeam: int | None = Field(..., description="群友所属队伍 (1 或 2)")
    team1Score: int = Field(..., description="队伍 1 分数")
    team2Score: int = Field(..., description="队伍 2 分数")
    team1LegacyScore: float | None = Field(..., description="队伍 1 底蕴均分")
    team2LegacyScore: float | None = Field(..., description="队伍 2 底蕴均分")
    players: list[MatchGPPlayerInfo] = Field(..., description="参赛玩家信息列表")

@app.post("/api/match/infogp",
    response_model=MatchGPInfo,
    summary="获取官匹比赛详细信息",
    description="根据比赛 ID 获取官匹比赛的详细信息，包括参赛玩家的数据。"
)
async def get_match_gp_info(matchId: str = Body(..., embed=True), _ = Depends(get_current_user)):
    match_detail = await db_val.get_match_gp_detail(matchId)
    if not match_detail:
        raise HTTPException(status_code=404, detail="Match not found")
    match_extra = await db_val.get_match_gp_extra(matchId)
    return MatchGPInfo(
        matchId=matchId,
        timestamp=match_detail[0].timeStamp,
        winTeam=match_detail[0].winTeam,
        mode=match_detail[0].mode,
        mapName=match_detail[0].mapName,
        userTeam=await get_match_user_team([(player.steamid, player.team) for player in match_detail]),
        team1Score=match_detail[0].score1,
        team2Score=match_detail[0].score2,
        team1LegacyScore=match_extra.team1Legacy if match_extra else None,
        team2LegacyScore=match_extra.team2Legacy if match_extra else None,
        players=[
            MatchGPPlayerInfo(
                steamId=player.steamid,
                nickname=await get_nickname(player.steamid),
                team=player.team,
                rating=player.rating,
                adr=player.adpr,
                kills=player.kill,
                deaths=player.death,
                assists=player.assist,
                legacyScore=await get_legacy_score(player.steamid, player.timeStamp),
                rank=player.rank
            ) for player in match_detail
        ]
    )

class MatchGPHistoryItem(BaseModel):
    matchId: str = Field(..., description="比赛 ID")
    timeStamp: int = Field(..., description="比赛时间戳")
    mode: str = Field(..., description="比赛模式")
    mapName: str = Field(..., description="比赛地图")
    team1Score: int = Field(..., description="队伍 1 分数")
    team2Score: int = Field(..., description="队伍 2 分数")
    team: int = Field(..., description="玩家所在队伍")
    winTeam: int = Field(..., description="获胜队伍")
    rating: float = Field(..., description="玩家评分")
    adr: float = Field(..., description="玩家 ADR")
    rank: float = Field(..., description="官匹等级")
    legacyDiff: float | None = Field(..., description="底蕴分数变化")

class MatchGPHistoryResponse(BaseModel):
    totCount: int = Field(..., description="总比赛数")
    pageSize: int = Field(..., description="每页大小")
    matches: list[MatchGPHistoryItem] = Field(..., description="比赛列表")

@app.post("/api/match/historygp",
    response_model=MatchGPHistoryResponse,
    summary="获取官匹比赛历史",
    description="根据玩家 Steam ID 获取其官匹比赛历史记录。"
)
async def get_match_gp_history(
    steamId: str = Body(..., embed=True),
    timeType: str = Body(..., embed=True),
    page: int = Body(..., embed=True, ge=1),
    _ = Depends(get_current_user)):
    # 获取玩家的官匹比赛历史
    match_records = await db_val.get_matches_gp(steamId, timeType, offset=(page - 1) * 20, limit=20)
    total_count = await db_val.get_matches_gp_count(steamId, timeType)
    async def get_legacy_diff(player: MatchStatsGP) -> float | None:
        extra_info = await db_val.get_match_gp_extra(player.mid)
        if not extra_info:
            return None
        if not extra_info.team1Legacy or not extra_info.team2Legacy:
            return None
        if player.team == 1:
            return extra_info.team1Legacy - extra_info.team2Legacy
        else:
            return extra_info.team2Legacy - extra_info.team1Legacy
    if not total_count:
        raise HTTPException(status_code=404, detail="No match history found")
    
    return MatchGPHistoryResponse(
        totCount=total_count,
        pageSize=20,
        matches=[
            MatchGPHistoryItem(
                matchId=record.mid,
                timeStamp=record.timeStamp,
                mode=record.mode,
                mapName=record.mapName,
                team=record.team,
                winTeam=record.winTeam,
                team1Score=record.score1,
                team2Score=record.score2,
                rating=record.rating,
                adr=record.adpr,
                rank=record.rank,
                legacyDiff=await get_legacy_diff(record)
            ) for record in match_records
        ] if match_records else []
    )

class MatchFaceitPlayerInfo(BaseModel):
    faceitPlayerId: str = Field(..., description="FACEIT player_id")
    steamId: str | None = Field(..., description="Steam ID")
    nickname: str = Field(..., description="FACEIT 昵称")
    team: int = Field(..., description="玩家所在队伍")
    skillLevel: int = Field(..., description="FACEIT 等级")
    faceitElo: int = Field(..., description="FACEIT ELO")
    adr: float = Field(..., description="ADR")
    rating: float = Field(..., description="FACEIT Rating")
    kdRatio: float = Field(..., description="K/D Ratio")
    kills: int = Field(..., description="击杀")
    deaths: int = Field(..., description="死亡")
    assists: int = Field(..., description="助攻")
    headshotsPct: int = Field(..., description="爆头率")
    mvps: int = Field(..., description="MVP 数")

class MatchFaceitInfo(BaseModel):
    matchId: str = Field(..., description="比赛 ID")
    timestamp: int = Field(..., description="比赛时间戳")
    winTeam: int = Field(..., description="获胜队伍")
    mode: str = Field(..., description="比赛模式")
    competitionName: str = Field(..., description="赛事名称")
    region: str = Field(..., description="地区")
    mapName: str = Field(..., description="地图")
    userTeam: int | None = Field(..., description="群友所在队伍")
    team1Score: int = Field(..., description="队伍 1 分数")
    team2Score: int = Field(..., description="队伍 2 分数")
    players: list[MatchFaceitPlayerInfo] = Field(..., description="玩家列表")

@app.post("/api/match/infofaceit",
    response_model=MatchFaceitInfo,
    summary="获取 FACEIT 比赛详情",
    description="根据比赛 ID 获取 FACEIT 比赛详情。"
)
async def get_match_faceit_info(matchId: str = Body(..., embed=True), _ = Depends(get_current_user)):
    match_detail = await db_val.get_match_faceit_detail(matchId)
    if not match_detail:
        raise HTTPException(status_code=404, detail="Match not found")
    return MatchFaceitInfo(
        matchId=matchId,
        timestamp=match_detail[0].timeStamp,
        winTeam=match_detail[0].winTeam,
        mode=match_detail[0].mode,
        competitionName=match_detail[0].competitionName,
        region=match_detail[0].region,
        mapName=match_detail[0].mapName,
        userTeam=await get_match_user_team([(player.steamid, player.team) for player in match_detail if player.steamid]),
        team1Score=match_detail[0].score1,
        team2Score=match_detail[0].score2,
        players=[
            MatchFaceitPlayerInfo(
                faceitPlayerId=player.player_id,
                steamId=player.steamid,
                nickname=player.nickname,
                team=player.team,
                skillLevel=player.skillLevel,
                faceitElo=player.faceitElo,
                adr=player.adr,
                rating=player.rating,
                kdRatio=player.kdRatio,
                kills=player.kill,
                deaths=player.death,
                assists=player.assist,
                headshotsPct=player.headshotsPct,
                mvps=player.mvp,
            ) for player in match_detail
        ]
    )

class MatchFaceitHistoryItem(BaseModel):
    matchId: str = Field(..., description="比赛 ID")
    timeStamp: int = Field(..., description="比赛时间戳")
    mode: str = Field(..., description="比赛模式")
    competitionName: str = Field(..., description="赛事名称")
    mapName: str = Field(..., description="地图")
    team1Score: int = Field(..., description="队伍 1 分数")
    team2Score: int = Field(..., description="队伍 2 分数")
    team: int = Field(..., description="玩家所在队伍")
    winTeam: int = Field(..., description="获胜队伍")
    adr: float = Field(..., description="ADR")
    kdRatio: float = Field(..., description="K/D Ratio")
    kills: int = Field(..., description="击杀")
    deaths: int = Field(..., description="死亡")
    assists: int = Field(..., description="助攻")
    skillLevel: int = Field(..., description="FACEIT 等级")
    faceitElo: int = Field(..., description="FACEIT ELO")

class MatchFaceitHistoryResponse(BaseModel):
    totCount: int = Field(..., description="总比赛数")
    pageSize: int = Field(..., description="每页大小")
    matches: list[MatchFaceitHistoryItem] = Field(..., description="比赛列表")

@app.post("/api/match/historyfaceit",
    response_model=MatchFaceitHistoryResponse,
    summary="获取 FACEIT 比赛历史",
    description="根据 Steam ID 获取 FACEIT 比赛历史。"
)
async def get_match_faceit_history(
    steamId: str = Body(..., embed=True),
    timeType: str = Body(..., embed=True),
    page: int = Body(..., embed=True, ge=1),
    _ = Depends(get_current_user)):
    match_records = await db_val.get_matches_faceit(steamId, timeType, offset=(page - 1) * 20, limit=20)
    total_count = await db_val.get_matches_faceit_count(steamId, timeType)
    if not total_count:
        raise HTTPException(status_code=404, detail="No match history found")
    return MatchFaceitHistoryResponse(
        totCount=total_count,
        pageSize=20,
        matches=[
            MatchFaceitHistoryItem(
                matchId=record.mid,
                timeStamp=record.timeStamp,
                mode=record.mode,
                competitionName=record.competitionName,
                mapName=record.mapName,
                team=record.team,
                winTeam=record.winTeam,
                team1Score=record.score1,
                team2Score=record.score2,
                adr=record.adr,
                kdRatio=record.kdRatio,
                kills=record.kill,
                deaths=record.death,
                assists=record.assist,
                skillLevel=record.skillLevel,
                faceitElo=record.faceitElo,
            ) for record in match_records
        ] if match_records else []
    )

class AllMatchHistoryItem(BaseModel):
    matchId: str = Field(..., description="比赛 ID")
    timeStamp: int = Field(..., description="比赛时间戳")
    mode: str = Field(..., description="比赛模式")
    mapName: str = Field(..., description="比赛地图")
    isGP: bool = Field(..., description="是否为官匹")
    matchType: str = Field(..., description="比赛来源: pw/gp/faceit")
    team1Score: int = Field(..., description="队伍 1 分数")
    team2Score: int = Field(..., description="队伍 2 分数")
    team1Player: list[str] = Field(..., description="队伍 1 玩家 Steam ID 列表")
    team2Player: list[str] = Field(..., description="队伍 2 玩家 Steam ID 列表")
    winTeam: int = Field(..., description="获胜队伍")

class AllMatchHistoryResponse(BaseModel):
    totCount: int = Field(..., description="总比赛数")
    pageSize: int = Field(..., description="每页大小")
    matches: list[AllMatchHistoryItem] = Field(..., description="比赛列表")

@app.post("/api/match/historyall",
    response_model=AllMatchHistoryResponse,
    summary="获取所有比赛历史",
    description="所有比赛历史记录（包括官匹和非官匹）。"
)
async def get_all_match_history(
    page: int = Body(..., embed=True, ge=1),
    _ = Depends(get_current_user)):
    count = await db_val.get_all_matches_count()
    res = await db_val.get_all_matches(limit=20, offset=(page - 1) * 20)
    
    if not count:
        raise HTTPException(status_code=404, detail="No match history found")

    return AllMatchHistoryResponse(
        totCount=count,
        pageSize=20,
        matches=[
            AllMatchHistoryItem(
                matchId=record['mid'],
                timeStamp=record['timeStamp'],
                mode=record['mode'],
                mapName=record['mapName'],
                isGP=record['isGP'],
                matchType=record.get('matchType', 'gp' if record['isGP'] else 'pw'),
                team1Score=record['score1'],
                team2Score=record['score2'],
                team1Player=record['team1'],
                team2Player=record['team2'],
                winTeam=record['winTeam']
            ) for record in res
        ]
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

# 历史天梯
class LadderItem(BaseModel):
    seasonId: str = Field(..., description="赛季")
    pvpScore: int = Field(..., description="PVP分数")
    pvpStars: int = Field(..., description="PVP星级")


class PlayerDetailResponse(BaseModel):
    # 基础综合数据
    seasonId: str = Field(..., description="赛季")
    lastUpdate: int = Field(..., description="最后数据更新时间戳")
    pvpScore: int = Field(..., description="PVP分数")
    pvpStars: int = Field(..., description="PVP星级")
    cnt: int = Field(..., description="比赛场次")
    winRate: PlayerDetailItem = Field(..., description="胜率")
    ladderHistory: list[LadderItem] = Field(..., description="历史天梯记录")
    
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
    detail_info = await db_val.get_detail_info(steamId)
    base_info = await db_val.get_base_info(steamId)
    if not detail_info:
        raise HTTPException(status_code=404, detail="Player detail info not found")
    if not base_info:
        raise HTTPException(status_code=404, detail="Player base info not found")

    global_stats = SEASON_STATS_CACHE.get(detail_info.seasonId)
    if global_stats is None:
        logger.info(f"season stats cache miss, calculating on demand season={detail_info.seasonId}")
        global_stats = await _calculate_global_stats(detail_info.seasonId)
        SEASON_STATS_CACHE[detail_info.seasonId] = global_stats

    try:
        ladder_score = json.loads(base_info.ladderScore or "[]")
    except (TypeError, json.JSONDecodeError):
        logger.warning(f"invalid ladderScore for steam_id={steamId}")
        ladder_score = []
    
    # 使用全局统计数据计算个人的 PlayerDetailItem
    stats_data = _get_player_stats(detail_info, global_stats)
    
    return PlayerDetailResponse(
        seasonId=detail_info.seasonId,
        lastUpdate=base_info.updateTime,
        pvpScore=detail_info.pvpScore,
        pvpStars=detail_info.pvpStars,
        cnt=detail_info.cnt,
        winRate=stats_data['winRate'],
        ladderHistory=[
            LadderItem(
                seasonId=data.get('season', ''),
                pvpScore=data.get('score', 0),
                pvpStars=data.get('currSStars') or 0
            )
            for data in ladder_score
            if isinstance(data, dict)
        ],
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

class PlayerUpdateResponse(BaseModel):
    nickname: str = Field(..., description="玩家昵称")
    matchCount: int = Field(..., description="更新的比赛数量")
    matchgpCount: int = Field(..., description="更新的官匹比赛数量")
    faceitCount: int = Field(..., description="更新的 FACEIT 比赛数量")

@app.post("/api/player/update",
    response_model=PlayerUpdateResponse,
    summary="更新玩家数据",
    description="根据 Steam ID 更新玩家的数据。"
)
async def update_player_data(steamId: str = Body(..., embed=True), _ = Depends(get_current_user)):
    if not re.match(r'^\d{17}$', steamId):
        raise HTTPException(status_code=400, detail="不合法的 Steam ID 格式")
    try:
        res = await db_upd.update_stats(steamId)
    except TooFrequentError as e:
        raise HTTPException(status_code=429, detail=str(e))
    except LockingError as e:
        raise HTTPException(status_code=503, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail="获取失败: " + str(e))
    return PlayerUpdateResponse(
        nickname=res[0],
        matchCount=len(res[1]),
        matchgpCount=len(res[2]),
        faceitCount=len(res[3])
    )

class TimeResponse(BaseModel):
    timeTypes: list[str] = Field(..., description="支持的时间范围类型列表")
    gpTimeTypes: list[str] = Field(..., description="支持的官匹时间范围类型列表")

@app.post("/api/config/time",
    response_model=TimeResponse,
    summary="获取支持的时间范围类型",
    description="获取所有支持的时间范围类型列表。"
)
async def get_time_types(_ = Depends(get_current_user)):
    return TimeResponse(
        timeTypes=valid_time,
        gpTimeTypes=gp_time
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
    if info.group_id is None:
        return UserResponse(users=[])
    async with async_session_factory() as session:
        stmt = (
            select(
                GroupMember.uid,
                MemberSteamID.steamid,
                SteamBaseInfo.name,
                UserInfo.nickname,
            )
            .join(MemberSteamID, MemberSteamID.uid == GroupMember.uid)
            .join(SteamBaseInfo, SteamBaseInfo.steamid == MemberSteamID.steamid)
            .outerjoin(UserInfo, UserInfo.user_id == GroupMember.uid)
            .where(GroupMember.gid == info.group_id)
            .order_by(UserInfo.nickname, SteamBaseInfo.name)
        )
        rows = list((await session.execute(stmt)).all())
    users = [
        UserQQItem(
            qq=str(qq),
            qqNickname=qq_nickname or "未知用户",
            steamId=str(steam_id),
            nickname=nickname,
        )
        for qq, steam_id, nickname, qq_nickname in rows
    ]
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

class MajorHomeworkPick(BaseModel):
    team: str = Field(..., description="Team name")
    category: str = Field(..., description="Pick category")
    status: str = Field(..., description="correct, wrong, or pending")
    logo: str | None = Field(None, description="Team logo URL")
    wins: int = Field(..., description="Current wins")
    losses: int = Field(..., description="Current losses")
    recordStatus: str = Field(..., description="Current team record state")

class MajorHomeworkRankItem(BaseModel):
    uid: str = Field(..., description="QQ user id")
    avatar: str = Field(..., description="QQ avatar URL")
    probability: float | None = Field(None, description="Probability of passing the homework")
    expected: float | None = Field(None, description="Expected correct picks")
    score: float | None = Field(None, description="Current deterministic score")
    scoreLabel: str | None = Field(None, description="Human-readable score breakdown")
    picks: dict[str, list[MajorHomeworkPick]] = Field(..., description="Grouped picks")

class MajorHomeworkRankResponse(BaseModel):
    stage: str = Field(..., description="Major stage")
    stageType: str = Field("swiss", description="swiss or playoffs")
    categories: list[str] = Field(..., description="Pick categories")
    teams: list[str] = Field(..., description="Teams in this stage")
    resultPicks: dict[str, list[MajorHomeworkPick]] = Field(..., description="Current deterministic results")
    players: list[MajorHomeworkRankItem] = Field(..., description="Homework rankings")

class MajorHomeworkHistoryPoint(BaseModel):
    matchCount: int = Field(..., description="Finished match count when this snapshot was generated")
    createdAt: int = Field(..., description="Snapshot unix timestamp")
    homeworkText: str = Field(..., description="Canonical homework picks at this snapshot")
    probability: float | None = Field(None, description="Probability of passing the homework")
    expected: float | None = Field(None, description="Expected correct picks")

class MajorHomeworkHistoryItem(BaseModel):
    uid: str = Field(..., description="QQ user id")
    avatar: str = Field(..., description="QQ avatar URL")
    points: list[MajorHomeworkHistoryPoint] = Field(..., description="History points")

class MajorHomeworkHistoryResponse(BaseModel):
    stage: str = Field(..., description="Major stage")
    players: list[MajorHomeworkHistoryItem] = Field(..., description="Homework history by player")

class MajorHomeworkPersonalRow(BaseModel):
    matchCount: int = Field(..., description="Finished match count when this snapshot was generated")
    createdAt: int = Field(..., description="Snapshot unix timestamp")
    event: str = Field(..., description="Event that produced this row")
    eventWinner: str | None = Field(None, description="Winning team for a match event")
    eventWinnerLogo: str | None = Field(None, description="Winning team logo URL")
    eventLoser: str | None = Field(None, description="Losing team for a match event")
    eventLoserLogo: str | None = Field(None, description="Losing team logo URL")
    eventScore: str | None = Field(None, description="Match score for a match event")
    homeworkText: str = Field(..., description="Canonical homework picks at this snapshot")
    probability: float | None = Field(None, description="Probability of passing the homework")
    probabilityChange: float | None = Field(None, description="Probability delta from previous row")
    expected: float | None = Field(None, description="Expected correct picks")
    picks: dict[str, list[MajorHomeworkPick]] = Field(..., description="Grouped picks")

class MajorHomeworkPersonalResponse(BaseModel):
    stage: str = Field(..., description="Major stage")
    uid: str = Field(..., description="QQ user id")
    avatar: str = Field(..., description="QQ avatar URL")
    categories: list[str] = Field(..., description="Pick categories")
    rows: list[MajorHomeworkPersonalRow] = Field(..., description="Personal homework history")

MAJOR_TEAM_LOGOS: dict[str, str] = {
    "Vitality": "vita.png",
    "The MongolZ": "mong.png",
    "Falcons": "falc.png",
    "MOUZ": "mouz.png",
    "FURIA": "furi.png",
    "Natus Vincere": "navi.png",
    "Aurora": "auro.png",
    "PARIVISION": "pv.png",
    "FUT": "fut.png",
    "Spirit": "spir.png",
    "Astralis": "astr.png",
    "G2": "g2.png",
    "Legacy": "lega.png",
    "paiN": "pain.png",
    "Monte": "monte.png",
    "9z": "9z.png",
    "B8": "b8.png",
    "BetBoom": "betb.png",
    "GamerLegion": "gl.png",
    "M80": "m80.png",
    "MIBR": "mibr.png",
    "TYLOO": "tylo.png",
    "BIG": "big.png",
    "FlyQuest": "fly.png",
}

MAJOR_HOMEWORK_CATEGORIES = ["3-0", "3-1/3-2", "0-3"]
MAJOR_HOMEWORK_CATEGORY_SLOTS = {
    "3-0": 2,
    "3-1/3-2": 6,
    "0-3": 2,
}
MAJOR_PLAYOFF_CATEGORIES = PLAYOFF_CATEGORIES
MAJOR_PLAYOFF_CATEGORY_SLOTS = PLAYOFF_CATEGORY_SLOTS

def _major_team_logo(team: str) -> str | None:
    if logo := MAJOR_TEAM_LOGOS.get(team):
        return f"/team-icons/2026-cologne/{logo}"
    return None

def _major_safe_float(value: float) -> float | None:
    if value is None or not math.isfinite(value):
        return None
    return value

def _major_sort_value(value: float) -> float:
    safe_value = _major_safe_float(value)
    return safe_value if safe_value is not None else -1.0

def _major_records(games: list) -> dict[str, tuple[int, int]]:
    records: dict[str, list[int]] = {team: [0, 0] for team in major_teams}
    for game in games:
        if not isinstance(game, (list, tuple)) or len(game) < 2:
            continue
        winner, loser = game[0], game[1]
        if not isinstance(winner, str) or not isinstance(loser, str):
            continue
        winner = get_major_team_name(winner)
        loser = get_major_team_name(loser)
        records.setdefault(winner, [0, 0])[0] += 1
        records.setdefault(loser, [0, 0])[1] += 1
    return {team: (record[0], record[1]) for team, record in records.items()}

def _major_record_status(wins: int, losses: int) -> str:
    if wins >= 3:
        return "3-0" if losses == 0 else "advanced"
    if losses >= 3:
        return "0-3" if wins == 0 else "eliminated"
    return "pending"

def _major_pick_status(category: str, wins: int, losses: int) -> str:
    if category == "3-0":
        if wins >= 3 and losses == 0:
            return "correct"
        if losses > 0 or (wins >= 3 and losses > 0):
            return "wrong"
        return "pending"
    if category == "3-1/3-2":
        if wins >= 3 and losses > 0:
            return "correct"
        if losses >= 3 or (wins >= 3 and losses == 0):
            return "wrong"
        return "pending"
    if category == "0-3":
        if losses >= 3 and wins == 0:
            return "correct"
        if wins > 0 or (losses >= 3 and wins > 0):
            return "wrong"
        return "pending"
    return "pending"

def _major_pick(team: str, category: str, records: dict[str, tuple[int, int]]) -> MajorHomeworkPick:
    wins, losses = records.get(team, (0, 0))
    return MajorHomeworkPick(
        team=team,
        category=category,
        status=_major_pick_status(category, wins, losses),
        logo=_major_team_logo(team),
        wins=wins,
        losses=losses,
        recordStatus=_major_record_status(wins, losses),
    )

def _major_unknown_pick(category: str) -> MajorHomeworkPick:
    return MajorHomeworkPick(
        team="?",
        category=category,
        status="pending",
        logo=None,
        wins=0,
        losses=0,
        recordStatus="pending",
    )

def _major_team_order(team: str) -> int:
    try:
        return major_teams.index(team)
    except ValueError:
        return len(major_teams)

def _major_sort_picks(picks: list[MajorHomeworkPick]) -> list[MajorHomeworkPick]:
    return sorted(picks, key=lambda pick: _major_team_order(pick.team))

def _major_result_picks(records: dict[str, tuple[int, int]]) -> dict[str, list[MajorHomeworkPick]]:
    grouped = {category: [] for category in MAJOR_HOMEWORK_CATEGORIES}
    for team in major_teams:
        wins, losses = records.get(team, (0, 0))
        if wins >= 3 and losses == 0:
            grouped["3-0"].append(_major_pick(team, "3-0", records))
        elif wins >= 3 and losses > 0:
            grouped["3-1/3-2"].append(_major_pick(team, "3-1/3-2", records))
        elif losses >= 3 and wins == 0:
            grouped["0-3"].append(_major_pick(team, "0-3", records))

    for category, picks in grouped.items():
        picks = _major_sort_picks(picks)[:MAJOR_HOMEWORK_CATEGORY_SLOTS[category]]
        while len(picks) < MAJOR_HOMEWORK_CATEGORY_SLOTS[category]:
            picks.append(_major_unknown_pick(category))
        grouped[category] = picks
    return grouped

def _major_stage_data() -> dict:
    try:
        with (Path("assets") / f"{major_stage_name}.json").open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        logger.exception("failed to load major stage data")
        return {}

def _playoff_start_match_count() -> int:
    data = _major_stage_data()
    try:
        return int(data.get("playoff_start_match_count", 33))
    except Exception:
        return 33

def _playoff_games(games: list) -> list:
    chronological_games = list(reversed(games))
    start = _playoff_start_match_count()
    return chronological_games[start:start + 7]

def _playoff_match_detail(game) -> tuple[str, str] | None:
    if not isinstance(game, (list, tuple)) or len(game) < 2:
        return None
    if not isinstance(game[0], str) or not isinstance(game[1], str):
        return None
    return get_major_team_name(game[0]), get_major_team_name(game[1])

def _playoff_state(games: list) -> dict[str, object]:
    playoff_games = _playoff_games(games)
    rounds = {
        "4强": playoff_games[:4],
        "2强": playoff_games[4:6],
        "冠军": playoff_games[6:7],
    }
    winners: dict[str, list[str]] = {category: [] for category in MAJOR_PLAYOFF_CATEGORIES}
    losers: dict[str, list[str]] = {category: [] for category in MAJOR_PLAYOFF_CATEGORIES}
    records: dict[str, list[int]] = {team: [0, 0] for team in major_teams}

    for category, round_games in rounds.items():
        for game in round_games:
            detail = _playoff_match_detail(game)
            if detail is None:
                continue
            winner, loser = detail
            winners[category].append(winner)
            losers[category].append(loser)
            records.setdefault(winner, [0, 0])[0] += 1
            records.setdefault(loser, [0, 0])[1] += 1

    eliminated = set(losers["4强"]) | set(losers["2强"]) | set(losers["冠军"])
    return {
        "winners": winners,
        "losers": losers,
        "eliminated": eliminated,
        "records": {team: (record[0], record[1]) for team, record in records.items()},
    }

def _playoff_record_status(team: str, state: dict[str, object]) -> str:
    winners = state["winners"]
    losers = state["losers"]
    if team in winners["冠军"]:
        return "champion"
    if team in losers["冠军"]:
        return "runner-up"
    if team in losers["2强"] or team in winners["2强"]:
        return "finalist"
    if team in losers["4强"] or team in winners["4强"]:
        return "semifinalist"
    return "pending"

def _playoff_pick_status(team: str, category: str, state: dict[str, object]) -> str:
    winners = state["winners"]
    losers = state["losers"]
    eliminated = state["eliminated"]
    if team in winners[category]:
        return "correct"
    if team in eliminated:
        return "wrong"
    finished_slots = len(winners[category])
    if finished_slots >= MAJOR_PLAYOFF_CATEGORY_SLOTS[category] and team not in winners[category]:
        return "wrong"
    return "pending"

def _playoff_pick(team: str, category: str, state: dict[str, object]) -> MajorHomeworkPick:
    records = state["records"]
    wins, losses = records.get(team, (0, 0))
    return MajorHomeworkPick(
        team=team,
        category=category,
        status=_playoff_pick_status(team, category, state),
        logo=_major_team_logo(team),
        wins=wins,
        losses=losses,
        recordStatus=_playoff_record_status(team, state),
    )

def _playoff_result_picks(state: dict[str, object]) -> dict[str, list[MajorHomeworkPick]]:
    winners = state["winners"]
    grouped: dict[str, list[MajorHomeworkPick]] = {}
    for category in MAJOR_PLAYOFF_CATEGORIES:
        picks = _major_sort_picks([
            _playoff_pick(team, category, state)
            for team in winners[category]
        ])[:MAJOR_PLAYOFF_CATEGORY_SLOTS[category]]
        while len(picks) < MAJOR_PLAYOFF_CATEGORY_SLOTS[category]:
            picks.append(_major_unknown_pick(category))
        grouped[category] = picks
    return grouped

async def _major_playoff_homework_members(state: dict[str, object]) -> list[MajorHomeworkRankItem]:
    quad_rows = await db_major_hw.get_all_hw(major_stage_name + "-quad")
    semi_rows = await db_major_hw.get_all_hw(major_stage_name + "-semi")
    final_rows = await db_major_hw.get_all_hw(major_stage_name + "-final")
    rows_by_stage = {
        "4强": {row.uid: row for row in quad_rows},
        "2强": {row.uid: row for row in semi_rows},
        "冠军": {row.uid: row for row in final_rows},
    }
    uids = set().union(*(set(rows.keys()) for rows in rows_by_stage.values()))

    players: list[MajorHomeworkRankItem] = []
    for uid in uids:
        grouped: dict[str, list[MajorHomeworkPick]] = {}
        teams_by_category: dict[str, list[str]] = {}
        for category in MAJOR_PLAYOFF_CATEGORIES:
            row = rows_by_stage[category].get(uid)
            raw_teams = json.loads(row.teams) if row else []
            teams = [
                get_major_team_name(team)
                for team in raw_teams
                if isinstance(team, str)
            ]
            teams_by_category[category] = teams
            picks = _major_sort_picks([
                _playoff_pick(team, category, state)
                for team in teams
            ])[:MAJOR_PLAYOFF_CATEGORY_SLOTS[category]]
            grouped[category] = picks

        round_statuses = {
            category: playoff_category_status(
                teams_by_category.get(category, []),
                category,
                state["winners"],
                state["eliminated"],
            )
            for category in MAJOR_PLAYOFF_CATEGORIES
        }
        score = sum(1 for status in round_statuses.values() if status == "correct")
        status_parts = [
            PLAYOFF_STATUS_LABELS[round_statuses[category]]
            for category in MAJOR_PLAYOFF_CATEGORIES
        ]
        players.append(MajorHomeworkRankItem(
            uid=uid,
            avatar=f"https://q1.qlogo.cn/g?b=qq&nk={uid}&s=100",
            probability=None,
            expected=float(score),
            score=float(score),
            scoreLabel=" / ".join(status_parts),
            picks=grouped,
        ))

    return sorted(
        players,
        key=lambda player: (
            player.score or 0.0,
            sum(1 for pick in player.picks.get("冠军", []) if pick.status == "pending"),
            sum(1 for pick in player.picks.get("2强", []) if pick.status == "pending"),
            sum(1 for pick in player.picks.get("4强", []) if pick.status == "pending"),
        ),
        reverse=True,
    )

def _major_records_at_count(games: list, match_count: int) -> dict[str, tuple[int, int]]:
    chronological_games = list(reversed(games))
    return _major_records(chronological_games[:max(0, match_count)])

def _major_match_event_detail(games: list, match_count: int) -> tuple[str, str, str] | None:
    if match_count <= 0:
        return None
    chronological_games = list(reversed(games))
    index = match_count - 1
    if index >= len(chronological_games):
        return None
    game = chronological_games[index]
    if not isinstance(game, (list, tuple)) or len(game) < 3:
        return None
    winner = get_major_team_name(game[0])
    loser = get_major_team_name(game[1])
    return winner, loser, str(game[2])

def _major_match_event(games: list, match_count: int) -> str | None:
    if match_count <= 0:
        return None
    chronological_games = list(reversed(games))
    index = match_count - 1
    if index >= len(chronological_games):
        return f"第 {match_count} 场赛果更新"
    game = chronological_games[index]
    if not isinstance(game, (list, tuple)) or len(game) < 3:
        return f"第 {match_count} 场赛果更新"
    winner = get_major_team_name(game[0])
    loser = get_major_team_name(game[1])
    score = game[2]
    return f"{winner} 胜 {loser} {score}"

def _major_homework_text_to_grouped_picks(homework_text: str, records: dict[str, tuple[int, int]]) -> dict[str, list[MajorHomeworkPick]]:
    grouped: dict[str, list[str]]
    try:
        raw = json.loads(homework_text)
        if isinstance(raw, dict):
            grouped = {
                "3-0": list(raw.get("3-0", [])),
                "3-1/3-2": list(raw.get("3-1/3-2", [])),
                "0-3": list(raw.get("0-3", [])),
            }
        elif isinstance(raw, list):
            grouped = {
                "3-0": raw[:2],
                "3-1/3-2": raw[2:8],
                "0-3": raw[8:],
            }
        else:
            grouped = {category: [] for category in MAJOR_HOMEWORK_CATEGORIES}
    except Exception:
        grouped = {category: [] for category in MAJOR_HOMEWORK_CATEGORIES}

    return {
        category: _major_sort_picks([
            _major_pick(get_major_team_name(team), category, records)
            for team in grouped.get(category, [])
            if isinstance(team, str)
        ])
        for category in MAJOR_HOMEWORK_CATEGORIES
    }

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

@app.post(
    "/api/major/homework/rank",
    response_model=MajorHomeworkRankResponse,
    summary="Get Major homework ranking",
    description="Return current Major homework picks with deterministic result states.",
)
async def get_major_homework_rank(_ = Depends(get_current_user)):
    if major_hw_config.major_stage == "playoffs":
        games = json.loads(await local_storage.get(f"hltvresult{major_hw_config.major_event_id}", default="[]"))
        state = _playoff_state(games)
        return MajorHomeworkRankResponse(
            stage=major_stage_name,
            stageType="playoffs",
            categories=MAJOR_PLAYOFF_CATEGORIES,
            teams=major_teams,
            resultPicks=_playoff_result_picks(state),
            players=await _major_playoff_homework_members(state),
        )

    games = json.loads(await local_storage.get(f"hltvresult{major_hw_config.major_event_id}", default="[]"))
    records = _major_records(games)
    result_picks = _major_result_picks(records)
    members = await db_major_hw.get_all_hw(major_stage_name)
    members = sorted(members, key=lambda item: _major_sort_value(item.winrate), reverse=True)

    players: list[MajorHomeworkRankItem] = []
    for member in members:
        teams = json.loads(member.teams)
        grouped = {
            "3-0": _major_sort_picks([_major_pick(team, "3-0", records) for team in teams[:2]]),
            "3-1/3-2": _major_sort_picks([_major_pick(team, "3-1/3-2", records) for team in teams[2:8]]),
            "0-3": _major_sort_picks([_major_pick(team, "0-3", records) for team in teams[8:]]),
        }
        players.append(MajorHomeworkRankItem(
            uid=member.uid,
            avatar=f"https://q1.qlogo.cn/g?b=qq&nk={member.uid}&s=100",
            probability=_major_safe_float(member.winrate),
            expected=_major_safe_float(member.expval),
            picks=grouped,
        ))

    return MajorHomeworkRankResponse(
        stage=major_stage_name,
        stageType="swiss",
        categories=MAJOR_HOMEWORK_CATEGORIES,
        teams=major_teams,
        resultPicks=result_picks,
        players=players,
    )

@app.post(
    "/api/major/homework/history",
    response_model=MajorHomeworkHistoryResponse,
    summary="Get Major homework probability history",
    description="Return saved homework probabilities keyed by finished match count.",
)
async def get_major_homework_history(_ = Depends(get_current_user)):
    async with async_session_factory() as session:
        stmt = (
            select(MajorHWSnapshot)
            .where(MajorHWSnapshot.stage == major_stage_name)
            .order_by(MajorHWSnapshot.uid, MajorHWSnapshot.homework_text, MajorHWSnapshot.match_count)
        )
        result = await session.execute(stmt)
        snapshots = list(result.scalars().all())

    grouped: dict[str, list[MajorHomeworkHistoryPoint]] = {}
    for snapshot in snapshots:
        grouped.setdefault(snapshot.uid, []).append(MajorHomeworkHistoryPoint(
            matchCount=snapshot.match_count,
            createdAt=snapshot.created_at,
            homeworkText=snapshot.homework_text,
            probability=_major_safe_float(snapshot.winrate),
            expected=_major_safe_float(snapshot.expval),
        ))

    return MajorHomeworkHistoryResponse(
        stage=major_stage_name,
        players=[
            MajorHomeworkHistoryItem(
                uid=uid,
                avatar=f"https://q1.qlogo.cn/g?b=qq&nk={uid}&s=100",
                points=points,
            )
            for uid, points in grouped.items()
        ],
    )

@app.post(
    "/api/major/homework/personal",
    response_model=MajorHomeworkPersonalResponse,
    summary="Get personal Major homework history",
    description="Return current user's homework snapshots with event labels and probability changes.",
)
async def get_major_homework_personal(
    uid: str | None = Body(None, embed=True),
    info: AuthSession = Depends(get_current_user),
):
    if not info.user_id:
        raise HTTPException(status_code=401, detail="未绑定 QQ")
    if major_hw_config.major_stage == "playoffs":
        raise HTTPException(status_code=400, detail="Playoffs homework history is not supported on web yet")
    target_uid = str(uid).strip() if uid else info.user_id
    if not target_uid:
        raise HTTPException(status_code=400, detail="Invalid target user")

    games = json.loads(await local_storage.get(f"hltvresult{major_hw_config.major_event_id}", default="[]"))
    async with async_session_factory() as session:
        stmt = (
            select(MajorHWSnapshot)
            .where(MajorHWSnapshot.stage == major_stage_name)
            .where(MajorHWSnapshot.uid == target_uid)
            .order_by(MajorHWSnapshot.match_count, MajorHWSnapshot.created_at, MajorHWSnapshot.homework_text)
        )
        result = await session.execute(stmt)
        snapshots = list(result.scalars().all())

    rows: list[MajorHomeworkPersonalRow] = []
    previous_probability: float | None = None
    previous_match_count: int | None = None
    previous_homework_text: str | None = None
    for snapshot in snapshots:
        probability = _major_safe_float(snapshot.winrate)
        probability_change = None
        match_event: tuple[str, str, str] | None = None
        if probability is not None and previous_probability is not None:
            probability_change = probability - previous_probability

        if previous_match_count is None:
            event = "初始作业"
        elif snapshot.match_count == previous_match_count and snapshot.homework_text != previous_homework_text:
            event = "修改作业"
        else:
            match_event = _major_match_event_detail(games, snapshot.match_count)
            event = _major_match_event(games, snapshot.match_count) or "初始作业"

        records = _major_records_at_count(games, snapshot.match_count)
        rows.append(MajorHomeworkPersonalRow(
            matchCount=snapshot.match_count,
            createdAt=snapshot.created_at,
            event=event,
            eventWinner=match_event[0] if match_event else None,
            eventWinnerLogo=_major_team_logo(match_event[0]) if match_event else None,
            eventLoser=match_event[1] if match_event else None,
            eventLoserLogo=_major_team_logo(match_event[1]) if match_event else None,
            eventScore=match_event[2] if match_event else None,
            homeworkText=snapshot.homework_text,
            probability=probability,
            probabilityChange=probability_change,
            expected=_major_safe_float(snapshot.expval),
            picks=_major_homework_text_to_grouped_picks(snapshot.homework_text, records),
        ))
        previous_probability = probability
        previous_match_count = snapshot.match_count
        previous_homework_text = snapshot.homework_text

    return MajorHomeworkPersonalResponse(
        stage=major_stage_name,
        uid=target_uid,
        avatar=f"https://q1.qlogo.cn/g?b=qq&nk={target_uid}&s=100",
        categories=MAJOR_HOMEWORK_CATEGORIES,
        rows=rows,
    )

class AIRecordIdsResponse(BaseModel):
    isEnd: bool = Field(..., description="是否已结束生成")
    recordIds: list[int] = Field(..., description="此聊天记录编号列表")

class AIAskResponse(BaseModel):
    chatId: str = Field(..., description="AI chat id")

async def _run_web_ai_chat(chat_id: str, uid: str, sid: str, prompt: str, persona: str | None) -> None:
    try:
        await ai_ask_main(uid, sid, persona, prompt, chat_id=chat_id)
    except Exception as e:
        logger.exception("web ai chat failed")
        await db_ai.insert_chat_record(chat_id, "assistant", f"AI请求失败: {e}", None, None, True)

@app.post("/api/ai/ask",
    response_model=AIAskResponse,
    summary="创建AI聊天",
    description="从网页创建一次AI聊天，并返回聊天记录 ID。"
)
async def ask_ai_from_web(
    prompt: str = Body(..., embed=True),
    persona: str | None = Body(None, embed=True),
    info: AuthSession = Depends(get_current_user),
):
    if not info.user_id or not info.group_id:
        raise HTTPException(status_code=401, detail="未绑定 QQ 或群")
    prompt = prompt.strip()
    if not prompt:
        raise HTTPException(status_code=400, detail="Prompt is empty")
    chat_id = str(uuid.uuid4())
    sid = f"group_{info.group_id}_{info.user_id}"
    asyncio.create_task(_run_web_ai_chat(chat_id, info.user_id, sid, prompt, persona))
    return AIAskResponse(chatId=chat_id)

@app.post("/api/ai/recordids",
    response_model=AIRecordIdsResponse,
    summary="获取AI聊天记录编号列表",
    description="获取当前认证 Token 所在群组的AI聊天记录编号列表。"
)
async def get_ai_record_ids(chatId: str=Body(..., embed=True), _: AuthSession = Depends(get_current_user)):
    is_end, record_ids = await db_ai.get_chat_records_id(chatId)
    return AIRecordIdsResponse(
        isEnd=is_end,
        recordIds=record_ids
    )

class AiRecordResponse(BaseModel):
    timestamp: int = Field(..., description="聊天记录时间戳")
    role: str = Field(..., description="角色")
    content: str | None = Field(..., description="聊天内容")
    tools: str | None = Field(..., description="工具使用情况")
    reasons: str | None = Field(..., description="思考过程")
@app.post("/api/ai/record",
    response_model=AiRecordResponse,
    summary="获取AI聊天记录内容",
    description="根据聊天记录编号获取AI聊天记录的详细内容。"
)
async def get_ai_record(recordId: int=Body(..., embed=True), _: AuthSession = Depends(get_current_user)):
    record = await db_ai.get_chat_record(recordId)
    if not record:
        raise HTTPException(status_code=404, detail="Record not found")
    return AiRecordResponse(
        timestamp=record.timestamp,
        role=record.role,
        content=record.content,
        tools=record.tool_calls,
        reasons=record.reasoning_content
    )
