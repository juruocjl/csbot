from nonebot import get_plugin_config
from nonebot import require
from nonebot import on_command
from nonebot.adapters.onebot.v11 import Message, MessageSegment, Bot
from nonebot import get_driver
from nonebot import get_bot
from nonebot.plugin import PluginMetadata
from nonebot import logger

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
import time
import asyncio

require("nonebot_plugin_apscheduler")
require("cs_db_val")
require("cs_db_upd")
require("cs_server")
require("models")
require("utils")

from nonebot_plugin_apscheduler import scheduler


from ..cs_db_val import db as db_val
from ..cs_db_upd import db as db_upd
from ..cs_db_upd import LockingError, TooFrequentError
from ..cs_server import db as db_server
from ..cs_server import get_screenshot
from ..cs_server import _fetch_steam_status_payload
from ..models import UserPlayStatus
from ..utils import async_session_factory
from .config import Config

__plugin_meta__ = PluginMetadata(
    name="cs_watcher",
    description="",
    usage="",
    config=Config,
)

config = get_plugin_config(Config)

class DataManager:
    def __init__(self):
        self.update_queue: asyncio.Queue[str] = asyncio.Queue()
        self.queue_set: set[str] = set()  # 跟踪队列中的 steamid
        self.queue_lock: asyncio.Lock = asyncio.Lock()  # 保护 queue_set 的访问
        self.last_game_state: dict[str, str] = {}
    
    async def _get_game_status(self, steamid: str, session: AsyncSession) -> UserPlayStatus | None:
        stmt = (
            select(UserPlayStatus)
            .where(UserPlayStatus.steamid == steamid)
            .order_by(UserPlayStatus.timeStamp.desc())
            .limit(1)
        )
        result = await session.execute(stmt)
        return result.scalars().first()
    
    async def get_game_status(self, steamid: str) -> UserPlayStatus | None:
        async with async_session_factory() as session:
            return await self._get_game_status(steamid, session)
    
    async def set_game_status(self, steamid: str, gameid: int, gamename: str, timeStamp: int) -> bool:
        async with async_session_factory() as session:
            async with session.begin():
                result = await self._get_game_status(steamid, session)
                if result is None or result.gameId != gameid or result.gameName != gamename:
                    # 游戏状态改变，创建新记录（isFirst=True 记录开始时间）
                    new_status = UserPlayStatus(
                        steamid=steamid,
                        timeStamp=timeStamp,
                        isFirst=True,
                        gameId=gameid,
                        gameName=gamename,
                    )
                    await session.merge(new_status)
                    if result is not None and result.gameId == 730:
                        return True
                elif result.isFirst:
                    # 第一条记录时，创建第二条记录（isFirst=False 用于更新结束时间）
                    new_status = UserPlayStatus(
                        steamid=steamid,
                        timeStamp=timeStamp,
                        isFirst=False,
                        gameId=gameid,
                        gameName=gamename,
                    )
                    await session.merge(new_status)
                else:
                    # 非第一条记录时，直接更新时间戳（保持 isFirst=True 的开始时间）
                    result.timeStamp = timeStamp
                    await session.merge(result)
        return False
    
    async def add_queue(self, steamid: str):
        """加入更新队列（只有不在队列中的 steamid 才会被加入）"""
        async with self.queue_lock:
            if steamid not in self.queue_set:
                self.queue_set.add(steamid)
                self.update_queue.put_nowait(steamid)

    def get_last_game_state(self, steamid: str) -> str:
        return self.last_game_state.get(steamid, "")

    def set_last_game_state(self, steamid: str, game_state: str) -> None:
        self.last_game_state[steamid] = game_state
    
    async def process_update_queue(self):
        """持续处理队列中的玩家数据更新"""
        while True:
            try:
                # 阻塞等待队列有数据
                steamid = await self.update_queue.get()
                is_locking_error = False
                try:
                    logger.info(f"开始更新玩家数据: {steamid}")
                    nickname, pwlist, gplist = await db_upd.update_stats(steamid)
                    logger.info(f"玩家数据更新成功: {steamid} {nickname}")
                    if len(pwlist) + len(gplist) < 5:
                        await sendMatches(pwlist, gplist)
                except LockingError:
                    is_locking_error = True
                    logger.warning(f"数据库被锁定，保留在队列中: {steamid}")
                    # 重新放回队列，下次继续重试
                    self.update_queue.put_nowait(steamid)
                    await asyncio.sleep(1)
                except TooFrequentError as e:
                    logger.warning(f"更新过于频繁，跳过: {steamid}, {e}")
                except Exception as e:
                    logger.error(f"更新玩家数据失败: {steamid}, {e}")
                
                # 成功或失败（非 LockingError）都从队列集合中移除
                if not is_locking_error:
                    async with self.queue_lock:
                        self.queue_set.discard(steamid)
            except Exception as e:
                logger.error(f"队列处理异常: {e}")
                await asyncio.sleep(1)

async def sendMatches(pwlist, gplist):
    bot = get_bot()
    if isinstance(bot, Bot):
        for gid in config.cs_group_list:
            token = await db_server.get_bot_token(str(gid))
            for mid in pwlist:
                screenshot = await get_screenshot(f"/match?id={mid}", token)
                if screenshot:
                    await bot.send_group_msg(group_id=gid, message=Message(MessageSegment.image(screenshot)))
            for mid in gplist:
                screenshot = await get_screenshot(f"/match-gp?id={mid}", token)
                if screenshot:
                    await bot.send_group_msg(group_id=gid, message=Message(MessageSegment.image(screenshot)))
    else:
        logger.error("无法获取 Bot 实例，无法发送比赛通知")
        


db = DataManager()

@scheduler.scheduled_job("interval", minutes=1, id="cs_watcher_job")
async def cs_watcher_job():
    """定时检查用户的游戏状态"""
    logger.info("执行游戏状态检查任务...")
    steamids = await db_val.get_all_steamid()
    payload = await _fetch_steam_status_payload()
    raw_data = payload.get("data", [])
    players_by_steamid: dict[str, dict] = {}
    if isinstance(raw_data, list):
        for item in raw_data:
            if not isinstance(item, dict):
                continue
            sid = str(item.get("steamId") or item.get("steam_id") or "")
            if sid:
                players_by_steamid[sid] = item

    timeStamp = int(time.time())
    for steamid in steamids:
        try:
            player = players_by_steamid.get(steamid, {})

            gameid_raw = player.get("gameId") or player.get("gameid")
            gameid: int = int(gameid_raw) if gameid_raw is not None else -1
            gamename: str = str(player.get("gameName") or player.get("gameextrainfo") or "")
            result = await db.set_game_status(steamid, gameid, gamename, timeStamp)
            if result:
                logger.info(f"Player {steamid} has ended playing CS:GO.")
                await db.add_queue(steamid)

            rich_presence = player.get("richPresence") or player.get("rich_presence") or {}
            game_state = ""
            if isinstance(rich_presence, dict):
                game_state = str(rich_presence.get("game:state") or "").lower().strip()
            last_game_state = db.get_last_game_state(steamid)
            if game_state == "lobby" and last_game_state != "lobby":
                logger.info(f"Player {steamid} has entered lobby.")
                await db.add_queue(steamid)
            db.set_last_game_state(steamid, game_state)
        except Exception as e:
            logger.error(f"Error processing player data: {e}")

# 启动后台队列处理任务
driver = get_driver()

@driver.on_startup
async def startup_queue_processor():
    """在应用启动时启动队列处理循环"""
    asyncio.create_task(db.process_update_queue())
