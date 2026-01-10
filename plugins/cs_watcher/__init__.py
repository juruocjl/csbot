from nonebot import get_plugin_config
from nonebot import require
from nonebot import on_command
from nonebot.params import CommandArg
from nonebot.adapters.onebot.v11 import Message, GroupMessageEvent, Bot
from nonebot import get_driver
from nonebot.plugin import PluginMetadata
from nonebot import logger

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
import time
from collections import defaultdict
import asyncio

require("nonebot_plugin_apscheduler")
require("cs_db_val")
require("cs_db_upd")
require("models")
require("utils")

from nonebot_plugin_apscheduler import scheduler


from ..cs_db_val import db as db_val
from ..cs_db_upd import db as db_upd
from ..models import UserPlayStatus
from ..utils import get_session
from ..utils import async_session_factory
from ..utils import getcard
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
    
    async def process_update_queue(self):
        """持续处理队列中的玩家数据更新"""
        while True:
            try:
                # 阻塞等待队列有数据
                steamid = await self.update_queue.get()
                is_locking_error = False
                try:
                    logger.info(f"开始更新玩家数据: {steamid}")
                    await db_upd.update_stats(steamid)
                    logger.info(f"玩家数据更新成功: {steamid}")
                except db_upd.LockingError:
                    is_locking_error = True
                    logger.warning(f"数据库被锁定，保留在队列中: {steamid}")
                    # 重新放回队列，下次继续重试
                    self.update_queue.put_nowait(steamid)
                    await asyncio.sleep(1)
                except db_upd.TooFrequentError as e:
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
                

db = DataManager()

@scheduler.scheduled_job("interval", minutes=1, id="cs_watcher_job")
async def cs_watcher_job():
    """定时检查用户的游戏状态"""
    logger.info("执行游戏状态检查任务...")
    steamids = await db_val.get_all_steamid()
    url = "https://api.steampowered.com/ISteamUser/GetPlayerSummaries/v0002/"
    params = {
        "key": config.cs_steamkey,
        "steamids": ",".join(steamids),
    }
    async with get_session().get(url, params=params, proxy=config.cs_proxy) as resp:
        data = await resp.json()
        players = data["response"]["players"]
    timeStamp = int(time.time())
    for player in players:
        try:
            steamid: str = player["steamid"]
            gameid: int = int(player.get("gameid", "-1"))
            gamename: str = player.get("gameextrainfo", "")
            result = await db.set_game_status(steamid, gameid, gamename, timeStamp)
            if result:
                logger.info(f"Player {steamid} has ended playing CS:GO.")
                await db.add_queue(steamid)
        except Exception as e:
            logger.error(f"Error processing player data: {e}")

# 启动后台队列处理任务
driver = get_driver()

@driver.on_startup
async def startup_queue_processor():
    """在应用启动时启动队列处理循环"""
    asyncio.create_task(db.process_update_queue())

game_status = on_command("gamestatus", aliases={"游戏状态"}, priority=10)
@game_status.handle()
async def handle_game_status(bot: Bot, msg: GroupMessageEvent, args: Message = CommandArg()):
    """处理游戏状态查询命令"""
    uids = []
    for seg in args:
        if seg.type == "qq":
            uids.append(str(seg.data["qq"]))
    if not uids:
        for uid in await db_val.get_group_member(msg.group_id):
            if steamid := await db_val.get_steamid(uid):
                uids.append(str(uid))
    try:
        gamedict: dict[str, list[str]] = defaultdict(list)
        for uid in uids:
            if steamid := await db_val.get_steamid(uid):
                status = await db.get_game_status(steamid)
                if status and status.gameId != -1:
                    gamedict[status.gameName].append(uid)
                else:
                    # gamedict["未在游戏中"].append(uid)
                    pass
            else:
                gamedict["未绑定Steam"].append(uid)
    except Exception as e:
        await game_status.finish(f"查询游戏状态时出错: {e}")
    result = ""
    for gamename, members in gamedict.items():
        result += f"{gamename}\n"
        for member in members:
            card = await getcard(bot, str(msg.group_id), member)
            result += f"  > {card}\n"
    result = result.strip()
    if not result:
        result = "群成员均未在游戏中"
    await game_status.finish(result)