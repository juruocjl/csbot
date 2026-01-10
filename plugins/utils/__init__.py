from nonebot import get_plugin_config
from nonebot import get_driver
from nonebot.adapters import Bot
from nonebot import logger
from nonebot import require

from typing import overload
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
import datetime
import time
import aiohttp
import os
from pyppeteer import launch
import asyncio
from pathlib import Path
from alembic.runtime.migration import MigrationContext
from alembic.autogenerate import compare_metadata

from nonebot.plugin import PluginMetadata

require("models")
from ..models import StorageItem, Base

from .config import Config



__plugin_meta__ = PluginMetadata(
    name="utils",
    description="",
    usage="",
    config=Config,
)

config = get_plugin_config(Config)

logger.info("开始加载基本组件...")

temp_dir = Path("temp")
temp_dir.mkdir(exist_ok=True)

img_dir = Path("imgs")
img_dir.mkdir(exist_ok=True)

avatar_dir = img_dir / "avatar"
avatar_dir.mkdir(exist_ok=True)

goods_dir = img_dir / "goodsimg"
goods_dir.mkdir(exist_ok=True)


driver = get_driver()

def get_today_start_timestamp(refreshtime = 0):
    today = datetime.date.today()
    today_start = datetime.datetime.combine(today, datetime.time.min)
    timestamp = int(time.mktime(today_start.timetuple()))
    now = time.time()
    if now - timestamp < refreshtime:
        timestamp = timestamp - 86400 + refreshtime
    else:
        timestamp = timestamp + refreshtime
    return timestamp

def output(val, format):
    if format.startswith("d"):
        return f"{val: .{int(format[1:])}f}"
    elif format.startswith("p"):
        return f"{val * 100: .{int(format[1:])}f}%"



engine = create_async_engine(
    config.cs_database,
    pool_pre_ping=True,
    pool_recycle=3600,
)
async_session_factory = async_sessionmaker(engine, expire_on_commit=False)

class LocalStorage:
    async def set(self, key: str, val: str) -> None:
        async with async_session_factory() as session:
            async with session.begin():
                new_item = StorageItem(key=key, val=val)
                await session.merge(new_item)

    @overload
    async def get(self, key: str) -> str | None: ...

    @overload
    async def get(self, key: str, default: str) -> str: ...

    @overload
    async def get(self, key: str, default: None) -> str | None: ...

    async def get(self, key: str, default: str | None = None) -> str | None:
        async with async_session_factory() as session:
            item = await session.get(StorageItem, key)
            return item.val if item else default

session = None
local_storage = LocalStorage()

@driver.on_startup
async def init_session():
    global session
    session = aiohttp.ClientSession()

    logger.info("正在检查数据库结构与模型定义是否一致...")
    
    # 定义同步检查函数
    def check_diff(connection):
        # 配置迁移上下文
        opts = {
            'compare_type': True,  # 检查字段类型改变
            'compare_server_default': True, # 检查默认值改变
        }
        mc = MigrationContext.configure(connection, opts=opts)
        
        # 执行对比：返回差异列表
        diff = compare_metadata(mc, Base.metadata)
        return diff


    async with engine.connect() as conn:
        # 运行同步对比逻辑
        diff = await conn.run_sync(check_diff)

    if diff:
        logger.warning("!!! 检测到数据库结构变更 !!!")
        raise RuntimeError("数据库结构与代码模型不一致，已停止启动！")
    else:
        logger.success("数据库结构一致，继续启动。")

def get_session() -> aiohttp.ClientSession:
    global session
    assert session is not None, "Session 未初始化完成"
    return session



def path_to_file_url(path: str | Path) -> str:
    absolute_path = os.path.abspath(str(path))
    return 'file://' + absolute_path

async def screenshot_html_to_png(url: str, width: int, height: int):
    browser = await launch(headless=True, args=['--no-sandbox', '--disable-setuid-sandbox'])
    page = await browser.newPage()
    await page.setViewport({'width': width, 'height': height})
    await page.goto(url)
    await asyncio.sleep(1)
    image = await page.screenshot()
    await browser.close()
    return image

async def getcard(bot: Bot, gid: str, uid: str):
    try:
        info = await bot.get_group_member_info(group_id=gid, user_id=uid, no_cache=False)
        if info["card"]:
            return info["card"]
        return info["nickname"]
    except:
        try:
            info = await bot.get_stranger_info(user_id=uid, no_cache=False)
            return info["nickname"]
        except:
            return uid