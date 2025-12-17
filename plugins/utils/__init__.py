from nonebot import get_plugin_config
from nonebot import get_driver
from nonebot import require
from nonebot import logger
from typing import overload

from sqlalchemy import String, Text
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase, MappedAsDataclass, Mapped, mapped_column

import datetime
import time
import aiohttp
import os
from pyppeteer import launch
import asyncio
from pathlib import Path

from nonebot.plugin import PluginMetadata

from .config import Config



__plugin_meta__ = PluginMetadata(
    name="utils",
    description="",
    usage="",
    config=Config,
)

config = get_plugin_config(Config)

logger.info("开始加载基本组件...")

driver = get_driver()

if not os.path.exists("temp"):
    os.makedirs("temp", exist_ok=True)

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

class Base(MappedAsDataclass, DeclarativeBase):
    pass

class StorageItem(Base):
    __tablename__ = "local_storage"

    key: Mapped[str] = mapped_column(String(255), primary_key=True)
    val: Mapped[str] = mapped_column(Text)


engine = create_async_engine("sqlite+aiosqlite:///main.db")
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
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    logger.info("表结构初始化完成！")

def get_session() -> aiohttp.ClientSession:
    global session
    assert session is not None, "Session 未初始化完成"
    return session

async def async_download(url: str, file_path: str | Path) -> None:
    async with get_session().get(url) as response:
        if response.status == 200:
            with open(file_path, "wb") as f:
                while True:
                    chunk = await response.content.read(1024)
                    if not chunk:
                        break
                    f.write(chunk)
        else:
            raise Exception(f"下载失败，状态码：{response.status}")
        
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