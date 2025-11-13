from nonebot import get_plugin_config
from nonebot import get_driver

from nonebot.plugin import PluginMetadata

from .config import Config

import datetime
import time
import sqlite3
import aiohttp
import os
from pyppeteer import launch
import asyncio


__plugin_meta__ = PluginMetadata(
    name="utils",
    description="",
    usage="",
    config=Config,
)

config = get_plugin_config(Config)

driver = get_driver()

conn = sqlite3.connect("groups.db", autocommit=True)

if not os.path.exists("temp"):
    os.makedirs("temp", exist_ok=True)

def get_cursor():
    return conn.cursor()

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


class LocalStorage:
    def __init__(self):
        cursor = get_cursor()
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS local_storage (
            key TEXT,
            val TEXT,
            PRIMARY KEY (key)
        )
        ''')
    
    def set(self, key: str, val: str) -> None:
        cursor = get_cursor()
        cursor.execute('''
            INSERT OR REPLACE INTO local_storage (key, val)
            VALUES (?, ?)
        ''', (key, val))
    
    def get(self, key: str) -> str | None:
        cursor = get_cursor()
        cursor.execute('''
            SELECT val FROM local_storage WHERE key == ?
        ''', (key,))
        result = cursor.fetchone()
        return result[0] if result else None

localstorage = LocalStorage()

session = None

@driver.on_startup
async def init_session():
    global session
    session = aiohttp.ClientSession()

def get_session():
    global session
    return session

async def async_download(url, file_path):
    async with session.get(url) as response:
        if response.status == 200:
            with open(file_path, "wb") as f:
                while True:
                    chunk = await response.content.read(1024)
                    if not chunk:
                        break
                    f.write(chunk)
            return file_path
        else:
            raise Exception(f"下载失败，状态码：{response.status}")
        
def path_to_file_url(path):
    absolute_path = os.path.abspath(path)
    return 'file://' + absolute_path

async def screenshot_html_to_png(url, width, height):
    browser = await launch(headless=True, args=['--no-sandbox', '--disable-setuid-sandbox'])
    page = await browser.newPage()
    await page.setViewport({'width': width, 'height': height})
    await page.goto(url)
    await asyncio.sleep(1)
    image = await page.screenshot()
    await browser.close()
    return image