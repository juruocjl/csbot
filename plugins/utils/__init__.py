from nonebot import get_plugin_config
from nonebot.plugin import PluginMetadata

from .config import Config

import datetime
import time
import sqlite3

__plugin_meta__ = PluginMetadata(
    name="utils",
    description="",
    usage="",
    config=Config,
)

config = get_plugin_config(Config)


conn = sqlite3.connect("groups.db", autocommit=True)

def get_cursor():
    return conn.cursor()

def get_today_start_timestamp():
    today = datetime.date.today()
    today_start = datetime.datetime.combine(today, datetime.time.min)
    timestamp = int(time.mktime(today_start.timetuple()))
    return timestamp
