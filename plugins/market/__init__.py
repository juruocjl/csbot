from nonebot import get_plugin_config
from nonebot.plugin import PluginMetadata
from nonebot.adapters.onebot.v11 import Message, MessageEvent
from nonebot.permission import SUPERUSER
from nonebot.params import CommandArg
from nonebot import on_command
from nonebot import get_bot
from nonebot import require
from nonebot import logger
from typing import List

import json
import time
import asyncio

from .config import Config

scheduler = require("nonebot_plugin_apscheduler").scheduler

get_cursor = require("utils").get_cursor
get_session = require("utils").get_session


__plugin_meta__ = PluginMetadata(
    name="market",
    description="",
    usage="",
    config=Config,
)

config = get_plugin_config(Config)

class DataManager:
    def __init__(self):
        cursor = get_cursor()
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS member_goods (
            uid TEXT,
            marketHashName TEXT,
            PRIMARY KEY (uid, marketHashName)
        )
        ''')

        cursor.execute('''
        CREATE TABLE IF NOT EXISTS goods_info (
            marketHashName TEXT,
            timeStamp INT,
            goodId INT,
            name TEXT,
            buffSellPrice INT,
            buffSellNum INT,
            yyypSellPrice INT,
            yyypSellNum INT,
            steamSellPrice INT,
            steamSellNum INT,
            PRIMARY KEY (marketHashName, timeStamp)
        )
        ''')
    
    def addgoods(self, uid: str, name: str):
        cursor = get_cursor()
        cursor.execute(
            'INSERT OR IGNORE INTO  member_goods (uid, marketHashName) VALUES (?, ?)',
            (uid, name)
        )
        
    async def update_goods(self, goods_list: List[str]):
        cursor = get_cursor()
        while len(goods_list) > 0:
            now_goods = goods_list[:50]
            goods_list = goods_list[50:]
            async with get_session().post("https://api.csqaq.com/api/v1/goods/getPriceByMarketHashName", data=json.dumps({"marketHashNameList": now_goods}),headers={'ApiToken': config.csqaq_api}) as res:
                data = await res.json()
            print(data)
            if data['code'] == 200:
                for marketHashName, good_info in data['data']['success'].items():
                    cursor.execute(
                        'INSERT INTO goods_info (marketHashName,timeStamp,goodId,name,buffSellPrice,buffSellNum,yyypSellPrice,yyypSellNum,steamSellPrice,steamSellNum) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
                        (marketHashName, time.time(), good_info['goodId'], good_info['name'],
                        round(good_info['buffSellPrice'] * 100), good_info['buffSellNum'],
                        round(good_info['yyypSellPrice'] * 100), good_info['yyypSellNum'],
                        round(good_info['steamSellPrice'] * 100), good_info['steamSellNum']
                        )
                    )
                    
            else:
                logger.error("update_goods "+data['msg'])
            await asyncio.sleep(1.1)

    def getallgoods(self) -> List[str]:
        cursor = get_cursor()
        cursor.execute('SELECT DISTINCT marketHashName FROM member_goods')
        res = cursor.fetchall()
        return [a[0] for a in res]
    
    def getgoodsinfo(self, marketHashName: str):
        cursor = get_cursor()
        cursor.execute('SELECT * FROM goods_info WHERE marketHashName == ? ORDER BY timeStamp DESC LIMIT 1', (marketHashName, ))
        return cursor.fetchone()

    def getgoodsinfo_time(self, marketHashName: str, TimeStamp: int):
        cursor = get_cursor()
        cursor.execute('SELECT * FROM goods_info WHERE marketHashName == ? and timeStamp >= ? ORDER BY timeStamp ASC LIMIT 1', (marketHashName, TimeStamp))
        return cursor.fetchone()

db = DataManager()

baojia = on_command("报价", priority=10, block=True)

search = on_command("搜索", priority=10, block=True)

addgoods = on_command("加仓", priority=10, block=True)

updallgoods = on_command("更新饰品", priority=10, block=True, permission=SUPERUSER)

def get_baojia(title: str = "当前底价"):
    allgoods = db.getallgoods()
    logger.info(allgoods)
    data = []
    for goods in allgoods:
        info = db.getgoodsinfo(goods)
        info1d = db.getgoodsinfo_time(goods, time.time() - 24 * 3600)
        info7d = db.getgoodsinfo_time(goods, time.time() - 7 * 24 * 3600)
        delta1d = str((info[6] - info1d[6]) / 100) if info[1] - info1d[1] >= 20 * 3600 else "无数据"
        delta7d = str((info[6] - info7d[6]) / 100) if info[1] - info7d[1] >= 6 * 24 * 3600 else "无数据"
        
        data.append((info[3], info[6], delta1d, delta7d))
    data = sorted(data, key = lambda x: x[1])
    return (title + "\n" + "\n".join([a[0] + "\n> " + str(a[1]/100) + "   Δ1d=" + a[2] + "   Δ7d=" + a[3] for a in data])).strip()

@baojia.handle()
async def baojia_function():
    await baojia.finish(get_baojia())

@search.handle()
async def search_function(args: Message = CommandArg()):
    async with get_session().get("https://api.csqaq.com/api/v1/search/suggest", params={"text": args.extract_plain_text()}, headers={'ApiToken': config.csqaq_api}) as res:
        data = await res.json()
    if data['code'] == 200:
        await search.finish(("搜索结果\n" + "\n".join([f"{item['id']}. {item['value']}" for item in data['data'][:10]])).strip())
    else:
        await search.finish("搜索出错 " + data['msg'])

@addgoods.handle()
async def addgoods_function(message: MessageEvent, args: Message = CommandArg()):
    uid = message.get_user_id()
    res = ""
    try:
        async with get_session().get("https://api.csqaq.com/api/v1/info/good", params={"id": args.extract_plain_text()}, headers={'ApiToken': config.csqaq_api}) as res:
            data = await res.json()
        asyncio.sleep(1.1)
        await db.update_goods([data['data']['goods_info']['market_hash_name']])
        db.addgoods(uid, data['data']['goods_info']['market_hash_name'])
        res = "成功加仓 "+data['data']['goods_info']['market_hash_name']
    except Exception as e:
        res = f"加仓失败 {e}"
    await addgoods.finish(res)

@scheduler.scheduled_job("cron", hour="0-9,11-23", id="hourupdate")
async def hour_update_baojia():
    await db.update_goods(db.getallgoods())

@scheduler.scheduled_job("cron", hour="10", id="baojia")
async def send_baojia():
    await db.update_goods(db.getallgoods())
    bot = get_bot()
    for groupid in config.cs_group_list:
        await bot.send_msg(
            message_type="group",
            group_id=groupid,
            message=get_baojia(title = "10点自动更新")
        )

@updallgoods.handle()
async def updallgoods_function():
    goods = db.getallgoods()
    msg = "成功更新 {} 件饰品".format(len(goods))
    try:
        await db.update_goods(goods)
    except:
        msg = "更新失败"
    await updallgoods.finish(msg)