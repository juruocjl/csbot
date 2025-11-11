from nonebot import get_plugin_config
from nonebot.plugin import PluginMetadata
from nonebot.adapters.onebot.v11 import Message, MessageEvent, MessageSegment
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
import os
from pathlib import Path
import tempfile

from .config import Config

scheduler = require("nonebot_plugin_apscheduler").scheduler

get_cursor = require("utils").get_cursor
get_session = require("utils").get_session
async_download = require("utils").async_download
path_to_file_url = require("utils").path_to_file_url
screenshot_html_to_png = require("utils").screenshot_html_to_png

__plugin_meta__ = PluginMetadata(
    name="market2",
    description="",
    usage="",
    config=Config,
)

config = get_plugin_config(Config)


if not os.path.exists("goodsimg"):
    os.makedirs("goodsimg", exist_ok=True)


headers = {'ApiToken': config.csqaq_api, 'Content-Type': 'application/json'}

class DataManager:
    def __init__(self):
        cursor = get_cursor()
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS member_goodid (
            uid TEXT,
            goodid INT,
            PRIMARY KEY (uid, goodid)
        )
        ''')

        cursor.execute('''
        CREATE TABLE IF NOT EXISTS good_info (
            goodId INT,
            name TEXT,
            marketHashName TEXT,
            type_localized_name TEXT,
            quality_localized_name TEXT,
            exterior_localized_name TEXT,
            PRIMARY KEY (goodId)
        )
        ''')

        cursor.execute('''
        CREATE TABLE IF NOT EXISTS good_price (
            goodId INT,
            timeStamp INT,
            platform INT,
            sellprice INT,
            sellnum INT,
            PRIMARY KEY (goodId, timeStamp)
        )
        ''')
    
    def add_good(self, uid: str, goodid: int):
        cursor = get_cursor()
        cursor.execute(
            'INSERT OR IGNORE INTO member_goodid (uid, goodid) VALUES (?, ?)',
            (uid, goodid)
        )
    
    def get_good_info(self, goodid: int):
        cursor = get_cursor()
        cursor.execute('SELECT * FROM good_info WHERE goodId == ? ', (goodid, ))
        return cursor.fetchone()

    def check_good_info(self, goodid: int) -> bool:
        cursor = get_cursor()
        cursor.execute('SELECT COUNT(*) FROM good_info WHERE goodId == ? ', (goodid, ))
        res = cursor.fetchone()
        return res[0] > 0

    def set_good_info(self, goodid: int, name: str, marketHashName: str, type_localized_name: str, quality_localized_name: str, exterior_localized_name: str):
        cursor = get_cursor()
        cursor.execute(
            'INSERT OR REPLACE INTO good_info (goodId, name, marketHashName, type_localized_name, quality_localized_name, exterior_localized_name) VALUES (?, ?, ?, ?, ?, ?)',
            (goodid, name, marketHashName, type_localized_name, quality_localized_name, exterior_localized_name)
        )

    def ins_good_price(self, goodid: int, timeStamp: int, platform: int, sellprice: int, sellnum: int):
        cursor = get_cursor()
        cursor.execute(
            'INSERT OR IGNORE INTO good_price (goodId, timeStamp, platform, sellprice, sellnum) VALUES (?, ?, ?, ?, ?)',
            (goodid, timeStamp, platform, sellprice, sellnum)
        )

    async def update_goods(self, goods_list: List[str]):
        while len(goods_list) > 0:
            now_goods = [self.getgoodinfo(id)[2] for id in goods_list[:50]]
            print(now_goods)
            goods_list = goods_list[50:]
            async with get_session().post("https://api.csqaq.com/api/v1/goods/getPriceByMarketHashName", data=json.dumps({"marketHashNameList": now_goods}),headers=headers) as res:
                data = await res.json()
            if data['code'] == 200:
                timeStamp = int(time.time())
                for marketHashName, good_info in data['data']['success'].items():
                    self.ins_good_price(
                        good_info['goodId'],
                        timeStamp,
                        1,
                        good_info['buffSellPrice'],
                        good_info['buffSellNum']
                    )
                    self.ins_good_price(
                        good_info['goodId'],
                        timeStamp,
                        2,
                        good_info['yyypSellPrice'],
                        good_info['yyypSellNum']
                    )
                    self.ins_good_price(
                        good_info['goodId'],
                        timeStamp,
                        3,
                        good_info['steamSellPrice'],
                        good_info['steamSellNum']
                    )

            else:
                logger.error("update_goods "+data['msg'])

    def get_all_goodid(self) -> List[str]:
        cursor = get_cursor()
        cursor.execute('SELECT DISTINCT goodid FROM member_goodid')
        res = cursor.fetchall()
        return [a[0] for a in res]
    
    def get_good_price(self, goodid: int, platform: int = 2):
        cursor = get_cursor()
        cursor.execute('SELECT * FROM good_price WHERE platform == ? AND goodId == ? ORDER BY timeStamp DESC LIMIT 1', (platform, goodid))
        return cursor.fetchone()

    def get_good_price_time(self, goodid: int, TimeStamp: int, platform: int = 2):
        cursor = get_cursor()
        cursor.execute('SELECT * FROM good_price WHERE platform == ? AND goodId == ? AND timeStamp >= ? ORDER BY timeStamp ASC LIMIT 1', (platform, goodid, TimeStamp))
        return cursor.fetchone()

db = DataManager()

baojia = on_command("报价2", priority=10, block=True)

search = on_command("搜索2", priority=10, block=True)

addgoods = on_command("加仓2", priority=10, block=True)

updallgoods = on_command("更新饰品2", priority=10, block=True, permission=SUPERUSER)

with open(Path("assets") / "market.html", 'r', encoding='utf-8') as file:
    market_content = file.read().split("<!--SPLIT--->")

async def get_baojia_image(title: str = "当前底价"):
    allgoodid = db.get_all_goodid()
    logger.info(allgoodid)
    data = []
    html = market_content[0].replace("_TITLE_", title)
    for id in allgoodid:
        info = db.get_good_info(id)
        price = db.get_good_price(id)
        price1d = db.get_good_price_time(id, time.time() - 1 * 24 * 3600)
        price7d = db.get_good_price_time(id, time.time() - 7 * 24 * 3600)
        price1d = price1d[3] if price[1] - price1d[1] >= 20 * 3600 else None
        price7d = price7d[3] if price[1] - price7d[1] >= 6 * 24 * 3600 else None
        
        data.append((id, info[1], price[6], price1d, price7d))
    data = sorted(data, key = lambda x: x[2])
    for item in data:
        temp_html = market_content[1]
        temp_html = temp_html.replace("_IMG_", path_to_file_url(Path("goodsimg") / f"{item[0]}.jpg"))
        temp_html = temp_html.replace("_NAME_", item[1])
        temp_html = temp_html.replace("_PRICE_", str(item[2]/100))
        if item[3] != None:
            delta1d = item[2] - item[3]
            if delta1d > 0:
                temp_html = temp_html.replace("_1DC_", "red")
            elif delta1d < 0:
                temp_html = temp_html.replace("_1DC_", "green")
            else:
                temp_html = temp_html.replace("_1DC_", "black")
            temp_html = temp_html.replace("_1D_", f"{delta1d/100}")
            if item[3] != 0:
                temp_html = temp_html.replace("_1DP_", f"{delta1d / item[3] * 100:.1f}%")
            else:
                temp_html = temp_html.replace("_1DP_", f"Nan")
        else:
            temp_html = temp_html.replace("_1DC_", "black")
            temp_html = temp_html.replace("_1D_", "无数据")
            temp_html = temp_html.replace("_1DP_", "无数据")

        if item[4] != None:
            delta7d = item[2] - item[4]
            if delta7d > 0:
                temp_html = temp_html.replace("_7DC_", "red")
            elif delta7d < 0:
                temp_html = temp_html.replace("_7DC_", "green")
            else:
                temp_html = temp_html.replace("_7DC_", "black")
            temp_html = temp_html.replace("_7D_", f"{delta7d/100}")
            if item[4] != 0:
                temp_html = temp_html.replace("_7DP_", f"{delta7d / item[4] * 100:.1f}%")
            else:
                temp_html = temp_html.replace("_7DP_", f"Nan")
        else:
            temp_html = temp_html.replace("_7DC_", "black")
            temp_html = temp_html.replace("_7D_", "无数据")
            temp_html = temp_html.replace("_7DP_", "无数据")
        html += temp_html
    html += market_content[2]
    with tempfile.NamedTemporaryFile(mode='w', encoding='utf-8', suffix=".html", dir="temp", delete=False) as temp_file:
        temp_file.write(html)
        temp_file.close()
        img = await screenshot_html_to_png(path_to_file_url(temp_file.name), 850, 120 + len(data) * 60)
        os.remove(temp_file.name)
    return img

@baojia.handle()
async def baojia_function():
    # await baojia.finish(get_baojia())
    await baojia.finish(MessageSegment.image(await get_baojia_image()))

@search.handle()
async def search_function(args: Message = CommandArg()):
    async with get_session().get("https://api.csqaq.com/api/v1/search/suggest", params={"text": args.extract_plain_text()}, headers=headers) as res:
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
        goodid = int(args.extract_plain_text())
        async with get_session().get("https://api.csqaq.com/api/v1/info/good", params={"id": goodid}, headers=headers) as res:
            data = await res.json()
        await asyncio.sleep(1.1)
        if not db.check_good_info(goodid):
            db.set_good_info(
                data['data']['goods_info']['id'],
                data['data']['goods_info']['name'],
                data['data']['goods_info']['market_hash_name'],
                data['data']['goods_info']['type_localized_name'],
                data['data']['goods_info']['quality_localized_name'],
                data['data']['goods_info']['exterior_localized_name']
            )
            for plat in range(1, 4):
                async with get_session().post("https://api.csqaq.com/api/v1/info/chart", params={"good_id": goodid, "key": "sell_price","platform": plat,"period": "30",}, headers=headers) as res:
                    ddata = (await res.json())['data']
                await asyncio.sleep(1.1)
                for i in range(len(ddata['timestamp'])):
                    db.ins_good_price(
                        int(goodid),
                        ddata['timestamp'][i],
                        plat,
                        ddata['main_data'][i],
                        ddata['num_data'][i]
                    )
            await async_download(data['data']['goods_info']['img'], Path("goodsimg") / f"{data['data']['goods_info']['id']}.jpg")
        db.add_good(uid, goodid)
        res = "成功加仓 "+data['data']['goods_info']['name']
    except Exception as e:
        res = f"加仓失败 {e}"
    await addgoods.finish(res)

# @scheduler.scheduled_job("cron", hour="0-9,11-23", id="hourupdate")
async def hour_update_baojia():
    await db.update_goods(db.get_all_goodid())

# @scheduler.scheduled_job("cron", hour="10", id="baojia")
async def send_baojia():
    await db.update_goods(db.get_all_goodid())
    bot = get_bot()
    for groupid in config.cs_group_list:
        await bot.send_msg(
            message_type="group",
            group_id=groupid,
            message=MessageSegment.image(await get_baojia_image(title="10点自动更新"))
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