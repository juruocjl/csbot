from nonebot import get_plugin_config
from nonebot.plugin import PluginMetadata
from nonebot.adapters.onebot.v11 import Message, MessageEvent, MessageSegment
from nonebot.permission import SUPERUSER
from nonebot.params import CommandArg
from nonebot import on_command
from nonebot import get_bot
from nonebot import require
from nonebot import logger

import json
import time
import asyncio
import os
from pathlib import Path
import tempfile

from .config import Config

scheduler = require("nonebot_plugin_apscheduler").scheduler

require("utils")

from ..utils import async_session_factory, Base
from ..utils import get_session, path_to_file_url, screenshot_html_to_png

from sqlalchemy import String, Integer, select, desc, asc
from sqlalchemy.orm import Mapped, mapped_column

__plugin_meta__ = PluginMetadata(
    name="market",
    description="",
    usage="",
    config=Config,
)

config = get_plugin_config(Config)


if not os.path.exists("goodsimg"):
    os.makedirs("goodsimg", exist_ok=True)


headers = {'ApiToken': config.csqaq_api, 'Content-Type': 'application/json'}

import requests

try:
    res = requests.post("https://api.csqaq.com/api/v1/sys/bind_local_ip", headers=headers, timeout=5)
    assert(res.json()['code'] == 200)
    logger.info("Bind csqaq api success: " + res.text)
except:
    logger.error("Bind csqaq api fail")


class MemberGoods(Base):
    __tablename__ = "member_goods"
    uid: Mapped[str] = mapped_column(String, primary_key=True)
    marketHashName: Mapped[str] = mapped_column(String, primary_key=True)

class GoodsInfo(Base):
    __tablename__ = "goods_info"
    marketHashName: Mapped[str] = mapped_column(String, primary_key=True)
    timeStamp: Mapped[int] = mapped_column(Integer, primary_key=True)
    goodId: Mapped[int] = mapped_column(Integer)
    name: Mapped[str] = mapped_column(String)
    buffSellPrice: Mapped[int] = mapped_column(Integer)
    buffSellNum: Mapped[int] = mapped_column(Integer)
    yyypSellPrice: Mapped[int] = mapped_column(Integer)
    yyypSellNum: Mapped[int] = mapped_column(Integer)
    steamSellPrice: Mapped[int] = mapped_column(Integer)
    steamSellNum: Mapped[int] = mapped_column(Integer)

class DataManager:
    def __init__(self):
        pass
    
    async def addgoods(self, uid: str, name: str):
        async with async_session_factory() as session:
            async with session.begin():
                await session.merge(MemberGoods(uid=uid, marketHashName=name))
        
    async def update_goods(self, goods_list: list[str]):
        while len(goods_list) > 0:
            now_goods = goods_list[:50]
            goods_list = goods_list[50:]
            async with get_session().post("https://api.csqaq.com/api/v1/goods/getPriceByMarketHashName", data=json.dumps({"marketHashNameList": now_goods}),headers=headers) as res:
                data = await res.json()
            if data['code'] == 200:
                async with async_session_factory() as session:
                    async with session.begin():
                        for marketHashName, good_info in data['data']['success'].items():
                            new_info = GoodsInfo(
                                marketHashName=marketHashName,
                                timeStamp=int(time.time()),
                                goodId=good_info['goodId'],
                                name=good_info['name'],
                                buffSellPrice=round(good_info['buffSellPrice'] * 100),
                                buffSellNum=good_info['buffSellNum'],
                                yyypSellPrice=round(good_info['yyypSellPrice'] * 100),
                                yyypSellNum=good_info['yyypSellNum'],
                                steamSellPrice=round(good_info['steamSellPrice'] * 100),
                                steamSellNum=good_info['steamSellNum']
                            )
                            session.add(new_info)
            else:
                logger.error("update_goods "+data['msg'])

    async def getallgoods(self) -> list[str]:
        async with async_session_factory() as session:
            stmt = select(MemberGoods.marketHashName).distinct()
            result = await session.execute(stmt)
            return list(result.scalars().all())
    
    async def getgoodsinfo(self, marketHashName: str) -> GoodsInfo | None:
        async with async_session_factory() as session:
            stmt = select(GoodsInfo).where(GoodsInfo.marketHashName == marketHashName).order_by(GoodsInfo.timeStamp.desc()).limit(1)
            result = await session.execute(stmt)
            return result.scalar_one_or_none()

    async def getgoodsinfo_time(self, marketHashName: str, TimeStamp: int) -> GoodsInfo | None:
        async with async_session_factory() as session:
            stmt = select(GoodsInfo).where(GoodsInfo.marketHashName == marketHashName).where(GoodsInfo.timeStamp >= TimeStamp).order_by(GoodsInfo.timeStamp.asc()).limit(1)
            result = await session.execute(stmt)
            return result.scalar_one_or_none()

db = DataManager()

baojia = on_command("报价", priority=10, block=True)

search = on_command("搜索", priority=10, block=True)

addgoods = on_command("加仓", priority=10, block=True)

updallgoods = on_command("更新饰品", priority=10, block=True, permission=SUPERUSER)

with open(Path("assets") / "market.html", 'r', encoding='utf-8') as file:
    market_content = file.read().split("<!--SPLIT--->")

async def get_baojia(title: str = "当前底价"):
    allgoods = await db.getallgoods()
    logger.info(allgoods)
    data = []
    for goods in allgoods:
        info = await db.getgoodsinfo(goods)
        info1d = await db.getgoodsinfo_time(goods, int(time.time() - 24 * 3600))
        info7d = await db.getgoodsinfo_time(goods, int(time.time() - 7 * 24 * 3600))
        
        if not info: continue

        delta1d = "无数据"
        if info1d and info.timeStamp - info1d.timeStamp >= 20 * 3600:
             delta1d = str((info.yyypSellPrice - info1d.yyypSellPrice) / 100)
        
        delta7d = "无数据"
        if info7d and info.timeStamp - info7d.timeStamp >= 6 * 24 * 3600:
             delta7d = str((info.yyypSellPrice - info7d.yyypSellPrice) / 100)
        
        data.append((info.name, info.yyypSellPrice, delta1d, delta7d))
    data = sorted(data, key = lambda x: x[1])
    return (title + "\n" + "\n".join([a[0] + "\n> " + str(a[1]/100) + "   Δ1d=" + a[2] + "   Δ7d=" + a[3] for a in data])).strip()

async def get_baojia_image(title: str = "当前底价"):
    allgoods = await db.getallgoods()
    logger.info(allgoods)
    data = []
    html = market_content[0].replace("_TITLE_", title)
    for goods in allgoods:
        info = await db.getgoodsinfo(goods)
        info1d = await db.getgoodsinfo_time(goods, int(time.time() - 24 * 3600))
        info7d = await db.getgoodsinfo_time(goods, int(time.time() - 7 * 24 * 3600))
        
        if not info: continue

        price1d = None
        if info1d and info.timeStamp - info1d.timeStamp >= 20 * 3600:
            price1d = info1d.yyypSellPrice
        
        price7d = None
        if info7d and info.timeStamp - info7d.timeStamp >= 6 * 24 * 3600:
            price7d = info7d.yyypSellPrice
        
        data.append((info.goodId, info.name, info.yyypSellPrice, price1d, price7d))
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
        async with get_session().get("https://api.csqaq.com/api/v1/info/good", params={"id": args.extract_plain_text()}, headers=headers) as resgood:
            data = await resgood.json()
        await asyncio.sleep(1.1)
        await db.update_goods([data['data']['goods_info']['market_hash_name']])
        with open(Path("goodsimg") / f"{data['data']['goods_info']['id']}.jpg", "wb") as f:
            async with get_session().get(data['data']['goods_info']['img']) as imgres:
                f.write(await imgres.read())
        await db.addgoods(uid, data['data']['goods_info']['market_hash_name'])
        res = "成功加仓 "+data['data']['goods_info']['market_hash_name']
    except Exception as e:
        res = f"加仓失败 {e}"
    await addgoods.finish(res)

@scheduler.scheduled_job("cron", hour="0-9,11-23", id="hourupdate")
async def hour_update_baojia():
    await db.update_goods(await db.getallgoods())

@scheduler.scheduled_job("cron", hour="10", id="baojia")
async def send_baojia():
    await db.update_goods(await db.getallgoods())
    bot = get_bot()
    for groupid in config.cs_group_list:
        await bot.send_msg(
            message_type="group",
            group_id=groupid,
            message=MessageSegment.image(await get_baojia_image(title="10点自动更新"))
        )

@updallgoods.handle()
async def updallgoods_function():
    goods = await db.getallgoods()
    msg = "成功更新 {} 件饰品".format(len(goods))
    try:
        await db.update_goods(goods)
    except:
        msg = "更新失败"
    await updallgoods.finish(msg)