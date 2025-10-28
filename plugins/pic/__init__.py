from nonebot import get_plugin_config
from nonebot.plugin import PluginMetadata
from nonebot.adapters import Bot
from nonebot.adapters.onebot.v11 import Message, MessageEvent, MessageSegment
from nonebot.params import Arg
from nonebot import on_command
from nonebot import logger
from nonebot.log import default_format
logger.add("app.log", level="INFO", format=default_format, rotation="1 week")


from .config import Config

import hashlib
import os
from pathlib import Path
import random
import uuid
import urllib
import asyncio

__plugin_meta__ = PluginMetadata(
    name="pic",
    description="",
    usage="",
    config=Config,
)

config = get_plugin_config(Config)


getpic = on_command("getpic", priority=10, block=True)

addpic = on_command("addpic", priority=10, block=True)

getmgz = on_command("getmgz", priority=10, block=True)

addmgz = on_command("addmgz", priority=10, block=True)

def get_file_hash(file_path, chunk_size=8192, algorithm='sha256'):
    hash_obj = hashlib.new(algorithm)
    with open(file_path, 'rb') as f:
        while chunk := f.read(chunk_size):
            hash_obj.update(chunk)
    return hash_obj.hexdigest()

class PicDir:
    def __init__(self, dirname):
        self.dirname = dirname
        if not os.path.exists(dirname):
            os.makedirs(dirname, exist_ok=True)
        self.hashset = set()
        files = [f for f in Path(self.dirname).iterdir() if f.is_file()]
        for f in files:
            hashval = get_file_hash(f)
            if hashval in self.hashset:
                f.unlink()
            self.hashset.add(hashval)
    
    def getpic(self):
        files = [f for f in Path(self.dirname).iterdir() if f.is_file()]
        if len(files) == 0:
            return None
        return random.choice(files)
    
    def addpic(self, filename, url):
        filepath = Path(self.dirname) / (str(uuid.uuid4()) + "." + (filename.split('.')[-1]))
        urllib.request.urlretrieve(url, filepath)
        hashval = get_file_hash(filepath)
        if hashval in self.hashset:
            logger.info(f"[add{self.dirname}]  {filename} existed")
            filepath.unlink()
            return 0
        logger.info(f"[add{self.dirname}] {filename}")
        self.hashset.add(hashval)
        return 1


Pic1 = PicDir("pic")
lastpic = None
Pic2 = PicDir("mgz")

@getpic.handle()
async def getpic_function(bot: Bot, message: MessageEvent):
    global lastpic
    imgpath = Pic1.getpic()
    if not imgpath:
        await getpic.finish("没有图片")
    lastpic = imgpath
    msg = await getpic.send(MessageSegment.image(imgpath))
    await asyncio.sleep(600)
    await bot.delete_msg(message_id = msg['message_id'])
@addpic.handle()
async def addpic_handle_function():
    pass
@addpic.got("imgs", prompt="请发送图片")
async def addpic_got_imgs(imgs: Message = Arg()):
    addcnt = 0
    for seg in imgs:
        if seg.type == 'image':
            addcnt += Pic1.addpic(seg.data['file'], seg.data["url"])
    if addcnt > 0:
        await addpic.finish(f"成功添加 {addcnt} 张图片")
    await addpic.finish(f"添加失败")

@getmgz.handle()
async def getmgz_function(bot: Bot, message: MessageEvent):
    imgpath = Pic2.getpic()
    if not imgpath:
        await getpic.finish("没有图片")
    msg = await getpic.send(MessageSegment.image(imgpath))
    await asyncio.sleep(600)
    await bot.delete_msg(message_id = msg['message_id'])
@addmgz.handle()
async def addmgz_handle_function():
    pass
@addmgz.got("imgs", prompt="请发送图片")
async def addmgz_got_imgs(imgs: Message = Arg()):
    addcnt = 0
    for seg in imgs:
        if seg.type == 'image':
            addcnt += Pic2.addpic(seg.data['file'], seg.data["url"])
    if addcnt > 0:
        await addmgz.finish(f"成功添加 {addcnt} 张图片")
    await addmgz.finish(f"添加失败")

