from nonebot import get_plugin_config
from nonebot.plugin import PluginMetadata
from nonebot.adapters import Bot
from nonebot.adapters.onebot.v11 import Message, MessageEvent, MessageSegment
from nonebot.params import Arg
from nonebot import on_command, on_message
from nonebot import require
from nonebot import logger
from nonebot import get_driver

require("utils")
from ..utils import local_storage

require("allmsg")
from ..allmsg import get_image

from .config import Config

import hashlib
from pathlib import Path
import random
import uuid
import asyncio
import ast
from meme_generator import Image, get_meme

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

def islp(evt: MessageEvent) -> bool:
    text = evt.get_message().extract_plain_text().lower().strip()
    return text in ["wlp", "nlg"]

checklp = on_message(priority=0, block=False, rule=islp)

def get_file_hash(file_path, chunk_size=8192, algorithm='sha256'):
    hash_obj = hashlib.new(algorithm)
    with open(file_path, 'rb') as f:
        while chunk := f.read(chunk_size):
            hash_obj.update(chunk)
    return hash_obj.hexdigest()

class PicDir:
    def __init__(self, dirname: str):
        self.dirname = Path("imgs") / dirname
        self.keyname = f"hashset{dirname}"
        self.hashset: set[str] = set()
        if not self.dirname.exists():
            self.dirname.mkdir(parents=True)

    async def real_init(self):
        if value := await local_storage.get(self.keyname):
            self.hashset = ast.literal_eval(value)
        else:
            self.hashset = set()
            await self.rebuild()
        logger.info(f"init {self.dirname} with {len(self.hashset)} pics")
    
    def __len__(self):
        return len(self.hashset)

    def getpic(self):
        files = [f for f in Path(self.dirname).iterdir() if f.is_file()]
        if len(files) == 0:
            return None
        return random.choice(files)
    
    async def rebuild(self):
        files = [f for f in Path(self.dirname).iterdir() if f.is_file()]
        for f in files:
            hashval = get_file_hash(f)
            if hashval in self.hashset:
                f.unlink()
            self.hashset.add(hashval)
        await local_storage.set(self.keyname, str(self.hashset))

    async def addpic(self, filename: str, url: str):
        filepath = self.dirname / (str(uuid.uuid4()) + "." + (filename.split('.')[-1]))
        with open(filepath, 'wb') as f:
            f.write(await get_image(filename, url))
        hashval = get_file_hash(filepath)
        if hashval in self.hashset:
            logger.info(f"add{self.dirname}  {filename} existed")
            filepath.unlink()
            return 0
        logger.info(f"add{self.dirname} {filename}")
        self.hashset.add(hashval)
        await local_storage.set(self.keyname, str(self.hashset))
        return 1

Pic1 = PicDir("pic")
lastpic = None
Pic2 = PicDir("mgz")


driver = get_driver()
@driver.on_startup
async def init_picdir():
    await Pic1.real_init()
    await Pic2.real_init()
    logger.info("图片库初始化完成")



@getpic.handle()
async def getpic_function(bot: Bot, message: MessageEvent):
    global lastpic
    imgpath = Pic1.getpic()
    if not imgpath:
        await getpic.finish("没有图片")
    lastpic = imgpath
    msg = await getpic.send(MessageSegment.image(open(imgpath, "rb").read()))
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
            addcnt += await Pic1.addpic(seg.data['file'], seg.data["url"])
    if addcnt > 0:
        await addpic.finish(f"成功添加 {addcnt} 张图片")
    await addpic.finish(f"添加失败")

@getmgz.handle()
async def getmgz_function(bot: Bot, message: MessageEvent):
    imgpath = Pic2.getpic()
    if not imgpath:
        await getpic.finish("没有图片")
    msg = await getpic.send(MessageSegment.image(open(imgpath, "rb").read()))
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
            addcnt += await Pic2.addpic(seg.data['file'], seg.data["url"])
    if addcnt > 0:
        await addmgz.finish(f"成功添加 {addcnt} 张图片")
    await addmgz.finish(f"添加失败")

def get_pic_status():
    return "pic {}, mgz {}".format(len(Pic1), len(Pic2))

@checklp.handle()
async def checklp_function(message: MessageEvent):
    global lastpic
    msg = message.get_message()
    text = msg.extract_plain_text().lower().strip()
    
    if text == "wlp" and lastpic:
        meme = get_meme("my_wife")
        with open(lastpic, "rb") as f:
            data = f.read()
        result = meme.generate([Image("test", data)], [], {})
        lastpic = None
        if isinstance(result, bytes):
            await checklp.send(MessageSegment.image(result))
    if text == "nlg" and lastpic:
        meme = get_meme("dog_dislike")
        with open(lastpic, "rb") as f:
            data = f.read()
        result = meme.generate([Image("test", data)], [], {})
        lastpic = None
        if isinstance(result, bytes):
            await checklp.send(MessageSegment.image(result))