from nonebot import get_plugin_config
from nonebot.plugin import PluginMetadata
from nonebot.adapters.onebot.v11 import GroupMessageEvent, Message, MessageSegment
from nonebot.params import CommandArg
from nonebot.plugin import on_command
from nonebot import require
from nonebot import get_bot
from nonebot import logger

require("nonebot_plugin_apscheduler")
from nonebot_plugin_apscheduler import scheduler

require("utils")
from ..utils import get_today_start_timestamp

require("allmsg")
from ..allmsg import db, extra_plain_text

import time
from wordcloud import WordCloud
from io import BytesIO
import emoji
import jieba
from pathlib import Path
from collections import defaultdict
from .config import Config

__plugin_meta__ = PluginMetadata(
    name="wordcloud",
    description="",
    usage="",
    config=Config,
)

config = get_plugin_config(Config)

jieba.load_userdict(str(Path("assets") / "dict.txt"))

wordcloud = on_command("词云", priority=10, block=True)

mywordcloud = on_command("我的词云", priority=10, block=True)


valid_time = ["今日", "昨日", "本周", "全部"]
def get_time_range(time_type):
    if time_type == "今日":
        return get_today_start_timestamp(), 1e10
    if time_type == "昨日":
        return get_today_start_timestamp() - 24 * 3600, get_today_start_timestamp()
    if time_type == "本周":
        return time.time() - 7 * 24 * 3600, 1e10
    if time_type == "全部":
        return 0, 1e10
    raise RuntimeError("no time type")

async def get_wordcloud(groud_id, user_id = "%", time_type = "全部"):
    if time_type not in valid_time:
        time_type = "全部"
    msgdict = await db.get_all_msg(groud_id, userid=user_id, tmrange=get_time_range(time_type))
    stopwords = {
        "怎么", "感觉", "什么", "真是", "不是", "一个", "可以", "没有", "你们", "但是", "现在", "这个",
    }
    wordcount = defaultdict(int)
    for x in msgdict.values():
        msg = extra_plain_text(x[2])
        seg_list = list(jieba.cut(msg, cut_all=False))
        wordset = set()
        for word in seg_list:
            if emoji.emoji_count(word) == 1:
                wordset.add(word)
            elif word not in stopwords and len(word) > 1:
                wordset.add(word)
        for word in wordset:
            wordcount[word] += 1
    
    buffer = BytesIO()
    WordCloud(
        width=800,
        height=600,
        background_color='white',
        font_path=Path("./assets") / "merged.ttf",
        max_words=200,
        colormap='viridis',
        collocations=False
    ).generate_from_frequencies(wordcount).to_image().save(buffer, format='PNG') 
    return buffer

@wordcloud.handle()
async def wordcloud_function(message: GroupMessageEvent, args: Message = CommandArg()):
    sid = message.get_session_id()
    assert(sid.startswith("group"))
    gid = sid.split('_')[1]
    msg = args.extract_plain_text().strip()
    uid = "%"
    for seg in message.get_message():
        if seg.type == "at":
            uid = seg.data["qq"]
    image = await get_wordcloud(gid, user_id=uid, time_type=msg)
    await wordcloud.finish(MessageSegment.image(image))

@mywordcloud.handle()
async def mywordcloud_function(message: GroupMessageEvent, args: Message = CommandArg()):
    sid = message.get_session_id()
    assert(sid.startswith("group"))
    gid = sid.split('_')[1]
    msg = args.extract_plain_text().strip()
    uid = message.get_user_id()
    image = await get_wordcloud(gid, user_id=uid, time_type=msg)
    await mywordcloud.finish(MessageSegment.image(image))

@scheduler.scheduled_job("cron", hour="23", minute="50", id="todaywc")
async def todaywc():
    bot = get_bot()
    for group in config.cs_group_list:
        image = await get_wordcloud(group, time_type="今日")
        await bot.send_group_msg(group_id=group, message=Message([MessageSegment.image(image)]))

