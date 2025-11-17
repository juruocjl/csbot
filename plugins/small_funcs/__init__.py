from nonebot import get_plugin_config
from nonebot.plugin import PluginMetadata
from nonebot.adapters.onebot.v11 import Message, MessageEvent, GroupMessageEvent, MessageSegment
from nonebot.adapters import Bot
from nonebot import on_command
from nonebot import require

get_pic_status = require("pic").get_pic_status
get_msg_status = require("allmsg").get_msg_status
get_session = require("utils").get_session

import time
import psutil
import random
import asyncio

from .config import Config

__plugin_meta__ = PluginMetadata(
    name="small_funcs",
    description="",
    usage="",
    config=Config,
)

config = get_plugin_config(Config)


help = on_command("帮助", priority=20, block=True)

getstatus = on_command("状态", priority=10, block=True)

caigou = on_command("采购", priority=10, block=True)

langeng = on_command("烂梗", priority=10, block=True)

@help.handle()
async def help_function():
    await help.finish(f"""可用指令：
/绑定 steamid64
/解绑
/更新数据
/查看数据 (用户名匹配)
默认查看自己数据。你可以使用用户名匹配查看第一个匹配到用户的数据。
/记录 (用户名匹配) (时间)
默认查看自己记录。最多 20 条。如果只有一个参数，会优先判断是否为时间。默认时间为全部。
/排名 [选项] (时间)
查看指定时间指定排名，具体可选项可以使用 /排名 查看。
/ai /aitb /aixmm /aixhs [内容]
向ai提问，风格为 普通ai，贴吧老哥，可爱女友，小红书
/ai记忆 [内容]
向ai增加记忆内容
/搜索 [饰品名称]
/加仓 [饰品id]
/报价
/状态
查询服务器状态。
/复读点数
/复读帮助
/我的词云 [时间]
/词云 (@人) [时间]
[时间] 和 (@人) 都为可选项，默认为全部，所有人

(用户名匹配) 使用语法为 % 匹配任意长度串，_ 匹配长度为 1 串。
可选 (时间)：{require("cs").valid_time}
在 /查看数据 /记录 /ai* 时你的@消息会被替换成对应的用户名，找不到则会被替换为<未找到用户>
""")


@caigou.handle()
async def caigou_function(bot: Bot, message: MessageEvent):
    sid = message.get_session_id()
    await caigou.send(MessageSegment.face(317))
    if sid.startswith('group'):
        await asyncio.sleep(random.random() * 5)
        await bot.call_api("group_poke", group_id=sid.split('_')[1], user_id=sid.split('_')[2])

@getstatus.handle()
async def getstatus_function(message: GroupMessageEvent):
    cpu_usage = psutil.cpu_percent()
    
    # 获取内存信息
    memory = psutil.virtual_memory()
    total_mem = memory.total / (1024 **3)  # 转换为GB
    used_mem = memory.used / (1024** 3)
    available_mem = memory.available / (1024 **3)
    mem_usage = memory.percent
    
    # 组织结果
    status = {
        'timestamp': time.strftime('%Y-%m-%d %H:%M:%S'),
        'cpu_usage_percent': cpu_usage,
        'memory': {
            'total_gb': round(total_mem, 2),
            'used_gb': round(used_mem, 2),
            'available_gb': round(available_mem, 2),
            'usage_percent': mem_usage
        }
    }

    tuku = get_pic_status()

    msgcount = get_msg_status(message.group_id)

    await getstatus.finish(Message([
        MessageSegment.at(message.get_user_id()),
        f"""\nCPU 总使用率: {status['cpu_usage_percent']}%
内存总容量: {status['memory']['total_gb']}GB
已使用内存: {status['memory']['used_gb']}GB ({status['memory']['usage_percent']}%)
可用内存: {status['memory']['available_gb']}GB
当前图库: {tuku}
{msgcount}"""]))

@langeng.handle()
async def langeng_function():
    async with get_session().get("https://hguofichp.cn:10086/machine/getRandOne") as res:
        data = await res.json()
        await langeng.finish(data['data']['barrage'])
