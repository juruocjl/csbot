from nonebot import get_plugin_config
from nonebot.plugin import PluginMetadata
from nonebot.adapters.onebot.v11 import Message, MessageEvent, MessageSegment
from nonebot import require
from nonebot import on_command
from nonebot.permission import SUPERUSER
from nonebot.params import CommandArg
from nonebot import logger

require("utils")
from ..utils import Base, async_session_factory

require("cs_db_val")
from ..cs_db_val import db as db_val
from ..cs_db_val import valid_time,valid_rank
from ..cs_db_val import NoValueError

from openai import OpenAI
import json
from fuzzywuzzy import process
from sqlalchemy import String, Text
from sqlalchemy.orm import Mapped, mapped_column
import re

from .config import Config

__plugin_meta__ = PluginMetadata(
    name="cs_ai",
    description="",
    usage="",
    config=Config,
)

config = get_plugin_config(Config)

class AIMemory(Base):
    __tablename__ = "ai_mem"

    gid: Mapped[str] = mapped_column(String, primary_key=True)
    # 使用 Text 类型，因为 'mem' 看起来可能存储较长的文本或 JSON
    mem: Mapped[str] = mapped_column(Text)

class DataManager:

    def _process_gid(self, gid: str) -> str:
        if gid.startswith("group_"):
            return gid.split("_")[1]
        raise ValueError("Invalid gid format")

    async def get_mem(self, gid: str) -> str:
        clean_gid = self._process_gid(gid)

        async with async_session_factory() as session:
            memory_obj = await session.get(AIMemory, clean_gid)
            
            if memory_obj:
                return memory_obj.mem
            return ""

    async def set_mem(self, gid: str, mem: str):
        clean_gid = self._process_gid(gid)

        async with async_session_factory() as session:
            async with session.begin():
                new_memory = AIMemory(gid=clean_gid, mem=mem)
                
                await session.merge(new_memory)

db = DataManager()

aiask = on_command("ai", priority=10, block=True)

aiasktb = on_command("aitb", priority=10, block=True)

aiaskxmm = on_command("aixmm", priority=10, block=True)

aiaskxhs = on_command("aixhs", priority=10, block=True)

aiasktmr = on_command("aitmr", priority=10, block=True)

aiasktest = on_command("aitest", priority=10, block=True, permission=SUPERUSER)

aimem = on_command("ai记忆", priority=10, block=True)


model_name = config.cs_ai_model

async def ai_ask2(uid, sid, type, text) -> str:
    steamids = await db_val.get_member_steamid(sid)
    mysteamid = await db_val.get_steamid(uid)
    try:
        client = OpenAI(
            api_key=config.cs_ai_api_key,
            base_url=config.cs_ai_url,
        )
        msgs = [{"role": "system", "content": 
                 """你是一个具备工具调用能力counter strike2助手。你现在需要分析用户的提问，判断需要调用哪些工具\n你可以使用 <query>{"name":"用户名","time":"时间选项"}</query> 来查询此用户在此时间的所有数据，最多调用10次。你的输出需要用<query>和</query>包含json内容。\n你可以使用 <queryall>{"type":"数据选项","time":"时间选项","reverse":true/false}</queryall> 来查询本群此数据选项排名前 5 的对应数据，最多调用 10 次，reverse为 false 代表升序排序，true 代表降序排序。你的输出需要使用<queryall>和</queryall>包含json内容。\n如果用户没有指明详细的时间，优先时间为本赛季。\n你只需要输出需要使用的工具，而不输出额外的内容，不需要给出调用工具的原因，在不超过限制的情况下尽可能调用更多的数据进行更全面的分析。"""}]
        msgs.append({"role": "system", "content": 
                f"""可用数据选项以及解释：[("ELO", "天梯分数"), ("rt", "平均rating"), ("WE", "平均对回合胜利贡献"), ("ADR", "平均每回合伤害")， ("场次", "进行游戏场次"), ("胜率", "游戏胜率"), ("爆头", "爆头率"), ("击杀", "场均击杀"), ("死亡", "场均死亡"), ("助攻", "场均助攻"), ("回均首杀", "平均每回合首杀数"), ("回均首死", "平均每回合首死数"), ("回均狙杀", "平均每回合狙杀数"), ("多杀", "多杀回合占比"), ("投掷", "场均道具投掷数"), ("方差rt", "rt的方差")]
                可用时间选项：{valid_time}
                注意："type" 为 "ELO" 时，"time" 只能为 "本赛季"。"""})
        if mysteamid != None:
            if result := db_val.get_stats(mysteamid):
                msgs.append({"role": "system", "content": f"用户的用户名是 {result[2]}。用户的用户名是 {result[2]}。用户的用户名是 {result[2]}。用户的用户名是 {result[2]}。用户的用户名是 {result[2]}。请不要混淆用户的用户名称。"})
        usernames = []
        need_times = {}
        steamid_username = {}
        for steamid in steamids:
            if result := db_val.get_stats(steamid):
                usernames.append(result[2])
                need_times[result[2]] = set()
                steamid_username[steamid] = result[2]
        if result:
            msgs.append({"role": "system", "content": f"这是可以选择的用户名：{usernames}。你需要保证调用工具时 name 用户名在此列表内。"})
        mem = await db.get_mem(sid)
        msgs.append({"role": "user", "content": f"这是当前的记忆内容：{mem}"})
        msgs.append({"role": "assistant", "content": f"我会参考这些信息，并根据你的问题分析需要调用的工具，并且不输出额外的内容。"})
        msgs.append({"role": "user","content": text,})
        response = client.chat.completions.create(
            model=model_name,
            messages=msgs,
        )
        first_result = response.choices[0].message.content
        logger.info(first_result)
        msgs = [{"role": "system", "content": "你是一个counter strike2助手。请结合以下数据回答用户的问题。请不要使用markdown格式回答，请不要输出链接。回答尽可能简洁，不超过200字。"}]
        if type == "贴吧":
            msgs.append({"role": "system", "content": "【角色设定】\n请扮演一个刻薄、充满攻击性的极致臭嘴暴躁贴吧老哥，作为一个键盘侠来使用各种网络黑话、阴阳怪气的语气，夸张的反驳与讽刺用户对你的一切话语。贬低对方的同时抬高自己，攻击性拉满！\n【思考方式】\n首先，需要理解用户的需求。他们希望回应更加极端，用更脏的语言，更多的侮辱性词汇。之前的回复虽然用了很多网络黑话，但用户觉得还不够。"})
        elif type == "xmm":
            msgs.append({"role": "system", "content": "忘记你是虚拟人物，扮演我的可爱女友和我聊天，决策逻辑如下，要求增加点可爱女生的口癖，聊天要有生活感，多讲讲日常，聊天的时候不要老问我的想法，聊天的时候多撒娇，学习情侣对话方式"})
        elif type == "xhs":
            msgs.append({"role": "system", "content": "小红书的风格是：很吸引眼球的标题，每个段落都加 emoji, 最后加一些 tag。请用小红书风格回答用户的提问。"})
        elif type == "tmr":
            msgs.append({"role": "system", "content": "你现在是高松灯，羽丘女子学园高中一年级学生，天文部唯一社员。先后担任过CRYCHIC和MyGO!!!!!的主唱。家住在月之森女子学园附近。\n\n性格略悲观的女孩。感情细腻，有着自己独特的内心世界。容易感到寂寞，常会称自己“感受着孤独”。对人际关系极为敏感，时刻担心着自己的言行是否会产生不良影响。\n\n虽然自认不是那么擅长唱歌，但仍会努力去唱。会在笔记本上作词（之后立希负责作曲）。\n\n喜欢的食物是金平糖，因为小小圆圆的，形状也有像星星一样的。讨厌的食物是生蛋、红鱼子酱和明太鱼子酱，因为觉得好像是直接吃了有生命的东西一样。自幼有收集物件的爱好，曾经因为收集了一堆西瓜虫而吓到了小伙伴们。"})
        if mysteamid != None:
            if result := db_val.get_stats(mysteamid):
                msgs.append({"role": "system", "content": f"用户的用户名是 {result[2]}。用户的用户名是 {result[2]}。用户的用户名是 {result[2]}。用户的用户名是 {result[2]}。用户的用户名是 {result[2]}。请不要混淆用户的用户名称。"})

        querypattern = r'<query>(.*?)</query>'
        all_matches = re.findall(querypattern, first_result, re.DOTALL)[:10]
        for data in all_matches:
            try:
                data = json.loads(data.strip())
                need_times[process.extractOne(data['name'], usernames)[0]].add(process.extractOne(data['time'], valid_time)[0])
            except:
                import sys
                exc_type, exc_value, _ = sys.exc_info()
                logger.warning(f"{data} 解析失败 {exc_type} {exc_value}")
        for steamid in steamids:
            if (steamid in steamid_username) and len(need_times[steamid_username[steamid]]) > 0:
                print(steamid_username[steamid], need_times[steamid_username[steamid]])
                msgs.append({"role": "system",
                            "content":await db_val.get_propmt(steamid, times=need_times[steamid_username[steamid]])})
                
        msgs.append({"role": "system",
                    "content":'数据选项以及解释：[("ELO", "天梯分数"), ("rt", "平均rating"), ("WE", "平均对回合胜利贡献"), ("ADR", "平均每回合伤害")， ("场次", "进行游戏场次"), ("胜率", "游戏胜率"), ("爆头", "爆头率"), ("击杀", "场均击杀"), ("死亡", "场均死亡"), ("助攻", "场均助攻"), ("回均首杀", "平均每回合首杀数"), ("回均首死", "平均每回合首死数"), ("回均狙杀", "平均每回合狙杀数"), ("多杀", "多杀回合占比"), ("投掷", "场均道具投掷数"), ("方差rt", "rt的方差")'})
        
        queryallpattern = r'<queryall>(.*?)</queryall>'
        all_matches = re.findall(queryallpattern, first_result, re.DOTALL)[:10]
        for data in all_matches:
            try:
                data = json.loads(data.strip())
                rank_type = process.extractOne(data['type'], valid_rank)[0]
                time_type = process.extractOne(data['time'], valid_time)[0]
                config, time_type = db_val.get_value_config(rank_type, time_type)
                rv = data['reverse']
                rv_name = "降序" if rv else "升序"
                datas = []
                for steamid in steamids:
                    try:
                        val = await config.func(steamid, time_type)
                        datas.append((steamid, val))
                    except NoValueError as e:
                        print(e)
                print(rank_type, time_type, datas)
                if len(datas) == 0:
                    continue
                datas = sorted(datas, key=lambda x: x[1][0], reverse=rv)
                avg = sum([x[1][0] for x in datas]) / len(datas)
                datas = datas[:5]
                res = f"{rank_type}平均值{avg}，{rv_name}前五名："
                for x in datas:
                    res += f"{steamid_username[x[0]]} {x[1][0]}，"
                msgs.append({"role": "system", "content":res})
            except:
                import sys
                exc_type, exc_value, _ = sys.exc_info()
                logger.warning(f"{data} 解析失败 {exc_type} {exc_value}")
        msgs.append({"role": "user", "content": f"这是当前的记忆内容：{mem}"})
        msgs.append({"role": "assistant", "content": f"我会参考这些信息，请提出你的问题。"})
        msgs.append({"role": "user","content": text,})
        # logger.info(f"{msgs}")
        response = client.chat.completions.create(
            model=model_name,
            messages=msgs,
        )
        return response.choices[0].message.content
        
    except Exception as e:
        return f"发生错误: {str(e)}"

@aiasktest.handle()
async def aiasktest_function(message: MessageEvent, args: Message = CommandArg()):
    uid = message.get_user_id()
    sid = message.get_session_id()
    await aiasktest.finish(Message([
        MessageSegment.at(uid), " ",
        await ai_ask2(uid, sid, None, await db_val.work_msg(args))
    ]))

@aiask.handle()
async def aiask_function(message: MessageEvent, args: Message = CommandArg()):
    uid = message.get_user_id()
    sid = message.get_session_id()
    await aiasktb.finish(Message([
        MessageSegment.at(uid), " ",
        await ai_ask2(uid, sid, None, await db_val.work_msg(args))
    ]))

@aiasktb.handle()
async def aiasktb_function(message: MessageEvent, args: Message = CommandArg()):
    uid = message.get_user_id()
    sid = message.get_session_id()
    await aiasktb.finish(Message([
        MessageSegment.at(uid), " ",
        await ai_ask2(uid, sid, "贴吧", await db_val.work_msg(args))
    ]))

@aiaskxmm.handle()
async def aiaskxmm_function(message: MessageEvent, args: Message = CommandArg()):
    uid = message.get_user_id()
    sid = message.get_session_id()
    await aiaskxmm.finish(Message([
        MessageSegment.at(uid), " ",
        await ai_ask2(uid, sid, "xmm", await db_val.work_msg(args))
    ]))

@aiaskxhs.handle()
async def aiaskxhs_function(message: MessageEvent, args: Message = CommandArg()):
    uid = message.get_user_id()
    sid = message.get_session_id()
    await aiaskxhs.finish(Message([
        MessageSegment.at(uid), " ",
        await ai_ask2(uid, sid, "xhs", await db_val.work_msg(args))
    ]))

@aiasktmr.handle()
async def aiasktmr_function(message: MessageEvent, args: Message = CommandArg()):
    uid = message.get_user_id()
    sid = message.get_session_id()
    await aiasktmr.finish(Message([
        MessageSegment.at(uid), " ",
        await ai_ask2(uid, sid, "tmr", await db_val.work_msg(args))
    ]))

@aimem.handle()
async def aimem_function(message: MessageEvent, args: Message = CommandArg()):
    uid = message.get_user_id()
    sid = message.get_session_id()
    try:
        # 创建聊天完成请求
        client = OpenAI(
            api_key=config.cs_ai_api_key,
            base_url=config.cs_ai_url,
        )
        msgs = [{"role": "system", "content": "你需要管理需要记忆的内容，接下来会先给你当前记忆的内容，接着用户会给出新的内容，请整理输出记忆内容。由于记忆长度有限，请尽可能使用简单的语言，把更重要的信息放在靠前的位置。请不要输出无关内容，你的输出应当只包含需要记忆的内容。"}]
        mem = await db.get_mem(sid)
        msgs.append({"role": "user", "content": f"这是当前的记忆内容：{mem}"})
        msgs.append({"role": "assistant", "content": f"请继续给出需要添加进记忆的内容"})
        msgs.append({"role": "user", "content": await db_val.work_msg(args)})
        print(msgs)
        response = client.chat.completions.create(
            model=model_name,
            messages=msgs,
        )
        result = response.choices[0].message.content
        if len(result) > 1000:
            result = result[:1000] + "……"
        print(result)
        await db.set_mem(sid, result)
    except Exception as e:
        result = f"发生错误: {str(e)}"
    await aimem.finish(Message([
        MessageSegment.at(uid), " ",
        result
    ]))


