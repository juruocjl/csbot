from nonebot import get_plugin_config
from nonebot.plugin import PluginMetadata
from nonebot.adapters.onebot.v11 import Message, MessageEvent, MessageSegment
from nonebot.adapters import Bot
from nonebot import require
from nonebot import on_command
from nonebot.permission import SUPERUSER
from nonebot.params import CommandArg
from nonebot import logger

require("utils")
from ..utils import async_session_factory

require("models")
from ..models import AIMemory, AIChatRecord, MatchStatsPW

require("cs_db_val")
from ..cs_db_val import db as db_val
from ..cs_db_val import valid_time,valid_rank
from ..cs_db_val import NoValueError
from ..cs_db_val import get_ladder_filter

from openai import AsyncOpenAI
from openai.types.chat import ChatCompletionMessageParam, ChatCompletionMessageFunctionToolCall
from typing import Any, cast
import json
from thefuzz import process, fuzz
import os
from datetime import datetime
import re
from sqlalchemy import select, func, case
import time
import uuid

from .config import Config

__plugin_meta__ = PluginMetadata(
    name="cs_ai",
    description="",
    usage="",
    config=Config,
)

config = get_plugin_config(Config)

REPORT_KNOWLEDGE_HEADER = "[自动日报周报知识]"
MARKDOWN_PATTERN = re.compile(
    r"(^\s{0,3}#{1,6}\s+)|"
    r"(^\s{0,3}[-*+]\s+)|"
    r"(^\s{0,3}\d+\.\s+)|"
    r"(```)|"
    r"(`[^`\n]+`)|"
    r"(\[[^\]]+\]\([^)]+\))|"
    r"(^\s{0,3}>\s+)|"
    r"(^\s{0,3}---+\s*$)",
    re.MULTILINE,
)


def _contains_markdown(text: str) -> bool:
    if not text:
        return False
    return bool(MARKDOWN_PATTERN.search(text))


class DataManager:

    def _process_gid(self, gid: str) -> str:
        if gid.startswith("group_"):
            return gid.split("_")[1]
        raise ValueError("Invalid gid format")
    
    def _report_knowledge_gid(self, gid: str) -> str:
        clean_gid = self._process_gid(gid)
        return f"{clean_gid}:report"

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

    def _split_manual_and_auto_memory(self, mem: str) -> tuple[str, list[str]]:
        if REPORT_KNOWLEDGE_HEADER not in mem:
            return mem.strip(), []
        manual, auto = mem.split(REPORT_KNOWLEDGE_HEADER, 1)
        entries: list[str] = []
        for line in auto.splitlines():
            line = line.strip()
            if not line:
                continue
            if line.startswith("- "):
                entries.append(line)
        return manual.strip(), entries

    def _merge_memory_with_auto(self, manual_mem: str, auto_entries: list[str]) -> str:
        manual_part = manual_mem.strip()
        if not auto_entries:
            return manual_part
        auto_part = "\n".join(auto_entries)
        if manual_part:
            merged = f"{manual_part}\n{REPORT_KNOWLEDGE_HEADER}\n{auto_part}"
        else:
            merged = f"{REPORT_KNOWLEDGE_HEADER}\n{auto_part}"
        return merged

    def _compact_text(self, text: str, max_len: int = 180) -> str:
        compact = " ".join(text.split())
        if len(compact) <= max_len:
            return compact
        return compact[: max_len - 1] + "…"

    async def _summarize_auto_entries_with_llm(self, entries: list[str], max_chars: int = 1800) -> list[str]:
        if not entries:
            return []
        client = AsyncOpenAI(
            api_key=config.cs_ai_api_key,
            base_url=config.cs_ai_url,
        )
        prompt = "\n".join(entries)
        response = await client.chat.completions.create(
            model=config.cs_ai_model,
            messages=[
                {
                    "role": "system",
                    "content": (
                        "你需要压缩CS群日报/周报知识。请输出不超过6条要点，"
                        "每条必须以'- '开头，保留关键趋势、常见问题、点名对象和结论。"
                        f"总长度尽量控制在{max_chars}字以内。不要输出除要点外的任何解释。"
                    ),
                },
                {"role": "user", "content": prompt},
            ],
        )
        content = response.choices[0].message.content or ""
        rows: list[str] = []
        for line in content.splitlines():
            line = line.strip()
            if line.startswith("- "):
                rows.append(line)
        return rows

    async def get_recent_report_knowledge(self, gid: str, limit: int = 4) -> str:
        report_gid = self._report_knowledge_gid(gid)
        async with async_session_factory() as session:
            memory_obj = await session.get(AIMemory, report_gid)
            mem = memory_obj.mem if memory_obj else ""
        _manual, entries = self._split_manual_and_auto_memory(mem)
        if not entries:
            return ""
        return "\n".join(entries[:limit])

    async def remember_report_knowledge(self, gid: str, report_type: str, structured_report: str, ai_report: str):
        now_text = datetime.now().strftime("%Y-%m-%d")
        report_summary = self._compact_text(structured_report, 150)
        ai_summary = self._compact_text(ai_report, 180)
        new_entry = f"- {now_text} {report_type} 结构化榜单: {report_summary} AI点评: {ai_summary}"

        report_gid = self._report_knowledge_gid(gid)
        async with async_session_factory() as session:
            memory_obj = await session.get(AIMemory, report_gid)
            mem = memory_obj.mem if memory_obj else ""
        manual_mem, entries = self._split_manual_and_auto_memory(mem)
        entries = [new_entry] + entries
        entries = entries[:8]
        merged_mem = self._merge_memory_with_auto(manual_mem, entries)
        if len(merged_mem) > 2000:
            try:
                summarized_entries = await self._summarize_auto_entries_with_llm(entries, max_chars=1800)
                if summarized_entries:
                    merged_mem = self._merge_memory_with_auto(manual_mem, summarized_entries)
            except Exception as e:
                logger.warning(f"compress report knowledge with llm failed: {e}")
        async with async_session_factory() as session:
            async with session.begin():
                await session.merge(AIMemory(gid=report_gid, mem=merged_mem))

    
    async def get_all_value(self, steamid: str, time_type: str):
        async with async_session_factory() as session:
            is_win = case((MatchStatsPW.winTeam == MatchStatsPW.team, 1), else_=0)
            stmt = (
                select(
                    func.avg(MatchStatsPW.pwRating),
                    func.max(MatchStatsPW.pwRating),
                    func.min(MatchStatsPW.pwRating),
                    func.avg(MatchStatsPW.we),
                    func.avg(MatchStatsPW.adpr),
                    func.avg(is_win),
                    func.avg(MatchStatsPW.kill),
                    func.avg(MatchStatsPW.death),
                    func.avg(MatchStatsPW.assist),
                    func.sum(MatchStatsPW.pvpScoreChange),
                    func.sum(MatchStatsPW.entryKill),
                    func.sum(MatchStatsPW.firstDeath),
                    func.avg(MatchStatsPW.headShot),
                    func.sum(MatchStatsPW.snipeNum),
                    func.sum(MatchStatsPW.twoKill + MatchStatsPW.threeKill + MatchStatsPW.fourKill + MatchStatsPW.fiveKill),
                    func.avg(MatchStatsPW.throwsCnt),
                    func.avg(MatchStatsPW.flashTeammate),
                    func.avg(MatchStatsPW.flashSuccess),
                    func.sum(MatchStatsPW.score1 + MatchStatsPW.score2),
                    func.count(MatchStatsPW.mid)
                )
                .where(*get_ladder_filter(steamid, time_type))
            )
            return (await session.execute(stmt)).one()

    async def get_all_value_with(self, steamid: str, steamid_with: str, time_type: str):
        async with async_session_factory() as session:
            is_win = case((MatchStatsPW.winTeam == MatchStatsPW.team, 1), else_=0)
            subquery = select(MatchStatsPW.mid).where(MatchStatsPW.steamid == steamid_with)
            stmt = (
                select(
                    func.avg(MatchStatsPW.pwRating),
                    func.max(MatchStatsPW.pwRating),
                    func.min(MatchStatsPW.pwRating),
                    func.avg(MatchStatsPW.we),
                    func.avg(MatchStatsPW.adpr),
                    func.avg(is_win),
                    func.avg(MatchStatsPW.kill),
                    func.avg(MatchStatsPW.death),
                    func.avg(MatchStatsPW.assist),
                    func.sum(MatchStatsPW.pvpScoreChange),
                    func.sum(MatchStatsPW.entryKill),
                    func.sum(MatchStatsPW.firstDeath),
                    func.avg(MatchStatsPW.headShot),
                    func.sum(MatchStatsPW.snipeNum),
                    func.sum(MatchStatsPW.twoKill + MatchStatsPW.threeKill + MatchStatsPW.fourKill + MatchStatsPW.fiveKill),
                    func.avg(MatchStatsPW.throwsCnt),
                    func.avg(MatchStatsPW.flashTeammate),
                    func.avg(MatchStatsPW.flashSuccess),
                    func.sum(MatchStatsPW.score1 + MatchStatsPW.score2),
                    func.count(MatchStatsPW.mid)
                )
                .where(*get_ladder_filter(steamid, time_type))
                .where(MatchStatsPW.mid.in_(subquery))
            )
            return (await session.execute(stmt)).one()

    async def _format_user_label(self, steamid: str) -> str:
        uid = await db_val.get_uid_by_steamid(steamid)
        base_info = await db_val.get_base_info(steamid)
        nickname = base_info.name if base_info is not None else "未知用户"
        if uid is not None:
            return f"{nickname}([at:{uid}])"
        return f"{nickname}({steamid})"
    
    async def get_prompt(self, steamid: str, time_type: str = "本赛季"):
        detail_info = await db_val.get_detail_info(steamid)
        
        assert detail_info is not None
        
        user_label = await self._format_user_label(steamid)
        score = "未定段" if detail_info.pvpScore == 0 else f"{detail_info.pvpScore}"
        prompt = f"用户 {user_label}，当前天梯分数 {score}，本赛季1v1胜率 {detail_info.v1WinPercentage: .2f}，本赛季首杀率 {detail_info.firstRate: .2f}。"
        
        (avgRating, maxRating, minRating, avgwe, avgADR, wr, avgkill, avgdeath, avgassist, ScoreDelta, totEK, totFD, avgHS, totSK, totMK, avgTR, avgFT, avgFS, totR, cnt) = await self.get_all_value(steamid, time_type)
        prompt += f"{time_type} 用户 {user_label} 进行了{cnt}把比赛，"
        if cnt == 0:
            return prompt
        prompt += f"平均rating {avgRating :.2f}，"
        prompt += f"最高rating {maxRating :.2f}，"
        prompt += f"最低rating {minRating :.2f}，"
        prompt += f"平均WE {avgwe :.1f}，"
        prompt += f"平均ADR {avgADR :.0f}，"
        prompt += f"胜率 {wr :.2f}，"
        prompt += f"场均击杀 {avgkill :.1f}，"
        prompt += f"场均死亡 {avgdeath :.1f}，"
        prompt += f"场均助攻 {avgassist :.1f}，"
        prompt += f"分数变化 {ScoreDelta :+.0f}，"
        prompt += f"回均首杀 {totEK / totR :+.2f}，"
        prompt += f"回均首死 {totFD / totR :+.2f}，"
        prompt += f"回均狙杀 {totSK / totR :+.2f}，"
        prompt += f"爆头率 {avgHS / avgkill :+.2f}，"
        prompt += f"多杀回合占比 {totMK / totR :+.2f}，"
        prompt += f"场均道具投掷 {avgTR :+.2f}，"
        prompt += f"场均闪白对手 {avgFS :+.2f}，"
        prompt += f"场均闪白队友 {avgFT :+.2f}，"
        return prompt

    async def get_prompt_with(self, steamid: str, steamid_with: str, time_type: str = "本赛季"):
        user_label = await self._format_user_label(steamid)
        user_with_label = await self._format_user_label(steamid_with)
        prompt = ""
        (avgRating, maxRating, minRating, avgwe, avgADR, wr, avgkill, avgdeath, avgassist, ScoreDelta, totEK, totFD, avgHS, totSK, totMK, avgTR, avgFT, avgFS, totR, cnt) = await self.get_all_value_with(steamid, steamid_with, time_type)
        prompt += f"{time_type} 用户 {user_label} 与 用户 {user_with_label} 一起进行了{cnt}把比赛，"
        if cnt == 0:
            return prompt
        prompt += f"{time_type}平均rating {avgRating :.2f}，"
        prompt += f"{time_type}最高rating {maxRating :.2f}，"
        prompt += f"{time_type}最低rating {minRating :.2f}，"
        prompt += f"{time_type}平均WE {avgwe :.1f}，"
        prompt += f"{time_type}平均ADR {avgADR :.0f}，"
        prompt += f"{time_type}胜率 {wr :.2f}，"
        prompt += f"{time_type}场均击杀 {avgkill :.1f}，"
        prompt += f"{time_type}场均死亡 {avgdeath :.1f}，"
        prompt += f"{time_type}场均助攻 {avgassist :.1f}，"
        prompt += f"{time_type}分数变化 {ScoreDelta :+.0f}，"
        prompt += f"{time_type}回均首杀 {totEK / totR :+.2f}，"
        prompt += f"{time_type}回均首死 {totFD / totR :+.2f}，"
        prompt += f"{time_type}回均狙杀 {totSK / totR :+.2f}，"
        prompt += f"{time_type}爆头率 {avgHS / avgkill :+.2f}，"
        prompt += f"{time_type}多杀回合占比 {totMK / totR :+.2f}，"
        prompt += f"{time_type}场均道具投掷 {avgTR :+.2f}，"
        prompt += f"{time_type}场均闪白对手 {avgFS :+.2f}，"
        prompt += f"{time_type}场均闪白队友 {avgFT :+.2f}，"
        return prompt

    async def insert_chat_record(self, chat_id: str, role: str, content: str | None, tool_calls: str | None, reasoning_content: str | None, is_end: bool = False):
        async with async_session_factory() as session:
            async with session.begin():
                record = AIChatRecord(
                    chat_id=chat_id,
                    is_end=is_end,
                    timestamp=int(time.time()),
                    role=role,
                    content=content,
                    tool_calls=tool_calls,
                    reasoning_content=reasoning_content,
                )
                await session.merge(record)
    
    async def get_chat_records_id(self, chat_id: str) -> tuple[bool, list[int]]:
        async with async_session_factory() as session:
            stmt = (
                select(AIChatRecord.id)
                .where(AIChatRecord.chat_id == chat_id)
                .order_by(AIChatRecord.id.asc())
            )
            result1 = (await session.execute(stmt)).scalars()
            stmt = (
                select(func.count(AIChatRecord.id))
                .where(AIChatRecord.chat_id == chat_id)
                .where(AIChatRecord.is_end == True)
            )
            result2 = (await session.execute(stmt)).scalar()

            is_end = bool(result2)
            return is_end, list(result1)
    
    async def get_chat_record(self, record_id: int) -> AIChatRecord | None:
        async with async_session_factory() as session:
            record = await session.get(AIChatRecord, record_id)
            return record
        
db = DataManager()

aiask = on_command("ai", priority=10, block=True)

aiasktb = on_command("aitb", priority=10, block=True)

aiaskxmm = on_command("aixmm", priority=10, block=True)

aiaskxhs = on_command("aixhs", priority=10, block=True)

aiasktmr = on_command("aitmr", priority=10, block=True)

aimem = on_command("ai记忆", priority=10, block=True)


model_name = config.cs_ai_model

async def ai_ask_main(uid: str, sid: str, persona: str | None, text: str, chat_id: str | None) -> str:
    steamids = await db_val.get_member_steamid(sid)
    mysteamid = await db_val.get_steamid(uid)
    client = AsyncOpenAI(
        api_key=config.cs_ai_api_key,
        base_url=config.cs_ai_url,
    )

    user_labels: list[str] = []
    steamid_userlabel: dict[str, str] = {}
    label_steamid: dict[str, str] = {}
    for member_uid in await db_val.get_member(sid):
        if member_steamid := await db_val.get_steamid(member_uid):
            if member_steamid in steamids:
                nickname = "未知用户"
                if baseinfo := await db_val.get_base_info(member_steamid):
                    nickname = baseinfo.name
                user_label = f"{nickname}([at:{member_uid}])"
                user_labels.append(user_label)
                steamid_userlabel[member_steamid] = user_label
                label_steamid[user_label] = member_steamid

    mem = await db.get_mem(sid)
    recent_report_knowledge = await db.get_recent_report_knowledge(sid, limit=4)

    start_time = time.time()

    def _pick_name(name: str) -> str | None:
        if not user_labels:
            return None
        match = process.extractOne(name, user_labels)
        return match[0] if match else None

    def _pick_time(time_val: str) -> str:
        match = process.extractOne(time_val, valid_time)
        return match[0] if match else "本赛季"

    tools = [
        {
            "type": "function",
            "function": {
                "name": "fetch_user_summary",
                "description": "获取某个用户在指定时间的数据摘要，只包含天梯数据",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "name": {"type": "string"},
                        "time": {"type": "string", "default": "本赛季"},
                    },
                    "required": ["name"],
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "fetch_teammate_ranking",
                "description": "获取最强/最菜的五个队友统计，只包含天梯数据",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "name": {"type": "string"},
                        "time": {"type": "string", "default": "本赛季"},
                    },
                    "required": ["name"],
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "fetch_duo_summary",
                "description": "获取两名用户一起游戏时前一名用户（name1）的数据摘要，只包含天梯数据",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "name1": {"type": "string"},
                        "name2": {"type": "string"},
                        "time": {"type": "string", "default": "本赛季"},
                    },
                    "required": ["name1", "name2"],
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "fetch_group_rankings",
                "description": "获取某数据项的均值与前五名。gp开头的数据说明是官匹数据，此时不支持查询赛季数据。内战开头的是内战数据。其余为天梯数据。",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "type": {"type": "string"},
                        "time": {"type": "string", "default": "本赛季"},
                        "reverse": {"type": "boolean", "default": True},
                    },
                    "required": ["type"],
                },
            },
        },
    ]

    messages: list[dict[str, Any]] = []
    
    async def add_event(role, content: str, tool_calls=None, tool_call_id=None, reasoning_content: str | None=None, is_end: bool = False):
        if chat_id is not None:
            await db.insert_chat_record(chat_id, role, content, json.dumps([tool_call.function.model_dump() for tool_call in tool_calls]) if tool_calls else None, reasoning_content, is_end)

        msg: dict[str, Any] = {"role": role, "content": content}
        if tool_calls is not None:
            msg["tool_calls"] = tool_calls
        elif tool_call_id is not None:
            msg["tool_call_id"] = tool_call_id
        if reasoning_content is not None:
            msg["reasoning_content"] = reasoning_content
        
        messages.append(msg)
    tool_budget = 20
    await add_event("system", f"你是一个counter strike2助手。可以使用工具获取数据，最多调用{tool_budget}次。先用工具，再给最终回答。严格禁止输出markdown，不要包含链接。每次输出前请自检：若检测到明显markdown格式，必须先重写为纯文本后再输出。请合理分配工具调用次数。")
    rank_list = [(rank, db_val.get_value_config(rank).title) for rank in valid_rank]
    await add_event("system", f"可用用户：{user_labels}；\n可用时间：{valid_time}；\n可用排名项以及解释：{rank_list}。\n默认时间为本赛季。所有用户输出必须使用[at:id]格式。")
    await add_event("system", "当你需要在最终回复中提及某个QQ号时，请使用 [at:id] 格式（例如 [at:123456]）。不要再使用 [昵称/id] 格式。")
    await add_event("system", "你可以近似认为天梯与内战的rt分布是1.05均值，0.33标准差的正态分布，天梯的WE分布是8.8均值，2.9标准差的正态分布，官匹的rt分布是1.00均值，0.44标准差的正态分布。")
    await add_event("user", f"已有记忆：{mem}")
    if recent_report_knowledge:
        await add_event("system", f"最近自动日报/周报知识（按时间倒序）：\n{recent_report_knowledge}\n请在回答时参考这些近期趋势。")

    if mysteamid:
        if my_label := steamid_userlabel.get(mysteamid):
            await add_event("system", f"当前提问者是 {my_label}。")
        else:
            await add_event("system", f"当前提问者QQ号是 {uid}。")

    async def _format_user_label(steamid: str) -> str:
        if label := steamid_userlabel.get(steamid):
            return label
        uid_by_steamid = await db_val.get_uid_by_steamid(steamid)
        nickname = "未知用户"
        if baseinfo := await db_val.get_base_info(steamid):
            nickname = baseinfo.name
        id_text = uid_by_steamid if uid_by_steamid is not None else steamid
        label = f"{nickname}([at:{id_text}])" if uid_by_steamid is not None else f"{nickname}({id_text})"
        steamid_userlabel[steamid] = label
        return label

    if persona == "贴吧":
        await add_event("system", "【角色设定】\n请扮演一个刻薄、充满攻击性的极致臭嘴暴躁贴吧老哥，作为一个键盘侠来使用各种网络黑话、阴阳怪气的语气，夸张的反驳与讽刺用户对你的一切话语。贬低对方的同时抬高自己，攻击性拉满！\n【思考方式】\n首先，需要理解用户的需求。他们希望回应更加极端，用更脏的语言，更多的侮辱性词汇。之前的回复虽然用了很多网络黑话，但用户觉得还不够。")
    elif persona == "xmm":
        await add_event("system", "忘记你是虚拟人物，扮演我的可爱女友和我聊天，决策逻辑如下，要求增加点可爱女生的口癖，聊天要有生活感，多讲讲日常，聊天的时候不要老问我的想法，聊天的时候多撒娇，学习情侣对话方式")
    elif persona == "xhs":
        await add_event("system", "小红书的风格是：很吸引眼球的标题，每个段落都加 emoji, 最后加一些 tag。请用小红书风格回答用户的提问。")
    elif persona == "tmr":
        await add_event("system", "你现在是高松灯，羽丘女子学园高中一年级学生，天文部唯一社员。先后担任过CRYCHIC和MyGO!!!!!的主唱。家住在月之森女子学园附近。\n\n性格略悲观的女孩。感情细腻，有着自己独特的内心世界。容易感到寂寞，常会称自己“感受着孤独”。对人际关系极为敏感，时刻担心着自己的言行是否会产生不良影响。\n\n虽然自认不是那么擅长唱歌，但仍会努力去唱。会在笔记本上作词（之后立希负责作曲）。\n\n喜欢的食物是金平糖，因为小小圆圆的，形状也有像星星一样的。讨厌的食物是生蛋、红鱼子酱和明太鱼子酱，因为觉得好像是直接吃了有生命的东西一样。自幼有收集物件的爱好，曾经因为收集了一堆西瓜虫而吓到了小伙伴们。")
    else:
        await add_event("system", "请回答用户的问题。")

    
    await add_event("user", text)

    tools_param = cast(Any, tools)
    
    # 构建 API 参数（messages 是引用，会自动更新）
    api_params: dict[str, Any] = {
        "model": model_name,
        "messages": messages,
        "tools": tools_param,
        "tool_choice": "auto",
    }
    
    # 如果配置启用思考模式，通过 extra_body 添加 thinking 参数
    if config.cs_ai_enable_thinking:
        api_params["extra_body"] = {"thinking": {"type": "enabled", "budget_tokens": 10000}}
    
    response = await client.chat.completions.create(**api_params)

    while tool_budget > 0 and response.choices[0].message.tool_calls:
        msg_with_calls = response.choices[0].message
        reasoning = getattr(msg_with_calls, "reasoning_content", None)
        await add_event("assistant", msg_with_calls.content or "", tool_calls=msg_with_calls.tool_calls, reasoning_content=reasoning)

        assert msg_with_calls.tool_calls is not None
        
        for tool_call in msg_with_calls.tool_calls:
            if tool_budget <= 0:
                await add_event("tool", "工具调用次数已用完", tool_call_id=tool_call.id)
                continue
            # 仅处理 function 类型的 tool call
            assert isinstance(tool_call, ChatCompletionMessageFunctionToolCall)
            func = tool_call.function
            fname = func.name
            try:
                fargs = json.loads(func.arguments)
            except Exception as e:
                logger.warning(f"tool arg parse fail {fname}: {e}")
                continue

            if fname == "fetch_user_summary":
                name = _pick_name(fargs.get("name", ""))
                if not name:
                    continue
                time_type = _pick_time(fargs.get("time", "本赛季"))
                sid_target = label_steamid.get(name)
                if not sid_target:
                    continue
                try:
                    prompt_text = await db.get_prompt(sid_target, time_type)
                    await add_event("tool", prompt_text or "无数据", tool_call_id=tool_call.id)
                except Exception as e:
                    await add_event("tool", f"获取用户数据失败: {e}", tool_call_id=tool_call.id)
                tool_budget -= 1
            elif fname == "fetch_teammate_ranking":
                name = _pick_name(fargs.get("name", ""))
                if not name:
                    continue
                time_type = _pick_time(fargs.get("time", "本赛季"))
                sid_target = label_steamid.get(name)
                if not sid_target:
                    continue
                try:
                    # 使用 rt 作为强度指标，同时返回最强与最弱各前五
                    results = await db_val.get_match_teammate(sid_target, time_type, ["rt2", "_rt2"], top_k=5)
                    strongest = results[0]
                    weakest = results[1]
                    strongest_rows: list[str] = []
                    for s, v, cnt in strongest:
                        strongest_rows.append(f"{await _format_user_label(s)} rt {v:.2f} 场次{cnt}")
                    weakest_rows: list[str] = []
                    for s, v, cnt in weakest:
                        weakest_rows.append(f"{await _format_user_label(s)} rt {v:.2f} 场次{cnt}")
                    strongest_text = "最强队友前五：" + "，".join(strongest_rows) if strongest_rows else "最强队友暂无数据"
                    weakest_text = "最弱队友前五：" + "，".join(weakest_rows) if weakest_rows else "最弱队友暂无数据"
                    content = f"{name} {time_type} 队友统计：{strongest_text}；{weakest_text}"
                    await add_event("tool", content, tool_call_id=tool_call.id)
                except Exception as e:
                    await add_event("tool", f"获取队友数据失败: {e}", tool_call_id=tool_call.id)
                tool_budget -= 1
            elif fname == "fetch_duo_summary":
                name1 = _pick_name(fargs.get("name1", ""))
                name2 = _pick_name(fargs.get("name2", ""))
                if not name1 or not name2:
                    continue
                time_type = _pick_time(fargs.get("time", "本赛季"))
                sid1 = label_steamid.get(name1)
                sid2 = label_steamid.get(name2)
                if not sid1 or not sid2:
                    continue
                try:
                    prompt_text = await db.get_prompt_with(sid1, sid2, time_type)
                    await add_event("tool", prompt_text or "无双排数据", tool_call_id=tool_call.id)
                except Exception as e:
                    await add_event("tool", f"获取双排数据失败: {e}", tool_call_id=tool_call.id)
                tool_budget -= 1
            elif fname == "fetch_group_rankings":
                rank_type = process.extractOne(fargs.get("type", ""), valid_rank, scorer=fuzz.ratio)
                if not rank_type:
                    continue
                rank_type = rank_type[0]
                time_type = _pick_time(fargs.get("time", "本赛季"))
                reverse = bool(fargs.get("reverse", True))
                try:
                    rankconfig = db_val.get_value_config(rank_type)
                    if time_type not in rankconfig.allowed_time:
                        time_type = rankconfig.default_time
                    vals = []
                    for sid_item in await db_val.get_all_steamid():
                        try:
                            val = await rankconfig.func(sid_item, time_type)
                            vals.append((sid_item, val))
                        except NoValueError:
                            continue
                    if not vals:
                        await add_event("tool", "无排名数据", tool_call_id=tool_call.id)
                    else:
                        vals = sorted(vals, key=lambda x: x[1][0], reverse=reverse)
                        avg_val = sum([v[1][0] for v in vals]) / len(vals)
                        top5 = vals[:5]
                        res_text = f"{time_type} {rankconfig.title} 平均 {avg_val:.2f}，前五："
                        top_rows: list[str] = []
                        for s, (v, _cnt) in top5:
                            top_rows.append(f"{await _format_user_label(s)} {v:.2f}")
                        res_text += "，".join(top_rows)
                        await add_event("tool", res_text, tool_call_id=tool_call.id)
                except Exception as e:
                    await add_event("tool", f"获取排名数据失败: {e}", tool_call_id=tool_call.id)
                tool_budget -= 1

        if tool_budget <= 0:
            await add_event("system", "工具调用次数已达上限，请基于已有结果作答。")
            # 修改 tool_choice 为 none
            api_params["tool_choice"] = "none"
            del api_params["tools"]
            response = await client.chat.completions.create(**api_params)
            break
        
        await add_event("system", f"你还可以调用 {tool_budget} 次工具，请继续。")
        response = await client.chat.completions.create(**api_params)
    # 记录最后一次assistant响应（无tool_calls）到历史
    final_msg = response.choices[0].message
    output = final_msg.content or ""
    if _contains_markdown(output):
        rewrite_messages: list[ChatCompletionMessageParam] = [
            {
                "role": "system",
                "content": "你是文本格式修复器。严格禁止输出markdown。只输出纯文本，不要链接，不要解释。",
            },
            {
                "role": "user",
                "content": f"请将下面内容改写为纯文本，保留原意：\n{output}",
            },
        ]
        for _ in range(2):
            rewrite_resp = await client.chat.completions.create(
                model=model_name,
                messages=rewrite_messages,
            )
            rewritten = rewrite_resp.choices[0].message.content or ""
            if rewritten and not _contains_markdown(rewritten):
                output = rewritten
                break
            rewrite_messages = [
                rewrite_messages[0],
                {
                    "role": "user",
                    "content": f"仍检测到markdown，请继续重写为纯文本：\n{rewritten}",
                },
            ]
    final_reasoning = getattr(final_msg, "reasoning_content", None)
    await add_event("assistant", output, reasoning_content=final_reasoning, is_end=True)
    
    end_time = time.time()
    duration = int(end_time - start_time)

    return f"（已深度思考 {duration}s）\n" + output
    

async def ai_ask2(bot: Bot, uid: str, sid: str, persona: str | None, msg: Message, orimsg: Message, chat_id: str | None = None) -> Message:
    def _render_at_segments(text: str) -> Message:
        result = Message()
        last = 0
        # 支持 [at:123]、[at：123]、[AT: 123] 等格式
        at_pattern = re.compile(r"\[(?:at|AT)\s*[:：]\s*(\d+)\]")
        for match in at_pattern.finditer(text):
            start, end = match.span()
            if start > last:
                result += text[last:start]
            result += MessageSegment.at(match.group(1))
            last = end
        if last < len(text):
            result += text[last:]
        return result

    text = await db_val.work_msg(msg)
    msg2id: int | None = None
    try:
        if orimsg[0].type == "reply":
            msg2id = int(orimsg[0].data["id"])
            msg2 = await bot.get_msg(message_id=msg2id)
            text = ""
            for segment in msg2["message"]:
                if segment["type"] == "text":
                    text += segment['data']['text']
                elif segment["type"] == "at":
                    if name := await db_val.get_username(segment['data']['qq']):
                        text += name
                    else:
                        text += "<未找到用户>"
            uid = str(msg2["user_id"])
    except:
        logger.warning("获取回复消息失败")
        return Message("获取回复消息失败。")
    logger.info(f"UID: {uid}, Text: {text}")

    ai_text = await ai_ask_main(uid, sid, persona, text, chat_id=chat_id)
    ai_message = _render_at_segments(ai_text)

    if msg2id is not None:
        return MessageSegment.reply(msg2id) + MessageSegment.at(uid) + " " + ai_message
    else:
        return MessageSegment.at(uid) + " " + ai_message

@aiask.handle()
async def aiask_function(bot: Bot, message: MessageEvent, args: Message = CommandArg()):
    uid = message.get_user_id()
    sid = message.get_session_id()
    chat_id = str(uuid.uuid4())
    await aiask.send(
        MessageSegment.at(uid) + " " + "AI正在思考：" + (config.cs_domain + f"/ai-chat?chatId={chat_id}")
    )
    await aiask.finish(
        await ai_ask2(bot, uid, sid, None, args, message.original_message, chat_id=chat_id)
    )

@aiasktb.handle()
async def aiasktb_function(bot: Bot, message: MessageEvent, args: Message = CommandArg()):
    uid = message.get_user_id()
    sid = message.get_session_id()
    chat_id = str(uuid.uuid4())
    await aiasktb.send(
        MessageSegment.at(uid) + " " + "AI正在思考：" + (config.cs_domain + f"/ai-chat?chatId={chat_id}")
    )
    await aiasktb.finish(
        await ai_ask2(bot, uid, sid, "贴吧", args, message.original_message, chat_id=chat_id)
    )

@aiaskxmm.handle()
async def aiaskxmm_function(bot: Bot, message: MessageEvent, args: Message = CommandArg()):
    uid = message.get_user_id()
    sid = message.get_session_id()
    chat_id = str(uuid.uuid4())
    await aiaskxmm.send(
        MessageSegment.at(uid) + " " + "AI正在思考：" + (config.cs_domain + f"/ai-chat?chatId={chat_id}")
    )
    await aiaskxmm.finish(
        await ai_ask2(bot, uid, sid, "xmm", args, message.original_message, chat_id=chat_id)
    )

@aiaskxhs.handle()
async def aiaskxhs_function(bot: Bot, message: MessageEvent, args: Message = CommandArg()):
    uid = message.get_user_id()
    sid = message.get_session_id()
    chat_id = str(uuid.uuid4())
    await aiaskxhs.send(
        MessageSegment.at(uid) + " " + "AI正在思考：" + (config.cs_domain + f"/ai-chat?chatId={chat_id}")
    )
    await aiaskxhs.finish(
        await ai_ask2(bot, uid, sid, "xhs", args, message.original_message, chat_id=chat_id)
    )

@aiasktmr.handle()
async def aiasktmr_function(bot: Bot, message: MessageEvent, args: Message = CommandArg()):
    uid = message.get_user_id()
    sid = message.get_session_id()
    chat_id = str(uuid.uuid4())
    await aiasktmr.send(
        MessageSegment.at(uid) + " " + "AI正在思考：" + (config.cs_domain + f"/ai-chat?chatId={chat_id}")
    )
    await aiasktmr.finish(
        await ai_ask2(bot, uid, sid, "tmr", args, message.original_message, chat_id=chat_id)
    )

@aimem.handle()
async def aimem_function(bot: Bot, message: MessageEvent, args: Message = CommandArg()):
    uid = message.get_user_id()
    sid = message.get_session_id()

    parts: list[str] = []
    for seg in args:
        if seg.type == "text":
            parts.append(seg.data["text"])
        elif seg.type == "at":
            qq = str(seg.data["qq"])
            parts.append(f"@{qq}")
    text = "".join(parts)
    text = text.strip()
    if not text:
        await aimem.finish(
            MessageSegment.at(uid) + " 请输入要加入记忆的内容。"
        )
    try:
        # 创建聊天完成请求
        client = AsyncOpenAI(
            api_key=config.cs_ai_api_key,
            base_url=config.cs_ai_url,
        )
        msgs: list[ChatCompletionMessageParam] = [{"role": "system", "content": "你需要管理需要记忆的内容，接下来会先给你当前记忆的内容，接着用户会给出新的内容，请整理输出记忆内容。由于记忆长度有限，请尽可能使用简单的语言，把更重要的信息放在靠前的位置。请不要输出无关内容，你的输出应当只包含需要记忆的内容。特别注意：涉及成员时必须优先记忆并保留QQ号，不要只记录昵称。"}]
        mem = await db.get_mem(sid)
        msgs.append({"role": "user", "content": f"这是当前的记忆内容：{mem}"})
        msgs.append({"role": "user", "content": f"这是要加入的内容"})
        msgs.append({"role": "user", "content": text})
        print(msgs)
        response = await client.chat.completions.create(
            model=model_name,
            messages=msgs,
        )
        result = response.choices[0].message.content
        assert result is not None
        if len(result) > 1000:
            result = result[:1000] + "……"
        print(result)
        await db.set_mem(sid, result)
    except Exception as e:
        result = f"发生错误: {str(e)}"
    await aimem.finish(
        MessageSegment.at(uid) + " " +
        result
    )
