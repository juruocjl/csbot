from ai_runtime.image_archive import image_segment
from nonebot import get_plugin_config
from nonebot.plugin import PluginMetadata
from nonebot.adapters.onebot.v11 import Message, MessageEvent, MessageSegment, GroupMessageEvent
from nonebot.adapters import Bot
from nonebot import require
from nonebot import on_command, on_message
from nonebot.permission import SUPERUSER
from nonebot.params import CommandArg
from nonebot import logger

require("utils")
from ..utils import async_session_factory, get_session

require("runtime_config")
from ..runtime_config import runtime_config

require("models")
from ..models import AIChatRecord, ImgCacheInfo, MatchStatsGP, MatchStatsPW

require("chat_history")
from ..chat_history import db as chat_history_db
from ..chat_history import group_id_from_sid

require("cs_db_val")
from ..cs_db_val import db as db_val
from ..cs_db_val import valid_time,valid_rank
from ..cs_db_val import gp_time
from ..cs_db_val import NoValueError
from ..cs_db_val import get_ladder_filter

from openai import AsyncOpenAI
from openai.types.chat import ChatCompletionMessageParam
from typing import Any, cast
import json
import base64
from io import BytesIO
from thefuzz import process, fuzz
import os
from datetime import datetime
from pathlib import Path
import re
from PIL import Image
from ai_runtime.media import media_cache
from sqlalchemy import select, func, case
import time
import uuid
import aiohttp

from .config import Config
from .output_guard import (
    QQ_GUARD_FAILED_TEXT,
    QQ_OUTPUT_GUARD_TOOL,
    QQOutputGuardResult,
    build_qq_guard_messages,
    normalize_qq_plain_text,
    parse_qq_guard_response,
)

__plugin_meta__ = PluginMetadata(
    name="cs_ai",
    description="",
    usage="",
    config=Config,
)

config = get_plugin_config(Config)
runtime_config.register_default("cs_ai_model", config.cs_ai_model)

GROUP_RANKING_RESULT_LIMIT = 10
AI_FORWARD_MIN_CHARS = 500
AI_FORWARD_MIN_LINES = 6
AI_FORWARD_NODE_MAX_CHARS = 1600

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
QQ_ID_PATTERN = re.compile(r"^\d{5,12}$")
IMAGE_ID_PATTERN = re.compile(r"^[0-9a-fA-F]{16,64}$")
IMAGE_CACHE_DIR = Path("imgs") / "history" / "full"
MAX_AI_IMAGE_BYTES = 20 * 1024 * 1024
MAX_AI_GIF_FRAMES = 6
MAX_AI_GIF_FRAME_EDGE = 1280


def _contains_markdown(text: str) -> bool:
    if not text:
        return False
    return bool(MARKDOWN_PATTERN.search(text))


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


def _should_forward_ai_result(text: str) -> bool:
    return len(text) >= AI_FORWARD_MIN_CHARS or text.count("\n") + 1 >= AI_FORWARD_MIN_LINES


def _split_forward_text(text: str, max_chars: int = AI_FORWARD_NODE_MAX_CHARS) -> list[str]:
    paragraphs = text.splitlines(keepends=True)
    chunks: list[str] = []
    current = ""
    for paragraph in paragraphs:
        if len(paragraph) > max_chars:
            if current.strip():
                chunks.append(current.rstrip())
                current = ""
            for start in range(0, len(paragraph), max_chars):
                chunk = paragraph[start : start + max_chars].strip()
                if chunk:
                    chunks.append(chunk)
            continue
        if len(current) + len(paragraph) > max_chars and current.strip():
            chunks.append(current.rstrip())
            current = paragraph
        else:
            current += paragraph
    if current.strip():
        chunks.append(current.rstrip())
    return chunks or [text]


async def _guard_qq_output(client: AsyncOpenAI, draft: str) -> QQOutputGuardResult:
    guard_model = config.cs_ai_guard_model.strip() or await runtime_config.get("cs_ai_model")
    last_error: Exception | None = None
    for attempt in range(2):
        try:
            guard_messages = build_qq_guard_messages(draft)
            if attempt:
                guard_messages.append(
                    {
                        "role": "system",
                        "content": "上一次返回格式无效。必须调用 submit_qq_guard_decision，不要输出普通文本。",
                    }
                )
            response = await client.chat.completions.create(
                model=guard_model,
                messages=cast(list[ChatCompletionMessageParam], guard_messages),
                tools=cast(Any, [QQ_OUTPUT_GUARD_TOOL]),
                tool_choice={
                    "type": "function",
                    "function": {"name": "submit_qq_guard_decision"},
                },
                extra_body={"thinking": {"type": "disabled"}},
            )
            message = response.choices[0].message
            raw_result = message.content or ""
            if message.tool_calls:
                matching_call = next(
                    (
                        tool_call
                        for tool_call in message.tool_calls
                        if tool_call.function.name == "submit_qq_guard_decision"
                    ),
                    None,
                )
                if matching_call is not None:
                    raw_result = matching_call.function.arguments
            return parse_qq_guard_response(raw_result, draft)
        except Exception as e:
            last_error = e
            logger.warning(f"QQ output guard attempt {attempt + 1} failed: {e}")
    assert last_error is not None
    raise last_error


async def _send_ai_forward_result(
    bot: Bot,
    group_id: str,
    text: str,
    *,
    prompt: str | None = None,
    prompt_user_id: str | None = None,
) -> bool:
    try:
        bot_uin = int(str(getattr(bot, "self_id", "0")) or "0")
        nodes = Message()
        if prompt:
            prompt_uin = int(prompt_user_id) if prompt_user_id and prompt_user_id.isdigit() else bot_uin
            nodes += MessageSegment.node_custom(prompt_uin, "用户提问", _render_at_segments(prompt))
        chunks = _split_forward_text(text)
        for index, chunk in enumerate(chunks, start=1):
            nickname = "CSBot AI" if len(chunks) == 1 else f"CSBot AI {index}/{len(chunks)}"
            nodes += MessageSegment.node_custom(bot_uin, nickname, _render_at_segments(chunk))
        await bot.call_api("send_group_forward_msg", group_id=int(group_id), messages=nodes)
        return True
    except Exception as e:
        logger.warning(f"send ai forward result failed: {e}")
        return False


def _chat_user_filters(values: Any) -> list[str]:
    if not isinstance(values, list):
        return []
    return [str(item) for item in values if QQ_ID_PATTERN.fullmatch(str(item))]


def _string_list(values: Any, limit: int = 20) -> list[str]:
    if not isinstance(values, list):
        return []
    result: list[str] = []
    for item in values:
        text = str(item).strip()
        if text:
            result.append(text)
        if len(result) >= limit:
            break
    return result


def _format_ai_message(msg: Message, image_ids: list[str] | None = None,
                       at_names: dict[str, str] | None = None) -> str:
    from ai_runtime.mentions import render_at
    image_ids = image_ids or []
    at_names = at_names or {}
    image_index = 0
    parts: list[str] = []
    for seg in msg:
        if seg.type == "text":
            parts.append(str(seg.data.get("text", "")))
        elif seg.type == "at":
            uid=str(seg.data.get("qq"))
            parts.append(render_at(uid,at_names.get(uid)))
        elif seg.type == "image":
            if image_index < len(image_ids):
                parts.append(f"[image:{image_ids[image_index]}]")
            else:
                parts.append("[image:unavailable]")
            image_index += 1
    return "".join(parts).strip()


def _image_mime_type(content: bytes) -> str:
    if content.startswith(b"\x89PNG\r\n\x1a\n"):
        return "image/png"
    if content.startswith(b"\xff\xd8\xff"):
        return "image/jpeg"
    if content.startswith((b"GIF87a", b"GIF89a")):
        return "image/gif"
    if content.startswith(b"RIFF") and content[8:12] == b"WEBP":
        return "image/webp"
    return "application/octet-stream"


def _image_data_url(content: bytes, mime_type: str) -> str:
    encoded = base64.b64encode(content).decode("ascii")
    return f"data:{mime_type};base64,{encoded}"


def _prepare_vision_images(content: bytes) -> tuple[list[tuple[int | None, str]], int]:
    mime_type = _image_mime_type(content)
    if mime_type == "application/octet-stream":
        raise ValueError("cached file is not a supported image")
    if mime_type != "image/gif":
        return [(None, _image_data_url(content, mime_type))], 1

    with Image.open(BytesIO(content)) as image:
        source_frame_count = max(1, int(getattr(image, "n_frames", 1)))
        if source_frame_count <= MAX_AI_GIF_FRAMES:
            frame_indices = list(range(source_frame_count))
        else:
            frame_indices = sorted(
                {
                    round(index * (source_frame_count - 1) / (MAX_AI_GIF_FRAMES - 1))
                    for index in range(MAX_AI_GIF_FRAMES)
                }
            )

        frames: list[tuple[int | None, str]] = []
        for frame_index in frame_indices:
            image.seek(frame_index)
            frame = image.convert("RGBA")
            frame.thumbnail(
                (MAX_AI_GIF_FRAME_EDGE, MAX_AI_GIF_FRAME_EDGE),
                Image.Resampling.LANCZOS,
            )
            output = BytesIO()
            frame.save(output, format="PNG")
            frames.append((frame_index, _image_data_url(output.getvalue(), "image/png")))
    return frames, source_frame_count


async def _load_cached_image(image_id: str) -> tuple[str, list[tuple[int | None, str]], int]:
    normalized_id = str(image_id or "").strip().lower()
    if not IMAGE_ID_PATTERN.fullmatch(normalized_id):
        raise ValueError("image_id must be a 16-64 character hexadecimal id")

    async with async_session_factory() as session:
        result = await session.execute(
            select(ImgCacheInfo.hash)
            .where(
                ImgCacheInfo.hash.like(f"{normalized_id}%"),
            )
            .limit(2)
        )
        hashes = list(result.scalars().all())
    if not hashes:
        raise ValueError("image not found in cache")
    if len(hashes) > 1:
        raise ValueError("image id is ambiguous; use more hash characters")

    full_hash = hashes[0]
    with media_cache().lease(full_hash) as image_path:
        content = image_path.read_bytes()
    if len(content) > MAX_AI_IMAGE_BYTES:
        raise ValueError("cached image is too large for vision input")
    vision_images, source_frame_count = _prepare_vision_images(content)
    return full_hash[:16], vision_images, source_frame_count


async def tavily_search(
    query: str,
    *,
    max_results: int = 5,
    search_depth: str = "basic",
    topic: str = "general",
    time_range: str | None = None,
    include_domains: list[str] | None = None,
    exclude_domains: list[str] | None = None,
) -> str:
    if not config.cs_tavily_api_key:
        return json.dumps({"error": "Tavily API key is not configured."}, ensure_ascii=False)

    query = (query or "").strip()
    if not query:
        return json.dumps({"error": "query is required."}, ensure_ascii=False)

    max_results = max(1, min(int(max_results or 5), 8))
    if search_depth not in {"basic", "advanced"}:
        search_depth = "basic"
    if topic not in {"general", "news", "finance"}:
        topic = "general"
    if time_range not in {"day", "week", "month", "year"}:
        time_range = None

    payload: dict[str, Any] = {
        "query": query,
        "max_results": max_results,
        "search_depth": search_depth,
        "topic": topic,
        "include_answer": True,
        "include_raw_content": False,
        "include_images": False,
        "include_image_descriptions": False,
    }
    if time_range:
        payload["time_range"] = time_range
    if include_domains:
        payload["include_domains"] = include_domains[:20]
    if exclude_domains:
        payload["exclude_domains"] = exclude_domains[:20]

    try:
        session = get_session()
        async with session.post(
            "https://api.tavily.com/search",
            json=payload,
            headers={
                "Authorization": f"Bearer {config.cs_tavily_api_key}",
                "Content-Type": "application/json",
            },
            timeout=aiohttp.ClientTimeout(total=20),
        ) as response:
            response_text = await response.text()
            if response.status >= 400:
                return json.dumps(
                    {"error": f"Tavily request failed with status {response.status}.", "detail": response_text[:500]},
                    ensure_ascii=False,
                )
            data = await response.json(content_type=None)
    except Exception as e:
        return json.dumps({"error": f"Tavily request failed: {e}"}, ensure_ascii=False)

    records = []
    for item in data.get("results", [])[:max_results]:
        content = str(item.get("content") or "")
        if len(content) > 700:
            content = content[:697] + "..."
        records.append(
            {
                "title": item.get("title"),
                "url": item.get("url"),
                "content": content,
                "score": item.get("score"),
                "published_date": item.get("published_date"),
            }
        )
    return json.dumps(
        {
            "query": data.get("query", query),
            "answer": data.get("answer"),
            "results": records,
            "response_time": data.get("response_time"),
        },
        ensure_ascii=False,
    )


class DataManager:

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
                .where(*await get_ladder_filter(steamid, time_type))
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
                .where(*await get_ladder_filter(steamid, time_type))
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

    async def _format_match_player_label(self, steamid: str) -> str:
        uid = await db_val.get_uid_by_steamid(steamid)
        base_info = await db_val.get_base_info(steamid)
        nickname = base_info.name if base_info is not None else "未知玩家"
        if uid is not None:
            return f"{nickname}([at:{uid}])"
        return f"{nickname}(路人 steamid={steamid})"

    def _format_match_time(self, timestamp: int) -> str:
        return datetime.fromtimestamp(int(timestamp)).strftime("%Y-%m-%d %H:%M:%S")

    def _parse_match_time(self, value: Any, *, is_end: bool = False) -> int | None:
        if value is None:
            return None
        text = str(value).strip()
        if not text:
            return None
        if text.isdigit():
            return int(text)
        try:
            if re.fullmatch(r"\d{4}-\d{2}-\d{2}", text):
                parsed = datetime.strptime(text, "%Y-%m-%d")
                if is_end:
                    parsed = parsed.replace(hour=23, minute=59, second=59)
                return int(parsed.timestamp())
            normalized = text.replace("T", " ")
            parsed = datetime.fromisoformat(normalized)
            return int(parsed.timestamp())
        except Exception:
            return None

    def _format_duration(self, duration: int | None) -> str:
        if not duration:
            return "未知"
        if int(duration) < 180:
            return f"{int(duration)}分"
        minutes, seconds = divmod(int(duration), 60)
        return f"{minutes}分{seconds:02d}秒"

    def _format_match_result(self, team: int, win_team: int) -> str:
        if win_team <= 0:
            return "未知"
        return "胜" if team == win_team else "负"

    def _format_signed(self, value: float | int | None, digits: int = 0) -> str:
        if value is None:
            return "未知"
        if digits <= 0:
            return f"{float(value):+.0f}"
        return f"{float(value):+.{digits}f}"

    def _format_optional_number(self, value: float | int | None, digits: int = 0) -> str:
        if value is None:
            return "未知"
        if digits <= 0:
            return f"{float(value):.0f}"
        return f"{float(value):.{digits}f}"

    def _is_ladder_mode(self, mode: str) -> bool:
        return mode.startswith("天梯") or mode == "PVP周末联赛"

    def _previous_season_id(self, season: str) -> str | None:
        match = re.fullmatch(r"S(\d+)", season)
        if not match:
            return None
        season_num = int(match.group(1))
        if season_num <= 1:
            return None
        return f"S{season_num - 1}"

    def _predict_reset_hidden_score(self, prev_end_score: int) -> float:
        if prev_end_score < 1600:
            return 0.504089 * prev_end_score + 594.34
        if prev_end_score < 2000:
            return 0.099434 * prev_end_score + 1351.14
        return 0.119768 * prev_end_score + 1584.41

    async def _get_display_pvp_score(self, player: MatchStatsPW) -> tuple[int | None, bool]:
        if player.pvpScore > 0:
            return int(player.pvpScore), False
        if not self._is_ladder_mode(player.mode):
            return None, False

        mode_filter = (MatchStatsPW.mode.like("天梯%")) | (MatchStatsPW.mode == "PVP周末联赛")
        async with async_session_factory() as session:
            season_stmt = (
                select(
                    MatchStatsPW.mid,
                    MatchStatsPW.pvpScore,
                    MatchStatsPW.pvpScoreChange,
                    MatchStatsPW.timeStamp,
                )
                .where(MatchStatsPW.steamid == player.steamid)
                .where(MatchStatsPW.seasonId == player.seasonId)
                .where(mode_filter)
                .order_by(MatchStatsPW.timeStamp.asc(), MatchStatsPW.mid.asc())
            )
            season_rows = list((await session.execute(season_stmt)).all())
            current_index = next(
                (
                    index for index, row in enumerate(season_rows)
                    if row.mid == player.mid and int(row.timeStamp) == int(player.timeStamp)
                ),
                None,
            )
            if current_index is None:
                return None, False

            visible_index = next(
                (index for index, row in enumerate(season_rows) if int(row.pvpScore or 0) > 0),
                None,
            )
            if visible_index is not None:
                hidden_start = int(season_rows[visible_index].pvpScore) - sum(
                    int(row.pvpScoreChange or 0) for row in season_rows[: visible_index + 1]
                )
            else:
                prev_season = self._previous_season_id(player.seasonId)
                if not prev_season:
                    return None, False
                prev_stmt = (
                    select(MatchStatsPW.pvpScore)
                    .where(MatchStatsPW.steamid == player.steamid)
                    .where(MatchStatsPW.seasonId == prev_season)
                    .where(mode_filter)
                    .where(MatchStatsPW.pvpScore > 0)
                    .order_by(MatchStatsPW.timeStamp.desc(), MatchStatsPW.mid.desc())
                    .limit(1)
                )
                prev_end_score = (await session.execute(prev_stmt)).scalar_one_or_none()
                if not prev_end_score:
                    return None, False
                hidden_start = self._predict_reset_hidden_score(int(prev_end_score))

            display_score = hidden_start + sum(
                int(row.pvpScoreChange or 0) for row in season_rows[: current_index + 1]
            )
            return max(1, int(round(display_score))), True

    async def _get_legacy_score(self, steamid: str, timestamp: int) -> float | None:
        extra_info = await db_val.get_extra_info(steamid, timeStamp=timestamp)
        return extra_info.legacyScore if extra_info else None

    async def _get_match_user_team(self, players: list[tuple[str, int]]) -> int | None:
        team1_users = False
        team2_users = False
        for steamid, team in players:
            is_user = await db_val.is_user(steamid)
            if is_user and team == 1:
                team1_users = True
            elif is_user and team == 2:
                team2_users = True
        if team1_users and not team2_users:
            return 1
        if team2_users and not team1_users:
            return 2
        return None

    async def _format_pw_history_item(self, record: MatchStatsPW) -> str:
        display_score, is_predicted = await self._get_display_pvp_score(record)
        display_score_text = "未知" if display_score is None else f"{display_score}{' 预测' if is_predicted else ''}"
        extra_info = await db_val.get_match_extra(record.mid)
        legacy_diff: float | None = None
        if extra_info:
            legacy_diff = extra_info.team1Legacy - extra_info.team2Legacy if record.team == 1 else extra_info.team2Legacy - extra_info.team1Legacy
        result = self._format_match_result(record.team, record.winTeam)
        return (
            f"{self._format_match_time(record.timeStamp)} mid={record.mid} 完美 {record.mode} {record.mapName} "
            f"{record.score1}:{record.score2} 队伍{record.team}{result}；"
            f"KDA {record.kill}/{record.death}/{record.assist} rating {record.pwRating:.2f} WE {record.we:.1f} "
            f"ADR {record.adpr:.0f} 分数 {display_score_text} 变化{self._format_signed(record.pvpScoreChange)} "
            f"底蕴差（己方-对方）{self._format_signed(legacy_diff)}"
        )

    async def _format_gp_history_item(self, record: Any) -> str:
        extra_info = await db_val.get_match_gp_extra(record.mid)
        legacy_diff: float | None = None
        if extra_info and extra_info.team1Legacy is not None and extra_info.team2Legacy is not None:
            legacy_diff = extra_info.team1Legacy - extra_info.team2Legacy if record.team == 1 else extra_info.team2Legacy - extra_info.team1Legacy
        result = self._format_match_result(record.team, record.winTeam)
        return (
            f"{self._format_match_time(record.timeStamp)} mid={record.mid} 官匹 {record.mode} {record.mapName} "
            f"{record.score1}:{record.score2} 队伍{record.team}{result}；"
            f"KDA {record.kill}/{record.death}/{record.assist} rating {record.rating:.2f} ADR {record.adpr:.0f} "
            f"官匹等级 {record.rank} 底蕴差（己方-对方）{self._format_signed(legacy_diff)}"
        )

    async def _get_pw_history_records(
        self,
        steamid: str,
        time_type: str,
        limit: int,
        start_ts: int | None,
        end_ts: int | None,
    ) -> tuple[list[MatchStatsPW], int, str]:
        if start_ts is None and end_ts is None:
            return (
                await db_val.get_matches(steamid, time_type, limit=limit) or [],
                await db_val.get_matches_count(steamid, time_type),
                time_type,
            )
        async with async_session_factory() as session:
            filters = [MatchStatsPW.steamid == steamid]
            if start_ts is not None:
                filters.append(MatchStatsPW.timeStamp >= start_ts)
            if end_ts is not None:
                filters.append(MatchStatsPW.timeStamp <= end_ts)
            records_stmt = (
                select(MatchStatsPW)
                .where(*filters)
                .order_by(MatchStatsPW.timeStamp.desc())
                .limit(limit)
            )
            count_stmt = select(func.count(MatchStatsPW.mid)).where(*filters)
            records = list((await session.execute(records_stmt)).scalars().all())
            count = int((await session.execute(count_stmt)).scalar_one())
        return records, count, self._format_time_range_label(start_ts, end_ts)

    async def _get_gp_history_records(
        self,
        steamid: str,
        time_type: str,
        limit: int,
        start_ts: int | None,
        end_ts: int | None,
    ) -> tuple[list[MatchStatsGP], int, str]:
        if start_ts is None and end_ts is None:
            gp_time_type = time_type if time_type in gp_time else "全部"
            return (
                await db_val.get_matches_gp(steamid, gp_time_type, limit=limit) or [],
                await db_val.get_matches_gp_count(steamid, gp_time_type),
                gp_time_type,
            )
        async with async_session_factory() as session:
            filters = [MatchStatsGP.steamid == steamid]
            if start_ts is not None:
                filters.append(MatchStatsGP.timeStamp >= start_ts)
            if end_ts is not None:
                filters.append(MatchStatsGP.timeStamp <= end_ts)
            records_stmt = (
                select(MatchStatsGP)
                .where(*filters)
                .order_by(MatchStatsGP.timeStamp.desc())
                .limit(limit)
            )
            count_stmt = select(func.count(MatchStatsGP.mid)).where(*filters)
            records = list((await session.execute(records_stmt)).scalars().all())
            count = int((await session.execute(count_stmt)).scalar_one())
        return records, count, self._format_time_range_label(start_ts, end_ts)

    def _format_time_range_label(self, start_ts: int | None, end_ts: int | None) -> str:
        if start_ts is None and end_ts is None:
            return "未指定"
        start_text = self._format_match_time(start_ts) if start_ts is not None else "不限开始"
        end_text = self._format_match_time(end_ts) if end_ts is not None else "不限结束"
        return f"{start_text} 至 {end_text}"

    async def get_player_match_history_prompt(
        self,
        steamid: str,
        time_type: str = "本赛季",
        match_type: str = "all",
        limit: int = 10,
        time_start: Any = None,
        time_end: Any = None,
    ) -> str:
        match_type = match_type if match_type in {"all", "perfect", "gp"} else "all"
        limit = max(1, min(int(limit or 10), 20))
        start_ts = self._parse_match_time(time_start)
        end_ts = self._parse_match_time(time_end, is_end=True)
        if time_start and start_ts is None:
            return f"time_start 无法解析：{time_start}。请使用 YYYY-MM-DD、YYYY-MM-DD HH:MM:SS 或 Unix 秒。"
        if time_end and end_ts is None:
            return f"time_end 无法解析：{time_end}。请使用 YYYY-MM-DD、YYYY-MM-DD HH:MM:SS 或 Unix 秒。"
        user_label = await self._format_user_label(steamid)
        range_text = self._format_time_range_label(start_ts, end_ts) if start_ts is not None or end_ts is not None else time_type
        sections: list[str] = [f"玩家 {user_label} 比赛记录查询：请求类型={match_type}，时间范围={range_text}，每类最多{limit}场。"]

        if match_type in {"all", "perfect"}:
            pw_records, pw_count, pw_range = await self._get_pw_history_records(steamid, time_type, limit, start_ts, end_ts)
            sections.append(f"完美记录：{pw_range} 共{pw_count}场，返回{len(pw_records)}场。")
            if pw_records:
                sections.extend([await self._format_pw_history_item(record) for record in pw_records])

        if match_type in {"all", "gp"}:
            gp_records, gp_count, gp_range = await self._get_gp_history_records(steamid, time_type, limit, start_ts, end_ts)
            suffix = "" if start_ts is not None or end_ts is not None or gp_range == time_type else f"（官匹不支持 {time_type}，实际使用 {gp_range}）"
            sections.append(f"官匹记录：{gp_range} 共{gp_count}场，返回{len(gp_records)}场。{suffix}")
            if gp_records:
                sections.extend([await self._format_gp_history_item(record) for record in gp_records])

        return "\n".join(sections)

    async def _format_pw_match_detail(self, match_id: str) -> str | None:
        match_detail = await db_val.get_match_detail(match_id)
        if not match_detail:
            return None
        match_extra = await db_val.get_match_extra(match_id)
        first = match_detail[0]
        user_team = await self._get_match_user_team([(player.steamid, player.team) for player in match_detail])
        lines = [
            f"完美比赛详情 mid={match_id}",
            f"时间 {self._format_match_time(first.timeStamp)}；赛季 {first.seasonId}；模式 {first.mode}；地图 {first.mapName}；比分 {first.score1}:{first.score2}；胜方 队伍{first.winTeam}；群友队伍 {user_team if user_team is not None else '无法唯一确定'}；时长 {self._format_duration(first.duration)}。",
            f"队伍底蕴：队伍1 {match_extra.team1Legacy:.0f}，队伍2 {match_extra.team2Legacy:.0f}。" if match_extra else "队伍底蕴：无额外数据。",
        ]
        for team in [1, 2]:
            lines.append(f"队伍{team}玩家：")
            team_players = sorted([player for player in match_detail if player.team == team], key=lambda p: p.pwRating, reverse=True)
            for player in team_players:
                label = await self._format_match_player_label(player.steamid)
                legacy_score = await self._get_legacy_score(player.steamid, player.timeStamp)
                display_score, is_predicted = await self._get_display_pvp_score(player)
                score_text = "未知" if display_score is None else f"{display_score}{' 预测' if is_predicted else ''}"
                lines.append(
                    f"- {label}：KDA {player.kill}/{player.death}/{player.assist}，rating {player.pwRating:.2f}，WE {player.we:.1f}，ADR {player.adpr:.0f}，"
                    f"首杀/首死 {player.entryKill}/{player.firstDeath}，爆头 {player.headShot}({player.headShotRatio:.0%})，狙杀 {player.snipeNum}，"
                    f"多杀 2K/3K/4K/5K={player.twoKill}/{player.threeKill}/{player.fourKill}/{player.fiveKill}，"
                    f"道具 {player.throwsCnt}，闪白敌/队友 {player.flashSuccess}/{player.flashTeammate}，"
                    f"天梯分 {score_text}，分数变化{self._format_signed(player.pvpScoreChange)}，星级 {player.pvpStars}，底蕴 {self._format_optional_number(legacy_score)}。"
                )
        return "\n".join(lines)

    async def _format_gp_match_detail(self, match_id: str) -> str | None:
        match_detail = await db_val.get_match_gp_detail(match_id)
        if not match_detail:
            return None
        match_extra = await db_val.get_match_gp_extra(match_id)
        first = match_detail[0]
        user_team = await self._get_match_user_team([(player.steamid, player.team) for player in match_detail])
        lines = [
            f"官匹比赛详情 mid={match_id}",
            f"时间 {self._format_match_time(first.timeStamp)}；模式 {first.mode}；地图 {first.mapName}；比分 {first.score1}:{first.score2}；胜方 队伍{first.winTeam}；群友队伍 {user_team if user_team is not None else '无法唯一确定'}；时长 {self._format_duration(first.duration)}。",
            f"队伍底蕴：队伍1 {match_extra.team1Legacy:.0f}，队伍2 {match_extra.team2Legacy:.0f}。" if match_extra and match_extra.team1Legacy is not None and match_extra.team2Legacy is not None else "队伍底蕴：无额外数据。",
        ]
        for team in [1, 2]:
            lines.append(f"队伍{team}玩家：")
            team_players = sorted([player for player in match_detail if player.team == team], key=lambda p: p.rating, reverse=True)
            for player in team_players:
                label = await self._format_match_player_label(player.steamid)
                legacy_score = await self._get_legacy_score(player.steamid, player.timeStamp)
                lines.append(
                    f"- {label}：KDA {player.kill}/{player.death}/{player.assist}，rating {player.rating:.2f}，ADR {player.adpr:.0f}，RWS {player.rws:.1f}，KAST {player.kast:.1f}，"
                    f"首杀/首死 {player.entryKill}/{player.entryDeath}，爆头 {player.headShot}，AWP {player.awpKill}，"
                    f"多杀 2K/3K/4K/5K={player.twoKill}/{player.threeKill}/{player.fourKill}/{player.fiveKill}，"
                    f"道具 {player.throwsCnt}，闪光 {player.flash}，闪白敌/队友 {player.flashSuccess}/{player.flashTeammate}，"
                    f"下包/拆包 {player.bombPlanted}/{player.bombDefused}，雷伤/火伤 {player.grenadeDamage}/{player.infernoDamage}，"
                    f"官匹等级 {player.rank}，底蕴 {self._format_optional_number(legacy_score)}。"
                )
        return "\n".join(lines)

    async def get_match_detail_prompt(self, match_id: str, match_type: str = "auto") -> str:
        match_type = match_type if match_type in {"auto", "perfect", "gp"} else "auto"
        results: list[str] = []
        if match_type in {"auto", "perfect"}:
            pw_detail = await self._format_pw_match_detail(match_id)
            if pw_detail:
                results.append(pw_detail)
        if match_type in {"auto", "gp"}:
            gp_detail = await self._format_gp_match_detail(match_id)
            if gp_detail:
                results.append(gp_detail)
        if not results:
            return f"未找到比赛 mid={match_id}。"
        return "\n\n".join(results)
    
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

async def _human_group(bot: Bot, event: GroupMessageEvent) -> bool:
    return str(event.user_id) != str(bot.self_id)


aiask = on_command("ai", rule=_human_group, priority=10, block=True)

aiasktb = on_command("aitb", rule=_human_group, priority=10, block=True)

aiaskxmm = on_command("aixmm", rule=_human_group, priority=10, block=True)

aiaskxhs = on_command("aixhs", rule=_human_group, priority=10, block=True)

aiasktmr = on_command("aitmr", rule=_human_group, priority=10, block=True)



async def _explicitly_called(bot: Bot, event: GroupMessageEvent) -> bool:
    if str(event.user_id)==str(bot.self_id): return False
    if any(seg.type=="at" and str(seg.data.get("qq"))==str(bot.self_id) for seg in event.original_message):
        return True
    return bool(event.reply and str(event.reply.sender.user_id)==str(bot.self_id))


ai_called = on_message(rule=_explicitly_called,priority=11,block=True)


@ai_called.handle()
async def ai_called_function(bot: Bot, event: GroupMessageEvent):
    from ..allmsg.outgoing import current_run
    chat_id=str(uuid.uuid4())
    current_run.set(chat_id)
    question=Message([seg for seg in event.message if not (seg.type=="at" and str(seg.data.get("qq"))==str(bot.self_id))])
    response=await ai_ask2(bot,str(event.user_id),event.get_session_id(),None,question,
                           event.original_message,chat_id,event.message_id,notify=ai_called.send)
    if response is not None: await ai_called.finish(response)
    await ai_called.finish()


async def ai_ask_main(uid: str, sid: str, persona: str | None, text: str, chat_id: str | None,
                      *, channel: str | None = None, conversation: str = "default") -> str:
    from ai_runtime.service import ask
    channel = channel or ("qq" if uid else "report")
    chat_id = chat_id or str(uuid.uuid4())
    from ..allmsg.outgoing import current_run
    current_run.set(chat_id)
    if config.cs_ai_engine != "dsh":
        raise ValueError("旧 AI 引擎已停用；CS_AI_ENGINE 必须为 dsh")
    model_name = await runtime_config.get("cs_ai_model")
    async def guard(output):
        if channel == "web": return output
        if config.cs_ai_output_guard_enabled:
            try:
                async with AsyncOpenAI(api_key=config.cs_ai_api_key,base_url=config.cs_ai_url) as client:
                    verdict = await _guard_qq_output(client, output)
                return normalize_qq_plain_text(verdict.text)
            except Exception:
                logger.warning("QQ output guard failed closed")
                return QQ_GUARD_FAILED_TEXT
        return normalize_qq_plain_text(output)
    return await ask(chat_id=chat_id,gid=group_id_from_sid(sid),uid=uid,prompt=text,persona=persona,
        channel=channel,conversation=chat_id if channel=="report" else conversation,
        model=model_name,endpoint=config.cs_ai_url,api_key=config.cs_ai_api_key,
        factory=async_session_factory,chat_history=chat_history_db,archive=db.insert_chat_record,
        guard=guard,vision=config.cs_ai_vision,thinking=config.cs_ai_enable_thinking)


def _ai_chat_link(chat_id: str) -> str:
    from ai_runtime.store import state_store
    target=state_store().short_run_id(chat_id) or chat_id
    return config.cs_domain+f'/ai-chat?chatId={target}'


async def ai_ask2(
    bot: Bot,
    uid: str,
    sid: str,
    persona: str | None,
    msg: Message,
    orimsg: Message,
    chat_id: str | None = None,
    current_mid: int | None = None,
    notify=None,
) -> Message | None:
    chat_id=chat_id or str(uuid.uuid4())
    group_id = group_id_from_sid(sid)
    current_record=(await chat_history_db.get_message_reference_by_mid(group_id,current_mid)
                    if current_mid is not None else None)
    image_ids=re.findall(r"\[image:([0-9a-f]{16})\]",current_record["text"]) if current_record else []
    text = _format_ai_message(msg, image_ids,
                              current_record.get("mentioned_names",{}) if current_record else {})
    is_supplement=text.startswith(('补充：','补充:'))
    msg2id: int | None = None
    try:
        if orimsg[0].type == "reply":
            msg2id = int(orimsg[0].data["id"])
            reference = await chat_history_db.get_message_reference_by_mid(group_id, msg2id)
            if reference is None:
                raise ValueError(f"reply target mid={msg2id} is not recorded")
            if text:
                text = f"[reply:{reference['message_id']}] {text}"
            else:
                text = f"请回应我引用的消息：[reply:{reference['message_id']}] {reference['text']}"
    except Exception as e:
        logger.warning(f"获取回复消息失败: {e}")
        return Message("获取回复消息失败。")
    logger.info(f"UID: {uid}, Text: {text}")

    if is_supplement and config.cs_ai_engine=='dsh':
        from ai_runtime.store import state_store
        from ai_runtime.runner import steer_run
        from ai_runtime.events import publish
        active=state_store().rows("SELECT id FROM runs WHERE channel='qq' AND group_id=? AND user_id=? AND status='running' ORDER BY created_at DESC LIMIT 1",(group_id,uid))
        if active:
            target=active[0]['id']
            try:accepted=await steer_run(target,f'QQ用户 {uid} 补充当前问题：{text}')
            except ValueError as exc:
                publish(target,{'type':'supplement','text':text,'delivery':'unknown'})
                return MessageSegment.at(uid)+' '+str(exc)+' '+_ai_chat_link(target)
            if accepted:
                publish(target,{'type':'supplement','text':text,'delivery':'accepted'})
                from ..allmsg.outgoing import current_run
                current_run.set(target)
                return MessageSegment.at(uid)+' 补充收到了，会接着一起看。'+_ai_chat_link(target)

    if notify:
        if config.cs_ai_engine=='dsh':
            from ai_runtime.service import register
            try:register(chat_id,group_id,uid,text)
            except ValueError as exc:return MessageSegment.at(uid)+' '+str(exc)
        try:
            await notify(MessageSegment.at(uid)+' 我看看，记录在这里：'+_ai_chat_link(chat_id))
        except Exception:
            logger.warning('AI progress notification failed; continuing the registered request')

    try:
        ai_text = await ai_ask_main(uid, sid, persona, text, chat_id=chat_id)
    except Exception as exc:
        logger.error(f"AI request did not complete: {type(exc).__name__}")
        if config.cs_ai_engine=='dsh':
            from ai_runtime.store import state_store
            from ai_runtime.events import publish
            if state_store().execute("UPDATE runs SET status='interrupted',error=? WHERE id=? AND status IN ('queued','running')",(type(exc).__name__,chat_id)):
                publish(chat_id,{'type':'status','status':'interrupted'})
        return MessageSegment.at(uid) + " 这次没能完成，记录已保留，可以稍后重试。"
    from ai_runtime.store import state_store
    generated=state_store().rows('SELECT digest FROM run_images WHERE run_id=?',(chat_id,))
    if _should_forward_ai_result(ai_text) and not generated:
        sent_forward = False
        try:
            sent_forward = await _send_ai_forward_result(
                bot,
                group_id_from_sid(sid),
                ai_text,
                prompt=text,
                prompt_user_id=uid,
            )
        except Exception as e:
            logger.warning(f"prepare ai forward result failed: {e}")
        if sent_forward:
            return None

    ai_message = _render_at_segments(ai_text)
    if generated:
        from ai_runtime.media import media_cache
        for image in generated:
            try:
                with media_cache().lease(image['digest']) as image_path:
                    ai_message+=image_segment(image_path.read_bytes())
            except FileNotFoundError:
                ai_message+='\n生成的原图已淘汰，未发送低清替代图；可让我重新生成。'

    if msg2id is not None:
        return MessageSegment.reply(msg2id) + MessageSegment.at(uid) + " " + ai_message
    else:
        return MessageSegment.at(uid) + " " + ai_message

@aiask.handle()
async def aiask_function(bot: Bot, message: GroupMessageEvent, args: Message = CommandArg()):
    uid = message.get_user_id()
    sid = message.get_session_id()
    chat_id = str(uuid.uuid4())
    from ..allmsg.outgoing import current_run
    current_run.set(chat_id)
    response = await ai_ask2(bot, uid, sid, None, args, message.original_message, chat_id=chat_id, current_mid=message.message_id,notify=aiask.send)
    if response is None:
        await aiask.finish()
    await aiask.finish(response)

@aiasktb.handle()
async def aiasktb_function(bot: Bot, message: GroupMessageEvent, args: Message = CommandArg()):
    uid = message.get_user_id()
    sid = message.get_session_id()
    chat_id = str(uuid.uuid4())
    from ..allmsg.outgoing import current_run
    current_run.set(chat_id)
    response = await ai_ask2(bot, uid, sid, "贴吧", args, message.original_message, chat_id=chat_id, current_mid=message.message_id,notify=aiasktb.send)
    if response is None:
        await aiasktb.finish()
    await aiasktb.finish(response)

@aiaskxmm.handle()
async def aiaskxmm_function(bot: Bot, message: GroupMessageEvent, args: Message = CommandArg()):
    uid = message.get_user_id()
    sid = message.get_session_id()
    chat_id = str(uuid.uuid4())
    from ..allmsg.outgoing import current_run
    current_run.set(chat_id)
    response = await ai_ask2(bot, uid, sid, "xmm", args, message.original_message, chat_id=chat_id, current_mid=message.message_id,notify=aiaskxmm.send)
    if response is None:
        await aiaskxmm.finish()
    await aiaskxmm.finish(response)

@aiaskxhs.handle()
async def aiaskxhs_function(bot: Bot, message: GroupMessageEvent, args: Message = CommandArg()):
    uid = message.get_user_id()
    sid = message.get_session_id()
    chat_id = str(uuid.uuid4())
    from ..allmsg.outgoing import current_run
    current_run.set(chat_id)
    response = await ai_ask2(bot, uid, sid, "xhs", args, message.original_message, chat_id=chat_id, current_mid=message.message_id,notify=aiaskxhs.send)
    if response is None:
        await aiaskxhs.finish()
    await aiaskxhs.finish(response)

@aiasktmr.handle()
async def aiasktmr_function(bot: Bot, message: GroupMessageEvent, args: Message = CommandArg()):
    uid = message.get_user_id()
    sid = message.get_session_id()
    chat_id = str(uuid.uuid4())
    from ..allmsg.outgoing import current_run
    current_run.set(chat_id)
    response = await ai_ask2(bot, uid, sid, "tmr", args, message.original_message, chat_id=chat_id, current_mid=message.message_id,notify=aiasktmr.send)
    if response is None:
        await aiasktmr.finish()
    await aiasktmr.finish(response)
