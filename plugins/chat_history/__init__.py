from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, time as datetime_time
import asyncio
import json
import math
import re
from typing import Any, Callable, Iterable

from nonebot import get_driver, get_plugin_config, logger, require
from nonebot.plugin import PluginMetadata
from sqlalchemy import delete, func, or_, select, update
from sqlalchemy.orm import aliased

require("utils")
from ..utils import async_session_factory

require("models")
from ..models import (
    ChatChunkIndex,
    ChatChunkMessage,
    ChatMessageIndex,
    ChatReplyEdge,
    ChatRetrievalSpan,
    ChatTokenLexicon,
    GroupMsg,
)

from .config import Config

__plugin_meta__ = PluginMetadata(
    name="chat_history",
    description="Indexed group chat retrieval for AI tools.",
    usage="",
    config=Config,
)

config = get_plugin_config(Config)

MAX_CHUNK_MESSAGES = 20
MIN_CHUNK_MESSAGES = 3
MAX_CHUNK_CHARS = 2500
CHUNK_GAP_SECONDS = 600
MAX_SPAN_MESSAGES = 30
MIN_SPAN_MESSAGES = 8
MAX_SPAN_CHARS = 3000
LIVE_REBUILD_SECONDS = 7200
STARTUP_REPAIR_SECONDS = 86400
STARTUP_REPAIR_LIMIT = 5000
BM25_CANDIDATE_LIMIT = 50000
BM25_K1 = 1.5
BM25_B = 0.75
LEXICON_MAX_TERMS = 2000
LEXICON_MIN_FREQ = 3
LEXICON_MIN_DOC_FREQ = 2

TOKEN_RE = re.compile(r"[A-Za-z0-9_\u4e00-\u9fff]+")
CJK_RUN_RE = re.compile(r"[\u4e00-\u9fff]{2,}")
ALNUM_RUN_RE = re.compile(r"[A-Za-z][A-Za-z0-9_]{1,31}")
SPACE_RE = re.compile(r"\s+")
MESSAGE_HEADER_RE = re.compile(
    r"^(?:anchor\s+)?\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}\s+qq=\S+\s+mid=\d+:\s*"
)
KEYWORD_STOP_WORDS = {
    "qq",
    "mid",
    "anchor",
    "reply",
    "anchors",
    "at",
    "image",
    "face",
    "什么",
    "时候",
    "怎么",
    "为什么",
    "哪个",
    "哪里",
    "有没有",
    "是不是",
    "有没",
    "没有",
    "是不",
    "不是",
    "多少",
    "几号",
    "几点",
    "如何",
    "是否",
    "这个",
    "那个",
    "一下",
    "一个",
    "了吗",
    "了么",
}
ProgressCallback = Callable[[str, int, int | None], None]


@dataclass(frozen=True)
class ParsedMessage:
    plain_text: str
    normalized_text: str
    mentioned_uids: list[str]
    reply_to_record_id: int | None
    reply_to_mid: int | None
    has_image: bool
    image_summaries: list[str]
    segment_types: list[str]


def _json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def _json_loads_list(value: str | None) -> list[Any]:
    if not value:
        return []
    try:
        result = json.loads(value)
        return result if isinstance(result, list) else []
    except Exception:
        return []


def group_id_from_sid(sid: str) -> str:
    parts = sid.split("_")
    if len(parts) < 3 or parts[0] != "group":
        raise ValueError(f"invalid group session id: {sid}")
    return parts[1]


def user_id_from_sid(sid: str) -> str:
    parts = sid.split("_")
    if len(parts) < 3 or parts[0] != "group":
        raise ValueError(f"invalid group session id: {sid}")
    return parts[2]


def normalize_text(text: str) -> str:
    text = text.replace("\x00", " ")
    text = text.lower()
    text = SPACE_RE.sub(" ", text)
    return " ".join(TOKEN_RE.findall(text))


def _bm25_tokens(text: str, lexicon_terms: Iterable[str] | None = None) -> list[str]:
    text = _keyword_source_text(text)
    normalized = normalize_text(text)
    if not normalized:
        return []
    tokens: list[str] = []
    try:
        import jieba

        rough_tokens = jieba.lcut(normalized)
    except Exception:
        rough_tokens = TOKEN_RE.findall(normalized)
    for token in rough_tokens:
        token = token.strip().lower()
        if not token:
            continue
        for part in TOKEN_RE.findall(token):
            if not _is_noise_keyword(part):
                tokens.append(part)
    seen_tokens = set(tokens)
    for run in CJK_RUN_RE.findall(normalized):
        for idx in range(len(run) - 1):
            gram = run[idx : idx + 2]
            if gram not in seen_tokens and not _is_noise_keyword(gram):
                tokens.append(gram)
                seen_tokens.add(gram)
    if lexicon_terms:
        for term in lexicon_terms:
            term = normalize_text(str(term)).replace(" ", "")
            if not term or term in seen_tokens or _is_noise_keyword(term):
                continue
            if term in normalized.replace(" ", ""):
                tokens.append(term)
                seen_tokens.add(term)
    return tokens


def _bm25_rank(query_terms: list[str], documents: list[list[str]]) -> list[float]:
    if not query_terms or not documents:
        return [0.0 for _ in documents]
    doc_count = len(documents)
    avg_len = sum(len(doc) for doc in documents) / doc_count if doc_count else 0.0
    if avg_len <= 0:
        return [0.0 for _ in documents]

    doc_freq: Counter[str] = Counter()
    query_terms = list(dict.fromkeys(query_terms))
    query_set = set(query_terms)
    for doc in documents:
        doc_freq.update(set(doc) & query_set)

    idf = {
        term: math.log(1 + (doc_count - doc_freq.get(term, 0) + 0.5) / (doc_freq.get(term, 0) + 0.5))
        for term in query_terms
    }

    scores: list[float] = []
    for doc in documents:
        term_freq = Counter(doc)
        doc_len = len(doc)
        score = 0.0
        for term in query_terms:
            freq = term_freq.get(term, 0)
            if freq <= 0:
                continue
            denominator = freq + BM25_K1 * (1 - BM25_B + BM25_B * doc_len / avg_len)
            score += idf[term] * (freq * (BM25_K1 + 1)) / denominator
        scores.append(score)
    return scores


def _keyword_source_text(text: str) -> str:
    lines: list[str] = []
    for line in text.splitlines():
        line = line.strip()
        if not line or line == "reply anchors:":
            continue
        lines.append(MESSAGE_HEADER_RE.sub("", line))
    return "\n".join(lines)


def _is_noise_keyword(word: str) -> bool:
    normalized = word.strip().lower()
    if len(normalized) < 2 or normalized in KEYWORD_STOP_WORDS:
        return True
    if normalized.isdigit():
        return True
    if re.fullmatch(r"\d{1,4}[-_/]\d{1,2}([-_/]\d{1,2})?", normalized):
        return True
    if re.fullmatch(r"\d{1,2}:\d{1,2}(:\d{1,2})?", normalized):
        return True
    return False


def _is_good_lexicon_term(term: str) -> bool:
    term = normalize_text(term).replace(" ", "")
    if _is_noise_keyword(term):
        return False
    if len(term) > 32:
        return False
    if re.fullmatch(r"[a-z]+", term) and len(term) < 3:
        return False
    if re.fullmatch(r"[\u4e00-\u9fff]+", term):
        if len(term) < 2 or len(term) > 6:
            return False
        if term[0] in {"的", "了", "是", "不", "有", "没", "在", "和", "跟", "就"}:
            return False
        if term[-1] in {"的", "了", "吗", "么", "啊", "吧", "呢"}:
            return False
    return True


def _lexicon_candidates_from_text(text: str) -> set[str]:
    source = _keyword_source_text(text)
    normalized = normalize_text(source)
    compact = normalized.replace(" ", "")
    candidates: set[str] = set()
    for term in ALNUM_RUN_RE.findall(normalized):
        term = term.lower()
        if _is_good_lexicon_term(term):
            candidates.add(term)
    for run in CJK_RUN_RE.findall(compact):
        max_n = min(5, len(run))
        for n in range(2, max_n + 1):
            for idx in range(len(run) - n + 1):
                term = run[idx : idx + n]
                if _is_good_lexicon_term(term):
                    candidates.add(term)
    return candidates


def _score_lexicon_term(term: str, freq: int, doc_freq: int) -> float:
    length_bonus = min(len(term), 6) / 2
    return float(freq) * (1.0 + math.log1p(doc_freq)) * length_bonus


def extract_keywords(text: str, limit: int = 12) -> list[str]:
    text = _keyword_source_text(text)
    try:
        import jieba.analyse

        words = [word.strip() for word in jieba.analyse.extract_tags(text, topK=limit * 2)]
    except Exception:
        words = TOKEN_RE.findall(normalize_text(text))
    filtered: list[str] = []
    seen: set[str] = set()
    for word in words:
        key = word.lower()
        if _is_noise_keyword(word) or key in seen:
            continue
        seen.add(key)
        filtered.append(word)
        if len(filtered) >= limit:
            break
    return filtered


def parse_message_segments(segments: Iterable[Any]) -> ParsedMessage:
    text_parts: list[str] = []
    mentioned_uids: list[str] = []
    image_summaries: list[str] = []
    segment_types: list[str] = []
    reply_to_record_id: int | None = None
    reply_to_mid: int | None = None
    has_image = False

    for raw_seg in segments:
        if not isinstance(raw_seg, (list, tuple)) or not raw_seg:
            continue
        seg_type = str(raw_seg[0])
        segment_types.append(seg_type)
        if seg_type == "text" and len(raw_seg) >= 2:
            text_parts.append(str(raw_seg[1]).replace("\x00", " "))
        elif seg_type == "at" and len(raw_seg) >= 2:
            uid = str(raw_seg[1])
            mentioned_uids.append(uid)
            text_parts.append(f"[at:{uid}]")
        elif seg_type == "reply" and len(raw_seg) >= 2:
            try:
                reply_to_record_id = int(raw_seg[1])
            except Exception:
                reply_to_record_id = None
            if len(raw_seg) >= 3:
                try:
                    reply_to_mid = int(raw_seg[2])
                except Exception:
                    reply_to_mid = None
        elif seg_type == "imagev2":
            has_image = True
            summary = str(raw_seg[3]).replace("\x00", " ") if len(raw_seg) >= 4 and raw_seg[3] else ""
            if summary:
                image_summaries.append(summary)
                text_parts.append(f"[image:{summary}]")
            else:
                text_parts.append("[image]")
        elif seg_type == "face" and len(raw_seg) >= 2:
            text_parts.append(f"[face:{raw_seg[1]}]")
        else:
            text_parts.append(f"[{seg_type}]")

    plain_text = SPACE_RE.sub(" ", "".join(text_parts)).strip()
    return ParsedMessage(
        plain_text=plain_text,
        normalized_text=normalize_text(plain_text),
        mentioned_uids=mentioned_uids,
        reply_to_record_id=reply_to_record_id if reply_to_record_id and reply_to_record_id > 0 else None,
        reply_to_mid=reply_to_mid,
        has_image=has_image,
        image_summaries=image_summaries,
        segment_types=segment_types,
    )


def _format_message(row: ChatMessageIndex, prefix: str | None = None) -> str:
    dt = datetime.fromtimestamp(row.timestamp).strftime("%Y-%m-%d %H:%M:%S")
    head = f"{dt} qq={row.user_id} mid={row.record_id}"
    if prefix:
        head = f"{prefix} {head}"
    body = row.plain_text or ""
    return f"{head}: {body}"


def _short_text(text: str, limit: int = 220) -> str:
    text = SPACE_RE.sub(" ", text).strip()
    if len(text) <= limit:
        return text
    return text[: limit - 1] + "..."


def _message_lines(rows: Iterable[ChatMessageIndex], prefix: str | None = None) -> list[str]:
    return [_format_message(row, prefix=prefix) for row in rows if row.plain_text or row.has_image]


def _parse_time_bound(value: int | float | str | None, *, is_end: bool) -> int | None:
    if value is None or value == "":
        return None
    if isinstance(value, (int, float)):
        return int(value)

    text = str(value).strip()
    if not text:
        return None
    if text.isdigit():
        return int(text)

    normalized = text.replace("T", " ")
    formats = [
        ("%Y-%m-%d %H:%M:%S", False),
        ("%Y-%m-%d %H:%M", False),
        ("%Y-%m-%d", True),
    ]
    for fmt, date_only in formats:
        try:
            parsed = datetime.strptime(normalized, fmt)
            if date_only:
                parsed = datetime.combine(
                    parsed.date(),
                    datetime_time.max if is_end else datetime_time.min,
                )
            return int(parsed.timestamp())
        except ValueError:
            continue
    raise ValueError(f"invalid time format: {value}; use YYYY-MM-DD or YYYY-MM-DD HH:MM[:SS]")


def _participants(rows: Iterable[ChatMessageIndex]) -> list[str]:
    return sorted({row.user_id for row in rows})


def _chunk_summary(text: str) -> str:
    return _short_text(text.replace("\n", " "), 360)


def _split_conversation_segments(rows: list[ChatMessageIndex]) -> list[list[ChatMessageIndex]]:
    if not rows:
        return []
    segments: list[list[ChatMessageIndex]] = []
    current: list[ChatMessageIndex] = [rows[0]]
    for row in rows[1:]:
        if row.timestamp - current[-1].timestamp > CHUNK_GAP_SECONDS:
            segments.append(current)
            current = [row]
        else:
            current.append(row)
    segments.append(current)
    return segments


def _build_core_groups(rows: list[ChatMessageIndex]) -> list[list[ChatMessageIndex]]:
    groups: list[list[ChatMessageIndex]] = []
    for segment in _split_conversation_segments(rows):
        current: list[ChatMessageIndex] = []
        chars = 0
        for row in segment:
            row_len = len(row.plain_text or "")
            should_cut = (
                current
                and len(current) >= MIN_CHUNK_MESSAGES
                and (len(current) >= MAX_CHUNK_MESSAGES or chars + row_len > MAX_CHUNK_CHARS)
            )
            if should_cut:
                groups.append(current)
                current = []
                chars = 0
            current.append(row)
            chars += row_len
        if current:
            groups.append(current)
    return groups


def _build_span_groups(rows: list[ChatMessageIndex]) -> list[list[ChatMessageIndex]]:
    spans: list[list[ChatMessageIndex]] = []
    for segment in _split_conversation_segments(rows):
        if len(segment) <= MAX_SPAN_MESSAGES:
            if segment:
                spans.append(segment)
            continue
        start = 0
        while start < len(segment):
            current: list[ChatMessageIndex] = []
            chars = 0
            idx = start
            while idx < len(segment) and len(current) < MAX_SPAN_MESSAGES:
                row = segment[idx]
                row_len = len(row.plain_text or "")
                if current and len(current) >= MIN_SPAN_MESSAGES and chars + row_len > MAX_SPAN_CHARS:
                    break
                current.append(row)
                chars += row_len
                idx += 1
            if current:
                spans.append(current)
            if idx >= len(segment):
                break
            start += max(1, len(current) // 2)
    return spans


class DataManager:
    def __init__(self) -> None:
        self._lexicon_cache: dict[str, set[str]] = {}

    def _index_row_from_group_msg(self, msg: GroupMsg) -> ChatMessageIndex | None:
        try:
            group_id = group_id_from_sid(msg.sid)
            user_id = user_id_from_sid(msg.sid)
            import msgpack

            parsed = parse_message_segments(msgpack.loads(msg.data))
        except Exception as e:
            logger.warning(f"parse group message {msg.id} failed: {e}")
            return None
        return ChatMessageIndex(
            record_id=msg.id,
            mid=msg.mid,
            group_id=group_id,
            user_id=user_id,
            timestamp=msg.timestamp,
            plain_text=parsed.plain_text,
            normalized_text=parsed.normalized_text,
            mentioned_uids=_json_dumps(parsed.mentioned_uids),
            reply_to_record_id=parsed.reply_to_record_id,
            reply_to_mid=parsed.reply_to_mid,
            has_image=parsed.has_image,
            image_summaries=_json_dumps(parsed.image_summaries),
            segment_types=_json_dumps(parsed.segment_types),
            primary_chunk_id=None,
        )

    async def index_group_message(self, record_id: int, rebuild_tail: bool = True) -> None:
        group_id: str | None = None
        timestamp = 0
        async with async_session_factory() as session:
            async with session.begin():
                msg = await session.get(GroupMsg, record_id)
                if msg is None:
                    return
                row = self._index_row_from_group_msg(msg)
                if row is None:
                    return
                group_id = row.group_id
                timestamp = row.timestamp
                await session.merge(row)

        await self.refresh_reply_edge(record_id)
        if rebuild_tail and group_id is not None:
            await self.update_group_lexicon(group_id, start_time=timestamp, replace=False)
            await self.rebuild_group_indexes(group_id, start_time=max(0, timestamp - LIVE_REBUILD_SECONDS))

    async def refresh_reply_edge(self, record_id: int) -> None:
        async with async_session_factory() as session:
            async with session.begin():
                row = await session.get(ChatMessageIndex, record_id)
                if row is None or row.reply_to_record_id is None:
                    return
                target = await session.get(ChatMessageIndex, row.reply_to_record_id)
                if target is None or target.group_id != row.group_id:
                    return
                edge = ChatReplyEdge(
                    from_message_id=row.record_id,
                    to_message_id=target.record_id,
                    group_id=row.group_id,
                    from_time=row.timestamp,
                    to_time=target.timestamp,
                )
                await session.merge(edge)

    async def repair_recent_indexes(
        self,
        lookback_seconds: int = STARTUP_REPAIR_SECONDS,
        limit: int = STARTUP_REPAIR_LIMIT,
    ) -> int:
        cutoff = int(datetime.now().timestamp()) - max(0, int(lookback_seconds))
        limit = max(100, int(limit))
        affected_groups: dict[str, int] = {}
        repaired_ids: list[int] = []

        async with async_session_factory() as session:
            result = await session.execute(
                select(GroupMsg)
                .outerjoin(ChatMessageIndex, ChatMessageIndex.record_id == GroupMsg.id)
                .where(
                    GroupMsg.timestamp >= cutoff,
                    or_(
                        ChatMessageIndex.record_id.is_(None),
                        ChatMessageIndex.primary_chunk_id.is_(None),
                    ),
                )
                .order_by(GroupMsg.timestamp.asc(), GroupMsg.id.asc())
                .limit(limit)
            )
            messages = list(result.scalars().all())

        if not messages:
            return 0

        async with async_session_factory() as session:
            async with session.begin():
                for msg in messages:
                    row = self._index_row_from_group_msg(msg)
                    if row is None:
                        continue
                    await session.merge(row)
                    repaired_ids.append(row.record_id)
                    current_min = affected_groups.get(row.group_id)
                    if current_min is None or row.timestamp < current_min:
                        affected_groups[row.group_id] = row.timestamp

        for record_id in repaired_ids:
            await self.refresh_reply_edge(record_id)

        for group_id, min_timestamp in affected_groups.items():
            await self.rebuild_group_indexes(
                group_id,
                start_time=max(0, min_timestamp - LIVE_REBUILD_SECONDS),
            )
        return len(repaired_ids)

    async def rebuild_all_message_indexes(
        self,
        batch_size: int = 1000,
        progress_callback: ProgressCallback | None = None,
    ) -> int:
        batch_size = max(100, int(batch_size))
        count = 0
        last_id = 0
        async with async_session_factory() as session:
            total = await session.scalar(select(func.count()).select_from(GroupMsg)) or 0
        if progress_callback:
            progress_callback("messages", 0, total)
        while True:
            async with async_session_factory() as session:
                async with session.begin():
                    result = await session.execute(
                        select(GroupMsg)
                        .where(GroupMsg.id > last_id)
                        .order_by(GroupMsg.id.asc())
                        .limit(batch_size)
                    )
                    messages = list(result.scalars().all())
                    if not messages:
                        break
                    for msg in messages:
                        row = self._index_row_from_group_msg(msg)
                        if row is not None:
                            await session.merge(row)
                    last_id = messages[-1].id
                    count += len(messages)
                    if progress_callback:
                        progress_callback("messages", count, total)
        await self.rebuild_all_reply_edges(batch_size=batch_size, progress_callback=progress_callback)
        return count

    async def rebuild_all_reply_edges(
        self,
        batch_size: int = 1000,
        progress_callback: ProgressCallback | None = None,
    ) -> int:
        batch_size = max(100, int(batch_size))
        src = aliased(ChatMessageIndex)
        dst = aliased(ChatMessageIndex)
        async with async_session_factory() as session:
            total = await session.scalar(
                select(func.count())
                .select_from(src)
                .join(dst, src.reply_to_record_id == dst.record_id)
                .where(src.reply_to_record_id.is_not(None), src.group_id == dst.group_id)
            ) or 0
        if progress_callback:
            progress_callback("reply_edges", 0, total)
        async with async_session_factory() as session:
            async with session.begin():
                await session.execute(delete(ChatReplyEdge))

        count = 0
        last_id = 0
        while True:
            async with async_session_factory() as session:
                async with session.begin():
                    result = await session.execute(
                        select(
                            src.record_id,
                            dst.record_id,
                            src.group_id,
                            src.timestamp,
                            dst.timestamp,
                        )
                        .join(dst, src.reply_to_record_id == dst.record_id)
                        .where(
                            src.record_id > last_id,
                            src.reply_to_record_id.is_not(None),
                            src.group_id == dst.group_id,
                        )
                        .order_by(src.record_id.asc())
                        .limit(batch_size)
                    )
                    rows = list(result.all())
                    if not rows:
                        break
                    for from_id, to_id, group_id, from_time, to_time in rows:
                        await session.merge(ChatReplyEdge(
                            from_message_id=from_id,
                            to_message_id=to_id,
                            group_id=group_id,
                            from_time=from_time,
                            to_time=to_time,
                        ))
                    last_id = rows[-1][0]
                    count += len(rows)
                    if progress_callback:
                        progress_callback("reply_edges", count, total)
        return count

    async def rebuild_all_group_indexes(self, progress_callback: ProgressCallback | None = None) -> None:
        async with async_session_factory() as session:
            result = await session.execute(select(ChatMessageIndex.group_id).distinct())
            groups = list(result.scalars().all())
        if progress_callback:
            progress_callback("chunks_spans", 0, len(groups))
        for index, group_id in enumerate(groups, start=1):
            await self.rebuild_group_indexes(group_id)
            if progress_callback:
                progress_callback("chunks_spans", index, len(groups))

    async def _enabled_lexicon_terms(self, group_id: str, *, refresh: bool = False) -> set[str]:
        if not refresh and group_id in self._lexicon_cache:
            return self._lexicon_cache[group_id]
        async with async_session_factory() as session:
            result = await session.execute(
                select(ChatTokenLexicon.term)
                .where(ChatTokenLexicon.group_id == group_id, ChatTokenLexicon.enabled == True)
                .order_by(ChatTokenLexicon.score.desc())
                .limit(LEXICON_MAX_TERMS)
            )
            terms = {str(term) for term in result.scalars().all()}
        try:
            import jieba

            for term in terms:
                jieba.add_word(term)
        except Exception:
            pass
        self._lexicon_cache[group_id] = terms
        return terms

    async def update_group_lexicon(
        self,
        group_id: str,
        start_time: int = 0,
        replace: bool = True,
        batch_size: int = 2000,
    ) -> int:
        batch_size = max(100, int(batch_size))
        term_freq: Counter[str] = Counter()
        doc_freq: Counter[str] = Counter()
        last_seen: dict[str, int] = {}
        last_id = 0
        total_docs = 0

        while True:
            async with async_session_factory() as session:
                result = await session.execute(
                    select(ChatMessageIndex.record_id, ChatMessageIndex.timestamp, ChatMessageIndex.plain_text)
                    .where(
                        ChatMessageIndex.group_id == group_id,
                        ChatMessageIndex.record_id > last_id,
                        ChatMessageIndex.timestamp >= max(0, int(start_time)),
                    )
                    .order_by(ChatMessageIndex.record_id.asc())
                    .limit(batch_size)
                )
                rows = list(result.all())
            if not rows:
                break
            for record_id, timestamp, plain_text in rows:
                last_id = int(record_id)
                if not plain_text:
                    continue
                candidates = _lexicon_candidates_from_text(plain_text)
                if not candidates:
                    continue
                compact = normalize_text(plain_text).replace(" ", "")
                total_docs += 1
                for term in candidates:
                    count = compact.count(term)
                    term_freq[term] += max(1, count)
                    doc_freq[term] += 1
                    last_seen[term] = max(last_seen.get(term, 0), int(timestamp))

        if not term_freq:
            return 0

        if replace:
            selected_terms = set(term_freq.keys())
        else:
            selected_terms = set(term_freq.keys())
            async with async_session_factory() as session:
                result = await session.execute(
                    select(ChatTokenLexicon).where(
                        ChatTokenLexicon.group_id == group_id,
                        ChatTokenLexicon.term.in_(selected_terms),
                    )
                )
                existing = {row.term: row for row in result.scalars().all()}
            for term, row in existing.items():
                term_freq[term] += row.freq
                doc_freq[term] += row.doc_freq
                last_seen[term] = max(last_seen.get(term, 0), row.last_seen)

        scored = [
            (term, term_freq[term], doc_freq[term], _score_lexicon_term(term, term_freq[term], doc_freq[term]))
            for term in term_freq
            if _is_good_lexicon_term(term)
        ]
        scored.sort(key=lambda item: (item[3], item[1], len(item[0])), reverse=True)
        keep = scored[: LEXICON_MAX_TERMS * 2]
        enabled_terms = {term for term, freq, docs, _score in scored[:LEXICON_MAX_TERMS] if freq >= LEXICON_MIN_FREQ and docs >= LEXICON_MIN_DOC_FREQ}

        async with async_session_factory() as session:
            async with session.begin():
                if replace:
                    await session.execute(delete(ChatTokenLexicon).where(ChatTokenLexicon.group_id == group_id))
                for term, freq, docs, score in keep:
                    await session.merge(ChatTokenLexicon(
                        group_id=group_id,
                        term=term,
                        freq=int(freq),
                        doc_freq=int(docs),
                        score=float(score),
                        last_seen=int(last_seen.get(term, 0)),
                        enabled=term in enabled_terms,
                    ))
        self._lexicon_cache.pop(group_id, None)
        await self._enabled_lexicon_terms(group_id, refresh=True)
        logger.info(f"chat lexicon updated group={group_id} docs={total_docs} terms={len(keep)} enabled={len(enabled_terms)}")
        return len(enabled_terms)

    async def rebuild_group_indexes(self, group_id: str, start_time: int = 0) -> None:
        if start_time <= 0:
            await self.update_group_lexicon(group_id, replace=True)
        lexicon_terms = await self._enabled_lexicon_terms(group_id)
        async with async_session_factory() as session:
            async with session.begin():
                if start_time <= 0:
                    await session.execute(delete(ChatChunkMessage).where(ChatChunkMessage.chunk_id.in_(
                        select(ChatChunkIndex.id).where(ChatChunkIndex.group_id == group_id)
                    )))
                    await session.execute(delete(ChatChunkIndex).where(ChatChunkIndex.group_id == group_id))
                    await session.execute(delete(ChatRetrievalSpan).where(ChatRetrievalSpan.group_id == group_id))
                    await session.execute(
                        update(ChatMessageIndex)
                        .where(ChatMessageIndex.group_id == group_id)
                        .values(primary_chunk_id=None)
                    )
                    rebuild_from = 0
                else:
                    rebuild_from = max(0, start_time - CHUNK_GAP_SECONDS)
                    old_chunk_ids = select(ChatChunkIndex.id).where(
                        ChatChunkIndex.group_id == group_id,
                        ChatChunkIndex.end_time >= rebuild_from,
                    )
                    await session.execute(delete(ChatChunkMessage).where(ChatChunkMessage.chunk_id.in_(old_chunk_ids)))
                    await session.execute(
                        delete(ChatChunkIndex).where(
                            ChatChunkIndex.group_id == group_id,
                            ChatChunkIndex.end_time >= rebuild_from,
                        )
                    )
                    await session.execute(
                        delete(ChatRetrievalSpan).where(
                            ChatRetrievalSpan.group_id == group_id,
                            ChatRetrievalSpan.end_time >= rebuild_from,
                        )
                    )
                    await session.execute(
                        update(ChatMessageIndex)
                        .where(ChatMessageIndex.group_id == group_id, ChatMessageIndex.timestamp >= rebuild_from)
                        .values(primary_chunk_id=None)
                    )

                result = await session.execute(
                    select(ChatMessageIndex)
                    .where(ChatMessageIndex.group_id == group_id, ChatMessageIndex.timestamp >= rebuild_from)
                    .order_by(ChatMessageIndex.timestamp.asc(), ChatMessageIndex.record_id.asc())
                )
                rows = list(result.scalars().all())
                msg_by_id = {row.record_id: row for row in rows}

                for group in _build_core_groups(rows):
                    if not group:
                        continue
                    text = "\n".join(_message_lines(group))
                    chunk = ChatChunkIndex(
                        group_id=group_id,
                        start_time=group[0].timestamp,
                        end_time=group[-1].timestamp,
                        chunk_text=text,
                        keywords=_json_dumps(extract_keywords(text)),
                        summary=_chunk_summary(text),
                        participant_uids=_json_dumps(_participants(group)),
                    )
                    session.add(chunk)
                    await session.flush()
                    for order, row in enumerate(group):
                        row.primary_chunk_id = chunk.id
                        session.add(ChatChunkMessage(
                            chunk_id=chunk.id,
                            message_id=row.record_id,
                            role="core",
                            message_order=order,
                        ))

                reply_targets: defaultdict[int, list[ChatMessageIndex]] = defaultdict(list)
                for row in rows:
                    if row.reply_to_record_id is not None:
                        reply_targets[row.reply_to_record_id].append(row)

                for span_rows in _build_span_groups(rows):
                    if not span_rows:
                        continue
                    span_ids = {row.record_id for row in span_rows}
                    lines = _message_lines(span_rows)
                    anchor_rows: list[ChatMessageIndex] = []
                    for row in span_rows:
                        if row.reply_to_record_id and row.reply_to_record_id not in span_ids:
                            target = msg_by_id.get(row.reply_to_record_id)
                            if target is not None:
                                anchor_rows.append(target)
                        for reply_row in reply_targets.get(row.record_id, [])[:3]:
                            if reply_row.record_id not in span_ids:
                                anchor_rows.append(reply_row)
                    if anchor_rows:
                        seen: set[int] = set()
                        lines.append("reply anchors:")
                        for anchor in anchor_rows:
                            if anchor.record_id in seen:
                                continue
                            seen.add(anchor.record_id)
                            lines.append(_format_message(anchor, prefix="anchor"))
                    text = "\n".join(lines)
                    span_tokens = _bm25_tokens(text, lexicon_terms)
                    chunk_ids = sorted({row.primary_chunk_id for row in span_rows if row.primary_chunk_id is not None})
                    span = ChatRetrievalSpan(
                        group_id=group_id,
                        start_time=span_rows[0].timestamp,
                        end_time=span_rows[-1].timestamp,
                        span_text=text,
                        keywords=_json_dumps(extract_keywords(text)),
                        token_text=_json_dumps(span_tokens),
                        token_count=len(span_tokens),
                        participant_uids=_json_dumps(_participants(span_rows)),
                        message_ids=_json_dumps([row.record_id for row in span_rows]),
                        chunk_ids=_json_dumps(chunk_ids),
                    )
                    session.add(span)

    def _message_to_dict(self, row: ChatMessageIndex, role: str | None = None) -> dict[str, Any]:
        payload = {
            "message_id": row.record_id,
            "qq": row.user_id,
            "timestamp": row.timestamp,
            "time": datetime.fromtimestamp(row.timestamp).strftime("%Y-%m-%d %H:%M:%S"),
            "text": row.plain_text,
            "mentioned_uids": _json_loads_list(row.mentioned_uids),
            "reply_to_message_id": row.reply_to_record_id,
            "reply_to_mid": row.reply_to_mid,
            "has_image": row.has_image,
            "image_summaries": _json_loads_list(row.image_summaries),
            "primary_chunk_id": row.primary_chunk_id,
        }
        if role is not None:
            payload["role"] = role
        return payload

    async def search_chat_spans(
        self,
        group_id: str,
        query: str = "",
        users: list[str] | None = None,
        time_start: int | str | None = None,
        time_end: int | str | None = None,
        strict_time_end: bool = False,
        limit: int = 10,
    ) -> str:
        limit = max(1, min(int(limit or 10), 20))
        start_ts = _parse_time_bound(time_start, is_end=False)
        end_ts = _parse_time_bound(time_end, is_end=True)
        lexicon_terms = await self._enabled_lexicon_terms(group_id)
        query_terms = _bm25_tokens(query, lexicon_terms)
        async with async_session_factory() as session:
            stmt = select(ChatRetrievalSpan).where(ChatRetrievalSpan.group_id == group_id)
            if start_ts is not None:
                stmt = stmt.where(ChatRetrievalSpan.end_time >= start_ts)
            if end_ts is not None:
                if strict_time_end:
                    stmt = stmt.where(ChatRetrievalSpan.end_time <= end_ts)
                else:
                    stmt = stmt.where(ChatRetrievalSpan.start_time <= end_ts)
            if users:
                user_filters = [ChatRetrievalSpan.participant_uids.like(f'%"{str(user)}"%') for user in users]
                stmt = stmt.where(or_(*user_filters))
            result = await session.execute(stmt.order_by(ChatRetrievalSpan.end_time.desc()).limit(BM25_CANDIDATE_LIMIT))
            spans = list(result.scalars().all())

        candidate_count = len(spans)
        if query_terms:
            documents: list[list[str]] = []
            for span in spans:
                tokens = [str(token) for token in _json_loads_list(getattr(span, "token_text", None))]
                if not tokens:
                    tokens = _bm25_tokens(span.span_text, lexicon_terms)
                documents.append(tokens)
            scores = _bm25_rank(query_terms, documents)
            scored_spans = [
                (span, score)
                for span, score in zip(spans, scores)
                if score > 0
            ]
            scored_spans = sorted(scored_spans, key=lambda item: (item[1], item[0].end_time), reverse=True)[:limit]
        else:
            scored_spans = [(span, 1.0) for span in spans[:limit]]

        records = []
        for span, bm25_score in scored_spans:
            snippet = _short_text(span.span_text, 700)
            records.append({
                "span_id": span.id,
                "group_id": span.group_id,
                "start_time": span.start_time,
                "end_time": span.end_time,
                "start": datetime.fromtimestamp(span.start_time).strftime("%Y-%m-%d %H:%M:%S"),
                "end": datetime.fromtimestamp(span.end_time).strftime("%Y-%m-%d %H:%M:%S"),
                "participant_uids": _json_loads_list(span.participant_uids),
                "message_ids": _json_loads_list(span.message_ids),
                "chunk_ids": _json_loads_list(span.chunk_ids),
                "keywords": _json_loads_list(span.keywords),
                "score": round(bm25_score, 4),
                "score_type": "bm25" if query_terms else "recency",
                "snippet": snippet,
            })
        return _json_dumps({
            "records": records,
            "truncated": candidate_count >= BM25_CANDIDATE_LIMIT,
            "candidate_count": candidate_count,
            "query_terms": query_terms,
            "strict_time_end": bool(strict_time_end),
        })

    async def fetch_span_messages(self, group_id: str, span_id: int) -> str:
        async with async_session_factory() as session:
            span = await session.get(ChatRetrievalSpan, int(span_id))
            if span is None or span.group_id != group_id:
                return _json_dumps({"records": [], "error": "span not found"})
            ids = [int(value) for value in _json_loads_list(span.message_ids)]
            result = await session.execute(
                select(ChatMessageIndex)
                .where(ChatMessageIndex.record_id.in_(ids))
                .order_by(ChatMessageIndex.timestamp.asc(), ChatMessageIndex.record_id.asc())
            )
            rows = list(result.scalars().all())
        order = {message_id: idx for idx, message_id in enumerate(ids)}
        rows = sorted(rows, key=lambda row: order.get(row.record_id, 10**9))
        return _json_dumps({"records": [self._message_to_dict(row) for row in rows]})

    async def fetch_chunk_messages(self, group_id: str, chunk_id: int) -> str:
        async with async_session_factory() as session:
            chunk = await session.get(ChatChunkIndex, int(chunk_id))
            if chunk is None or chunk.group_id != group_id:
                return _json_dumps({"records": [], "error": "chunk not found"})
            result = await session.execute(
                select(ChatMessageIndex, ChatChunkMessage.role)
                .join(ChatChunkMessage, ChatChunkMessage.message_id == ChatMessageIndex.record_id)
                .where(ChatChunkMessage.chunk_id == int(chunk_id))
                .order_by(ChatChunkMessage.message_order.asc())
            )
            rows = list(result.all())
        return _json_dumps({"records": [self._message_to_dict(row, role=role) for row, role in rows]})

    async def fetch_message_context(self, group_id: str, message_id: int, before: int = 8, after: int = 8) -> str:
        before = max(0, min(int(before), 30))
        after = max(0, min(int(after), 30))
        async with async_session_factory() as session:
            row = await session.get(ChatMessageIndex, int(message_id))
            if row is None or row.group_id != group_id:
                return _json_dumps({"records": [], "error": "message not found"})
            prev_result = await session.execute(
                select(ChatMessageIndex)
                .where(
                    ChatMessageIndex.group_id == group_id,
                    (ChatMessageIndex.timestamp < row.timestamp)
                    | ((ChatMessageIndex.timestamp == row.timestamp) & (ChatMessageIndex.record_id < row.record_id)),
                )
                .order_by(ChatMessageIndex.timestamp.desc(), ChatMessageIndex.record_id.desc())
                .limit(before)
            )
            next_result = await session.execute(
                select(ChatMessageIndex)
                .where(
                    ChatMessageIndex.group_id == group_id,
                    (ChatMessageIndex.timestamp > row.timestamp)
                    | ((ChatMessageIndex.timestamp == row.timestamp) & (ChatMessageIndex.record_id > row.record_id)),
                )
                .order_by(ChatMessageIndex.timestamp.asc(), ChatMessageIndex.record_id.asc())
                .limit(after)
            )
            rows = list(reversed(list(prev_result.scalars().all()))) + [row] + list(next_result.scalars().all())
        return _json_dumps({"records": [self._message_to_dict(item) for item in rows]})

    async def fetch_reply_thread(
        self,
        group_id: str,
        message_id: int,
        direction: str = "both",
        context: int = 3,
    ) -> str:
        context = max(0, min(int(context), 10))
        message_id = int(message_id)
        seen: set[int] = set()
        collected: set[int] = {message_id}
        async with async_session_factory() as session:
            root = await session.get(ChatMessageIndex, message_id)
            if root is None or root.group_id != group_id:
                return _json_dumps({"records": [], "error": "message not found"})

            if direction in {"ancestors", "both"}:
                current = message_id
                for _ in range(8):
                    if current in seen:
                        break
                    seen.add(current)
                    edge = (
                        await session.execute(
                            select(ChatReplyEdge).where(
                                ChatReplyEdge.group_id == group_id,
                                ChatReplyEdge.from_message_id == current,
                            )
                        )
                    ).scalar_one_or_none()
                    if edge is None:
                        break
                    collected.add(edge.to_message_id)
                    current = edge.to_message_id

            if direction in {"replies", "both"}:
                frontier = [message_id]
                for _ in range(4):
                    if not frontier:
                        break
                    result = await session.execute(
                        select(ChatReplyEdge).where(
                            ChatReplyEdge.group_id == group_id,
                            ChatReplyEdge.to_message_id.in_(frontier),
                        )
                    )
                    edges = list(result.scalars().all())
                    frontier = []
                    for edge in edges:
                        if edge.from_message_id not in collected:
                            collected.add(edge.from_message_id)
                            frontier.append(edge.from_message_id)

            if context:
                context_ids = set(collected)
                for mid in list(collected):
                    row = await session.get(ChatMessageIndex, mid)
                    if row is None:
                        continue
                    prev_result = await session.execute(
                        select(ChatMessageIndex.record_id)
                        .where(
                            ChatMessageIndex.group_id == group_id,
                            ChatMessageIndex.timestamp <= row.timestamp,
                        )
                        .order_by(ChatMessageIndex.timestamp.desc(), ChatMessageIndex.record_id.desc())
                        .limit(context + 1)
                    )
                    next_result = await session.execute(
                        select(ChatMessageIndex.record_id)
                        .where(
                            ChatMessageIndex.group_id == group_id,
                            ChatMessageIndex.timestamp >= row.timestamp,
                        )
                        .order_by(ChatMessageIndex.timestamp.asc(), ChatMessageIndex.record_id.asc())
                        .limit(context + 1)
                    )
                    context_ids.update(prev_result.scalars().all())
                    context_ids.update(next_result.scalars().all())
                collected = context_ids

            result = await session.execute(
                select(ChatMessageIndex)
                .where(ChatMessageIndex.record_id.in_(collected))
                .order_by(ChatMessageIndex.timestamp.asc(), ChatMessageIndex.record_id.asc())
            )
            rows = list(result.scalars().all())
            edge_result = await session.execute(
                select(ChatReplyEdge).where(
                    ChatReplyEdge.group_id == group_id,
                    ChatReplyEdge.from_message_id.in_([row.record_id for row in rows]),
                )
            )
            edges = [
                {"from_message_id": edge.from_message_id, "to_message_id": edge.to_message_id}
                for edge in edge_result.scalars().all()
            ]
        return _json_dumps({"records": [self._message_to_dict(row) for row in rows], "reply_edges": edges})

    async def fetch_chat_stats(
        self,
        group_id: str,
        query: str = "",
        users: list[str] | None = None,
        time_start: int | str | None = None,
        time_end: int | str | None = None,
        bucket: str = "day",
    ) -> str:
        start_ts = _parse_time_bound(time_start, is_end=False)
        end_ts = _parse_time_bound(time_end, is_end=True)
        async with async_session_factory() as session:
            stmt = (
                select(ChatMessageIndex)
                .join(ChatChunkMessage, ChatChunkMessage.message_id == ChatMessageIndex.record_id)
                .where(
                    ChatMessageIndex.group_id == group_id,
                    ChatChunkMessage.role == "core",
                )
            )
            if users:
                stmt = stmt.where(ChatMessageIndex.user_id.in_([str(user) for user in users]))
            if start_ts is not None:
                stmt = stmt.where(ChatMessageIndex.timestamp >= start_ts)
            if end_ts is not None:
                stmt = stmt.where(ChatMessageIndex.timestamp <= end_ts)
            if query:
                terms = TOKEN_RE.findall(normalize_text(query))
                for term in terms[:8]:
                    stmt = stmt.where(func.lower(ChatMessageIndex.normalized_text).like(f"%{term}%"))
            result = await session.execute(
                stmt.order_by(ChatMessageIndex.timestamp.desc(), ChatMessageIndex.record_id.desc()).limit(5000)
            )
            rows = list(result.scalars().all())

        user_counts = Counter(row.user_id for row in rows)
        image_count = sum(1 for row in rows if row.has_image)
        at_count = sum(len(_json_loads_list(row.mentioned_uids)) for row in rows)
        time_counts: Counter[str] = Counter()
        for row in rows:
            dt = datetime.fromtimestamp(row.timestamp)
            key = dt.strftime("%Y-%m-%d %H:00") if bucket == "hour" else dt.strftime("%Y-%m-%d")
            time_counts[key] += 1
        return _json_dumps({
            "message_count": len(rows),
            "user_counts": user_counts.most_common(20),
            "image_count": image_count,
            "at_count": at_count,
            "time_counts": sorted(time_counts.items()),
            "truncated": len(rows) >= 5000,
        })


db = DataManager()


@get_driver().on_startup
async def _schedule_chat_history_indexes_repair() -> None:
    asyncio.create_task(_repair_chat_history_indexes_in_background())


async def _repair_chat_history_indexes_in_background() -> None:
    try:
        repaired = await db.repair_recent_indexes()
        if repaired:
            logger.info(f"chat history startup repair indexed {repaired} recent messages")
    except Exception as e:
        logger.warning(f"chat history startup repair failed: {e}")
