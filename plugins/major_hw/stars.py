"""Major 作业赛事级打🌟名单（未购通行证）。

打🌟只影响**展示层**：

* ``/major-homework`` 排名列：打🌟玩家显示 ``🌟``、不占数字名次，但仍按原排序
  规则原地混排；
* ``/作业排名`` 文字兜底、``/赛事作业结果`` 标注、``/作业走势`` 折线虚线。

名单按 ``config.major_name``（如 ``2026-singapore``）持久化到
``local_storage["major_hw_stars"]``，同一届 Major 的各个阶段共享，不同赛事天然
隔离。本模块不依赖数据库结构（零迁移），纯逻辑与异步持久化分离，便于离线单测。
"""

from __future__ import annotations

import json
import time
from collections.abc import Collection, Iterable, Sequence

from nonebot import logger

STORAGE_KEY = "major_hw_stars"
CACHE_TTL_SECONDS = 30.0

#: 进程内缓存：``{event_name: sorted_uid_list}``
_cache: dict[str, list[str]] | None = None
_cache_loaded_at = 0.0


# --------------------------------------------------------------------------- #
# 纯函数
# --------------------------------------------------------------------------- #
def normalize_event_key(event_name: str) -> str:
    """赛事键（``major_name``）去首尾空白；空字符串抛 ``ValueError``。"""

    text = str(event_name or "").strip()
    if not text:
        raise ValueError("赛事名称不能为空")
    return text


def normalize_uid(uid: str | int) -> str:
    """UID 转字符串并去首尾空白；空字符串抛 ``ValueError``。"""

    text = str(uid if uid is not None else "").strip()
    if not text:
        raise ValueError("UID 不能为空")
    return text


def parse_stars_payload(raw: str | dict | None) -> dict[str, list[str]]:
    """容错解析打🌟名单，非法值返回 ``{}``。

    * 支持 JSON 字符串或已解析的 ``dict``；
    * 每个赛事下的 UID 去重、排序并过滤空串；
    * 非对象 / 非法 JSON / 非列表值一律安全忽略。
    """

    if raw is None or raw == "":
        return {}
    data: object = raw
    if isinstance(raw, str):
        try:
            data = json.loads(raw)
        except (TypeError, ValueError):
            logger.warning("major_hw_stars 不是合法 JSON，已按空名单处理")
            return {}
    if not isinstance(data, dict):
        logger.warning(f"major_hw_stars 结构非法（{type(data).__name__}），已按空名单处理")
        return {}

    result: dict[str, list[str]] = {}
    for raw_event, raw_uids in data.items():
        try:
            event_key = normalize_event_key(raw_event)
        except ValueError:
            continue
        if not isinstance(raw_uids, (list, tuple, set)):
            logger.warning(f"major_hw_stars[{event_key}] 不是列表，已忽略")
            continue
        cleaned: set[str] = set()
        for uid in raw_uids:
            if isinstance(uid, (dict, list, tuple, set)):
                continue
            try:
                cleaned.add(normalize_uid(uid))
            except ValueError:
                continue
        result[event_key] = sorted(cleaned)
    return result


def serialize_stars_payload(data: dict[str, list[str]]) -> str:
    """序列化打🌟名单：过滤空列表键，键与 UID 排序，便于稳定比较。"""

    cleaned: dict[str, list[str]] = {}
    for raw_event, raw_uids in (data or {}).items():
        try:
            event_key = normalize_event_key(raw_event)
        except ValueError:
            continue
        uids: set[str] = set()
        for uid in raw_uids or []:
            if isinstance(uid, (dict, list, tuple, set)):
                continue
            try:
                uids.add(normalize_uid(uid))
            except ValueError:
                continue
        if uids:
            cleaned[event_key] = sorted(uids)
    return json.dumps(cleaned, ensure_ascii=False, sort_keys=True)


def assign_homework_ranks(
    uids: Sequence[str],
    starred_uids: Collection[str],
) -> list[tuple[bool, int | None]]:
    """按输入顺序给出 ``(is_starred, rank)``。

    * 打🌟玩家产出 ``(True, None)``，**不递增**排名计数；
    * 普通玩家产出 ``(False, next_rank)``，``next_rank`` 从 1 开始；
    * 完全不重排输入，保持原排序原地混排。
    """

    starred_set = {str(uid).strip() for uid in starred_uids or ()}
    result: list[tuple[bool, int | None]] = []
    next_rank = 1
    for uid in uids or ():
        key = str(uid).strip()
        if key and key in starred_set:
            result.append((True, None))
            continue
        result.append((False, next_rank))
        next_rank += 1
    return result


# --------------------------------------------------------------------------- #
# 异步读写与缓存
# --------------------------------------------------------------------------- #
def invalidate_stars_cache() -> None:
    """清空进程内缓存（测试或外部改写 ``local_storage`` 后调用）。"""

    global _cache, _cache_loaded_at
    _cache = None
    _cache_loaded_at = 0.0


def _write_cache(data: dict[str, list[str]]) -> None:
    global _cache, _cache_loaded_at
    _cache = {key: list(value) for key, value in data.items()}
    _cache_loaded_at = time.monotonic()


async def load_all_stars(*, force_refresh: bool = False) -> dict[str, list[str]]:
    """读取全部赛事打🌟名单（TTL 缓存）；存储异常时记 warning 并降级 ``{}``。"""

    if (
        not force_refresh
        and _cache is not None
        and (time.monotonic() - _cache_loaded_at) <= CACHE_TTL_SECONDS
    ):
        return {key: list(value) for key, value in _cache.items()}

    try:
        from ..utils import local_storage

        raw = await local_storage.get(STORAGE_KEY)
    except Exception:
        logger.warning("读取 major_hw_stars 失败，按空名单处理", exc_info=True)
        return {}

    data = parse_stars_payload(raw)
    _write_cache(data)
    return {key: list(value) for key, value in data.items()}


async def get_starred_uids(event_name: str, *, force_refresh: bool = False) -> set[str]:
    """当前赛事的打🌟 UID 集合。"""

    event_key = normalize_event_key(event_name)
    data = await load_all_stars(force_refresh=force_refresh)
    return set(data.get(event_key, []))


async def list_starred_uids(event_name: str, *, force_refresh: bool = False) -> list[str]:
    """当前赛事的打🌟 UID 排序列表。"""

    return sorted(await get_starred_uids(event_name, force_refresh=force_refresh))


async def is_starred_uid(event_name: str, uid: str | int) -> bool:
    """判断 UID 是否在当前赛事打🌟名单内；空 UID 直接返回 ``False``。"""

    try:
        clean_uid = normalize_uid(uid)
    except ValueError:
        return False
    return clean_uid in await get_starred_uids(event_name)


def _clean_uids(uids: Iterable[str | int]) -> list[str]:
    cleaned: list[str] = []
    seen: set[str] = set()
    for uid in uids or ():
        if isinstance(uid, (dict, list, tuple, set)):
            continue
        try:
            key = normalize_uid(uid)
        except ValueError:
            continue
        if key not in seen:
            seen.add(key)
            cleaned.append(key)
    return cleaned


async def _persist(event_name: str, data: dict[str, list[str]]) -> list[str]:
    from ..utils import local_storage

    await local_storage.set(STORAGE_KEY, serialize_stars_payload(data))
    _write_cache(data)
    return sorted(data.get(event_name, []))


async def add_starred_uids(
    event_name: str,
    uids: Iterable[str | int],
) -> tuple[list[str], list[str]]:
    """加入打🌟名单，返回 ``(本次新增, 当前排序名单)``；重复添加幂等。"""

    event_key = normalize_event_key(event_name)
    targets = _clean_uids(uids)
    data = await load_all_stars(force_refresh=True)
    current = set(data.get(event_key, []))
    added = sorted(set(targets) - current)
    data[event_key] = sorted(current | set(targets))
    current_sorted = await _persist(event_key, data)
    return added, current_sorted


async def remove_starred_uids(
    event_name: str,
    uids: Iterable[str | int],
) -> tuple[list[str], list[str]]:
    """移出打🌟名单，返回 ``(本次移除, 当前排序名单)``；移除不存在项幂等。"""

    event_key = normalize_event_key(event_name)
    targets = _clean_uids(uids)
    data = await load_all_stars(force_refresh=True)
    current = set(data.get(event_key, []))
    removed = sorted(current & set(targets))
    data[event_key] = sorted(current - set(targets))
    current_sorted = await _persist(event_key, data)
    return removed, current_sorted


__all__ = [
    "CACHE_TTL_SECONDS",
    "STORAGE_KEY",
    "add_starred_uids",
    "assign_homework_ranks",
    "get_starred_uids",
    "invalidate_stars_cache",
    "is_starred_uid",
    "list_starred_uids",
    "load_all_stars",
    "normalize_event_key",
    "normalize_uid",
    "parse_stars_payload",
    "remove_starred_uids",
    "serialize_stars_payload",
]
