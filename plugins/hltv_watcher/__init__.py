from __future__ import annotations

import asyncio
import json
import random
import os
import time

from nonebot import get_bot, get_plugin_config, logger, on_command, require
from nonebot.adapters.onebot.v11 import Bot, Message, MessageSegment
from nonebot.exception import ActionFailed
from nonebot.plugin import PluginMetadata

require("nonebot_plugin_apscheduler")
from nonebot_plugin_apscheduler import scheduler

require("utils")
from ..utils import local_storage

require("runtime_config")
from ..runtime_config import runtime_config

event_update = require("major_hw").event_update


from .get5e import FiveEApiError, fetch_events, get_event_schedule
from .store import schedule_store
from .schedule_logic import legacy_results
from .logic import (
    DEFAULT_FOLLOWED_TEAMS,
    EventInfo,
    events_from_json,
    events_to_json,
    should_poll_event,
)

from .config import Config

__plugin_meta__ = PluginMetadata(
    name="hltv_watcher",
    description="自动维护关注赛事并推送 5EPlay 比赛结果",
    usage="",
    config=Config,
)

config = get_plugin_config(Config)

updategame = on_command("更新比赛", priority=10, block=True)

# General discovery runs every 30 minutes; team discovery is spread over ticks.
# Live results and upcoming schedules share the bounded polling queue.
DISCOVERY_INTERVAL_SECONDS = 30 * 60
DISCOVERY_FAILURE_BASE_SECONDS = 5 * 60
DISCOVERY_FAILURE_MAX_SECONDS = 60 * 60
RATE_LIMIT_COOLDOWN_SECONDS = 30 * 60
EVENT_REQUEST_DELAY_SECONDS = 2
EVENT_POLL_INTERVAL_SECONDS = 5 * 60
MAX_EVENTS_PER_MINUTE = 2

META_KEY = "hltv_event_meta"
NEXT_REFRESH_KEY = "hltv_next_discovery"
DISCOVERY_FAILURES_KEY = "hltv_discovery_failures"
POLL_COOLDOWN_KEY = "hltv_poll_cooldown"

update_lock = asyncio.Lock()
# event_id -> next poll timestamp.  Rebuilt after a restart; watched events are
# spread across the 5-minute interval instead of being polled in one batch.
poll_schedule: dict[int, float] = {}


async def _load_int(key: str, default: int = 0) -> int:
    raw = await local_storage.get(key, default=str(default))
    try:
        return int(raw)
    except (TypeError, ValueError):
        return default


async def _save_int(key: str, value: int) -> None:
    await local_storage.set(key, str(value))


async def _load_metadata() -> dict[int, EventInfo]:
    raw = await local_storage.get(META_KEY, default="")
    try:
        return events_from_json(raw)
    except (TypeError, ValueError, json.JSONDecodeError):
        logger.exception("赛事缓存解析失败，本次按无缓存处理")
        return {}


def _get_bot_or_none() -> Bot | None:
    try:
        return get_bot()
    except Exception:
        return None


async def _followed_teams() -> list[str]:
    value = await runtime_config.get("hltv_followed_teams")
    if not isinstance(value, list):
        return list(DEFAULT_FOLLOWED_TEAMS)
    return [str(item) for item in value if str(item).strip()]


async def refresh_watch_list(force: bool = False) -> None:
    """Rebuild the auto-managed watch list from the current 5E event list.

    The list contains active events that either include a followed team or are
    Major/S-tier.  Finished events are dropped.  On an API failure the previous
    list and metadata are kept, and a backoff is stored so the next 5-minute
    tick does not immediately retry.
    """
    now_ts = int(time.time())
    next_refresh = await _load_int(NEXT_REFRESH_KEY)
    if not force and next_refresh > now_ts:
        return

    try:
        events = await fetch_events()
        await schedule_store.save_events(events)
        selected, _ = await schedule_store.watched(await _followed_teams(), await runtime_config.get("hltv_manual_event_ids"))
    except Exception as exc:
        failures = await _load_int(DISCOVERY_FAILURES_KEY) + 1
        delay = min(
            DISCOVERY_FAILURE_MAX_SECONDS,
            DISCOVERY_FAILURE_BASE_SECONDS * (2 ** min(failures - 1, 4)),
        )
        if getattr(exc, "status", None) in (403, 429):
            delay = max(delay, RATE_LIMIT_COOLDOWN_SECONDS)
            await _save_int(POLL_COOLDOWN_KEY, now_ts + RATE_LIMIT_COOLDOWN_SECONDS)
        await _save_int(DISCOVERY_FAILURES_KEY, failures)
        await _save_int(NEXT_REFRESH_KEY, now_ts + delay)
        logger.warning(f"刷新赛事监视列表失败，{delay} 秒后重试: {exc}")
        return

    event_ids = [event.event_id for event in selected]
    current_ids = await runtime_config.get("hltv_event_id_list")
    if sorted(int(value) for value in current_ids) != sorted(event_ids):
        await runtime_config.set("hltv_event_id_list", event_ids, "auto")
        logger.info(
            "赛事监视列表已更新: "
            + (", ".join(f"{event.name}({event.event_id})" for event in selected) or "空")
        )

    await local_storage.set(META_KEY, events_to_json(selected))
    await _save_int(DISCOVERY_FAILURES_KEY, 0)
    # Spread refresh times a little so multiple bots do not align exactly.
    await _save_int(
        NEXT_REFRESH_KEY,
        now_ts + DISCOVERY_INTERVAL_SECONDS + random.randint(0, 60),
    )
    live_count = sum(1 for event in selected if event.status == "live")
    logger.info(
        f"赛事监视列表刷新完成: {len(selected)} 个赛事，其中 {live_count} 个进行中"
    )


async def _rebuild_watch_list() -> None:
    if not await schedule_store.has_catalog() and not await runtime_config.get("hltv_manual_event_ids"):
        return  # Initial discovery failed: retain the previous deployment's cache.
    selected, _ = await schedule_store.watched(await _followed_teams(), await runtime_config.get("hltv_manual_event_ids"))
    ids = [event.event_id for event in selected]
    if sorted(await runtime_config.get("hltv_event_id_list")) != sorted(ids):
        await runtime_config.set("hltv_event_id_list", ids, "auto")
    await local_storage.set(META_KEY, events_to_json(selected))


async def _discover_followed_team() -> None:
    if await _load_int(POLL_COOLDOWN_KEY) > int(time.time()):
        return
    due = await schedule_store.due_team(await _followed_teams())
    if due is None:
        return
    team_id, external_id = due
    try:
        events = await fetch_events(team_id=external_id)
        await schedule_store.save_events(events)
        await schedule_store.delay_team(team_id, DISCOVERY_INTERVAL_SECONDS)
    except FiveEApiError as exc:
        await schedule_store.delay_team(team_id, DISCOVERY_FAILURE_MAX_SECONDS)
        if exc.status in (403, 429):
            await _save_int(POLL_COOLDOWN_KEY, int(time.time()) + RATE_LIMIT_COOLDOWN_SECONDS)
        logger.warning(f"发现关注队伍 {external_id} 的赛事失败: {exc}")
    await asyncio.sleep(EVENT_REQUEST_DELAY_SECONDS)


async def _send_pending_notifications(bot: Bot) -> None:
    for _ in range(10):
        delivery = await schedule_store.claim_delivery(
            {str(g) for g in config.cs_group_list}, {str(g) for g in config.cs_event_group_list})
        if delivery is None:
            return
        message = Message(delivery["text"])
        if delivery["users"]:
            message += MessageSegment.text("\n")
            for uid in delivery["users"]:
                message += MessageSegment.at(uid)
        try:
            response = await bot.send_group_msg(group_id=int(delivery["group_id"]), message=message)
            status = "sent" if response and response.get("message_id") is not None else "unknown"
        except ActionFailed:
            status = "pending"  # OneBot explicitly rejected this attempt.
            logger.exception("赛果发送被拒绝，将退避重试")
        except Exception:
            status = "unknown"  # A timeout may have delivered: do not duplicate it.
            logger.exception("赛果发送结果未知，已保留记录")
        await schedule_store.delivery_result(delivery["key"], status)
        await asyncio.sleep(1)


async def _poll_event(event: EventInfo, followed: list[str], bot: Bot) -> bool:
    try:
        source_event, matches = await get_event_schedule(event.event_id)
    except FiveEApiError as exc:
        logger.warning(f"抓取赛事 {event.event_id} 失败: {exc}")
        if exc.status in (403, 429):
            await _save_int(POLL_COOLDOWN_KEY, int(time.time()) + RATE_LIMIT_COOLDOWN_SECONDS)
        return False
    if source_event is not None:
        await schedule_store.save_events([source_event])
        event = source_event
    try:
        stored = json.loads(await local_storage.get(f"hltvresult{event.event_id}", default="[]"))
        if not isinstance(stored, list):
            stored = []
    except (TypeError, ValueError):
        stored = []
    known = {str(row[3]) for row in stored if isinstance(row, (list, tuple)) and len(row) >= 4}
    default_ids = await schedule_store.default_ids(followed)
    await schedule_store.ingest(event, matches, default_ids, config.cs_group_list, known)
    # Keep the winner-first legacy feed and its per-result Major update semantics.
    indexed = {str(row[3]): row for row in reversed(stored) if isinstance(row, (list, tuple)) and len(row) >= 4}
    for match in legacy_results(matches):
        if indexed.get(match[3]) == list(match) or indexed.get(match[3]) == match:
            continue
        indexed[match[3]] = match
        snapshot = list(reversed(list(indexed.values())))
        await local_storage.set(f"hltvresult{event.event_id}", json.dumps(snapshot, ensure_ascii=False))
        try:
            await event_update(event.event_id, snapshot)
        except Exception:
            logger.exception(f"更新赛事 {event.event_id} 的 Major 作业失败")
    return True


async def _poll_due_events(bot: Bot, now_ts: int | None = None) -> None:
    event_ids: list[int] = await runtime_config.get("hltv_event_id_list")
    metadata = await _load_metadata()
    followed = await _followed_teams()
    if now_ts is None:
        now_ts = int(time.time())

    cooldown = await _load_int(POLL_COOLDOWN_KEY)
    if cooldown > now_ts:
        logger.warning(
            f"5E 结果抓取处于冷却期，跳过本轮（还剩 {cooldown - now_ts} 秒）"
        )
        return

    pollable: list[EventInfo] = []
    for raw_id in event_ids:
        event_id = int(raw_id)
        event = metadata.get(event_id)
        if event is None:
            # No discovery metadata yet (for example a manual ID or a failed
            # refresh).  Keep polling it once; before the first successful
            # discovery, keep the previous "notify all results" behaviour so a
            # transient list failure cannot silence a Major.
            event = EventInfo(
                event_id=event_id,
                name=f"赛事 {event_id}",
                grade="" if metadata else "1",
                grade_label="" if metadata else "Major",
                status="live",
            )
        if should_poll_event(event, now_ts) or event.status == "upcoming":
            pollable.append(event)

    # Drop schedule entries for events that are no longer live.
    live_ids = {event.event_id for event in pollable}
    for event_id in list(poll_schedule):
        if event_id not in live_ids:
            poll_schedule.pop(event_id, None)

    # Spread newly discovered live events across one poll interval instead of
    # requesting them all at the same moment.
    total = len(pollable)
    for index, event in enumerate(pollable):
        if event.event_id in poll_schedule:
            continue
        offset = 30 + int(
            index * (EVENT_POLL_INTERVAL_SECONDS - 30) / max(1, total)
        )
        poll_schedule[event.event_id] = now_ts + offset

    due = [
        event
        for event in pollable
        if poll_schedule[event.event_id] <= now_ts
    ]
    due.sort(
        key=lambda event: (
            event.status != "live",
            not event.is_major_tier,
            event.start_ts,
            event.event_id,
        )
    )
    if len(due) > MAX_EVENTS_PER_MINUTE:
        logger.info(
            f"本轮有 {len(due)} 个赛事到期，按每分钟上限只抓取 "
            f"{MAX_EVENTS_PER_MINUTE} 个"
        )
        due = due[:MAX_EVENTS_PER_MINUTE]

    for event in due:
        logger.info(
            f"抓取赛事 {event.name or event.event_id}({event.event_id}) 结果"
        )
        ok = await _poll_event(event, followed, bot)
        if ok:
            poll_schedule[event.event_id] = (
                now_ts
                + (30 * 60 if event.status == "upcoming" else EVENT_POLL_INTERVAL_SECONDS)
                + random.randint(-10, 10)
            )
        else:
            # A 5E error (rate limit, maintenance, network) usually affects the
            # whole API; stop this cycle instead of hammering the remaining
            # events.
            poll_schedule[event.event_id] = now_ts + EVENT_POLL_INTERVAL_SECONDS
            logger.warning("本轮停止继续抓取赛事结果，避免连续请求 5E")
            break
        await asyncio.sleep(EVENT_REQUEST_DELAY_SECONDS)


@scheduler.scheduled_job(
    "cron",
    minute="*",
    id="hltv",
    max_instances=1,
    coalesce=True,
    misfire_grace_time=120,
)
async def scheduled_update_events() -> None:
    if os.getenv("CS_DISABLE_BACKGROUND_JOBS", "").lower() in {"1", "true", "yes"}:
        return
    await _run_update(force_discovery=False)


@updategame.handle()
async def manual_update_events() -> None:
    # /更新比赛 should pick up an edited follow list immediately.
    await _run_update(force_discovery=True)


async def _run_update(force_discovery: bool) -> None:
    async with update_lock:
        await refresh_watch_list(force=force_discovery)
        await _discover_followed_team()
        await _rebuild_watch_list()
        bot = _get_bot_or_none()
        if bot is None:
            logger.warning("没有已连接的 Bot，跳过本轮赛事结果抓取")
            return
        try:
            await _poll_due_events(bot)
        finally:
            await _send_pending_notifications(bot)


from . import commands
