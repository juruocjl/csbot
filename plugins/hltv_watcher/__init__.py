from __future__ import annotations

import asyncio
import json
import random
import time

from nonebot import get_bot, get_plugin_config, logger, on_command, require
from nonebot.adapters.onebot.v11 import Bot
from nonebot.plugin import PluginMetadata

require("nonebot_plugin_apscheduler")
from nonebot_plugin_apscheduler import scheduler

require("utils")
from ..utils import local_storage

require("runtime_config")
from ..runtime_config import runtime_config

event_update = require("major_hw").event_update


from .get5e import FiveEApiError, fetch_events, get_matches
from .logic import (
    DEFAULT_FOLLOWED_TEAMS,
    EventInfo,
    build_message,
    events_from_json,
    events_to_json,
    notify_matches,
    select_watch_events,
    should_poll_event,
    split_message,
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

# Discovery is cheap (one list request) and runs every 30 minutes.  Result
# polling only touches events that are actually live.
DISCOVERY_INTERVAL_SECONDS = 30 * 60
DISCOVERY_FAILURE_BASE_SECONDS = 5 * 60
DISCOVERY_FAILURE_MAX_SECONDS = 60 * 60
RATE_LIMIT_COOLDOWN_SECONDS = 30 * 60
EVENT_REQUEST_DELAY_SECONDS = 2
MAX_EVENTS_PER_CYCLE = 8

META_KEY = "hltv_event_meta"
NEXT_REFRESH_KEY = "hltv_next_discovery"
DISCOVERY_FAILURES_KEY = "hltv_discovery_failures"
POLL_COOLDOWN_KEY = "hltv_poll_cooldown"

update_lock = asyncio.Lock()


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
        followed = await _followed_teams()
        selected = select_watch_events(events, followed, now_ts)
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
    logger.info(
        "赛事监视列表刷新完成: %s 个赛事，其中 %s 个进行中",
        len(selected),
        sum(1 for event in selected if event.status == "live"),
    )


async def _send_notification(bot: Bot, text: str) -> None:
    group_ids = list(config.cs_group_list)
    if not group_ids:
        logger.warning("CS_GROUP_LIST 为空，赛事结果未发送")
        return
    for chunk in split_message(text):
        for group_id in group_ids:
            try:
                await bot.send_msg(
                    message_type="group",
                    group_id=group_id,
                    message=chunk,
                )
            except Exception:
                logger.exception(f"发送赛事结果到群 {group_id} 失败")


async def _poll_event(event: EventInfo, followed: list[str], bot: Bot) -> bool:
    try:
        title, results = await get_matches(
            event.event_id, fallback_title=event.name
        )
    except FiveEApiError as exc:
        logger.warning(f"抓取赛事 {event.event_id} 结果失败: {exc}")
        if exc.status in (403, 429):
            await _save_int(
                POLL_COOLDOWN_KEY,
                int(time.time()) + RATE_LIMIT_COOLDOWN_SECONDS,
            )
        return False

    stored_raw = await local_storage.get(
        f"hltvresult{event.event_id}", default="[]"
    )
    try:
        stored = json.loads(stored_raw)
    except (TypeError, ValueError):
        logger.warning(f"赛事 {event.event_id} 的历史结果无效，重新建立")
        stored = []
    if not isinstance(stored, list):
        stored = []

    chronological = list(reversed(stored))
    known_ids = {
        str(match[3])
        for match in chronological
        if isinstance(match, (list, tuple)) and len(match) >= 4
    }
    new_matches: list[tuple[str, str, str, str]] = []
    for match in results:
        if match[3] in known_ids:
            continue
        new_matches.append(match)
        known_ids.add(match[3])

    if not new_matches:
        return True

    for match in new_matches:
        chronological.append(match)
        snapshot = list(reversed(chronological))
        await local_storage.set(
            f"hltvresult{event.event_id}",
            json.dumps(snapshot, ensure_ascii=False),
        )
        try:
            await event_update(event.event_id, snapshot)
        except Exception:
            logger.exception(f"更新赛事 {event.event_id} 的 Major 作业失败")

    notify = notify_matches(event, new_matches, followed)
    if notify:
        text = build_message(event, list(reversed(notify)))
        logger.info(
            "赛事 %s(%s) 新增 %s 场结果，推送 %s 场",
            event.name or title,
            event.event_id,
            len(new_matches),
            len(notify),
        )
        await _send_notification(bot, text)
    else:
        logger.info(
            "赛事 %s(%s) 新增 %s 场结果，均不涉及关注队伍，不推送",
            event.name or title,
            event.event_id,
            len(new_matches),
        )
    return True


async def _poll_watched_events(bot: Bot) -> None:
    event_ids: list[int] = await runtime_config.get("hltv_event_id_list")
    metadata = await _load_metadata()
    followed = await _followed_teams()
    now_ts = int(time.time())

    cooldown = await _load_int(POLL_COOLDOWN_KEY)
    if cooldown > now_ts:
        logger.warning(
            "5E 结果抓取处于冷却期，跳过本轮（还剩 %s 秒）", cooldown - now_ts
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
        if should_poll_event(event, now_ts):
            pollable.append(event)

    pollable.sort(
        key=lambda event: (
            event.status != "live",
            not event.is_major_tier,
            event.start_ts,
            event.event_id,
        )
    )
    if len(pollable) > MAX_EVENTS_PER_CYCLE:
        logger.warning(
            "本轮有 %s 个赛事需要抓取，超过上限 %s，只抓取前 %s 个",
            len(pollable),
            MAX_EVENTS_PER_CYCLE,
            MAX_EVENTS_PER_CYCLE,
        )
        pollable = pollable[:MAX_EVENTS_PER_CYCLE]

    for event in pollable:
        ok = await _poll_event(event, followed, bot)
        if not ok:
            # A 5E error (rate limit, maintenance, network) usually affects the
            # whole API; stop this cycle instead of hammering the remaining
            # events.
            logger.warning("本轮停止继续抓取赛事结果，避免连续请求 5E")
            break
        await asyncio.sleep(EVENT_REQUEST_DELAY_SECONDS)


@scheduler.scheduled_job(
    "cron",
    minute="*/5",
    id="hltv",
    max_instances=1,
    coalesce=True,
    misfire_grace_time=240,
)
async def scheduled_update_events() -> None:
    await _run_update(force_discovery=False)


@updategame.handle()
async def manual_update_events() -> None:
    # /更新比赛 should pick up an edited follow list immediately.
    await _run_update(force_discovery=True)


async def _run_update(force_discovery: bool) -> None:
    async with update_lock:
        await refresh_watch_list(force=force_discovery)
        bot = _get_bot_or_none()
        if bot is None:
            logger.warning("没有已连接的 Bot，跳过本轮赛事结果抓取")
            return
        await _poll_watched_events(bot)
