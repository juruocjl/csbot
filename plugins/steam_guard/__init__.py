import asyncio
import time

from nonebot import get_plugin_config
from nonebot import logger
from nonebot import on_message
from nonebot import require
from nonebot.adapters.onebot.v11 import Bot, GroupMessageEvent, MessageSegment
from nonebot.plugin import PluginMetadata

require("utils")
from ..utils import local_storage

require("cs_db_val")
from ..cs_db_val import db as db_val

require("cs_server")
from ..cs_server import _fetch_steam_status_payload

from .config import Config


__plugin_meta__ = PluginMetadata(
    name="steam_guard",
    description="",
    usage="",
    config=Config,
)

config = get_plugin_config(Config)

steam_guard = on_message(priority=55, block=False)

_monitor_cache_lock = asyncio.Lock()
_monitor_cache_steamids: set[str] = set()
_monitor_cache_ts: float = 0.0


def _is_group_enabled(group_id: int) -> bool:
    if not config.cs_steam_guard_enable_group_list:
        return True
    return group_id in config.cs_steam_guard_enable_group_list


def _build_warn_text(now_count: int) -> str:
    text = f"你还未完成 Steam 绑定校验，请先绑定 Steam 账号（{now_count}/{config.cs_steam_guard_ban_after}），不绑定会被禁言"
    target_user = str(config.cs_steam_guard_target_user).strip()
    if target_user:
        text += f"，并添加指定用户：{target_user}"
    return text


async def _get_monitor_steamids() -> set[str]:
    global _monitor_cache_ts, _monitor_cache_steamids
    now_ts = time.time()
    if now_ts - _monitor_cache_ts <= config.cs_steam_guard_monitor_cache_seconds and _monitor_cache_steamids:
        return set(_monitor_cache_steamids)

    async with _monitor_cache_lock:
        now_ts = time.time()
        if now_ts - _monitor_cache_ts <= config.cs_steam_guard_monitor_cache_seconds and _monitor_cache_steamids:
            return set(_monitor_cache_steamids)

        payload = await _fetch_steam_status_payload()
        raw_data = payload.get("data", [])
        if not isinstance(raw_data, list):
            _monitor_cache_steamids = set()
            _monitor_cache_ts = now_ts
            return set()

        _monitor_cache_steamids = {
            str(item.get("steam_id", ""))
            for item in raw_data
            if isinstance(item, dict) and item.get("steam_id")
        }
        _monitor_cache_ts = now_ts
        return set(_monitor_cache_steamids)


async def _warn_or_ban(bot: Bot, event: GroupMessageEvent) -> None:
    gid = str(event.group_id)
    uid = event.get_user_id()

    last_warn_key = f"steam_guard_last_warn_{gid}_{uid}"
    warn_count_key = f"steam_guard_warn_count_{gid}_{uid}"

    now_ts = int(time.time())
    last_warn_ts = int(await local_storage.get(last_warn_key, "0"))
    if now_ts - last_warn_ts < config.cs_steam_guard_warn_cooldown_seconds:
        return

    now_count = int(await local_storage.get(warn_count_key, "0")) + 1
    await local_storage.set(last_warn_key, str(now_ts))
    await local_storage.set(warn_count_key, str(now_count))

    await bot.send_group_msg(
        group_id=event.group_id,
        message=MessageSegment.at(uid) + _build_warn_text(now_count),
    )

    if now_count >= config.cs_steam_guard_ban_after and config.cs_steam_guard_ban_duration > 0:
        try:
            await bot.set_group_ban(
                group_id=event.group_id,
                user_id=int(uid),
                duration=config.cs_steam_guard_ban_duration,
            )
        except Exception as exc:
            logger.warning(f"steam_guard set_group_ban failed group={gid} uid={uid} err={exc}")


@steam_guard.handle()
async def steam_guard_handle(bot: Bot, event: GroupMessageEvent) -> None:
    if not _is_group_enabled(event.group_id):
        return

    uid = event.get_user_id()
    gid = str(event.group_id)

    steam_id = await db_val.get_steamid(uid)
    if not steam_id:
        await _warn_or_ban(bot, event)
        return

    try:
        monitor_steamids = await _get_monitor_steamids()
    except Exception as exc:
        logger.warning(f"steam_guard fetch monitor failed: {exc}")
        return

    if steam_id not in monitor_steamids:
        await _warn_or_ban(bot, event)
        return

    await local_storage.set(f"steam_guard_warn_count_{gid}_{uid}", "0")
    await local_storage.set(f"steam_guard_last_warn_{gid}_{uid}", "0")
