from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from zoneinfo import ZoneInfoNotFoundError

from nonebot import get_plugin_config, logger, on_command
from nonebot.adapters.onebot.v11 import Bot, Message
from nonebot.params import CommandArg
from nonebot.plugin import PluginMetadata

from .config import Config
from .logic import UnknownLocationError, format_location_time, select_locations


__plugin_meta__ = PluginMetadata(
    name="time_location",
    description="查询配置地点的当前时间",
    usage="/时间 [地点]",
    config=Config,
)

config = get_plugin_config(Config)

time_location = on_command(
    "时间",
    aliases={"时间地点", "time"},
    priority=10,
    block=True,
)

_delete_tasks: set[asyncio.Task[None]] = set()


async def _delete_message_later(bot: Bot, message_id: int, delay_seconds: int) -> None:
    await asyncio.sleep(delay_seconds)
    try:
        await bot.delete_msg(message_id=message_id)
    except Exception:
        logger.exception(f"failed to delete time message message_id={message_id}")


def _schedule_auto_delete(bot: Bot, sent_message: object) -> None:
    delay_seconds = config.cs_time_auto_delete_seconds
    if delay_seconds <= 0:
        return

    try:
        message_id = int(sent_message["message_id"])  # type: ignore[index]
    except (KeyError, TypeError, ValueError):
        logger.warning(f"time send result has no message_id: {sent_message}")
        return

    task = asyncio.create_task(
        _delete_message_later(bot, message_id, delay_seconds),
    )
    _delete_tasks.add(task)
    task.add_done_callback(_delete_tasks.discard)


async def _send_temporary(bot: Bot, message: str) -> None:
    sent_message = await time_location.send(message)
    _schedule_auto_delete(bot, sent_message)


@time_location.handle()
async def show_time(bot: Bot, args: Message = CommandArg()) -> None:
    query = args.extract_plain_text().strip()
    locations = config.cs_time_locations
    if not locations:
        await _send_temporary(bot, "尚未配置可查询的地点。")
        return

    try:
        selected = select_locations(locations, query)
    except UnknownLocationError:
        available = "、".join(locations)
        await _send_temporary(bot, f"未找到地点“{query}”。可用地点：{available}")
        return

    now = datetime.now(timezone.utc)
    lines: list[str] = []
    invalid: list[str] = []
    for name, zone_name in selected:
        try:
            lines.append(format_location_time(name, zone_name, now))
        except ZoneInfoNotFoundError:
            invalid.append(f"{name}={zone_name}")

    if invalid:
        logger.error(f"invalid configured IANA time zones: {', '.join(invalid)}")
    if not lines:
        await _send_temporary(bot, "时区配置无效，请联系管理员检查 IANA 时区名称。")
        return

    await _send_temporary(bot, "\n".join(lines))
