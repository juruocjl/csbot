"""Send an archived image, then recall only its successful OneBot receipt."""
from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Mapping
from typing import Any

from nonebot import logger
from nonebot.adapters.onebot.v11 import Bot, MessageSegment

from ai_runtime.image_archive import image_segment


_delete_tasks: set[asyncio.Task[None]] = set()


async def _delete_message_later(bot: Bot, message_id: int, delay_seconds: int) -> None:
    await asyncio.sleep(delay_seconds)
    try:
        await bot.delete_msg(message_id=message_id)
    except Exception:
        logger.exception(f"定时撤回图片失败 message_id={message_id}")


async def send_temporary_image(
    bot: Bot,
    send: Callable[[MessageSegment], Awaitable[Any]],
    image: bytes,
    delay_seconds: int,
) -> Any:
    receipt = await send(image_segment(image))
    if delay_seconds <= 0:
        return receipt
    try:
        if not isinstance(receipt, Mapping):
            raise ValueError("missing send receipt")
        raw_id = receipt["message_id"]
        if isinstance(raw_id, bool) or not isinstance(raw_id, (str, int)):
            raise ValueError("invalid message id")
        message_id = int(raw_id)
    except (KeyError, TypeError, ValueError):
        logger.warning("发图回执缺少有效 message_id，跳过定时撤回")
        return receipt
    task = asyncio.create_task(_delete_message_later(bot, message_id, delay_seconds))
    _delete_tasks.add(task)
    task.add_done_callback(_delete_tasks.discard)
    return receipt
