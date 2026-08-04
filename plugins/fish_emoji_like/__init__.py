from nonebot import logger, on_message
from nonebot.adapters.onebot.v11 import Bot, MessageEvent
from nonebot.plugin import PluginMetadata


__plugin_meta__ = PluginMetadata(
    name="fish_emoji_like",
    description="消息中包含鱼相关关键词时添加 285 号表情。",
    usage="",
)


TRIGGER_KEYWORDS = ("🐟", "yu", "玉", "鱼")
EMOJI_ID = 285


def contains_trigger_keyword(event: MessageEvent) -> bool:
    text = event.get_message().extract_plain_text().lower()
    return any(keyword in text for keyword in TRIGGER_KEYWORDS)


fish_emoji_like = on_message(priority=1, block=False)


@fish_emoji_like.handle()
async def fish_emoji_like_handle(bot: Bot, event: MessageEvent) -> None:
    if not contains_trigger_keyword(event):
        return

    try:
        await bot.call_api(
            "set_msg_emoji_like",
            message_id=event.message_id,
            emoji_id=EMOJI_ID,
        )
    except Exception as exc:
        logger.warning(
            f"fish_emoji_like failed for message {event.message_id}: {exc}"
        )
