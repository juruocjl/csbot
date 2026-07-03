from nonebot import get_plugin_config, logger
from nonebot.adapters import Event
from nonebot.exception import IgnoredException
from nonebot.message import event_preprocessor
from nonebot.plugin import PluginMetadata

from .config import Config


__plugin_meta__ = PluginMetadata(
    name="blocker",
    description="Block events outside CS_EVENT_GROUP_LIST before matchers run.",
    usage="",
    config=Config,
)

config = get_plugin_config(Config)


def _allowed_groups() -> set[str]:
    return {str(group_id) for group_id in config.cs_event_group_list}


@event_preprocessor
async def block_non_whitelisted_events(event: Event) -> None:
    allowed_groups = _allowed_groups()
    group_id = getattr(event, "group_id", None)
    if group_id is not None and str(group_id) in allowed_groups:
        return

    logger.debug(f"Ignore event outside CS_EVENT_GROUP_LIST: {event.get_event_name()}")
    raise IgnoredException("event not in CS_EVENT_GROUP_LIST")
