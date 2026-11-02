"""QQ commands use the same authenticated pages as browser users."""
import re

from nonebot import get_plugin_config, logger, on_command, require
from nonebot.adapters.onebot.v11 import Bot, GroupMessageEvent, Message
from nonebot.params import CommandArg
from ..utils.message_recall import send_temporary_image

from .config import Config
from .get5e import FiveEApiError, fetch_events
from .store import schedule_store

config = get_plugin_config(Config)


follow_team = on_command("关注队伍", priority=10, block=True)
unfollow_team = on_command("取消关注队伍", priority=10, block=True)
mute_team = on_command("关闭赛果提醒", aliases={"关闭比赛提醒"}, priority=10, block=True)
my_follows = on_command("我的关注", priority=10, block=True)
search_team = on_command("搜索队伍", priority=10, block=True)
schedule = on_command("赛程", priority=10, block=True)
my_schedule = on_command("我的赛程", priority=10, block=True)


async def resolve(term, *, discover=True):
    choices = await schedule_store.search(term)
    # A provider ID can find a team outside the recent default event catalogue.
    if discover and not choices and re.fullmatch(r"(?:csgo_tm_|hltv_team_)\d+", term):
        from . import update_lock, _load_int, _save_int, POLL_COOLDOWN_KEY, RATE_LIMIT_COOLDOWN_SECONDS
        import time
        if await _load_int(POLL_COOLDOWN_KEY) <= int(time.time()):
            async with update_lock:
                try:
                    await schedule_store.save_events(await fetch_events(team_id=term))
                except FiveEApiError as exc:
                    if exc.status in (403, 429):
                        await _save_int(POLL_COOLDOWN_KEY, int(time.time()) + RATE_LIMIT_COOLDOWN_SECONDS)
                    raise
            choices = await schedule_store.search(term)
    exact = [t for t in choices if t["exact"]]
    if len(exact) == 1:
        return exact[0], ""
    if not choices:
        return None, "未找到队伍。请使用完整队名或 5E 队伍 ID；队伍目录会随赛事发现更新。"
    lines = ["请选择具体队伍，再使用 /关注队伍 队伍ID："]
    lines += [f"{t['name']} · {t['external_ids'][0]}" for t in choices]
    return None, "\n".join(lines)


@search_team.handle()
async def search_handler(args: Message = CommandArg()):
    choices = await schedule_store.search(args.extract_plain_text())
    await search_team.finish("\n".join(f"{t['name']} · {t['external_ids'][0]}" for t in choices) or "未找到队伍，请输入队名或队伍 ID")


@follow_team.handle()
async def follow_handler(event: GroupMessageEvent, args: Message = CommandArg()):
    try:
        team, hint = await resolve(args.extract_plain_text().strip())
    except FiveEApiError:
        await follow_team.finish("赛事数据源暂不可用，请稍后重试")
        return
    if team is None:
        await follow_team.finish(hint)
        return
    await schedule_store.follow(str(event.user_id), team["id"], str(event.group_id))
    from . import _rebuild_watch_list
    await _rebuild_watch_list()
    await follow_team.finish(f"已关注 {team['name']}，开赛和赛果都会在本群通知并 @你。已安排发现该队赛事，可用 /我的赛程 查看。")


@unfollow_team.handle()
async def unfollow_handler(event: GroupMessageEvent, args: Message = CommandArg()):
    team, hint = await resolve(args.extract_plain_text().strip(), discover=False)
    if team is None:
        await unfollow_team.finish(hint)
        return
    await schedule_store.unfollow(str(event.user_id), team["id"])
    await unfollow_team.finish(f"已取消关注 {team['name']}，并关闭该队在各群对你的提醒")


@mute_team.handle()
async def mute_handler(event: GroupMessageEvent, args: Message = CommandArg()):
    team, hint = await resolve(args.extract_plain_text().strip(), discover=False)
    if team is None:
        await mute_team.finish(hint)
        return
    await schedule_store.unfollow(str(event.user_id), team["id"], group_only=str(event.group_id))
    await mute_team.finish(f"已关闭 {team['name']} 在本群对你的提醒，个人赛程仍保留")


@my_follows.handle()
async def follows_handler(event: GroupMessageEvent):
    teams = await schedule_store.follows(str(event.user_id))
    await my_follows.finish("\n".join(f"{t['name']} · 提醒群：{', '.join(t['groups']) or '未开启'}" for t in teams) or "还没有关注队伍，使用 /关注队伍 队名 添加")


async def render_schedule(bot: Bot, event, args, matcher, personal=False):
    page_text = args.extract_plain_text().strip() or "1"
    if not page_text.isdigit() or not 1 <= int(page_text) <= 10000:
        await matcher.finish("用法：/赛程 [页码] 或 /我的赛程 [页码]")
        return
    server = require("cs_server")
    token = await server.db.get_bot_token(str(event.group_id))
    path = f"/schedule/user/{event.user_id}" if personal else "/schedule"
    try:
        image = await server.get_screenshot(f"{path}?page={int(page_text)}", token, width=1100)
    except Exception:
        logger.exception("赛程截图失败")
        image = None
    if not image:
        await matcher.finish("生成赛程图片失败，请稍后重试")
        return
    await send_temporary_image(bot, matcher.send, image, config.cs_schedule_auto_delete_seconds)
    await matcher.finish()


@schedule.handle()
async def schedule_handler(bot: Bot, event: GroupMessageEvent, args: Message = CommandArg()):
    await render_schedule(bot, event, args, schedule)


@my_schedule.handle()
async def my_schedule_handler(bot: Bot, event: GroupMessageEvent, args: Message = CommandArg()):
    await render_schedule(bot, event, args, my_schedule, personal=True)
