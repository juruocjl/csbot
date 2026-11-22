"""CSBot automatic birthday wishes.

Every day at 23:50 (Asia/Shanghai) the plugin pre-generates tomorrow's wishes
with the same DSH chatbot used for normal chat (read-only group memory
snapshot), then publishes them as plain-text QQ group notices at 00:00. Every
send is idempotent per (group, member, year) so a restart never re-sends.
"""
from __future__ import annotations

import asyncio
from datetime import date, datetime, timedelta
import os
import re
import time
import uuid
from zoneinfo import ZoneInfo

from nonebot import get_bot, get_driver, get_plugin_config, logger, on_command, require
from nonebot.adapters.onebot.v11 import Bot, GroupMessageEvent, Message, MessageSegment
from nonebot.exception import ActionFailed
from nonebot.params import CommandArg
from nonebot.permission import SUPERUSER
from nonebot.plugin import PluginMetadata

require("nonebot_plugin_apscheduler")
from nonebot_plugin_apscheduler import scheduler

require("runtime_config")
from ..runtime_config import runtime_config

require("cs_ai")
from ..cs_ai import _ai_chat_link, ai_ask_main
from ..cs_ai.output_guard import QQ_GUARD_BLOCKED_TEXT, QQ_GUARD_FAILED_TEXT

from ai_runtime.store import state_store

from .config import Config
from .logic import (
    CLEAR_OVERRIDE,
    DISABLED_VALUE,
    BirthdayResolution,
    MemberBirthday,
    build_birthday_prompt,
    check_cooldown,
    format_cooldown_remaining,
    matches_birthday_on_date,
    parse_birthday_input,
    resolve_effective_birthday,
    sanitize_notice_text,
    validate_mmdd,
)

__plugin_meta__ = PluginMetadata(
    name="birthday",
    description="自动生日祝福：读取 QQ 资料生日、支持覆盖/关闭，0 点发群公告",
    usage="/生日 [@人]；/设置生日 <MM-DD|关闭|默认>；/清除生日",
    config=Config,
)

config = get_plugin_config(Config)

# Model stage direction is never publishable as a group notice.
BLOCKED_TEXTS = frozenset({QQ_GUARD_BLOCKED_TEXT, QQ_GUARD_FAILED_TEXT})

# Serializes the 23:50 pre-generation, the 00:00 dispatch and restart catch-up.
BIRTHDAY_LOCK = asyncio.Lock()


def now_shanghai() -> datetime:
    return datetime.now(ZoneInfo(config.cs_birthday_timezone))


def _background_disabled() -> bool:
    if not config.cs_birthday_enabled:
        return True
    return os.getenv("CS_DISABLE_BACKGROUND_JOBS", "").lower() in {"1", "true", "yes"}


async def _group_members(bot, group_id: str, *, no_cache: bool = False) -> list[dict]:
    members = await bot.get_group_member_list(group_id=int(group_id), no_cache=no_cache)
    return [item for item in (members or []) if isinstance(item, dict)]


def _display_name(member: dict, uid: str) -> str:
    return str(member.get("card") or member.get("nickname") or uid)


def _known_override(overrides: dict | None, uid: str) -> str | None:
    """Return a recognized override value, or ``None`` when the profile is needed."""
    raw = (overrides or {}).get(str(uid))
    if not isinstance(raw, str):
        return None
    value = raw.strip()
    if value.lower() == DISABLED_VALUE:
        return DISABLED_VALUE
    match = re.fullmatch(r"(\d{1,2})-(\d{1,2})", value)
    if not match:
        return None
    try:
        return validate_mmdd(int(match.group(1)), int(match.group(2)))
    except ValueError:
        return None


async def _profile_birthday(bot, uid: str) -> tuple[int | None, int | None, int | None]:
    try:
        info = await asyncio.wait_for(
            bot.get_stranger_info(user_id=int(uid), no_cache=True), timeout=8
        )
    except Exception as exc:  # one member's privacy/failure must not stop the scan
        logger.warning(f"读取 QQ {uid} 资料生日失败，按未公开处理: {type(exc).__name__}")
        return None, None, None
    if not isinstance(info, dict):
        return None, None, None
    return info.get("birthday_month"), info.get("birthday_day"), info.get("birthday_year")


async def _resolve_member(bot, uid: str, member: dict, overrides: dict) -> BirthdayResolution:
    if _known_override(overrides, uid) is not None:
        # An explicit override wins; skipping the stranger query saves API calls.
        return resolve_effective_birthday(uid, None, None, overrides)
    month, day, year = await _profile_birthday(bot, uid)
    return resolve_effective_birthday(uid, month, day, overrides, profile_year=year)


async def fetch_group_birthdays(bot, group_id: str, overrides: dict) -> list[MemberBirthday]:
    """Scan one group and return every member with an effective birthday."""
    gid = str(group_id)
    result: list[MemberBirthday] = []
    for item in await _group_members(bot, gid, no_cache=True):
        uid = str(item.get("user_id") or "").strip()
        if not uid or uid == str(bot.self_id):
            continue
        if item.get("is_robot") is True:
            continue
        resolution = await _resolve_member(bot, uid, item, overrides or {})
        if not resolution.mmdd:
            continue
        result.append(MemberBirthday(
            uid=uid,
            mmdd=resolution.mmdd,
            source=resolution.source,
            name=_display_name(item, uid),
            nickname=str(item.get("nickname") or ""),
            card=str(item.get("card") or ""),
            profile_year=resolution.profile_year,
        ))
    return result


async def _generate_wish(gid: str, member: MemberBirthday, target_date: date) -> tuple[str, str]:
    """Run the shared DSH pipeline and return ``(notice_text, run_id)``."""
    chat_id = str(uuid.uuid4())
    prompt = build_birthday_prompt(
        group_id=str(gid),
        user_id=member.uid,
        display_name=member.name,
        nickname=member.nickname,
        card=member.card,
        target_date=target_date,
        source=member.source,
        birthday_year=member.profile_year,
    )
    raw = await ai_ask_main(
        "", f"group_{gid}_?", None, prompt, chat_id=chat_id, channel="birthday"
    )
    text = sanitize_notice_text(raw, fallback_name=member.name, blocked_texts=BLOCKED_TEXTS)
    return text, chat_id


async def pregen_birthdays(bot, target_date: date | None = None) -> int:
    """23:50 job: pre-generate drafts for tomorrow (idempotent per year)."""
    if _background_disabled():
        return 0
    async with BIRTHDAY_LOCK:
        target = target_date or (now_shanghai().date() + timedelta(days=1))
        overrides = await runtime_config.get("birthday_overrides") or {}
        drafted = 0
        for group_id in config.cs_group_list:
            gid = str(group_id)
            try:
                members = await fetch_group_birthdays(bot, gid, overrides)
            except Exception as exc:
                logger.warning(f"群 {gid} 生日预生成扫描失败: {type(exc).__name__}")
                continue
            for member in members:
                if not matches_birthday_on_date(member.mmdd, target):
                    continue
                existing = state_store().get_birthday_wish(gid, member.uid, target.year)
                if existing and existing["status"] == "sent":
                    continue
                if (existing and existing["status"] == "drafted"
                        and existing["target_date"] == target.isoformat()):
                    try:
                        sanitize_notice_text(existing["draft_text"] or "",
                                             fallback_name=member.name,
                                             blocked_texts=BLOCKED_TEXTS)
                        continue
                    except ValueError:
                        pass
                try:
                    text, run_id = await _generate_wish(gid, member, target)
                except Exception as exc:
                    logger.warning(f"群 {gid} 成员 {member.uid} 生日祝福预生成失败: {type(exc).__name__}")
                    state_store().mark_birthday_failed(
                        gid, member.uid, target.year, target.isoformat(), member.source,
                        f"{type(exc).__name__}: {exc}"[:300],
                    )
                    continue
                state_store().upsert_birthday_draft(
                    gid, member.uid, target.year, target.isoformat(), member.source,
                    run_id, text,
                )
                drafted += 1
        return drafted


async def _send_notice(bot, gid: str, uid: str, text: str) -> str:
    """Publish a plain-text group notice, falling back to an @ message."""
    try:
        await bot.call_api(
            "_send_group_notice",
            group_id=str(gid),
            content=text,
            pinned=0,
            confirm_required=0,
            is_show_edit_card=0,
            tip_window_type=0,
        )
        return "group_notice"
    except ActionFailed:
        await bot.send_group_msg(
            group_id=int(gid), message=MessageSegment.at(uid) + "\n" + text
        )
        return "group_msg_fallback"


async def dispatch_birthdays_for_date(bot, target_date: date | None = None) -> int:
    """00:00 job: send today's wishes, strictly idempotent per (group, uid, year)."""
    if _background_disabled():
        return 0
    async with BIRTHDAY_LOCK:
        today = target_date or now_shanghai().date()
        # Re-read the overrides so a 23:50~00:00 edit/disable takes effect.
        overrides = await runtime_config.get("birthday_overrides") or {}
        sent = 0
        for group_id in config.cs_group_list:
            gid = str(group_id)
            try:
                members = [member for member in await fetch_group_birthdays(bot, gid, overrides)
                           if matches_birthday_on_date(member.mmdd, today)]
            except Exception as exc:
                logger.warning(f"群 {gid} 生日发送扫描失败: {type(exc).__name__}")
                continue
            members.sort(key=lambda member: int(member.uid) if member.uid.isdigit() else 10**18)
            for member in members:
                wish = state_store().get_birthday_wish(gid, member.uid, today.year)
                if wish and wish["status"] == "sent":
                    continue
                text = None
                run_id = wish.get("run_id") if wish else None
                if (wish and wish["status"] == "drafted"
                        and wish["target_date"] == today.isoformat()):
                    try:
                        text = sanitize_notice_text(wish["draft_text"] or "",
                                                    fallback_name=member.name,
                                                    blocked_texts=BLOCKED_TEXTS)
                    except ValueError:
                        text = None
                if not text:
                    try:
                        text, run_id = await _generate_wish(gid, member, today)
                    except Exception as exc:
                        logger.warning(f"群 {gid} 成员 {member.uid} 生日祝福生成失败: {type(exc).__name__}")
                        state_store().mark_birthday_failed(
                            gid, member.uid, today.year, today.isoformat(), member.source,
                            f"{type(exc).__name__}: {exc}"[:300], run_id,
                        )
                        continue
                try:
                    delivered_via = await _send_notice(bot, gid, member.uid, text)
                except Exception as exc:
                    logger.error(f"群 {gid} 成员 {member.uid} 生日祝福发送失败: {type(exc).__name__}")
                    state_store().mark_birthday_failed(
                        gid, member.uid, today.year, today.isoformat(), member.source,
                        f"{type(exc).__name__}: {exc}"[:300], run_id,
                    )
                    continue
                state_store().mark_birthday_sent(
                    gid, member.uid, today.year, today.isoformat(), member.source,
                    run_id, text, delivered_via,
                )
                sent += 1
        return sent


async def _is_superuser(bot, event) -> bool:
    try:
        return bool(await SUPERUSER(bot, event))
    except Exception:
        return False


def _parse_target(event, args: Message) -> tuple[str, str]:
    """Extract ``(target_uid, remaining_text)`` from a command's arguments."""
    uid = str(event.user_id)
    texts: list[str] = []
    for segment in args:
        if segment.type == "at":
            qq = str(segment.data.get("qq", "")).strip()
            if qq and qq != "all":
                uid = qq
        elif segment.type == "text":
            texts.append(segment.data.get("text", ""))
    raw = " ".join(texts).strip()
    tokens = raw.split()
    if uid == str(event.user_id) and len(tokens) > 1 and re.fullmatch(r"\d{5,12}", tokens[0]):
        uid = tokens[0]
        raw = " ".join(tokens[1:])
    return uid, raw


async def describe_birthday(bot, gid: str, actor_uid: str, target_uid: str) -> str:
    gid, actor_uid, target_uid = str(gid), str(actor_uid), str(target_uid)
    members = {str(item.get("user_id")): item for item in await _group_members(bot, gid)}
    member = members.get(target_uid)
    if member is None:
        return f"QQ {target_uid} 不在本群，无法查询。"
    overrides = await runtime_config.get("birthday_overrides") or {}
    resolution = await _resolve_member(bot, target_uid, member, overrides)
    name = _display_name(member, target_uid)
    if resolution.source == "override":
        body = f"生日 {resolution.mmdd}（来自覆盖设置）"
    elif resolution.source == "disabled":
        body = "已关闭自动生日祝福（none）"
    elif resolution.source == "profile":
        body = f"生日 {resolution.mmdd}（来自 QQ 资料）"
    else:
        body = "未公开生日，当前不会收到自动生日祝福"
    note = ""
    if target_uid == actor_uid:
        allowed, remaining = check_cooldown(
            state_store().get_birthday_cooldown(actor_uid), int(time.time()),
            config.cs_birthday_self_cooldown_seconds,
        )
        if not allowed:
            note = f"\n修改冷却中，还需 {format_cooldown_remaining(remaining)}。"
    owner = "你" if target_uid == actor_uid else f"群友 {name}（{target_uid}）"
    return f"{owner}：{body}。{note}".rstrip()


async def set_birthday_for(bot, event, actor_uid: str, target_uid: str,
                           raw_value: str) -> str:
    """Apply a self-service / superuser birthday edit without touching sent records."""
    actor_uid, target_uid = str(actor_uid), str(target_uid)
    try:
        value = parse_birthday_input(raw_value)
    except ValueError as exc:
        return f"用法有误：{exc}"
    superuser = await _is_superuser(bot, event)
    if target_uid != actor_uid and not superuser:
        return "只能修改自己的生日，发送 /设置生日 <MM-DD|关闭|默认> 即可。"
    if not superuser:
        allowed, remaining = check_cooldown(
            state_store().get_birthday_cooldown(actor_uid), int(time.time()),
            config.cs_birthday_self_cooldown_seconds,
        )
        if not allowed:
            return f"生日修改还在冷却中，请在 {format_cooldown_remaining(remaining)} 后再试。"
    overrides = dict(await runtime_config.get("birthday_overrides") or {})
    if value == CLEAR_OVERRIDE:
        overrides.pop(target_uid, None)
    else:
        overrides[target_uid] = value
    await runtime_config.set("birthday_overrides", overrides, updated_by=actor_uid)
    if not superuser:
        state_store().set_birthday_cooldown(actor_uid)
    removed = state_store().clear_unsent_birthday_draft(target_uid)
    if value == DISABLED_VALUE:
        action = f"已关闭 {target_uid} 的自动生日祝福"
    elif value == CLEAR_OVERRIDE:
        action = f"已清除 {target_uid} 的生日覆盖，恢复读取 QQ 资料"
    else:
        action = f"已把 {target_uid} 的生日设为 {value}"
    suffix = f"，并清理了 {removed} 条未发送草稿" if removed else ""
    return action + suffix + "。"


async def birthday_overview(bot, gid: str) -> str:
    gid = str(gid)
    overrides = await runtime_config.get("birthday_overrides") or {}
    entries: list[tuple[str, str, str, str]] = []
    disabled = 0
    for item in await _group_members(bot, gid):
        uid = str(item.get("user_id") or "").strip()
        if not uid or uid == str(bot.self_id) or item.get("is_robot") is True:
            continue
        resolution = await _resolve_member(bot, uid, item, overrides)
        if resolution.source == "disabled":
            disabled += 1
        if resolution.mmdd:
            entries.append((resolution.mmdd, uid, _display_name(item, uid), resolution.source))
    entries.sort()
    lines = [f"本群共有 {len(entries)} 位成员有生效生日，另有 {disabled} 人关闭了自动祝福。"]
    for mmdd, uid, name, source in entries:
        tag = "[覆盖]" if source == "override" else "[资料]"
        lines.append(f"{mmdd} {tag} {name}（{uid}）")
    return "\n".join(lines)


async def send_test_wish(bot, gid: str, actor_uid: str, target_uid: str,
                         dry: bool) -> str:
    gid, target_uid = str(gid), str(target_uid)
    members = {str(item.get("user_id")): item for item in await _group_members(bot, gid)}
    member = members.get(target_uid)
    if member is None:
        return f"QQ {target_uid} 不在本群，无法测试。"
    overrides = await runtime_config.get("birthday_overrides") or {}
    resolution = await _resolve_member(bot, target_uid, member, overrides)
    source = resolution.source if resolution.source in ("override", "profile") else "profile"
    wish_member = MemberBirthday(
        uid=target_uid,
        mmdd=resolution.mmdd or "01-01",
        source=source,
        name=_display_name(member, target_uid),
        nickname=str(member.get("nickname") or ""),
        card=str(member.get("card") or ""),
        profile_year=resolution.profile_year,
    )
    try:
        text, run_id = await _generate_wish(gid, wish_member, now_shanghai().date())
    except Exception as exc:
        return f"生日祝福测试生成失败：{type(exc).__name__}"
    link = _ai_chat_link(run_id)
    if dry:
        return f"生日祝福预览（未发送）：\n{text}\n\nAI 记录：{link}"
    try:
        delivered_via = await _send_notice(bot, gid, target_uid, text)
    except Exception as exc:
        return f"生日祝福测试发送失败：{type(exc).__name__}"
    return f"已在当前群发送测试生日祝福（{delivered_via}）。\nAI 记录：{link}"


birthday_query = on_command("生日", aliases={"查看生日"}, priority=10, block=True)
birthday_set = on_command("设置生日", priority=10, block=True)
birthday_clear = on_command("清除生日", aliases={"重置生日"}, priority=10, block=True)
birthday_list = on_command("生日列表", priority=10, block=True, permission=SUPERUSER)
birthday_test = on_command("测试生日祝福", priority=10, block=True, permission=SUPERUSER)


@birthday_query.handle()
async def birthday_query_function(bot: Bot, event: GroupMessageEvent,
                                  args: Message = CommandArg()):
    if str(event.user_id) == str(bot.self_id):
        return
    uid, raw = _parse_target(event, args)
    if uid == str(event.user_id) and re.fullmatch(r"\d{5,12}", raw or ""):
        uid = raw
    reply = await describe_birthday(bot, str(event.group_id), str(event.user_id), uid)
    await birthday_query.finish(reply)


@birthday_set.handle()
async def birthday_set_function(bot: Bot, event: GroupMessageEvent,
                                args: Message = CommandArg()):
    if str(event.user_id) == str(bot.self_id):
        return
    uid, raw = _parse_target(event, args)
    reply = await set_birthday_for(bot, event, str(event.user_id), uid, raw)
    await birthday_set.finish(reply)


@birthday_clear.handle()
async def birthday_clear_function(bot: Bot, event: GroupMessageEvent,
                                  args: Message = CommandArg()):
    if str(event.user_id) == str(bot.self_id):
        return
    uid, raw = _parse_target(event, args)
    if uid == str(event.user_id) and re.fullmatch(r"\d{5,12}", raw or ""):
        uid = raw
    reply = await set_birthday_for(bot, event, str(event.user_id), uid, "默认")
    await birthday_clear.finish(reply)


@birthday_list.handle()
async def birthday_list_function(bot: Bot, event: GroupMessageEvent):
    await birthday_list.finish(await birthday_overview(bot, str(event.group_id)))


@birthday_test.handle()
async def birthday_test_function(bot: Bot, event: GroupMessageEvent,
                                 args: Message = CommandArg()):
    uid, raw = _parse_target(event, args)
    if uid == str(event.user_id) and re.fullmatch(r"\d{5,12}", raw.split()[0] if raw else ""):
        parts = raw.split()
        uid, raw = parts[0], " ".join(parts[1:])
    dry = "--dry" in raw or "预览" in raw
    reply = await send_test_wish(bot, str(event.group_id), str(event.user_id),
                                 uid, dry)
    await birthday_test.finish(reply)


@scheduler.scheduled_job(
    "cron",
    hour=config.cs_birthday_pregen_hour,
    minute=config.cs_birthday_pregen_minute,
    timezone=ZoneInfo(config.cs_birthday_timezone),
    id="birthday_pregen",
    max_instances=1,
    coalesce=True,
    misfire_grace_time=600,
)
async def scheduled_birthday_pregen() -> None:
    if _background_disabled():
        return
    try:
        bot = get_bot()
    except Exception:
        return
    try:
        await pregen_birthdays(bot)
    except Exception as exc:
        logger.error(f"生日祝福预生成任务异常: {type(exc).__name__}")


@scheduler.scheduled_job(
    "cron",
    hour=0,
    minute=0,
    timezone=ZoneInfo(config.cs_birthday_timezone),
    id="birthday_dispatch",
    max_instances=1,
    coalesce=True,
    misfire_grace_time=300,
)
async def scheduled_birthday_dispatch() -> None:
    if _background_disabled():
        return
    try:
        bot = get_bot()
    except Exception:
        return
    try:
        await dispatch_birthdays_for_date(bot)
    except Exception as exc:
        logger.error(f"生日祝福发送任务异常: {type(exc).__name__}")


@get_driver().on_bot_connect
async def compensate_birthday_dispatch(bot: Bot) -> None:
    """Restart catch-up: only 00:00~00:15, and only unsent wishes."""
    if _background_disabled():
        return
    now = now_shanghai()
    if not (now.hour == 0 and 0 <= now.minute < 15):
        return

    async def run() -> None:
        try:
            await dispatch_birthdays_for_date(bot, now.date())
        except Exception as exc:
            logger.error(f"生日祝福重启补偿异常: {type(exc).__name__}")

    asyncio.create_task(run())
