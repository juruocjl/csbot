"""CSBot automatic birthday wishes.

Every day at 23:50 (Asia/Shanghai) the plugin refreshes one group member list
per configured group (card names + incremental profile lookups for newcomers),
pre-generates tomorrow's wishes from the SQLite ``member_birthdays`` table with
the same DSH chatbot used for normal chat (read-only group memory snapshot),
then publishes them as plain-text QQ group notices at 00:00. Every send is
idempotent per (group, member, year) so a restart never re-sends.

Birthday data lives in ``member_birthdays`` as the single source of truth;
legacy ``runtime_config.birthday_overrides`` entries are migrated on connect.
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
    MemberBirthday,
    build_birthday_prompt,
    check_cooldown,
    days_until_birthday,
    format_cooldown_remaining,
    matches_birthday_on_date,
    parse_birthday_input,
    sanitize_notice_text,
    validate_mmdd,
)

__plugin_meta__ = PluginMetadata(
    name="birthday",
    description="自动生日祝福：生日落库 SQLite、增量同步 QQ 资料，0 点发群公告",
    usage="/生日 [@人]；/设置生日 <MM-DD|关闭|默认>；/清除生日；/同步生日 [全量]",
    config=Config,
)

config = get_plugin_config(Config)

# Model stage direction is never publishable as a group notice.
BLOCKED_TEXTS = frozenset({QQ_GUARD_BLOCKED_TEXT, QQ_GUARD_FAILED_TEXT})

# Serializes the 23:50 pre-generation, the 00:00 dispatch, startup sync and
# restart catch-up so two flows can never scan the same group at once.
BIRTHDAY_LOCK = asyncio.Lock()

# One in-flight startup sync at a time (on_bot_connect can fire on reconnects).
STARTUP_SYNC = {"task": None}


def now_shanghai() -> datetime:
    return datetime.now(ZoneInfo(config.cs_birthday_timezone))


def _background_disabled() -> bool:
    if not config.cs_birthday_enabled:
        return True
    return os.getenv("CS_DISABLE_BACKGROUND_JOBS", "").lower() in {"1", "true", "yes"}


async def _group_members(bot, group_id: str, *, no_cache: bool = False) -> list[dict]:
    members = await bot.get_group_member_list(group_id=int(group_id), no_cache=no_cache)
    return [item for item in (members or []) if isinstance(item, dict)]


def _trackable_members(bot, members: list[dict]) -> list[dict]:
    """Drop the bot itself and other robots from a group member list."""
    result: list[dict] = []
    for item in members or []:
        uid = str(item.get("user_id") or "").strip()
        if not uid or uid == str(bot.self_id) or item.get("is_robot") is True:
            continue
        result.append(item)
    return result


def _display_name(member: dict, uid: str) -> str:
    return str(member.get("card") or member.get("nickname") or uid)


async def _fetch_profile_birthday(bot, uid: str) -> tuple[str | None, int | None, bool]:
    """Query one QQ profile once.

    Returns ``(mmdd, year, ok)``: ``ok=False`` means the query itself failed (a
    later incremental sync may retry); ``ok=True`` with no ``mmdd`` means the
    profile was read and does not expose a birthday, so it is stored ``unknown``.
    """
    try:
        info = await asyncio.wait_for(
            bot.get_stranger_info(user_id=int(uid), no_cache=True), timeout=8
        )
    except Exception as exc:  # one member's privacy/failure must not stop the sync
        logger.warning(f"读取 QQ {uid} 资料生日失败，稍后重试: {type(exc).__name__}")
        return None, None, False
    if not isinstance(info, dict):
        return None, None, False
    try:
        mmdd = validate_mmdd(info.get("birthday_month"), info.get("birthday_day"))
    except ValueError:
        mmdd = None
    year = info.get("birthday_year")
    return mmdd, (year if type(year) is int and 1 <= year <= 9999 else None), True


async def _store_profile_birthday(bot, uid: str, name: str = "",
                                  *, force: bool = False) -> dict | None:
    """Fetch one profile and persist it; ``None`` when the query failed."""
    mmdd, year, ok = await _fetch_profile_birthday(bot, uid)
    if not ok:
        return None
    store = state_store()
    if mmdd:
        return store.upsert_member_birthday(
            uid, mmdd, "qq_profile", profile_year=year,
            name=name or None, force=force,
        )
    return store.upsert_member_birthday(uid, None, "unknown", name=name or None, force=force)


async def refresh_group_index(bot, gid: str, *, full: bool = False,
                              no_cache: bool = False) -> dict:
    """Refresh one group's card names and incrementally fill the birthday table.

    Only members missing from ``member_birthdays`` trigger a profile query; with
    ``full=True`` every row that is not ``manual``/``disabled`` is refreshed.
    """
    gid = str(gid)
    members = _trackable_members(bot, await _group_members(bot, gid, no_cache=no_cache))
    uids = [str(item.get("user_id") or "").strip() for item in members]
    index = state_store().list_member_birthdays(uids)
    stats = {"members": len(members), "added": 0, "refreshed": 0, "unknown": 0,
             "cached": 0, "manual": 0, "failed": 0, "names_updated": 0}
    names: dict[str, str] = {}
    for item in members:
        uid = str(item.get("user_id") or "").strip()
        name = _display_name(item, uid)
        row = index.get(uid)
        if row is None:
            try:
                row = await _store_profile_birthday(bot, uid, name)
            except Exception as exc:
                stats["failed"] += 1
                logger.warning(f"群 {gid} 成员 {uid} 生日入库失败: {type(exc).__name__}")
                continue
            if row is None:
                stats["failed"] += 1
                continue
            index[uid] = row
            stats["added"] += 1
            if row["source"] == "unknown":
                stats["unknown"] += 1
            continue
        if full and row["source"] not in ("manual", "disabled"):
            try:
                row = await _store_profile_birthday(bot, uid, name, force=True)
            except Exception as exc:
                stats["failed"] += 1
                logger.warning(f"群 {gid} 成员 {uid} 生日刷新失败: {type(exc).__name__}")
                continue
            if row is None:
                stats["failed"] += 1
                continue
            index[uid] = row
            if row["source"] == "unknown":
                stats["unknown"] += 1
            else:
                stats["refreshed"] += 1
            continue
        if row["source"] in ("manual", "disabled"):
            stats["manual"] += 1
        else:
            stats["cached"] += 1
        if name and row.get("name") != name:
            names[uid] = name
    if names:
        stats["names_updated"] = state_store().update_member_birthday_names(names)
        for uid, name in names.items():
            index[uid]["name"] = name
    return {"members": members, "index": index, "stats": stats}


def group_birthdays(index: dict, members: list[dict]) -> list[MemberBirthday]:
    """Build prompt-facing entries for group members with an enabled birthday."""
    result: list[MemberBirthday] = []
    for item in members:
        uid = str(item.get("user_id") or "").strip()
        row = (index or {}).get(uid)
        if not row or not row.get("mmdd"):
            continue
        result.append(MemberBirthday(
            uid=uid,
            mmdd=row["mmdd"],
            source="override" if row["source"] == "manual" else "profile",
            name=_display_name(item, uid),
            nickname=str(item.get("nickname") or ""),
            card=str(item.get("card") or ""),
            profile_year=row.get("profile_year"),
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
    """23:50 job: refresh names/newcomers once, then draft tomorrow's wishes."""
    if _background_disabled():
        return 0
    async with BIRTHDAY_LOCK:
        target = target_date or (now_shanghai().date() + timedelta(days=1))
        drafted = 0
        for group_id in config.cs_group_list:
            gid = str(group_id)
            try:
                snapshot = await refresh_group_index(bot, gid)
            except Exception as exc:
                logger.warning(f"群 {gid} 生日预生成扫描失败: {type(exc).__name__}")
                continue
            for member in group_birthdays(snapshot["index"], snapshot["members"]):
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
        sent = 0
        for group_id in config.cs_group_list:
            gid = str(group_id)
            try:
                snapshot = await refresh_group_index(bot, gid)
            except Exception as exc:
                logger.warning(f"群 {gid} 生日发送扫描失败: {type(exc).__name__}")
                continue
            members = [member for member in group_birthdays(snapshot["index"], snapshot["members"])
                       if matches_birthday_on_date(member.mmdd, today)]
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


def _text_args(args: Message) -> str:
    return " ".join(
        str(segment.data.get("text", "")) for segment in args if segment.type == "text"
    ).strip()


async def describe_birthday(bot, gid: str, actor_uid: str, target_uid: str) -> str:
    gid, actor_uid, target_uid = str(gid), str(actor_uid), str(target_uid)
    members = {str(item.get("user_id")): item for item in await _group_members(bot, gid)}
    member = members.get(target_uid)
    if member is None:
        return f"QQ {target_uid} 不在本群，无法查询。"
    name = _display_name(member, target_uid)
    row = state_store().get_member_birthday(target_uid)
    if row is None:
        row = await _store_profile_birthday(bot, target_uid, name)
    if row is None:
        return f"读取 QQ {target_uid} 资料失败，请稍后再试。"
    if row["source"] == "manual":
        body = f"生日 {row['mmdd']}（来自手动设置）"
    elif row["source"] == "disabled":
        body = "已关闭自动生日祝福（none）"
    elif row["source"] == "qq_profile":
        body = f"生日 {row['mmdd']}（来自 QQ 资料）"
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


async def _drop_legacy_override(uid: str) -> bool:
    """Best-effort removal of a stale ``birthday_overrides`` entry (double source)."""
    try:
        overrides = dict(await runtime_config.get("birthday_overrides") or {})
    except Exception:
        return False
    if uid not in overrides:
        return False
    overrides.pop(uid, None)
    try:
        await runtime_config.set("birthday_overrides", overrides, updated_by="birthday")
    except Exception as exc:
        logger.warning(f"清理旧生日覆盖 {uid} 失败: {type(exc).__name__}")
        return False
    return True


async def migrate_legacy_birthday_overrides() -> dict:
    """Import legacy runtime-config overrides into the member table once."""
    overrides = dict(await runtime_config.get("birthday_overrides") or {})
    if not overrides:
        return {"migrated": 0, "remaining": 0}
    migrated, remaining = state_store().migrate_birthday_overrides(overrides)
    if migrated:
        # Clear the legacy map so queries only ever use member_birthdays.
        await runtime_config.set("birthday_overrides", {}, updated_by="birthday-migration")
    if remaining:
        logger.warning(f"生日覆盖迁移丢弃 {len(remaining)} 条无法识别的旧配置")
    return {"migrated": migrated, "remaining": len(remaining)}


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
    store = state_store()
    await _drop_legacy_override(target_uid)
    if value == CLEAR_OVERRIDE:
        row = await _store_profile_birthday(bot, target_uid, "", force=True)
        if row is None:
            return f"读取 QQ {target_uid} 资料失败，请稍后再发送 /清除生日。"
        if row["source"] == "qq_profile":
            action = f"已清除 {target_uid} 的生日设置，按 QQ 资料恢复为 {row['mmdd']}"
        else:
            action = f"已清除 {target_uid} 的生日设置，QQ 资料未公开生日，后续不会自动祝福"
    elif value == DISABLED_VALUE:
        store.upsert_member_birthday(target_uid, None, "disabled", force=True)
        action = f"已关闭 {target_uid} 的自动生日祝福"
    else:
        store.upsert_member_birthday(target_uid, value, "manual", force=True)
        action = f"已把 {target_uid} 的生日设为 {value}"
    if not superuser:
        store.set_birthday_cooldown(actor_uid)
    removed = store.clear_unsent_birthday_draft(target_uid)
    suffix = f"，并清理了 {removed} 条未发送草稿" if removed else ""
    return action + suffix + "。"


async def birthday_overview(bot, gid: str) -> str:
    """``/生日列表``: read the DB, show sources and order by next occurrence."""
    gid = str(gid)
    try:
        snapshot = await refresh_group_index(bot, gid)
    except Exception as exc:
        logger.warning(f"群 {gid} 生日列表读取失败: {type(exc).__name__}")
        return "读取生日库失败，请稍后重试。"
    today = now_shanghai().date()
    entries: list[tuple[int, str, dict, dict]] = []
    disabled = unknown = 0
    for item in snapshot["members"]:
        uid = str(item.get("user_id") or "").strip()
        row = snapshot["index"].get(uid)
        if not row:
            continue
        if row["source"] == "disabled":
            disabled += 1
            continue
        if not row.get("mmdd"):
            unknown += 1
            continue
        days = days_until_birthday(row["mmdd"], today)
        entries.append((days if days is not None else 10**6, uid, row, item))
    entries.sort(key=lambda entry: (entry[0], entry[2]["mmdd"], entry[1]))
    lines = [
        f"本群共有 {len(entries)} 位成员有生效生日，"
        f"另有 {disabled} 人关闭了自动祝福，{unknown} 人未公开生日。"
    ]
    if not entries:
        lines.append("当前没有可用的生日记录。")
        return "\n".join(lines)
    lines.append("按离今天最近的排期排序：")
    for days, uid, row, item in entries:
        tag = "[手动]" if row["source"] == "manual" else "[资料]"
        when = "今天" if days == 0 else f"{days} 天后"
        lines.append(f"{row['mmdd']} {tag} {_display_name(item, uid)}（{uid}）· {when}")
    return "\n".join(lines)


async def sync_birthdays(bot, gid: str, *, full: bool = False) -> str:
    """``/同步生日``: incremental (default) or forced full profile refresh."""
    gid = str(gid)
    try:
        snapshot = await refresh_group_index(bot, gid, full=full, no_cache=True)
    except Exception as exc:
        logger.warning(f"群 {gid} 生日同步失败: {type(exc).__name__}")
        return f"生日库同步失败：{type(exc).__name__}"
    stats = snapshot["stats"]
    mode = "全量" if full else "增量"
    lines = [
        f"生日库{mode}同步完成：扫描 {stats['members']} 人，新增入库 {stats['added']} 人，"
        f"刷新资料 {stats['refreshed']} 人，未公开 {stats['unknown']} 人，"
        f"手动/关闭 {stats['manual']} 人，库内复用 {stats['cached']} 人，"
        f"失败 {stats['failed']} 人。"
    ]
    if stats["names_updated"]:
        lines.append(f"更新群名片 {stats['names_updated']} 人。")
    return "\n".join(lines)


async def send_test_wish(bot, gid: str, actor_uid: str, target_uid: str,
                         dry: bool) -> str:
    gid, target_uid = str(gid), str(target_uid)
    members = {str(item.get("user_id")): item for item in await _group_members(bot, gid)}
    member = members.get(target_uid)
    if member is None:
        return f"QQ {target_uid} 不在本群，无法测试。"
    name = _display_name(member, target_uid)
    row = state_store().get_member_birthday(target_uid)
    if row is None:
        row = await _store_profile_birthday(bot, target_uid, name)
    if row is None:
        row = {"mmdd": None, "source": "unknown", "profile_year": None}
    source = "override" if row["source"] == "manual" else "profile"
    wish_member = MemberBirthday(
        uid=target_uid,
        mmdd=row["mmdd"] or "01-01",
        source=source,
        name=name,
        nickname=str(member.get("nickname") or ""),
        card=str(member.get("card") or ""),
        profile_year=row.get("profile_year"),
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


async def _run_startup_birthday_sync(bot) -> None:
    try:
        await migrate_legacy_birthday_overrides()
    except Exception as exc:
        logger.warning(f"旧生日覆盖迁移失败: {type(exc).__name__}")
    async with BIRTHDAY_LOCK:
        for group_id in config.cs_group_list:
            gid = str(group_id)
            try:
                await refresh_group_index(bot, gid, no_cache=True)
            except Exception as exc:
                logger.warning(f"群 {gid} 生日启动同步失败: {type(exc).__name__}")


birthday_query = on_command("生日", aliases={"查看生日"}, priority=10, block=True)
birthday_set = on_command("设置生日", priority=10, block=True)
birthday_clear = on_command("清除生日", aliases={"重置生日"}, priority=10, block=True)
birthday_list = on_command("生日列表", priority=10, block=True, permission=SUPERUSER)
birthday_sync = on_command("同步生日", priority=10, block=True, permission=SUPERUSER)
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


@birthday_sync.handle()
async def birthday_sync_function(bot: Bot, event: GroupMessageEvent,
                                 args: Message = CommandArg()):
    full = _text_args(args).lower() in {"全量", "全部", "all", "force", "--all", "--force"}
    await birthday_sync.finish(await sync_birthdays(bot, str(event.group_id), full=full))


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
async def startup_birthday_sync(bot: Bot) -> None:
    """First connect: migrate legacy overrides and seed missing members once."""
    if _background_disabled():
        return
    task = STARTUP_SYNC.get("task")
    if task is not None and not task.done():
        return
    STARTUP_SYNC["task"] = asyncio.create_task(_run_startup_birthday_sync(bot))


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
