"""Archive actual OneBot sends, including calls made through matcher.send/finish.

The public call_api boundary is wrapped because NoneBot's pre-call hooks swallow
ordinary exceptions; an archive staging failure must stop the send. Post-send
indexing failure is retried from the durable outbox and never re-sends to QQ.
"""
from __future__ import annotations

import asyncio
import base64
import contextvars
import json
import time
from pathlib import Path

from nonebot import get_driver, logger
from nonebot.adapters.onebot.v11 import Bot, Message, MessageSegment
from nonebot.adapters.onebot.v11.exception import ActionFailed

from ai_runtime.store import state_store
from ai_runtime.image_archive import MARKER, validate_metadata

current_run = contextvars.ContextVar("csbot_outgoing_run", default=None)
_installed = False
_drain_lock = asyncio.Lock()


def freeze_message(value) -> list:
    message = Message(value) if isinstance(value, (str, MessageSegment)) else value
    frozen = []
    for raw in message:
        kind = raw.type if hasattr(raw, "type") else raw["type"]
        data = dict(raw.data if hasattr(raw, "data") else raw["data"])
        if kind == "image" and MARKER in data:
            frozen.append({"type": "image_meta", "data": validate_metadata(data[MARKER])})
            continue
        if kind in {"image", "record", "video"}:
            file = data.get("file")
            if isinstance(file, Path):
                file = str(file.resolve())
            if isinstance(file, bytes):
                if len(file)>32*1024**2:
                    raise ValueError("outgoing media exceeds archive byte limit")
                data["file"] = "base64://" + base64.b64encode(file).decode()
            elif isinstance(file, str) and (file.startswith("file://") or Path(file).is_absolute()):
                path = Path(file.removeprefix("file://"))
                if not path.is_file() or path.stat().st_size > 32 * 1024**2:
                    raise ValueError("outgoing media cannot be durably archived")
                data["file"] = "base64://" + base64.b64encode(path.read_bytes()).decode()
        if kind == "node" and "content" in data:
            data["content"] = freeze_message(data["content"])
        frozen.append({"type": kind, "data": data})
    # Fail before sending if an unhandled payload would not be recoverable.
    if len(json.dumps(frozen).encode())>48*1024**2:
        raise ValueError("outgoing message exceeds archive byte limit")
    return frozen


def wire_message(value):
    """Keep private archive descriptors out of platform requests, including nodes."""
    if isinstance(value, str):
        return value
    message = Message(value) if isinstance(value, MessageSegment) else value
    result = []
    for raw in message:
        kind = raw.type if hasattr(raw, 'type') else raw['type']
        data = dict(raw.data if hasattr(raw, 'data') else raw['data'])
        data.pop(MARKER, None)
        if kind == 'node' and 'content' in data:
            data['content'] = wire_message(data['content'])
        result.append({'type': kind, 'data': data})
    return result


def searchable_message(payload: list) -> Message:
    result = Message()
    for item in payload:
        if item["type"] == "node":
            data = item["data"]
            label = str(data.get("name", "转发节点"))[:100]
            result += MessageSegment.text(f"\n[机器人合并转发节点，展示名：{label}，非认证发言身份]\n")
            if "content" in data:
                result += searchable_message(data["content"])
            else:
                result += MessageSegment.text(f"[转发引用：{data.get('id', '内容未取得')}]\n")
        else:
            result += MessageSegment(item["type"], item["data"])
    return result


async def drain(bot: Bot):
    async with _drain_lock:
        await _drain(bot)


async def _drain(bot: Bot):
    from . import insert_message, db
    store = state_store()
    for row in store.rows("SELECT * FROM outbound WHERE status='sent' AND bot_id=? ORDER BY created_at LIMIT 50", (str(bot.self_id),)):
        try:
            sid = f"group_{row['group_id']}_{row['bot_id']}"
            await insert_message(bot, row["message_id"], sid, row["created_at"], searchable_message(json.loads(row["payload"])),strict_index=True)
            record_id = await db.get_id_by_mid(row["message_id"], row["group_id"])
            store.execute("UPDATE outbound SET status='indexed',record_id=?,payload='[]' WHERE id=?", (record_id, row["id"]))
        except Exception:
            logger.exception("outgoing archive indexing failed; retained for retry")


def install():
    global _installed
    if _installed:
        return
    _installed = True
    original = Bot.call_api

    async def archived_call(self, api: str, **data):
        gid = data.get("group_id")
        is_send = api in {"send_group_msg", "send_group_forward_msg", "send_msg"} and gid is not None
        if not is_send:
            result = await original(self, api, **data)
            if api == "delete_msg":
                state_store().execute("UPDATE outbound SET recalled_at=? WHERE bot_id=? AND message_id=?",
                                      (int(time.time()),str(self.self_id), data.get("message_id")))
            return result
        raw = data.get("messages", data.get("message", []))
        if isinstance(raw,str) and data.get("auto_escape"):
            raw=MessageSegment.text(raw)
        payload = freeze_message(raw)
        from . import snapshot_outgoing_ats
        await snapshot_outgoing_ats(self,str(gid),payload)
        store = state_store()
        key = store.stage(str(self.self_id), str(gid), api, payload, current_run.get())
        data = dict(data)
        field = 'messages' if 'messages' in data else 'message'
        data[field] = wire_message(data.get(field, []))
        try:
            result = await original(self, api, **data)
        except Exception as exc:
            # Transport failures may have delivered: do not label them confirmed failures.
            failed=isinstance(exc,ActionFailed)
            store.execute("UPDATE outbound SET status=?,error=?,payload=CASE WHEN ? THEN '[]' ELSE payload END WHERE id=?",
                          ("failed" if failed else "unknown",type(exc).__name__,failed,key))
            raise
        mid = result.get("message_id") if isinstance(result, dict) else None
        store.outbound_result(key, int(mid) if mid is not None else None)
        if mid is not None:
            await drain(self)
        return result

    Bot.call_api = archived_call

    @get_driver().on_bot_connect
    async def recover(bot):
        await drain(bot)

    from nonebot_plugin_apscheduler import scheduler

    @scheduler.scheduled_job("interval", seconds=30, id="outgoing_archive_retry", max_instances=1)
    async def retry():
        from nonebot import get_bots
        for bot in get_bots().values():
            await drain(bot)
