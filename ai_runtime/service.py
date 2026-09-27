"""NoneBot-facing orchestration; no second conversation-history implementation."""
import asyncio
from contextlib import ExitStack
import json
import os
from pathlib import Path
import re
import weakref
import time
import uuid
from datetime import datetime
from zoneinfo import ZoneInfo

import msgpack
from sqlalchemy import text

from .context import catch_up,fetch_block
from .data import DataBroker
from .media import media_cache
from .runner import run_dsh,scope_key
from .store import state_store
from .style import instruction
from .events import publish

LOCKS=weakref.WeakValueDictionary()


def register(chat_id,gid,uid,prompt,channel="qq",conversation="default"):
    store=state_store()
    scope=scope_key(channel,gid,uid,conversation)
    row=store.get_run(chat_id)
    if row:
        if (row["scope"],row["group_id"],row["user_id"]) != (scope,gid,uid):
            raise ValueError("run identity mismatch")
        return row
    if store.rows("SELECT count(*) AS n FROM runs WHERE status IN ('queued','running')")[0]["n"]>=8:
        raise ValueError("AI 队列已满，稍后再试")
    store.register_run(chat_id,scope,gid,uid,channel,prompt)
    from .events import prune
    prune()
    publish(chat_id,{'type':'request','text':prompt,'channel':channel})
    publish(chat_id,{'type':'status','status':'queued'})
    return store.get_run(chat_id)


async def authorized_image(factory,gid,image_id):
    value=str(image_id).removeprefix("img_")
    if not re.fullmatch(r"[a-f0-9]{12,64}",value):
        raise ValueError("invalid image identifier")
    short=value[:16]
    async with factory() as session:
        result=await session.execute(text('''SELECT g.data FROM groupmsg g JOIN chat_message_index m ON m.record_id=g.id
            WHERE m.group_id=:gid AND m.has_image=true AND m.plain_text LIKE :pattern LIMIT 50'''),
            {"gid":gid,"pattern":f"%[image:{short}]%"})
        hashes=set()
        for blob in result.scalars():
            for segment in msgpack.unpackb(blob,raw=False):
                if segment[0]=="imagev2" and str(segment[1]).startswith(value): hashes.add(str(segment[1]))
    if len(hashes)!=1:
        raise ValueError("image not found or identifier ambiguous in this group")
    return media_cache().info(hashes.pop())


async def ask(*,chat_id,gid,uid,prompt,persona,channel,conversation,model,endpoint,api_key,
              factory,chat_history,archive,guard=None,vision=False,thinking=False):
    store=state_store()
    row=register(chat_id,gid,uid,prompt,channel,conversation)
    if row["status"]=="completed": return row["response"]
    if row["status"] not in {"queued"}:
        raise ValueError("该请求已处理或中断，请查看记录后重新发送")
    scope=row["scope"]
    lock=LOCKS.setdefault(scope,asyncio.Lock())
    async with lock:
        # Recheck after waiting: a duplicate HTTP/task dispatch must not run twice.
        if store.get_run(chat_id)["status"]!="queued":
            raise ValueError("request already consumed")
        try:
            await archive(chat_id,"user",prompt,None,None,False)
            broker=DataBroker(factory,gid,Path(os.environ["CS_AI_GAME_DB"]) if os.getenv("CS_AI_GAME_DB") else None)
            with ExitStack() as leases:
                pinned=set()
                async def dispatch(request):
                    method=request.get("method")
                    if method=="search":
                        allowed={"users","time_start","time_end","strict_time_end","limit"}
                        filters=request.get("filters",{})
                        if set(filters)-allowed: raise ValueError("unknown search filter")
                        return json.loads(await chat_history.search_chat_spans(gid,str(request.get("query",""))[:2000],**filters))
                    if method=="block": return await fetch_block(factory,store,gid,request["id"])
                    if method=="image":
                        image=await authorized_image(factory,gid,request["id"])
                        digest=image["image_id"]
                        if image["full_path"] and digest not in pinned:
                            if len(pinned)>=8: raise ValueError("image budget exhausted (8 per turn)")
                            leases.enter_context(media_cache().lease(digest)); pinned.add(digest)
                        return image
                    return await broker.dispatch(request)
                if channel=="qq":
                    context,cutoff=await catch_up(factory,store,gid,scope)
                else:
                    context,cutoff="网页个人对话；需要群资料时主动检索，不把网页内容写入群记忆。" if channel=="web" else "自动报告任务；不提炼长期记忆。",0
                context="本轮表达风格："+instruction(persona)+"\n服务端确认的本轮身份："+json.dumps({"channel":channel,"group_id":gid,"user_id":uid,"persona_this_turn":persona,
                    "current_time":datetime.now(ZoneInfo("Asia/Shanghai")).isoformat()},ensure_ascii=False)+"\n"+context
                store.execute("UPDATE runs SET cutoff=? WHERE id=?",(cutoff,chat_id))
                async def started():
                    store.execute("UPDATE runs SET status='running' WHERE id=?",(chat_id,))
                    publish(chat_id,{'type':'status','status':'running','thinking_enabled':thinking})
                async def images(values):
                    if len(store.rows('SELECT id FROM run_images WHERE run_id=?',(chat_id,)))+len(values)>4:
                        raise ValueError('每轮最多提交 4 张图片')
                    for value in values:
                        digest=media_cache().put(Path(value['path']).read_bytes(),private=True)
                        image_id=uuid.uuid4().hex
                        store.execute('INSERT INTO run_images(id,run_id,digest,caption) VALUES(?,?,?,?)',
                                      (image_id,chat_id,digest,value['caption']))
                        publish(chat_id,{'type':'image','id':image_id,'caption':value['caption']})
                result=await run_dsh(scope=scope,text=f"QQ用户 {uid}：{prompt}" if uid else prompt,
                    context=context,model=model,endpoint=endpoint,api_key=api_key,dispatch=dispatch,
                    remember=channel in {"qq","web"},vision=vision,thinking=thinking,guard=guard if channel!='web' else None,
                    run_id=chat_id,on_event=lambda event:publish(chat_id,event),on_images=images,on_started=started)
                output="\n\n".join(item["content"] for item in result["messages"])
                if not output.strip(): raise RuntimeError("model returned no public answer")
                # Legacy record compatibility; detailed native events use scoped SSE access.
                for event in result["tools"]:
                    await archive(chat_id,"tool",json.dumps(event,ensure_ascii=False)[:24000],None,None,False)
                await archive(chat_id,"assistant",output,None,None,True)
                store.execute("UPDATE runs SET status='completed',response=? WHERE id=?",(output,chat_id))
                publish(chat_id,{'type':'final','text':output})
                publish(chat_id,{'type':'status','status':'completed'})
                if channel=="qq":
                    store.execute("INSERT INTO scopes(id,cursor) VALUES(?,?) ON CONFLICT(id) DO UPDATE SET cursor=max(cursor,excluded.cursor)",(scope,cutoff))
                return output
        except BaseException as exc:
            store.execute("UPDATE runs SET status='interrupted',error=? WHERE id=?",(type(exc).__name__,chat_id))
            publish(chat_id,{'type':'status','status':'interrupted'})
            await archive(chat_id,"assistant","这次没能完成查询，记录已保留，可以稍后重试。",None,None,True)
            raise
