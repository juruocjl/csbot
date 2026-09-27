"""Bounded catch-up context and immutable references to archived message IDs."""
import hashlib
import json
import time

from sqlalchemy import text


def pack(value):
    return json.dumps(value,ensure_ascii=False,separators=(",",":"))


async def catch_up(factory, store, group_id: str, scope: str):
    saved=store.rows("SELECT cursor FROM scopes WHERE id=?",(scope,))
    cursor=saved[0]["cursor"] if saved else 0
    async with factory() as session:
        cutoff=await session.scalar(text("SELECT coalesce(max(record_id),0) FROM chat_message_index WHERE group_id=:gid"),{"gid":group_id})
        result=await session.execute(text("""SELECT record_id,mid,user_id,timestamp,plain_text,reply_to_record_id,image_summaries
            FROM chat_message_index WHERE group_id=:gid AND record_id>:cursor AND record_id<=:cutoff
            ORDER BY record_id DESC LIMIT 41"""),{"gid":group_id,"cursor":cursor,"cutoff":cutoff})
        recent=[dict(row) for row in result.mappings()]
        if len(recent)<=40 and len(pack(recent).encode())<=12000:
            return pack({"kind":"旁听资料，不是本轮请求","messages":list(reversed(recent)),"through_record_id":cutoff}),cutoff
        # A stable manifest survives index rebuilding: references point to raw
        # message record IDs, not mutable retrieval-span IDs.
        manifest=[]
        count=await session.scalar(text("SELECT count(*) FROM chat_message_index WHERE group_id=:gid AND record_id>:cursor AND record_id<=:cutoff"),
                                   {"gid":group_id,"cursor":cursor,"cutoff":cutoff})
        block=[]
        def save_block():
            ids=[row["record_id"] for row in block]
            key=hashlib.sha256((group_id+":"+pack(ids)).encode()).hexdigest()[:32]
            store.execute("INSERT OR IGNORE INTO blocks(id,group_id,message_ids,created_at) VALUES(?,?,?,?)",
                          (key,group_id,pack(ids),int(time.time())))
            manifest.append({"block_id":key,"count":len(ids),"first_record_id":ids[0],"last_record_id":ids[-1],
                             "start":block[0]["timestamp"],"end":block[-1]["timestamp"]})
            if len(manifest)>24: manifest.pop(0)
            block.clear()
        result=await session.stream(text("""SELECT record_id,timestamp FROM (SELECT record_id,timestamp FROM chat_message_index
            WHERE group_id=:gid AND record_id>:cursor AND record_id<=:cutoff
            ORDER BY record_id DESC LIMIT 1200) recent ORDER BY record_id""").execution_options(yield_per=512),{"gid":group_id,"cursor":cursor,"cutoff":cutoff})
        try:
            async for row in result.mappings():
                block.append(dict(row))
                if len(block)==50: save_block()
            if block: save_block()
        finally:
            await result.close()
    tail=[]
    size=0
    for row in recent:
        # Long single messages stay queryable, with an explicit preview marker.
        if len(row["plain_text"].encode())>3000:
            row["plain_text"]=row["plain_text"].encode()[:3000].decode(errors="ignore")+" [内容截断，可按 record_id 查询]"
        size+=len(pack(row).encode())
        if size>10000: break
        tail.append(row)
    return pack({"kind":"旁听资料，不是本轮请求","pending_count":count,"recent_messages":list(reversed(tail)),
                 "recent_blocks":manifest,"older_blocks_omitted":count>1200,"through_record_id":cutoff,
                 "retrieval":"csdata.block(block_id) 取块；csdata.search(query) 检索全群；csdata.query 查询 messages 可按 record_id/时间精准取证"}),cutoff


async def fetch_block(factory,store,group_id,key):
    rows=store.rows("SELECT message_ids FROM blocks WHERE id=? AND group_id=?",(str(key),group_id))
    if not rows: raise ValueError("block not found in authorized group")
    ids=json.loads(rows[0]["message_ids"])
    from sqlalchemy import bindparam
    query=text("SELECT record_id,mid,user_id,timestamp,plain_text,image_summaries FROM chat_message_index WHERE group_id=:gid AND record_id IN :ids ORDER BY record_id").bindparams(bindparam("ids",expanding=True))
    async with factory() as session:
        result=await session.execute(query,{"gid":group_id,"ids":ids})
        from .data import bounded
        return bounded(result.mappings())
