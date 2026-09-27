"""Bounded UI projection of native DSH events, never an agent loop or memory store."""
import asyncio
import json
import time
import weakref
from .store import state_store

SIGNALS=weakref.WeakValueDictionary()
MAX_BYTES=8*1024**2

def signal(run_id):
    event=SIGNALS.get(run_id)
    if event is None:event=asyncio.Event();SIGNALS[run_id]=event
    return event

def publish(run_id,payload):
    store=state_store()
    encoded=json.dumps(payload,ensure_ascii=False)
    # Per-run bounded diagnostics; terminal state and final answer always survive.
    if payload.get('type') not in {'status','final','image','supplement'}:
        used=store.rows('SELECT coalesce(sum(bytes),0) n FROM run_events WHERE run_id=?',(run_id,))[0]['n']
        if used+len(encoded.encode())>MAX_BYTES:
            if not store.rows("SELECT 1 FROM run_events WHERE run_id=? AND payload LIKE '%\"trace_limit\"%' LIMIT 1",(run_id,)):
                publish(run_id,{'type':'status','status':'trace_limit','message':'过程记录已达到本轮 8 MiB 上限，最终回答仍会保存。'})
            return
    store.execute('INSERT INTO run_events(run_id,payload,created_at,bytes) VALUES(?,?,?,?)',(run_id,encoded,time.time(),len(encoded.encode())))
    if event:=SIGNALS.get(run_id):event.set()

def read(run_id,after=0):
    return [{'id':r['seq'],'event':json.loads(r['payload'])} for r in state_store().rows(
        'SELECT seq,payload FROM run_events WHERE run_id=? AND seq>? ORDER BY seq LIMIT 100',(run_id,after))]

def prune():
    store=state_store()
    total=store.rows('SELECT coalesce(sum(bytes),0) n FROM run_events')[0]['n']
    if total<=256*1024**2:return
    for row in store.rows("SELECT e.run_id,sum(e.bytes) n FROM run_events e JOIN runs r ON r.id=e.run_id WHERE r.status IN ('completed','interrupted') GROUP BY e.run_id ORDER BY min(e.seq)"):
        if total<=192*1024**2:break
        store.execute('DELETE FROM run_events WHERE run_id=?',(row['run_id'],));total-=row['n']
