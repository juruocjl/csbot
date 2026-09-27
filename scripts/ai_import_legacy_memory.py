"""Offline, idempotent import/audit of legacy manual group memory into native Mneme.

JSON [{gid,mem}] on stdin, normally exported from ai_mem. No credentials/models
are required. Dry run by default. --apply requires the backend to be stopped and
creates a private backup before any write. Original PostgreSQL rows are untouched.
"""
import argparse
import asyncio
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import sqlite3
import sys
import time
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from ai_runtime.runner import run_dsh,scope_key


def plan(rows,state):
    if not isinstance(rows,list):raise ValueError('expected memory rows')
    seen=set();items=[];excluded=0
    db_path=state/'state.sqlite3'
    with sqlite3.connect(db_path.resolve().as_uri()+'?mode=ro',uri=True) as db:
        for row in rows:
            gid=row.get('gid');body=row.get('mem') or ''
            if not isinstance(gid,str) or not isinstance(body,str):raise ValueError('invalid source row')
            if gid in seen:raise ValueError('duplicate source key')
            seen.add(gid)
            if re.fullmatch(r'\d{1,20}:(qa|report)',gid):excluded+=1;continue
            if not re.fullmatch(r'\d{1,20}',gid):raise ValueError('unconfirmed source ownership')
            manual=body.split('[自动日报周报知识]',1)[0].strip()
            if len(manual)>64000:raise ValueError('manual memory exceeds 64000 characters; explicit split required')
            parts=[manual[i:i+4000] for i in range(0,len(manual),4000)]
            scope=scope_key('qq',gid,'');root=state/'scopes'/scope
            candidates=list((root/'memory').glob('*.db'))
            if len(candidates)>1:raise ValueError('ambiguous Mneme database')
            stored=[]
            if candidates:
                with sqlite3.connect(candidates[0].resolve().as_uri()+'?mode=ro',uri=True) as memory:
                    stored=memory.execute("SELECT title,content,archived FROM memories WHERE source='legacy-ai-mem-explicit'").fetchall()
            marker=db.execute("SELECT 1 FROM migrations WHERE scope=? AND name='legacy-manual-memory'",(scope,)).fetchone()
            titles=[f'旧群记忆迁移 {i+1}' for i in range(len(parts))]
            matched=all(any(title==t and content==p and not archived for title,content,archived in stored) for t,p in zip(titles,parts))
            status='empty' if not parts else 'already_present' if matched else 'conflict' if marker or stored else 'pending'
            items.append({'gid':gid,'scope':scope,'source_sha256':hashlib.sha256(manual.encode()).hexdigest(),'chars':len(manual),'status':status,'parts':parts,'titles':titles})
    return items,excluded


def public(items,excluded):
    return {'groups':[{k:v for k,v in item.items() if k not in {'parts','titles'}} for item in items],'excluded_qa_report_rows':excluded}


async def migrate(rows,state,backup):
    if backup==state or backup.is_relative_to(state):raise ValueError('backup must be outside the runtime state directory')
    # Share the backend's lifecycle lock without creating StateStore or changing runs.
    with open(state/'state.lock','a') as owner:
        try:fcntl.flock(owner,fcntl.LOCK_EX|fcntl.LOCK_NB)
        except BlockingIOError:raise ValueError('stop csbot.service before applying the migration') from None
        items,excluded=plan(rows,state)
        if any(i['status']=='conflict' for i in items):raise ValueError('existing migration differs or was forgotten; refusing overwrite/resurrection')
        backup.mkdir(parents=True,mode=0o700,exist_ok=False);backup.chmod(0o700)
        (backup/'source.json').write_text(json.dumps(rows,ensure_ascii=False));(backup/'source.json').chmod(0o600)
        with sqlite3.connect(state/'state.sqlite3') as src,sqlite3.connect(backup/'state.sqlite3') as dst:src.backup(dst)
        for item in items:
            root=state/'scopes'/item['scope']
            if root.exists():shutil.copytree(root,backup/'scopes'/item['scope'])
        (backup/'before.json').write_text(json.dumps(public(items,excluded),ensure_ascii=False,indent=2))
        async def deny(_):raise ValueError('migration has no data access')
        for item in items:
            if item['status']=='empty':continue
            # Native memory_save/search only, no user message, model call or distillation.
            result=await run_dsh(scope=item['scope'],text='',context='',model='migration-only',
                endpoint='http://127.0.0.1:1',api_key='not-used',dispatch=deny,state_root=state,
                remember=False,memory_only=True,memory_check_titles=item['titles'],
                legacy_memory=''.join(item['parts']) if item['status']=='pending' else '')
            if result['verified']!=len(item['parts']):raise ValueError('native search verification failed')
            if item['status']=='pending':item['status']='imported'
        after,_=plan(rows,state)
        if any(i['status'] not in {'already_present','empty'} for i in after):raise ValueError('content verification failed')
        with sqlite3.connect(state/'state.sqlite3') as db:
            db.execute('CREATE TABLE IF NOT EXISTS memory_import_receipts(source_key TEXT PRIMARY KEY,source_sha256 TEXT NOT NULL,scope TEXT NOT NULL,verified_at INTEGER NOT NULL)')
            for item in items:
                if item['status']=='empty':continue
                db.execute("INSERT OR IGNORE INTO migrations VALUES(?,'legacy-manual-memory',?)",(item['scope'],int(time.time())))
                db.execute('INSERT INTO memory_import_receipts VALUES(?,?,?,?) ON CONFLICT(source_key) DO UPDATE SET source_sha256=excluded.source_sha256,scope=excluded.scope,verified_at=excluded.verified_at',
                           (item['gid'],item['source_sha256'],item['scope'],int(time.time())))
        result=public(items,excluded);result['native_search_verified']=True
        (backup/'receipt.json').write_text(json.dumps(result,ensure_ascii=False,indent=2))
        return result


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--state-dir',type=Path,required=True)
    parser.add_argument('--apply',action='store_true')
    parser.add_argument('--backup-dir',type=Path)
    args=parser.parse_args();rows=json.load(sys.stdin)
    if args.apply:
        if not args.backup_dir:parser.error('--apply requires --backup-dir')
        result=asyncio.run(migrate(rows,args.state_dir.resolve(),args.backup_dir.resolve()))
    else:result=public(*plan(rows,args.state_dir.resolve()))
    print(json.dumps(result,ensure_ascii=False,indent=2))

if __name__=='__main__':main()
