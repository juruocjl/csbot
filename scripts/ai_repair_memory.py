"""Apply an explicit, reviewed memory correction through native Mneme tools.

Read a JSON plan {gid, forget:[{id,sha256}], save:[{type,title,content,importance,source}]}
on stdin. Dry run by default. Apply requires the backend stopped, a fresh backup
directory, exact old-content hashes, and unique correction titles. No model calls.
"""
import argparse
import asyncio
import fcntl
import hashlib
import json
from pathlib import Path
import re
import shutil
import sqlite3
import sys
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from ai_runtime.runner import run_dsh,scope_key


def fingerprint(row):
    return hashlib.sha256(json.dumps({k:row[k] for k in ('id','type','title','content')},ensure_ascii=False,sort_keys=True).encode()).hexdigest()


def inspect(plan,state):
    if not isinstance(plan,dict) or set(plan)!={'gid','forget','save'} or not re.fullmatch(r'\d{1,20}',str(plan['gid'])):
        raise ValueError('explicit group correction plan required')
    if not isinstance(plan['save'],list) or not isinstance(plan['forget'],list) or not 1<=len(plan['save'])<=10 or len(plan['forget'])>10:
        raise ValueError('bounded correction required')
    scope=scope_key('qq',str(plan['gid']),'');root=state/'scopes'/scope
    for p in (state/'scopes',root,root/'memory',root/'memory/memory.db'):
        if p.is_symlink():raise ValueError('symlink forbidden')
    actions=[];seen=set()
    with sqlite3.connect((root/'memory/memory.db').resolve().as_uri()+'?mode=ro',uri=True) as db:
        db.row_factory=sqlite3.Row
        for item in plan['save']:
            if set(item)!={'type','title','content','importance','source'} or item['type'] not in {'project','history','decision','preference'}:
                raise ValueError('invalid correction')
            if not all(isinstance(item[k],str) and 0<len(item[k])<=limit for k,limit in [('title',150),('content',4000),('source',150)]) or type(item['importance']) is not int or not 1<=item['importance']<=5:
                raise ValueError('invalid correction fields')
            if item['title'] in seen:raise ValueError('duplicate correction title')
            seen.add(item['title'])
            existing=db.execute('SELECT * FROM memories WHERE title=?',(item['title'],)).fetchall()
            if existing:
                if len(existing)!=1 or any(existing[0][k]!=v for k,v in item.items()) or existing[0]['forgotten'] or existing[0]['archived']:
                    raise ValueError('correction title collision; refusing merge or resurrection')
            else:actions.append({'name':'memory_save','arguments':item})
        seen=set()
        for item in plan['forget']:
            if set(item)!={'id','sha256'} or item['id'] in seen:raise ValueError('invalid old memory reference')
            seen.add(item['id'])
            row=db.execute('SELECT * FROM memories WHERE id=?',(item['id'],)).fetchone()
            if not row or fingerprint(row)!=item['sha256']:raise ValueError('old memory changed or belongs to a different scope')
            if row['title'] in {i['title'] for i in plan['save']}:raise ValueError('correction must have a new unique title')
            if not row['forgotten']:actions.append({'name':'memory_forget','arguments':{'id':item['id'],'forgotten':True}})
    return scope,actions


async def apply(plan,state,backup):
    if backup==state or backup.is_relative_to(state):raise ValueError('backup must be outside runtime state')
    with open(state/'state.lock','a') as owner:
        try:fcntl.flock(owner,fcntl.LOCK_EX|fcntl.LOCK_NB)
        except BlockingIOError:raise ValueError('stop backend before memory repair') from None
        scope,actions=inspect(plan,state)
        if not actions:return {'status':'already_applied','actions':0}
        backup.mkdir(mode=0o700,parents=True,exist_ok=False);backup.chmod(0o700)
        (backup/'plan.json').write_text(json.dumps(plan,ensure_ascii=False,indent=2));(backup/'plan.json').chmod(0o600)
        with sqlite3.connect(state/'state.sqlite3') as src,sqlite3.connect(backup/'state.sqlite3') as dst:src.backup(dst)
        shutil.copytree(state/'scopes'/scope,backup/'scope')
        async def deny(_):raise ValueError('memory repair has no data access')
        result=await run_dsh(scope=scope,text='',context='',model='memory-repair-only',endpoint='http://127.0.0.1:1',api_key='not-used',dispatch=deny,state_root=state,
                             remember=False,memory_only=True,memory_actions=actions,memory_check_titles=[r['title'] for r in plan['save']])
        _,pending=inspect(plan,state)
        if pending or result.get('verified')!=len(plan['save']):raise ValueError('post-repair verification failed')
        receipt={'status':'applied','scope':scope,'actions':len(actions),'native_search_verified':True}
        (backup/'receipt.json').write_text(json.dumps(receipt,indent=2))
        return receipt


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--state-dir',type=Path,required=True)
    parser.add_argument('--backup-dir',type=Path)
    parser.add_argument('--apply',action='store_true')
    args=parser.parse_args();plan=json.load(sys.stdin);state=args.state_dir.resolve()
    if args.apply:
        if not args.backup_dir:parser.error('--apply requires backup directory')
        result=asyncio.run(apply(plan,state,args.backup_dir.resolve()))
    else:
        scope,actions=inspect(plan,state);result={'status':'pending' if actions else 'already_applied','scope':scope,'actions':len(actions)}
    print(json.dumps(result,ensure_ascii=False))

if __name__=='__main__':main()
