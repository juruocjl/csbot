"""Explicit administrator mapping of historical AI records. Default: dry run."""
import argparse
import json
from pathlib import Path
import re
import sqlite3
import uuid


def validate(rows):
    if not isinstance(rows,list) or len(rows)>10000: raise ValueError('expected <=10000 mapping objects')
    seen=set()
    for row in rows:
        if set(row)!={'chat_id','group_id','user_id','channel'}: raise ValueError('unexpected/missing mapping fields')
        key=str(uuid.UUID(row['chat_id']))
        if key!=row['chat_id'] or key in seen: raise ValueError('duplicate or noncanonical chat_id')
        seen.add(key)
        if row['channel'] not in {'qq','web','report'}: raise ValueError('invalid channel')
        if not re.fullmatch(r'\d{1,20}',row['group_id']): raise ValueError('invalid group_id')
        if not re.fullmatch(r'\d{1,20}',row['user_id']): raise ValueError('explicit user_id is required')
    return rows


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--state',type=Path,required=True)
    parser.add_argument('--mapping',type=Path,required=True)
    parser.add_argument('--apply',action='store_true')
    args=parser.parse_args()
    rows=validate(json.loads(args.mapping.read_text()))
    # Do not instantiate StateStore: that is the live backend's lifecycle owner.
    with sqlite3.connect(args.state.resolve().as_uri()+'?mode=rw',uri=True,timeout=10) as db:
        for row in rows:
            existing=db.execute('SELECT group_id,user_id,channel FROM runs WHERE id=?',(row['chat_id'],)).fetchone()
            expected=(row['group_id'],row['user_id'],row['channel'])
            if existing and existing!=expected: raise ValueError('existing ownership conflicts; no changes applied')
        if args.apply:
            for row in rows:
                db.execute("INSERT OR IGNORE INTO runs(id,scope,group_id,user_id,channel,created_at,status,request) VALUES(?,?,?,?,?,0,'completed','旧记录：管理员确认归属')",
                    (row['chat_id'],'legacy:'+row['chat_id'],row['group_id'],row['user_id'],row['channel']))
    print(f'{"APPLIED" if args.apply else "DRY RUN"}: {len(rows)} confirmed mappings; no conversation/memory imported')


if __name__=='__main__':main()
