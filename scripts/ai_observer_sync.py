"""Publish bounded, complete-line snapshots; never open a bot session for writing."""
import argparse
import json
import os
from pathlib import Path
import sqlite3
import tempfile
import time

def sync(source,destination):
    destination.mkdir(parents=True,exist_ok=True,mode=0o755)
    files=[]
    for scope in (source/'scopes').glob('*'):
        root=scope/'harness/sessions'
        if scope.is_symlink() or not root.is_dir():continue
        for file in root.glob('*/*/session*.jsonl'):
            if file.is_symlink() or not file.is_file():continue
            if file.stat().st_size>64*1024**2:raise ValueError('Session exceeds observer file budget')
            files.append((file,destination/file.relative_to(root)))
    if sum(p.stat().st_size for p,_ in files)>256*1024**2:raise ValueError('Observer snapshot budget exceeded')
    labels={}
    state=source/'state.sqlite3'
    if state.exists():
        with sqlite3.connect(f'file:{state}?mode=ro',uri=True) as db:
            for scope,channel,gid,uid in db.execute('SELECT scope,channel,group_id,user_id FROM runs ORDER BY created_at'):
                labels['csbot-'+scope]=(f'群 {gid}' if channel=='qq' else f'{channel} · 群 {gid} · 用户 {uid}')
    manifest_path=destination/'manifest.json'
    previous=json.loads(manifest_path.read_text()) if manifest_path.exists() else {}
    manifest={}
    for source_file,target in files:
        before=source_file.stat()
        unchanged=target.exists() and target.stat().st_mtime_ns==before.st_mtime_ns
        session_id=source_file.parent.name
        if unchanged and session_id in previous:
            manifest[session_id]={**previous[session_id],'title':labels.get(session_id,session_id)}
            continue
        target.parent.mkdir(parents=True,exist_ok=True,mode=0o755)
        # Drop an incomplete final line while a durable append is in progress.
        with source_file.open('rb') as stream:data=stream.read(before.st_size)
        data=data[:data.rfind(b'\n')+1]
        if not data:continue
        header=json.loads(data[:data.index(b'\n')])
        last=json.loads(data[data.rfind(b'\n',0,len(data)-1)+1:])
        manifest[header['id']]={'title':labels.get(header['id'],header['id']), 'seq':last.get('seq',0)}
        if unchanged:continue
        with tempfile.NamedTemporaryFile(dir=target.parent,delete=False) as out:
            temp=Path(out.name);out.write(data)
        os.chmod(temp,0o644)
        os.utime(temp,ns=(before.st_atime_ns,before.st_mtime_ns))
        os.replace(temp,target)
    retained={target for _,target in files}
    for old in destination.glob('*/*/session*.jsonl'):
        if old not in retained:old.unlink()
    encoded=json.dumps(manifest,ensure_ascii=False)
    if not manifest_path.exists() or manifest_path.read_text()!=encoded:
        temp=destination/'manifest.tmp';temp.write_text(encoded);temp.chmod(0o644);temp.replace(manifest_path)
    return len(files)

if __name__=='__main__':
    parser=argparse.ArgumentParser()
    parser.add_argument('--source',type=Path,required=True)
    parser.add_argument('--destination',type=Path,required=True)
    parser.add_argument('--watch',action='store_true')
    args=parser.parse_args()
    while True:
        count=sync(args.source,args.destination)
        if not args.watch:print('Snapshot sessions:',count);break
        time.sleep(5)
