"""Synthetic Mneme fixtures: ownership, forgotten state, pagination and no writes."""
from pathlib import Path
import hashlib
import json
import sqlite3
import sys
import tempfile
import unittest
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from ai_runtime.store import StateStore
from ai_runtime.runner import scope_key
from ai_runtime.memory_browser import scopes,browse,detail,MemoryUnavailable

class Checks(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.root=Path(self.tmp.name)
        self.store=StateStore(self.root/'state.sqlite3')
        self.group=scope_key('qq','1','10');self.own=scope_key('web','1','10');self.other=scope_key('web','1','11')
        for scope,uid in [(self.own,'10'),(self.other,'11')]:
            self.store.register_run(scope,scope,'1',uid,'web','private question')
        for scope in [self.group,self.own,self.other]:
            path=self.root/'scopes'/scope/'memory'/'memory.db';path.parent.mkdir(parents=True)
            with sqlite3.connect(path) as db:
                db.execute('CREATE TABLE memories(id TEXT PRIMARY KEY,type TEXT,title TEXT,content TEXT,tags TEXT,importance INTEGER,forgotten INTEGER,archived INTEGER,created_at TEXT,updated_at TEXT)')
                for i in range(25):
                    db.execute('INSERT INTO memories VALUES(?,?,?,?,?,?,?,?,?,?)',(f'{i:03}','fact',f'记忆{i}',f'正文{i}','["测试"]',3,0,0,'2026-09-27T00:00:00Z','2026-09-27T00:00:00Z'))
                db.execute("INSERT INTO memories VALUES('forgotten','fact','secret','secret','[]',3,1,0,'x','x')")
                db.execute("INSERT INTO memories VALUES('archive','history','旧的','archive','[]',3,0,1,'x','x')")
    def tearDown(self):
        self.store.close();self.tmp.cleanup()
    def test_scope_isolation(self):
        self.assertEqual({r['id'] for r in scopes(self.store,'1','10')['scopes']},{self.group,self.own})
        for scope in [self.other,scope_key('qq','2','10'),'../'*21+'x']:
            with self.assertRaises(PermissionError):browse(self.store,'1','10',scope,root=self.root)
            with self.assertRaises(PermissionError):detail(self.store,'1','10',scope,'000',root=self.root)
        self.assertEqual(browse(self.store,'1','10',self.own,root=self.root)['total'],25)
        with self.assertRaises(ValueError):scopes(self.store,'','10')
    def test_paging_filter_and_forget(self):
        a=browse(self.store,'1','10',self.group,root=self.root)
        b=browse(self.store,'1','10',self.group,cursor=a['nextCursor'],root=self.root)
        self.assertEqual(len(a['items']),20);self.assertEqual(len(b['items']),5)
        self.assertFalse({r['id'] for r in a['items']}&{r['id'] for r in b['items']})
        self.assertEqual(browse(self.store,'1','10',self.group,query='测试',root=self.root)['total'],25)
        self.assertEqual(browse(self.store,'1','10',self.group,query="%' OR 1=1 --",root=self.root)['total'],0)
        self.assertEqual(browse(self.store,'1','10',self.group,archived=True,root=self.root)['total'],1)
        with self.assertRaises(PermissionError):detail(self.store,'1','10',self.group,'forgotten',root=self.root)
        for cursor in ['bad','W10=','e30=']:
            with self.assertRaises(ValueError):browse(self.store,'1','10',self.group,cursor=cursor,root=self.root)
    def test_readonly_missing_and_symlink(self):
        path=self.root/'scopes'/self.group/'memory'/'memory.db'
        before=hashlib.sha256(path.read_bytes()).hexdigest()
        self.assertEqual(detail(self.store,'1','10',self.group,'000',root=self.root)['content'],'正文0')
        browse(self.store,'1','10',self.group,root=self.root)
        self.assertEqual(hashlib.sha256(path.read_bytes()).hexdigest(),before)
        missing=scope_key('qq','3','10')
        self.assertEqual(browse(self.store,'3','10',missing,root=self.root)['total'],0)
        self.assertFalse((self.root/'scopes'/missing).exists())
        path.unlink();path.symlink_to(self.root/'scopes'/self.other/'memory'/'memory.db')
        with self.assertRaises(MemoryUnavailable):browse(self.store,'1','10',self.group,root=self.root)
    def test_layers_and_evidence(self):
        path=self.root/'scopes'/self.group/'memory'/'memory.db'
        data={'schema':'csbot-memory-v1','tier':'foundation','category':'alias','subject':'22222','text':'小茶是茶茶','evidence':[{'id':'e1','kind':'user','quote':'小茶是QQ22222'}]}
        with sqlite3.connect(path) as db:db.execute('UPDATE memories SET content=?,tags=? WHERE id=?',(json.dumps(data,ensure_ascii=False),'["tier:foundation","category:alias","小茶"]','000'))
        page=browse(self.store,'1','10',self.group,tier='foundation',category='alias',root=self.root)
        self.assertEqual(page['total'],1);self.assertEqual(page['items'][0]['preview'],'小茶是茶茶')
        self.assertEqual(page['items'][0]['tags'],['小茶'])
        result=detail(self.store,'1','10',self.group,'000',root=self.root)
        self.assertEqual(result['subject'],'22222');self.assertEqual(result['evidence'][0]['id'],'e1')
        self.assertEqual(browse(self.store,'1','10',self.group,tier='legacy',root=self.root)['total'],24)
        with self.assertRaises(ValueError):browse(self.store,'1','10',self.group,tier='bad',root=self.root)

    def test_visible_truncation(self):
        path=self.root/'scopes'/self.group/'memory'/'memory.db'
        with sqlite3.connect(path) as db:db.execute('UPDATE memories SET content=? WHERE id=?',('大'*140000,'000'))
        result=detail(self.store,'1','10',self.group,'000',root=self.root)
        self.assertTrue(result['truncated']);self.assertEqual(len(result['content']),131072)

if __name__=='__main__':unittest.main()
