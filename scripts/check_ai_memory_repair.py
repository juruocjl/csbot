"""Exact-scope/hash repair planning; no production data or model calls."""
import importlib.util
from pathlib import Path
import sqlite3
import sys
import tempfile
import unittest
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from ai_runtime.runner import scope_key
spec=importlib.util.spec_from_file_location('repair',Path(__file__).with_name('ai_repair_memory.py'))
repair=importlib.util.module_from_spec(spec);spec.loader.exec_module(repair)

class Checks(unittest.TestCase):
    def test_hash_and_idempotency(self):
        with tempfile.TemporaryDirectory() as tmp:
            state=Path(tmp);scope=scope_key('qq','1','');path=state/'scopes'/scope/'memory/memory.db';path.parent.mkdir(parents=True)
            with sqlite3.connect(path) as db:
                db.row_factory=sqlite3.Row
                db.execute('CREATE TABLE memories(id TEXT,type TEXT,title TEXT,content TEXT,importance INTEGER,source TEXT,forgotten INTEGER,archived INTEGER)')
                db.execute("INSERT INTO memories VALUES('old','project','wrong','alias=asker',4,'summary',0,0)")
                row=db.execute('SELECT * FROM memories').fetchone()
                save={'type':'project','title':'confirmed','content':'alias=other','importance':5,'source':'user-confirmed'}
                plan={'gid':'1','forget':[{'id':'old','sha256':repair.fingerprint(row)}],'save':[save]}
            self.assertEqual([a['name'] for a in repair.inspect(plan,state)[1]],['memory_save','memory_forget'])
            with sqlite3.connect(path) as db:db.execute("UPDATE memories SET content='changed' WHERE id='old'")
            with self.assertRaises(ValueError):repair.inspect(plan,state)
            with sqlite3.connect(path) as db:
                db.execute("UPDATE memories SET content='alias=asker',forgotten=1 WHERE id='old'")
                db.execute("INSERT INTO memories VALUES('new',?,?,?,?,?,0,0)",tuple(save[k] for k in ['type','title','content','importance','source']))
            self.assertEqual(repair.inspect(plan,state)[1],[])
            with sqlite3.connect(path) as db:db.execute("UPDATE memories SET forgotten=1 WHERE id='new'")
            with self.assertRaises(ValueError):repair.inspect(plan,state)
            plan['gid']='2'
            with self.assertRaises(sqlite3.Error):repair.inspect(plan,state)

if __name__=='__main__':unittest.main()
