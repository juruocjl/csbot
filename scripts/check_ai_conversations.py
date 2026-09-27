"""Conversation list access, pagination, and bounded previews (no business DB)."""
from pathlib import Path
import sys
import tempfile
import unittest
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from ai_runtime.store import StateStore

class Checks(unittest.TestCase):
    def test_scope_and_pagination(self):
        with tempfile.TemporaryDirectory() as tmp:
            db=StateStore(Path(tmp)/'state.sqlite3')
            try:
                for key,gid,uid,channel in [('group','1','2','qq'),('report','1','0','report'),
                                            ('own','1','1','web'),('other-user','1','2','web'),
                                            ('other-group','2','1','qq'),('unknown-kind','1','1','unknown')]:
                    db.register_run(key,'scope',gid,uid,channel,'x'*1000)
                page=db.conversations('1','1')
                self.assertEqual({r['id'] for r in page['records']},{'group','report','own'})
                self.assertTrue(all(len(r['request'])==300 and 'response' not in r for r in page['records']))
                for i in range(35): db.register_run(f'new{i}','scope','1','1','web','fixture')
                first=db.conversations('1','1');self.assertEqual(len(first['records']),30)
                db.register_run('later','scope','1','1','qq','new arrival')
                second=db.conversations('1','1',first['nextCursor'])
                ids=[r['id'] for r in first['records']+second['records']]
                self.assertEqual(len(ids),len(set(ids)));self.assertEqual(len(ids),38)
                self.assertIsNone(second['nextCursor']);self.assertNotIn('later',ids)
                self.assertEqual(db.conversations('3','1')['records'],[])
                for value in (-1,0,True,2**63):
                    with self.assertRaises(ValueError):db.conversations('1','1',value)
                with self.assertRaises(ValueError):db.conversations('','1')
            finally:db.close()

if __name__=='__main__':unittest.main()
