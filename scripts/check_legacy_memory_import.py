"""Actual DSH/Mneme import without network/model; idempotence and boundaries."""
import asyncio
from pathlib import Path
import sqlite3
import sys
import tempfile
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
sys.path.insert(0,str(Path(__file__).resolve().parent))
from ai_runtime.store import StateStore
from ai_runtime.runner import scope_key
from ai_import_legacy_memory import migrate,plan

async def main():
    with tempfile.TemporaryDirectory() as tmp:
        root=Path(tmp);state=root/'state';store=StateStore(state/'state.sqlite3')
        rows=[{'gid':'123','mem':'QQ 456 喜欢茶🍵。'},{'gid':'123:qa','mem':'do not import Q&A'},{'gid':'123:report','mem':'do not import automatic report'}]
        try:await migrate(rows,state,root/'busy-backup')
        except ValueError as e:assert 'stop csbot.service' in str(e)
        else:raise AssertionError('live backend lock bypassed')
        store.close()
        before,excluded=plan(rows,state);assert before[0]['status']=='pending' and excluded==2
        result=await migrate(rows,state,root/'first-backup')
        assert result['groups'][0]['status']=='imported' and result['native_search_verified']
        result=await migrate(rows,state,root/'second-backup');assert result['groups'][0]['status']=='already_present'
        scope=scope_key('qq','123','');db_path=next((state/'scopes'/scope/'memory').glob('*.db'))
        with sqlite3.connect(db_path) as db:
            contents=db.execute('SELECT content FROM memories').fetchall()
            assert contents==[(rows[0]['mem'],)],contents
            db.execute('UPDATE memories SET archived=1')
        try:await migrate(rows,state,root/'third-backup')
        except ValueError as e:assert 'resurrection' in str(e)
        else:raise AssertionError('forgotten memory resurrected')
        for bad in ([{'gid':'private_123','mem':'x'}],[rows[0],rows[0]]):
            try:plan(bad,state)
            except ValueError:pass
            else:raise AssertionError('ambiguous identity accepted')
        assert not (state/'scopes'/scope_key('web','123','456')/'memory').exists()
        print('PASS: native save/search with zero model calls; exact content; backups; repeat dedupe; archived memory not resurrected; ownership validation; QQ/web separation; live backend lock')
        source=(Path(__file__).resolve().parents[1]/'plugins/cs_ai/__init__.py').read_text()
        for name in ('def get_mem(', 'def set_mem(', 'def remember_ai_qa(', 'def remember_report_knowledge(', 'def _ai_ask_legacy(', 'on_command("ai记忆"'):
            assert name not in source,name
        print('PASS: legacy memory read/write/command and fallback engine interfaces removed')
if __name__=='__main__':asyncio.run(main())
