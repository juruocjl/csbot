"""Actual context replay, legacy provenance and authenticated access (temp DB)."""
import ast
import json
import os
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from ai_runtime.run_context import run_context, MAX_BYTES
from ai_runtime.store import StateStore

class Checks(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.root=Path(self.tmp.name)
        self.store=StateStore(self.root/'state.sqlite3')
        self.id='11111111-1111-4111-a111-123456abcdef';self.scope='a'*64
        self.store.register_run(self.id,self.scope,'g','u','qq','reply fixture')
        self.store.execute('INSERT INTO run_events(run_id,payload,created_at,bytes) VALUES(?,?,?,?)',(self.id,'{"type":"status","status":"running"}',100,1))
        self.store.execute('INSERT INTO run_events(run_id,payload,created_at,bytes) VALUES(?,?,?,?)',(self.id,'{"type":"final"}',110,1))
    def tearDown(self):self.store.close();self.tmp.cleanup()
    def read(self):return run_context(self.store,self.store.get_run(self.id),self.root)
    def write_native(self,events):
        path=self.root/'scopes'/self.scope/'harness/sessions/fixture'/('csbot-'+self.scope)/'session.v3.jsonl'
        path.parent.mkdir(parents=True);path.write_text('\n'.join(json.dumps(e) for e in events))
    def native(self):
        def message(seq,text,source):return {'type':'user/message','seq':seq,'time':101000+seq,'data':{'source':source,'content':[{'type':'text','text':text}]}}
        return [{'type':'step/start','seq':1,'time':101000,'data':{'turn':1}},
                {'type':'system/message','seq':0,'time':101000,'data':{'turn':1,'step':1,'message':{'content':[{'type':'text','text':'ACTUAL_SYSTEM'}]}}},
                message(2,'ACTUAL_CONTEXT [image:fixture] source matchId',{'kind':'plugin','plugin':'csbot-context'}),
                message(3,'QQ用户 u：reply fixture',{'kind':'user'}),
                message(4,'ACTUAL_MEMORY',{'kind':'plugin','plugin':'@deepseek-ai/dsh-system-prompt'}),
                {'type':'assistant/message','seq':5,'time':102000,'data':{'content':[{'type':'reasoning','text':'PRIVATE_REASONING'}]}},
                {'type':'step/start','seq':6,'time':103000,'data':{'turn':2}},
                message(7,'OTHER_TURN_CONTEXT',{'kind':'plugin','plugin':'csbot-context'})]
    def test_legacy_replays_same_turn_without_assistant_or_next_turn(self):
        self.write_native(self.native());value=self.read()
        self.assertEqual(value['origin'],'native_archive')
        self.assertEqual([e['text'] for e in value['entries']],['ACTUAL_SYSTEM','ACTUAL_CONTEXT [image:fixture] source matchId','ACTUAL_MEMORY'])
        self.assertNotIn('PRIVATE_REASONING',json.dumps(value))
    def test_missing_and_ambiguous_request_never_fabricate_old_context(self):
        self.assertEqual(self.read()['origin'],'missing')
        events=self.native();events.insert(4,events[3]);self.write_native(events)
        self.assertEqual(self.read()['entries'],[])
    def test_captured_context_wins_and_limits_are_explicit(self):
        event={'type':'context','id':'2','title':'本轮资料','text':'x'*(MAX_BYTES+1),'time':101000}
        encoded=json.dumps(event)
        self.store.execute('INSERT INTO run_events(run_id,payload,created_at,bytes) VALUES(?,?,?,?)',(self.id,encoded,101,len(encoded)))
        value=self.read();self.assertEqual(value['origin'],'captured');self.assertTrue(value['truncated'])
        self.assertEqual(len(value['entries'][0]['text']),MAX_BYTES)
        self.assertEqual(len(json.loads(self.store.rows("SELECT payload FROM run_events WHERE payload LIKE '%context%'")[0]['payload'])['text']),MAX_BYTES+1)
    def test_actual_endpoint_auth_and_web_owner_boundaries(self):
        from fastapi import FastAPI,Body,Depends,HTTPException
        from fastapi.testclient import TestClient
        import asyncio
        app=FastAPI()
        async def auth():raise HTTPException(401,'Authentication required')
        source=Path(__file__).resolve().parents[1]/'plugins/cs_server/__init__.py'
        endpoint=next(e for e in ast.parse(source.read_text()).body if isinstance(e,ast.AsyncFunctionDef) and e.name=='ai_context')
        env=dict(app=app,Body=Body,Depends=Depends,HTTPException=HTTPException,AuthSession=SimpleNamespace,get_current_user=auth,asyncio=asyncio)
        exec(compile(ast.Module(body=[endpoint],type_ignores=[]),str(source),'exec'),env)
        self.write_native(self.native())
        with patch('ai_runtime.store.state_store',return_value=self.store),patch.dict(os.environ,{'CS_AI_STATE_DIR':str(self.root)}),TestClient(app) as client:
            self.assertEqual(client.post('/api/ai/context',json={'chatId':self.id}).status_code,401)
            app.dependency_overrides[auth]=lambda:SimpleNamespace(group_id='other',user_id='u')
            self.assertEqual(client.post('/api/ai/context',json={'chatId':self.id}).status_code,404)
            app.dependency_overrides[auth]=lambda:SimpleNamespace(group_id='g',user_id='reader')
            self.assertEqual(client.post('/api/ai/context',json={'chatId':self.id}).json()['origin'],'native_archive')
            self.store.execute("UPDATE runs SET channel='web' WHERE id=?",(self.id,))
            self.assertEqual(client.post('/api/ai/context',json={'chatId':self.id}).status_code,404)
            app.dependency_overrides[auth]=lambda:SimpleNamespace(group_id='g',user_id='u')
            self.assertEqual(client.post('/api/ai/context',json={'chatId':self.id}).status_code,200)

if __name__=='__main__':unittest.main()
