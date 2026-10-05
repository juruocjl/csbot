"""Stable AI aliases and authenticated resolution, using temporary SQLite only."""
import ast
from pathlib import Path
import sqlite3
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ai_runtime.store import StateStore

FIRST = '11111111-1111-4111-a111-123456abcdef'
SECOND = '22222222-2222-4222-b222-123456abcdef'


class Checks(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = Path(self.tmp.name)/'state.sqlite3'
        self.store = StateStore(self.path)

    def tearDown(self):
        self.store.close()
        self.tmp.cleanup()

    def register(self, key=FIRST, gid='g', uid='owner', channel='qq'):
        self.store.register_run(key, 'scope', gid, uid, channel, 'fixture')

    def test_collision_does_not_retarget_old_link_and_survives_restart(self):
        self.register()
        self.register(SECOND)
        self.assertEqual(self.store.short_run_id(FIRST), '123456abcdef')
        self.assertEqual(self.store.short_run_id(SECOND), 'b222123456abcdef')
        self.assertEqual(self.store.resolve_run_id('123456ABCDEF', 'g', 'reader'), FIRST)
        self.assertEqual(self.store.resolve_run_id('b222123456abcdef', 'g', 'reader'), SECOND)
        self.store.close()
        self.store = StateStore(self.path)
        self.assertEqual(self.store.short_run_id(FIRST), '123456abcdef')
        self.assertEqual(self.store.short_run_id(SECOND), 'b222123456abcdef')

    def test_collision_extends_again_when_longer_suffix_is_taken(self):
        self.register()
        self.register(SECOND)
        third = '33333333-3333-4333-b222-123456abcdef'
        self.register(third)
        self.assertEqual(self.store.short_run_id(third), '4333b222123456abcdef')

    def test_resolution_matches_conversation_permissions(self):
        for index, channel in enumerate(('qq', 'report', 'web', 'unknown')):
            key = f'{index:08x}-0000-4000-a000-00000000000{index}'
            self.register(key, channel=channel)
            alias = self.store.short_run_id(key)
            for candidate in (key, key.upper(), alias):
                self.assertEqual(self.store.resolve_run_id(candidate, 'g', 'owner'), key if channel != 'unknown' else None)
                self.assertEqual(self.store.resolve_run_id(candidate, 'g', 'reader'), key if channel in ('qq', 'report') else None)
                self.assertIsNone(self.store.resolve_run_id(candidate, 'other', 'owner'))
                self.assertIsNone(self.store.resolve_run_id(candidate, '', 'owner'))
                self.assertIsNone(self.store.resolve_run_id(candidate, 'g', ''))
        for value in ('', '../state.sqlite3', '123456abcde', 'z'*12, 'f'*12, None):
            self.assertIsNone(self.store.resolve_run_id(value, 'g', 'owner'))

    def test_alias_failure_rolls_back_new_run(self):
        with patch.object(self.store, '_assign_run_alias', side_effect=ValueError('fixture')):
            with self.assertRaises(ValueError):
                self.register()
        self.assertIsNone(self.store.get_run(FIRST))
        self.assertIsNone(self.store.short_run_id(FIRST))

    def test_old_database_backfills_once_in_insertion_order(self):
        self.register()
        self.register(SECOND)
        self.store.close()
        with sqlite3.connect(self.path) as db:
            db.execute('DROP TABLE run_aliases')
        self.store = StateStore(self.path)
        self.assertEqual(self.store.short_run_id(FIRST), '123456abcdef')
        self.assertEqual(self.store.short_run_id(SECOND), 'b222123456abcdef')
        self.assertEqual(len(self.store.rows('SELECT * FROM run_aliases')), 2)

    def test_actual_api_authentication_and_hidden_unauthorized_ids(self):
        from fastapi import Body, Depends, FastAPI, HTTPException
        from fastapi.testclient import TestClient
        from pydantic import BaseModel
        self.register()
        app = FastAPI()
        class AIAskResponse(BaseModel):
            chatId: str
        async def get_current_user():
            raise HTTPException(status_code=401, detail='Authentication required')
        namespace = dict(app=app, Body=Body, Depends=Depends, HTTPException=HTTPException,
                         AuthSession=SimpleNamespace, AIAskResponse=AIAskResponse, get_current_user=get_current_user)
        source = Path(__file__).resolve().parents[1]/'plugins/cs_server/__init__.py'
        endpoint = next(n for n in ast.parse(source.read_text()).body if isinstance(n, ast.AsyncFunctionDef) and n.name == 'ai_resolve')
        exec(compile(ast.Module(body=[endpoint], type_ignores=[]), str(source), 'exec'), namespace)
        with patch('ai_runtime.store.state_store', return_value=self.store), TestClient(app) as client:
            with patch.object(self.store, 'resolve_run_id', wraps=self.store.resolve_run_id) as resolve:
                self.assertEqual(client.post('/api/ai/resolve', json={'chatId': '123456abcdef'}).status_code, 401)
                resolve.assert_not_called()
            app.dependency_overrides[get_current_user] = lambda: SimpleNamespace(group_id='g', user_id='reader')
            for candidate in ('123456ABCDEF', FIRST.upper()):
                response = client.post('/api/ai/resolve', json={'chatId': candidate})
                self.assertEqual(response.status_code, 200)
                self.assertEqual(response.json(), {'chatId': FIRST})
            app.dependency_overrides[get_current_user] = lambda: SimpleNamespace(group_id='other', user_id='reader')
            unauthorized = client.post('/api/ai/resolve', json={'chatId': '123456abcdef'})
            missing = client.post('/api/ai/resolve', json={'chatId': '000000000000'})
            self.assertEqual(unauthorized.status_code, 404)
            self.assertEqual(unauthorized.json(), missing.json())


if __name__ == '__main__':
    unittest.main()
