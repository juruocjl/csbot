"""Read-only group query boundaries and business-day regression checks."""
import asyncio
from datetime import datetime
from io import BytesIO
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from PIL import Image
from ai_runtime.group import GroupKnowledge, point_window
from ai_runtime.media import MediaCache
from ai_runtime.queries import compile_query


class Broker:
    group_id = '123'
    async def call(self, name):
        assert name == 'election_state'
        return {'rows': [{'key': 'selected_uid', 'value': '2'}, {'key': 'active', 'value': '1'}]}
    async def query(self, sql, params):
        compile_query(sql, params, self.group_id)
        return {'rows': [], 'truncated': False}


class Bot:
    calls = 0
    async def get_group_member_list(self, **kw):
        assert kw == {'group_id': 123, 'no_cache': True}
        self.calls += 1
        return [{'user_id': 1, 'group_id': 123, 'nickname': '重名', 'card': '群主', 'role': 'owner', 'private': 'omit'},
                {'user_id': 2, 'group_id': 123, 'nickname': '重名', 'role': 'member'}]


class Checks(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.bot = Bot()
        self.group = GroupKnowledge(Broker(), bot_provider=lambda: self.bot,
                                    settings_provider=lambda: {'cs_group_list': [123], 'cs_fudu_ban_group_list': [], 'cs_fudu_delay': 10})

    async def test_identity_roles_and_params(self):
        rows = await self.group.call('group_members', {'search': '重名', 'limit': 1})
        self.assertEqual(rows['total'], 2)
        self.assertTrue(rows['truncated'])
        self.assertNotIn('private', rows['rows'][0])
        info = await self.group.call('member_info', {'uid': '2'})
        self.assertEqual(info['member']['role'], 'member')
        result = await self.group.call('group_admins', {})
        self.assertEqual([r['uid'] for r in result['qq_roles']], ['1'])
        self.assertEqual(result['election_state']['rows'][0]['value'], '2')
        self.assertEqual(self.bot.calls, 1)
        for name, params in [('member_info', {'uid': '999'}), ('member_avatar', {'uid': '999'}),
                             ('member_avatar', {'uid': '../secret'}), ('member_info', {'uid': '1', 'gid': '456'}),
                             ('group_members', {'limit': True}), ('points_today', {'day': -1})]:
            with self.subTest(name=name, params=params), self.assertRaises(ValueError):
                await self.group.call(name, params)
        points = await self.group.call('points_today', {'uid': '2'})
        self.assertEqual(points['rows'][0]['regular_points'], 0)
        rules = await self.group.call('election_rules', {})
        self.assertTrue(rules['scheduled_election_enabled'])
        self.assertFalse(rules['punishment_ban_enabled'])

    async def test_platform_failure_does_not_fake_members(self):
        def fail(): raise RuntimeError('secret must not leak')
        group = GroupKnowledge(Broker(), bot_provider=fail)
        with self.assertRaisesRegex(ValueError, 'roles are unknown'):
            await group.call('group_admins', {})

    async def test_avatar_fixed_url_private_cache(self):
        buffer = BytesIO(); Image.new('RGB', (16, 16), 'blue').save(buffer, format='JPEG')
        class Response:
            status = 200
            content = None
            async def __aenter__(self): self.content = self; return self
            async def __aexit__(self, *args): pass
            async def iter_chunked(self, size): yield buffer.getvalue()
        class Session:
            def __init__(self, **kw): assert kw['trust_env'] is False
            async def __aenter__(self): return self
            async def __aexit__(self, *args): pass
            def get(self, url, **kw):
                assert url == 'https://q1.qlogo.cn/g?b=qq&nk=2&s=640'
                assert kw == {'allow_redirects': False}
                return Response()
        with tempfile.TemporaryDirectory() as tmp, patch('ai_runtime.group.aiohttp.ClientSession', Session):
            self.group.cache = MediaCache(Path(tmp))
            result = await self.group.call('member_avatar', {'uid': '2'})
            self.assertEqual(result['full_status'], 'available')
            self.assertTrue(Path(result['full_path']).is_relative_to(Path(tmp).resolve()/'.private'))
            self.assertEqual(Image.open(result['full_path']).format, 'PNG')
            self.assertTrue(Path(result['thumbnail_path']).is_file())
            self.group.cache.db.close()

    def test_business_day_boundary(self):
        before = datetime(2026, 9, 27, 23, 54, 59).timestamp()
        after = before + 1
        self.assertEqual(point_window(now=before)[0], int(datetime(2026, 9, 26, 23, 55).timestamp()))
        self.assertEqual(point_window(now=after)[0], int(after))
        self.assertEqual(point_window(day=1, now=after), point_window(now=before))

    def test_sql_scope_and_storage_allowlist(self):
        sql = compile_query('SELECT * FROM points', {}, '123')
        self.assertIn("'^group_' || '123' || '_[0-9]+$'", sql)
        sql = compile_query('SELECT * FROM election_state', {}, '123')
        self.assertIn("'adminqq' || '123'", sql)
        self.assertNotIn('card_manager', sql)
        for query in ('SELECT * FROM local_storage', 'SELECT * FROM user_info', 'SELECT * FROM public.fudu_points'):
            with self.assertRaises(ValueError): compile_query(query, {}, '123')


if __name__ == '__main__': unittest.main()
