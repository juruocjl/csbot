"""Birthday runtime checks: temp dirs + mock OneBot, no network/DB/QQ.

Covers StateStore persistence, the SQLite memory snapshot, the DSH runner
environment contract and the 23:50 -> 00:00 scheduler flow.
"""
from __future__ import annotations

import ast
import asyncio
from contextlib import nullcontext
from datetime import date, datetime
import importlib.util
import json
import os
from pathlib import Path
import re
import shutil
import sqlite3
import sys
import tempfile
import time
import types
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

try:  # POSIX-only dependency of the operational store; Windows checks stub it.
    import fcntl  # noqa: F401
except ModuleNotFoundError:  # pragma: no cover - Windows local checks only
    sys.modules['fcntl'] = types.SimpleNamespace(
        flock=lambda *args, **kwargs: None, LOCK_EX=2, LOCK_NB=4
    )

from nonebot.adapters.onebot.v11 import MessageSegment
from nonebot.exception import ActionFailed

import ai_runtime.runner as runner
import ai_runtime.store as store_module
from ai_runtime.store import StateStore

REPO = Path(__file__).resolve().parents[1]
PLUGIN = REPO / 'plugins' / 'birthday'
GID = '1058060222'

LOGIC_SPEC = importlib.util.spec_from_file_location('birthday_runtime_logic', PLUGIN / 'logic.py')
LOGIC = importlib.util.module_from_spec(LOGIC_SPEC)
sys.modules[LOGIC_SPEC.name] = LOGIC
LOGIC_SPEC.loader.exec_module(LOGIC)

GUARD_SPEC = importlib.util.spec_from_file_location(
    'birthday_runtime_guard', REPO / 'plugins' / 'cs_ai' / 'output_guard.py'
)
GUARD = importlib.util.module_from_spec(GUARD_SPEC)
sys.modules[GUARD_SPEC.name] = GUARD
GUARD_SPEC.loader.exec_module(GUARD)


class BirthdayStateStoreTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.store = StateStore(Path(self.tmp.name) / 'state.sqlite3')

    def tearDown(self) -> None:
        self.store.close()
        self.tmp.cleanup()

    def test_cooldown_read_write(self) -> None:
        self.assertIsNone(self.store.get_birthday_cooldown('1001'))
        self.store.set_birthday_cooldown('1001', 1_700_000_000)
        self.assertEqual(self.store.get_birthday_cooldown('1001'), 1_700_000_000)
        before = int(time.time())
        self.store.set_birthday_cooldown('1001')
        self.assertGreaterEqual(self.store.get_birthday_cooldown('1001'), before)

    def test_draft_sent_idempotency_and_clear(self) -> None:
        store = self.store
        store.upsert_birthday_draft(GID, '1001', 2026, '2026-07-10', 'profile', 'run-1', '草稿')
        wish = store.get_birthday_wish(GID, '1001', 2026)
        self.assertEqual((wish['status'], wish['draft_text']), ('drafted', '草稿'))

        store.mark_birthday_sent(GID, '1001', 2026, '2026-07-10', 'profile', 'run-1',
                                 '正式祝福', 'group_notice')
        # A late draft must never demote an already sent wish.
        store.upsert_birthday_draft(GID, '1001', 2026, '2026-07-10', 'profile', 'run-2', '新草稿')
        store.mark_birthday_failed(GID, '1001', 2026, '2026-07-10', 'profile', 'boom', 'run-2')
        wish = store.get_birthday_wish(GID, '1001', 2026)
        self.assertEqual(wish['status'], 'sent')
        self.assertEqual(wish['draft_text'], '正式祝福')
        self.assertEqual(wish['delivered_via'], 'group_notice')

        self.store.set_birthday_cooldown('1001')
        self.store.upsert_birthday_draft(GID, '1001', 2027, '2027-07-10', 'profile', 'run-3', '明年草稿')
        removed = store.clear_unsent_birthday_draft('1001')
        self.assertEqual(removed, 1)
        self.assertIsNone(store.get_birthday_wish(GID, '1001', 2027))
        self.assertIsNotNone(store.get_birthday_wish(GID, '1001', 2026))

    def test_clear_unsent_can_target_one_year(self) -> None:
        store = self.store
        store.upsert_birthday_draft(GID, '1002', 2026, '2026-01-01', 'profile', 'r1', 'a')
        store.upsert_birthday_draft(GID, '1002', 2027, '2027-01-01', 'profile', 'r2', 'b')
        self.assertEqual(store.clear_unsent_birthday_draft('1002', 2026), 1)
        self.assertIsNone(store.get_birthday_wish(GID, '1002', 2026))
        self.assertIsNotNone(store.get_birthday_wish(GID, '1002', 2027))

    def test_birthday_conversation_is_readable_only_inside_its_group(self) -> None:
        store = self.store
        store.register_run('run-birthday', 'scope', GID, '', 'birthday', '生日祝福')
        listed = store.conversations(GID, '1001')
        self.assertIn('run-birthday', {row['id'] for row in listed['records']})
        self.assertTrue(store.can_read('run-birthday', GID, '1001'))
        self.assertFalse(store.can_read('run-birthday', '999', '1001'))


class MemorySnapshotTest(unittest.TestCase):
    def test_wal_snapshot_is_consistent_and_independent(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            source = Path(tmp) / 'source'
            source.mkdir()
            live = sqlite3.connect(source / 'memory.db')
            try:
                live.execute('PRAGMA journal_mode=WAL')
                live.execute('CREATE TABLE memory(id INTEGER PRIMARY KEY, text TEXT)')
                live.execute("INSERT INTO memory(text) VALUES('未 checkpoint 的记忆')")
                live.commit()
                self.assertTrue((source / 'memory.db-wal').exists())

                target = Path(tmp) / 'target'
                self.assertTrue(runner._snapshot_memory_dir(source, target))
                copied = sqlite3.connect(target / 'memory.db')
                try:
                    rows = copied.execute('SELECT text FROM memory').fetchall()
                finally:
                    copied.close()
                self.assertEqual(rows, [('未 checkpoint 的记忆',)])

                # The copy is a real snapshot: later source writes stay invisible.
                live.execute("INSERT INTO memory(text) VALUES('后续写入')")
                live.commit()
                copied = sqlite3.connect(target / 'memory.db')
                try:
                    count = copied.execute('SELECT count(*) FROM memory').fetchone()[0]
                finally:
                    copied.close()
                self.assertEqual(count, 1)
                self.assertEqual(live.execute('SELECT count(*) FROM memory').fetchone()[0], 2)
            finally:
                live.close()

    def test_missing_source_is_silent(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            self.assertFalse(runner._snapshot_memory_dir(Path(tmp) / 'missing',
                                                         Path(tmp) / 'out'))

    def test_symlinked_memory_db_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            source = Path(tmp) / 'source'
            source.mkdir()
            real = Path(tmp) / 'real.db'
            real.write_bytes(b'')
            try:
                (source / 'memory.db').symlink_to(real)
            except (OSError, NotImplementedError):
                self.skipTest('symlinks unavailable')
            self.assertFalse(runner._snapshot_memory_dir(source, Path(tmp) / 'out'))


class _FakeStdin:
    def __init__(self) -> None:
        self.messages: list[dict] = []

    def write(self, data: bytes) -> None:
        self.messages.append(json.loads(data.decode()))

    async def drain(self) -> None:
        return None


class _FakeStdout:
    def __init__(self, events: list[dict]) -> None:
        self.lines = [b'CSBOT:' + json.dumps(event).encode() + b'\n' for event in events]

    async def readline(self) -> bytes:
        return self.lines.pop(0) if self.lines else b''


class _FakeStderr:
    async def read(self, size: int = -1) -> bytes:
        return b''


class _FakeProcess:
    def __init__(self, events: list[dict]) -> None:
        self.stdin = _FakeStdin()
        self.stdout = _FakeStdout(events)
        self.stderr = _FakeStderr()
        self.returncode: int | None = None

    async def wait(self) -> int:
        self.returncode = 0
        return 0

    def kill(self) -> None:
        self.returncode = -9


class RunnerEnvironmentTest(unittest.IsolatedAsyncioTestCase):
    async def test_birthday_run_mounts_readonly_snapshot_and_scene(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            runtime = root / 'runtime'
            (runtime / 'dsh' / 'node_modules' / '.bin').mkdir(parents=True)
            shutil.copyfile(REPO / 'ai_runtime' / 'dsh' / 'profile.json',
                            runtime / 'dsh' / 'profile.json')
            (runtime / 'dsh' / 'node_modules' / '.bin' / 'dsh').write_bytes(b'')

            source = root / 'group-memory'
            source.mkdir()
            live = sqlite3.connect(source / 'memory.db')
            try:
                live.execute('PRAGMA journal_mode=WAL')
                live.execute('CREATE TABLE memory(id INTEGER PRIMARY KEY, text TEXT)')
                live.execute("INSERT INTO memory(text) VALUES('群记忆')")
                live.commit()
            finally:
                live.close()

            captured: dict = {}
            process = _FakeProcess([
                {'type': 'ready'},
                {'type': 'complete', 'reason': {'kind': 'completed'},
                 'messages': [{'content': '祝福正文'}], 'tools': []},
            ])

            async def fake_exec(*args, **kwargs):
                captured['args'] = args
                captured['env'] = dict(kwargs.get('env') or {})
                captured['cwd'] = kwargs.get('cwd')
                return process

            async def dispatch(request):
                raise AssertionError('no tool call expected')

            scope = runner.scope_key('birthday', GID, '', conversation='run-1')
            with patch.object(runner, 'RUNTIME', runtime), \
                    patch.object(runner, 'source_snapshot',
                                 lambda: nullcontext(SimpleNamespace(root=None, guide='', files=()))), \
                    patch.object(runner.asyncio, 'create_subprocess_exec', fake_exec):
                result = await runner.run_dsh(
                    scope=scope, text='写祝福', context='ctx', model='deepseek-chat',
                    endpoint='https://api.deepseek.com/v1', api_key='sk-test',
                    dispatch=dispatch, remember=False, state_root=root / 'state',
                    run_id='run-1', memory_source_dir=source, read_only_memory=True,
                    usage_scene='birthday_wish', enable_image_gen=False,
                )

            self.assertEqual(result['messages'], [{'content': '祝福正文'}])
            env = captured['env']
            self.assertEqual(env['CSBOT_USAGE_SCENE'], 'birthday_wish')
            self.assertEqual(env['CSBOT_MEMORY_READONLY'], '1')
            self.assertEqual(env['CSBOT_REMEMBER'], '0')
            self.assertNotIn('CSBOT_IMAGE_GEN_ENABLED', env)
            mounted = root / 'state' / 'scopes' / scope / 'memory' / 'memory.db'
            self.assertTrue(mounted.is_file())
            copied = sqlite3.connect(mounted)
            try:
                self.assertEqual(copied.execute('SELECT text FROM memory').fetchone()[0], '群记忆')
            finally:
                copied.close()


def _load_plugin() -> dict:
    source = (PLUGIN / '__init__.py').read_text(encoding='utf-8')
    tree = ast.parse(source)
    functions = {
        'now_shanghai', '_background_disabled', '_group_members', '_trackable_members',
        '_display_name', '_fetch_profile_birthday', '_store_profile_birthday',
        'refresh_group_index', 'group_birthdays', '_generate_wish', 'pregen_birthdays',
        '_send_notice', 'dispatch_birthdays_for_date', '_is_superuser', '_parse_target',
        '_text_args', 'describe_birthday', '_drop_legacy_override',
        'migrate_legacy_birthday_overrides', 'set_birthday_for', 'birthday_overview',
        'sync_birthdays', 'send_test_wish', '_run_startup_birthday_sync',
        'startup_birthday_sync',
    }
    constants = {'BLOCKED_TEXTS'}
    nodes = []
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name in functions:
            node.decorator_list = []
            nodes.append(node)
        elif (isinstance(node, ast.Assign) and len(node.targets) == 1
              and isinstance(node.targets[0], ast.Name) and node.targets[0].id in constants):
            nodes.append(node)

    class RuntimeConfigStub:
        def __init__(self) -> None:
            self.values: dict = {}
            self.last_updated_by = None

        async def get(self, key):
            if key == 'birthday_overrides':
                return dict(self.values.get(key) or {})
            return self.values.get(key)

        async def set(self, key, value, updated_by=None):
            self.values[key] = value
            self.last_updated_by = updated_by

    class SuperuserStub:
        def __init__(self) -> None:
            self.allowed: set[str] = set()

        async def __call__(self, bot, event, *args, **kwargs):
            return str(event.user_id) in self.allowed

    runtime_config = RuntimeConfigStub()
    superuser = SuperuserStub()
    state_dir = tempfile.mkdtemp(prefix='csbot-birthday-state-')
    store = StateStore(Path(state_dir) / 'state.sqlite3')
    ai_ask = AsyncMock(return_value='生日快乐呀。')
    ns = {
        'asyncio': asyncio, 'os': os, 're': re, 'time': time, 'uuid': __import__('uuid'),
        'date': date, 'datetime': __import__('datetime').datetime,
        'timedelta': __import__('datetime').timedelta,
        'ZoneInfo': __import__('zoneinfo').ZoneInfo,
        'logger': SimpleNamespace(warning=lambda *a, **k: None,
                                  error=lambda *a, **k: None,
                                  info=lambda *a, **k: None),
        'config': SimpleNamespace(
            cs_group_list=[GID], cs_birthday_enabled=True,
            cs_birthday_self_cooldown_seconds=86400,
            cs_birthday_timezone='Asia/Shanghai',
            cs_domain='https://cs.example.com',
        ),
        'runtime_config': runtime_config,
        'state_store': lambda: store,
        'ai_ask_main': ai_ask,
        '_ai_chat_link': lambda run_id: f'https://cs.example.com/ai-chat?chatId={run_id}',
        'SUPERUSER': superuser,
        'MessageSegment': MessageSegment,
        'ActionFailed': ActionFailed,
        'QQ_GUARD_BLOCKED_TEXT': GUARD.QQ_GUARD_BLOCKED_TEXT,
        'QQ_GUARD_FAILED_TEXT': GUARD.QQ_GUARD_FAILED_TEXT,
        'BIRTHDAY_LOCK': asyncio.Lock(),
        'STARTUP_SYNC': {'task': None},
        'MemberBirthday': LOGIC.MemberBirthday,
        'CLEAR_OVERRIDE': LOGIC.CLEAR_OVERRIDE,
        'DISABLED_VALUE': LOGIC.DISABLED_VALUE,
        'build_birthday_prompt': LOGIC.build_birthday_prompt,
        'check_cooldown': LOGIC.check_cooldown,
        'days_until_birthday': LOGIC.days_until_birthday,
        'format_cooldown_remaining': LOGIC.format_cooldown_remaining,
        'matches_birthday_on_date': LOGIC.matches_birthday_on_date,
        'parse_birthday_input': LOGIC.parse_birthday_input,
        'sanitize_notice_text': LOGIC.sanitize_notice_text,
        'validate_mmdd': LOGIC.validate_mmdd,
    }
    exec(compile(ast.Module(body=nodes, type_ignores=[]), str(PLUGIN / '__init__.py'), 'exec'), ns)
    ns['_store'] = store
    ns['_config_stub'] = runtime_config
    ns['_superuser'] = superuser
    ns['_ask'] = ai_ask
    ns['_state_dir'] = state_dir
    return ns


class MockBot:
    self_id = 999999

    def __init__(self, members: list[dict], profiles: dict | None = None,
                 notice_error: Exception | None = None,
                 profile_errors: set[str] | None = None) -> None:
        self.members = members
        self.profiles = profiles or {}
        self.notice_error = notice_error
        self.profile_errors = profile_errors or set()
        self.calls: list[tuple] = []
        self.group_messages: list[tuple] = []

    async def get_group_member_list(self, group_id, no_cache=False):
        self.calls.append(('member_list', group_id, no_cache))
        return list(self.members)

    async def get_stranger_info(self, user_id, no_cache=True):
        self.calls.append(('stranger', user_id, no_cache))
        if str(user_id) in self.profile_errors:
            raise RuntimeError('profile unavailable')
        return dict(self.profiles.get(str(user_id), {}))

    async def call_api(self, api, **params):
        self.calls.append(('call_api', api, params))
        if api == '_send_group_notice' and self.notice_error is not None:
            raise self.notice_error
        return {'message_id': 1}

    async def send_group_msg(self, group_id, message):
        self.group_messages.append((group_id, message))
        return {'message_id': 2}

    def notices(self) -> list[dict]:
        return [call[2] for call in self.calls
                if call[0] == 'call_api' and call[1] == '_send_group_notice']

    def stranger_uids(self) -> list[int]:
        return [call[1] for call in self.calls if call[0] == 'stranger']

    def member_list_calls(self) -> int:
        return len([call for call in self.calls if call[0] == 'member_list'])


MEMBER_1001 = {'user_id': 1001, 'nickname': '小明', 'card': '明', 'is_robot': False}
MEMBER_1002 = {'user_id': 1002, 'nickname': '小红', 'card': '', 'is_robot': False}
MEMBER_1003 = {'user_id': 1003, 'nickname': '小刚', 'card': '刚', 'is_robot': False}
MEMBER_1004 = {'user_id': 1004, 'nickname': '小美', 'card': '', 'is_robot': False}
PROFILES = {
    '1001': {'birthday_month': 7, 'birthday_day': 10, 'birthday_year': 2000},
    '1002': {'birthday_month': 7, 'birthday_day': 11, 'birthday_year': 1999},
}


class BirthdayScheduleTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self._env = patch.dict(os.environ, {'CS_DISABLE_BACKGROUND_JOBS': ''})
        self._env.start()
        self.ns = _load_plugin()

    def tearDown(self) -> None:
        self.ns['_store'].close()
        shutil.rmtree(self.ns['_state_dir'], ignore_errors=True)
        self._env.stop()

    async def test_pregen_then_dispatch_reuses_draft_and_never_resends(self) -> None:
        bot = MockBot([MEMBER_1001], PROFILES)
        self.ns['_ask'].return_value = '[at:1001] 生日快乐呀。'
        drafted = await self.ns['pregen_birthdays'](bot, date(2026, 7, 10))
        self.assertEqual(drafted, 1)
        row = self.ns['_store'].get_member_birthday('1001')
        self.assertEqual((row['source'], row['mmdd']), ('qq_profile', '07-10'))
        self.assertEqual(bot.stranger_uids(), [1001])  # one profile query on first sync
        self.assertEqual(bot.member_list_calls(), 1)
        wish = self.ns['_store'].get_birthday_wish(GID, '1001', 2026)
        self.assertEqual(wish['status'], 'drafted')
        self.assertEqual(wish['draft_text'], '明 生日快乐呀。')
        self.assertEqual(wish['source'], 'profile')

        sent = await self.ns['dispatch_birthdays_for_date'](bot, date(2026, 7, 10))
        self.assertEqual(sent, 1)
        self.assertEqual(self.ns['_ask'].await_count, 1)  # draft reused
        notices = bot.notices()
        self.assertEqual(len(notices), 1)
        self.assertEqual(notices[0]['group_id'], GID)
        self.assertEqual(notices[0]['pinned'], 0)
        self.assertEqual(notices[0]['confirm_required'], 0)
        self.assertEqual(notices[0]['content'], '明 生日快乐呀。')
        wish = self.ns['_store'].get_birthday_wish(GID, '1001', 2026)
        self.assertEqual((wish['status'], wish['delivered_via']), ('sent', 'group_notice'))
        # 23:50 + 00:00 reuse the stored row: no repeated full profile scan.
        self.assertEqual(bot.stranger_uids(), [1001])
        self.assertEqual(bot.member_list_calls(), 2)  # one member list per job

        sent_again = await self.ns['dispatch_birthdays_for_date'](bot, date(2026, 7, 10))
        self.assertEqual(sent_again, 0)
        self.assertEqual(len(bot.notices()), 1)
        self.assertEqual(bot.stranger_uids(), [1001])

    async def test_disable_between_pregen_and_dispatch_skips_send(self) -> None:
        bot = MockBot([MEMBER_1001], PROFILES)
        await self.ns['pregen_birthdays'](bot, date(2026, 7, 10))
        self.ns['_store'].upsert_member_birthday('1001', None, 'disabled', force=True)
        sent = await self.ns['dispatch_birthdays_for_date'](bot, date(2026, 7, 10))
        self.assertEqual(sent, 0)
        self.assertEqual(bot.notices(), [])

    async def test_dispatch_generates_when_pregen_missed(self) -> None:
        bot = MockBot([MEMBER_1001], PROFILES)
        sent = await self.ns['dispatch_birthdays_for_date'](bot, date(2026, 7, 10))
        self.assertEqual(sent, 1)
        self.assertEqual(self.ns['_ask'].await_count, 1)
        self.assertEqual(len(bot.notices()), 1)

    async def test_multiple_birthdays_get_independent_notices_and_robots_are_filtered(self) -> None:
        members = [
            dict(MEMBER_1001),
            {'user_id': 1002, 'nickname': '小红', 'card': '', 'is_robot': False},
            {'user_id': 999999, 'nickname': '机器人自己', 'card': '', 'is_robot': False},
            {'user_id': 1003, 'nickname': '另一个机器人', 'card': '', 'is_robot': True},
        ]
        profiles = dict(PROFILES)
        profiles['1002'] = {'birthday_month': 7, 'birthday_day': 10}
        profiles['1003'] = {'birthday_month': 7, 'birthday_day': 10}
        bot = MockBot(members, profiles)
        sent = await self.ns['dispatch_birthdays_for_date'](bot, date(2026, 7, 10))
        self.assertEqual(sent, 2)
        self.assertEqual(len(bot.notices()), 2)
        self.assertEqual(sorted(call[1] for call in bot.calls if call[0] == 'stranger'),
                         [1001, 1002])

    async def test_action_failed_falls_back_to_group_message(self) -> None:
        bot = MockBot([dict(MEMBER_1001)], PROFILES, notice_error=ActionFailed('OneBot V11'))
        sent = await self.ns['dispatch_birthdays_for_date'](bot, date(2026, 7, 10))
        self.assertEqual(sent, 1)
        self.assertEqual(len(bot.group_messages), 1)
        self.assertEqual(bot.group_messages[0][0], int(GID))
        wish = self.ns['_store'].get_birthday_wish(GID, '1001', 2026)
        self.assertEqual(wish['delivered_via'], 'group_msg_fallback')

    async def test_guard_placeholder_blocks_notice_and_marks_failed(self) -> None:
        bot = MockBot([dict(MEMBER_1001)], PROFILES)
        self.ns['_ask'].return_value = GUARD.QQ_GUARD_BLOCKED_TEXT
        sent = await self.ns['dispatch_birthdays_for_date'](bot, date(2026, 7, 10))
        self.assertEqual(sent, 0)
        self.assertEqual(bot.notices(), [])
        wish = self.ns['_store'].get_birthday_wish(GID, '1001', 2026)
        self.assertEqual(wish['status'], 'failed')
        self.assertIn('祝福正文不可发布', wish['error'])

    async def test_command_permissions_cooldown_and_legacy_cleanup(self) -> None:
        bot = MockBot([MEMBER_1001], PROFILES)
        event = SimpleNamespace(user_id='2001')
        ns = self.ns
        ns['_config_stub'].values['birthday_overrides'] = {'1001': '01-01'}

        first = await ns['set_birthday_for'](bot, event, '2001', '2001', '7月10日')
        self.assertIn('设为 07-10', first)
        row = ns['_store'].get_member_birthday('2001')
        self.assertEqual((row['source'], row['mmdd']), ('manual', '07-10'))
        self.assertIsNotNone(ns['_store'].get_birthday_cooldown('2001'))

        again = await ns['set_birthday_for'](bot, event, '2001', '2001', '08-11')
        self.assertIn('冷却', again)
        row = ns['_store'].get_member_birthday('2001')
        self.assertEqual((row['source'], row['mmdd']), ('manual', '07-10'))

        other = await ns['set_birthday_for'](bot, event, '2001', '1001', '09-09')
        self.assertIn('只能修改自己的生日', other)

        ns['_superuser'].allowed.add('2002')
        super_event = SimpleNamespace(user_id='2002')
        changed = await ns['set_birthday_for'](bot, super_event, '2002', '1001', '09-09')
        self.assertIn('设为 09-09', changed)
        row = ns['_store'].get_member_birthday('1001')
        self.assertEqual((row['source'], row['mmdd']), ('manual', '09-09'))
        self.assertNotIn('1001', ns['_config_stub'].values.get('birthday_overrides', {}))
        self.assertIsNone(ns['_store'].get_birthday_cooldown('2002'))

        cleared = await ns['set_birthday_for'](bot, super_event, '2002', '1001', '默认')
        self.assertIn('按 QQ 资料恢复为 07-10', cleared)
        row = ns['_store'].get_member_birthday('1001')
        self.assertEqual((row['source'], row['mmdd']), ('qq_profile', '07-10'))

        unknown_bot = MockBot([], {})
        unknown = await ns['set_birthday_for'](unknown_bot, super_event, '2002', '1004', '默认')
        self.assertIn('未公开生日', unknown)
        row = ns['_store'].get_member_birthday('1004')
        self.assertEqual(row['source'], 'unknown')

    async def test_dry_test_wish_does_not_write_sent_record(self) -> None:
        bot = MockBot([dict(MEMBER_1001)], PROFILES)
        reply = await self.ns['send_test_wish'](bot, GID, '2002', '1001', True)
        self.assertIn('预览', reply)
        self.assertIn('AI 记录', reply)
        self.assertEqual(bot.notices(), [])
        self.assertIsNone(self.ns['_store'].get_birthday_wish(GID, '1001', 2026))

        reply = await self.ns['send_test_wish'](bot, GID, '2002', '1001', False)
        self.assertIn('group_notice', reply)
        self.assertEqual(len(bot.notices()), 1)
        self.assertIsNone(self.ns['_store'].get_birthday_wish(GID, '1001', 2026))


    async def test_first_sync_stores_profiles_marks_unknown_and_skips_on_rerun(self) -> None:
        members = [MEMBER_1001, MEMBER_1002, MEMBER_1003, MEMBER_1004]
        bot = MockBot(members, {'1001': PROFILES['1001'], '1002': PROFILES['1002']})
        snapshot = await self.ns['refresh_group_index'](bot, GID)
        self.assertEqual(snapshot['stats']['added'], 4)
        self.assertEqual(snapshot['stats']['unknown'], 2)
        self.assertEqual(bot.stranger_uids(), [1001, 1002, 1003, 1004])
        self.assertEqual(self.ns['_store'].get_member_birthday('1003')['source'], 'unknown')
        self.assertEqual(self.ns['_store'].get_member_birthday('1003')['mmdd'], None)

        again = await self.ns['refresh_group_index'](bot, GID)
        self.assertEqual(again['stats']['added'], 0)
        self.assertEqual(again['stats']['cached'], 4)
        # Every known row (including unknown) is reused without another API call.
        self.assertEqual(bot.stranger_uids(), [1001, 1002, 1003, 1004])
        birthdays = self.ns['group_birthdays'](again['index'], again['members'])
        self.assertEqual({member.uid for member in birthdays}, {'1001', '1002'})

    async def test_profile_failure_is_not_persisted_so_next_sync_retries(self) -> None:
        bot = MockBot([MEMBER_1001], {}, profile_errors={'1001'})
        snapshot = await self.ns['refresh_group_index'](bot, GID)
        self.assertEqual(snapshot['stats']['added'], 0)
        self.assertEqual(snapshot['stats']['failed'], 1)
        self.assertIsNone(self.ns['_store'].get_member_birthday('1001'))
        await self.ns['refresh_group_index'](bot, GID)
        self.assertEqual(bot.stranger_uids(), [1001, 1001])
        bot.profile_errors.clear()
        bot.profiles['1001'] = PROFILES['1001']
        recovered = await self.ns['refresh_group_index'](bot, GID)
        self.assertEqual(recovered['stats']['added'], 1)
        self.assertEqual(self.ns['_store'].get_member_birthday('1001')['source'], 'qq_profile')

    async def test_sync_command_incremental_then_full(self) -> None:
        members = [MEMBER_1001, MEMBER_1002, MEMBER_1003]
        bot = MockBot(members, {'1001': PROFILES['1001'], '1002': PROFILES['1002']})

        incremental = await self.ns['sync_birthdays'](bot, GID)
        self.assertIn('增量', incremental)
        self.assertIn('新增入库 3 人', incremental)
        self.assertIn('未公开 1 人', incremental)

        # Incremental sync never re-reads a known profile.
        bot.profiles['1001'] = {'birthday_month': 12, 'birthday_day': 25}
        cached = await self.ns['sync_birthdays'](bot, GID)
        self.assertIn('刷新资料 0 人', cached)
        self.assertEqual(self.ns['_store'].get_member_birthday('1001')['mmdd'], '07-10')

        # A manual choice is protected even by the forced full sync.
        self.ns['_store'].upsert_member_birthday('1002', '01-01', 'manual', force=True)
        bot.profiles['1002'] = {'birthday_month': 2, 'birthday_day': 2}
        bot.profiles['1003'] = {'birthday_month': 3, 'birthday_day': 3}
        full = await self.ns['sync_birthdays'](bot, GID, full=True)
        self.assertIn('全量', full)
        self.assertIn('刷新资料 2 人', full)
        self.assertIn('手动/关闭 1 人', full)
        self.assertEqual(self.ns['_store'].get_member_birthday('1001')['mmdd'], '12-25')
        self.assertEqual(self.ns['_store'].get_member_birthday('1002')['source'], 'manual')
        self.assertEqual(self.ns['_store'].get_member_birthday('1003')['source'], 'qq_profile')
        self.assertEqual(bot.stranger_uids().count(1002), 1)

    async def test_birthday_list_reads_db_with_sources_and_proximity(self) -> None:
        members = [MEMBER_1001, MEMBER_1002, MEMBER_1003, MEMBER_1004]
        bot = MockBot(members, {})
        store = self.ns['_store']
        store.upsert_member_birthday('1001', '12-30', 'manual', force=True)
        store.upsert_member_birthday('1002', '01-01', 'qq_profile')
        store.upsert_member_birthday('1003', None, 'disabled', force=True)
        store.upsert_member_birthday('1004', None, 'unknown')
        self.ns['now_shanghai'] = lambda: datetime(2026, 12, 30, 12, 0)

        text = await self.ns['birthday_overview'](bot, GID)
        self.assertIn('2 位成员有生效生日', text)
        self.assertIn('1 人关闭了自动祝福', text)
        self.assertIn('1 人未公开生日', text)
        rows = [line for line in text.splitlines() if '（1001）' in line or '（1002）' in line]
        self.assertEqual(len(rows), 2)
        self.assertIn('[手动]', rows[0])
        self.assertIn('12-30', rows[0])
        self.assertIn('今天', rows[0])
        self.assertIn('[资料]', rows[1])
        self.assertIn('01-01', rows[1])
        self.assertIn('2 天后', rows[1])
        self.assertEqual(bot.stranger_uids(), [])

    async def test_migrate_legacy_overrides_once(self) -> None:
        self.ns['_config_stub'].values['birthday_overrides'] = {
            '1001': '8-8', '1002': 'none', 'oops': '07-10',
        }
        result = await self.ns['migrate_legacy_birthday_overrides']()
        self.assertEqual(result['migrated'], 2)
        row = self.ns['_store'].get_member_birthday('1001')
        self.assertEqual((row['source'], row['mmdd']), ('manual', '08-08'))
        self.assertEqual(self.ns['_store'].get_member_birthday('1002')['source'], 'disabled')
        self.assertEqual(self.ns['_config_stub'].values['birthday_overrides'], {})

        second = await self.ns['migrate_legacy_birthday_overrides']()
        self.assertEqual(second['migrated'], 0)
        row = self.ns['_store'].get_member_birthday('1001')
        self.assertEqual((row['source'], row['mmdd']), ('manual', '08-08'))

    async def test_startup_sync_seeds_database_once(self) -> None:
        bot = MockBot([MEMBER_1001, MEMBER_1002], PROFILES)
        await self.ns['startup_birthday_sync'](bot)
        task = self.ns['STARTUP_SYNC']['task']
        self.assertIsNotNone(task)
        # A second connect event while the first sync is pending must not start another.
        await self.ns['startup_birthday_sync'](bot)
        self.assertIs(self.ns['STARTUP_SYNC']['task'], task)
        await task
        self.assertEqual(self.ns['_store'].get_member_birthday('1001')['source'], 'qq_profile')
        self.assertEqual(self.ns['_store'].get_member_birthday('1002')['source'], 'qq_profile')
        self.assertEqual(sorted(bot.stranger_uids()), [1001, 1002])

        await self.ns['_run_startup_birthday_sync'](bot)
        self.assertEqual(sorted(bot.stranger_uids()), [1001, 1002])
        self.assertEqual(bot.member_list_calls(), 2)  # one member list per startup sync

    async def test_describe_birthday_reads_db_and_queries_missing_once(self) -> None:
        bot = MockBot([MEMBER_1001], PROFILES)
        store = self.ns['_store']
        first = await self.ns['describe_birthday'](bot, GID, '2001', '1001')
        self.assertIn('来自 QQ 资料', first)
        self.assertEqual(bot.stranger_uids(), [1001])

        store.upsert_member_birthday('1001', '08-08', 'manual', force=True)
        self.assertIn('来自手动设置', await self.ns['describe_birthday'](bot, GID, '2001', '1001'))
        store.upsert_member_birthday('1001', None, 'disabled', force=True)
        self.assertIn('已关闭自动生日祝福',
                      await self.ns['describe_birthday'](bot, GID, '2001', '1001'))
        store.upsert_member_birthday('1001', None, 'unknown', force=True)
        self.assertIn('未公开生日', await self.ns['describe_birthday'](bot, GID, '2001', '1001'))
        self.assertEqual(bot.stranger_uids(), [1001])
        missing = await self.ns['describe_birthday'](MockBot([], {}), GID, '2001', '1001')
        self.assertIn('不在本群', missing)

    async def test_describe_and_clear_report_profile_failure(self) -> None:
        bot = MockBot([MEMBER_1001], {}, profile_errors={'1001'})
        reply = await self.ns['describe_birthday'](bot, GID, '2001', '1001')
        self.assertIn('资料失败', reply)
        self.assertIsNone(self.ns['_store'].get_member_birthday('1001'))

        self.ns['_superuser'].allowed.add('2002')
        clear_bot = MockBot([MEMBER_1001], {}, profile_errors={'1001'})
        cleared = await self.ns['set_birthday_for'](
            clear_bot, SimpleNamespace(user_id='2002'), '2002', '1001', '默认')
        self.assertIn('资料失败', cleared)
        self.assertIsNone(self.ns['_store'].get_member_birthday('1001'))


class BridgeContractTest(unittest.TestCase):
    def test_bridge_uses_configurable_usage_scene_and_readonly_memory_tools(self) -> None:
        source = (REPO / 'ai_runtime' / 'dsh' / 'bridge.mjs').read_text(encoding='utf-8')
        self.assertIn("process.env.CSBOT_USAGE_SCENE || 'chat'", source)
        self.assertIn("process.env.CSBOT_MEMORY_READONLY==='1'", source)
        self.assertIn("allow.delete('memory_save')", source)
        self.assertIn("allow.delete('memory_forget')", source)
        self.assertIn("const emitAndExit =", source)
        self.assertNotIn("process.exit(0);\n", source)

    def test_large_utf8_stdout_flush_before_exit(self) -> None:
        node = shutil.which('node')
        if not node:
            self.skipTest('node is not installed')
        import subprocess
        script = (
            "const payload = {type:'complete', text:'生日快乐'.repeat(16000)};"
            "const finish = () => process.exit(0);"
            "process.stdout.once('error', finish);"
            "process.stdout.write('CSBOT:' + JSON.stringify(payload) + '\\n', finish);"
        )
        result = subprocess.run([node, '-e', script], capture_output=True, timeout=30)
        self.assertEqual(result.returncode, 0, result.stderr.decode('utf-8', errors='replace'))
        raw = result.stdout
        self.assertGreater(len(raw), 128 * 1024)
        self.assertTrue(raw.startswith(b'CSBOT:'))
        decoded = json.loads(raw[6:].decode('utf-8'))
        self.assertEqual(decoded['type'], 'complete')
        self.assertEqual(len(decoded['text']), 4 * 16000)

    def test_runner_signature_and_snapshot_order(self) -> None:
        source = (REPO / 'ai_runtime' / 'runner.py').read_text(encoding='utf-8')
        tree = ast.parse(source)
        run_dsh = next(node for node in ast.walk(tree)
                       if isinstance(node, ast.AsyncFunctionDef) and node.name == 'run_dsh')
        args = {arg.arg for arg in run_dsh.args.kwonlyargs}
        self.assertTrue({'memory_source_dir', 'read_only_memory', 'usage_scene'} <= args)
        self.assertLess(source.index('_snapshot_memory_dir(Path(memory_source_dir)'),
                        source.index('create_subprocess_exec'))

    def test_node_syntax_check_when_available(self) -> None:
        node = shutil.which('node')
        if not node:
            self.skipTest('node is not installed')
        import subprocess
        result = subprocess.run(
            [node, '--check', str(REPO / 'ai_runtime' / 'dsh' / 'bridge.mjs')],
            capture_output=True, text=True, timeout=60,
        )
        self.assertEqual(result.returncode, 0, result.stderr)


if __name__ == '__main__':
    unittest.main()
