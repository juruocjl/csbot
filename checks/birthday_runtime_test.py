"""Birthday runtime checks: temp dirs + mock OneBot, no network/DB/QQ.

Covers StateStore persistence, the SQLite memory snapshot, the DSH runner
environment contract and the 23:50 -> 00:00 scheduler flow.
"""
from __future__ import annotations

import ast
import asyncio
from contextlib import nullcontext
from datetime import date
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
        'now_shanghai', '_background_disabled', '_group_members', '_display_name',
        '_known_override', '_profile_birthday', '_resolve_member', 'fetch_group_birthdays',
        '_generate_wish', 'pregen_birthdays', '_send_notice', 'dispatch_birthdays_for_date',
        '_is_superuser', '_parse_target', 'describe_birthday', 'set_birthday_for',
        'birthday_overview', 'send_test_wish',
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
        'MemberBirthday': LOGIC.MemberBirthday,
        'BirthdayResolution': LOGIC.BirthdayResolution,
        'CLEAR_OVERRIDE': LOGIC.CLEAR_OVERRIDE,
        'DISABLED_VALUE': LOGIC.DISABLED_VALUE,
        'build_birthday_prompt': LOGIC.build_birthday_prompt,
        'check_cooldown': LOGIC.check_cooldown,
        'format_cooldown_remaining': LOGIC.format_cooldown_remaining,
        'matches_birthday_on_date': LOGIC.matches_birthday_on_date,
        'parse_birthday_input': LOGIC.parse_birthday_input,
        'resolve_effective_birthday': LOGIC.resolve_effective_birthday,
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
                 notice_error: Exception | None = None) -> None:
        self.members = members
        self.profiles = profiles or {}
        self.notice_error = notice_error
        self.calls: list[tuple] = []
        self.group_messages: list[tuple] = []

    async def get_group_member_list(self, group_id, no_cache=False):
        self.calls.append(('member_list', group_id, no_cache))
        return list(self.members)

    async def get_stranger_info(self, user_id, no_cache=True):
        self.calls.append(('stranger', user_id, no_cache))
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


MEMBER_1001 = {'user_id': 1001, 'nickname': '小明', 'card': '明', 'is_robot': False}
MEMBER_1002 = {'user_id': 1002, 'nickname': '小红', 'card': '', 'is_robot': False}
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

        sent_again = await self.ns['dispatch_birthdays_for_date'](bot, date(2026, 7, 10))
        self.assertEqual(sent_again, 0)
        self.assertEqual(len(bot.notices()), 1)

    async def test_disable_between_pregen_and_dispatch_skips_send(self) -> None:
        bot = MockBot([MEMBER_1001], PROFILES)
        await self.ns['pregen_birthdays'](bot, date(2026, 7, 10))
        self.ns['_config_stub'].values['birthday_overrides'] = {'1001': 'none'}
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

    async def test_command_permissions_and_cooldown(self) -> None:
        bot = MockBot([])
        event = SimpleNamespace(user_id='2001')
        ns = self.ns

        first = await ns['set_birthday_for'](bot, event, '2001', '2001', '7月10日')
        self.assertIn('设为 07-10', first)
        self.assertEqual(ns['_config_stub'].values['birthday_overrides'], {'2001': '07-10'})
        self.assertIsNotNone(ns['_store'].get_birthday_cooldown('2001'))

        again = await ns['set_birthday_for'](bot, event, '2001', '2001', '08-11')
        self.assertIn('冷却', again)
        self.assertEqual(ns['_config_stub'].values['birthday_overrides'], {'2001': '07-10'})

        other = await ns['set_birthday_for'](bot, event, '2001', '1001', '09-09')
        self.assertIn('只能修改自己的生日', other)

        ns['_superuser'].allowed.add('2002')
        super_event = SimpleNamespace(user_id='2002')
        changed = await ns['set_birthday_for'](bot, super_event, '2002', '1001', '09-09')
        self.assertIn('设为 09-09', changed)
        self.assertEqual(ns['_config_stub'].values['birthday_overrides']['1001'], '09-09')
        self.assertIsNone(ns['_store'].get_birthday_cooldown('2002'))

        cleared = await ns['set_birthday_for'](bot, super_event, '2002', '1001', '默认')
        self.assertIn('恢复读取 QQ 资料', cleared)
        self.assertNotIn('1001', ns['_config_stub'].values['birthday_overrides'])

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
