"""Exercise successful send receipts and delayed recalls with mock OneBot only."""
import ast
import asyncio
import importlib.util
from pathlib import Path
import sys
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, Mock, patch

ROOT = Path(__file__).parents[1]
sys.path.insert(0, str(ROOT))
from ai_runtime.image_archive import MARKER, snapshot_image


def load_file(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


TEMPORARY = load_file('temporary_image_acceptance', ROOT/'plugins/utils/message_recall.py')
CONFIG = load_file('temporary_schedule_config', ROOT/'plugins/hltv_watcher/config.py').Config


def load_render(server):
    path = ROOT/'plugins/hltv_watcher/commands.py'
    node = next(n for n in ast.parse(path.read_text()).body
                if isinstance(n, ast.AsyncFunctionDef) and n.name == 'render_schedule')
    namespace = {'Bot': object, 'require': lambda _: server, 'logger': Mock(),
                 'config': CONFIG(cs_group_list=[]),
                 'send_temporary_image': TEMPORARY.send_temporary_image}
    exec(compile(ast.Module(body=[node], type_ignores=[]), str(path), 'exec'), namespace)
    return namespace['render_schedule']


class TemporaryImageAcceptance(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.bot = SimpleNamespace(delete_msg=AsyncMock())
        self.image = snapshot_image(b'fixture', '赛程', {'matches': [{'id': 'match-1'}]})

    async def asyncTearDown(self):
        tasks = list(TEMPORARY._delete_tasks)
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def test_countdown_starts_after_successful_send_and_keeps_image_metadata(self):
        sending, acknowledged, timer_started, elapsed = (asyncio.Event() for _ in range(4))
        async def send(segment):
            self.assertIn(MARKER, segment.data)
            sending.set()
            await acknowledged.wait()
            return {'message_id': 123}
        async def sleep(seconds):
            self.assertEqual(seconds, 90)
            timer_started.set()
            await elapsed.wait()
        with patch.object(TEMPORARY.asyncio, 'sleep', side_effect=sleep):
            task = asyncio.create_task(TEMPORARY.send_temporary_image(self.bot, send, self.image, 90))
            await sending.wait()
            self.assertFalse(TEMPORARY._delete_tasks)
            self.assertFalse(timer_started.is_set())
            acknowledged.set()
            self.assertEqual(await task, {'message_id': 123})
            await timer_started.wait()
            self.bot.delete_msg.assert_not_awaited()
            pending = list(TEMPORARY._delete_tasks)
            elapsed.set()
            await asyncio.gather(*pending)
        self.bot.delete_msg.assert_awaited_once_with(message_id=123)
        self.assertFalse(TEMPORARY._delete_tasks)

    async def test_zero_disables_recall_but_still_sends_image(self):
        send = AsyncMock(return_value={'message_id': 123})
        result = await TEMPORARY.send_temporary_image(self.bot, send, self.image, 0)
        self.assertEqual(result, {'message_id': 123})
        send.assert_awaited_once()
        self.assertFalse(TEMPORARY._delete_tasks)
        self.bot.delete_msg.assert_not_awaited()

    async def test_invalid_receipts_never_recall_an_unrelated_message(self):
        with patch.object(TEMPORARY, 'logger', Mock()):
            for receipt in [None, {}, {'message_id': None}, {'message_id': True},
                            {'message_id': 1.5}, {'message_id': 'invalid'}]:
                with self.subTest(receipt=receipt):
                    await TEMPORARY.send_temporary_image(self.bot, AsyncMock(return_value=receipt), self.image, 90)
                    self.assertFalse(TEMPORARY._delete_tasks)
        self.bot.delete_msg.assert_not_awaited()

    async def test_send_failure_does_not_start_timer(self):
        with self.assertRaises(RuntimeError):
            await TEMPORARY.send_temporary_image(self.bot, AsyncMock(side_effect=RuntimeError('send failed')), self.image, 90)
        self.assertFalse(TEMPORARY._delete_tasks)
        self.bot.delete_msg.assert_not_awaited()

    async def test_recall_failure_is_logged_and_task_is_released(self):
        self.bot.delete_msg.side_effect = RuntimeError('offline')
        with patch.object(TEMPORARY.asyncio, 'sleep', AsyncMock()), patch.object(TEMPORARY, 'logger', Mock()) as logger:
            await TEMPORARY.send_temporary_image(self.bot, AsyncMock(return_value={'message_id': '-123'}), self.image, 90)
            await asyncio.gather(*list(TEMPORARY._delete_tasks))
            logger.exception.assert_called_once()
        self.bot.delete_msg.assert_awaited_once_with(message_id=-123)
        self.assertFalse(TEMPORARY._delete_tasks)

    async def test_both_schedule_paths_send_once_and_recall_only_the_receipt(self):
        for personal, path in [(False, '/schedule?page=2'), (True, '/schedule/user/111?page=2')]:
            with self.subTest(personal=personal), patch.object(TEMPORARY.asyncio, 'sleep', AsyncMock()) as sleep:
                bot = SimpleNamespace(delete_msg=AsyncMock())
                server = SimpleNamespace(db=SimpleNamespace(get_bot_token=AsyncMock(return_value='fixture')),
                                         get_screenshot=AsyncMock(return_value=self.image))
                matcher = SimpleNamespace(send=AsyncMock(return_value={'message_id': 222}), finish=AsyncMock())
                await load_render(server)(bot, SimpleNamespace(group_id=100, user_id=111),
                    SimpleNamespace(extract_plain_text=lambda: '2'), matcher, personal)
                await asyncio.gather(*list(TEMPORARY._delete_tasks))
                server.get_screenshot.assert_awaited_once_with(path, 'fixture', width=1100)
                matcher.send.assert_awaited_once()
                self.assertIn(MARKER, matcher.send.await_args.args[0].data)
                matcher.finish.assert_awaited_once_with()
                sleep.assert_awaited_once_with(90)
                bot.delete_msg.assert_awaited_once_with(message_id=222)

    async def test_error_and_usage_messages_have_no_recall_timer(self):
        server = SimpleNamespace(db=SimpleNamespace(get_bot_token=AsyncMock(return_value='fixture')),
                                 get_screenshot=AsyncMock(return_value=None))
        for arg in ['bad-page', '1']:
            matcher = SimpleNamespace(send=AsyncMock(), finish=AsyncMock())
            await load_render(server)(self.bot, SimpleNamespace(group_id=100, user_id=111),
                SimpleNamespace(extract_plain_text=lambda: arg), matcher)
            matcher.send.assert_not_awaited()
            matcher.finish.assert_awaited_once()
            self.assertFalse(TEMPORARY._delete_tasks)

    async def test_configuration_default_disable_and_validation(self):
        self.assertEqual(CONFIG(cs_group_list=[]).cs_schedule_auto_delete_seconds, 90)
        self.assertEqual(CONFIG(cs_group_list=[], cs_schedule_auto_delete_seconds=0).cs_schedule_auto_delete_seconds, 0)
        with self.assertRaises(ValueError):
            CONFIG(cs_group_list=[], cs_schedule_auto_delete_seconds=-1)


if __name__ == '__main__': unittest.main()
