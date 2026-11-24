"""Offline checks for the resident screenshot Chromium pool.

No Chromium, network, database or NoneBot startup is used: fake browser objects
record the launch/context/page lifecycle, so reuse, cookie isolation through
separate incognito contexts, self-healing retries, shutdown and the cold-launch
fallback are deterministic.
"""

import asyncio
import importlib.util
import os
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

ROOT = Path(__file__).parents[1]
MODULE_PATH = ROOT / 'ai_runtime/screenshot_browser.py'


def load_module():
    spec = importlib.util.spec_from_file_location('screenshot_browser_under_test', MODULE_PATH)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


POOL = load_module()


class FakeProcess:
    def __init__(self):
        self.returncode = None
        self.killed = False

    def poll(self):
        return self.returncode

    def kill(self):
        self.killed = True
        self.returncode = -9


class FakePage:
    def __init__(self, context):
        self.context = context
        self.closed = False

    async def close(self):
        self.closed = True
        self.context.browser.closed_pages.append(self)


class FakeContext:
    def __init__(self, browser):
        self.browser = browser
        self.closed = False
        self.pages = []

    async def newPage(self):
        page = FakePage(self)
        self.pages.append(page)
        return page

    async def close(self):
        self.closed = True
        self.browser.closed_contexts.append(self)


class FakeBrowser:
    def __init__(self, launcher):
        self.launcher = launcher
        self.process = FakeProcess()
        self._connection = SimpleNamespace(_connected=True, _ws=SimpleNamespace(closed=False))
        self.contexts = []
        self.closed_contexts = []
        self.closed_pages = []
        self.page = None
        self.closed = False

    async def createIncognitoBrowserContext(self):
        context = FakeContext(self)
        self.contexts.append(context)
        return context

    async def newPage(self):
        self.page = FakePage(SimpleNamespace(browser=self))
        return self.page

    async def close(self):
        self.closed = True
        self.process.returncode = 0


class FakeLauncher:
    def __init__(self):
        self.calls = 0
        self.browsers = []
        self.kwargs = None

    async def __call__(self, **kwargs):
        self.calls += 1
        self.kwargs = kwargs
        browser = FakeBrowser(self)
        self.browsers.append(browser)
        return browser


class PoolTestCase(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.launcher = FakeLauncher()
        self.pool = POOL.ScreenshotBrowserPool(launcher=self.launcher)
        self.environment = patch.dict(os.environ, {'CS_SCREENSHOT_REUSE_BROWSER': '1'})
        self.environment.start()

    async def asyncTearDown(self):
        await self.pool.aclose()
        self.environment.stop()

    async def _render(self, page):
        return 'ok'

    # 1) sequential captures reuse one launch but never share a session
    async def test_sequential_captures_launch_once_and_isolate_sessions(self):
        contexts = []

        async def render(page):
            contexts.append(page.context)
            return 'image'

        for _ in range(3):
            self.assertEqual(await self.pool.run(render), 'image')

        self.assertEqual(self.launcher.calls, 1)
        self.assertFalse(self.launcher.browsers[0].closed)
        self.assertEqual(len(contexts), 3)
        self.assertEqual(len({id(context) for context in contexts}), 3)
        browser = self.launcher.browsers[0]
        self.assertEqual(len(browser.contexts), 3)
        for context in browser.contexts:
            self.assertTrue(context.closed)
            self.assertEqual(len(context.pages), 1)
            self.assertTrue(context.pages[0].closed)
        self.assertEqual(self.launcher.kwargs['args'], ['--no-sandbox'])
        self.assertTrue(self.launcher.kwargs['headless'])
        # The pool owns Chromium: pyppeteer must not hijack NoneBot's shutdown
        # signals or pile up one atexit closure per rebuild.
        self.assertFalse(self.launcher.kwargs['handleSIGINT'])
        self.assertFalse(self.launcher.kwargs['handleSIGTERM'])
        self.assertFalse(self.launcher.kwargs['autoClose'])

    # 2) concurrent first calls still launch exactly one browser
    async def test_concurrent_first_calls_launch_once(self):
        barrier = asyncio.Barrier(4)
        contexts = []

        async def render(page):
            contexts.append(page.context)
            await barrier.wait()
            return 'ok'

        results = await asyncio.gather(*(self.pool.run(render) for _ in range(4)))

        self.assertEqual(results, ['ok'] * 4)
        self.assertEqual(self.launcher.calls, 1)
        self.assertEqual(len({id(context) for context in contexts}), 4)
        self.assertEqual(len(self.launcher.browsers[0].contexts), 4)

    async def test_max_concurrency_limits_parallel_pages(self):
        with patch.dict(os.environ, {'CS_SCREENSHOT_MAX_CONCURRENCY': '2'}):
            active = peak = 0

            async def render(page):
                nonlocal active, peak
                active += 1
                peak = max(peak, active)
                await asyncio.sleep(0.02)
                active -= 1
                return 'ok'

            await asyncio.gather(*(self.pool.run(render) for _ in range(6)))

        self.assertEqual(peak, 2)
        self.assertEqual(self.launcher.calls, 1)
        self.assertEqual(len(self.launcher.browsers[0].contexts), 6)

    # 3) crash / disconnect self-healing
    async def test_dead_process_is_replaced_before_next_capture(self):
        await self.pool.run(self._render)
        self.launcher.browsers[0].process.returncode = 1

        self.assertEqual(await self.pool.run(self._render), 'ok')

        self.assertEqual(self.launcher.calls, 2)
        self.assertTrue(self.launcher.browsers[0].closed)
        self.assertFalse(self.launcher.browsers[1].closed)

    async def test_closed_connection_is_replaced(self):
        await self.pool.run(self._render)
        self.launcher.browsers[0]._connection._connected = False

        await self.pool.run(self._render)

        self.assertEqual(self.launcher.calls, 2)

    async def test_disconnect_during_capture_rebuilds_and_retries_once(self):
        pages = []

        async def render(page):
            pages.append(page)
            if len(pages) == 1:
                raise POOL.NetworkError('Protocol error (Page.captureScreenshot): Target closed.')
            return 'recovered'

        self.assertEqual(await self.pool.run(render), 'recovered')

        self.assertEqual(len(pages), 2)
        self.assertEqual(self.launcher.calls, 2)
        broken = self.launcher.browsers[0]
        self.assertTrue(broken.closed)
        self.assertEqual(len(broken.closed_contexts), 1)
        self.assertTrue(broken.closed_contexts[0].closed)
        self.assertTrue(broken.closed_contexts[0].pages[0].closed)
        healthy = self.launcher.browsers[1]
        self.assertFalse(healthy.closed)
        self.assertTrue(healthy.contexts[0].closed)
        self.assertTrue(healthy.contexts[0].pages[0].closed)

    async def test_repeated_disconnect_is_reported_after_single_retry(self):
        async def render(page):
            raise POOL.NetworkError('Target closed')

        with self.assertRaises(POOL.NetworkError):
            await self.pool.run(render)

        self.assertEqual(self.launcher.calls, 2)

    async def test_page_error_is_not_retried_but_session_is_closed(self):
        async def render(page):
            raise POOL.PageError('document.querySelector is not a function')

        with self.assertRaises(POOL.PageError):
            await self.pool.run(render)

        self.assertEqual(self.launcher.calls, 1)
        browser = self.launcher.browsers[0]
        self.assertFalse(browser.closed)
        self.assertTrue(browser.contexts[0].closed)
        self.assertTrue(browser.contexts[0].pages[0].closed)

    # 4) shutdown / explicit close
    async def test_explicit_close_reclaims_instance_and_allows_relaunch(self):
        await self.pool.run(self._render)
        browser = self.launcher.browsers[0]

        await self.pool.aclose()
        await self.pool.aclose()

        self.assertTrue(browser.closed)
        self.assertIsNotNone(browser.process.returncode)
        self.assertEqual(await self.pool.run(self._render), 'ok')
        self.assertEqual(self.launcher.calls, 2)

    async def test_close_kills_browser_process_left_running(self):
        await self.pool.run(self._render)
        browser = self.launcher.browsers[0]

        async def stubborn_close():
            browser.closed = True  # connection dropped but Chromium is still alive

        browser.close = stubborn_close
        await self.pool.aclose()

        self.assertTrue(browser.process.killed)

    async def test_module_shutdown_hook_closes_shared_pool(self):
        shared = POOL.ScreenshotBrowserPool(launcher=self.launcher)
        with patch.object(POOL, 'screenshot_browser', shared):
            await POOL.screenshot_browser.run(self._render)
            await POOL.close_screenshot_browser()

        self.assertTrue(self.launcher.browsers[0].closed)

    # 5) flag off: previous per-capture cold launch behaviour
    async def test_reuse_disabled_falls_back_to_per_capture_browser(self):
        pages = []

        async def render(page):
            pages.append(page)
            return 'ok'

        with patch.dict(os.environ, {'CS_SCREENSHOT_REUSE_BROWSER': '0'}):
            for _ in range(2):
                self.assertEqual(await self.pool.run(render), 'ok')

        self.assertEqual(len(pages), 2)
        self.assertEqual(self.launcher.calls, 2)
        for browser in self.launcher.browsers:
            self.assertTrue(browser.closed)
            self.assertIsNotNone(browser.process.returncode)
            self.assertEqual(browser.contexts, [])  # no incognito context in cold mode
            self.assertTrue(browser.page.closed)

    async def test_reuse_flag_parsing(self):
        for value, expected in [('1', True), ('true', True), ('0', False), ('false', False),
                                ('FALSE', False), ('off', False), ('no', False), ('yes', True)]:
            with self.subTest(value=value), patch.dict(os.environ, {'CS_SCREENSHOT_REUSE_BROWSER': value}):
                self.assertEqual(POOL.reuse_enabled(), expected)
        with patch.dict(os.environ, {}, clear=True):
            self.assertTrue(POOL.reuse_enabled())

    # 6) use-count recycling
    async def test_use_limit_recycles_browser_between_captures(self):
        with patch.dict(os.environ, {'CS_SCREENSHOT_BROWSER_MAX_USES': '2'}):
            for _ in range(3):
                self.assertEqual(await self.pool.run(self._render), 'ok')

        self.assertEqual(self.launcher.calls, 2)
        self.assertTrue(self.launcher.browsers[0].closed)
        self.assertFalse(self.launcher.browsers[1].closed)

    async def test_use_limit_waits_for_the_last_active_page(self):
        with patch.dict(os.environ, {'CS_SCREENSHOT_BROWSER_MAX_USES': '1',
                                     'CS_SCREENSHOT_MAX_CONCURRENCY': '4'}):
            started = asyncio.Event()
            release = asyncio.Event()

            async def slow(page):
                started.set()
                await release.wait()
                return 'slow'

            slow_task = asyncio.create_task(self.pool.run(slow))
            await started.wait()
            self.assertEqual(await self.pool.run(self._render), 'ok')

            browser = self.launcher.browsers[0]
            self.assertFalse(browser.closed)  # still serving the active page
            release.set()
            self.assertEqual(await slow_task, 'slow')

            self.assertTrue(browser.closed)  # recycled once idle
            self.assertEqual(self.launcher.calls, 1)
            self.assertEqual(await self.pool.run(self._render), 'ok')
            self.assertEqual(self.launcher.calls, 2)


class DisconnectClassificationTest(unittest.TestCase):
    def test_connection_failures_are_retryable_and_page_failures_are_not(self):
        self.assertTrue(POOL.is_disconnect_error(POOL.NetworkError('Target closed')))
        self.assertTrue(POOL.is_disconnect_error(POOL.BrowserError('Failed to create page.')))
        self.assertTrue(POOL.is_disconnect_error(ConnectionError('Connection is closed')))
        self.assertTrue(POOL.is_disconnect_error(RuntimeError('browser has disconnected')))
        self.assertFalse(POOL.is_disconnect_error(POOL.PageError('document is not defined')))
        self.assertFalse(POOL.is_disconnect_error(asyncio.TimeoutError()))
        self.assertFalse(POOL.is_disconnect_error(asyncio.CancelledError()))
        self.assertFalse(POOL.is_disconnect_error(ValueError('bad selector')))


if __name__ == '__main__':
    unittest.main()
