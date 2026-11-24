"""Resident headless Chromium shared by every page screenshot.

One lazily launched Chromium serves all captures.  Each capture runs in its own
incognito browser context, so cookies (including the web `token` cookie) and
local storage never leak between renders.  The shared instance is rebuilt when
its process or DevTools connection dies, when a capture fails because the
browser disconnected, and after a configurable number of uses; callers get one
transparent retry before the original error is reported.

The module deliberately has no NoneBot dependency: it is imported by the
plugins that render pages and exercised directly by offline unit tests.
"""

import asyncio
import logging
import os

from pyppeteer import launch as default_launch
from pyppeteer.errors import BrowserError, NetworkError, PageError

try:  # websockets is a pyppeteer dependency; keep the import defensive.
    from websockets.exceptions import ConnectionClosed as _WebSocketClosed
except Exception:  # pragma: no cover - dependency is always installed
    _WebSocketClosed = ()

LAUNCH_ARGS = ['--no-sandbox']
DEFAULT_MAX_CONCURRENCY = 4
DEFAULT_MAX_USES = 200
CLOSE_TIMEOUT = 10.0

# Chromium lifetime belongs to this pool: NoneBot's driver.on_shutdown and the
# rebuild path close it.  pyppeteer must therefore not replace the process-wide
# SIGINT/SIGTERM handlers (it would bypass NoneBot's graceful shutdown) nor
# register one atexit closure per launch (they accumulate across rebuilds and
# cannot run once the event loop is closed).
LAUNCH_OPTIONS = {
    'headless': True,
    'args': LAUNCH_ARGS,
    'handleSIGINT': False,
    'handleSIGTERM': False,
    'handleSIGHUP': False,
    'autoClose': False,
}

logger = logging.getLogger(__name__)


def _positive_int(name, default):
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        value = int(str(raw).strip())
    except (TypeError, ValueError):
        return default
    return value if value > 0 else default


def reuse_enabled():
    """Whether the resident-browser mode is on (default: on)."""
    raw = str(os.getenv('CS_SCREENSHOT_REUSE_BROWSER', '1')).strip().lower()
    return raw not in {'0', 'false', 'no', 'off'}


_DISCONNECT_MARKERS = (
    'target closed',
    'connection closed',
    'connection is closed',
    'browser has disconnected',
    'browser disconnected',
    'browser closed',
    'websocket',
    'protocol error',
    'page crashed',
    'target crashed',
    'session closed',
)


def is_disconnect_error(exc):
    """True only when the shared browser itself is no longer usable.

    Ordinary page failures (selector timeouts, JavaScript errors, HTTP errors)
    return False so they are reported immediately instead of retried forever.
    """
    if isinstance(exc, asyncio.CancelledError):
        return False
    if isinstance(exc, PageError):
        return False
    if isinstance(exc, (BrowserError, NetworkError, ConnectionError)):
        return True
    if _WebSocketClosed and isinstance(exc, _WebSocketClosed):
        return True
    text = str(exc).lower()
    return any(marker in text for marker in _DISCONNECT_MARKERS)


def browser_healthy(browser):
    """Check the shared browser's process and DevTools connection."""
    if browser is None:
        return False
    process = getattr(browser, 'process', None)
    if process is not None and process.poll() is not None:
        return False
    connection = getattr(browser, '_connection', None)
    if connection is not None:
        if not getattr(connection, '_connected', True):
            return False
        websocket = getattr(connection, '_ws', None)
        if websocket is not None and getattr(websocket, 'closed', False):
            return False
    return True


async def _shutdown_browser(browser, log):
    """Close a browser and make sure the Chromium process is gone."""
    if browser is None:
        return
    cancelled = False
    try:
        await asyncio.wait_for(browser.close(), timeout=CLOSE_TIMEOUT)
    except asyncio.CancelledError:
        cancelled = True
    except Exception as exc:
        log.warning(f'failed to close screenshot browser cleanly: {exc!r}')
    process = getattr(browser, 'process', None)
    if process is not None and process.poll() is None:
        try:
            process.kill()
        except Exception as exc:
            log.warning(f'failed to kill screenshot browser process: {exc!r}')
    if cancelled:
        raise asyncio.CancelledError


async def _close_quietly(target, log):
    """Close a page or context without masking the capture's own error."""
    if target is None:
        return
    try:
        await asyncio.wait_for(target.close(), timeout=CLOSE_TIMEOUT)
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        # A page on a dead connection cannot be closed; the browser rebuild
        # (or the next acquire) reclaims it and the browser process.
        log.debug(f'failed to close screenshot page/context: {exc!r}')


class ScreenshotBrowserPool:
    """Lazily launched, reused Chromium with isolated per-capture contexts."""

    def __init__(self, launcher=None):
        self._launch = launcher or default_launch
        self.logger = logger
        self._browser = None
        self._lock = asyncio.Lock()
        self._semaphore = None
        self._semaphore_limit = None
        self._uses = 0
        self._active = 0
        self._stale = False

    async def run(self, render):
        """Run ``await render(page)`` in a private context and return its result."""
        limit = _positive_int('CS_SCREENSHOT_MAX_CONCURRENCY', DEFAULT_MAX_CONCURRENCY)
        async with self._semaphore_for(limit):
            if not reuse_enabled():
                return await self._run_cold(render)
            return await self._run_reused(render)

    async def aclose(self):
        """Close the shared browser; safe to call repeatedly."""
        async with self._lock:
            browser, self._browser = self._browser, None
            self._uses = 0
            self._stale = False
        if browser is not None:
            await _shutdown_browser(browser, self.logger)

    async def close(self):
        await self.aclose()

    def _semaphore_for(self, limit):
        if self._semaphore is None or self._semaphore_limit != limit:
            self._semaphore = asyncio.Semaphore(limit)
            self._semaphore_limit = limit
        return self._semaphore

    async def _launch_browser(self):
        return await self._launch(**dict(LAUNCH_OPTIONS, args=list(LAUNCH_ARGS)))

    async def _run_cold(self, render):
        """Previous behaviour: one Chromium per capture, closed afterwards."""
        browser = await self._launch_browser()
        page = None
        try:
            page = await browser.newPage()
            return await render(page)
        finally:
            await _close_quietly(page, self.logger)
            await _shutdown_browser(browser, self.logger)

    async def _run_reused(self, render):
        for attempt in (0, 1):
            browser = context = page = None
            counted = False
            result = None
            error = None
            try:
                browser = await self._acquire()
                self._active += 1
                counted = True
                context = await browser.createIncognitoBrowserContext()
                page = await context.newPage()
                result = await render(page)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                error = exc
            else:
                await self._account_success()
            finally:
                # Always release the private session before touching the browser.
                await _close_quietly(page, self.logger)
                await _close_quietly(context, self.logger)
                if counted:
                    self._active -= 1
                await self._recycle_if_idle()
            if error is None:
                return result
            if attempt == 0 and browser is not None and is_disconnect_error(error):
                self.logger.warning(f'screenshot browser disconnected; rebuilding and retrying once: {error!r}')
                await self._discard(browser)
                continue
            raise error
        raise RuntimeError('unreachable screenshot retry state')

    async def _acquire(self):
        async with self._lock:
            if self._browser is not None:
                if not browser_healthy(self._browser):
                    self.logger.warning('screenshot browser is unhealthy; rebuilding')
                    await self._discard_locked()
                elif self._stale and self._active == 0:
                    await self._discard_locked()
            if self._browser is None:
                self._browser = await self._launch_browser()
                self._uses = 0
                self._stale = False
            return self._browser

    async def _discard(self, browser):
        async with self._lock:
            if self._browser is browser:
                self._browser = None
                self._uses = 0
                self._stale = False
        await _shutdown_browser(browser, self.logger)

    async def _discard_locked(self):
        browser, self._browser = self._browser, None
        self._uses = 0
        self._stale = False
        await _shutdown_browser(browser, self.logger)

    async def _account_success(self):
        self._uses += 1
        if self._uses >= _positive_int('CS_SCREENSHOT_BROWSER_MAX_USES', DEFAULT_MAX_USES):
            self._stale = True

    async def _recycle_if_idle(self):
        browser = None
        async with self._lock:
            if self._browser is not None and self._stale and self._active == 0:
                browser, self._browser = self._browser, None
                self._uses = 0
                self._stale = False
        if browser is not None:
            self.logger.info('recycling long-lived screenshot browser after use limit')
            await _shutdown_browser(browser, self.logger)


screenshot_browser = ScreenshotBrowserPool()


async def close_screenshot_browser():
    """NoneBot shutdown hook target: release the resident Chromium."""
    await screenshot_browser.aclose()
