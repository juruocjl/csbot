"""JS-rendering web fetch that reuses the resident screenshot Chromium.

`web_fetch` (native DSH HTTP fetch) is cheaper and stays the default.  This
module is the fallback for pages whose visible text only exists after client
side JavaScript runs.  It never starts a second Chromium: it reuses the single
instance owned by :mod:`ai_runtime.screenshot_browser` and every capture gets
its own incognito context, so cookies and storage never leak between fetches
or into page screenshots.

Security is the primary concern: the URL, its DNS answers and every single
request (including each redirect hop and every subresource) are validated
before Chromium may see them.  Private, loopback, link-local, CGNAT/Tailscale,
metadata and non-HTTP(S) targets are refused fail-closed, and the CDP
`remoteIPAddress` of every response is checked again so a DNS-rebinding answer
cannot smuggle content back.

The module deliberately has no NoneBot dependency: it is imported by the DSH
runner and exercised directly by offline unit tests.
"""

import asyncio
import inspect
import ipaddress
import os
import re
import socket
import weakref
from urllib.parse import urlsplit

from .screenshot_browser import is_disconnect_error, screenshot_browser

DEFAULT_TIMEOUT_SECONDS = 18.0
DEFAULT_SETTLE_SECONDS = 2.5
DEFAULT_MAX_CHARS = 40000
DEFAULT_MAX_CONCURRENCY = 2
MAX_URL_LENGTH = 2048
MAX_REDIRECTS = 5
BLOCKED_RESOURCE_TYPES = frozenset({
    'image', 'media', 'font', 'texttrack', 'manifest', 'ping',
    'cspviolationreport', 'websocket', 'eventsource',
})
# Anonymous stock Chrome UA: no HeadlessChrome marker, no site identifiers.
USER_AGENT = (
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
    '(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36'
)

_BLOCKED_HOSTNAMES = frozenset({
    'localhost', 'metadata', 'metadata.google.internal', 'metadata.goog',
    'instance-data', 'ip6-localhost', 'ip6-loopback',
})
_BLOCKED_SUFFIXES = (
    '.localhost', '.local', '.internal', '.localdomain', '.home.arpa', '.ts.net',
)
_IPV4_BLOCKED_NETWORKS = tuple(ipaddress.ip_network(value) for value in (
    '0.0.0.0/8', '10.0.0.0/8', '100.64.0.0/10', '127.0.0.0/8',
    '169.254.0.0/16', '172.16.0.0/12', '192.0.0.0/24', '192.0.2.0/24',
    '192.168.0.0/16', '198.18.0.0/15', '198.51.100.0/24', '203.0.113.0/24',
    '224.0.0.0/4', '240.0.0.0/4',
))
_IPV6_BLOCKED_NETWORKS = tuple(ipaddress.ip_network(value) for value in (
    '64:ff9b::/96', '64:ff9b:1::/48', '::/96', 'fc00::/7', 'fe80::/10',
    'ff00::/8', '100::/64', '2001:db8::/32', '::1/128', '::/128', 'fec0::/10',
))
_NUMERIC_LABEL = re.compile(r'^(?:0[xX][0-9a-fA-F]+|[0-9]+)$')

_DISABLE_JS = r"""
() => {
  const deny = function () {
    throw new DOMException('Blocked by CSBot policy', 'SecurityError');
  };
  for (const key of ['WebSocket', 'EventSource', 'RTCPeerConnection',
                     'webkitRTCPeerConnection', 'mozRTCPeerConnection']) {
    try { Object.defineProperty(window, key, {configurable: true, writable: false, value: deny}); }
    catch (error) {}
  }
  try { window.open = () => null; } catch (error) {}
}
"""

_EXTRACT_JS = r"""
() => {
  const removable = 'script, style, noscript, template, svg, canvas, iframe, object, embed';
  try {
    if (document.body) {
      for (const node of document.body.querySelectorAll(removable)) node.remove();
      for (const node of document.body.querySelectorAll('[hidden], [aria-hidden="true"]')) node.remove();
    }
  } catch (error) {}
  const body = document.body;
  const root = document.documentElement;
  let text = '';
  try {
    text = (body && body.innerText) || (root && root.innerText) || (body && body.textContent) || '';
  } catch (error) { text = ''; }
  return {title: String(document.title || '').slice(0, 300), text: String(text || '')};
}
"""

_SEMAPHORES = weakref.WeakKeyDictionary()


class BrowserFetchError(ValueError):
    """Safe, model-facing browser fetch error; never treated as browser crash."""

    retryable_browser_disconnect = False


def _float_env(name, default, low, high):
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        value = float(str(raw).strip())
    except (TypeError, ValueError):
        return default
    if value != value:  # NaN
        return default
    return min(high, max(low, value))


def _int_env(name, default, low, high):
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        value = int(str(raw).strip())
    except (TypeError, ValueError):
        return default
    return min(high, max(low, value))


def _effective_timeout(timeout_seconds=None):
    if timeout_seconds is not None:
        try:
            value = float(timeout_seconds)
        except (TypeError, ValueError):
            value = DEFAULT_TIMEOUT_SECONDS
        return value if value > 0 else DEFAULT_TIMEOUT_SECONDS
    return _float_env('CS_AI_BROWSER_FETCH_TIMEOUT_SECONDS', DEFAULT_TIMEOUT_SECONDS, 5.0, 30.0)


def _effective_max_chars(max_chars=None):
    if max_chars is not None:
        try:
            value = int(max_chars)
        except (TypeError, ValueError):
            value = DEFAULT_MAX_CHARS
        return min(100000, max(1000, value))
    return _int_env('CS_AI_BROWSER_FETCH_MAX_CHARS', DEFAULT_MAX_CHARS, 1000, 100000)


def _effective_max_concurrency(max_concurrency=None):
    if max_concurrency is not None:
        try:
            value = int(max_concurrency)
        except (TypeError, ValueError):
            value = DEFAULT_MAX_CONCURRENCY
        return min(4, max(1, value))
    return _int_env('CS_AI_BROWSER_FETCH_MAX_CONCURRENCY', DEFAULT_MAX_CONCURRENCY, 1, 4)


def _semaphore_for(limit):
    """One low-concurrency gate per (event loop, limit) so tests stay isolated."""
    loop = asyncio.get_running_loop()
    per_loop = _SEMAPHORES.get(loop)
    if per_loop is None:
        per_loop = {}
        try:
            _SEMAPHORES[loop] = per_loop
        except TypeError:  # pragma: no cover - non weak-referenceable loop
            pass
    semaphore = per_loop.get(limit)
    if semaphore is None:
        semaphore = asyncio.Semaphore(limit)
        per_loop[limit] = semaphore
    return semaphore


def is_public_ip(raw_ip):
    """True only for a globally routable unicast IP literal."""
    text = str(raw_ip or '').strip()
    if text.startswith('[') and text.endswith(']'):
        text = text[1:-1].strip()
    try:
        address = ipaddress.ip_address(text)
    except ValueError:
        return False
    if isinstance(address, ipaddress.IPv6Address):
        if address.ipv4_mapped is not None:
            return is_public_ip(str(address.ipv4_mapped))
        if address.sixtofour is not None or address.teredo is not None:
            return False
        if any(address in network for network in _IPV6_BLOCKED_NETWORKS):
            return False
        return bool(
            address.is_global
            and not address.is_private
            and not address.is_loopback
            and not address.is_link_local
            and not address.is_multicast
            and not address.is_reserved
            and not address.is_unspecified
            and not address.is_site_local
        )
    if any(address in network for network in _IPV4_BLOCKED_NETWORKS):
        return False
    return bool(
        address.is_global
        and not address.is_private
        and not address.is_loopback
        and not address.is_link_local
        and not address.is_multicast
        and not address.is_reserved
        and not address.is_unspecified
    )


def validate_url_syntax(raw_url):
    """Validate an anonymous public HTTP(S) URL; return (url, host, port)."""
    if not isinstance(raw_url, str):
        raise BrowserFetchError('URL must be a string')
    normalized_url = raw_url.strip()
    if not normalized_url:
        raise BrowserFetchError('URL is empty')
    if len(normalized_url) > MAX_URL_LENGTH:
        raise BrowserFetchError(f'URL exceeds {MAX_URL_LENGTH} characters')
    if any(ord(char) < 32 or ord(char) == 127 for char in normalized_url):
        raise BrowserFetchError('URL contains control characters')
    try:
        parsed = urlsplit(normalized_url)
    except ValueError:
        raise BrowserFetchError('Invalid URL') from None
    scheme = parsed.scheme.lower()
    if scheme not in ('http', 'https'):
        raise BrowserFetchError('Only http and https URLs are allowed')
    if parsed.username or parsed.password:
        raise BrowserFetchError('Authenticated URLs are not allowed')
    if not parsed.hostname:
        raise BrowserFetchError('URL is missing a host')
    try:
        port = parsed.port
    except ValueError:
        raise BrowserFetchError('URL has an invalid port') from None
    if port is None:
        port = 443 if scheme == 'https' else 80
    if not 1 <= port <= 65535:
        raise BrowserFetchError('URL has an invalid port')
    host = parsed.hostname.lower().rstrip('.')
    if not host:
        raise BrowserFetchError('URL is missing a host')
    if host in _BLOCKED_HOSTNAMES or host.endswith(_BLOCKED_SUFFIXES):
        raise BrowserFetchError(f'Blocked host: {host}')
    try:
        literal = ipaddress.ip_address(host)
    except ValueError:
        literal = None
    if literal is not None:
        if not is_public_ip(host):
            raise BrowserFetchError(f'Blocked non-public IP: {host}')
        return normalized_url, host, port
    labels = host.split('.')
    if len(labels) < 2 or any(not label for label in labels):
        raise BrowserFetchError(f'Blocked host: {host}')
    if all(_NUMERIC_LABEL.match(label) for label in labels):
        raise BrowserFetchError(f'Blocked numeric host: {host}')
    return normalized_url, host, port


async def _default_resolver(host, port, dns_timeout):
    loop = asyncio.get_running_loop()
    return await asyncio.wait_for(
        loop.getaddrinfo(host, port, type=socket.SOCK_STREAM), timeout=dns_timeout)


def _resolver_addresses(result):
    addresses = []
    for item in result or ():
        if isinstance(item, str):
            addresses.append(item)
        elif isinstance(item, (tuple, list)) and len(item) >= 5:
            addresses.append(str(item[4][0]))
        elif isinstance(item, (tuple, list)) and item:
            addresses.append(str(item[0]))
        else:
            addresses.append(str(item))
    return [address for address in addresses if address]


async def resolve_and_validate_url(raw_url, *, resolver=None, dns_timeout=5.0):
    """Resolve a URL's host and fail closed on any non-public answer."""
    normalized_url, host, port = validate_url_syntax(raw_url)
    try:
        ipaddress.ip_address(host)
    except ValueError:
        pass
    else:
        return normalized_url, [host]
    try:
        if resolver is None:
            result = await _default_resolver(host, port, dns_timeout)
        else:
            result = resolver(host, port)
            if inspect.isawaitable(result):
                result = await result
    except asyncio.CancelledError:
        raise
    except Exception:
        raise BrowserFetchError(f'Could not resolve host: {host}') from None
    addresses = _resolver_addresses(result)
    if not addresses:
        raise BrowserFetchError(f'Could not resolve host: {host}')
    for address in addresses:
        if not is_public_ip(address):
            raise BrowserFetchError(f'Blocked URL resolving to non-public IP: {host}')
    return normalized_url, addresses


def classify_content_type(content_type):
    """Map a response MIME type to 'html', 'text' or None (unsupported)."""
    if content_type is None:
        return 'html'
    mime = str(content_type).split(';', 1)[0].strip().lower()
    if not mime:
        return 'html'
    if mime in ('text/html', 'application/xhtml+xml'):
        return 'html'
    if (mime.startswith('text/') or mime in ('application/json', 'application/xml')
            or mime.endswith('+json') or mime.endswith('+xml')):
        return 'text'
    return None


def clean_extracted_text(raw_text, max_chars):
    """Normalize rendered text and bound it to ``max_chars``."""
    text = str(raw_text or '').replace('\r\n', '\n').replace('\r', '\n')
    text = '\n'.join(line.rstrip() for line in text.split('\n')).strip()
    while '\n\n\n' in text:
        text = text.replace('\n\n\n', '\n\n')
    if max_chars is not None and len(text) > max_chars:
        return text[:max_chars], True
    return text, False


async def _dismiss_quietly(dialog):
    try:
        dismiss = getattr(dialog, 'dismiss', None)
        if dismiss is not None:
            await dismiss()
    except Exception:
        pass


async def _abort_quietly(request, error_code):
    try:
        await request.abort(error_code)
    except Exception:
        pass


async def _render(page, normalized_url, *, resolver, timeout_seconds, max_chars):
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout_seconds
    blocked_error = None
    blocked_subresources = 0
    pending_tasks = set()
    active_requests = set()

    def _remember_blocked(error):
        nonlocal blocked_error
        if blocked_error is None:
            blocked_error = error

    if hasattr(page, 'setUserAgent'):
        await page.setUserAgent(USER_AGENT)
    if hasattr(page, 'setViewport'):
        await page.setViewport({'width': 1280, 'height': 800})
    if hasattr(page, 'on'):
        def _on_dialog(dialog):
            asyncio.create_task(_dismiss_quietly(dialog))
        page.on('dialog', _on_dialog)
    if hasattr(page, 'evaluateOnNewDocument'):
        await page.evaluateOnNewDocument(_DISABLE_JS)

    cdp_client = getattr(page, '_client', None)
    cdp_handler = None

    def _on_response_received(event):
        try:
            response = (event or {}).get('response') or {}
            remote_ip = response.get('remoteIPAddress')
            if remote_ip and not is_public_ip(str(remote_ip)):
                _remember_blocked(BrowserFetchError(
                    'Blocked response from a non-public remote IP'))
        except Exception:
            pass

    if cdp_client is not None and hasattr(cdp_client, 'on'):
        cdp_handler = _on_response_received
        cdp_client.on('Network.responseReceived', cdp_handler)

    try:
        def _release_request(request):
            active_requests.discard(request)

        async def _handle_request(request):
            nonlocal blocked_subresources
            try:
                frame = getattr(request, 'frame', None)
                main_frame = getattr(page, 'mainFrame', frame)
                is_main = bool(request.isNavigationRequest() and frame is main_frame)
                redirect_chain = getattr(request, 'redirectChain', ()) or ()
                if len(redirect_chain) > MAX_REDIRECTS:
                    if is_main:
                        _remember_blocked(BrowserFetchError(
                            f'Exceeded maximum redirects ({MAX_REDIRECTS})'))
                    else:
                        blocked_subresources += 1
                    await _abort_quietly(request, 'aborted')
                    return
                resource_type = str(getattr(request, 'resourceType', '') or '').lower()
                if not is_main and resource_type in BLOCKED_RESOURCE_TYPES:
                    blocked_subresources += 1
                    await _abort_quietly(request, 'blockedbyclient')
                    return
                raw_url = str(getattr(request, 'url', '') or '')
                if not is_main and raw_url == 'about:blank':
                    await request.continue_()
                    return
                try:
                    await resolve_and_validate_url(raw_url, resolver=resolver)
                except BrowserFetchError as exc:
                    if is_main:
                        _remember_blocked(exc)
                    else:
                        blocked_subresources += 1
                    await _abort_quietly(request, 'accessdenied')
                    return
                if resource_type in ('xhr', 'fetch', 'script'):
                    active_requests.add(request)
                await request.continue_()
            except Exception:
                await _abort_quietly(request, 'failed')

        def _schedule_request(request):
            # pyee EventEmitter is synchronous: never hand it an async function.
            task = asyncio.create_task(_handle_request(request))
            pending_tasks.add(task)
            task.add_done_callback(pending_tasks.discard)

        if hasattr(page, 'on'):
            page.on('request', _schedule_request)
            page.on('requestfinished', _release_request)
            page.on('requestfailed', _release_request)
        await page.setRequestInterception(True)

        nav_timeout_ms = int(min(12.0, timeout_seconds * 0.7) * 1000)
        response = None
        try:
            response = await page.goto(
                normalized_url, {'waitUntil': 'domcontentloaded', 'timeout': nav_timeout_ms})
        except Exception as exc:
            # A security block wins over every transport symptom it caused.
            if blocked_error is not None:
                raise blocked_error
            if is_disconnect_error(exc):
                raise
            if isinstance(exc, asyncio.TimeoutError):
                raise BrowserFetchError('Browser fetch timed out') from None
            raise BrowserFetchError(f'Browser navigation failed: {type(exc).__name__}') from None
        if blocked_error is not None:
            raise blocked_error

        status_code = 0
        raw_content_type = None
        if response is not None:
            try:
                status_code = int(getattr(response, 'status', 0) or 0)
            except (TypeError, ValueError):
                status_code = 0
            try:
                headers = response.headers
            except Exception:
                headers = None
            if isinstance(headers, dict):
                for key, value in headers.items():
                    if str(key).lower() == 'content-type':
                        raw_content_type = value
                        break
        if classify_content_type(raw_content_type) is None:
            mime = str(raw_content_type or '').split(';', 1)[0].strip().lower()
            raise BrowserFetchError(f'Unsupported content type: {mime}')
        mime = str(raw_content_type or '').split(';', 1)[0].strip().lower()

        settle_budget = max(0.0, min(DEFAULT_SETTLE_SECONDS, deadline - loop.time()))
        settle_deadline = loop.time() + settle_budget
        while True:
            try:
                ready_state = await page.evaluate('() => document.readyState')
            except Exception:
                ready_state = 'complete'
            if ready_state == 'complete' and not active_requests:
                break
            if loop.time() >= settle_deadline:
                break
            await asyncio.sleep(0.15)
        # One bounded hydration tick even for pages that reported `complete` early.
        await asyncio.sleep(min(0.35, max(0.0, deadline - loop.time())))

        if blocked_error is not None:
            raise blocked_error
        final_url = str(getattr(page, 'url', '') or normalized_url)
        validate_url_syntax(final_url)

        data = await page.evaluate(_EXTRACT_JS)
        title = ''
        raw_text = ''
        if isinstance(data, dict):
            title = str(data.get('title') or '')[:300]
            raw_text = str(data.get('text') or '')
        cleaned_text, truncated = clean_extracted_text(raw_text, max_chars)
        return {
            'url': final_url,
            'requested_url': normalized_url,
            'status_code': status_code,
            'title': title,
            'content_type': mime,
            'text': cleaned_text,
            'truncated': truncated,
            'blocked_subresources': blocked_subresources,
        }
    finally:
        if cdp_client is not None and cdp_handler is not None and hasattr(cdp_client, 'remove_listener'):
            try:
                cdp_client.remove_listener('Network.responseReceived', cdp_handler)
            except Exception:
                pass
        for task in list(pending_tasks):
            task.cancel()
        if pending_tasks:
            try:
                await asyncio.gather(*pending_tasks, return_exceptions=True)
            except asyncio.CancelledError:
                pass


async def fetch_page_with_browser(url, *, pool=None, resolver=None,
                                  timeout_seconds=None, max_chars=None,
                                  max_concurrency=None):
    """Render one public page in the resident Chromium and extract visible text."""
    normalized_url, _ = await resolve_and_validate_url(url, resolver=resolver)
    effective_timeout = _effective_timeout(timeout_seconds)
    effective_max_chars = _effective_max_chars(max_chars)
    limit = _effective_max_concurrency(max_concurrency)

    async def _run():
        async def render(page):
            return await _render(
                page, normalized_url, resolver=resolver,
                timeout_seconds=effective_timeout, max_chars=effective_max_chars)
        return await (pool or screenshot_browser).run(render)

    async with _semaphore_for(limit):
        try:
            return await asyncio.wait_for(_run(), timeout=effective_timeout)
        except asyncio.TimeoutError:
            raise BrowserFetchError('Browser fetch timed out') from None
