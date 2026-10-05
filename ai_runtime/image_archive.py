"""Semantic archives for bot-produced images; unknown images keep normal caching.

Only trusted producers attach metadata. Never infer meaning from a command name or
image pixels. These descriptors travel in the durable outbox, never to OneBot.
"""
from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal
import hashlib
from io import BytesIO
import json
import logging
from pathlib import Path
import re
from urllib.parse import parse_qsl, urlsplit

MARKER = '_csbot_image_archive'
MAX_METADATA_BYTES = 128 * 1024
LIBRARIES = {'pic', 'mgz'}
PAGE_PARAMS = {'id', 'steamId', 'rankName', 'timeType', 'seasonId', 'stage', 'eventId'}
PAGE_ROUTES = {'/steam-status', '/match', '/match-gp', '/match-faceit', '/rank', '/watch-stage',
               '/major-homework', '/major-homework/me', '/data', '/history', '/history-gp', '/history-faceit', '/history-all'}


class ArchiveImage(bytes):
    """Image bytes plus provenance until converted to a platform message."""
    def __new__(cls, content, metadata):
        obj = super().__new__(cls, content)
        obj.metadata = metadata
        return obj


def _json_default(value):
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    # Trusted producers pass data models; never pass auth/config models here.
    from sqlalchemy import inspect
    state = inspect(value, raiseerr=False)
    if state is not None and hasattr(state, 'mapper'):
        return {column.key: getattr(value, column.key) for column in state.mapper.column_attrs}
    raise TypeError('snapshot must contain JSON values')


def validate_metadata(value):
    if not isinstance(value, dict) or value.get('schema') != 'csbot-image-v1':
        raise ValueError('invalid image metadata schema')
    raw = json.dumps(value, ensure_ascii=False, allow_nan=False, default=_json_default)
    if len(raw.encode()) > MAX_METADATA_BYTES:
        raise ValueError('image metadata exceeds limit; keep image instead')
    item = json.loads(raw)
    if item.get('mode') not in {'snapshot', 'resource'} or not re.fullmatch(r'[a-f0-9]{64}', item.get('image_id', '')):
        raise ValueError('invalid image archive identity')
    if not item.get('title') or len(item['title']) > 200 or not item.get('captured_at'):
        raise ValueError('image archive needs title and capture time')
    if item['mode'] == 'snapshot' and not item.get('snapshot'):
        raise ValueError('empty snapshot must retain image')
    if item['mode'] == 'resource':
        source = item.get('source', {})
        name = source.get('filename', '')
        if source.get('library') not in LIBRARIES or not name or Path(name).name != name or name in {'.', '..'}:
            raise ValueError('invalid library reference')
        if not re.fullmatch(r'[a-f0-9]{64}', source.get('sha256', '')):
            raise ValueError('resource digest required')
    unsigned = {k: v for k, v in item.items() if k != 'image_id'}
    if hashlib.sha256(json.dumps(unsigned, ensure_ascii=False, sort_keys=True, separators=(',', ':')).encode()).hexdigest() != item['image_id']:
        raise ValueError('image descriptor digest mismatch')
    return item


def _descriptor(mode, title, source, snapshot=None):
    value = {'schema': 'csbot-image-v1', 'mode': mode, 'title': title,
             'captured_at': datetime.now(timezone.utc).isoformat(), 'source': source}
    if snapshot is not None:
        value['snapshot'] = snapshot
    value = json.loads(json.dumps(value, ensure_ascii=False, allow_nan=False, default=_json_default))
    value['image_id'] = hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
    return validate_metadata(value)


def snapshot_image(content, title, snapshot, *, source=None):
    raw = content.getvalue() if isinstance(content, BytesIO) else bytes(content)
    try:
        return ArchiveImage(raw, _descriptor('snapshot', title, source or {}, snapshot))
    except (ValueError, TypeError):
        logging.getLogger(__name__).warning('Image snapshot unavailable or oversized; retaining original image')
        return raw


def library_image(path):
    path = Path(path)
    raw = path.read_bytes()
    root = Path('imgs').resolve()
    resolved = path.resolve()
    if path.is_symlink() or resolved.parent.parent != root or resolved.parent.name not in LIBRARIES:
        return raw
    source = {'library': resolved.parent.name, 'filename': resolved.name,
              'sha256': hashlib.sha256(raw).hexdigest()}
    return ArchiveImage(raw, _descriptor('resource', '图库图片', source))


def image_segment(content, **kwargs):
    from nonebot.adapters.onebot.v11 import MessageSegment
    segment = MessageSegment.image(content, **kwargs)
    if isinstance(content, ArchiveImage):
        segment.data[MARKER] = validate_metadata(content.metadata)
    return segment


def page_source(path):
    url = urlsplit(path)
    route = url.path
    if url.scheme or url.netloc or not (route in PAGE_ROUTES or re.fullmatch(r'/major-homework/user/[0-9]+', route)):
        return None
    query = parse_qsl(url.query, keep_blank_values=True)
    if any(k not in PAGE_PARAMS | {'hideSidebar'} for k, _ in query):
        return None  # Do not archive arbitrary URL parameters, tokens or pages.
    return {'page': route, 'params': {k: v for k, v in query if k in PAGE_PARAMS}}


async def capture_page_image(page, options=None, *, title, source=None, selector='body'):
    """Archive complete business responses collected by the screenshot frontend.

    Missing, failed, changing or oversized metadata retains the actual pixels.
    """
    async def read():
        return await page.evaluate('''() => {
            const value = window.__CSBOT_PAGE_METADATA__;
            if (!value || value.documentKey !== location.pathname + location.search ||
                value.pending || value.failed || !value.responses.length) return null;
            return {kind: 'business_api', responses: value.responses};
        }''')
    try:
        before = await read()
    except Exception:
        before = None
    content = await page.screenshot(options or {})
    try:
        after = await read()
    except Exception:
        after = None
    if before and before == after:
        return snapshot_image(content, title, before, source=source)
    logging.getLogger(__name__).warning('Rendered snapshot unavailable or changed during capture; retaining original image')
    return bytes(content)


def metadata_text(item):
    item = validate_metadata(item)
    # Only provenance travels in passive context. Full metadata stays in the
    # immutable group message and is fetched through the existing image broker.
    source = item['source']
    reference = {key: source[key] for key in ('page', 'params', 'steamid', 'time_range', 'library', 'filename') if key in source}
    summary = json.dumps(reference, ensure_ascii=False, separators=(',', ':'))
    if len(summary) > 1000: summary = '{}'
    return f"[image:{item['image_id'][:16]}] {item['title']}（{item['mode']} metadata；{item['captured_at']}；用 csdata.image 读取完整资料）{summary}"


def compact_metadata_text(value):
    """Project legacy indexed snapshots as references, without changing history."""
    pattern = re.compile(r'(\[image:[a-f0-9]{16}\] [^\n]*?（(?:发送时数据快照，未归档图片像素|图库资源引用，未复制原图)；[^）]*）)')
    decoder = json.JSONDecoder()
    result, cursor = [], 0
    for match in pattern.finditer(value):
        if match.start() < cursor: continue
        start = match.end()
        try:
            _, length = decoder.raw_decode(value[start:])
        except ValueError:
            # Older indexes may carry the explicitly truncated 6000-char summary.
            marker = '…[摘要截断，用 csdata.image 读取完整 metadata]'
            end = value.find(marker, start)
            if end < 0: continue
            length = end + len(marker) - start
        result.extend((value[cursor:match.start()], match.group(1) + '（用 csdata.image 读取完整 metadata）'))
        cursor = start + length
    return ''.join(result) + value[cursor:]


def resource_info(item):
    """Resolve only an immutable, group-authorized library reference."""
    item = validate_metadata(item)
    result = {'image_id': item['image_id'], 'full_status': 'metadata_only',
              'full_path': None, 'thumbnail_path': None, 'metadata': item, 'storage': item['mode']}
    if item['mode'] == 'resource':
        source = item['source']
        base = Path('imgs').resolve()
        directory = base / source['library']
        root = directory.resolve()
        path = root / source['filename']
        result['full_status'] = 'missing'
        if not directory.is_symlink() and root.parent == base and not path.is_symlink() and path.is_file() and path.resolve().parent == root and path.stat().st_size <= 32 * 1024**2:
            if hashlib.sha256(path.read_bytes()).hexdigest() == source['sha256']:
                result.update(full_status='available', full_path=str(path), size_bytes=path.stat().st_size)
            else:
                result['full_status'] = 'source_changed'
    return result
