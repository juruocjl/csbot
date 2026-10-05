"""Read the context actually injected into one run; never regenerate old inputs."""
import json
import os
from pathlib import Path
import re

MAX_BYTES = 512 * 1024


def _entry(event):
    data = event.get('data', {})
    kind = event.get('type')
    plugin = data.get('source', {}).get('plugin')
    if kind != 'system/message' and not (kind == 'user/message' and data.get('source', {}).get('kind') == 'plugin' and plugin != 'csbot-report'):
        return None
    blocks = data.get('message', {}).get('content', []) if kind == 'system/message' else data.get('content', [])
    content = '\n'.join(block.get('text', '') for block in blocks if block.get('type') == 'text')
    if not content: return None
    return {'id': str(event['seq']), 'title': '系统提示' if kind == 'system/message' else
            '本轮身份、风格与群聊资料' if plugin == 'csbot-context' else '运行时资料与记忆',
            'text': content, 'time': event.get('time', 0)}


def run_context(store, row, state_root=None):
    entries = [json.loads(item['payload']) for item in store.rows(
        'SELECT payload FROM run_events WHERE run_id=? ORDER BY seq', (row['id'],))]
    captured = [{key: event[key] for key in ('id', 'title', 'text', 'time')} for event in entries if event.get('type') == 'context']
    origin = 'captured'
    if not captured:
        captured = _legacy_context(store, row, state_root)
        origin = 'native_archive'
    output, used, truncated = [], 0, False
    for item in captured:
        remaining = MAX_BYTES - used
        raw = item['text'].encode()
        if len(raw) > remaining:
            item = item | {'text': raw[:remaining].decode(errors='ignore')}
            truncated = True
        used += len(item['text'].encode())
        if item['text']: output.append(item)
        if truncated: break
    return {'entries': output, 'origin': origin if output else 'missing', 'truncated': truncated}


def _legacy_context(store, row, state_root):
    scope = row['scope']
    if not re.fullmatch(r'[a-f0-9]{64}', scope): return []
    root = Path(state_root or os.getenv('CS_AI_STATE_DIR', 'data/ai')) / 'scopes' / scope / 'harness/sessions'
    files = list(root.glob('*/csbot-' + scope + '/session.v3.jsonl'))
    if len(files) != 1 or files[0].is_symlink(): return []
    times = store.rows("SELECT min(CASE WHEN payload LIKE '%\"running\"%' THEN created_at END) AS started,max(created_at) AS ended FROM run_events WHERE run_id=?", (row['id'],))[0]
    started, ended = times['started'], times['ended']
    if started is None or ended is None: return []
    expected = f"QQ用户 {row['user_id']}：{row['request']}" if row['user_id'] else row['request']
    events, matches = [], []
    try:
        with files[0].open() as source:
            for line in source:
                try: event = json.loads(line)
                except ValueError: continue  # An active writer may have a partial last line.
                # Keep only this run's time window, including its step/start.
                if not (started * 1000 - 1000 <= event.get('time', 0) <= ended * 1000 + 1000): continue
                events.append(event)
                data = event.get('data', {})
                if event.get('type') == 'user/message' and (data.get('source', {}).get('kind') == 'user' or data.get('source', {}).get('plugin') == 'csbot-report'):
                    texts = [block.get('text') for block in data.get('content', []) if block.get('type') == 'text']
                    if texts == [expected]: matches.append(len(events)-1)
    except (OSError, UnicodeError): return []
    if len(matches) != 1: return []  # Never attach another run's context by guessing.
    index = matches[0]
    starts = [n for n in range(index+1) if events[n].get('type') == 'step/start']
    if not starts: return []
    first = starts[-1]
    turn = events[first].get('data', {}).get('turn')
    result = []
    for event in events[first:]:
        if event.get('type') == 'step/start' and event.get('data', {}).get('turn') != turn: break
        item = _entry(event)
        if item: result.append(item)
    return result
