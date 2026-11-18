#!/usr/bin/env python3
"""Local billing checks: no network egress, no credentials, no bot startup."""
from __future__ import annotations

import ast
import asyncio
import importlib.util
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import types
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

try:  # POSIX-only dependency of the operational store; Windows checks stub it.
    import fcntl  # noqa: F401
except ModuleNotFoundError:  # pragma: no cover - Windows local checks only
    sys.modules['fcntl'] = types.SimpleNamespace(
        flock=lambda *args, **kwargs: None, LOCK_EX=2, LOCK_NB=4
    )

import ai_runtime.store as store_module
from ai_runtime.store import StateStore
from ai_runtime.usage import (
    is_internal_usage_request_allowed,
    normalize_dsh_usage,
    normalize_guard_usage,
    normalize_image_usage,
    record_usage_for_run,
)

REPO = Path(__file__).resolve().parents[1]
IDLE_DEEPSEEK = (0.15, 0.003, 0.60)
PEAK_DEEPSEEK = (0.30, 0.006, 1.20)


def _load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _load_guard_function():
    """Reuse the shipped `_guard_qq_output` extraction from the runtime checks."""
    guard = _load_module(
        'csbot_openai_runtime_check', REPO / 'checks/openai_runtime_test.py'
    )._load_guard_function()
    # The runtime check only exercises the Responses path; the chat-completions
    # branch also names this star-imported alias, supplied here explicitly.
    from openai.types.chat import ChatCompletionMessageParam

    guard.__globals__['ChatCompletionMessageParam'] = ChatCompletionMessageParam
    return guard


def _load_internal_usage_route():
    """Compile only the ledger route from the real plugin source (no NoneBot boot)."""
    from fastapi import HTTPException, Request
    from fastapi.responses import JSONResponse

    path = REPO / 'plugins/cs_server/__init__.py'
    tree = ast.parse(path.read_text(encoding='utf-8'), filename=str(path))
    node = next(
        item for item in tree.body
        if isinstance(item, ast.AsyncFunctionDef) and item.name == 'internal_ai_usage'
    )
    # Drop the FastAPI decorator: the route is registered on the test app instead.
    node.decorator_list = []
    namespace = {
        'Request': Request,
        'HTTPException': HTTPException,
        'JSONResponse': JSONResponse,
    }
    exec(compile(ast.Module(body=[node], type_ignores=[]), str(path), 'exec'), namespace)
    return namespace['internal_ai_usage']


def _usage_event(**overrides) -> dict:
    event = {
        'type': 'usage',
        'provider': 'openai',
        'model': 'gpt-6.2-sol',
        'scene': 'chat',
        'uncached_input_tokens': 0,
        'cached_tokens': 0,
        'cache_write_tokens': 0,
        'output_tokens': 0,
        'reasoning_tokens': 0,
    }
    event.update(overrides)
    return event


class TextPricingTest(unittest.TestCase):
    def test_gpt62_sol_regular_rates_and_cache_offsets(self):
        item = normalize_dsh_usage(_usage_event(
            uncached_input_tokens=100_000,
            cached_tokens=100_000,
            cache_write_tokens=72_000,
            output_tokens=100_000,
            reasoning_tokens=25_000,
        ))
        self.assertEqual(item['input_tokens'], 272_000)
        self.assertEqual(item['uncached_input_tokens'], 100_000)
        self.assertEqual(item['cached_tokens'], 100_000)
        self.assertEqual(item['cache_write_tokens'], 72_000)
        self.assertEqual(item['reasoning_tokens'], 25_000)
        expected = (100_000 * 2.0 + 100_000 * 0.10 + 72_000 * 2.50 + 100_000 * 10.0) / 1e6
        self.assertAlmostEqual(item['cost_usd'], round(expected, 8))
        # No-cache comparison charges every input token at the uncached rate.
        self.assertAlmostEqual(
            item['nocache_cost_usd'], round((272_000 * 2.0 + 100_000 * 10.0) / 1e6, 8)
        )

    def test_dsh_input_tokens_are_already_uncached(self):
        item = normalize_dsh_usage(_usage_event(
            uncached_input_tokens=800, cached_tokens=200, output_tokens=100
        ))
        self.assertEqual(item['input_tokens'], 1000)
        self.assertEqual(item['uncached_input_tokens'], 800)
        expected = (800 * 2.0 + 200 * 0.10 + 100 * 10.0) / 1e6
        self.assertAlmostEqual(item['cost_usd'], round(expected, 8))

    def test_long_context_threshold_doubles_input_and_scales_output(self):
        item = normalize_dsh_usage(_usage_event(uncached_input_tokens=300_000, output_tokens=1_000))
        self.assertAlmostEqual(item['cost_usd'], (300_000 * 4.0 + 1_000 * 15.0) / 1e6)
        self.assertAlmostEqual(item['nocache_cost_usd'], item['cost_usd'])
        boundary = normalize_dsh_usage(_usage_event(uncached_input_tokens=272_000, output_tokens=1_000))
        self.assertAlmostEqual(
            boundary['cost_usd'], (272_000 * 2.0 + 1_000 * 10.0) / 1e6
        )
        # Cached and write rates share the long-context multiplier.
        mixed = normalize_dsh_usage(_usage_event(
            uncached_input_tokens=0, cached_tokens=300_000, cache_write_tokens=0,
            output_tokens=0,
        ))
        self.assertAlmostEqual(mixed['cost_usd'], 300_000 * 0.20 / 1e6)
        with_cached = normalize_dsh_usage(_usage_event(
            uncached_input_tokens=100_000, cached_tokens=200_000, output_tokens=0
        ))
        self.assertAlmostEqual(
            with_cached['cost_usd'], (100_000 * 4.0 + 200_000 * 0.20) / 1e6
        )

    def test_environment_rate_overrides(self):
        with patch.dict(os.environ, {'CS_AI_PRICE_GPT62_SOL_OUTPUT': '20'}):
            item = normalize_dsh_usage(_usage_event(output_tokens=1_000_000))
            self.assertAlmostEqual(item['cost_usd'], 20.0)

    def test_pricing_json_override_wins(self):
        table = json.dumps({
            'custom-mini': {
                'input': 1.0, 'cached': 0.5, 'cache_write': 2.0, 'output': 3.0,
            }
        })
        with patch.dict(os.environ, {'CS_AI_MODEL_PRICING_JSON': table}):
            item = normalize_dsh_usage(_usage_event(
                model='Custom-Mini', uncached_input_tokens=100_000,
                cached_tokens=100_000, cache_write_tokens=72_000,
                output_tokens=100_000,
            ))
            self.assertAlmostEqual(
                item['cost_usd'], round((100_000 * 1.0 + 100_000 * 0.5 + 72_000 * 2.0 + 100_000 * 3.0) / 1e6, 8)
            )
            self.assertAlmostEqual(
                item['nocache_cost_usd'], round((272_000 * 1.0 + 100_000 * 3.0) / 1e6, 8)
            )
        # A custom threshold opts the model into the same long-context scaling.
        custom_threshold = json.dumps({
            'custom-long': {'input': 1.0, 'cached': 0.5, 'cache_write': 2.0,
                            'output': 3.0, 'long_context_threshold': 1000},
        })
        with patch.dict(os.environ, {'CS_AI_MODEL_PRICING_JSON': custom_threshold}):
            long = normalize_dsh_usage(_usage_event(model='custom-long', uncached_input_tokens=2_000))
            self.assertAlmostEqual(long['cost_usd'], 2_000 * 2.0 / 1e6)
            short = normalize_dsh_usage(_usage_event(model='custom-long', uncached_input_tokens=500))
            self.assertAlmostEqual(short['cost_usd'], 500 * 1.0 / 1e6)
        with patch.dict(os.environ, {'CS_AI_MODEL_PRICING_JSON': 'not-json'}):
            item = normalize_dsh_usage(_usage_event(model='gpt-6.2-sol', uncached_input_tokens=1_000_000))
            self.assertAlmostEqual(item['cost_usd'], 4.0)

    def test_fixture_is_free_and_deepseek_uses_beijing_peak_windows(self):
        fixture = normalize_dsh_usage(_usage_event(model='fixture', uncached_input_tokens=10_000, output_tokens=5_000))
        self.assertEqual((fixture['cost_usd'], fixture['nocache_cost_usd']), (0.0, 0.0))
        event = _usage_event(provider='deepseek', model='deepseek-flash',
                             uncached_input_tokens=1_000_000, output_tokens=1_000_000)
        with patch('ai_runtime.usage._beijing_peak', return_value=True):
            peak = normalize_dsh_usage(event)
        with patch('ai_runtime.usage._beijing_peak', return_value=False):
            idle = normalize_dsh_usage(event)
        self.assertAlmostEqual(peak['cost_usd'], PEAK_DEEPSEEK[0] + PEAK_DEEPSEEK[2])
        self.assertAlmostEqual(idle['cost_usd'], IDLE_DEEPSEEK[0] + IDLE_DEEPSEEK[2])


class ImagePricingTest(unittest.TestCase):
    def test_generate_and_edit_split_text_image_and_cached_tokens(self):
        raw = {
            'input_tokens': 2600,
            'input_tokens_details': {'text_tokens': 600, 'image_tokens': 2000},
            'cached_tokens': 1000,
            'cached_tokens_details': {'text_tokens': 200, 'image_tokens': 800},
            'output_tokens': 4000,
            'output_tokens_details': {'image_tokens': 4000},
        }
        edit = normalize_image_usage(
            raw, model='gpt-image-2.6-sunburst', mode='edit', size='1024x1024', quality='medium'
        )
        self.assertEqual(edit['scene'], 'image_edit')
        self.assertEqual(edit['provider'], 'openai')
        self.assertEqual(edit['input_tokens'], 2600)
        self.assertEqual(edit['uncached_input_tokens'], 1600)
        self.assertEqual(edit['cached_tokens'], 1000)
        self.assertEqual(edit['image_input_tokens'], 2000)
        self.assertEqual(edit['image_output_tokens'], 4000)
        self.assertEqual(edit['size'], '1024x1024')
        self.assertEqual(edit['quality'], 'medium')
        expected = (400 * 5.0 + 200 * 1.25 + 1200 * 8.0 + 800 * 2.0 + 4000 * 30.0) / 1e6
        self.assertAlmostEqual(edit['cost_usd'], round(expected, 8))
        self.assertAlmostEqual(
            edit['nocache_cost_usd'], round((600 * 5.0 + 2000 * 8.0 + 4000 * 30.0) / 1e6, 8)
        )

        generated = normalize_image_usage(raw, model='gpt-image-2.6-sunburst', mode='generate',
                                         size='1024x1024', quality='low')
        self.assertEqual(generated['scene'], 'image_gen')
        self.assertEqual(generated['cost_usd'], edit['cost_usd'])

    def test_total_cached_tokens_offset_image_first(self):
        split = normalize_image_usage(
            {'input_tokens': 1000, 'input_tokens_details': {'text_tokens': 1000, 'image_tokens': 0},
             'cached_tokens': 400, 'output_tokens': 0},
            model='gpt-image-2.6-sunburst', mode='generate', size='1024x1024', quality='medium',
        )
        self.assertEqual(split['uncached_input_tokens'], 600)
        self.assertEqual(split['cached_tokens'], 400)
        self.assertAlmostEqual(split['cost_usd'], (600 * 5.0 + 400 * 1.25) / 1e6)

        image_first = normalize_image_usage(
            {'input_tokens': 1000, 'input_tokens_details': {'text_tokens': 400, 'image_tokens': 600},
             'cached_tokens': 500, 'output_tokens': 0},
            model='gpt-image-2.6-sunburst', mode='edit', size='auto', quality='auto',
        )
        # 500 cached tokens consume the 600 image tokens first, 100 stay text-side.
        self.assertEqual(image_first['cached_tokens'], 500)
        self.assertEqual(image_first['uncached_input_tokens'], 500)

    def test_missing_usage_degrades_to_zero(self):
        item = normalize_image_usage(None, model='gpt-image-2.6-sunburst', mode='generate',
                                     size='1024x1024', quality='medium')
        self.assertEqual(item['cost_usd'], 0.0)
        self.assertEqual(item['nocache_cost_usd'], 0.0)
        self.assertEqual(item['input_tokens'], 0)
        self.assertEqual(item['image_output_tokens'], 0)
        self.assertEqual(item['scene'], 'image_gen')

    def test_image_output_falls_back_to_output_tokens(self):
        item = normalize_image_usage(
            {'input_tokens': 0, 'output_tokens': 1234},
            model='gpt-image-2.6-sunburst', mode='generate', size='auto', quality='auto',
        )
        self.assertEqual(item['image_output_tokens'], 1234)
        self.assertAlmostEqual(item['cost_usd'], round(1234 * 30.0 / 1e6, 8))


class GuardUsageTest(unittest.TestCase):
    def test_responses_api_cached_and_reasoning_tokens(self):
        usage = types.SimpleNamespace(
            input_tokens=1000,
            input_tokens_details=types.SimpleNamespace(cached_tokens=400),
            output_tokens=200,
            output_tokens_details=types.SimpleNamespace(reasoning_tokens=50),
        )
        item = normalize_guard_usage(usage, provider='openai', model='gpt-6.2-sol')
        self.assertEqual(item['scene'], 'output_guard')
        self.assertEqual(item['input_tokens'], 1000)
        self.assertEqual(item['uncached_input_tokens'], 600)
        self.assertEqual(item['cached_tokens'], 400)
        self.assertEqual(item['output_tokens'], 200)
        self.assertEqual(item['reasoning_tokens'], 50)
        # Reasoning is part of the output total; never billed twice.
        self.assertAlmostEqual(item['cost_usd'], (600 * 2.0 + 400 * 0.10 + 200 * 10.0) / 1e6)

    def test_chat_completions_shape_and_cache_hit_field(self):
        usage = types.SimpleNamespace(
            prompt_tokens=800,
            prompt_tokens_details=types.SimpleNamespace(cached_tokens=300),
            completion_tokens=100,
        )
        item = normalize_guard_usage(usage, provider='openai', model='gpt-6.2-sol')
        self.assertEqual(item['uncached_input_tokens'], 500)
        self.assertAlmostEqual(item['cost_usd'], (500 * 2.0 + 300 * 0.10 + 100 * 10.0) / 1e6)
        hit = normalize_guard_usage(
            types.SimpleNamespace(prompt_tokens=100, prompt_cache_hit_tokens=40, completion_tokens=10),
            provider='deepseek', model='deepseek-flash',
        )
        self.assertEqual(hit['cached_tokens'], 40)
        self.assertEqual(hit['uncached_input_tokens'], 60)

    def test_guard_function_records_usage_for_both_apis(self):
        guard = _load_guard_function()
        responses_usage = types.SimpleNamespace(
            input_tokens=1200,
            input_tokens_details=types.SimpleNamespace(cached_tokens=500),
            output_tokens=90,
            output_tokens_details=types.SimpleNamespace(reasoning_tokens=30),
        )
        call_item = types.SimpleNamespace(
            type='function_call', name='submit_qq_guard_decision',
            arguments=json.dumps({'decision': 'allow', 'text': '', 'reason': 'ok'}),
        )

        class _Responses:
            def __init__(self, response): self.response = response
            async def create(self, **kwargs): return self.response

        class _Client:
            def __init__(self, response): self.responses = _Responses(response)

        collected = []
        response = types.SimpleNamespace(
            output=[call_item], usage=responses_usage, model='gpt-6.2-sol', output_text=''
        )
        verdict = asyncio.run(
            guard(_Client(response), '草稿', use_responses_api=True, on_usage=collected.append)
        )
        self.assertEqual(verdict.text, '草稿')
        self.assertEqual(len(collected), 1)
        self.assertEqual(collected[0]['scene'], 'output_guard')
        self.assertEqual(collected[0]['provider'], 'openai')
        self.assertEqual(collected[0]['cached_tokens'], 500)
        self.assertEqual(collected[0]['reasoning_tokens'], 30)
        self.assertEqual(collected[0]['uncached_input_tokens'], 700)

        chat_usage = types.SimpleNamespace(
            prompt_tokens=900,
            prompt_tokens_details=types.SimpleNamespace(cached_tokens=250),
            completion_tokens=60,
        )
        chat_response = types.SimpleNamespace(
            model='deepseek-flash',
            usage=chat_usage,
            choices=[types.SimpleNamespace(message=types.SimpleNamespace(
                content=json.dumps({'decision': 'rewrite', 'text': '改好的草稿', 'reason': 'r'}),
                tool_calls=None,
            ))],
        )

        class _ChatCompletions:
            def __init__(self, response): self.response = response
            async def create(self, **kwargs): return self.response

        class _ChatClient:
            def __init__(self, response):
                self.chat = types.SimpleNamespace(completions=_ChatCompletions(response))

        chat_items = []
        rewritten = asyncio.run(
            guard(_ChatClient(chat_response), '草稿', use_responses_api=False, on_usage=chat_items.append)
        )
        self.assertEqual(rewritten.text, '改好的草稿')
        self.assertEqual(len(chat_items), 1)
        self.assertEqual(chat_items[0]['provider'], 'deepseek')
        self.assertEqual(chat_items[0]['cached_tokens'], 250)
        self.assertEqual(chat_items[0]['uncached_input_tokens'], 650)

    def test_guard_function_without_callback_or_usage_is_a_no_op(self):
        guard = _load_guard_function()
        call_item = types.SimpleNamespace(
            type='function_call', name='submit_qq_guard_decision',
            arguments=json.dumps({'decision': 'allow', 'text': '', 'reason': 'ok'}),
        )

        class _Responses:
            async def create(self, **kwargs):
                return types.SimpleNamespace(output=[call_item], usage=None, model='gpt-6.2-sol')

        class _Client:
            def __init__(self): self.responses = _Responses()

        verdict = asyncio.run(guard(_Client(), '草稿', use_responses_api=True, on_usage=None))
        self.assertEqual(verdict.text, '草稿')


class DeepSeekSideChannelTest(unittest.TestCase):
    """Side-channel calls must bill as `deepseek`, never `deepseek-official`."""

    def test_dsh_memory_scenes_canonicalize_and_price(self):
        for scene in ('memory_extract', 'memory_verify'):
            with self.subTest(scene=scene):
                event = _usage_event(
                    provider='deepseek-official', model='deepseek-flash', scene=scene,
                    uncached_input_tokens=600, cached_tokens=400,
                    cache_write_tokens=0, output_tokens=250,
                )
                with patch('ai_runtime.usage._beijing_peak', return_value=False):
                    idle = normalize_dsh_usage(event)
                with patch('ai_runtime.usage._beijing_peak', return_value=True):
                    peak = normalize_dsh_usage(event)
                self.assertEqual(idle['provider'], 'deepseek')
                self.assertEqual(idle['scene'], scene)
                self.assertEqual(idle['input_tokens'], 1000)
                self.assertEqual(
                    idle['input_tokens'],
                    idle['uncached_input_tokens']
                    + idle['cached_tokens']
                    + idle['cache_write_tokens'],
                )
                self.assertAlmostEqual(
                    idle['cost_usd'],
                    round((600 * IDLE_DEEPSEEK[0] + 400 * IDLE_DEEPSEEK[1]
                           + 250 * IDLE_DEEPSEEK[2]) / 1e6, 8),
                )
                self.assertAlmostEqual(
                    idle['nocache_cost_usd'],
                    round((1000 * IDLE_DEEPSEEK[0] + 250 * IDLE_DEEPSEEK[2]) / 1e6, 8),
                )
                self.assertAlmostEqual(
                    peak['cost_usd'],
                    round((600 * PEAK_DEEPSEEK[0] + 400 * PEAK_DEEPSEEK[1]
                           + 250 * PEAK_DEEPSEEK[2]) / 1e6, 8),
                )

    def test_guard_usage_canonicalizes_official_alias(self):
        item = normalize_guard_usage(
            types.SimpleNamespace(
                prompt_tokens=120,
                prompt_cache_hit_tokens=40,
                prompt_cache_miss_tokens=80,
                completion_tokens=20,
            ),
            provider='deepseek-official', model='deepseek-flash',
        )
        self.assertEqual(item['provider'], 'deepseek')
        self.assertEqual(item['input_tokens'], 120)
        self.assertEqual(item['uncached_input_tokens'], 80)
        self.assertEqual(item['cached_tokens'], 40)

    def test_single_run_mixed_providers_stay_separate_in_the_ledger(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        store = StateStore(Path(tmp.name) / 'state.sqlite3')
        self.addCleanup(store.close)
        with patch('ai_runtime.usage._beijing_peak', return_value=False):
            items = [
                normalize_dsh_usage(_usage_event(
                    provider='openai', model='gpt-6.2-sol', scene='chat',
                    uncached_input_tokens=1000, output_tokens=100,
                )),
                normalize_guard_usage(
                    types.SimpleNamespace(
                        prompt_tokens=200, prompt_cache_hit_tokens=50, completion_tokens=10,
                    ),
                    provider='deepseek-official', model='deepseek-flash',
                ),
                normalize_dsh_usage(_usage_event(
                    provider='deepseek-official', model='deepseek-flash',
                    scene='memory_extract', uncached_input_tokens=300,
                    cached_tokens=100, output_tokens=30,
                )),
                normalize_dsh_usage(_usage_event(
                    provider='deepseek-official', model='deepseek-flash',
                    scene='memory_verify', uncached_input_tokens=120,
                    cached_tokens=80, output_tokens=12,
                )),
            ]
        for item in items:
            record_usage_for_run(
                store, run_id='mix-1', channel='qq', group_id='g1', item=item
            )
        rows = store.list_usage_after()['items']
        self.assertEqual(
            [(row['provider'], row['model'], row['scene']) for row in rows],
            [
                ('openai', 'gpt-6.2-sol', 'chat'),
                ('deepseek', 'deepseek-flash', 'output_guard'),
                ('deepseek', 'deepseek-flash', 'memory_extract'),
                ('deepseek', 'deepseek-flash', 'memory_verify'),
            ],
        )
        summary = store.run_usage_summary('mix-1')
        self.assertEqual(summary['calls'], 4)
        self.assertEqual(summary['input_tokens'], sum(row['input_tokens'] for row in rows))
        self.assertEqual(summary['output_tokens'], sum(row['output_tokens'] for row in rows))
        self.assertEqual(summary['cached_tokens'], sum(row['cached_tokens'] for row in rows))
        self.assertAlmostEqual(
            summary['cost_usd'], round(sum(row['cost_usd'] for row in rows), 8)
        )
        self.assertEqual(
            summary['cache_hit_pct'],
            round(sum(row['cached_tokens'] for row in rows)
                  / summary['input_tokens'] * 100.0, 1),
        )


class StoreLedgerTest(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.store = StateStore(Path(self._tmp.name) / 'state.sqlite3')
        self.addCleanup(self.store.close)

    def _insert(self, run_id='r1', **overrides):
        item = {
            'run_id': run_id,
            'channel': 'qq',
            'group_id': 'g1',
            'provider': 'openai',
            'model': 'gpt-6.2-sol',
            'scene': 'chat',
            'input_tokens': 1000,
            'uncached_input_tokens': 400,
            'cached_tokens': 600,
            'cache_write_tokens': 0,
            'output_tokens': 100,
            'reasoning_tokens': 20,
            'image_input_tokens': 0,
            'image_output_tokens': 0,
            'cost_usd': 0.002,
            'nocache_cost_usd': 0.003,
        }
        item.update(overrides)
        return self.store.record_usage(item)

    def test_autoincrement_ids_and_field_round_trip(self):
        first = self._insert()
        second = self._insert(scene='output_guard', cost_usd=0.0001)
        self.assertEqual((first, second), (1, 2))
        page = self.store.list_usage_after()
        self.assertEqual(page['count'], 2)
        self.assertEqual(page['schema_version'], 1)
        self.assertEqual(page['max_id'], 2)
        self.assertFalse(page['has_more'])
        self.assertEqual(page['next_after_id'], 2)
        row = page['items'][0]
        self.assertEqual(row['run_id'], 'r1')
        self.assertEqual(row['group_id'], 'g1')
        self.assertEqual(row['channel'], 'qq')
        self.assertEqual(row['provider'], 'openai')
        self.assertEqual(row['model'], 'gpt-6.2-sol')
        self.assertEqual(row['scene'], 'chat')
        self.assertEqual(row['input_tokens'], 1000)
        self.assertEqual(row['uncached_input_tokens'], 400)
        self.assertEqual(row['cached_tokens'], 600)
        self.assertEqual(row['output_tokens'], 100)
        self.assertEqual(row['reasoning_tokens'], 20)
        self.assertAlmostEqual(row['cost_usd'], 0.002)
        self.assertAlmostEqual(row['nocache_cost_usd'], 0.003)
        self.assertIsInstance(row['created_at'], int)
        self.assertIsInstance(row['created_at_ms'], int)
        self.assertGreaterEqual(row['created_at_ms'], row['created_at'] * 1000)
        self.assertEqual(row['size'], '')
        self.assertEqual(row['quality'], '')

    def test_cursor_pagination_and_empty_ledger(self):
        empty = self.store.list_usage_after(after_id=7, limit=10)
        self.assertEqual(empty['count'], 0)
        self.assertEqual(empty['max_id'], 0)
        self.assertEqual(empty['next_after_id'], 7)
        self.assertFalse(empty['has_more'])
        self.assertEqual(empty['items'], [])

        for _ in range(5):
            self._insert()
        page1 = self.store.list_usage_after(limit=2)
        self.assertEqual([row['id'] for row in page1['items']], [1, 2])
        self.assertTrue(page1['has_more'])
        self.assertEqual(page1['next_after_id'], 2)
        self.assertEqual(page1['max_id'], 5)
        page2 = self.store.list_usage_after(after_id=page1['next_after_id'], limit=2)
        self.assertEqual([row['id'] for row in page2['items']], [3, 4])
        self.assertTrue(page2['has_more'])
        page3 = self.store.list_usage_after(after_id=page2['next_after_id'], limit=2)
        self.assertEqual([row['id'] for row in page3['items']], [5])
        self.assertFalse(page3['has_more'])
        self.assertEqual(page3['next_after_id'], 5)
        exact = self.store.list_usage_after(after_id=3, limit=2)
        self.assertEqual([row['id'] for row in exact['items']], [4, 5])
        # Exactly `limit` rows present -> no false has_more.
        self.assertFalse(exact['has_more'])

    def test_parameter_validation(self):
        for kwargs in ({'after_id': -1}, {'limit': 0}, {'limit': 2001}, {'limit': 2000.5}, {'after_id': 'x'}):
            with self.subTest(**kwargs), self.assertRaises(ValueError):
                self.store.list_usage_after(**kwargs)
        self.assertEqual(self.store.list_usage_after(limit=2000)['count'], 0)

    def test_run_summary_aggregates_per_run(self):
        self._insert('r1', input_tokens=1000, uncached_input_tokens=400, cached_tokens=600,
                     output_tokens=100, cost_usd=0.002, nocache_cost_usd=0.003)
        self._insert('r1', scene='output_guard', input_tokens=500, uncached_input_tokens=500,
                     cached_tokens=0, output_tokens=50, cost_usd=0.0009, nocache_cost_usd=0.0011)
        self._insert('r2', input_tokens=9999, cost_usd=9.0)
        summary = self.store.run_usage_summary('r1')
        self.assertEqual(summary['calls'], 2)
        self.assertEqual(summary['input_tokens'], 1500)
        self.assertEqual(summary['uncached_input_tokens'], 900)
        self.assertEqual(summary['cached_tokens'], 600)
        self.assertEqual(summary['cache_write_tokens'], 0)
        self.assertEqual(summary['output_tokens'], 150)
        self.assertEqual(summary['reasoning_tokens'], 40)
        self.assertEqual(summary['image_input_tokens'], 0)
        self.assertEqual(summary['image_output_tokens'], 0)
        self.assertAlmostEqual(summary['cost_usd'], 0.0029)
        self.assertAlmostEqual(summary['nocache_cost_usd'], 0.0041)
        self.assertEqual(summary['cache_hit_pct'], 40.0)
        empty = self.store.run_usage_summary('missing')
        self.assertEqual(empty['calls'], 0)
        self.assertEqual(empty['cache_hit_pct'], 0.0)

    def test_record_usage_for_run_fills_run_identity_and_publishes(self):
        published = []
        row_id = record_usage_for_run(
            self.store, run_id='run-x', channel='web', group_id='g9',
            item=normalize_dsh_usage(_usage_event(uncached_input_tokens=1000, output_tokens=10)),
            publish_fn=lambda run_id, payload: published.append((run_id, payload)),
        )
        self.assertEqual(row_id, 1)
        row = self.store.list_usage_after()['items'][0]
        self.assertEqual((row['run_id'], row['channel'], row['group_id']), ('run-x', 'web', 'g9'))
        self.assertIsInstance(row['created_at_ms'], int)
        self.assertEqual(len(published), 1)
        self.assertEqual(published[0][0], 'run-x')
        self.assertEqual(published[0][1]['type'], 'usage')
        self.assertEqual(published[0][1]['usage']['calls'], 1)

        def _broken_publish(*args, **kwargs):
            raise RuntimeError('ui is down')

        self.assertEqual(
            record_usage_for_run(self.store, run_id='run-x', channel='web', group_id='g9',
                                 item=normalize_dsh_usage(_usage_event()), publish_fn=_broken_publish),
            2,
        )


class InternalAccessTest(unittest.TestCase):
    ALLOWED = (
        '127.0.0.1',
        '::1',
        '100.114.230.23',
        '100.101.30.74',
        '100.64.0.1',
        '::ffff:100.114.230.23',
        'fd7a:115c:a1e0::2301:1e87',
    )
    DENIED = (
        '172.18.0.4',
        '10.0.0.5',
        '192.168.1.10',
        '42.193.244.178',
        'fe80::1%eth0',
        '',
        None,
        'not-an-ip',
        'example.com',
    )

    def test_loopback_and_tailnet_direct_connections(self):
        for host in self.ALLOWED:
            with self.subTest(host=host):
                self.assertTrue(is_internal_usage_request_allowed(host, {}))
        # A zone suffix is stripped, then the address itself is still link-local.
        self.assertFalse(is_internal_usage_request_allowed('fe80::1%1', {}))

    def test_forwarding_headers_are_rejected_even_from_loopback(self):
        for header in ('X-Forwarded-For', 'x-real-ip', 'Forwarded', 'X-Forwarded-Host',
                       'X-Forwarded-Proto', 'CF-Connecting-IP', 'True-Client-IP'):
            for host in ('127.0.0.1', '100.101.30.74'):
                with self.subTest(header=header, host=host):
                    self.assertFalse(
                        is_internal_usage_request_allowed(host, {header: '203.0.113.5'})
                    )
        self.assertTrue(is_internal_usage_request_allowed('127.0.0.1', {'x-forwarded-for': '   '}))

    def test_docker_bridge_lan_public_and_empty_hosts_are_rejected(self):
        for host in self.DENIED:
            with self.subTest(host=host):
                self.assertFalse(is_internal_usage_request_allowed(host, {}))
        # An absent header mapping only means "no proxy evidence"; the IP decides.
        self.assertTrue(is_internal_usage_request_allowed('100.101.30.74', None))

    def test_bracketed_ipv6_and_case_insensitive_headers(self):
        self.assertTrue(is_internal_usage_request_allowed('[::1]', {}))
        self.assertFalse(
            is_internal_usage_request_allowed('[::1]', {'X-REAL-IP': '203.0.113.9'})
        )


class InternalLedgerRouteTest(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self._previous = os.environ.get('CS_AI_STATE_DIR')
        os.environ['CS_AI_STATE_DIR'] = self._tmp.name
        store_module._store = None
        self.store = store_module.state_store()
        self.addCleanup(self._release_store)
        for index in range(3):
            record_usage_for_run(
                self.store, run_id='r1', channel='qq', group_id='g1',
                item=normalize_dsh_usage(_usage_event(uncached_input_tokens=100 * (index + 1))),
            )
        from fastapi import FastAPI
        self.app = FastAPI()
        self.app.get('/api/internal/ai-usage')(_load_internal_usage_route())

    def _release_store(self):
        try:
            self.store.close()
        except Exception:
            pass
        store_module._store = None
        if self._previous is None:
            os.environ.pop('CS_AI_STATE_DIR', None)
        else:
            os.environ['CS_AI_STATE_DIR'] = self._previous

    def _get(self, host, *, headers=None, **params):
        from fastapi.testclient import TestClient
        client = TestClient(self.app, client=(host, 43210))
        try:
            return client.get('/api/internal/ai-usage', params=params, headers=headers or {})
        finally:
            client.close()

    def test_tailnet_direct_request_returns_the_incremental_ledger(self):
        response = self._get('100.101.30.74', limit=2)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers.get('cache-control'), 'no-store')
        body = response.json()
        self.assertEqual(body['schema_version'], 1)
        self.assertEqual(body['count'], 2)
        self.assertTrue(body['has_more'])
        self.assertEqual(body['next_after_id'], 2)
        self.assertEqual(body['max_id'], 3)
        self.assertEqual(body['items'][0]['model'], 'gpt-6.2-sol')
        self.assertEqual(body['items'][0]['scene'], 'chat')

        second = self._get('127.0.0.1', after_id=2, limit=500)
        self.assertEqual(second.status_code, 200)
        self.assertEqual([row['id'] for row in second.json()['items']], [3])
        self.assertEqual(self._get('::1').json()['count'], 3)

    def test_reverse_proxy_docker_and_public_traffic_is_forbidden(self):
        self.assertEqual(self._get('172.18.0.4').status_code, 403)
        self.assertEqual(self._get('192.168.1.10').status_code, 403)
        self.assertEqual(self._get('42.193.244.178').status_code, 403)
        self.assertEqual(self._get('', limit=10).status_code, 403)
        self.assertEqual(
            self._get('127.0.0.1', headers={'X-Real-IP': '203.0.113.5'}).status_code, 403
        )
        self.assertEqual(
            self._get('100.101.30.74', headers={'X-Forwarded-For': '203.0.113.5'}).status_code,
            403,
        )
        self.assertEqual(
            self._get('100.101.30.74', headers={'X-Forwarded-Proto': 'https'}).status_code, 403
        )

    def test_invalid_cursor_or_limit_is_a_bad_request(self):
        self.assertEqual(self._get('100.101.30.74', after_id=-1).status_code, 400)
        self.assertEqual(self._get('100.101.30.74', limit=0).status_code, 400)
        self.assertEqual(self._get('100.101.30.74', limit=2001).status_code, 400)
        # The access check runs first: a proxied caller learns nothing from 400.
        self.assertEqual(self._get('172.18.0.4', after_id=-1).status_code, 403)


class BridgeHookTest(unittest.TestCase):
    def test_bridge_syntax_and_usage_taps(self):
        bridge = REPO / 'ai_runtime/dsh/bridge.mjs'
        source = bridge.read_text(encoding='utf-8')
        node = shutil.which('node')
        if node:
            result = subprocess.run([node, '--check', str(bridge)], capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, result.stderr)
        else:  # pragma: no cover - delivery machines have Node installed
            self.skipTest('node is not installed')
        self.assertEqual(source.count('tapUsage(next())'), 3)
        for scene in ('memory_extract', 'memory_verify', 'compaction', 'chat'):
            self.assertIn(f"'{scene}'", source)
        for field in ('uncached_input_tokens', 'cached_tokens', 'cache_write_tokens',
                      'output_tokens', 'reasoning_tokens'):
            self.assertIn(f'{field}: Number(chunk.usage.', source)
        # Dual-provider side channel: memory verify/distill may target the
        # domestic direct provider, and billing must read `deepseek`.
        for marker in (
            'CSBOT_AUX_PROVIDER', 'CSBOT_AUX_MODEL', 'provider:auxProvider',
            'model:auxModel', 'canonicalProvider',
            "value.toLowerCase() === 'deepseek-official' ? 'deepseek'",
            'provider: canonicalProvider(options.provider || provider)',
        ):
            self.assertIn(marker, source)


if __name__ == '__main__':
    unittest.main()
