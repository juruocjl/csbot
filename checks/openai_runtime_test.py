#!/usr/bin/env python3
"""Local OpenAI runtime checks; no network egress, no real keys, no bot startup."""
from __future__ import annotations

import ast
import asyncio
import base64
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from io import BytesIO
import importlib.util
import json
import os
from pathlib import Path
import sys
import tempfile
import threading
import types
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from PIL import Image
from openai import AsyncOpenAI

from ai_runtime.openai_support import (
    generate_openai_image,
    is_openai_endpoint,
    is_openai_non_reasoning_model,
    normalize_openai_base_url,
    resolve_openai_proxy,
    validate_loopback_http_proxy,
)
from ai_runtime.runner import render_profile

REPO = Path(__file__).resolve().parents[1]
_OUTPUT_GUARD_SPEC = importlib.util.spec_from_file_location(
    "cs_ai_output_guard", REPO / "plugins/cs_ai/output_guard.py"
)
GUARD = importlib.util.module_from_spec(_OUTPUT_GUARD_SPEC)
_OUTPUT_GUARD_SPEC.loader.exec_module(GUARD)
QQ_OUTPUT_GUARD_TOOL = GUARD.QQ_OUTPUT_GUARD_TOOL
QQOutputGuardResult = GUARD.QQOutputGuardResult
build_qq_guard_messages = GUARD.build_qq_guard_messages
parse_qq_guard_response = GUARD.parse_qq_guard_response


def _load_guard_function():
    """Compile only _guard_qq_output from the real plugin source.

    Importing plugins.cs_ai would boot NoneBot and the business database, which
    this local check must not touch. Extracting the function node keeps the test
    bound to the shipped implementation without that startup.
    """
    path = REPO / "plugins/cs_ai/__init__.py"
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    node = next(
        item for item in tree.body
        if isinstance(item, ast.AsyncFunctionDef) and item.name == "_guard_qq_output"
    )
    namespace = {
        "AsyncOpenAI": AsyncOpenAI,
        "QQOutputGuardResult": QQOutputGuardResult,
        "QQ_OUTPUT_GUARD_TOOL": QQ_OUTPUT_GUARD_TOOL,
        "build_qq_guard_messages": build_qq_guard_messages,
        "parse_qq_guard_response": parse_qq_guard_response,
        "cast": __import__("typing").cast,
        "Any": __import__("typing").Any,
        "config": types.SimpleNamespace(cs_ai_guard_model=""),
        "runtime_config": types.SimpleNamespace(get=_model_getter()),
        "logger": types.SimpleNamespace(warning=lambda *a, **k: None),
    }
    exec(compile(ast.Module(body=[node], type_ignores=[]), str(path), "exec"), namespace)
    return namespace["_guard_qq_output"]


def _model_getter():
    async def get(key):
        return "gpt-6.2-sol"
    return get


class OpenAIHelpersTest(unittest.TestCase):
    def test_endpoint_detection_and_base_url(self):
        self.assertTrue(is_openai_endpoint("https://api.openai.com"))
        self.assertTrue(is_openai_endpoint("https://api.openai.com/v1"))
        self.assertFalse(is_openai_endpoint("https://api.deepseek.com"))
        self.assertFalse(is_openai_endpoint("http://127.0.0.1:8080/v1"))
        self.assertTrue(is_openai_endpoint("http://127.0.0.1:8080/v1", "openai"))
        with patch.dict(os.environ, {"CS_AI_PROVIDER": "openai"}):
            self.assertTrue(is_openai_endpoint("http://127.0.0.1:8080/v1"))
        with patch.dict(os.environ, {"CS_AI_PROVIDER": ""}):
            self.assertFalse(is_openai_endpoint("https://api.deepseek.com", "deepseek-official"))

        self.assertEqual(normalize_openai_base_url("https://api.openai.com"), "https://api.openai.com/v1")
        self.assertEqual(normalize_openai_base_url("https://api.openai.com/"), "https://api.openai.com/v1")
        self.assertEqual(normalize_openai_base_url("https://api.openai.com/v1"), "https://api.openai.com/v1")
        self.assertEqual(
            normalize_openai_base_url("https://gateway.example.com/openai"),
            "https://gateway.example.com/openai",
        )

    def test_proxy_validation_and_resolution(self):
        self.assertEqual(validate_loopback_http_proxy(""), "")
        self.assertEqual(
            validate_loopback_http_proxy("http://127.0.0.1:7890"),
            "http://127.0.0.1:7890",
        )
        self.assertEqual(validate_loopback_http_proxy("http://[::1]:7890/"), "http://[::1]:7890")
        for bad in (
            "http://10.0.0.1:7890",
            "https://127.0.0.1:7890",
            "http://user:pass@127.0.0.1:7890",
            "http://127.0.0.1:7890/path",
            "http://127.0.0.1:7890?x=1",
            "http://127.0.0.1:99999",
            "socks5://127.0.0.1:7891",
        ):
            with self.subTest(bad=bad), self.assertRaises(ValueError):
                validate_loopback_http_proxy(bad)

        with patch.dict(os.environ, {"CS_AI_OPENAI_PROXY_URL": ""}, clear=False):
            self.assertEqual(
                resolve_openai_proxy("https://api.openai.com/v1", "http://127.0.0.1:7890"),
                "http://127.0.0.1:7890",
            )
        self.assertEqual(resolve_openai_proxy("https://api.deepseek.com", "http://127.0.0.1:7890"), "")
        with patch.dict(os.environ, {"CS_AI_PROVIDER": "openai"}, clear=False):
            os.environ.pop("CS_AI_FORCE_LOOPBACK_LLM_PROXY", None)
            self.assertEqual(
                resolve_openai_proxy("http://127.0.0.1:8080/v1", "http://127.0.0.1:7890"), ""
            )
            with patch.dict(os.environ, {"CS_AI_FORCE_LOOPBACK_LLM_PROXY": "1"}):
                self.assertEqual(
                    resolve_openai_proxy("http://127.0.0.1:8080/v1", "http://127.0.0.1:7890"),
                    "http://127.0.0.1:7890",
                )
        self.assertFalse(is_openai_non_reasoning_model("gpt-6.2-sol"))
        self.assertTrue(is_openai_non_reasoning_model("gpt-4.1"))
        self.assertTrue(is_openai_non_reasoning_model("GPT-3.5-turbo"))


class RenderProfileTest(unittest.TestCase):
    def env(self, **overrides):
        env = {
            "CSBOT_PROVIDER": "deepseek-official",
            "CSBOT_REMEMBER": "0",
            "CSBOT_MODEL": "gpt-6.2-sol",
            "DSH_CONTEXT_WINDOW": "131072",
            "CSBOT_VISION": "1",
            "CSBOT_THINKING": "1",
            "CSBOT_WEB_ENABLED": "0",
            "CSBOT_OPENAI_BASE_URL": "https://api.openai.com/v1",
        }
        env.update(overrides)
        return env

    @staticmethod
    def pi_ai_config(profile):
        for entry in profile:
            for item in entry.get("insert", []) if isinstance(entry, dict) else []:
                if item.get("name") == "@deepseek-ai/dsh-llm-pi-ai":
                    return item["config"]
        return None

    def test_deepseek_mode_keeps_existing_provider(self):
        profile = render_profile(self.env())
        default_model = next(
            item
            for entry in profile
            for item in entry.get("insert", [])
            if item.get("id") == "agent-default-model"
        )
        self.assertEqual(default_model["config"]["provider"], "deepseek-official")
        llm = next(item for item in profile if item.get("id") == "llm-deepseek")
        self.assertNotIn("disabled", llm)
        self.assertIsNone(self.pi_ai_config(profile))

    def test_openai_mode_mounts_pi_ai_with_reasoning_map(self):
        profile = render_profile(self.env(CSBOT_PROVIDER="openai"))
        llm = next(item for item in profile if item.get("id") == "llm-deepseek")
        self.assertTrue(llm["disabled"])
        config = self.pi_ai_config(profile)
        self.assertIsNotNone(config)
        provider = config["providers"]["openai"]
        self.assertEqual(provider["api"], "openai-responses")
        self.assertEqual(provider["apiKeyEnv"], "OPENAI_API_KEY")
        self.assertEqual(provider["baseURL"], "https://api.openai.com/v1")
        self.assertEqual(provider["streamIdleTimeoutMs"], 120000)
        self.assertEqual(provider["reasoning"], "medium")
        model = provider["models"][0]
        self.assertEqual(model["id"], "gpt-6.2-sol")
        self.assertEqual(model["contextWindow"], 131072)
        self.assertEqual(model["maxTokens"], 16384)
        self.assertEqual(model["input"], ["text", "image"])
        self.assertEqual(model["reasoningEfforts"]["off"], "low")
        self.assertEqual(model["reasoningEfforts"]["medium"], "medium")

    def test_non_reasoning_model_disables_reasoning_efforts(self):
        profile = render_profile(self.env(CSBOT_PROVIDER="openai", CSBOT_MODEL="gpt-4.1"))
        provider = self.pi_ai_config(profile)["providers"]["openai"]
        self.assertFalse(provider["models"][0]["reasoningEfforts"])
        self.assertNotIn("reasoning", provider)


def _png_bytes(color=(10, 20, 30), size=(8, 8)):
    buffer = BytesIO()
    Image.new("RGB", size, color).save(buffer, format="PNG")
    return buffer.getvalue()


class _ImageHandler(BaseHTTPRequestHandler):
    calls = []
    png = _png_bytes()

    def log_message(self, *args):
        return

    def do_POST(self):
        length = int(self.headers.get("Content-Length") or 0)
        payload = json.loads(self.rfile.read(length) or b"{}")
        type(self).calls.append({"path": self.path, "auth": self.headers.get("Authorization"), "payload": payload})
        if len(type(self).calls) == 1:
            self.send_response(503)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(b'{"error":{"message":"temporary upstream failure"}}')
            return
        if payload.get("prompt") == "LEAK":
            secret = self.headers.get("Authorization", "").removeprefix("Bearer ")
            self.send_response(400)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"error": {"message": f"bad key {secret}"}}).encode())
            return
        body = json.dumps({"data": [{"b64_json": base64.b64encode(type(self).png).decode(),
                                     "revised_prompt": "a revised prompt"}]}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(body)


class GenerateImageTest(unittest.TestCase):
    def setUp(self):
        _ImageHandler.calls = []
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), _ImageHandler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.endpoint = f"http://127.0.0.1:{self.server.server_address[1]}"
        self.tmp = tempfile.TemporaryDirectory()

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)
        self.tmp.cleanup()

    def test_retries_transient_503_then_writes_valid_png(self):
        result = asyncio.run(generate_openai_image(
            prompt="a tiny test image",
            api_key="sk-secret-test-key",
            endpoint=self.endpoint,
            artifacts_dir=Path(self.tmp.name),
            caption="测试图" * 40,
        ))
        self.assertEqual(len(_ImageHandler.calls), 2)
        self.assertEqual(_ImageHandler.calls[0]["auth"], "Bearer sk-secret-test-key")
        self.assertEqual(_ImageHandler.calls[0]["path"], "/images/generations")
        self.assertNotIn("sk-secret-test-key", json.dumps(result))
        self.assertTrue(result["ok"] and result["delivered"])
        self.assertEqual(result["size"], "8x8")
        self.assertEqual(result["model"], "gpt-image-2.6-sunburst")
        self.assertLessEqual(len(result["caption"]), 120)
        self.assertEqual(result["revised_prompt"], "a revised prompt")
        path = Path(result["file_path"])
        self.assertTrue(path.is_file())
        with Image.open(path) as image:
            self.assertEqual((image.format, image.size), ("PNG", (8, 8)))
        self.assertEqual(result["bytes"], len(_ImageHandler.png))

    def test_invalid_parameters_fail_before_any_request(self):
        cases = [
            {"prompt": ""},
            {"prompt": "  "},
            {"prompt": "ok", "size": "999x999"},
            {"prompt": "ok", "quality": "ultra"},
            {"prompt": "ok", "background": "red"},
        ]
        for kwargs in cases:
            with self.subTest(kwargs=kwargs), self.assertRaises(ValueError):
                asyncio.run(generate_openai_image(
                    api_key="key", endpoint=self.endpoint,
                    artifacts_dir=Path(self.tmp.name), **kwargs,
                ))
        self.assertEqual(_ImageHandler.calls, [])

    def test_http_error_never_leaks_api_key(self):
        # First call is the 503 retry, second one carries the LEAK prompt -> HTTP 400.
        with self.assertRaises(ValueError) as caught:
            asyncio.run(generate_openai_image(
                prompt="LEAK",
                api_key="sk-secret-test-key",
                endpoint=self.endpoint,
                artifacts_dir=Path(self.tmp.name),
            ))
        message = str(caught.exception)
        self.assertIn("HTTP 400", message)
        self.assertNotIn("sk-secret-test-key", message)


class _FakeResponses:
    def __init__(self, items):
        self.items = items
        self.calls = []

    async def create(self, **kwargs):
        self.calls.append(kwargs)
        return types.SimpleNamespace(output=self.items, output_text="")


class GuardResponsesTest(unittest.TestCase):
    def run_guard(self, arguments):
        guard = _load_guard_function()
        responses = _FakeResponses([
            types.SimpleNamespace(
                type="function_call",
                name="submit_qq_guard_decision",
                arguments=json.dumps(arguments),
            )
        ])
        client = types.SimpleNamespace(responses=responses)
        result = asyncio.run(guard(client, "草稿", use_responses_api=True))
        return result, responses.calls

    def test_responses_branch_payloads_and_decisions(self):
        result, calls = self.run_guard({"decision": "allow", "text": "", "reason": "safe"})
        self.assertEqual(result.decision, "allow")
        self.assertEqual(result.text, "草稿")
        call = calls[0]
        self.assertEqual(call["reasoning"], {"effort": "low"})
        self.assertFalse(call["store"])
        self.assertEqual(call["model"], "gpt-6.2-sol")
        self.assertEqual(call["tool_choice"], {"type": "function", "name": "submit_qq_guard_decision"})
        tool = call["tools"][0]
        self.assertEqual(tool["type"], "function")
        self.assertEqual(tool["name"], "submit_qq_guard_decision")
        self.assertFalse(tool["parameters"]["additionalProperties"])

        rewritten, _ = self.run_guard({"decision": "rewrite", "text": "改写后的文本", "reason": "risk"})
        self.assertEqual((rewritten.decision, rewritten.text), ("rewrite", "改写后的文本"))
        blocked, _ = self.run_guard({"decision": "block", "text": "", "reason": "risk"})
        self.assertEqual(blocked.decision, "block")


if __name__ == "__main__":
    unittest.main()
