#!/usr/bin/env python3
"""Local OpenAI runtime checks; no network egress, no real keys, no bot startup."""
from __future__ import annotations

import ast
import asyncio
import base64
from contextlib import ExitStack
from email.parser import BytesParser
from email.policy import HTTP
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
import uuid
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

try:  # POSIX-only dependency of the operational store; Windows checks stub it.
    import fcntl  # noqa: F401
except ModuleNotFoundError:  # pragma: no cover - Windows local checks only
    sys.modules["fcntl"] = types.SimpleNamespace(
        flock=lambda *args, **kwargs: None, LOCK_EX=2, LOCK_NB=4
    )

from PIL import Image
from openai import AsyncOpenAI

from ai_runtime.media import MediaCache
from ai_runtime.openai_support import (
    MAX_REFERENCE_IMAGES,
    generate_openai_image,
    is_openai_endpoint,
    is_openai_non_reasoning_model,
    normalize_openai_base_url,
    resolve_authorized_image_paths,
    resolve_openai_proxy,
    validate_loopback_http_proxy,
)
from ai_runtime.runner import render_profile
from ai_runtime.service import recent_scope_image_paths
from ai_runtime.store import StateStore

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


def _jpeg_bytes(color=(40, 50, 60), size=(12, 9)):
    buffer = BytesIO()
    Image.new("RGB", size, color).save(buffer, format="JPEG")
    return buffer.getvalue()


def _gif_bytes(size=(10, 10)):
    frames = [Image.new("RGB", size, (200, 0, 0)), Image.new("RGB", size, (0, 0, 200))]
    buffer = BytesIO()
    frames[0].save(buffer, format="GIF", save_all=True, append_images=frames[1:])
    return buffer.getvalue()


def _parse_multipart(raw: bytes, content_type: str):
    """Minimal RFC 7578 reader so the mock can inspect the real edits payload."""
    message = BytesParser(policy=HTTP).parsebytes(
        b"Content-Type: " + content_type.encode() + b"\r\nMIME-Version: 1.0\r\n\r\n" + raw
    )
    fields: dict[str, str] = {}
    files: list[dict] = []
    for part in message.iter_parts():
        name = part.get_param("name", header="content-disposition")
        filename = part.get_filename()
        data = part.get_payload(decode=True) or b""
        if filename is not None:
            files.append(
                {
                    "name": name,
                    "filename": filename,
                    "content_type": part.get_content_type(),
                    "data": data,
                }
            )
        else:
            fields[str(name)] = data.decode("utf-8", "replace")
    return fields, files


class _ImageHandler(BaseHTTPRequestHandler):
    calls = []
    png = _png_bytes()
    inject_503 = False

    def log_message(self, *args):
        return

    def _respond(self, status: int, body: bytes):
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self):
        length = int(self.headers.get("Content-Length") or 0)
        raw = self.rfile.read(length) if length else b""
        content_type = self.headers.get("Content-Type") or ""
        if content_type.startswith("multipart/form-data"):
            fields, files = _parse_multipart(raw, content_type)
            type(self).calls.append({
                "path": self.path,
                "auth": self.headers.get("Authorization"),
                "content_type": content_type,
                "fields": fields,
                "files": files,
            })
            prompt = fields.get("prompt", "")
        else:
            payload = json.loads(raw or b"{}")
            type(self).calls.append({
                "path": self.path,
                "auth": self.headers.get("Authorization"),
                "content_type": content_type,
                "payload": payload,
            })
            prompt = payload.get("prompt") if isinstance(payload, dict) else ""
        if type(self).inject_503 and len(type(self).calls) == 1:
            self._respond(503, b'{"error":{"message":"temporary upstream failure"}}')
            return
        if prompt == "LEAK":
            secret = self.headers.get("Authorization", "").removeprefix("Bearer ")
            self._respond(400, json.dumps({"error": {"message": f"bad key {secret}"}}).encode())
            return
        body = json.dumps({"data": [{"b64_json": base64.b64encode(type(self).png).decode(),
                                     "revised_prompt": "a revised prompt"}]}).encode()
        self._respond(200, body)


class _UpstreamTest(unittest.TestCase):
    """Loopback only: a local stand-in for the upstream image API."""

    def setUp(self):
        _ImageHandler.calls = []
        _ImageHandler.inject_503 = False
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), _ImageHandler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.endpoint = f"http://127.0.0.1:{self.server.server_address[1]}"
        self.tmp = tempfile.TemporaryDirectory()
        self.tmp_path = Path(self.tmp.name)

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)
        self.tmp.cleanup()

    def write_png(self, name="ref.png", **kwargs) -> Path:
        path = self.tmp_path / name
        path.write_bytes(_png_bytes(**kwargs))
        return path

    def generate(self, **kwargs):
        return asyncio.run(generate_openai_image(
            api_key="sk-secret-test-key",
            endpoint=self.endpoint,
            artifacts_dir=self.tmp_path,
            **kwargs,
        ))


class GenerateImageTest(_UpstreamTest):

    def test_retries_transient_503_then_writes_valid_png(self):
        _ImageHandler.inject_503 = True
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
        self.assertEqual(result["mode"], "generate")
        self.assertEqual(result["reference_count"], 0)
        self.assertEqual(_ImageHandler.calls[1]["payload"]["output_format"], "png")
        self.assertEqual(_ImageHandler.calls[1]["payload"]["n"], 1)
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
        _ImageHandler.inject_503 = True
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


class ReferenceEditTest(_UpstreamTest):
    def test_single_reference_uses_image_field(self):
        png = self.write_png()
        result = self.generate(
            prompt="把这张图改成水彩风",
            image_paths=[str(png)],
            allowed_image_paths={png},
            caption="改图",
        )
        self.assertEqual(len(_ImageHandler.calls), 1)
        call = _ImageHandler.calls[0]
        self.assertEqual(call["path"], "/images/edits")
        self.assertTrue(call["content_type"].startswith("multipart/form-data"))
        fields = call["fields"]
        self.assertEqual(fields["model"], "gpt-image-2.6-sunburst")
        self.assertEqual(fields["prompt"], "把这张图改成水彩风")
        self.assertEqual(fields["n"], "1")
        self.assertEqual(fields["size"], "1024x1024")
        self.assertEqual(fields["quality"], "medium")
        self.assertEqual(fields["background"], "auto")
        self.assertEqual(fields["output_format"], "png")
        self.assertNotIn("input_fidelity", fields)
        self.assertEqual(len(call["files"]), 1)
        part = call["files"][0]
        self.assertEqual(part["name"], "image")
        self.assertEqual(part["content_type"], "image/png")
        self.assertTrue(part["filename"].startswith("ref_0"))
        self.assertEqual(part["data"], png.read_bytes())
        self.assertEqual((result["mode"], result["reference_count"]), ("edit", 1))
        self.assertNotIn("sk-secret-test-key", json.dumps(result))

    def test_multi_reference_combines_heterogeneous_formats(self):
        png = self.write_png("photo.png")
        legacy = self.tmp_path / "legacy.png"  # .png name, real JPEG bytes
        legacy.write_bytes(_jpeg_bytes())
        sticker = self.tmp_path / "sticker.gif"
        sticker.write_bytes(_gif_bytes())
        result = self.generate(
            prompt="把三张参考图合成一张海报",
            image_paths=[str(png), str(legacy), str(sticker)],
            allowed_image_paths={png, legacy, sticker},
            size="1536x1024",
            quality="high",
        )
        call = _ImageHandler.calls[0]
        self.assertEqual(call["path"], "/images/edits")
        self.assertEqual(call["fields"]["size"], "1536x1024")
        self.assertEqual(call["fields"]["quality"], "high")
        self.assertNotIn("input_fidelity", call["fields"])
        parts = call["files"]
        self.assertEqual([part["name"] for part in parts], ["image[]"] * 3)
        self.assertEqual(parts[0]["content_type"], "image/png")
        self.assertEqual(parts[1]["content_type"], "image/jpeg")
        self.assertEqual(parts[2]["content_type"], "image/png")
        self.assertEqual(parts[1]["data"], legacy.read_bytes())
        with Image.open(BytesIO(parts[0]["data"])) as image:
            self.assertEqual(image.format, "PNG")
        with Image.open(BytesIO(parts[1]["data"])) as image:
            self.assertEqual(image.format, "JPEG")
        with Image.open(BytesIO(parts[2]["data"])) as image:
            self.assertEqual(image.format, "PNG")
        self.assertEqual((result["mode"], result["reference_count"]), ("edit", 3))

    def test_oversized_reference_is_reencoded_and_downscaled(self):
        big = self.tmp_path / "big.png"
        buffer = BytesIO()
        Image.new("RGB", (2400, 1000), (5, 6, 7)).save(buffer, format="PNG")
        big.write_bytes(buffer.getvalue())
        result = self.generate(
            prompt="按这个构图重画", image_paths=[str(big)], allowed_image_paths={big}
        )
        part = _ImageHandler.calls[0]["files"][0]
        self.assertEqual(part["content_type"], "image/png")
        with Image.open(BytesIO(part["data"])) as image:
            self.assertEqual(image.format, "PNG")
            self.assertEqual(image.size, (2048, 853))
        self.assertEqual(result["reference_count"], 1)


class ReferenceAuthorizationTest(_UpstreamTest):
    def assert_rejected(self, message: str, **kwargs):
        with self.assertRaises(ValueError) as caught:
            self.generate(**kwargs)
        self.assertIn(message, str(caught.exception))
        self.assertEqual(_ImageHandler.calls, [])

    def test_missing_or_empty_allowlist_fails_closed(self):
        png = self.write_png()
        for allowed in (None, set(), frozenset()):
            with self.subTest(allowed=allowed):
                _ImageHandler.calls = []
                self.assert_rejected(
                    "outside the authorized readable images",
                    prompt="改图", image_paths=[str(png)], allowed_image_paths=allowed,
                )

    def test_unavailable_and_unauthorized_local_paths_are_rejected(self):
        png = self.write_png()
        other = self.write_png("other.png", color=(90, 91, 92))
        self.assert_rejected(
            "outside the authorized readable images",
            prompt="改图", image_paths=[str(other)], allowed_image_paths={png},
        )
        evicted = self.tmp_path / "evicted.png"
        self.assert_rejected(
            "unavailable",
            prompt="改图", image_paths=[str(evicted)], allowed_image_paths={evicted},
        )

    def test_url_is_rejected_with_avatar_guidance(self):
        with self.assertRaises(ValueError) as caught:
            self.generate(
                prompt="改头像",
                image_paths=["https://q1.qlogo.cn/g?b=qq&nk=10001&s=640"],
                allowed_image_paths={self.write_png()},
            )
        message = str(caught.exception)
        self.assertIn("member_avatar", message)
        self.assertIn("full_path", message)
        self.assertEqual(_ImageHandler.calls, [])

    def test_non_image_bytes_are_rejected_even_when_authorized(self):
        text = self.tmp_path / "notes.txt"
        text.write_text("plain text, not an image", encoding="utf-8")
        self.assert_rejected(
            "unsupported reference image format",
            prompt="改图", image_paths=[str(text)], allowed_image_paths={text},
        )

    def test_symlink_reference_is_rejected(self):
        png = self.write_png()
        link = self.tmp_path / "link.png"
        try:
            link.symlink_to(png)
        except (OSError, NotImplementedError):
            link = None
        if link is not None:
            with self.assertRaisesRegex(ValueError, "symlink"):
                resolve_authorized_image_paths([str(link)], allowed_image_paths={link, png})
        else:
            # Windows without symlink rights: force the same branch deterministically.
            with patch.object(Path, "is_symlink", return_value=True):
                with self.assertRaisesRegex(ValueError, "symlink"):
                    resolve_authorized_image_paths([str(png)], allowed_image_paths={png})
        self.assertEqual(_ImageHandler.calls, [])

    def test_more_than_eight_references_are_rejected(self):
        png = self.write_png()
        images = [str(png)] * (MAX_REFERENCE_IMAGES + 1)
        self.assert_rejected(
            "at most 8 reference images",
            prompt="合成", image_paths=images, allowed_image_paths={png},
        )


class LatestScopeImagesTest(unittest.TestCase):
    """Cross-turn authorization: only this scope's surviving images come back."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.root = Path(self.tmp.name)
        self.cache = MediaCache(self.root / "media", budget=1024**3)
        self.store = StateStore(self.root / "state" / "state.sqlite3")

    def tearDown(self):
        self.cache.db.close()
        self.store.close()
        self.tmp.cleanup()

    def add_image(self, scope, content, uid="1"):
        digest = self.cache.put(content, private=True)
        run_id = str(uuid.uuid4())
        self.store.register_run(run_id, scope, "10001", uid, "qq", "画图")
        self.store.execute(
            "INSERT INTO run_images(id,run_id,digest,caption) VALUES(?,?,?,?)",
            (uuid.uuid4().hex, run_id, digest, "图"),
        )
        return digest

    def paths_for(self, scope):
        with ExitStack() as leases:
            return recent_scope_image_paths(self.store, scope, leases, cache=self.cache)

    def test_only_same_scope_surviving_images_are_preauthorized(self):
        first = self.add_image("scope-a", _png_bytes((1, 2, 3)))
        second = self.add_image("scope-a", _png_bytes((4, 5, 6)))
        other = self.add_image("scope-b", _png_bytes((7, 8, 9)), uid="2")
        stale = self.add_image("scope-a", _png_bytes((10, 11, 12)))
        Path(self.cache.info(stale)["full_path"]).unlink()  # simulate LRU eviction

        paths = self.paths_for("scope-a")
        self.assertEqual([Path(p).name for p in paths], [f"{second}.png", f"{first}.png"])
        self.assertEqual(
            resolve_authorized_image_paths(paths, allowed_image_paths=set(paths)),
            [Path(p).resolve() for p in paths],
        )
        other_path = self.cache.info(other)["full_path"]
        self.assertNotIn(other_path, paths)
        with self.assertRaisesRegex(ValueError, "outside the authorized"):
            resolve_authorized_image_paths([other_path], allowed_image_paths=set(paths))
        self.assertEqual(self.paths_for("scope-b"), [other_path])

    def test_at_most_eight_images_per_turn(self):
        digests = [self.add_image("scope-c", _png_bytes((i, i, i))) for i in range(10)]
        paths = self.paths_for("scope-c")
        self.assertEqual(len(paths), 8)
        self.assertEqual(
            [Path(p).name for p in paths], [f"{digest}.png" for digest in reversed(digests[-8:])]
        )


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
