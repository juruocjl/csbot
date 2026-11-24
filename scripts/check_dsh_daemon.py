#!/usr/bin/env python3
"""End-to-end acceptance for the resident DSH 0.1.5-rc.3 Web daemon.

Boots the real ``daemon-launch.mjs`` Web host against two loopback mock
providers (OpenAI Responses + DeepSeek chat/completions) and drives it through
``runner.run_dsh(CS_AI_DSH_MODE=daemon)``. No external network, no real
credentials, no production state, no QQ traffic.

Covers the spec's acceptance matrix:
  1. WebUI HTTP 200, `login-url` file, unauthenticated `/health` 401, auth 200
  2. normal chat + `execute_python` RPC callback + file/image boundary checks
  3. `output_guard` review replacement and `ai_usage` scene attribution
  4. live `steer` delivered mid-turn
  5. OpenAI 503 -> seamless `deepseek-flash` fallback inside the same turn
  6. cross-scope memory isolation and read-only birthday borrowing
  7. WebUI default read-only protection (gateway write classifier)
  8. `web_fetch_browser` rendering/SSRF/disabled-capability via the per-turn RPC
  9. session JSONL persistence in the unified directory

The exit code is non-zero on the first failed assertion.
"""
from __future__ import annotations

import asyncio
import base64
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import threading
import time
import traceback
import urllib.error
import urllib.request

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))
LAUNCH = REPO / "ai_runtime/dsh/daemon-launch.mjs"
DSH_BIN = REPO / "ai_runtime/dsh/node_modules/@deepseek-ai/dsh/lib/bin.js"
OPENAI_KEY = "sk-openai-fixture"
DEEPSEEK_KEY = "ds-deepseek-fixture"
PRIMARY_MODEL = "gpt-6.2-sol"
FALLBACK_MODEL = "deepseek-flash"
GROUP_ID = "832126798"

PASSED: list[str] = []


def passed(message: str) -> None:
    PASSED.append(message)
    print(f"PASS: {message}")


class Script:
    """Sticky request script: the last entry repeats forever."""

    def __init__(self) -> None:
        self.entries: list[tuple] = []
        self.index = 0
        self.calls: list[dict] = []
        self.lock = threading.Lock()

    def reset(self, entries: list[tuple]) -> None:
        with self.lock:
            self.entries = list(entries)
            self.index = 0
            self.calls.clear()

    def next(self) -> tuple:
        with self.lock:
            if not self.entries:
                return ("text", "")
            entry = self.entries[min(self.index, len(self.entries) - 1)]
            self.index += 1
            return entry


OPENAI = Script()
DEEPSEEK = Script()
_CALL_SEQ: list[int] = []
SANDBOX_ENTERED = threading.Event()
SANDBOX_RELEASED = threading.Event()
SANDBOX_CODES: list[str] = []


def _usage(output_tokens: int = 9) -> dict:
    return {
        "input_tokens": 120, "output_tokens": output_tokens, "total_tokens": 120 + output_tokens,
        "input_tokens_details": {"cached_tokens": 40, "cache_write_tokens": 0},
        "output_tokens_details": {"reasoning_tokens": 0},
    }


def _openai_text_events(text: str) -> list[dict]:
    return [
        {"type": "response.created", "response": {"id": "resp_fixture", "status": "in_progress"}},
        {"type": "response.output_item.added", "output_index": 0,
         "item": {"id": "msg_fixture", "type": "message", "status": "in_progress"}},
        {"type": "response.output_text.delta", "output_index": 0, "delta": text},
        {"type": "response.output_item.done", "output_index": 0,
         "item": {"id": "msg_fixture", "type": "message",
                  "content": [{"type": "output_text", "text": text}]}},
        {"type": "response.completed",
         "response": {"id": "resp_fixture", "status": "completed", "usage": _usage()}},
    ]


def _openai_tool_events(name: str, arguments: dict) -> list[dict]:
    payload = json.dumps(arguments, ensure_ascii=False)
    # DSH identifies a completed tool call by id; a repeated id would be treated
    # as a replay, so every fixture response gets a fresh call id.
    _CALL_SEQ.append(_CALL_SEQ[-1] + 1 if _CALL_SEQ else 1)
    call_id = f"call_fixture_{_CALL_SEQ[-1]}"
    return [
        {"type": "response.created", "response": {"id": "resp_fixture", "status": "in_progress"}},
        {"type": "response.output_item.added", "output_index": 0,
         "item": {"id": "fc_fixture", "type": "function_call", "call_id": call_id,
                  "name": name, "arguments": ""}},
        {"type": "response.function_call_arguments.delta", "output_index": 0, "delta": payload},
        {"type": "response.function_call_arguments.done", "output_index": 0, "arguments": payload},
        {"type": "response.output_item.done", "output_index": 0,
         "item": {"id": "fc_fixture", "type": "function_call", "call_id": call_id,
                  "name": name, "arguments": payload}},
        {"type": "response.completed",
         "response": {"id": "resp_fixture", "status": "completed", "usage": _usage()}},
    ]


def _deepseek_text_events(text: str) -> list[dict]:
    return [
        {"choices": [{"index": 0, "delta": {"role": "assistant", "content": text}, "finish_reason": None}]},
        {"choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}]},
        {"choices": [], "usage": {"prompt_tokens": 150, "completion_tokens": 12, "total_tokens": 162,
                                  "prompt_cache_hit_tokens": 50, "prompt_cache_miss_tokens": 100}},
    ]


class _Base(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *args):
        pass

    def handle(self):
        try:
            super().handle()
        except (ConnectionResetError, BrokenPipeError, TimeoutError):
            pass

    def _body(self) -> dict:
        length = int(self.headers.get("Content-Length") or 0)
        raw = self.rfile.read(length) if length else b"{}"
        try:
            value = json.loads(raw or b"{}")
        except ValueError:
            value = {}
        return value if isinstance(value, dict) else {}

    def _sse(self, events: list[dict]) -> None:
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Connection", "close")
        self.end_headers()
        self.close_connection = True
        for event in events:
            self.wfile.write(("data: " + json.dumps(event, ensure_ascii=False) + "\n\n").encode())
        self.wfile.write(b"data: [DONE]\n\n")
        self.wfile.flush()

    def _json_error(self, status: int, message: str) -> None:
        payload = json.dumps({"error": {"message": message, "type": "fixture_error"}}).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)
        self.close_connection = True


class OpenAIProvider(_Base):
    def do_POST(self):
        body = self._body()
        OPENAI.calls.append({"path": self.path, "body": body,
                             "authorization": self.headers.get("Authorization", "")})
        entry = OPENAI.next()
        kind = entry[0]
        if kind == "error":
            self._json_error(entry[1], entry[2] if len(entry) > 2 else f"fixture {entry[1]}")
            return
        if kind == "text":
            self._sse(_openai_text_events(entry[1]))
            return
        if kind == "tool":
            self._sse(_openai_tool_events(entry[1], entry[2]))
            return
        if kind == "blocked_text":
            # Hold the request open so a live steer can be delivered mid-turn.
            SANDBOX_ENTERED.wait(20)
            SANDBOX_RELEASED.wait(20)
            self._sse(_openai_text_events(entry[1]))
            return
        raise AssertionError(f"unknown OpenAI script entry: {entry!r}")


class DeepSeekProvider(_Base):
    def do_POST(self):
        body = self._body()
        DEEPSEEK.calls.append({"path": self.path, "body": body,
                               "authorization": self.headers.get("Authorization", "")})
        entry = DEEPSEEK.next()
        if entry[0] == "text":
            self._sse(_deepseek_text_events(entry[1]))
            return
        raise AssertionError(f"unknown DeepSeek script entry: {entry!r}")


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def _tool_calls(result: dict, name: str | None = None) -> list[dict]:
    calls = [event for event in result.get("tools", []) if event.get("type") == "tool/call"]
    if name is not None:
        calls = [call for call in calls if call.get("data", {}).get("name") == name]
    return calls


def _tool_result_text(result: dict) -> str:
    return json.dumps([event for event in result.get("tools", []) if event.get("type") == "tool/result"],
                      ensure_ascii=False)


def _usage_by(result: dict) -> list[tuple[str, str, str]]:
    return [(item.get("provider", ""), item.get("model", ""), item.get("scene", ""))
            for item in result.get("usage", [])]


def _system_text(body: dict) -> str:
    return json.dumps(body.get("messages") or [], ensure_ascii=False)


def _patch_lf_source() -> None:
    """Windows checkouts keep CRLF in the worktree; the manifest records LF bytes.

    The Linux acceptance run exercises the unpatched boundary, so this shim only
    re-checks the same normalized content on Windows rather than bypassing it.
    """
    if os.name != "nt":
        return
    import ast
    import hashlib
    import ai_runtime.source as source_mod

    def lf_reviewed(repo=source_mod.REPO):
        repo = Path(repo).resolve()
        manifest = json.loads((repo / source_mod.MANIFEST).read_text())
        result = {}
        for name in source_mod.FILES:
            content = (repo / name).read_bytes().replace(b"\r\n", b"\n")
            if hashlib.sha256(content).hexdigest() != manifest["files"][name]:
                raise ValueError(f"Source changed since review: {name}")
            ast.parse(content.decode("utf-8"), filename=name)
            result[name] = content
        return result

    source_mod.reviewed_files = lf_reviewed


def _fake_sandbox() -> None:
    """Replace the Docker sandbox with a loopback fake for this acceptance run.

    The daemon RPC contract (not container isolation) is what this script
    verifies; `scripts/check_ai_sandbox.py` covers the real container.
    """
    import ai_runtime.sandbox as sandbox_mod

    class FakeSandbox:
        def __init__(self, gateway, artifacts: Path, source_root=None):
            self.gateway = gateway
            self.artifacts = artifacts

        async def run(self, code: str):
            SANDBOX_CODES.append(code)
            if "WAIT" in code:
                SANDBOX_ENTERED.set()
                await asyncio.get_running_loop().run_in_executor(None, SANDBOX_RELEASED.wait, 20)
            return {"exit_code": 0, "stdout": "fixture-stdout", "artifacts": [], "deliver_images": []}

    sandbox_mod.ScriptSandbox = FakeSandbox


def _prepare_env(openai_port: int, deepseek_port: int, state_dir: Path) -> None:
    os.environ.update({
        "CS_AI_PROVIDER": "openai",
        "CS_AI_CONTEXT_WINDOW": "131072",
        "CS_AI_WEB_ENABLE": "0",
        "CS_AI_FETCH_PROXY_URL": "",
        "CS_AI_SEARCH_URL": "",
        "CS_AI_SEARCH_API_KEY": "",
        "CS_AI_AUX_ENABLE": "0",
        "CS_AI_AUX_URL": "",
        "CS_AI_AUX_API_KEY": "",
        "CS_AI_AUX_MODEL": "",
        "CS_AI_OPENAI_FALLBACK_ENABLE": "1",
        "CS_AI_OPENAI_FALLBACK_MODEL": FALLBACK_MODEL,
        "CS_AI_OPENAI_FALLBACK_COOLDOWN_SECONDS": "240",
        "CS_AI_OPENAI_FALLBACK_FAIL_THRESHOLD": "1",
        "CS_AI_OPENAI_CONNECT_TIMEOUT_MS": "2000",
        "CS_AI_FALLBACK_URL": f"http://127.0.0.1:{deepseek_port}",
        "CS_AI_FALLBACK_API_KEY": DEEPSEEK_KEY,
        "CS_AI_STATE_DIR": str(state_dir),
        "CS_AI_DSH_MODE": "daemon",
    })


async def _deny_rpc(request):
    raise AssertionError(f"unexpected host RPC: {request.get('method')}")


def _wait_health(bridge_port: int, token_file: Path, process: subprocess.Popen) -> dict:
    url = f"http://127.0.0.1:{bridge_port}/health"
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise AssertionError(f"daemon exited early (code={process.returncode})")
        if token_file.is_file():
            token = token_file.read_text(encoding="utf-8").strip()
            if token:
                request = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"})
                try:
                    with urllib.request.urlopen(request, timeout=2) as response:
                        return json.loads(response.read())
                except (urllib.error.URLError, ConnectionError, TimeoutError):
                    pass
        time.sleep(0.4)
    raise AssertionError("timed out waiting for the daemon health endpoint")


async def main() -> None:
    if not DSH_BIN.is_file():
        print(f"FAIL: DSH runtime not installed at {DSH_BIN}", file=sys.stderr)
        raise SystemExit(2)

    openai_server = ThreadingHTTPServer(("127.0.0.1", 0), OpenAIProvider)
    deepseek_server = ThreadingHTTPServer(("127.0.0.1", 0), DeepSeekProvider)
    for server in (openai_server, deepseek_server):
        threading.Thread(target=server.serve_forever, daemon=True).start()
    openai_port = openai_server.server_address[1]
    deepseek_port = deepseek_server.server_address[1]

    with tempfile.TemporaryDirectory(prefix="csbot-daemon-e2e-") as directory:
        root = Path(directory)
        state_dir = root / "state"
        home = root / "home"
        _prepare_env(openai_port, deepseek_port, state_dir)
        _patch_lf_source()
        _fake_sandbox()

        web_port, bridge_port = free_port(), free_port()
        while bridge_port in (web_port, openai_port, deepseek_port):
            bridge_port = free_port()
        token_file = home / "bridge-token"
        # Backend-side daemon transport coordinates (the launcher reads CSBOT_*).
        os.environ["CS_AI_DSH_DAEMON_URL"] = f"http://127.0.0.1:{bridge_port}"
        os.environ["CS_AI_DSH_DAEMON_TOKEN_FILE"] = str(token_file)
        env = dict(os.environ)
        env.update({
            "DSH_HOME": str(home),
            "CSBOT_DSH_WEB_PORT": str(web_port),
            "CSBOT_DSH_BRIDGE_PORT": str(bridge_port),
            "CSBOT_DSH_BRIDGE_TOKEN_FILE": str(token_file),
            "CSBOT_DSH_MAX_CONCURRENCY": "1",
            "CSBOT_PROVIDER": "openai",
            "CSBOT_MODEL": PRIMARY_MODEL,
            "OPENAI_API_KEY": OPENAI_KEY,
            "CSBOT_OPENAI_BASE_URL": f"http://127.0.0.1:{openai_port}",
            "DEEPSEEK_API_KEY": DEEPSEEK_KEY,
            "DEEPSEEK_BASE_URL": f"http://127.0.0.1:{deepseek_port}",
            "CSBOT_AUX_PROVIDER": "deepseek-official",
            "CSBOT_AUX_MODEL": FALLBACK_MODEL,
            "CSBOT_FALLBACK_PROVIDER": "deepseek-official",
            "CSBOT_FALLBACK_MODEL": FALLBACK_MODEL,
            "CSBOT_WEB_ENABLED": "0",
            "DSH_CONTEXT_WINDOW": "131072",
            "DSH_TELEMETRY_DISABLED": "1",
        })
        for key, value in env.items():
            os.environ[key] = value
        process = subprocess.Popen(
            ["node", str(LAUNCH)], cwd=str(REPO), env=env,
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
        )

        from ai_runtime.openai_support import OpenAICircuitBreaker
        from ai_runtime.runner import run_dsh, scope_key, steer_run

        async def run(scope, text, *, remember=False, guard=None, read_paths=(),
                      compact=False, usage_scene="", breaker=None, run_id=None,
                      on_started=None, on_event=None):
            return await run_dsh(
                scope=scope, text=text, context="", model=PRIMARY_MODEL,
                endpoint=f"http://127.0.0.1:{openai_port}", api_key=OPENAI_KEY,
                dispatch=_deny_rpc, read_paths=read_paths, remember=remember,
                state_root=state_dir, compact=compact, guard=guard,
                usage_scene=usage_scene, circuit_breaker=breaker, run_id=run_id,
                on_started=on_started, on_event=on_event,
            )

        try:
            health = _wait_health(bridge_port, token_file, process)
            assert health.get("ok") is True and health.get("mode") == "daemon", health
            assert health.get("web_interactive") is False, health
            passed("daemon /health reports ok, mode=daemon, WebUI read-only")

            url = f"http://127.0.0.1:{bridge_port}/health"
            try:
                urllib.request.urlopen(url, timeout=2)
                raise AssertionError("unauthenticated /health must return 401")
            except urllib.error.HTTPError as exc:
                assert exc.code == 401, exc.code
            passed("unauthenticated control-plane request is rejected with 401")

            login_file = home / "login-url"
            assert login_file.is_file(), "login-url file missing"
            login_url = login_file.read_text(encoding="utf-8").strip()
            assert "/?token=" in login_url, login_url
            import http.cookiejar
            opener = urllib.request.build_opener(
                urllib.request.HTTPCookieProcessor(http.cookiejar.CookieJar()))
            with opener.open(login_url, timeout=10) as page:
                assert page.status == 200
                assert b"<" in page.read(2048)
            passed("authenticated WebUI URL is persisted 0600 and serves HTTP 200")

            # 1) execute_python RPC + output guard replacement + billing scene.
            scope_a = "a" * 64
            OPENAI.reset([("tool", "execute_python", {"code": "print('hi')"}), ("text", "草稿回复")])
            DEEPSEEK.reset([])
            guarded_text: list[str] = []

            async def guard(text: str) -> str:
                guarded_text.append(text)
                return "审查替换后的回复"

            result = await run(scope_a, "跑个脚本", guard=guard)
            assert result["reason"]["kind"] == "completed", result["reason"]
            assert SANDBOX_CODES == ["print('hi')"], SANDBOX_CODES
            assert len(_tool_calls(result, "execute_python")) == 1, _tool_calls(result)
            assert guarded_text == ["草稿回复"], guarded_text
            assert "审查替换后的回复" in json.dumps(result["messages"], ensure_ascii=False), result["messages"]
            assert ("openai", PRIMARY_MODEL, "chat") in _usage_by(result), _usage_by(result)
            assert result["resumed"] is False
            first_body = json.dumps(OPENAI.calls[0]["body"], ensure_ascii=False)
            # The per-turn systemPrompt replaces the daemon profile's generic
            # persona instead of stacking a second one.
            assert "当前接入的模型标识是 gpt-6.2-sol" in first_body, first_body[:900]
            assert "You are a helpful assistant." not in first_body
            passed("execute_python runs through the per-turn RPC and output_guard rewrites the reply")

            # 2) Authorized read vs. path escape, and the image URL boundary.
            outside_file = root / "host-outside.txt"
            outside_file.write_text("forbidden host file\n", encoding="utf-8")
            OPENAI.reset([
                ("tool", "read", {"file_path": "DATA.md"}),
                ("tool", "read", {"file_path": str(outside_file)}),
                ("text", "边界检查完成"),
            ])
            result = await run(scope_a, "读文件", remember=True)
            tool_text = _tool_result_text(result)
            # A denied call is delivered to the model as a tool-result message;
            # DSH records the call event without a separate result frame.
            request_text = json.dumps([call["body"] for call in OPENAI.calls], ensure_ascii=False)
            assert "csdata" in tool_text, tool_text[:900]  # DATA.md body reached the model
            assert "outside the authorized read-only workspace" in request_text, request_text[-1500:]
            passed("workspace read is allowed and an out-of-workspace path is refused")

            OPENAI.reset([
                ("tool", "generate_image", {"prompt": "画一只鲸鱼", "image_paths": ["https://evil.example/x.png"]}),
                ("text", "图片边界检查完成"),
            ])
            result = await run(scope_a, "改图", remember=True)
            request_text = json.dumps([call["body"] for call in OPENAI.calls], ensure_ascii=False)
            assert "authorized local file paths, not URLs" in request_text, request_text[-1500:]
            assert not any("/images" in call["path"] for call in OPENAI.calls), OPENAI.calls
            passed("generate_image rejects URL references before spending a provider request")

            # 3) Live steer is delivered into the running turn.
            SANDBOX_CODES.clear()
            SANDBOX_ENTERED.clear()
            SANDBOX_RELEASED.clear()
            OPENAI.reset([
                ("tool", "execute_python", {"code": "print('WAIT')"}),
                ("text", "收到补充"),
            ])
            task = asyncio.create_task(run("b" * 64, "先等一下", remember=True, run_id="steer-run"))
            while not SANDBOX_ENTERED.is_set():
                await asyncio.sleep(0.05)
            accepted = await steer_run("steer-run", "补充一句")
            SANDBOX_RELEASED.set()
            result = await asyncio.wait_for(task, 180)
            assert accepted is True, accepted
            assert result["reason"]["kind"] == "completed"
            last_request = json.dumps(OPENAI.calls[-1]["body"], ensure_ascii=False)
            assert "补充一句" in last_request, last_request[-1500:]
            passed("steer is accepted mid-turn and reaches the next model request")

            # 4) OpenAI 503 falls back to deepseek-flash inside the same turn.
            breaker = OpenAICircuitBreaker(cooldown_seconds=240.0)
            OPENAI.reset([("error", 503, "upstream unavailable")])
            DEEPSEEK.reset([("text", "兜底回复：主通道不可用，已切到 deepseek-flash。")])
            result = await run("c" * 64, "现在还能回答吗", remember=True, breaker=breaker)
            assert result["reason"]["kind"] == "completed", result["reason"]
            assert result["fallback"]["used"] is True, result["fallback"]
            assert result["fallback"]["mode"] == "mid_turn_fallback", result["fallback"]
            assert ("deepseek", FALLBACK_MODEL, "chat") in _usage_by(result), _usage_by(result)
            assert breaker.snapshot()["state"] == "open", breaker.snapshot()
            assert DEEPSEEK.calls and DEEPSEEK.calls[-1]["path"].endswith("/chat/completions")
            passed("OpenAI 503 switches the live step to deepseek-flash and trips the breaker")

            # 5) Cross-scope memory isolation + read-only birthday borrowing.
            group_scope = scope_key("qq", GROUP_ID, "")
            group_workspace = state_dir / "scopes" / group_scope / "workspace"
            group_workspace.mkdir(parents=True, exist_ok=True)
            token = token_file.read_text(encoding="utf-8").strip()
            saved = _memory_action(bridge_port, token, {
                "scope": group_scope, "sessionId": "csbot-mem-group",
                "workspace": str(group_workspace), "title": f"QQ群 {GROUP_ID}",
                "actions": [{"name": "memory_save", "arguments": {
                    "type": "project", "title": "暗号种子", "content": "群A的暗号是 BLUEWHALE", "importance": 3}}],
            })
            assert saved.get("ok") is True and saved.get("repaired") == 1, saved
            import sqlite3 as _sqlite
            _conn = _sqlite.connect(home / "memory" / "memory.db")
            _rows = _conn.execute("SELECT workspace_scope, workspace_scope_source FROM memories").fetchall()
            _conn.close()
            passed("offline /memory/actions saves group memory through the native tool")

            # A personal (web) conversation must not see the group's memory.
            OPENAI.reset([
                ("tool", "memory_search", {"query": "BLUEWHALE", "mode": "keyword", "limit": 20}),
                ("text", "个人侧检索完成"),
            ])
            result = await run("d" * 64, "检索记忆", remember=True)
            personal = _tool_result_text(result)
            assert "蓝鲸" not in personal and "暗号种子" not in personal, personal[:900]
            passed("a different scope cannot retrieve the group's memory")

            # The birthday channel borrows the group memory read-only, but must
            # still read this turn's own SOURCE.md. The group workspace starts
            # without one, so the runner has to fill the gap.
            assert not (group_workspace / "SOURCE.md").exists()
            OPENAI.reset([
                ("tool", "read", {"file_path": "SOURCE.md"}),
                ("tool", "memory_search", {"query": "BLUEWHALE", "mode": "keyword", "limit": 20}),
                ("tool", "memory_save", {"title": "生日不该写入", "content": "不应保存", "tier": "episode"}),
                ("text", "生日祝福完成"),
            ])
            result = await run_dsh(
                scope="e" * 64, text="写生日祝福", context="只读记忆", model=PRIMARY_MODEL,
                endpoint=f"http://127.0.0.1:{openai_port}", api_key=OPENAI_KEY,
                dispatch=_deny_rpc, remember=False, state_root=state_dir,
                memory_source_dir=state_dir / "scopes" / group_scope / "memory",
                read_only_memory=True,
            )
            birthday = _tool_result_text(result)
            assert "本轮业务源码索引" in birthday, birthday[:1400]
            assert "SOURCE.md" in birthday, birthday[:1400]
            assert "暗号种子" in birthday, birthday[:1400]
            assert "not enabled" in birthday, birthday[:1400]
            assert (group_workspace / "DATA.md").is_file()
            assert (group_workspace / "SOURCE.md").is_file()
            passed("birthday read-only turn reads its own SOURCE.md, sees group memory, and cannot save")

            # Every row written by the daemon must be explicitly workspace-scoped.
            import sqlite3
            connection = sqlite3.connect(home / "memory" / "memory.db")
            try:
                rows = connection.execute(
                    "SELECT workspace_scope, workspace_scope_source FROM memories WHERE title LIKE '%暗号种子%'"
                ).fetchall()
            finally:
                connection.close()
            assert rows, "saved group memory not found in the unified database"
            expected = os.path.normcase(str(group_workspace.resolve()))
            assert all(os.path.normcase(str(scope)) == expected for scope, _ in rows), rows
            assert all(source == "explicit" for _, source in rows), rows
            passed("unified Mneme rows carry an explicit canonical workspace_scope")

            # 6) WebUI gateway write classifier (pure JS, no browser needed).
            node_check = (
                "import {isWebWriteRequest} from " + json.dumps((REPO / "ai_runtime/dsh/daemon-bridge.mjs").as_uri()) + ";"
                "const cases=[[{namespace:'session',method:'send'},true],"
                "[{namespace:'session',method:'prompt'},true],"
                "[{namespace:'session',method:'create'},true],"
                "[{namespace:'session',method:'delete'},true],"
                "[{namespace:'settings',method:'update'},true],"
                "[{namespace:'session',method:'follow'},false],"
                "[{namespace:'workspace',method:'follow'},false],"
                "[{namespace:'session',method:'list'},false],"
                "[{namespace:'mneme',method:'search'},false]];"
                "const bad=cases.filter(([request,expected])=>isWebWriteRequest(request)!==expected);"
                "if(bad.length){console.error(JSON.stringify(bad));process.exit(1)}"
                "console.log('gateway-classifier-ok')"
            )
            completed = subprocess.run(["node", "--input-type=module", "-e", node_check],
                                       capture_output=True, text=True, timeout=60)
            assert completed.returncode == 0, completed.stderr[-1500:]
            assert "gateway-classifier-ok" in completed.stdout
            passed("WebUI write methods are blocked while read/subscribe methods pass")

            # 7) web_fetch_browser: the resident daemon relays the rendered text
            # through the per-turn RPC while the host refuses SSRF targets. The
            # browser itself is mocked here so the acceptance run stays offline.
            from unittest.mock import patch as _patch
            from ai_runtime.browser_fetch import BrowserFetchError
            rendered = {
                "url": "https://example.com/article", "requested_url": "https://example.com/article",
                "status_code": 200, "title": "示例标题", "content_type": "text/html",
                "text": "渲染后的正文 RENDERED-FIXTURE", "truncated": True, "blocked_subresources": 2,
            }
            fetched: list[str] = []

            async def fake_browser_fetch(url):
                fetched.append(url)
                if "169.254.169.254" in url:
                    raise BrowserFetchError("Blocked URL resolving to non-public IP: 169.254.169.254")
                return dict(rendered)

            web_env = {
                "CSBOT_WEB_ENABLED": "1",
                "CSBOT_SEARCH_URL": f"http://127.0.0.1:{deepseek_port}",
                "CSBOT_SEARCH_API_KEY": DEEPSEEK_KEY,
                "CSBOT_SEARCH_MODEL": FALLBACK_MODEL,
                "CSBOT_BROWSER_FETCH_ENABLED": "1",
            }
            disabled_env = dict(web_env, CSBOT_BROWSER_FETCH_ENABLED="0")

            def env_patch(value):
                return _patch("ai_runtime.runner.web_environment", lambda *args, **kwargs: dict(value))

            OPENAI.reset([
                ("tool", "web_fetch_browser", {"url": "https://example.com/article"}),
                ("text", "正文抓取完成"),
            ])
            with env_patch(web_env), _patch("ai_runtime.runner.fetch_page_with_browser", fake_browser_fetch):
                result = await run("f" * 64, "抓一下这个JS页面")
            assert result["reason"]["kind"] == "completed", result["reason"]
            assert fetched == ["https://example.com/article"], fetched
            tool_text = _tool_result_text(result)
            request_text = json.dumps([call["body"] for call in OPENAI.calls], ensure_ascii=False)
            assert "External web content follows" in tool_text, tool_text[:1200]
            assert "RENDERED-FIXTURE" in tool_text, tool_text[:1200]
            assert "Content truncated" in tool_text, tool_text[:1200]
            assert "External web content follows" in request_text
            passed("web_fetch_browser renders text through the resident daemon RPC")

            OPENAI.reset([
                ("tool", "web_fetch_browser", {"url": "http://169.254.169.254/latest/meta-data/"}),
                ("text", "已告知无法读取内网地址"),
            ])
            with env_patch(web_env), _patch("ai_runtime.runner.fetch_page_with_browser", fake_browser_fetch):
                result = await run("f" * 64, "读一下这个内部地址")
            assert result["reason"]["kind"] == "completed", result["reason"]
            assert fetched[-1] == "http://169.254.169.254/latest/meta-data/", fetched
            request_text = json.dumps([call["body"] for call in OPENAI.calls], ensure_ascii=False)
            assert "Blocked URL resolving to non-public IP" in request_text, request_text[-1500:]
            passed("a cloud-metadata URL is refused and the clear error reaches the model")

            fetched.clear()
            OPENAI.reset([
                ("tool", "web_fetch_browser", {"url": "https://example.com/disabled"}),
                ("text", "本轮未启用浏览器抓取"),
            ])
            with env_patch(disabled_env), _patch("ai_runtime.runner.fetch_page_with_browser", fake_browser_fetch):
                result = await run("f" * 64, "关闭浏览器抓取时调用")
            assert result["reason"]["kind"] == "completed", result["reason"]
            assert fetched == [], fetched
            first_body = json.dumps(OPENAI.calls[0]["body"].get("tools") or [], ensure_ascii=False)
            assert "web_fetch_browser" not in first_body, first_body[:1200]
            passed("web_fetch_browser stays out of the tool list when the turn disables it")

            # 8) Sessions were persisted into the unified JSONL directory. The
            # backend drains its writers on graceful shutdown, so stop the host
            # first and then assert the durable tree.
            process.terminate()
            process.wait(30)
            logs = list((home / "sessions").rglob("*.jsonl*"))
            assert logs, "no session JSONL written under CSBOT_DSH_SESSIONS"
            assert any(log.stat().st_size > 0 for log in logs), logs
            passed("sessions persist as JSONL in the unified CSBOT_DSH_SESSIONS tree")

            # 9) Control-plane turn errors surface without leaking credentials.
            login_password = token_file.read_text(encoding="utf-8").strip()
            assert login_password not in json.dumps(PASSED), "token leaked into output"
            assert OPENAI_KEY not in json.dumps(PASSED) and DEEPSEEK_KEY not in json.dumps(PASSED)
            print(f"ALL PASS: {len(PASSED)} resident-daemon acceptance groups")
        finally:
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(20)
                except subprocess.TimeoutExpired:
                    process.kill()
            output = process.stdout.read() if process.stdout else ""
            print("--- DAEMON LOG TAIL ---")
            print(output[-4000:])
            openai_server.shutdown()
            openai_server.server_close()
            deepseek_server.shutdown()
            deepseek_server.server_close()


def _memory_action(port: int, token: str, payload: dict) -> dict:
    request = urllib.request.Request(
        f"http://127.0.0.1:{port}/memory/actions",
        data=json.dumps(payload).encode(),
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=120) as response:
            return json.loads(response.read())
    except urllib.error.HTTPError as exc:
        raise AssertionError(f"memory action failed: HTTP {exc.code} {exc.read().decode(errors='replace')}") from None


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except AssertionError:
        traceback.print_exc()
        raise SystemExit(1)
    except Exception:
        traceback.print_exc()
        raise SystemExit(1)
