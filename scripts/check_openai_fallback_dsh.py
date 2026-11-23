#!/usr/bin/env python3
"""End-to-end acceptance for the OpenAI -> deepseek-flash fallback (Linux).

Runs the pinned DSH runtime (0.1.5-rc.3) against two loopback mock providers:
an OpenAI Responses endpoint and a DeepSeek `/chat/completions` endpoint. No
external network, no real credentials, no production state, no QQ traffic.

Covers the acceptance matrix from `spec.md` section 4.2:
  1. mid-turn seamless fallback + breaker trip
  2. trip-open turns go straight to DeepSeek with degraded tools
  3. half-open probe recovery closes the breaker
  4. half-open probe failure re-opens and the turn still succeeds
  5. tool side effects are never replayed by the retried step
  6. image tools are refused honestly instead of looping
  7. cross-provider history (image + reasoning blocks) stays usable
  8. business 400 never triggers fallback or a trip
  9. birthday scene attribution and compaction fallback

The exit code is non-zero on the first failed assertion.
"""
from __future__ import annotations

import asyncio
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import sys
import tempfile
import threading
import traceback

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

from ai_runtime.openai_support import OpenAICircuitBreaker  # noqa: E402
from ai_runtime.runner import run_dsh  # noqa: E402

DSH_BIN = REPO / "ai_runtime/dsh/node_modules/.bin/dsh"
OPENAI_KEY = "sk-openai-fixture"
DEEPSEEK_KEY = "ds-deepseek-fixture"
PRIMARY_MODEL = "gpt-6.2-sol"
FALLBACK_MODEL = "deepseek-flash"


class Script:
    """Sticky request script: the last entry repeats forever."""

    def __init__(self) -> None:
        self.entries: list[tuple] = []
        self.index = 0
        self.calls: list[dict] = []

    def reset(self, entries: list[tuple]) -> None:
        self.entries = list(entries)
        self.index = 0
        self.calls.clear()

    def next(self) -> tuple:
        if not self.entries:
            return ("text", "")
        entry = self.entries[min(self.index, len(self.entries) - 1)]
        self.index += 1
        return entry


OPENAI = Script()
DEEPSEEK = Script()


def _usage(output_tokens: int = 9) -> dict:
    return {
        "input_tokens": 120,
        "output_tokens": output_tokens,
        "total_tokens": 120 + output_tokens,
        "input_tokens_details": {"cached_tokens": 40, "cache_write_tokens": 0},
        "output_tokens_details": {"reasoning_tokens": 0},
    }


def _openai_text_events(text: str, reasoning: str = "") -> list[dict]:
    events: list[dict] = [
        {"type": "response.created", "response": {"id": "resp_fixture", "status": "in_progress"}}
    ]
    index = 0
    if reasoning:
        events += [
            {"type": "response.output_item.added", "output_index": 0,
             "item": {"id": "rs_fixture", "type": "reasoning", "summary": []}},
            {"type": "response.reasoning_summary_text.delta", "output_index": 0,
             "delta": reasoning},
            {"type": "response.output_item.done", "output_index": 0,
             "item": {"id": "rs_fixture", "type": "reasoning",
                      "summary": [{"type": "summary_text", "text": reasoning}]}},
        ]
        index = 1
    events += [
        {"type": "response.output_item.added", "output_index": index,
         "item": {"id": "msg_fixture", "type": "message", "status": "in_progress"}},
        {"type": "response.output_text.delta", "output_index": index, "delta": text},
        {"type": "response.output_item.done", "output_index": index,
         "item": {"id": "msg_fixture", "type": "message",
                  "content": [{"type": "output_text", "text": text}]}},
        {"type": "response.completed",
         "response": {"id": "resp_fixture", "status": "completed", "usage": _usage()}},
    ]
    return events


def _openai_tool_events(name: str, arguments: dict) -> list[dict]:
    payload = json.dumps(arguments, ensure_ascii=False)
    return [
        {"type": "response.created", "response": {"id": "resp_fixture", "status": "in_progress"}},
        {"type": "response.output_item.added", "output_index": 0,
         "item": {"id": "fc_fixture", "type": "function_call", "call_id": "call_fixture",
                  "name": name, "arguments": ""}},
        {"type": "response.function_call_arguments.delta", "output_index": 0, "delta": payload},
        {"type": "response.function_call_arguments.done", "output_index": 0,
         "arguments": payload},
        {"type": "response.output_item.done", "output_index": 0,
         "item": {"id": "fc_fixture", "type": "function_call", "call_id": "call_fixture",
                  "name": name, "arguments": payload}},
        {"type": "response.completed",
         "response": {"id": "resp_fixture", "status": "completed", "usage": _usage()}},
    ]


def _deepseek_text_events(text: str) -> list[dict]:
    return [
        {"choices": [{"index": 0, "delta": {"role": "assistant", "content": text},
                      "finish_reason": None}]},
        {"choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}]},
        {"choices": [], "usage": {"prompt_tokens": 150, "completion_tokens": 12,
                                  "total_tokens": 162, "prompt_cache_hit_tokens": 50,
                                  "prompt_cache_miss_tokens": 100}},
    ]


def _deepseek_tool_events(name: str, arguments: dict) -> list[dict]:
    call = {"index": 0, "id": "call_ds_fixture", "type": "function",
            "function": {"name": name, "arguments": json.dumps(arguments, ensure_ascii=False)}}
    return [
        {"choices": [{"index": 0, "delta": {"role": "assistant", "tool_calls": [call]},
                      "finish_reason": None}]},
        {"choices": [{"index": 0, "delta": {}, "finish_reason": "tool_calls"}]},
        {"choices": [], "usage": {"prompt_tokens": 150, "completion_tokens": 12,
                                  "total_tokens": 162, "prompt_cache_hit_tokens": 50,
                                  "prompt_cache_miss_tokens": 100}},
    ]


class _Base(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *args):  # keep the acceptance output readable
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
        if kind == "reason_text":
            self._sse(_openai_text_events(entry[2], reasoning=entry[1]))
            return
        if kind == "tool":
            self._sse(_openai_tool_events(entry[1], entry[2]))
            return
        if kind == "reason_tool":
            payload = json.dumps(entry[3], ensure_ascii=False)
            events = [
                {"type": "response.created",
                 "response": {"id": "resp_fixture", "status": "in_progress"}},
                {"type": "response.output_item.added", "output_index": 0,
                 "item": {"id": "rs_fixture", "type": "reasoning", "summary": []}},
                {"type": "response.reasoning_summary_text.delta", "output_index": 0,
                 "delta": entry[1]},
                {"type": "response.output_item.done", "output_index": 0,
                 "item": {"id": "rs_fixture", "type": "reasoning",
                          "summary": [{"type": "summary_text", "text": entry[1]}]}},
                {"type": "response.output_item.added", "output_index": 1,
                 "item": {"id": "fc_fixture", "type": "function_call",
                          "call_id": "call_fixture", "name": entry[2], "arguments": ""}},
                {"type": "response.function_call_arguments.delta", "output_index": 1,
                 "delta": payload},
                {"type": "response.function_call_arguments.done", "output_index": 1,
                 "arguments": payload},
                {"type": "response.output_item.done", "output_index": 1,
                 "item": {"id": "fc_fixture", "type": "function_call",
                          "call_id": "call_fixture", "name": entry[2], "arguments": payload}},
                {"type": "response.completed",
                 "response": {"id": "resp_fixture", "status": "completed", "usage": _usage()}},
            ]
            self._sse(events)
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
        if entry[0] == "tool":
            self._sse(_deepseek_tool_events(entry[1], entry[2]))
            return
        raise AssertionError(f"unknown DeepSeek script entry: {entry!r}")


def _tool_calls(result: dict, name: str | None = None) -> list[dict]:
    calls = [event for event in result.get("tools", [])
             if event.get("type") == "tool/call"]
    if name is not None:
        calls = [event for event in calls if event.get("data", {}).get("name") == name]
    return calls


def _tool_result_text(result: dict) -> str:
    return json.dumps(
        [event for event in result.get("tools", []) if event.get("type") == "tool/result"],
        ensure_ascii=False,
    )


def _usage_by(result: dict) -> list[tuple[str, str, str]]:
    return [(item.get("provider", ""), item.get("model", ""), item.get("scene", ""))
            for item in result.get("usage", [])]


def _system_text(body: dict) -> str:
    messages = body.get("messages") or []
    return json.dumps(messages, ensure_ascii=False)


def _tool_names(body: dict) -> set[str]:
    names = set()
    for tool in body.get("tools") or []:
        if isinstance(tool, dict):
            function = tool.get("function") or {}
            if isinstance(function, dict) and function.get("name"):
                names.add(str(function["name"]))
    return names


async def _deny_rpc(request):
    raise AssertionError(f"unexpected host RPC during fallback acceptance: {request.get('method')}")


def _diagnostics(value) -> None:
    text = str(value).strip()
    if text:
        print("DSH-DIAG: " + text[:2000], file=sys.stderr)


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
    })


def _clock_breaker(clock: list[float], **kwargs) -> OpenAICircuitBreaker:
    return OpenAICircuitBreaker(
        monotonic_fn=lambda: clock[0],
        wall_fn=lambda: 1_700_000_000.0 + clock[0],
        **kwargs,
    )


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
    endpoint = f"http://127.0.0.1:{openai_port}"

    with tempfile.TemporaryDirectory(prefix="csbot-fallback-e2e-") as directory:
        state_dir = Path(directory)
        _prepare_env(openai_port, deepseek_port, state_dir)

        async def run(text, *, scope, vision=False, remember=False, compact=False,
                      usage_scene="", read_paths=(), breaker=None):
            return await run_dsh(
                scope=scope, text=text, context="", model=PRIMARY_MODEL,
                endpoint=endpoint, api_key=OPENAI_KEY, dispatch=_deny_rpc,
                read_paths=read_paths, remember=remember, vision=vision,
                state_root=state_dir, compact=compact, usage_scene=usage_scene,
                circuit_breaker=breaker, diagnostics=_diagnostics,
            )

        try:
            clock = [0.0]
            breaker = _clock_breaker(clock, cooldown_seconds=240.0)

            # 1) Mid-turn fallback: every OpenAI attempt fails with 503. The
            #    native retry policy allows one fast retry, then the step moves.
            OPENAI.reset([("error", 503, "upstream unavailable")])
            DEEPSEEK.reset([("text", "兜底回复：主通道不可用，已切到 deepseek-flash。")])
            result = await run("今天状态如何？", scope="fallback-mid-turn", breaker=breaker)
            assert result["reason"]["kind"] == "completed"
            assert result["messages"], result
            assert FALLBACK_MODEL in json.dumps(result, ensure_ascii=False)
            assert result["fallback"]["used"] is True, result["fallback"]
            assert result["fallback"]["mode"] == "mid_turn_fallback", result["fallback"]
            usages = _usage_by(result)
            assert ("deepseek", FALLBACK_MODEL, "chat") in usages, usages
            snapshot = breaker.snapshot()
            assert snapshot["state"] == "open", snapshot
            assert snapshot["trip_count"] == 1, snapshot
            assert len(OPENAI.calls) == 2, len(OPENAI.calls)  # one fast retry only
            assert DEEPSEEK.calls and DEEPSEEK.calls[-1]["path"].endswith("/chat/completions")
            print("PASS: mid-turn 503 falls back to deepseek-flash, native retry capped at 1, breaker tripped")

            # 2) Inside the cooldown the next turn never touches OpenAI, and the
            #    degraded tool schema / system prompt are honest about it.
            OPENAI.calls.clear()
            DEEPSEEK.reset([("text", "熔断直降回复。")])
            result = await run("下一轮呢？", scope="fallback-mid-turn", breaker=breaker)
            assert len(OPENAI.calls) == 0, len(OPENAI.calls)
            assert result["fallback"]["mode"] == "circuit_open_fallback", result["fallback"]
            assert ("deepseek", FALLBACK_MODEL, "chat") in _usage_by(result)
            deepseek_body = DEEPSEEK.calls[-1]["body"]
            tools = _tool_names(deepseek_body)
            assert "generate_image" not in tools, tools
            assert "read_image" not in tools, tools
            prompt = _system_text(deepseek_body)
            assert FALLBACK_MODEL in prompt and "降级" in prompt, prompt[:500]
            print("PASS: trip-open turn skips OpenAI entirely, drops image tools, states the downgrade")

            # 3) After the cooldown one probe is admitted; success closes it.
            clock[0] = 241.0
            OPENAI.reset([("text", "主通道恢复回复。")])
            DEEPSEEK.calls.clear()
            result = await run("恢复了吗？", scope="fallback-mid-turn", breaker=breaker)
            assert result["fallback"]["mode"] == "primary", result["fallback"]
            assert ("openai", PRIMARY_MODEL, "chat") in _usage_by(result)
            snapshot = breaker.snapshot()
            assert snapshot["state"] == "closed", snapshot
            assert snapshot["recovery_count"] == 1, snapshot
            assert len(OPENAI.calls) == 1, len(OPENAI.calls)
            assert DEEPSEEK.calls == [], DEEPSEEK.calls
            print("PASS: half-open probe success closes the breaker and restores OpenAI")

            # 4) Probe failure: 429 fails fast (maxRetries=0) and re-opens while
            #    the turn itself still completes on deepseek.
            clock2 = [0.0]
            breaker2 = _clock_breaker(clock2, cooldown_seconds=240.0)
            OPENAI.reset([("error", 429, "rate limited")])
            DEEPSEEK.reset([("text", "限流后的兜底回复。")])
            await run("限流测试", scope="fallback-probe-fail", breaker=breaker2)
            assert breaker2.snapshot()["state"] == "open"
            clock2[0] = 241.0
            OPENAI.reset([("error", 429, "rate limited")])
            DEEPSEEK.reset([("text", "探测失败仍完成本轮。")])
            result = await run("探测失败", scope="fallback-probe-fail", breaker=breaker2)
            assert result["reason"]["kind"] == "completed"
            assert result["fallback"]["mode"] == "mid_turn_fallback", result["fallback"]
            assert len(OPENAI.calls) == 1, len(OPENAI.calls)  # probe fast-fail: one request
            assert breaker2.snapshot()["state"] == "open"
            assert breaker2.snapshot()["trip_count"] == 2, breaker2.snapshot()
            print("PASS: half-open 429 probe fails once, re-opens, and the turn still answers on deepseek")

            # 5) A completed tool side effect is never replayed by the retried step.
            OPENAI.reset([("tool", "read", {"file_path": "DATA.md"}),
                          ("error", 503, "upstream unavailable")])
            DEEPSEEK.reset([("text", "工具结果已保留，继续收尾。")])
            result = await run("先读 DATA.md 再总结", scope="fallback-tool-once",
                               breaker=_clock_breaker([0.0], cooldown_seconds=240.0))
            assert len(_tool_calls(result, "read")) == 1, _tool_calls(result, "read")
            assert result["fallback"]["mode"] == "mid_turn_fallback"
            assert "工具结果已保留" in json.dumps(result["messages"], ensure_ascii=False)
            print("PASS: completed tool call is executed exactly once across the fallback retry")

            # 6) Image tools are refused with honest guidance instead of looping.
            OPENAI.reset([("error", 503, "upstream unavailable")])
            DEEPSEEK.reset([
                ("tool", "generate_image", {"prompt": "画一只鲸鱼"}),
                ("tool", "read_image", {"file_path": "DATA.md"}),
                ("text", "当前无法生图或看图，已如实说明。"),
            ])
            result = await run("画只鲸鱼并看看那张图", scope="fallback-tools-denied",
                               breaker=_clock_breaker([0.0], cooldown_seconds=240.0))
            assert result["reason"]["kind"] == "completed"
            assert len(_tool_calls(result, "generate_image")) == 1, _tool_calls(result)
            assert len(_tool_calls(result, "read_image")) == 1, _tool_calls(result)
            tool_text = _tool_result_text(result)
            assert "无法生成或修改图片" in tool_text, tool_text[:800]
            assert "无法查看原图" in tool_text, tool_text[:800]
            assert not any("/images" in call["path"] for call in OPENAI.calls)
            print("PASS: generate_image / read_image are refused in degraded mode without extra loop")

            # 7) Cross-provider history: image + reasoning blocks from OpenAI must
            #    stay usable on the text-only fallback and again after recovery.
            from PIL import Image
            image_path = state_dir / "history-fixture.png"
            Image.new("RGB", (16, 16), (10, 20, 30)).save(image_path)
            OPENAI.reset([
                ("reason_tool", "先看一眼图片", "read_image", {"file_path": str(image_path)}),
                ("reason_text", "图片记下了", "第一轮已看过原图。"),
            ])
            DEEPSEEK.calls.clear()
            first = await run("看看这张图", scope="fallback-history", vision=True,
                              read_paths=[str(image_path)],
                              breaker=_clock_breaker([0.0], cooldown_seconds=0.0))
            assert "第一轮已看过原图" in json.dumps(first["messages"], ensure_ascii=False)
            assert len(_tool_calls(first, "read_image")) == 1

            OPENAI.reset([("error", 503, "upstream unavailable")])
            DEEPSEEK.reset([("text", "带着历史继续聊，没有报错。")])
            second = await run("继续聊", scope="fallback-history",
                               breaker=_clock_breaker([0.0], cooldown_seconds=0.0))
            assert second["fallback"]["mode"] == "mid_turn_fallback", second["fallback"]
            deepseek_body = json.dumps(DEEPSEEK.calls[-1]["body"], ensure_ascii=False)
            assert "image_url" not in deepseek_body, deepseek_body[:800]
            assert "data:image" not in deepseek_body, deepseek_body[:800]

            OPENAI.reset([("text", "第三轮主通道恢复。")])
            DEEPSEEK.calls.clear()
            third = await run("第三轮", scope="fallback-history",
                              breaker=_clock_breaker([0.0], cooldown_seconds=0.0))
            assert third["fallback"]["mode"] == "primary", third["fallback"]
            assert "第三轮主通道恢复" in json.dumps(third["messages"], ensure_ascii=False)
            print("PASS: cross-provider history with image/reasoning blocks replays on both channels")

            # 8) A business 400 must neither fall back nor trip the breaker.
            breaker8 = _clock_breaker([0.0], cooldown_seconds=240.0)
            OPENAI.reset([("error", 400, "invalid request: bad tool schema")])
            DEEPSEEK.reset([("text", "不应被调用。")])
            try:
                await run("非法请求", scope="fallback-invalid", breaker=breaker8)
            except RuntimeError as exc:
                assert "DSH turn did not complete" in str(exc), exc
            else:
                raise AssertionError("400 INVALID_REQUEST must fail the turn, not fall back")
            assert DEEPSEEK.calls == [], DEEPSEEK.calls
            assert breaker8.snapshot()["state"] == "closed", breaker8.snapshot()
            assert breaker8.snapshot()["trip_count"] == 0
            print("PASS: 400 INVALID_REQUEST fails closed without fallback or trip")

            # 9a) Birthday scene keeps its attribution on the fallback channel.
            OPENAI.reset([("error", 503, "upstream unavailable")])
            DEEPSEEK.reset([("text", "生日快乐，祝你新的一岁顺顺利利。")])
            birthday = await run("生日祝福", scope="fallback-birthday",
                                 usage_scene="birthday_wish",
                                 breaker=_clock_breaker([0.0], cooldown_seconds=240.0))
            assert ("deepseek", FALLBACK_MODEL, "birthday_wish") in _usage_by(birthday), _usage_by(birthday)
            print("PASS: birthday_wish scene is billed as deepseek/deepseek-flash")

            # 9b) Compaction failure falls back inside the same turn.
            OPENAI.reset([("text", "主回答已完成。"), ("error", 503, "upstream unavailable")])
            DEEPSEEK.reset([("text", "压缩摘要：本轮对话要点。")])
            compacted = await run("先回答再压缩", scope="fallback-compaction", compact=True,
                                  breaker=_clock_breaker([0.0], cooldown_seconds=240.0))
            assert compacted.get("compacted") is True, compacted.get("compacted")
            assert ("deepseek", FALLBACK_MODEL, "compaction") in _usage_by(compacted), _usage_by(compacted)
            print("PASS: compaction failure retries on deepseek-flash with compaction billing")

            # Credential hygiene: no fixture key ever reached an unrelated origin.
            leaked = [
                call for call in DEEPSEEK.calls
                if call["authorization"] not in ("", f"Bearer {DEEPSEEK_KEY}")
            ]
            assert leaked == [], [(call["path"], call["authorization"]) for call in leaked]
            assert not any(call["authorization"] == f"Bearer {OPENAI_KEY}" for call in DEEPSEEK.calls)
            assert not any(call["authorization"] == f"Bearer {DEEPSEEK_KEY}" for call in OPENAI.calls)
            print("PASS: provider credentials never cross origins")
        finally:
            openai_server.shutdown()
            openai_server.server_close()
            deepseek_server.shutdown()
            deepseek_server.server_close()

    print("ALL PASS: OpenAI fallback end-to-end acceptance")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except AssertionError:
        traceback.print_exc()
        raise SystemExit(1)
