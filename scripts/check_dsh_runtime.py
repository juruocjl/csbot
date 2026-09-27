"""DSH/Mneme protocol checks with a local mock provider, no real API key."""
import asyncio
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
import json
import os
import re
from pathlib import Path
import sqlite3
import sys
import tempfile
import threading
import time

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from ai_runtime.runner import run_dsh,scope_key

REQUESTS=[]


class Provider(BaseHTTPRequestHandler):
    def log_message(self,*_):pass
    def do_POST(self):
        body=json.loads(self.rfile.read(int(self.headers['Content-Length'])))
        REQUESTS.append(body)
        messages=body["messages"]
        if not body.get("tools"):
            time.sleep(0.3)  # Catch exit-before-distillation races with real latency.
            content='[{"type":"preference","title":"fixture preference","content":"QQ 123 喜欢茶。","importance":4}]'
            delta={"role":"assistant","content":content};finish="stop"
        elif messages[-1].get("role")=="tool" and "SOURCE_READ_CASE" in str(messages):
            body = str(messages[-1].get("content"))
            match = re.search(r'`([^`]+/plugins/fudu/__init__\.py)`', body)
            if match:
                delta={"role":"assistant","tool_calls":[{"index":0,"id":"call_source_code","type":"function","function":{"name":"read","arguments":json.dumps({"file_path":match[1],"offset":330,"limit":100})}}]};finish="tool_calls"
            else:
                delta={"role":"assistant","content":"Source checked."};finish="stop"
        elif messages[-1].get("role")=="tool":
            delta={"role":"assistant","content":"File permission checked."};finish="stop"
        elif "SOURCE_READ_CASE" in str(messages[-1].get("content")):
            delta={"role":"assistant","tool_calls":[{"index":0,"id":"call_source_index","type":"function","function":{"name":"read","arguments":json.dumps({"file_path":"SOURCE.md"})}}]};finish="tool_calls"
        elif "DENY_FILE" in str(messages[-1].get("content")):
            delta={"role":"assistant","tool_calls":[{"index":0,"id":"call_fixture","type":"function","function":{"name":"read","arguments":json.dumps({"file_path":"/etc/passwd"})}}]};finish="tool_calls"
        else:
            delta={"role":"assistant","content":"RAW_DRAFT" if "GUARD_CASE" in str(messages[-1].get("content")) else "收到，QQ 123 喜欢茶。"};finish="stop"
        self.send_response(200);self.send_header("Content-Type","text/event-stream");self.end_headers()
        for chunk in [{"choices":[{"index":0,"delta":delta,"finish_reason":None}]},
                      {"choices":[{"index":0,"delta":{},"finish_reason":finish}],"usage":{"prompt_tokens":100,"completion_tokens":30,"total_tokens":130}}]:
            self.wfile.write(('data: '+json.dumps({"id":"fixture","object":"chat.completion.chunk","created":1,"model":"fixture",**chunk})+'\n\n').encode())
        self.wfile.write(b'data: [DONE]\n\n')


async def main():
    server=ThreadingHTTPServer(("127.0.0.1",0),Provider)
    threading.Thread(target=server.serve_forever,daemon=True).start()
    try:
        with tempfile.TemporaryDirectory(prefix="dsh-runtime-check-") as tmp:
            root=Path(tmp)
            async def deny(_):raise ValueError("No data access in protocol fixture")
            async def run(scope,prompt,**options):
                return await run_dsh(scope=scope,text=prompt,context="UNTRUSTED_CHAT_SENTINEL: 旁听用户说了没有被调用的秘密。",
                    model="fixture",endpoint=f"http://127.0.0.1:{server.server_port}",api_key="fixture",
                    dispatch=deny,state_root=root,diagnostics=lambda value:print(value[-1500:]),**options)
            group=scope_key("qq","1","123")
            result=await run(group,"USER_SENTINEL: QQ 123 喜欢茶，请记住。")
            assert result["reason"]["kind"]=="completed" and not result["resumed"]
            distills=[r for r in REQUESTS if not r.get("tools")]
            assert distills,"Mneme did not run"
            assert "UNTRUSTED_CHAT_SENTINEL" not in json.dumps(distills,ensure_ascii=False)
            memory_files=list((root/"scopes"/group/"memory").rglob("*.db"))
            assert memory_files,"Mneme store missing"
            with sqlite3.connect(memory_files[0]) as db:
                assert db.execute("SELECT count(*) FROM memories").fetchone()[0]>0
            print("PASS: Mneme persists invoked dialogue; raw passive context excluded")
            start=len(REQUESTS)
            result=await run(group,"RESUME_CASE",remember=False)
            assert result["resumed"]
            assert "USER_SENTINEL" in json.dumps(REQUESTS[start]["messages"])
            print("PASS: stable group session resumes across process restart")
            start=len(REQUESTS)
            personal=scope_key("web","1","123")
            result=await run(personal,"PERSONAL_CASE",remember=False)
            assert "USER_SENTINEL" not in json.dumps(REQUESTS[start]["messages"])
            print("PASS: personal session is physically separate from group history")
            start=len(REQUESTS)
            result=await run(personal,"DENY_FILE",remember=False)
            tools={tool["function"]["name"] for tool in REQUESTS[start]["tools"]}
            assert tools<={"read","read_image","execute_python","memory_save","memory_search","memory_forget"}
            tool_messages=[m for r in REQUESTS[start:] for m in r["messages"] if m.get("role")=="tool"]
            assert any("outside the authorized" in str(m.get("content")) for m in tool_messages)
            assert not any("root:x:" in str(m.get("content")) for m in tool_messages)
            print("PASS: minimal tool schema; native file tool cannot read host files")
            start=len(REQUESTS)
            result=await run(scope_key("web","1","123","source"),"SOURCE_READ_CASE",remember=False)
            tool_messages=[m for r in REQUESTS[start:] for m in r["messages"] if m.get("role")=="tool"]
            assert any("random.choices" in str(m.get("content")) for m in tool_messages)
            assert any("calc_roll_point" in str(m.get("content")) for m in tool_messages)
            print("PASS: native read opens current source index and authorized business code without new tools")
            start=len(REQUESTS)
            async def guard(_):return "SAFE_FINAL"
            result=await run(scope_key("qq","2","123"),"GUARD_CASE",guard=guard)
            assert result["messages"][0]["content"]=="SAFE_FINAL"
            distills=[r for r in REQUESTS[start:] if not r.get("tools")]
            assert "SAFE_FINAL" in json.dumps(distills) and "RAW_DRAFT" not in json.dumps(distills)
            print("PASS: output guard acts before durable answer and Mneme distillation")
            imported=scope_key("qq","3","123")
            for _ in range(2):
                await run(imported,"IMPORT_CASE",remember=False,legacy_memory="QQ 123 手工保存：喜欢绿茶。")
            with sqlite3.connect(next((root/"scopes"/imported/"memory").rglob("*.db"))) as db:
                assert db.execute("SELECT count(*) FROM memories WHERE title='旧群记忆迁移 1'").fetchone()[0]==1
            print("PASS: explicit legacy memory import is durable and idempotent")
            result=await run_dsh(scope=group,text="COMPACT_CASE",context="旁听记录："+"合成聊天资料。"*6000,
                model="fixture",endpoint=f"http://127.0.0.1:{server.server_port}",api_key="fixture",
                dispatch=deny,state_root=root,remember=False,compact=True,diagnostics=lambda v:print(v[-1500:]))
            assert result["compacted"],"No durable compaction was performed"
            result=await run(group,"AFTER_COMPACT",remember=False)
            assert result["resumed"]
            print("PASS: DSH compacts a long session and resumes its durable replacement")
    finally:
        server.shutdown();server.server_close()


if __name__=="__main__":asyncio.run(main())
