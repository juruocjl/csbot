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
from ai_runtime.runner import run_dsh,scope_key,SYSTEM_PROMPT

REQUESTS=[]


class Provider(BaseHTTPRequestHandler):
    def log_message(self,*_):pass
    def do_POST(self):
        body=json.loads(self.rfile.read(int(self.headers['Content-Length'])))
        REQUESTS.append(body)
        messages=[m for m in body["messages"] if not (m.get("role")=="user" and "群聊基础知识（资料" in str(m.get("content")) and "完整证据目录" not in str(m.get("content")))]
        if not body.get("tools"):
            time.sleep(0.3)  # Catch exit-before-distillation races with real latency.
            content='[{"type":"preference","title":"fixture preference","content":"QQ 123 喜欢茶。","importance":4}]'
            if '核验群聊基础知识。' in str(messages):
                content='{"approved":true}'
            elif 'LAYER_FOUNDATION' in str(messages):
                catalog=json.loads(messages[-1]['content'].split('：\n',1)[1].split('\n已有基础知识',1)[0])
                ref=next(e for e in catalog if e['kind']=='user')
                content=json.dumps([{'title':'小茶称呼','content':'小茶是QQ 22222（茶茶）。','tier':'foundation','category':'alias','subject':'22222','keys':['小茶','茶茶'],'evidence':[{'id':ref['id'],'quote':'小茶是QQ 22222（茶茶）'}]}],ensure_ascii=False)
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
            system_text="\n".join(str(m.get("content", "")) for m in REQUESTS[0]["messages"] if m["role"]=="system")
            assert SYSTEM_PROMPT in system_text, "DSH profile patch dropped the actual system prompt"
            print("PASS: real provider request contains the complete reply and evidence policy")
            distills=[r for r in REQUESTS if not r.get("tools")]
            assert distills,"Mneme did not run"
            assert "群聊记忆的身份与证据规则" in json.dumps(distills,ensure_ascii=False)
            assert "发言者" in json.dumps(distills,ensure_ascii=False)
            assert "UNTRUSTED_CHAT_SENTINEL" not in json.dumps(distills,ensure_ascii=False)
            memory_files=list((root/"scopes"/group/"memory").rglob("*.db"))
            assert memory_files,"Mneme store missing"
            with sqlite3.connect(memory_files[0]) as db:
                assert db.execute("SELECT count(*) FROM memories").fetchone()[0]>0
            print("PASS: Mneme persists invoked dialogue; raw passive context excluded")
            with sqlite3.connect(memory_files[0]) as db:
                old=db.execute("SELECT id FROM memories WHERE title LIKE 'fixture preference [%'").fetchone()[0]
            start=len(REQUESTS)
            repaired=await run_dsh(scope=group,text='',context='',model='not-used',endpoint='http://127.0.0.1:1',api_key='not-used',dispatch=deny,state_root=root,remember=False,memory_only=True,
                memory_actions=[{'name':'memory_forget','arguments':{'id':old}},
                                {'name':'memory_save','arguments':{'type':'project','title':'群称呼确认 fixture','content':'群友明确确认：小茶指QQ 456，不是提问者QQ 123。','importance':5,'source':'explicit-correction-fixture'}}],
                memory_check_titles=['群称呼确认 fixture'])
            assert len(REQUESTS)==start and len(repaired['repaired'])==2
            with sqlite3.connect(memory_files[0]) as db:
                assert db.execute('SELECT forgotten FROM memories WHERE id=?',(old,)).fetchone()[0]==1
                assert db.execute("SELECT content FROM memories WHERE title='群称呼确认 fixture'").fetchone()[0].endswith('不是提问者QQ 123。')
            print('PASS: offline native correction is searchable, suppresses old memory, and makes no model call')
            start=len(REQUESTS)
            result=await run(group,"RESUME_CASE",remember=False)
            assert result["resumed"]
            assert "USER_SENTINEL" in json.dumps(REQUESTS[start]["messages"])
            assert SYSTEM_PROMPT in "\n".join(str(m.get("content", "")) for m in REQUESTS[start]["messages"] if m["role"]=="system")
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
            start=len(REQUESTS)
            await run(imported,'旧的偏好是什么？',remember=False)
            assert any('群聊基础知识' in str(m) and 'QQ 123 手工保存：喜欢绿茶。' in str(m) for m in REQUESTS[start]['messages'])
            print("PASS: explicit legacy memory import is durable, idempotent, and keeps confirmed priority in real model input")
            result=await run_dsh(scope=group,text="COMPACT_CASE",context="旁听记录："+"合成聊天资料。"*6000,
                model="fixture",endpoint=f"http://127.0.0.1:{server.server_port}",api_key="fixture",
                dispatch=deny,state_root=root,remember=False,compact=True,diagnostics=lambda v:print(v[-1500:]))
            assert result["compacted"],"No durable compaction was performed"
            result=await run(group,"AFTER_COMPACT",remember=False)
            assert result["resumed"]
            print("PASS: DSH compacts a long session and resumes its durable replacement")
            layered=scope_key('qq','layer-fixture','11111')
            await run(layered,'LAYER_FOUNDATION QQ用户 11111：确认小茶是QQ 22222（茶茶），记住。')
            with sqlite3.connect(root/'scopes'/layered/'memory/memory.db') as db:
                rows=[json.loads(r[0]) for r in db.execute('SELECT content FROM memories WHERE forgotten=0')]
                assert any(r['tier']=='foundation' and r['subject']=='22222' for r in rows),rows
            start=len(REQUESTS)
            await run(layered,'小茶是谁？',remember=False)
            assert any('群聊基础知识' in str(m) and '"subject":"22222"' in str(m) for m in REQUESTS[start]['messages']), REQUESTS[start]['messages']
            assert '"subject":"11111"' not in str(REQUESTS[start]['messages'])
            print('PASS: automatic promotion uses evidence, preserves subject, and injects foundation after process restart')

    finally:
        server.shutdown();server.server_close()


if __name__=="__main__":asyncio.run(main())
