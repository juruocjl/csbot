"""Native DSH streaming/steering checks; local synthetic provider, no credentials."""
import asyncio
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import sys
import tempfile
import threading
import time
from unittest.mock import patch
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from ai_runtime.runner import run_dsh, steer_run, scope_key
from ai_runtime.sandbox import ScriptSandbox

REQUESTS=[]
class Provider(BaseHTTPRequestHandler):
    def log_message(self,*_):pass
    def do_POST(self):
        body=json.loads(self.rfile.read(int(self.headers['Content-Length'])))
        REQUESTS.append(body)
        text=json.dumps(body['messages'])
        self.send_response(200);self.send_header('Content-Type','text/event-stream');self.end_headers()
        def send(delta,finish=None):
            chunk={'id':'fixture','object':'chat.completion.chunk','created':1,'model':'fixture','choices':[{'index':0,'delta':delta,'finish_reason':finish}]}
            self.wfile.write(('data: '+json.dumps(chunk)+'\n\n').encode());self.wfile.flush()
        send({'role':'assistant','reasoning_content':'REAL_REASONING_SENTINEL'})
        time.sleep(.25)
        if 'SUPPLEMENT_SENTINEL' in text:
            send({'content':'FIRST_ANSWER + SUPPLEMENT_ANSWER'},'stop')
        elif any(m['role']=='tool' for m in body['messages']):
            send({'content':'FIRST_ANSWER'},'stop')
        else:
            send({'tool_calls':[{'index':0,'id':'fixture_tool','type':'function','function':{'name':'execute_python','arguments':'{"code":"print(42)"}'}}]},'tool_calls')
        self.wfile.write(b'data: [DONE]\n\n');self.wfile.flush()

async def main():
    server=ThreadingHTTPServer(('127.0.0.1',0),Provider)
    threading.Thread(target=server.serve_forever,daemon=True).start()
    try:
        with tempfile.TemporaryDirectory() as tmp:
            for guarded in (False,True):
                started=asyncio.Event();release=asyncio.Event();called=asyncio.Event();seen=[];second_started=asyncio.Event()
                async def sandbox(self,code):
                    started.set();await release.wait()
                    return {'exit_code':0,'output':'42','artifacts':[],'deliver_images':[]}
                async def deny(_):raise AssertionError('not used')
                async def guard(text):return text
                def event(e):
                    seen.append(e)
                    if e['type']=='tool/call':called.set()
                options=dict(context='',model='fixture',endpoint=f'http://127.0.0.1:{server.server_port}',api_key='fixture',dispatch=deny,state_root=Path(tmp),remember=False,thinking=True,diagnostics=print)
                with patch.object(ScriptSandbox,'run',sandbox):
                    first=asyncio.create_task(run_dsh(scope=scope_key('qq' if guarded else 'web','g','u'),text='FIRST_QUESTION',run_id='first',on_event=event,guard=guard if guarded else None,**options))
                    await asyncio.wait_for(started.wait(),15)
                    await asyncio.wait_for(called.wait(),2)
                    assert not first.done(), 'events only arrived after completion'
                    assert any(e['type']=='reasoning_delta' or (e['type']=='assistant_stream' and e['frame'].get('chunk',{}).get('type')=='reasoning-delta') for e in seen)
                    async def on_second():second_started.set()
                    second=asyncio.create_task(run_dsh(scope=scope_key('web','g','queued'+str(guarded)),text='SECOND',on_started=on_second,**options))
                    await asyncio.sleep(.15)
                    assert not second_started.is_set(), 'global execution slot failed'
                    assert await asyncio.wait_for(steer_run('first','SUPPLEMENT_SENTINEL'),2), 'steer blocked behind tool execution'
                    release.set()
                    result=await asyncio.wait_for(first,20)
                    assert 'SUPPLEMENT_ANSWER' in str(result['messages']) and 'FIRST_ANSWER' in str(result['messages'])
                    assert not await steer_run('first','TOO_LATE')
                    await asyncio.wait_for(second,20)
                    results=[e for e in seen if e['type']=='tool/result']
                    assert results[0]['data']['message']['content'][0]['toolCallId']=='fixture_tool'
                    assert second_started.is_set()
                    Path('/tmp/csbot-native-trace.json').write_text(json.dumps(seen))
                    print('PASS:', 'QQ guarded' if guarded else 'web', 'real reasoning before completion, live tool start, steering during tool, both answers retained, global queue, late insertion rejected')
    finally:server.shutdown();server.server_close()

if __name__=='__main__':asyncio.run(main())
