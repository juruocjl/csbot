"""Native DSH web tools against a loopback mock; no real secrets or internet."""
import asyncio
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import sys
import tempfile
import threading
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ai_runtime.runner import run_dsh, web_environment

CALLS=[]
class Provider(BaseHTTPRequestHandler):
    def log_message(self, *_): pass
    def do_POST(self):
        body=json.loads(self.rfile.read(int(self.headers['Content-Length'])))
        CALLS.append((self.path,body))
        if self.path.endswith('/messages'):
            assert self.headers['x-api-key']=='search-fixture'
            assert body['tools'][0]['type']=='web_search_20250305'
            response={'content':[
                {'type':'web_search_tool_result','content':[{'type':'web_search_result','url':'https://example.com/official','title':'Official fixture'}]},
                {'type':'text','text':'not trusted as an answer','citations':[{'url':'https://example.com/official','cited_text':'Fixture is a timetable app.'}]}]}
            self.send_response(200);self.send_header('Content-Type','application/json');self.end_headers()
            self.wfile.write(json.dumps(response).encode());return
        messages=body['messages'];last=messages[-1]
        tools=[m for m in messages if m['role']=='tool']
        if tools:last=tools[-1]
        if not tools:
            name,args='web_search',{'queries':['Fixture official timetable']}
        elif len(tools)==1:
            assert 'Fixture is a timetable app.' in last['content'], last['content']
            name,args='web_fetch',{'url':'http://127.0.0.1:5555/private'}
        elif len(tools)==2:
            assert 'non-public' in last['content']
            name,args='web_fetch',{'url':'https://user:secret@example.com/'}
        elif len(tools)<6:
            name,args='web_search',{'queries':['Fixture official timetable']}
        else:
            assert 'Search budget exhausted' in last['content']
            name,args=None,None
        if name:
            delta={'role':'assistant','tool_calls':[{'index':0,'id':'call_'+str(len(tools)), 'type':'function','function':{'name':name,'arguments':json.dumps(args)}}]};finish='tool_calls'
        else:
            delta={'role':'assistant','content':'Native web verified.'};finish='stop'
        self.send_response(200);self.send_header('Content-Type','text/event-stream');self.end_headers()
        for choice in [{'delta':delta,'finish_reason':None},{'delta':{},'finish_reason':finish}]:
            self.wfile.write(('data: '+json.dumps({'id':'fixture','object':'chat.completion.chunk','model':'fixture','choices':[{'index':0,**choice}]})+'\n\n').encode())
        self.wfile.write(b'data: [DONE]\n\n')

async def main():
    with patch.dict('os.environ',{},clear=True):
        assert not web_environment('https://gateway.example/v1','gateway-key')
        env=web_environment('https://api.deepseek.com','official-key')
        assert env['CSBOT_SEARCH_API_KEY']=='official-key'
        with patch.dict('os.environ',{'CS_AI_SEARCH_URL':'https://other.example/anthropic/v1'}):
            try:web_environment('https://api.deepseek.com','official-key')
            except ValueError:pass
            else:raise AssertionError('key forwarded to other origin')
    server=ThreadingHTTPServer(('127.0.0.1',0),Provider)
    threading.Thread(target=server.serve_forever,daemon=True).start()
    async def deny(_):raise ValueError('No business DB')
    try:
        with tempfile.TemporaryDirectory() as tmp, patch('ai_runtime.runner.web_environment',return_value={
            'CSBOT_WEB_ENABLED':'1','CSBOT_SEARCH_URL':f'http://127.0.0.1:{server.server_port}/anthropic/v1',
            'CSBOT_SEARCH_API_KEY':'search-fixture','CSBOT_SEARCH_MODEL':'fixture'}):
            result=await run_dsh(scope='web-fixture',text='Check web.',context='',model='fixture',
                endpoint=f'http://127.0.0.1:{server.server_port}',api_key='chat-fixture',
                dispatch=deny,remember=False,state_root=Path(tmp))
            assert result['messages'][0]['content']=='Native web verified.'
            chats=[b for p,b in CALLS if not p.endswith('/messages')]
            assert {'web_search','web_fetch'} <= {t['function']['name'] for t in chats[0]['tools']}
            searches=[b for p,b in CALLS if p.endswith('/messages')]
            assert len(searches)==3
            assert 'search-fixture' not in json.dumps(chats)
            assert 'search-fixture' not in next(Path(tmp).rglob('profile.json')).read_text()
            calls=[e['data']['name'] for e in result['tools'] if e['type']=='tool/call']
            assert calls.count('web_search')==4 and calls.count('web_fetch')==2
            print('PASS: native tool schema, structured source/snippet, credential isolation, private/credential URL rejection, search budget and trace')
    finally:server.shutdown();server.server_close()

if __name__=='__main__':asyncio.run(main())
