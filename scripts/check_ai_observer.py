"""Read-only deployed observer check: BASE_URL LOGIN_URL_FILE; prints no chat or tokens."""
import json,sys,uuid,http.cookiejar,urllib.request,urllib.parse,urllib.error
from pathlib import Path
if len(sys.argv)!=3:raise SystemExit('Usage: check_ai_observer.py BASE_URL LOGIN_URL_FILE')
base=sys.argv[1]
login=Path(sys.argv[2]).read_text().strip()
url=base+'/?'+urllib.parse.urlsplit(login).query
client=urllib.request.build_opener(urllib.request.ProxyHandler({}),urllib.request.HTTPCookieProcessor(http.cookiejar.CookieJar()))
with client.open(url,timeout=30) as r:assert r.status==200
def call(method,args):
 body={'type':'client-request','rpcId':str(uuid.uuid4()),'method':method,'payload':{'args':args}}
 request=urllib.request.Request(base+'/api/'+method,data=json.dumps(body).encode(),headers={'Content-Type':'application/json','Origin':base})
 with client.open(request,timeout=30) as r:return json.load(r)
result=call('session/list',{'_request':{}})['result']
assert result['ok'],result
items=result['value']['items'];assert items
print('session list passed, count',len(items))
result=call('session/page',{'request':{'address':{'kind':'session','sessionId':items[0]['sessionId']},'throughSeq':items[0]['projections']['asOfSeq'],'maxMessages':30}})['result']
assert result['ok'],result
records=result['value']['records'];assert records
print('history page passed, events',len(records))
assert any(r['event']['type']=='assistant/message' for r in records)
for method in ('session/prompt','session/create','session/rename','settings/update','credentials/set'):
 result=call(method,{})['result'];assert not result['ok'] and result['error']['code']=='observer/read-only',result
print('write operations denied before agent activation')
try:
 urllib.request.build_opener(urllib.request.ProxyHandler({})).open(base+'/api/session/list',timeout=10)
 raise AssertionError('unauthenticated request accepted')
except urllib.error.HTTPError as e:assert e.code==401,e.code
print('unauthenticated API denied')
