"""Real DSH/model/public web evaluation in disposable scopes; no business DB/QQ.

Read model credentials from JSON stdin. --models compares explicit model IDs.
Answers and saved memories are printed for human review, not keyword grading.
"""
import argparse
import asyncio
import json
from pathlib import Path
import sqlite3
import sys
import tempfile
from unittest.mock import patch
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from ai_runtime.runner import run_dsh,scope_key
from ai_runtime.sandbox import ScriptSandbox
from ai_runtime.style import instruction

CASES={
 'quoted_visit': 'QQ用户11111：请回应我引用的消息：[引用，原作者QQ22222] 转眼13天假期已经过去了3天 接下来的假期我会前往杭州旅游 计划是在浙江大学参观3天，深入了解这所浙江百年名校。受到zdbk和celechron邀请，也是有幸旁听一些课程，希望能愉快度过',
 'new_metaphor': 'QQ用户11111：周末受Jira和Jenkins盛情邀请，来公司两日游，主打一个沉浸式体验。',
 'literal_plan': 'QQ用户11111：我确实已经订好10月3日去杭州旅游的车票了，不是玩梗，也不去上课。请帮我记住这个出发日期。',
 'unknown': 'QQ用户11111：他说“南门蓝饼又到了”，这到底指什么？',
}
async def main():
 args=argparse.ArgumentParser();args.add_argument('--seed-public-glossary',action='store_true');args.add_argument('--models',nargs='*');args.add_argument('--cases',nargs='*',choices=CASES);opts=args.parse_args()
 c=json.load(sys.stdin)
 async def deny(_):raise ValueError('Evaluation has no business data access')
 async def fixture(self,code):
  try:compile(code,'<agent-script>','exec')
  except SyntaxError as e:return {'exit_code':1,'output':f'SyntaxError: {e.msg}', 'artifacts':[]}
  return {'exit_code':0,'output':json.dumps({'rows':[],'message':'合成群检索没有补充线索'},ensure_ascii=False),'truncated':False,'artifacts':[]}
 with tempfile.TemporaryDirectory(prefix='csbot-pragmatics-') as tmp:
  for model in opts.models or [c['CS_AI_MODEL']]:
   for name in opts.cases or list(CASES):
    scope=scope_key('qq',model+name,'11111')
    if opts.seed_public_glossary:
     actions=[]
     for term,meaning in [('zdbk','zdbk是浙江大学本科教学管理信息服务平台，学生用于选课等本科教学管理。'),('Celechron','Celechron是服务于浙大学生的时间管理工具，提供课表、日程、DDL和成绩查询。')]:
      data={'schema':'csbot-memory-v1','tier':'foundation','category':'glossary','subject':'公开术语','keys':[term,term.lower()],'text':meaning,'evidence':[],'supersedes':[]}
      actions.append({'name':'memory_save','arguments':{'title':'已核实公开术语 '+term,'type':'project','content':json.dumps(data,ensure_ascii=False),'source':'public-source-fixture'}})
     await run_dsh(scope=scope,state_root=Path(tmp),text='',context='',model=model,endpoint=c['CS_AI_URL'],api_key=c['CS_AI_API_KEY'],dispatch=deny,remember=False,memory_only=True,memory_actions=actions)
    with patch.object(ScriptSandbox,'run',fixture):
     result=await run_dsh(scope=scope,state_root=Path(tmp),text=CASES[name],context=instruction(None),
      model=model,endpoint=c['CS_AI_URL'],api_key=c['CS_AI_API_KEY'],dispatch=deny,remember=True,thinking=True)
    assert result['reason']['kind']=='completed'
    memories=[]
    for path in (Path(tmp)/'scopes'/scope/'memory').rglob('*.db'):
     with sqlite3.connect(path) as db:
      memories.extend([{'title':r[0],'content':r[1]} for r in db.execute('SELECT title,content FROM memories WHERE forgotten=0')])
    print(json.dumps({'model':model,'case':name,'answer':[m['content'] for m in result['messages']],
      'tools':[{'name':e['data']['name'],'arguments':e['data'].get('arguments')} for e in result['tools'] if e['type']=='tool/call'],
      'web_results':[e['data'] for e in result['tools'] if e['type']=='tool/result' and any('External web' in str(b) or 'WEB_' in str(b) for b in e['data'].get('message',{}).get('content',[]))], 'memories':memories},ensure_ascii=False),flush=True)
if __name__=='__main__':asyncio.run(main())
