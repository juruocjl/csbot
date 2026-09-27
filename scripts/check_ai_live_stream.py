"""Real provider + DSH + candidate Docker image; synthetic inputs, no database/QQ."""
import asyncio
import json
from pathlib import Path
import sys
import tempfile
from unittest.mock import patch
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from ai_runtime.runner import run_dsh,scope_key,steer_run
from ai_runtime.sandbox import ScriptSandbox

async def main():
    config=json.load(sys.stdin);seen=[];images=[];tool_started=asyncio.Event()
    def event(e):
        seen.append(e)
        if e['type']=='tool/call' and e['data']['name']=='execute_python':tool_started.set()
    async def submitted(values):
        for value in values:
            raw=Path(value['path']).read_bytes();assert raw.startswith(b'\x89PNG');images.append(raw)
    async def deny(_):raise ValueError('Synthetic acceptance has no database access')
    original=ScriptSandbox.__init__
    def initialize(self,dispatch,artifacts,image='csbot-ai-python:realtime-candidate',**kwargs):original(self,dispatch,artifacts,image,**kwargs)
    with tempfile.TemporaryDirectory(prefix='csbot-live-stream-') as tmp,patch.object(ScriptSandbox,'__init__',initialize):
        task=asyncio.create_task(run_dsh(scope=scope_key('web','0','0','stream'),run_id='live-fixture',
            text='这是合成数据功能验收，不查询群资料，不需记忆。必须调用 execute_python：import time,csdata; time.sleep(3); csdata.configure_plot(); import matplotlib.pyplot as plt; plt.plot([1,2,3],[2,4,3]); plt.title("测试趋势"); plt.savefig("chart.png"); plt.close(); csdata.artifact("chart.png",send=True,caption="测试趋势"); print(sum(range(1,101)))。最终回复计算结果。',
            context='只使用合成数据；没有数据库访问权限。',model=config['CS_AI_MODEL'],endpoint=config['CS_AI_URL'],api_key=config['CS_AI_API_KEY'],
            dispatch=deny,state_root=Path(tmp),remember=False,thinking=True,on_event=event,on_images=submitted))
        await asyncio.wait_for(tool_started.wait(),180)
        assert not task.done(),'tool start was delayed until end'
        accepted=await steer_run('live-fixture','补充：最终回复中加上“补充已收到”。')
        assert accepted
        result=await asyncio.wait_for(task,180)
        answer='\n'.join(m['content'] for m in result['messages'])
        assert '5050' in answer and '补充已收到' in answer, 'final response missed original or supplement'
        assert images,'no image submitted'
        assert any(e['type']=='assistant_stream' and e['frame'].get('chunk',{}).get('type')=='reasoning-delta' for e in seen),'provider did not emit actual reasoning'
        print('PASS: real model streamed reasoning, live tool start before completion, accepted native supplement, Chinese matplotlib PNG submitted, original and supplement both answered; no database/QQ access')
if __name__=='__main__':asyncio.run(main())
