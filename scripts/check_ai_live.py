"""Linux end-to-end acceptance. JSON test config on stdin; never use production DB.

Requires pinned DSH/Mneme, Docker image, CS_AI_GAME_DB and service dependencies.
The model receives synthetic instructions and an aggregate count only.
"""
import asyncio
import json
import os
from pathlib import Path
import sys
import tempfile

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine,async_sessionmaker
from ai_runtime.data import DataBroker
from ai_runtime.runner import run_dsh,scope_key


async def main():
    config=json.load(sys.stdin)
    engine=create_async_engine(config['CS_DATABASE'])
    try:
        async with engine.connect() as connection:
            if await connection.scalar(text('SELECT current_database()'))!='csbot_backup':
                raise ValueError('acceptance must use csbot_backup')
        broker=DataBroker(async_sessionmaker(engine),str(config['CS_TEST_GROUP']),Path(os.environ['CS_AI_GAME_DB']))
        calls=[]
        async def dispatch(request):
            calls.append(request.get('method'))
            return await broker.dispatch(request)
        with tempfile.TemporaryDirectory(prefix='csbot-live-check-') as tmp:
            result=await run_dsh(scope=scope_key('web','0','0','synthetic'),
                text='这是技术验收。请必须使用 execute_python 运行脚本：import csdata；用 csdata.query("SELECT count(*) AS n FROM members") 查询主库绑定数量；再用 csdata.query("SELECT count(*) AS n FROM game_history",source="game") 查询游戏历史数量；最后 print(sum(range(1,101)))。不要读取或打印成员身份。最终回答必须包含计算结果。',
                context='只验证统计数量和计算，不需保存记忆。',
                model=config['CS_AI_MODEL'],endpoint=config['CS_AI_URL'],api_key=config['CS_AI_API_KEY'],
                dispatch=dispatch,remember=False,state_root=Path(tmp))
            assert calls.count('query')>=2,'model did not exercise both data sources'
            assert '5050' in ''.join(row['content'] for row in result['messages'])
            assert all(not event['data'].get('isError',False) for event in result['tools'])
            print('PASS: real model -> DSH -> isolated Python -> scoped main/game SQL -> final calculation')
    finally:
        await engine.dispose()


if __name__=='__main__':asyncio.run(main())
