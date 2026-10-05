"""Exercise the actual QQ reply function with fake generation; no QQ/DB calls."""
import ast
import asyncio
from io import BytesIO
import os
import re
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import uuid
from unittest.mock import patch,AsyncMock
import contextvars
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from nonebot.adapters.onebot.v11 import Message,MessageSegment
from PIL import Image
from ai_runtime.image_archive import image_segment

async def main():
    with tempfile.TemporaryDirectory() as tmp:
        os.environ['CS_AI_STATE_DIR']=str(Path(tmp)/'state')
        os.environ['CS_IMAGE_HISTORY_DIR']=str(Path(tmp)/'public')
        from ai_runtime.media import media_cache
        from ai_runtime.store import state_store
        source=Path(__file__).resolve().parents[1]/'plugins/cs_ai/__init__.py'
        functions=[n for n in ast.parse(source.read_text()).body if isinstance(n,(ast.AsyncFunctionDef,ast.FunctionDef)) and n.name in ('ai_ask2','_ai_chat_link')]
        module=ast.Module(body=functions,type_ignores=[])
        raw=BytesIO();Image.new('RGB',(8,8),'blue').save(raw,format='PNG')
        cache=media_cache();digest=cache.put(raw.getvalue(),private=True)
        assert not list(cache.full.glob('*.png')) and not list(cache.small.glob('*.png'))
        key=uuid.uuid4().hex;store=state_store()
        store.execute('INSERT INTO run_images VALUES(?,?,?,?)',('image',key,digest,'chart'))
        async def ask(*args,**kwargs):return '图在这。'
        namespace=dict(uuid=uuid,Message=Message,MessageSegment=MessageSegment,image_segment=image_segment,Bot=object,
            group_id_from_sid=lambda x:'g',_format_ai_message=lambda m,ids,names:str(m),re=re,config=SimpleNamespace(cs_ai_engine='dsh',cs_domain='https://fixture.invalid'),__package__='plugins.cs_ai',
            logger=SimpleNamespace(info=lambda *a:None,error=lambda *a:None),ai_ask_main=ask,_should_forward_ai_result=lambda s:False,_render_at_segments=Message)
        exec(compile(module,str(source),'exec'),namespace)
        reply=await namespace['ai_ask2'](None,'123','group_g_123',None,Message('画图'),Message('画图'),chat_id=key)
        assert any(s.type=='image' and s.data['file'].startswith('base64://') for s in reply)
        assert '图在这' in str(reply)
        # Both namespaces share the original budget; thumbs are permanent.
        cache.budget=1;cache.prune()
        assert cache.info(digest)['full_status']=='evicted'
        assert Path(cache.info(digest)['thumbnail_path']).is_file()
        reply=await namespace['ai_ask2'](None,'123','group_g_123',None,Message('画图'),Message('画图'),chat_id=key)
        assert not any(s.type=='image' for s in reply) and '已淘汰' in str(reply)
        print('PASS: actual QQ reply attaches generated PNG bytes; private originals never enter public paths; shared LRU eviction keeps thumbnail and is explicit')
        store.register_run(key,'scope','g','123','qq','original')
        store.execute("UPDATE runs SET status='running' WHERE id=?",(key,))
        notifications=[]
        async def notify(message):
            # The URL must reference an already registered request.
            linked=str(message).split('chatId=')[-1]
            assert store.resolve_run_id(linked,'g','123')
            notifications.append(str(message))
        module=SimpleNamespace(current_run=contextvars.ContextVar('fixture_run'))
        with patch.dict(sys.modules,{'plugins.allmsg.outgoing':module}),patch('ai_runtime.runner.steer_run',AsyncMock(return_value=True)):
            response=await namespace['ai_ask2'](None,'123','group_g_123',None,Message('补充：还有这个'),Message('补充：还有这个'),chat_id='unused-id',notify=notify)
            assert f'chatId={store.short_run_id(key)}' in str(response) and not notifications and not store.get_run('unused-id')
        with patch('ai_runtime.runner.steer_run',AsyncMock(return_value=False)):
            await namespace['ai_ask2'](None,'123','group_g_123',None,Message('补充：错过了'),Message('补充：错过了'),chat_id='fallback-id',notify=notify)
            assert len(notifications)==1 and f'chatId={store.short_run_id("fallback-id")}' in notifications[0]
        print('PASS: inserted QQ supplement links existing run without phantom notification; late supplement registers before notifying')
        namespace['ai_ask_main']=AsyncMock(side_effect=RuntimeError('fixture model config failure'))
        await namespace['ai_ask2'](None,'123','group_g_123',None,Message('test'),Message('test'),chat_id='failed-id',notify=notify)
        assert store.get_run('failed-id')['status']=='interrupted'
        print('PASS: failure before model startup terminates the registered QQ trace')
        store.close();cache.db.close()
if __name__=='__main__':asyncio.run(main())
