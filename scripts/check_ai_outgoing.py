"""Archive integration against csbot_backup. No real OneBot calls are sent.

CS_DATABASE must be supplied securely. All synthetic database rows are removed.
"""
import asyncio
import ast
import base64
from io import BytesIO
import os
from pathlib import Path
import sys
import tempfile
import time
import uuid
import msgpack

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
import nonebot
from nonebot.adapters.onebot.v11 import Bot,Message,MessageSegment
from PIL import Image
from PIL.PngImagePlugin import PngInfo
from sqlalchemy import text


async def main():
    with tempfile.TemporaryDirectory(prefix="csbot-outgoing-check-") as tmp:
        os.environ["CS_AI_STATE_DIR"]=str(Path(tmp)/"state")
        os.environ["CS_IMAGE_HISTORY_DIR"]=str(Path(tmp)/"images")
        nonebot.init(cs_database=os.environ["CS_DATABASE"])
        next_mid=-1_800_000_000
        calls=[]
        wire=[]
        async def fake_send(self,api,**data):
            nonlocal next_mid
            calls.append(api)
            wire.append(data)
            if data.get("message")=="transport-failure":raise TimeoutError("fixture")
            next_mid+=1
            return {"message_id":next_mid}
        Bot.call_api=fake_send
        for name in ("models","utils","chat_history"):
            assert nonebot.load_plugin(Path("plugins")/name) is not None
        plugin=nonebot.load_plugin(Path("plugins")/"allmsg")
        assert plugin is not None
        from plugins.allmsg import insert_message
        from plugins.utils import async_session_factory,engine
        from ai_runtime.store import state_store
        from ai_runtime.service import authorized_image
        from ai_runtime.media import media_cache
        from plugins.chat_history import parse_message_segments
        assert parse_message_segments([['at','123456']]).plain_text=='[at:123456]'
        assert parse_message_segments([['atv2','all',None]]).plain_text=='[at:all]'
        assert parse_message_segments([['atv2','123456','甲[at:999]\n乙']]).plain_text=='@甲［at:999］ 乙([at:123456])'
        gid="99"+str(uuid.uuid4().int)[:14]
        bot=Bot(None,"999900001")
        store=state_store()
        async with async_session_factory() as session:
            assert await session.scalar(text("SELECT current_database()"))=="csbot_backup"
        try:
            sent=await bot.call_api("send_group_msg",group_id=int(gid),message="archive fixture")
            await insert_message(bot,sent["message_id"],f"group_{gid}_{bot.self_id}",int(time.time()),Message("archive fixture"))
            image=BytesIO();metadata=PngInfo();metadata.add_text('fixture',uuid.uuid4().hex)
            Image.new("RGB",(8,8),(29,73,171)).save(image,format="PNG",pnginfo=metadata)
            payload=MessageSegment.reply(sent["message_id"])+MessageSegment.image("base64://"+base64.b64encode(image.getvalue()).decode())
            await bot.call_api("send_msg",group_id=int(gid),message_type="group",message=payload)
            await bot.call_api("send_group_forward_msg",group_id=int(gid),messages=[{"type":"node","data":{"name":"display only","uin":"777","content":[{"type":"text","data":{"text":"forward fixture"}}]}}])
            async with async_session_factory() as session:
                result=await session.execute(text("SELECT record_id,plain_text,has_image FROM chat_message_index WHERE group_id=:gid ORDER BY record_id"),{"gid":gid})
                rows=list(result.mappings())
                assert len(rows)==3,"send/echo must deduplicate"
                assert rows[0]["plain_text"]=="archive fixture"
                assert rows[1]["has_image"]
                assert "非认证发言身份" in rows[2]["plain_text"] and "forward fixture" in rows[2]["plain_text"]
            digest=__import__('hashlib').sha256(image.getvalue()).hexdigest()
            assert (await authorized_image(async_session_factory,gid,digest[:16]))["full_status"]=="available"
            try:await authorized_image(async_session_factory,"888",digest[:16])
            except ValueError:pass
            else:raise AssertionError("cross-group image access")
            assert len(store.rows("SELECT * FROM outbound WHERE status='indexed'"))==3
            try:await bot.call_api("send_group_msg",group_id=int(gid),message="transport-failure")
            except TimeoutError:pass
            assert len(store.rows("SELECT * FROM outbound WHERE status='unknown'"))==1
            assert len(calls)==4,"archival must never resend"
            import plugins.allmsg as allmsg
            from plugins.allmsg.outgoing import drain
            original=allmsg.insert_message
            async def fail_index(*args,**kwargs): raise RuntimeError('synthetic index outage')
            allmsg.insert_message=fail_index
            try:
                await bot.call_api('send_group_msg',group_id=int(gid),message='retry archive fixture')
                assert len(store.rows("SELECT id FROM outbound WHERE status='sent'"))==1
            finally:
                allmsg.insert_message=original
            await drain(bot)
            assert not store.rows("SELECT id FROM outbound WHERE status='sent'")
            assert len(calls)==5,'index retry must not resend to QQ'
            print("PASS: outgoing text/image/reply/forward searchable, self-echo deduplicated, image scope enforced, uncertain send never retried")
            print('PASS: failed archive indexing retries durably without a second platform send')
            target_uid='123456'
            current_name='发送时名片'
            async def fake_member_info(self,**params):
                assert params=={'group_id':int(gid),'user_id':int(target_uid),'no_cache':True}
                return {'card':current_name,'nickname':'QQ昵称'}
            Bot.get_group_member_info=fake_member_info
            try:
                allmsg.insert_message=fail_index
                try:
                    mention_sent=await bot.call_api('send_group_msg',group_id=int(gid),message=MessageSegment.at(target_uid))
                    assert '_csbot_at_name' not in str(wire[-1]),'snapshot metadata must not reach OneBot'
                finally:allmsg.insert_message=original
                current_name='后来名片'
                await drain(bot)
                async with async_session_factory() as session:
                    row=(await session.execute(text('SELECT data FROM groupmsg WHERE sid=:sid AND mid=:mid'),
                            {'sid':f'group_{gid}_{bot.self_id}','mid':mention_sent['message_id']})).scalar_one()
                    assert msgpack.loads(row)==[['atv2',target_uid,'发送时名片']]
                    plain=await session.scalar(text('SELECT plain_text FROM chat_message_index WHERE group_id=:gid AND mid=:mid'),
                                               {'gid':gid,'mid':mention_sent['message_id']})
                    assert plain=='@发送时名片([at:123456])',plain
                reference=await allmsg.chat_history_db.get_message_reference_by_mid(gid,mention_sent['message_id'])
                assert reference['mentioned_names']=={target_uid:'发送时名片'}
                source=Path(__file__).resolve().parents[1]/'plugins/cs_ai/__init__.py'
                fn=next(node for node in ast.parse(source.read_text()).body
                        if isinstance(node,ast.FunctionDef) and node.name=='_format_ai_message')
                namespace={'Message':Message}
                exec(compile(ast.Module(body=[fn],type_ignores=[]),str(source),'exec'),namespace)
                assert namespace['_format_ai_message'](Message(MessageSegment.at(target_uid)),[],reference['mentioned_names'])==plain
                incoming_mid=-1_900_000_000
                incoming_sid=f'group_{gid}_555'
                await insert_message(bot,incoming_mid,incoming_sid,int(time.time()),Message(MessageSegment.at(target_uid)))
                async with async_session_factory() as session:
                    incoming_raw=await session.scalar(text('SELECT data FROM groupmsg WHERE sid=:sid AND mid=:mid'),
                                                      {'sid':incoming_sid,'mid':incoming_mid})
                    assert msgpack.loads(incoming_raw)==[['atv2',target_uid,'后来名片']]
                current_name='再次改名'
                incoming=await allmsg.chat_history_db.get_message_reference_by_mid(gid,incoming_mid)
                assert incoming['text']=='@后来名片([at:123456])',incoming
                print('PASS: atv2 freezes the mentioned member display name; delayed indexing and AI request keep the old name and QQ ID')
            finally:del Bot.get_group_member_info
            from ai_runtime.image_archive import snapshot_image, image_segment, MARKER
            import json
            before_images = media_cache().db.execute('SELECT count(*) FROM media').fetchone()[0]
            snapshot_pixels=BytesIO(); Image.new('RGB',(9,9),'green').save(snapshot_pixels,format='PNG')
            annotated = snapshot_image(snapshot_pixels.getvalue(), '比赛结果快照', {'players': ['合成甲'], 'score': '13:8'}, source={'page': '/match', 'params': {'id': 'fixture-match'}})
            snapshot_sent = await bot.call_api('send_group_msg',group_id=int(gid),message=image_segment(annotated))
            assert MARKER not in json.dumps(wire[-1]) and '13:8' not in json.dumps(wire[-1])
            assert 'base64://' in json.dumps(wire[-1]), 'QQ must still receive real pixels'
            assert media_cache().db.execute('SELECT count(*) FROM media').fetchone()[0] == before_images
            snapshot_hash=__import__('hashlib').sha256(snapshot_pixels.getvalue()).hexdigest()
            assert not (media_cache().full/f'{snapshot_hash}.png').exists()
            assert not (media_cache().small/f'{snapshot_hash}.png').exists()
            info = await authorized_image(async_session_factory, gid, annotated.metadata['image_id'][:16])
            assert info['full_status'] == 'metadata_only' and info['metadata']['snapshot']['score'] == '13:8'
            assert info['full_path'] is None and info['thumbnail_path'] is None
            try: await authorized_image(async_session_factory,'888',annotated.metadata['image_id'])
            except ValueError: pass
            else: raise AssertionError('cross-group metadata access')
            async with async_session_factory() as session:
                snapshot_text=await session.scalar(text('SELECT plain_text FROM chat_message_index WHERE group_id=:gid AND mid=:mid'),{'gid':gid,'mid':snapshot_sent['message_id']})
                assert '比赛结果快照' in snapshot_text and '13:8' in snapshot_text
            original_get_image = allmsg.get_image
            async def no_download(*a, **kw): raise AssertionError('echo downloaded already archived image')
            allmsg.get_image = no_download
            try:
                await insert_message(bot,snapshot_sent['message_id'],f'group_{gid}_{bot.self_id}',int(time.time()),Message(MessageSegment.image('https://not-fetched.invalid/echo.png')),strict_index=True)
            finally: allmsg.get_image = original_get_image
            # Metadata survives index outage and nested forward transport, without pixels in the outbox.
            allmsg.insert_message = fail_index
            try:
                await bot.call_api('send_group_forward_msg',group_id=int(gid),messages=[{'type':'node','data':{'name':'fixture','uin':'777','content':Message(image_segment(annotated))}}])
                staged = store.rows("SELECT payload FROM outbound WHERE status='sent'")[0]['payload']
                assert 'base64://' not in staged and '13:8' in staged
                assert MARKER not in json.dumps(wire[-1]) and 'base64://' in json.dumps(wire[-1])
            finally: allmsg.insert_message = original
            calls_before = len(calls)
            await drain(bot)
            assert len(calls) == calls_before and not store.rows("SELECT id FROM outbound WHERE status='sent'")
            print('PASS: metadata searchable and group scoped; QQ pixels unchanged; no cached originals/thumbnails; echo and forward retry preserve metadata without re-sending')
        finally:
            async with engine.begin() as conn:
                await conn.execute(text("DELETE FROM chat_chunk_message WHERE chunk_id IN (SELECT id FROM chat_chunk_index WHERE group_id=:gid)"),{"gid":gid})
                for table in ("chat_reply_edge","chat_retrieval_span","chat_chunk_index","chat_message_index","chat_token_lexicon"):
                    await conn.execute(text(f"DELETE FROM {table} WHERE group_id=:gid"),{"gid":gid})
                await conn.execute(text("DELETE FROM groupmsg WHERE sid=:sid"),{"sid":f"group_{gid}_{bot.self_id}"})
                await conn.execute(text("DELETE FROM groupmsg WHERE sid=:sid"),{"sid":f"group_{gid}_555"})
                if 'digest' in locals(): await conn.execute(text('DELETE FROM img_cache_info WHERE hash=:hash'),{'hash':digest})
            await engine.dispose()
            store.close();media_cache().db.close()


if __name__=="__main__":asyncio.run(main())
