"""Archive integration against csbot_backup. No real OneBot calls are sent.

CS_DATABASE must be supplied securely. All synthetic database rows are removed.
"""
import asyncio
import base64
from io import BytesIO
import os
from pathlib import Path
import sys
import tempfile
import time
import uuid

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
        async def fake_send(self,api,**data):
            nonlocal next_mid
            calls.append(api)
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
        finally:
            async with engine.begin() as conn:
                await conn.execute(text("DELETE FROM chat_chunk_message WHERE chunk_id IN (SELECT id FROM chat_chunk_index WHERE group_id=:gid)"),{"gid":gid})
                for table in ("chat_reply_edge","chat_retrieval_span","chat_chunk_index","chat_message_index","chat_token_lexicon"):
                    await conn.execute(text(f"DELETE FROM {table} WHERE group_id=:gid"),{"gid":gid})
                await conn.execute(text("DELETE FROM groupmsg WHERE sid=:sid"),{"sid":f"group_{gid}_{bot.self_id}"})
                if 'digest' in locals(): await conn.execute(text('DELETE FROM img_cache_info WHERE hash=:hash'),{'hash':digest})
            await engine.dispose()
            store.close();media_cache().db.close()


if __name__=="__main__":asyncio.run(main())
