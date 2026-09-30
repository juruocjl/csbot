"""Real-model acceptance for a JPEG original archived with a legacy .png name.

Reads existing model test configuration on stdin. Uses only synthetic image data
and a broker fixture; no business database, QQ API, or production messages.
"""
import asyncio
from io import BytesIO
import json
import os
from pathlib import Path
import sys
import tempfile

from PIL import Image, ImageDraw

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from ai_runtime.runner import run_dsh


async def main():
    config=json.load(sys.stdin)
    os.environ["CS_AI_WEB_ENABLE"]="0"
    with tempfile.TemporaryDirectory(prefix="csbot-image-route-") as directory:
        root=Path(directory)
        picture=Image.new("RGB",(1170,2532),"white")
        draw=ImageDraw.Draw(picture)
        draw.rectangle((120,400,1050,1700),fill="#1378c9")
        raw=BytesIO()
        picture.save(raw,format="JPEG",quality=88)
        archived=root/"synthetic-original.png"
        archived.write_bytes(raw.getvalue())
        thumb=picture.copy()
        thumb.thumbnail((256,256))
        thumb_path=root/"synthetic-thumbnail.png"
        thumb.save(thumb_path,format="PNG")
        image_id="a"*64
        calls=[]

        async def dispatch(request):
            calls.append(request.get("method"))
            if request.get("method")=="image" and request.get("id")==image_id:
                return {"image_id":image_id,"full_status":"available",
                        "full_path":str(archived),"thumbnail_path":str(thumb_path),
                        "width":1170,"height":2532,"mime":"image/jpeg",
                        "size_bytes":len(raw.getvalue())}
            raise ValueError("Only the synthetic archived image is available")

        result=await run_dsh(
            scope="synthetic-image-route",state_root=root,
            text=f"请实际查看引用消息中的图片 [image:{image_id}]，简短说说主要颜色。",
            context="这是隔离验收的合成消息；请按图片ID查询实际图像。",
            model="deepseek-flash",endpoint=config["CS_AI_URL"],api_key=config["CS_AI_API_KEY"],
            dispatch=dispatch,remember=False,vision=True,thinking=True)
        events=result.get("tools") or []
        image_calls=[event for event in events if event.get("type")=="tool/call"
                     and event.get("data",{}).get("name")=="read_image"]
        image_ids={event["data"]["callId"] for event in image_calls}
        image_results=[block for event in events if event.get("type")=="tool/result"
                       for block in event.get("data",{}).get("message",{}).get("content",[])
                       if block.get("type")=="tool-result" and block.get("toolCallId") in image_ids]
        answer="\n".join(str(message.get("content","")) for message in result.get("messages",[]))
        print(json.dumps({"reason":result.get("reason"),"broker_calls":calls,
                          "image_call_count":len(image_calls),
                          "image_result_errors":[block.get("isError") for block in image_results],
                          "answer":answer},ensure_ascii=False))
        assert result.get("reason",{}).get("kind")=="completed"
        assert "image" in calls,"Model did not query the synthetic image ID"
        assert image_calls and image_results and all(not block.get("isError") for block in image_results), \
            "Model did not successfully read the original image"
        assert any(".jpg" in str(event.get("data",{}).get("arguments")) for event in image_calls), \
            "Model did not choose the format-correct original view"
        assert archived.read_bytes()==raw.getvalue(),"Archived original was modified"


if __name__=="__main__":asyncio.run(main())
