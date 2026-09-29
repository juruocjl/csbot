"""Real-model acceptance: fetch a synthetic group avatar through the broker.

Read CS_AI_URL and CS_AI_API_KEY as JSON on stdin. Run on a Linux host with
the normal isolated script image and Docker access; no business DB or QQ API.
"""
import asyncio
import json
import os
from pathlib import Path
import sys
import tempfile

from PIL import Image, ImageDraw

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ai_runtime.group import CALLS
from ai_runtime.runner import run_dsh


async def main():
    config = json.load(sys.stdin)
    os.environ["CS_AI_WEB_ENABLE"] = "0"
    uid = "3345039979"
    calls = []
    with tempfile.TemporaryDirectory(prefix="csbot-avatar-route-") as directory:
        avatar = Path(directory) / "synthetic-avatar.png"
        picture = Image.new("RGB", (256, 256), "white")
        draw = ImageDraw.Draw(picture)
        draw.ellipse((36, 36, 220, 220), fill="#1378c9")
        picture.save(avatar)

        async def dispatch(request):
            method = request.get("method")
            if method == "catalog":
                return {"group_calls": CALLS}
            if method != "call":
                raise ValueError("Only synthetic group calls are available")
            name = request.get("name")
            calls.append(name)
            params = request.get("params") or {}
            if name == "group_members":
                return {"rows": [{"uid": uid, "nickname": "fixture", "card": "fixture"}], "total": 1, "truncated": False}
            if name == "member_info" and params.get("uid") == uid:
                return {"member": {"uid": uid, "nickname": "fixture", "card": "fixture",
                                   "avatar_url": f"https://q1.qlogo.cn/g?b=qq&nk={uid}&s=640"}}
            if name == "member_avatar" and params.get("uid") == uid:
                return {"uid": uid, "image_id": "synthetic-avatar", "full_status": "available",
                        "full_path": str(avatar), "thumbnail_path": str(avatar),
                        "source": "synthetic acceptance fixture"}
            raise ValueError("Unavailable synthetic group call")

        result = await run_dsh(
            scope="synthetic-group-avatar", state_root=Path(directory),
            text=f"本群 QQ {uid} 的头像里画着什么？请实际查看图像；看不出具体人物就直说。",
            context="这是隔离验收用的合成QQ群；头像数据由只读模拟接口提供。",
            model="deepseek-flash", endpoint=config["CS_AI_URL"], api_key=config["CS_AI_API_KEY"],
            dispatch=dispatch, remember=False, vision=True, thinking=True,
        )
        tool_events = result.get("tools") or []
        names = [e["data"]["name"] for e in tool_events if e["type"] == "tool/call"]
        image_results = [block for e in tool_events if e["type"] == "tool/result"
                         for block in e["data"]["message"]["content"]
                         if block.get("type") == "tool-result" and block.get("toolCallId") in {
                             c["data"]["callId"] for c in tool_events
                             if c["type"] == "tool/call" and c["data"]["name"] == "read_image"}]
        answer = "\n".join(str(m.get("content", "")) for m in result.get("messages", []))
        print(json.dumps({"reason": result.get("reason"), "calls": calls, "tools": names,
                          "image_result_errors": [r.get("isError") for r in image_results],
                          "answer": answer}, ensure_ascii=False))
        assert result.get("reason", {}).get("kind") == "completed"
        assert "member_avatar" in calls, "model did not use the avatar broker"
        assert "read_image" in names and image_results and all(not r.get("isError") for r in image_results), \
            "model did not successfully read the authorized avatar path"


if __name__ == "__main__":
    asyncio.run(main())
