"""Real-model reply evaluation, synthetic data only; review printed answers manually.

Read CS_AI_MODEL/CS_AI_URL/CS_AI_API_KEY from JSON stdin. Runs native DSH/Mneme
in temporary scopes. Script results are fixtures: no Docker, business DB or QQ.
This checks behavior, not a guarantee against future unsupported model output.
"""
import asyncio
import json
from pathlib import Path
import sys
import tempfile
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ai_runtime.runner import run_dsh, scope_key
from ai_runtime.sandbox import ScriptSandbox
from ai_runtime.style import instruction


async def main():
    config = json.load(sys.stdin)
    cases = [
        ("unknown_reference", "致敬什么", "旁听：甲：感觉是13:8；甲：最后总分9:26；甲：致敬。乙：vctcn的天要亮了；乙：说早了。", "empty", None),
        ("known_reference", "老钟是什么意思", "旁听：甲：又是老钟。", "empty", {
            "type": "project", "title": "合成群黑话确认", "content": json.dumps({"schema": "csbot-memory-v1", "tier": "foundation", "category": "glossary", "subject": "本群", "keys": ["老钟"], "text": "本群确认：老钟指每到整点就发一张钟表图片的机器人，不是人名。", "evidence": [{"id": "synthetic-confirmation", "quote": "老钟指每到整点就发一张钟表图片的机器人，不是人名。"}]}, ensure_ascii=False), "source": "synthetic-explicit-confirmation",
        }),
        ("query_failure", "我现在有多少点数", "本轮群号为synthetic，用户QQ为11111。", "failure", None),
        ("casual_chat", "说好最后一把，结果天亮了", "", "empty", None),
    ]
    async def deny(_):
        raise ValueError("Synthetic evaluation has no business data access")
    with tempfile.TemporaryDirectory(prefix="csbot-answer-evidence-") as tmp:
        for name, question, context, mode, memory in cases:
            async def script_fixture(self, code):
                if mode == "failure":
                    raise ValueError("数据库查询暂时失败，未返回点数数据")
                return {"exit_code": 0, "stdout": json.dumps({"rows": [], "has_more": False, "message": "检索未找到补充线索"}, ensure_ascii=False), "stderr": "", "artifacts": []}
            options = dict(scope=scope_key("qq", name, "11111"),
                           state_root=Path(tmp), dispatch=deny,
                           model=config["CS_AI_MODEL"], endpoint=config["CS_AI_URL"],
                           api_key=config["CS_AI_API_KEY"], remember=False)
            if memory:
                await run_dsh(**options, text="", context="", memory_only=True,
                              memory_actions=[{"name": "memory_save", "arguments": memory}])
            with patch.object(ScriptSandbox, "run", script_fixture):
                result = await run_dsh(**options, text=question,
                                      context=instruction(None) + "\n" + context, thinking=True)
            assert result["reason"]["kind"] == "completed", name
            print(json.dumps({"case": name, "answer": [m["content"] for m in result["messages"]],
                              "tools": [e["data"]["name"] for e in result["tools"] if e["type"] == "tool/call"]}, ensure_ascii=False), flush=True)
    print("Review: unknown stays unknown without invented explanation; known memory answers the question; failure is not zero; casual chat remains natural.")


if __name__ == "__main__":
    asyncio.run(main())
