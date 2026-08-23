import importlib.util
from pathlib import Path
import unittest


MODULE_PATH = Path(__file__).parents[1] / "plugins" / "cs_ai" / "output_guard.py"
SPEC = importlib.util.spec_from_file_location("cs_ai_output_guard", MODULE_PATH)
assert SPEC and SPEC.loader
GUARD = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(GUARD)


class QQOutputGuardLogicTest(unittest.TestCase):
    def test_allow_preserves_original_draft(self):
        result = GUARD.parse_qq_guard_response(
            '{"decision":"allow","text":"被模型擅自修改","reason":"safe"}',
            "原始回答 [at:123456]",
        )
        self.assertEqual(result.decision, "allow")
        self.assertEqual(result.text, "原始回答 [at:123456]")

    def test_rewrite_uses_guarded_text(self):
        result = GUARD.parse_qq_guard_response(
            '{"decision":"rewrite","text":"已去除高风险推广和引流内容。","reason":"platform-risk-promotion"}',
            "草稿",
        )
        self.assertEqual(result.decision, "rewrite")
        self.assertEqual(result.text, "已去除高风险推广和引流内容。")

    def test_block_uses_fixed_message(self):
        result = GUARD.parse_qq_guard_response(
            '{"decision":"block","text":"不得泄露的内容","reason":"unsafe"}',
            "草稿",
        )
        self.assertEqual(result.decision, "block")
        self.assertEqual(result.text, GUARD.QQ_GUARD_BLOCKED_TEXT)

    def test_fenced_json_is_tolerated(self):
        result = GUARD.parse_qq_guard_response(
            '```json\n{"decision":"allow","text":"","reason":"safe"}\n```',
            "正常内容",
        )
        self.assertEqual(result.text, "正常内容")

    def test_invalid_response_fails(self):
        with self.assertRaises(ValueError):
            GUARD.parse_qq_guard_response("可以发送", "草稿")


if __name__ == "__main__":
    unittest.main()
