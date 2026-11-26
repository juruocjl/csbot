#!/usr/bin/env python3
"""AI 群聊黑名单规则检查：不启动 NoneBot、不连网络与数据库。

Importing ``plugins.cs_ai`` would boot NoneBot and the business database, so
only the three rule helpers are extracted from the shipped source and executed
against stub events.
"""
from __future__ import annotations

import ast
import asyncio
import importlib.util
from pathlib import Path
import sys
import types
import unittest

REPO = Path(__file__).resolve().parents[1]
PLUGIN_PATH = REPO / "plugins" / "cs_ai" / "__init__.py"
LOGIC_PATH = REPO / "plugins" / "runtime_config" / "logic.py"

BOT_ID = "10000"
BLOCKED_UID = "2864405242"
NORMAL_UID = "123456789"


class FakeRuntimeConfig:
    def __init__(self, value):
        self.value = value

    async def get(self, key):
        if isinstance(self.value, Exception):
            raise self.value
        if key != "cs_ai_blocked_users":
            raise AssertionError(f"unexpected config key: {key}")
        return self.value


def _load_helpers(blocked_value):
    tree = ast.parse(PLUGIN_PATH.read_text(encoding="utf-8"), filename=str(PLUGIN_PATH))
    wanted = ("_is_blocked_user", "_human_group", "_explicitly_called")
    nodes = [
        item
        for item in tree.body
        if isinstance(item, ast.AsyncFunctionDef) and item.name in wanted
    ]
    assert {node.name for node in nodes} == set(wanted)
    namespace = {
        "Bot": object,
        "GroupMessageEvent": object,
        "runtime_config": FakeRuntimeConfig(blocked_value),
        "logger": types.SimpleNamespace(warning=lambda *args, **kwargs: None),
    }
    exec(
        compile(ast.Module(body=nodes, type_ignores=[]), str(PLUGIN_PATH), "exec"),
        namespace,
    )
    return namespace["_is_blocked_user"], namespace["_human_group"], namespace["_explicitly_called"]


def _event(user_id, *, at_bot=False, reply_to_bot=False, reply_sender=None):
    segments = []
    if at_bot:
        segments.append(types.SimpleNamespace(type="at", data={"qq": BOT_ID}))
    segments.append(types.SimpleNamespace(type="text", data={"text": "/ai 你好"}))
    reply = None
    if reply_to_bot or reply_sender is not None:
        reply = types.SimpleNamespace(
            sender=types.SimpleNamespace(user_id=reply_sender or BOT_ID)
        )
    return types.SimpleNamespace(
        user_id=user_id,
        original_message=segments,
        reply=reply,
    )


def _bot():
    return types.SimpleNamespace(self_id=BOT_ID)


class BlockedUserRuleTest(unittest.TestCase):
    def setUp(self) -> None:
        self.is_blocked, self.human_group, self.explicitly_called = _load_helpers(
            [BLOCKED_UID, " 123456 "]
        )

    def test_blocked_user_never_triggers_ai(self) -> None:
        event = _event(BLOCKED_UID, at_bot=True, reply_to_bot=True)
        self.assertTrue(asyncio.run(self.is_blocked(BLOCKED_UID)))
        self.assertFalse(asyncio.run(self.human_group(_bot(), event)))
        self.assertFalse(asyncio.run(self.explicitly_called(_bot(), event)))

    def test_blocked_user_with_numeric_id_is_matched(self) -> None:
        event = _event(int(BLOCKED_UID), at_bot=True)
        self.assertTrue(asyncio.run(self.is_blocked(int(BLOCKED_UID))))
        self.assertFalse(asyncio.run(self.human_group(_bot(), event)))
        self.assertFalse(asyncio.run(self.explicitly_called(_bot(), event)))

    def test_normal_user_still_passes(self) -> None:
        plain = _event(NORMAL_UID)
        self.assertFalse(asyncio.run(self.is_blocked(NORMAL_UID)))
        self.assertTrue(asyncio.run(self.human_group(_bot(), plain)))
        self.assertFalse(asyncio.run(self.explicitly_called(_bot(), plain)))
        self.assertTrue(
            asyncio.run(self.explicitly_called(_bot(), _event(NORMAL_UID, at_bot=True)))
        )
        self.assertTrue(
            asyncio.run(
                self.explicitly_called(_bot(), _event(NORMAL_UID, reply_to_bot=True))
            )
        )

    def test_bot_itself_is_skipped_even_when_not_blocked(self) -> None:
        event = _event(BOT_ID, at_bot=True)
        self.assertFalse(asyncio.run(self.human_group(_bot(), event)))
        self.assertFalse(asyncio.run(self.explicitly_called(_bot(), event)))

    def test_whitespace_entry_matches_stripped_uid(self) -> None:
        self.assertTrue(asyncio.run(self.is_blocked(123456)))
        self.assertFalse(asyncio.run(self.human_group(_bot(), _event(123456))))

    def test_empty_list_allows_everyone(self) -> None:
        is_blocked, human_group, explicitly_called = _load_helpers([])
        self.assertFalse(asyncio.run(is_blocked(BLOCKED_UID)))
        self.assertTrue(asyncio.run(human_group(_bot(), _event(BLOCKED_UID))))
        self.assertTrue(
            asyncio.run(explicitly_called(_bot(), _event(BLOCKED_UID, at_bot=True)))
        )

    def test_config_read_failure_fails_open(self) -> None:
        is_blocked, human_group, explicitly_called = _load_helpers(
            RuntimeError("热配置读取失败")
        )
        event = _event(BLOCKED_UID, at_bot=True)
        self.assertFalse(asyncio.run(is_blocked(BLOCKED_UID)))
        self.assertTrue(asyncio.run(human_group(_bot(), event)))
        self.assertTrue(asyncio.run(explicitly_called(_bot(), event)))


class SourceWiringTest(unittest.TestCase):
    def test_default_registration_and_rule_wiring(self) -> None:
        source = PLUGIN_PATH.read_text(encoding="utf-8")
        self.assertIn('runtime_config.register_default("cs_ai_blocked_users", [])', source)
        for name in ("_human_group", "_explicitly_called"):
            tree = ast.parse(source, filename=str(PLUGIN_PATH))
            node = next(
                item
                for item in tree.body
                if isinstance(item, ast.AsyncFunctionDef) and item.name == name
            )
            self.assertTrue(
                any(
                    isinstance(call, ast.Call)
                    and isinstance(call.func, ast.Name)
                    and call.func.id == "_is_blocked_user"
                    for call in ast.walk(node)
                ),
                f"{name} 未接入 AI 黑名单拦截",
            )

    def test_definition_registered_in_logic(self) -> None:
        spec = importlib.util.spec_from_file_location("runtime_config_logic_blacklist", LOGIC_PATH)
        assert spec and spec.loader
        module = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = module
        spec.loader.exec_module(module)
        definition = module.get_definition("cs_ai_blocked_users")
        self.assertEqual(definition.value_type, "string_list")
        self.assertEqual(definition.default, [])


if __name__ == "__main__":
    unittest.main()
