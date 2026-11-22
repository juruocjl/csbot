"""Pure birthday logic checks: no bot, network, database or AI runtime."""
import importlib.util
from datetime import date
from pathlib import Path
import sys
import unittest


PLUGIN = Path(__file__).parents[1] / "plugins" / "birthday"
SPEC = importlib.util.spec_from_file_location("birthday_logic", PLUGIN / "logic.py")
assert SPEC and SPEC.loader
LOGIC = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = LOGIC
SPEC.loader.exec_module(LOGIC)

GUARD_SPEC = importlib.util.spec_from_file_location(
    "birthday_output_guard", Path(__file__).parents[1] / "plugins" / "cs_ai" / "output_guard.py"
)
assert GUARD_SPEC and GUARD_SPEC.loader
GUARD = importlib.util.module_from_spec(GUARD_SPEC)
sys.modules[GUARD_SPEC.name] = GUARD
GUARD_SPEC.loader.exec_module(GUARD)


class BirthdayParsingTest(unittest.TestCase):
    def test_supported_date_formats_normalize_to_mmdd(self) -> None:
        cases = {
            "07-10": "07-10",
            "7-1": "07-01",
            "7/1": "07-01",
            " 7 . 1 ": "07-01",
            "7月10日": "07-10",
            "7 月 1 号": "07-01",
            "2000-02-29": "02-29",
            "2024/7/10": "07-10",
            "2024年7月10日": "07-10",
            "12-31": "12-31",
        }
        for raw, expected in cases.items():
            with self.subTest(raw=raw):
                self.assertEqual(LOGIC.parse_birthday_input(raw), expected)

    def test_disable_and_clear_words(self) -> None:
        for raw in ("none", "NONE", " off ", "关闭", "取消", "禁用", "disable"):
            with self.subTest(raw=raw):
                self.assertEqual(LOGIC.parse_birthday_input(raw), LOGIC.DISABLED_VALUE)
        for raw in ("default", "clear", "reset", "默认", "清除", "恢复", "恢复默认"):
            with self.subTest(raw=raw):
                self.assertEqual(LOGIC.parse_birthday_input(raw), LOGIC.CLEAR_OVERRIDE)

    def test_invalid_dates_are_rejected(self) -> None:
        for raw in ("02-30", "04-31", "13-01", "00-10", "abc", "", "   ",
                    "7月32日", "2024-13-01", "2001-02-29", "07-10 额外"):
            with self.subTest(raw=raw):
                with self.assertRaises(ValueError):
                    LOGIC.parse_birthday_input(raw)

    def test_validate_mmdd_accepts_leap_day_only_for_real_calendar(self) -> None:
        self.assertEqual(LOGIC.validate_mmdd(2, 29), "02-29")
        for month, day in ((2, 30), (4, 31), (13, 1), (0, 10), (True, 1)):
            with self.subTest(month=month, day=day):
                with self.assertRaises(ValueError):
                    LOGIC.validate_mmdd(month, day)


class BirthdayResolutionTest(unittest.TestCase):
    def test_override_wins_over_profile(self) -> None:
        result = LOGIC.resolve_effective_birthday("1", 3, 4, {"1": "07-10"})
        self.assertEqual((result.mmdd, result.source), ("07-10", "override"))

    def test_disabled_override_blocks_profile(self) -> None:
        result = LOGIC.resolve_effective_birthday(
            "1", 3, 4, {"1": "none"}, profile_year=2000
        )
        self.assertEqual((result.mmdd, result.source), (None, "disabled"))

    def test_profile_is_used_without_override(self) -> None:
        result = LOGIC.resolve_effective_birthday(
            "1", 3, 4, {}, profile_year=2000
        )
        self.assertEqual((result.mmdd, result.source), ("03-04", "profile"))
        self.assertEqual(result.profile_year, 2000)

    def test_unpublished_profile_is_none(self) -> None:
        for month, day in ((0, 0), (None, None), (13, 1), ("07", "10")):
            with self.subTest(month=month, day=day):
                result = LOGIC.resolve_effective_birthday("1", month, day, {})
                self.assertEqual((result.mmdd, result.source), (None, "none"))

    def test_invalid_override_value_falls_back_to_profile(self) -> None:
        result = LOGIC.resolve_effective_birthday("1", 3, 4, {"1": "13-01"})
        self.assertEqual((result.mmdd, result.source), ("03-04", "profile"))


class BirthdayDateMatchTest(unittest.TestCase):
    def test_leap_day_matches_common_year_feb_28(self) -> None:
        self.assertTrue(LOGIC.matches_birthday_on_date("02-29", date(2024, 2, 29)))
        self.assertTrue(LOGIC.matches_birthday_on_date("02-29", date(2025, 2, 28)))
        self.assertTrue(LOGIC.matches_birthday_on_date("02-29", date(2023, 2, 28)))
        self.assertFalse(LOGIC.matches_birthday_on_date("02-29", date(2025, 3, 1)))
        self.assertFalse(LOGIC.matches_birthday_on_date("02-29", date(2024, 2, 28)))

    def test_normal_day_and_empty_value(self) -> None:
        self.assertTrue(LOGIC.matches_birthday_on_date("07-10", date(2026, 7, 10)))
        self.assertFalse(LOGIC.matches_birthday_on_date("07-11", date(2026, 7, 10)))
        self.assertFalse(LOGIC.matches_birthday_on_date(None, date(2026, 7, 10)))


class BirthdayCooldownTest(unittest.TestCase):
    def test_first_edit_is_allowed(self) -> None:
        self.assertEqual(LOGIC.check_cooldown(None, 1_000, 86_400), (True, 0))

    def test_cooldown_blocks_with_exact_remaining_seconds(self) -> None:
        self.assertEqual(LOGIC.check_cooldown(1_000, 1_100, 86_400), (False, 86_300))

    def test_cooldown_expires(self) -> None:
        self.assertEqual(LOGIC.check_cooldown(1_000, 87_400, 86_400), (True, 0))

    def test_zero_cooldown_disables_the_limit(self) -> None:
        self.assertEqual(LOGIC.check_cooldown(1_000, 1_001, 0), (True, 0))

    def test_format_remaining(self) -> None:
        self.assertEqual(LOGIC.format_cooldown_remaining(23 * 3600 + 15 * 60),
                         "23 小时 15 分钟")
        self.assertEqual(LOGIC.format_cooldown_remaining(45 * 60), "45 分钟")
        self.assertEqual(LOGIC.format_cooldown_remaining(30), "30 秒")
        self.assertEqual(LOGIC.format_cooldown_remaining(0), "0 秒")


class BirthdayNoticeTextTest(unittest.TestCase):
    def test_strips_at_markers(self) -> None:
        self.assertEqual(
            LOGIC.sanitize_notice_text("[at:12345] 生日快乐", fallback_name=""),
            "生日快乐",
        )
        self.assertEqual(
            LOGIC.sanitize_notice_text("祝福 [AT：12345] 你", fallback_name="小明"),
            "祝福 小明 你",
        )
        self.assertEqual(
            LOGIC.sanitize_notice_text("祝福 [at： 12345] 你\n[at:999]"),
            "祝福 你",
        )

    def test_normalizes_newlines_and_blank_lines(self) -> None:
        self.assertEqual(
            LOGIC.sanitize_notice_text("\r\n第一段\r\n\r\n第二段\r\n"),
            "第一段\n\n第二段",
        )

    def test_rejects_empty_and_guard_placeholders(self) -> None:
        for raw in ("", "   \n \n", GUARD.QQ_GUARD_BLOCKED_TEXT, GUARD.QQ_GUARD_FAILED_TEXT):
            with self.subTest(raw=raw):
                with self.assertRaisesRegex(ValueError, "祝福正文不可发布"):
                    LOGIC.sanitize_notice_text(
                        raw,
                        blocked_texts=(GUARD.QQ_GUARD_BLOCKED_TEXT, GUARD.QQ_GUARD_FAILED_TEXT),
                    )

    def test_long_text_is_truncated_at_sentence_boundary(self) -> None:
        raw = "生日快乐。" * 400
        result = LOGIC.sanitize_notice_text(raw)
        self.assertLessEqual(len(result), 1200)
        self.assertTrue(result.endswith("。"))

    def test_long_text_without_punctuation_stays_within_limit(self) -> None:
        result = LOGIC.sanitize_notice_text("生" * 1500)
        self.assertLessEqual(len(result), 1200)


class BirthdayPromptTest(unittest.TestCase):
    def test_prompt_requires_research_before_writing(self) -> None:
        prompt = LOGIC.build_birthday_prompt(
            group_id="1058060222",
            user_id="123456",
            display_name="小明",
            nickname="nick",
            card="群名片",
            target_date=date(2026, 7, 10),
            source="profile",
            birthday_year=2000,
        )
        for expected in ("2026年07月10日", "1058060222", "QQ=123456", "群名片", "nick",
                         "小明", "memory_search", "csdata.search", "csdata.query",
                         "matches_pw", "game_history", "不要输出 [at:QQ号]",
                         "严禁编造"):
            with self.subTest(expected=expected):
                self.assertIn(expected, prompt)

    def test_prompt_uses_deadpan_exaggeration_with_layered_fallback(self) -> None:
        prompt = LOGIC.build_birthday_prompt(
            group_id="1058060222",
            user_id="123456",
            display_name="小明",
            nickname="nick",
            card="群名片",
            target_date=date(2026, 7, 10),
            source="profile",
            birthday_year=2000,
        )
        for expected in ("一本正经的夸张反差", "夸张外壳", "群史馆", "授勋词",
                         "春晚式官话", "彩虹屁", "反差靠具体实料",
                         "料很足", "料不多", "几乎没料", "40~90",
                         "绝不假装很熟", "严禁编造", "350 字以内"):
            with self.subTest(expected=expected):
                self.assertIn(expected, prompt)
        # The old "sincere and warm" fallback was the AI-flavoured default we replaced.
        self.assertNotIn("真诚温暖地祝贺", prompt)
        self.assertNotIn("120~350", prompt)

    def test_prompt_marks_override_source(self) -> None:
        prompt = LOGIC.build_birthday_prompt(
            group_id="1", user_id="2", display_name="3", nickname="",
            card="", target_date=date(2026, 1, 1), source="override",
        )
        self.assertIn("覆盖设置", prompt)
        self.assertIn("群名片=无", prompt)

    def test_days_until_birthday_orders_next_occurrences(self) -> None:
        cases = (
            ("07-10", date(2026, 7, 10), 0),
            ("07-10", date(2026, 7, 11), 364),
            ("01-01", date(2026, 12, 30), 2),
            ("02-29", date(2026, 2, 27), 1),   # common year -> 02-28
            ("02-29", date(2028, 2, 29), 0),   # leap year
            ("02-29", date(2028, 3, 1), 364),  # next common-year 02-28
            ("12-31", date(2026, 1, 1), 364),
        )
        for mmdd, today, expected in cases:
            with self.subTest(mmdd=mmdd, today=today):
                self.assertEqual(LOGIC.days_until_birthday(mmdd, today), expected)
        for invalid in (None, "", "7-10", "13-01", "02-30", "abc"):
            with self.subTest(invalid=invalid):
                self.assertIsNone(LOGIC.days_until_birthday(invalid, date(2026, 1, 1)))


if __name__ == "__main__":
    unittest.main()
