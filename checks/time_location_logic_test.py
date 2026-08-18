from datetime import datetime, timezone
import importlib.util
from pathlib import Path
import unittest


LOGIC_PATH = Path(__file__).parents[1] / "plugins" / "time_location" / "logic.py"
SPEC = importlib.util.spec_from_file_location("time_location_logic", LOGIC_PATH)
assert SPEC and SPEC.loader
LOGIC = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(LOGIC)


class TimeLocationLogicTest(unittest.TestCase):
    def test_empty_query_selects_every_configured_location(self) -> None:
        locations = {
            "中国（北京）": "Asia/Shanghai",
            "美国东部（纽约）": "America/New_York",
        }

        self.assertEqual(LOGIC.select_locations(locations, ""), list(locations.items()))

    def test_country_keyword_selects_all_matching_locations(self) -> None:
        locations = {
            "中国（北京）": "Asia/Shanghai",
            "美国东部（纽约）": "America/New_York",
            "美国西部（洛杉矶）": "America/Los_Angeles",
        }

        selected = LOGIC.select_locations(locations, "美国")

        self.assertEqual([name for name, _ in selected], ["美国东部（纽约）", "美国西部（洛杉矶）"])

    def test_new_york_automatically_switches_dst(self) -> None:
        winter = LOGIC.format_location_time(
            "美国东部（纽约）",
            "America/New_York",
            datetime(2026, 1, 15, 12, tzinfo=timezone.utc),
        )
        summer = LOGIC.format_location_time(
            "美国东部（纽约）",
            "America/New_York",
            datetime(2026, 7, 15, 12, tzinfo=timezone.utc),
        )

        self.assertIn("07:00:00", winter)
        self.assertIn("EST，UTC-05:00", winter)
        self.assertIn("08:00:00", summer)
        self.assertIn("EDT，UTC-04:00", summer)

    def test_shanghai_does_not_change_offset(self) -> None:
        winter = LOGIC.format_location_time(
            "中国（北京）",
            "Asia/Shanghai",
            datetime(2026, 1, 15, 12, tzinfo=timezone.utc),
        )
        summer = LOGIC.format_location_time(
            "中国（北京）",
            "Asia/Shanghai",
            datetime(2026, 7, 15, 12, tzinfo=timezone.utc),
        )

        self.assertIn("20:00:00", winter)
        self.assertIn("UTC+08:00", winter)
        self.assertIn("20:00:00", summer)
        self.assertIn("UTC+08:00", summer)


if __name__ == "__main__":
    unittest.main()
