import importlib.util
from pathlib import Path
import sys
import unittest


LOGIC_PATH = Path(__file__).parents[1] / "plugins" / "runtime_config" / "logic.py"
SPEC = importlib.util.spec_from_file_location("runtime_config_logic", LOGIC_PATH)
assert SPEC and SPEC.loader
LOGIC = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = LOGIC
SPEC.loader.exec_module(LOGIC)


class RuntimeConfigLogicTest(unittest.TestCase):
    def setUp(self) -> None:
        self.definition = LOGIC.get_definition("hltv_event_id_list")

    def test_integer_list_round_trip(self) -> None:
        value, encoded = LOGIC.encode_value(self.definition, [101, 202])

        self.assertEqual(value, [101, 202])
        self.assertEqual(encoded, "[101,202]")
        self.assertEqual(LOGIC.decode_value(self.definition, encoded), [101, 202])

    def test_string_event_id_is_rejected(self) -> None:
        with self.assertRaisesRegex(ValueError, "integer_list"):
            LOGIC.encode_value(self.definition, ["101"])

    def test_non_positive_event_id_is_rejected(self) -> None:
        with self.assertRaisesRegex(ValueError, "integer_list"):
            LOGIC.encode_value(self.definition, [0])

    def test_invalid_stored_json_is_rejected(self) -> None:
        with self.assertRaisesRegex(ValueError, "JSON"):
            LOGIC.decode_value(self.definition, "not-json")

    def test_unknown_config_key_is_rejected(self) -> None:
        with self.assertRaisesRegex(KeyError, "未知的热配置项"):
            LOGIC.get_definition("missing")

    def test_season_values_round_trip(self) -> None:
        for key in ("cs_season_id", "cs_last_season_id"):
            with self.subTest(key=key):
                definition = LOGIC.get_definition(key)
                value, encoded = LOGIC.encode_value(definition, "S42")
                self.assertEqual(value, "S42")
                self.assertEqual(LOGIC.decode_value(definition, encoded), "S42")

    def test_invalid_season_values_rejected(self) -> None:
        for key in ("cs_season_id", "cs_last_season_id"):
            definition = LOGIC.get_definition(key)
            for value in ("", " ", "s21", "S0", "S-1", "S01", 21, True, ["S21"], "S21' OR 1=1 --"):
                with self.subTest(key=key, value=value):
                    with self.assertRaises(ValueError):
                        LOGIC.encode_value(definition, value)

    def test_ai_model_round_trip_strips_outer_whitespace(self) -> None:
        definition = LOGIC.get_definition("cs_ai_model")
        value, encoded = LOGIC.encode_value(definition, "  model-v2  ")

        self.assertEqual(value, "model-v2")
        self.assertEqual(encoded, '"model-v2"')
        self.assertEqual(LOGIC.decode_value(definition, encoded), "model-v2")

    def test_empty_ai_model_is_rejected(self) -> None:
        definition = LOGIC.get_definition("cs_ai_model")
        for value in ("", "   ", 123, None, ["model"]):
            with self.subTest(value=value):
                with self.assertRaises(ValueError):
                    LOGIC.encode_value(definition, value)

    def test_time_locations_round_trip(self) -> None:
        definition = LOGIC.get_definition("cs_time_locations")
        locations = {" 北京 ": " Asia/Shanghai ", "纽约": "America/New_York"}

        value, encoded = LOGIC.encode_value(definition, locations)

        self.assertEqual(value, {"北京": "Asia/Shanghai", "纽约": "America/New_York"})
        self.assertEqual(LOGIC.decode_value(definition, encoded), value)

    def test_invalid_time_locations_are_rejected(self) -> None:
        definition = LOGIC.get_definition("cs_time_locations")
        for value in ({"北京": 8}, {"": "Asia/Shanghai"}, [], "Asia/Shanghai"):
            with self.subTest(value=value):
                with self.assertRaises(ValueError):
                    LOGIC.encode_value(definition, value)


if __name__ == "__main__":
    unittest.main()
