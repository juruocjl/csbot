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


if __name__ == "__main__":
    unittest.main()
