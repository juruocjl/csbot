"""Offline checks for the 2026 Singapore Major assets and simulation pipeline.

No database, network request or QQ message is used: the stage assets, the Swiss
pairing rules, the team-name resolver, a small simulation sample and the 32 logo
files are all exercised locally.
"""

import ast
import json
import os
import shutil
import sys
import tempfile
import unittest
from pathlib import Path

import nonebot

ROOT = Path(__file__).parents[1]
ASSETS = ROOT / "assets"
FRONT_ICONS_CANDIDATES = (
    ROOT.parent / "csbot-front" / "public" / "team-icons" / "2026-singapore",
    ROOT / "dist" / "team-icons" / "2026-singapore",
)
FRONT_ICONS = next((p for p in FRONT_ICONS_CANDIDATES if p.is_dir()), FRONT_ICONS_CANDIDATES[0])
STAGE1 = ASSETS / "2026-singapore-stage1.json"
STAGE2_INVITES = ASSETS / "2026-singapore-stage2.invites.json"
STAGE3_INVITES = ASSETS / "2026-singapore-stage3.invites.json"
CS_SERVER_SOURCE = ROOT / "plugins" / "cs_server" / "__init__.py"
PNG_HEADER = b"\x89PNG\r\n\x1a\n"

sys.path.insert(0, str(ROOT))
nonebot.init(
    _env_file=None,
    driver="~fastapi",
    cs_database="sqlite+aiosqlite:///:memory:",
    major_name="2026-singapore",
    major_stage="stage1",
    major_event_id=9303,
    cs_group_list=[],
)
for _plugin in ("models", "utils"):
    assert nonebot.load_plugin(ROOT / "plugins" / _plugin)
assert nonebot.load_plugin(ROOT / "plugins" / "major_hw")

MAJOR_HW = nonebot.get_plugin("major_hw").module
GEN = sys.modules["plugins.major_hw.gen_win_matrix"]
SIM = sys.modules["plugins.major_hw.simulate"]
VERIFY = sys.modules["plugins.major_hw.verify"]

FIRST_ROUND = [
    ("Ninjas in Pyjamas", "M80"),
    ("PARIVISION", "DENDELE"),
    ("HEROIC", "Imperial"),
    ("FaZe", "Wildcard"),
    ("Liquid", "BC.Game"),
    ("Eternal Fire", "FlyQuest"),
    ("100 Thieves", "Chinggis Warriors"),
    ("9z", "Lynn Vision"),
]

DISP_NAMES = {
    "PVISION": "PARIVISION",
    "9z": "9z",
    "Liquid": "Liquid",
    "FaZe": "FaZe",
    "M80": "M80",
    "EF": "Eternal Fire",
    "NIP": "Ninjas in Pyjamas",
    "Heroic": "HEROIC",
    "DENDELE": "DENDELE",
    "100 Thieves": "100 Thieves",
    "Wildcard": "Wildcard",
    "LVG": "Lynn Vision",
    "BC.Game": "BC.Game",
    "Imperial": "Imperial",
    "FlyQuest": "FlyQuest",
    "CW": "Chinggis Warriors",
    "GOSH": "BC.Game",
    "nip": "Ninjas in Pyjamas",
    "pv": "PARIVISION",
    "ef": "Eternal Fire",
    "cw": "Chinggis Warriors",
    "lvg": "Lynn Vision",
    "bcg": "BC.Game",
    "bc.g": "BC.Game",
    "100t": "100 Thieves",
    "m80": "M80",
    "dend": "DENDELE",
    "impe": "Imperial",
    "wild": "Wildcard",
    "fly": "FlyQuest",
}


def load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def load_singapore_logos_from_source() -> dict[str, str]:
    """Read the literal dict without loading the heavyweight cs_server plugin."""
    tree = ast.parse(CS_SERVER_SOURCE.read_text(encoding="utf-8"))
    for node in tree.body:
        if not isinstance(node, ast.AnnAssign) or not isinstance(node.target, ast.Name):
            continue
        if node.target.id == "MAJOR_SINGAPORE_TEAM_LOGOS":
            return ast.literal_eval(node.value)
    raise AssertionError("MAJOR_SINGAPORE_TEAM_LOGOS not found in cs_server/__init__.py")


class StageAssetTest(unittest.TestCase):
    def test_stage1_asset_covers_16_seeds(self) -> None:
        data = load_json(STAGE1)
        teams = data["teams"]
        self.assertEqual(len(teams), 16)
        self.assertEqual(sorted(team["seed"] for team in teams.values()), list(range(1, 17)))
        self.assertEqual(set(data["systems"]), {"valve", "hltv"})
        for name, team in teams.items():
            with self.subTest(team=name):
                self.assertGreater(team["valve"], 0)
                self.assertGreater(team["hltv"], 0)
                self.assertTrue(team["alias"])

    def test_invite_templates_cover_8_seeds(self) -> None:
        for path in (STAGE2_INVITES, STAGE3_INVITES):
            with self.subTest(template=path.name):
                teams = load_json(path)["teams"]
                self.assertEqual(len(teams), 8)
                self.assertEqual(sorted(team["seed"] for team in teams.values()), list(range(1, 9)))

    def test_stage3_invites_declare_bo3(self) -> None:
        self.assertEqual(load_json(STAGE3_INVITES).get("match_format"), "bo3")
        self.assertIsNone(load_json(STAGE2_INVITES).get("match_format"))

    def test_stage1_source_url(self) -> None:
        self.assertEqual(load_json(STAGE1)["source"], "https://majors.im/2026/singapore")


class SwissFirstRoundTest(unittest.TestCase):
    def test_first_round_matches_majors_im_pairings(self) -> None:
        teams = GEN.load_teams(STAGE1)
        swiss = SIM.SwissSystem(
            win_matrix={},
            records={team: SIM.Record.new() for team in teams},
            remaining=set(teams),
        )
        pairs = [(team_a.name, team_b.name) for team_a, team_b in swiss.round_matches(1)]
        self.assertEqual(pairs, FIRST_ROUND)


class TeamResolverTest(unittest.TestCase):
    def setUp(self) -> None:
        self.alias2full = dict(MAJOR_HW.alias2full)

    def test_5e_names_and_short_codes_resolve_exactly(self) -> None:
        for raw, expected in DISP_NAMES.items():
            with self.subTest(raw=raw):
                self.assertEqual(GEN.resolve_team_name(raw, self.alias2full), expected)

    def test_no_cross_team_collision(self) -> None:
        resolved = [GEN.resolve_team_name(raw, self.alias2full) for raw in DISP_NAMES]
        self.assertEqual(len(set(resolved)), len(set(DISP_NAMES.values())))

    def test_unknown_name_falls_back_to_fuzzy_match(self) -> None:
        self.assertEqual(
            GEN.resolve_team_name("Team Liquid.", self.alias2full),
            "Liquid",
        )

    def test_empty_alias_map_is_rejected(self) -> None:
        with self.assertRaisesRegex(ValueError, "alias2full is empty"):
            GEN.resolve_team_name("NIP", {})


class SimulationPipelineTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.tmp_path = Path(self.tmp.name)
        self.previous_cwd = os.getcwd()

    def run_pipeline(self) -> tuple[Path, dict]:
        os.chdir(self.tmp_path)
        try:
            GEN.gen_win_matrix(str(STAGE1), [])
            simulation = SIM.Simulation(STAGE1, "win_matrix.csv")
            results = simulation.batch(200, show_progress=False)
            output = self.tmp_path / "result.txt"
            SIM.format_results(results, 200, 0.0, output)
        finally:
            os.chdir(self.previous_cwd)
        self.assertTrue((self.tmp_path / "win_matrix.csv").exists())
        parsed, total = VERIFY.parse_simulation_results(str(output))
        return output, parsed

    def test_small_simulation_output(self) -> None:
        output, parsed = self.run_pipeline()
        text = output.read_text(encoding="utf-8")
        self.assertIn("# major_hw_result_format=2", text)
        self.assertIn("# teams:", text)
        self.assertGreater(len(parsed), 0)
        self.assertEqual(set(parsed.team_to_bit), set(MAJOR_HW.major_teams))
        combo = {
            "3-0": ["Ninjas in Pyjamas", "PARIVISION"],
            "3-1/3-2": ["HEROIC", "FaZe", "Liquid", "Eternal Fire", "100 Thieves", "9z"],
            "0-3": ["M80", "DENDELE"],
        }
        _, prob_ge5, expected_value = VERIFY.evaluate_combination(combo, parsed)
        self.assertGreaterEqual(prob_ge5, 0.0)
        self.assertLessEqual(prob_ge5, 1.0)
        self.assertGreater(expected_value, 0.0)
        self.assertLess(expected_value, 10.0)

    def test_stale_result_file_is_ignored(self) -> None:
        stale = VERIFY.SimulationResults()
        stale.team_to_bit = {"TYLOO": 1, "BIG": 2, "GamerLegion": 4}
        stale[(1, 2, 4)] = 12.0
        self.assertEqual(MAJOR_HW._validate_loaded_results(stale, 12.0), ({}, 0.0))

    def test_current_stage_result_file_is_kept(self) -> None:
        _, parsed = self.run_pipeline()
        kept, kept_total = MAJOR_HW._validate_loaded_results(parsed, 200.0)
        self.assertIs(kept, parsed)
        self.assertEqual(kept_total, 200.0)


class LogoAssetTest(unittest.TestCase):
    def test_all_32_logos_exist_and_are_png(self) -> None:
        logos = load_singapore_logos_from_source()
        expected = {
            "nip.png", "pv.png", "hero.png", "faze.png", "liqu.png", "ef.png",
            "100t.png", "9z.png", "m80.png", "dend.png", "impe.png", "wild.png",
            "bcg.png", "fly.png", "cw.png", "lvg.png", "fut.png", "nemi.png",
            "g2.png", "1win.png", "navi.png", "astr.png", "mong.png", "mibr.png",
            "mouz.png", "vita.png", "furi.png", "auro.png", "spir.png", "betb.png",
            "falc.png", "lega.png",
        }
        self.assertEqual(set(logos.values()), expected)
        for logo in sorted(expected):
            path = FRONT_ICONS / logo
            with self.subTest(logo=logo):
                self.assertTrue(path.is_file(), f"missing {path}")
                data = path.read_bytes()
                self.assertGreater(len(data), 1000)
                self.assertTrue(data.startswith(PNG_HEADER))


if __name__ == "__main__":
    unittest.main()
