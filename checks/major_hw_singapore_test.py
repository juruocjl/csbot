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
STAGE2 = ASSETS / "2026-singapore-stage2.json"
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


STAGE1_RESULTS = [
    ["100T", "M80", "2:1", "csgo_mc_2400581"],
    ["Imperial", "Wildcard", "2:1", "csgo_mc_2400580"],
    ["LVG", "Heroic", "2:0", "csgo_mc_2400579"],
    ["Wildcard", "CW", "2:1", "csgo_mc_2400578"],
    ["Liquid", "Imperial", "2:0", "csgo_mc_2400577"],
    ["100T", "9z", "2:1", "csgo_mc_2400576"],
    ["PARIVISION", "LVG", "2:0", "csgo_mc_2400575"],
    ["Heroic", "FaZe", "2:1", "csgo_mc_2400574"],
    ["EF", "M80", "2:1", "csgo_mc_2400573"],
    ["CW", "BC.Game", "2:1", "csgo_mc_2400572"],
    ["NIP", "Liquid", "2:0", "csgo_mc_2400571"],
    ["FlyQuest", "Imperial", "2:0", "csgo_mc_2400569"],
    ["9z", "DENDELE", "2:0", "csgo_mc_2400570"],
    ["PARIVISION", "FaZe", "13:10", "csgo_mc_2400567"],
    ["LVG", "Wildcard", "13:8", "csgo_mc_2400568"],
    ["M80", "Heroic", "13:10", "csgo_mc_2400565"],
    ["EF", "100T", "13:6", "csgo_mc_2400566"],
    ["FaZe", "BC.Game", "13:3", "csgo_mc_2400564"],
    ["Liquid", "Wildcard", "13:8", "csgo_mc_2400563"],
    ["FlyQuest", "PARIVISION", "13:9", "csgo_mc_2400561"],
    ["EF", "DENDELE", "13:8", "csgo_mc_2400562"],
    ["Imperial", "100T", "13:11", "csgo_mc_2400559"],
    ["M80", "9z", "13:8", "csgo_mc_2400560"],
    ["NIP", "LVG", "13:10", "csgo_mc_2400557"],
    ["Heroic", "CW", "13:3", "csgo_mc_2400558"],
    ["FlyQuest", "EF", "13:7", "csgo_mc_2400501"],
    ["Wildcard", "FaZe", "13:9", "csgo_mc_2400500"],
    ["PARIVISION", "DENDELE", "13:3", "csgo_mc_2400499"],
    ["Liquid", "BC.Game", "13:1", "csgo_mc_2400498"],
    ["100T", "CW", "13:6", "csgo_mc_2400497"],
    ["NIP", "M80", "13:10", "csgo_mc_2400496"],
    ["LVG", "9z", "13:11", "csgo_mc_2400495"],
    ["Imperial", "Heroic", "13:6", "csgo_mc_2400494"],
]

STAGE2_SEEDS = [
    (1, "FUT"),
    (2, "Nemiga"),
    (3, "G2"),
    (4, "1win"),
    (5, "Natus Vincere"),
    (6, "Astralis"),
    (7, "The MongolZ"),
    (8, "MIBR"),
    (9, "FlyQuest"),
    (10, "Ninjas in Pyjamas"),
    (11, "Liquid"),
    (12, "Eternal Fire"),
    (13, "PARIVISION"),
    (14, "Imperial"),
    (15, "Lynn Vision"),
    (16, "100 Thieves"),
]

# majors.im 2026-singapore Stage 2 Round 1: seed N vs seed N+8.
STAGE2_FIRST_ROUND = [
    ("FUT", "FlyQuest"),
    ("Nemiga", "Ninjas in Pyjamas"),
    ("G2", "Liquid"),
    ("1win", "Eternal Fire"),
    ("Natus Vincere", "PARIVISION"),
    ("Astralis", "Imperial"),
    ("The MongolZ", "Lynn Vision"),
    ("MIBR", "100 Thieves"),
]

# Team names exactly as the 5E event 9304 roster / results spell them.
STAGE2_5E_NAMES = {
    "FUT": "FUT",
    "Nemiga": "Nemiga",
    "NAVI": "Natus Vincere",
    "G2": "G2",
    "MIBR": "MIBR",
    "Mongolz": "The MongolZ",
    "1win": "1win",
    "Astralis": "Astralis",
    "FlyQuest": "FlyQuest",
    "NIP": "Ninjas in Pyjamas",
    "EF": "Eternal Fire",
    "PARIVISION": "PARIVISION",
    "Liquid": "Liquid",
    "LVG": "Lynn Vision",
    "Imperial": "Imperial",
    "100T": "100 Thieves",
}


def load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def stage1_records() -> dict[str, tuple[int, int]]:
    """Replay the 33 stored Stage 1 results into (wins, losses) per 5E name."""
    records: dict[str, list[int]] = {}
    for winner, loser, _score, _match_id in STAGE1_RESULTS:
        records.setdefault(winner, [0, 0])
        records.setdefault(loser, [0, 0])
        records[winner][0] += 1
        records[loser][1] += 1
    return {name: (wins, losses) for name, (wins, losses) in records.items()}


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


class Stage2AssetTest(unittest.TestCase):
    def setUp(self) -> None:
        self.data = load_json(STAGE2)
        self.teams = self.data["teams"]

    def test_stage2_asset_covers_16_seeds(self) -> None:
        self.assertEqual(len(self.teams), 16)
        self.assertEqual(sorted(t["seed"] for t in self.teams.values()), list(range(1, 17)))
        self.assertEqual(set(self.data["systems"]), {"valve", "hltv"})
        self.assertEqual(self.data["source"], "https://majors.im/2026/singapore")
        for name, team in self.teams.items():
            with self.subTest(team=name):
                self.assertGreater(team["valve"], 0)
                self.assertGreater(team["hltv"], 0)
                self.assertTrue(team["alias"], f"{name} lost its alias list")
        seeds_to_names = {t["seed"]: name for name, t in self.teams.items()}
        self.assertEqual(
            [(seed, seeds_to_names[seed]) for seed in range(1, 17)],
            STAGE2_SEEDS,
        )

    def test_stage2_keeps_invite_ratings_for_seeds_1_to_8(self) -> None:
        invites = load_json(STAGE2_INVITES)["teams"]
        for name, invite in invites.items():
            with self.subTest(team=name):
                team = self.teams[name]
                self.assertEqual(team["seed"], invite["seed"])
                self.assertEqual(team["valve"], invite["valve"])
                self.assertEqual(team["hltv"], invite["hltv"])
                self.assertEqual(team["alias"], invite["alias"])

    def test_stage2_advancers_are_stage1_three_win_teams(self) -> None:
        records = stage1_records()
        self.assertEqual(len(records), 16)
        self.assertEqual(sum(record[0] for record in records.values()), len(STAGE1_RESULTS))
        self.assertEqual(sum(record[1] for record in records.values()), len(STAGE1_RESULTS))

        alias2full = {name: name for name in self.teams}
        for name, team in self.teams.items():
            for alias in team["alias"]:
                alias2full[alias] = name

        winners_by_seed: dict[str, list[int]] = {"3-0": [], "3-1": [], "3-2": []}
        advanced: set[str] = set()
        for raw_name, (wins, losses) in records.items():
            if wins != 3:
                continue
            canonical = GEN.resolve_team_name(raw_name, alias2full)
            advanced.add(canonical)
            winners_by_seed[f"3-{losses}"].append(self.teams[canonical]["seed"])

        self.assertEqual(advanced, {name for _seed, name in STAGE2_SEEDS[8:]})
        self.assertEqual(sorted(winners_by_seed["3-0"]), [9, 10])
        self.assertEqual(sorted(winners_by_seed["3-1"]), [11, 12, 13])
        self.assertEqual(sorted(winners_by_seed["3-2"]), [14, 15, 16])

    def test_stage2_ratings_replay_stage1_finished_matches(self) -> None:
        previous_teams = GEN.load_teams(STAGE1)
        system_names = GEN.load_system_names(STAGE1)
        alias2full = {team.name: team.name for team in previous_teams}
        for team in previous_teams:
            for alias in team.alias:
                alias2full[alias] = team.name
        rolled = GEN.apply_finished_matches_to_ratings(
            previous_teams,
            system_names,
            [tuple(row) for row in STAGE1_RESULTS],
            alias2full,
            newest_first=True,
        )
        rolled_by_name = {team.name: team for team in rolled}
        for name, team in self.teams.items():
            if name not in rolled_by_name:
                continue
            expected = [int(round(value)) for value in rolled_by_name[name].rating]
            with self.subTest(team=name):
                self.assertEqual([team[system] for system in system_names], expected)


class Stage2SwissTest(unittest.TestCase):
    def test_stage2_first_round_matches_majors_im_pairings(self) -> None:
        teams = GEN.load_teams(STAGE2)
        swiss = SIM.SwissSystem(
            win_matrix={},
            records={team: SIM.Record.new() for team in teams},
            remaining=set(teams),
        )
        pairs = [(team_a.name, team_b.name) for team_a, team_b in swiss.round_matches(1)]
        self.assertEqual(pairs, STAGE2_FIRST_ROUND)

    def test_stage2_5e_names_resolve_exactly(self) -> None:
        teams = GEN.load_teams(STAGE2)
        alias2full: dict[str, str] = {}
        for team in teams:
            for token in (team.name, *team.alias):
                self.assertNotIn(token, alias2full, f"duplicate token {token!r}")
                alias2full[token] = team.name
        resolved = {}
        for raw, expected in STAGE2_5E_NAMES.items():
            with self.subTest(raw=raw):
                canonical = GEN.resolve_team_name(raw, alias2full)
                self.assertEqual(canonical, expected)
                resolved[raw] = canonical
        self.assertEqual(set(resolved.values()), {name for _seed, name in STAGE2_SEEDS})

    def test_stage2_logos_exist_for_every_team(self) -> None:
        logos = load_singapore_logos_from_source()
        for _seed, name in STAGE2_SEEDS:
            with self.subTest(team=name):
                self.assertIn(name, logos)
                path = FRONT_ICONS / logos[name]
                self.assertTrue(path.is_file(), f"missing {path}")
                self.assertTrue(path.read_bytes().startswith(PNG_HEADER))

    def test_stage2_zero_match_simulation_runs(self) -> None:
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        tmp_path = Path(tmp.name)
        previous_cwd = os.getcwd()
        os.chdir(tmp_path)
        try:
            GEN.gen_win_matrix(str(STAGE2), [])
            simulation = SIM.Simulation(STAGE2, "win_matrix.csv")
            results = simulation.batch(40, show_progress=False)
            output = tmp_path / "result.txt"
            SIM.format_results(results, 40, 0.0, output)
        finally:
            os.chdir(previous_cwd)
        self.assertTrue((tmp_path / "win_matrix.csv").exists())
        parsed, total = VERIFY.parse_simulation_results(str(output))
        self.assertGreater(len(parsed), 0)
        self.assertEqual(total, 40.0)
        self.assertEqual(set(parsed.team_to_bit), {name for _seed, name in STAGE2_SEEDS})


if __name__ == "__main__":
    unittest.main()
