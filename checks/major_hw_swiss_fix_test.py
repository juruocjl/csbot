"""离线回归：瑞士轮盘中 Buchholz 对阵修复、对数走势图归零复活、重算脚本增强。

纯离线：不连接数据库、不发 QQ 消息。覆盖

1. 2026 新加坡 Major Stage 1 真实 21 场逐场对阵回归与精确遍历概率；
2. 第 2 / 4 / 5 轮盘中（含回避冲突）的对阵锁定；
3. Python 与 ``tools/major_simulator.cpp`` 一致性（无 ``g++`` 时 skip）；
4. ``chart.py`` 对数/线性归零与复活分段绘制；
5. ``scripts/recalculate_major_stage_results.py`` 的纯函数逻辑。
"""

from __future__ import annotations

import importlib.util
import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

import nonebot


ROOT = Path(__file__).parents[1]
STAGE1 = ROOT / "assets" / "2026-singapore-stage1.json"
PNG_HEADER = b"\x89PNG\r\n\x1a\n"
CPP_SOURCE = ROOT / "tools" / "major_simulator.cpp"
RECALC_SCRIPT = ROOT / "scripts" / "recalculate_major_stage_results.py"

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

SIM = sys.modules["plugins.major_hw.simulate"]
GEN = sys.modules["plugins.major_hw.gen_win_matrix"]
CHART = sys.modules["plugins.major_hw.chart"]
VERIFY = sys.modules["plugins.major_hw.verify"]

# 2026 新加坡 Stage 1 真实 21 场（按比赛场次升序）。
# 结构与 major_hw_bug_analysis.md 复盘完全一致：第 17~20 场是第 3 轮 1-1 组
# BO1，第 21 场为真实的 9z 2:0 DENDELE。
SINGAPORE_MATCHES: list[tuple[str, str]] = [
    # 第 1 轮（1v9 ~ 8v16）
    ("Ninjas in Pyjamas", "M80"),
    ("PARIVISION", "DENDELE"),
    ("Imperial", "HEROIC"),
    ("Wildcard", "FaZe"),
    ("Liquid", "BC.Game"),
    ("FlyQuest", "Eternal Fire"),
    ("100 Thieves", "Chinggis Warriors"),
    ("Lynn Vision", "9z"),
    # 第 2 轮（1-0 / 0-1 组各 4 场）
    ("Ninjas in Pyjamas", "Lynn Vision"),
    ("FlyQuest", "PARIVISION"),
    ("Liquid", "Wildcard"),
    ("Imperial", "100 Thieves"),
    ("HEROIC", "Chinggis Warriors"),
    ("FaZe", "BC.Game"),
    ("Eternal Fire", "DENDELE"),
    ("M80", "9z"),
    # 第 3 轮：先打完 4 场 1-1 组 BO1（#17~#20），再打 9z vs DENDELE（#21）
    ("M80", "HEROIC"),
    ("Eternal Fire", "100 Thieves"),
    ("Lynn Vision", "Wildcard"),
    ("PARIVISION", "FaZe"),
    ("9z", "DENDELE"),
]

SINGAPORE_FIRST_ROUND = [
    ("Ninjas in Pyjamas", "M80"),
    ("PARIVISION", "DENDELE"),
    ("HEROIC", "Imperial"),
    ("FaZe", "Wildcard"),
    ("Liquid", "BC.Game"),
    ("Eternal Fire", "FlyQuest"),
    ("100 Thieves", "Chinggis Warriors"),
    ("9z", "Lynn Vision"),
]

# 16 队合成赛程（T01 ~ T16 按 seed 排序），random.Random(2) 生成的固定结果。
# 前 24 场 = 完整 3 轮，恰好构造出第 4 轮 2-1 组 6 队的回避冲突。
SYNTH_TEAMS = [f"T{index:02d}" for index in range(1, 17)]
SYNTH_SCRIPT: list[tuple[str, str]] = [
    ("T09", "T01"),
    ("T10", "T02"),
    ("T03", "T11"),
    ("T04", "T12"),
    ("T13", "T05"),
    ("T14", "T06"),
    ("T15", "T07"),
    ("T08", "T16"),
    ("T15", "T03"),
    ("T14", "T04"),
    ("T13", "T08"),
    ("T09", "T10"),
    ("T01", "T16"),
    ("T02", "T12"),
    ("T11", "T05"),
    ("T07", "T06"),
    ("T14", "T09"),
    ("T13", "T15"),
    ("T03", "T02"),
    ("T10", "T11"),
    ("T01", "T08"),
    ("T04", "T07"),
    ("T05", "T16"),
    ("T06", "T12"),
    ("T09", "T04"),
    ("T01", "T15"),
    ("T10", "T03"),
    ("T05", "T07"),
    ("T08", "T06"),
    ("T11", "T02"),
    # 第 5 轮盘中先打完 1 场
    ("T15", "T05"),
]

# 第 4 轮开轮完整对阵（含 2-1 组的回避冲突，最终落到 (0,3)(1,5)(2,4) 模式）。
SYNTH_ROUND4_FULL = [
    ("T09", "T04"),
    ("T15", "T01"),
    ("T03", "T10"),
    ("T07", "T05"),
    ("T08", "T06"),
    ("T11", "T02"),
]
# 第 5 轮开轮完整对阵（6 队 2-2 组）。
SYNTH_ROUND5_FULL = [("T15", "T05"), ("T03", "T08"), ("T04", "T11")]


def _pairs(matches) -> list[tuple[str, str]]:
    return [(team_a.name, team_b.name) for team_a, team_b in matches]


def _normalized(pairs) -> list[tuple[str, str]]:
    return sorted(tuple(sorted(pair)) for pair in pairs)


def _new_swiss(teams) -> "SIM.SwissSystem":
    return SIM.SwissSystem(
        win_matrix={},
        records={team: SIM.Record.new() for team in teams},
        remaining=set(teams),
    )


def _replay(teams, script: list[tuple[str, str]], count: int | None = None) -> "SIM.SwissSystem":
    by_name = {team.name: team for team in teams}
    state = _new_swiss(teams)
    for winner, loser in script[:count]:
        state.apply_result(by_name[winner], by_name[loser])
    return state


def _unplayed_names(state, round_num: int) -> set[str]:
    return {
        team.name
        for team in state.remaining
        if state.records[team].wins + state.records[team].losses == round_num - 1
    }


def _played_in_round(state, round_num: int) -> set[str]:
    return {
        team.name
        for team in state.records
        if state.records[team].wins + state.records[team].losses >= round_num
    }


class SingaporePairingRegressionTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.teams = GEN.load_teams(STAGE1)
        cls.by_name = {team.name: team for team in cls.teams}

    def state(self, count: int):
        return _replay(self.teams, SINGAPORE_MATCHES, count)

    def locked_pairings(self, round_num: int) -> list[tuple[str, str]]:
        round_start = {1: 0, 2: 8, 3: 16}[round_num]
        return _pairs(self.state(round_start).round_matches(round_num))

    def test_first_round_matches_real_bracket(self) -> None:
        self.assertEqual(_pairs(self.state(0).round_matches(1)), SINGAPORE_FIRST_ROUND)

    def test_round_two_and_three_locked(self) -> None:
        self.assertEqual(
            _normalized(self.locked_pairings(2)),
            _normalized([
                ("Ninjas in Pyjamas", "Lynn Vision"),
                ("FlyQuest", "PARIVISION"),
                ("Liquid", "Wildcard"),
                ("Imperial", "100 Thieves"),
                ("HEROIC", "Chinggis Warriors"),
                ("FaZe", "BC.Game"),
                ("Eternal Fire", "DENDELE"),
                ("M80", "9z"),
            ]),
        )
        self.assertEqual(
            _normalized(self.locked_pairings(3)),
            _normalized([
                ("Ninjas in Pyjamas", "Liquid"),
                ("Imperial", "FlyQuest"),
                ("Lynn Vision", "Wildcard"),
                ("PARIVISION", "FaZe"),
                ("M80", "HEROIC"),
                ("Eternal Fire", "100 Thieves"),
                ("BC.Game", "Chinggis Warriors"),
                ("9z", "DENDELE"),
            ]),
        )

    def test_every_prefix_keeps_locked_round_pairings(self) -> None:
        """k=0..21 的剩余对阵必须严格等于开轮对阵剔除本轮已完赛场次。"""

        for k in range(len(SINGAPORE_MATCHES) + 1):
            state = self.state(k)
            round_num = state.next_round_num()
            if round_num > 5:
                continue
            played = _played_in_round(state, round_num)
            expected = [
                pair
                for pair in self.locked_pairings(round_num)
                if pair[0] not in played and pair[1] not in played
            ]
            with self.subTest(k=k):
                self.assertEqual(
                    _normalized(_pairs(state.round_matches(round_num))),
                    _normalized(expected),
                )

    def test_round_three_locked_pairings_survive_mid_round(self) -> None:
        """显式断言复盘里被盘中 Buchholz 污染的两组对阵。"""

        two_zero = {("Ninjas in Pyjamas", "Liquid"), ("Imperial", "FlyQuest")}
        zero_two = {("BC.Game", "Chinggis Warriors"), ("9z", "DENDELE")}
        for k in (17, 18, 19, 20):
            state = self.state(k)
            pairs = set(_normalized(_pairs(state.round_matches(3))))
            with self.subTest(k=k):
                for pair in two_zero | zero_two:
                    self.assertIn(tuple(sorted(pair)), pairs, f"k={k} 丢失锁定对阵 {pair}")
        # 第 21 场 9z 2:0 DENDELE 打完后只剩 BC.Game vs Chinggis Warriors
        pairs_k21 = set(_normalized(_pairs(self.state(21).round_matches(3))))
        self.assertNotIn(("9z", "DENDELE"), pairs_k21)
        self.assertIn(("BC.Game", "Chinggis Warriors"), pairs_k21)

    def test_no_team_is_dropped_or_duplicated(self) -> None:
        for k in range(len(SINGAPORE_MATCHES) + 1):
            state = self.state(k)
            round_num = state.next_round_num()
            if round_num > 5:
                continue
            matches = _pairs(state.round_matches(round_num))
            flat = [team for pair in matches for team in pair]
            unplayed = _unplayed_names(state, round_num)
            with self.subTest(k=k):
                self.assertEqual(len(flat), len(set(flat)), "同一队伍被配对多次")
                self.assertEqual(set(flat), unplayed, "有未赛队伍被静默丢弃")
                for team_a, team_b in matches:
                    self.assertNotIn(
                        team_b,
                        state.records[self.by_name[team_a]].teams_faced,
                        f"k={k} 出现重复对阵 {team_a} vs {team_b}",
                    )

    def test_exact_probability_is_positive_after_fix(self) -> None:
        """第 20 场精确遍历：9z 与 CW 可同时晋级，马嘉豪同结构作业不再 0%。"""

        finished_newest_first = [
            (winner, loser, "2:0", f"m{index + 1}")
            for index, (winner, loser) in enumerate(SINGAPORE_MATCHES[:20])
        ][::-1]

        previous_cwd = os.getcwd()
        tmp = tempfile.mkdtemp()
        try:
            os.chdir(tmp)
            GEN.gen_win_matrix(str(STAGE1), finished_newest_first, True)
            simulation = SIM.Simulation(STAGE1)
            exact = simulation.exact(finished_newest_first, newest_first=True)
            self.assertIsNotNone(exact, "第 20 场只剩 13 场，应能精确遍历")
            results, leaf_count = exact
            self.assertEqual(leaf_count, 2 ** 13)
            all_combinations = results[list(results)[0]].pickem_results
            self.assertAlmostEqual(sum(all_combinations.values()), 1.0, places=9)

            team_by_name = {team.name: team for team in simulation.teams}
            nine_z = 1 << team_by_name["9z"].id
            chinggis = 1 << team_by_name["Chinggis Warriors"].id
            dendele = 1 << team_by_name["DENDELE"].id
            bc_game = 1 << team_by_name["BC.Game"].id
            advanced_both = [
                key
                for key in all_combinations
                if key[1] & nine_z and key[1] & chinggis and key[2] == (dendele | bc_game)
            ]
            self.assertTrue(advanced_both, "修复后必须存在 9z 与 CW 同时 3-1/3-2 的分支")

            combo = {
                "3-0": ["FaZe", "100 Thieves"],  # 两队均 1-2，3-0 已挂
                "3-1/3-2": [
                    "Ninjas in Pyjamas",
                    "Liquid",
                    "9z",
                    "Chinggis Warriors",
                    "Eternal Fire",
                    "Lynn Vision",
                ],
                "0-3": ["DENDELE", "BC.Game"],
            }
            parsed = VERIFY.SimulationResults()
            parsed.team_to_bit = {team.name: 1 << team.id for team in simulation.teams}
            for key, count in all_combinations.items():
                parsed[key] += count
            _, probability, expected_value = VERIFY.evaluate_combination(combo, parsed)
            self.assertGreater(probability, 0.0)
            self.assertGreater(expected_value, 0.0)
        finally:
            os.chdir(previous_cwd)
            shutil.rmtree(tmp, ignore_errors=True)


class SyntheticMidRoundRegressionTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.teams = [
            SIM.Team(id=index, name=name, seed=index + 1, rating=())
            for index, name in enumerate(SYNTH_TEAMS)
        ]

    def state(self, count: int):
        return _replay(self.teams, SYNTH_SCRIPT, count)

    def test_round_two_mid_round_keeps_locked_pairings(self) -> None:
        full = _pairs(self.state(8).round_matches(2))
        # 第 2 轮先打完 2 场后，剩余对阵必须与开轮一致。
        played = {team for pair in full[:2] for team in pair}
        state = self.state(10)
        self.assertEqual(state.next_round_num(), 2)
        expected = [pair for pair in full if pair[0] not in played and pair[1] not in played]
        self.assertEqual(_normalized(_pairs(state.round_matches(2))), _normalized(expected))

    def test_round_four_mid_round_keeps_priority_patterns(self) -> None:
        state = self.state(24)
        full = _pairs(state.round_matches(4))
        self.assertEqual(_normalized(full), _normalized(SYNTH_ROUND4_FULL))
        # 2-1 组 6 队首选的 pattern0 存在重复对阵，必须回退到后续模式。
        two_one = [
            team
            for team in state.remaining
            if state.records[team].wins == 2 and state.records[team].losses == 1
        ]
        ordered = sorted(
            two_one,
            key=lambda team: (
                -sum(state.records[opp].diff for opp in state.records[team].teams_faced),
                team.seed,
            ),
        )
        self.assertTrue(
            any(
                ordered[j] in state.records[ordered[i]].teams_faced
                for i, j in ((0, 5), (1, 4), (2, 3))
            ),
            "该用例必须覆盖 pattern0 回避冲突",
        )

        mid_state = self.state(25)  # 盘中已打完 1 场
        self.assertEqual(mid_state.next_round_num(), 4)
        remaining = _pairs(mid_state.round_matches(4))
        played = SYNTH_SCRIPT[24]
        expected = [
            pair
            for pair in SYNTH_ROUND4_FULL
            if not (set(pair) == set(played))
        ]
        self.assertEqual(_normalized(remaining), _normalized(expected))
        flat = [team for pair in remaining for team in pair]
        self.assertEqual(len(flat), len(set(flat)))
        self.assertEqual(set(flat), _unplayed_names(mid_state, 4))

    def test_round_five_mid_round_keeps_locked_pairings(self) -> None:
        full = _pairs(self.state(30).round_matches(5))
        self.assertEqual(_normalized(full), _normalized(SYNTH_ROUND5_FULL))
        state = self.state(31)  # 第 5 轮已打完 1 场
        self.assertEqual(state.next_round_num(), 5)
        remaining = _pairs(state.round_matches(5))
        played = SYNTH_SCRIPT[30]
        expected = [pair for pair in SYNTH_ROUND5_FULL if not (set(pair) == set(played))]
        self.assertEqual(_normalized(remaining), _normalized(expected))
        self.assertEqual(set(team for pair in remaining for team in pair), _unplayed_names(state, 5))


class CppSimulatorConsistencyTest(unittest.TestCase):
    """把 ``tools/major_simulator.cpp`` 编到临时目录，与 Python 逐场对比。"""

    @classmethod
    def setUpClass(cls) -> None:
        cls.gpp = shutil.which("g++")
        cls.tmp = None
        cls.exe = None
        if not cls.gpp:
            return
        cls.tmp = tempfile.TemporaryDirectory()
        exe_path = Path(cls.tmp.name) / "major_simulator.exe"
        base = [cls.gpp, "-O2", "-std=c++17"]
        # Windows 上 PATH 里可能混入其它 libstdc++（如 GTK 运行时），静态链接更稳。
        commands = [
            base + ["-static-libstdc++", "-static-libgcc", "-o", str(exe_path), str(CPP_SOURCE)],
            base + ["-o", str(exe_path), str(CPP_SOURCE)],
        ]
        for command in commands:
            result = subprocess.run(command, capture_output=True, text=True)
            if result.returncode == 0:
                cls.exe = exe_path
                break
        if cls.exe is None:
            cls.tmp.cleanup()
            cls.tmp = None

    @classmethod
    def tearDownClass(cls) -> None:
        if cls.tmp is not None:
            cls.tmp.cleanup()

    def setUp(self) -> None:
        if self.exe is None:
            self.skipTest("g++ 不可用或编译失败")
        self.workdir = Path(self.tmp.name)

    def dump_pairs(
        self,
        team_names: list[str],
        seeds: list[int],
        finished_matches: list[tuple[str, str]],
        tag: str,
    ) -> tuple[int, list[tuple[int, int]]]:
        name_to_id = {name: index for index, name in enumerate(team_names)}
        input_path = self.workdir / f"{tag}.input.txt"
        output_path = self.workdir / f"{tag}.output.txt"
        lines = [f"1 1 0 {len(team_names)}"]
        for index, seed in enumerate(seeds):
            lines.append(f"{index} {seed}")
        for _ in range(len(team_names)):
            lines.append(" ".join("0.5" for _ in team_names))
        lines.append(str(len(finished_matches)))
        for winner, loser in finished_matches:
            lines.append(f"{name_to_id[winner]} {name_to_id[loser]}")
        input_path.write_text("\n".join(lines) + "\n", encoding="utf-8")

        result = subprocess.run(
            [str(self.exe), str(input_path), str(output_path), "--dump-first-round"],
            capture_output=True,
            text=True,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        dump_lines = [line for line in result.stdout.splitlines() if line.strip()]
        round_num = int(dump_lines[0].split("=", 1)[1])
        pairs = [tuple(int(value) for value in line.split()) for line in dump_lines[1:]]
        return round_num, pairs

    def python_pairs(self, teams, state) -> tuple[int, list[tuple[str, str]]]:
        round_num = state.next_round_num()
        return round_num, _pairs(state.round_matches(round_num))

    def assert_matches_python(self, teams, state, finished, tag: str) -> None:
        round_num, pairs = self.python_pairs(teams, state)
        python_names = [
            tuple(sorted((team_a, team_b))) for team_a, team_b in pairs
        ]
        cpp_round, cpp_pairs = self.dump_pairs(
            [team.name for team in teams],
            [team.seed for team in teams],
            finished,
            tag,
        )
        cpp_names = [
            tuple(sorted((teams[a].name, teams[b].name))) for a, b in cpp_pairs
        ]
        self.assertEqual(cpp_round, round_num, tag)
        self.assertEqual(sorted(cpp_names), sorted(python_names), tag)

    def test_singapore_all_prefixes_match_python(self) -> None:
        teams = GEN.load_teams(STAGE1)
        for k in (0, 8, 9, 12, 16, 17, 18, 19, 20, 21):
            state = _replay(teams, SINGAPORE_MATCHES, k)
            with self.subTest(k=k):
                self.assert_matches_python(teams, state, SINGAPORE_MATCHES[:k], f"singapore-{k}")

    def test_round_four_mid_round_matches_python(self) -> None:
        teams = [
            SIM.Team(id=index, name=name, seed=index + 1, rating=())
            for index, name in enumerate(SYNTH_TEAMS)
        ]
        state = _replay(teams, SYNTH_SCRIPT, 25)
        self.assert_matches_python(teams, state, SYNTH_SCRIPT[:25], "synthetic-r4")

    def test_round_five_mid_round_matches_python(self) -> None:
        teams = [
            SIM.Team(id=index, name=name, seed=index + 1, rating=())
            for index, name in enumerate(SYNTH_TEAMS)
        ]
        state = _replay(teams, SYNTH_SCRIPT, 31)
        self.assert_matches_python(teams, state, SYNTH_SCRIPT[:31], "synthetic-r5")


class ChartZeroRevivalTest(unittest.TestCase):
    @staticmethod
    def axes():
        from matplotlib.figure import Figure

        return Figure(figsize=(8, 5), dpi=100).add_subplot(1, 1, 1)

    @staticmethod
    def plot(points, *, starred: bool = False, is_log: bool = True):
        series = CHART.HomeworkSeries(uid="u1", name="测试玩家", points=points, starred=starred)
        axes = ChartZeroRevivalTest.axes()
        CHART._plot_series(axes, series, (0.1, 0.2, 0.3, 1.0), is_log=is_log, floor_value=1e-4)
        return axes

    @staticmethod
    def main_lines(axes):
        return [line for line in axes.lines if line.get_zorder() == 3]

    @staticmethod
    def helpers(axes):
        return [line for line in axes.lines if line.get_zorder() == 2]

    @staticmethod
    def zero_markers(axes):
        return [line for line in axes.lines if line.get_zorder() == 6]

    def assert_monotonic(self, axes) -> None:
        for line in axes.lines:
            xs = list(line.get_xdata())
            self.assertTrue(
                all(right >= left for left, right in zip(xs, xs[1:])),
                f"Line2D xdata 必须严格单调非递减：{xs}",
            )

    def test_single_zero_revival_splits_main_line(self) -> None:
        axes = self.plot([(0, 0.5), (1, 0.0), (2, 0.6)])
        main = self.main_lines(axes)
        self.assertEqual(len(main), 2, "归零复活必须拆成 2 段主线")
        self.assertEqual(list(main[0].get_xdata()), [0])
        self.assertEqual(list(main[1].get_xdata()), [2])
        self.assert_monotonic(axes)

        helpers = self.helpers(axes)
        self.assertEqual([list(line.get_xdata()) for line in helpers], [[0, 1], [1, 2]])
        self.assertEqual([list(line.get_ydata()) for line in helpers], [[0.5, 1e-4], [1e-4, 0.6]])
        for line in helpers:
            self.assertEqual(line._unscaled_dash_pattern, (0, (3, 2)))

        markers = self.zero_markers(axes)
        self.assertEqual(len(markers), 1)
        self.assertEqual(list(markers[0].get_xdata()), [1])
        self.assertEqual(list(markers[0].get_ydata()), [1e-4])

        _, labels = axes.get_legend_handles_labels()
        self.assertEqual(len(labels), 1)
        self.assertIn("测试玩家", labels[0])

    def test_multi_zero_revival_draws_bottom_helper(self) -> None:
        axes = self.plot([(0, 0.5), (1, 0.0), (2, 0.0), (3, 0.6)])
        self.assertEqual(len(self.main_lines(axes)), 2)
        helpers = self.helpers(axes)
        self.assertEqual(
            [list(line.get_xdata()) for line in helpers],
            [[0, 1], [1, 2], [2, 3]],
        )
        self.assertEqual(list(helpers[1].get_ydata()), [1e-4, 1e-4])
        self.assertEqual(len(self.zero_markers(axes)), 1)
        self.assert_monotonic(axes)

    def test_series_starts_at_zero_then_revives(self) -> None:
        axes = self.plot([(0, 0.0), (1, 0.0), (2, 0.5)])
        self.assertEqual(len(self.main_lines(axes)), 1)
        helpers = self.helpers(axes)
        self.assertEqual([list(line.get_xdata()) for line in helpers], [[1, 2]])
        markers = self.zero_markers(axes)
        self.assertEqual(len(markers), 1)
        self.assertEqual(list(markers[0].get_xdata()), [1])
        self.assert_monotonic(axes)

    def test_two_drops_mark_each_zero_without_tail(self) -> None:
        axes = self.plot([(0, 0.5), (1, 0.0), (2, 0.6), (3, 0.0)])
        self.assertEqual(len(self.main_lines(axes)), 2)
        helpers = self.helpers(axes)
        self.assertEqual(
            [list(line.get_xdata()) for line in helpers],
            [[0, 1], [1, 2], [2, 3]],
        )
        self.assertEqual(
            sorted(int(line.get_xdata()[0]) for line in self.zero_markers(axes)),
            [1, 3],
        )
        self.assert_monotonic(axes)

    def test_starred_revival_keeps_dash_and_single_legend_entry(self) -> None:
        axes = self.plot([(0, 0.5), (1, 0.0), (2, 0.6)], starred=True)
        main = self.main_lines(axes)
        self.assertEqual(len(main), 2)
        for line in main:
            self.assertEqual(line._unscaled_dash_pattern, (0, (6, 3)))
        _, labels = axes.get_legend_handles_labels()
        self.assertEqual(len(labels), 1)
        self.assertNotIn("🌟", labels[0])
        self.assertNotIn("★", labels[0])

    def test_all_zero_log_keeps_single_marker_legend(self) -> None:
        axes = self.plot([(0, 0.0), (1, 0.0)])
        _, labels = axes.get_legend_handles_labels()
        self.assertEqual(len(labels), 1)
        self.assertEqual(len(self.main_lines(axes)), 0)
        markers = self.zero_markers(axes)
        self.assertEqual(len(markers), 1)
        self.assertEqual(list(markers[0].get_xdata()), [0])

    def test_linear_mode_marks_every_drop(self) -> None:
        axes = self.plot([(0, 0.5), (1, 0.0), (2, 0.3), (3, 0.0)], is_log=False)
        markers = self.zero_markers(axes)
        self.assertEqual(sorted(int(line.get_xdata()[0]) for line in markers), [1, 3])
        for line in markers:
            self.assertEqual(list(line.get_ydata()), [0.0])

    def test_revived_series_renders_valid_png_in_both_scales(self) -> None:
        series = [
            CHART.HomeworkSeries(uid="u1", name="复活甲", points=[(0, 0.5), (1, 0.0), (2, 0.4)]),
            CHART.HomeworkSeries(uid="u2", name="稳定乙", points=[(0, 0.3), (1, 0.2), (2, 0.1)]),
        ]
        for scale in ("linear", "log"):
            png = CHART.render_homework_chart(series, [], scale=scale)
            self.assertEqual(png[:8], PNG_HEADER, scale)
            self.assertGreater(len(png), 10_000, scale)


class RecalculateScriptLogicTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.cwd = os.getcwd()
        try:
            spec = importlib.util.spec_from_file_location(
                "recalculate_major_stage_results_under_test", RECALC_SCRIPT
            )
            assert spec is not None and spec.loader is not None
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
        finally:
            os.chdir(cls.cwd)
        cls.module = module

    def test_parse_match_counts(self) -> None:
        parse = self.module.parse_match_counts
        self.assertEqual(parse("17,18,19,20", 21), [17, 18, 19, 20])
        self.assertEqual(parse("17-20", 21), [17, 18, 19, 20])
        self.assertEqual(parse(" 20, 17-18,18 ", 21), [17, 18, 20])
        self.assertEqual(parse("0", 0), [0])
        for bad in ("", "  ", "abc", "17-", "-1", "5-3", "17,,18", "22"):
            with self.subTest(spec=bad):
                with self.assertRaises(ValueError):
                    parse(bad, 21)

    def test_choose_simulation_engine(self) -> None:
        choose = self.module.choose_simulation_engine
        self.assertEqual(choose("python", 0, True), "python")
        self.assertEqual(choose("cpp", 0, True), "cpp")
        # 33 - 15 = 18 场，2**18 = 262144 > 200000 -> C++
        self.assertEqual(choose("auto", 15, True), "cpp")
        # 33 - 16 = 17 场，2**17 = 131072 <= 200000 -> Python 精确遍历
        self.assertEqual(choose("auto", 16, True), "python")
        self.assertEqual(choose("auto", 15, False), "python")
        self.assertEqual(
            choose("auto", 15, True, total_matches=33, max_exact_outcomes=300000), "python"
        )
        with self.assertRaises(ValueError):
            choose("gpu", 15, True)

    def test_parse_homework_text(self) -> None:
        parse = self.module.parse_homework_text
        dict_text = json.dumps(
            {
                "3-0": ["A", "B"],
                "3-1/3-2": ["C", "D", "E", "F", "G", "H"],
                "0-3": ["I", "J"],
            }
        )
        self.assertEqual(
            parse(dict_text),
            {
                "3-0": ["A", "B"],
                "3-1/3-2": ["C", "D", "E", "F", "G", "H"],
                "0-3": ["I", "J"],
            },
        )
        list_text = json.dumps([f"T{index}" for index in range(10)])
        self.assertEqual(
            parse(list_text),
            {
                "3-0": ["T0", "T1"],
                "3-1/3-2": ["T2", "T3", "T4", "T5", "T6", "T7"],
                "0-3": ["T8", "T9"],
            },
        )
        with self.assertRaises(ValueError):
            parse(json.dumps(["only", "three"]))
        with self.assertRaises(ValueError):
            parse("not json")


if __name__ == "__main__":
    unittest.main()
