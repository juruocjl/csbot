from __future__ import annotations

import argparse
import asyncio
import csv
import json
import os
from pathlib import Path
import subprocess
import sys

import nonebot


REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))
os.chdir(REPO_ROOT)

# 与 ``plugins/major_hw/simulate.py`` 完全同口径的常量；脚本本身在 nonebot
# 初始化前不能 import 插件包，因此这里复制默认值并读取同一个环境变量。
SWISS_TOTAL_MATCHES = 33
MAX_EXACT_OUTCOMES = int(os.getenv("MAJOR_EXACT_OUTCOMES", "200000"))

ENGINE_CHOICES = ("auto", "python", "cpp")


def init_nonebot(args: argparse.Namespace) -> None:
    overrides = {
        key: value
        for key, value in (
            ("cs_database", args.database),
            ("major_name", args.major_name),
            ("major_stage", args.major_stage),
            ("major_event_id", args.event_id),
        )
        if value is not None
    }
    if overrides:
        nonebot.init(**overrides)
    else:
        # No explicit overrides: load everything from the process environment
        # (ENVIRONMENT=prod + .env.prod) instead of passing CS_DATABASE on the CLI.
        nonebot.init()
    nonebot.load_plugin(Path("plugins") / "models")
    nonebot.load_plugin(Path("plugins") / "utils")
    nonebot.load_plugin(Path("plugins") / "major_hw")


def parse_match_counts(spec: str, max_count: int) -> list[int]:
    """把 ``17,18,19,20`` / ``17-20`` 解析为去重升序的场次列表。

    非法格式、空片段、``start > end`` 或越界（``< 0`` / ``> max_count``）一律
    抛 ``ValueError``，避免误重算不存在的快照。
    """

    if not isinstance(spec, str) or not spec.strip():
        raise ValueError("match counts spec is empty")
    if max_count < 0:
        raise ValueError(f"max_count must be >= 0, got {max_count}")

    counts: set[int] = set()
    for raw_part in spec.split(","):
        part = raw_part.strip()
        if not part:
            raise ValueError(f"invalid match counts spec: {spec!r}")
        if "-" in part:
            start_raw, _, end_raw = part.partition("-")
            try:
                start = int(start_raw)
                end = int(end_raw)
            except ValueError as exc:
                raise ValueError(f"invalid match count range: {part!r}") from exc
            if start > end:
                raise ValueError(f"invalid match count range: {part!r}")
            values = range(start, end + 1)
        else:
            try:
                values = [int(part)]
            except ValueError as exc:
                raise ValueError(f"invalid match count: {part!r}") from exc
        for value in values:
            if value < 0 or value > max_count:
                raise ValueError(
                    f"match count {value} out of range 0..{max_count}"
                )
            counts.add(value)
    return sorted(counts)


def choose_simulation_engine(
    requested: str,
    match_count: int,
    cpp_available: bool,
    *,
    total_matches: int = SWISS_TOTAL_MATCHES,
    max_exact_outcomes: int = MAX_EXACT_OUTCOMES,
) -> str:
    """决定某一场次用 Python 精确遍历还是 C++ 蒙特卡洛。

    ``auto`` 在剩余分支数不超过 ``max_exact_outcomes``（Python 会走
    ``total_weight = 1.0`` 的精确加权遍历）或没有 C++ 可执行文件时选 Python，
    否则交给 C++。
    """

    engine = str(requested or "auto").strip().lower()
    if engine not in ENGINE_CHOICES:
        raise ValueError(f"unknown simulation engine: {requested!r}")
    if engine == "python":
        return "python"
    if engine == "cpp":
        return "cpp"
    if not cpp_available:
        return "python"
    exact_outcomes = 2 ** max(0, total_matches - int(match_count))
    return "python" if exact_outcomes <= max_exact_outcomes else "cpp"


def parse_homework_text(homework_text: str) -> dict[str, list[str]]:
    """解析历史快照里的作业文本。

    兼容 ``{"3-0": [...], "3-1/3-2": [...], "0-3": [...]}`` 字典 JSON，也兼容
    ``major_hw`` 早期直接存储的 10 队列表 JSON（前 2 / 中 6 / 后 2）。
    """

    data = json.loads(homework_text)
    if isinstance(data, dict):
        return {
            "3-0": [str(team) for team in data.get("3-0", [])],
            "3-1/3-2": [str(team) for team in data.get("3-1/3-2", [])],
            "0-3": [str(team) for team in data.get("0-3", [])],
        }
    if isinstance(data, list) and len(data) == 10:
        return {
            "3-0": [str(team) for team in data[:2]],
            "3-1/3-2": [str(team) for team in data[2:8]],
            "0-3": [str(team) for team in data[8:]],
        }
    raise ValueError(f"unsupported homework text: {homework_text!r}")


async def load_existing_homework_rows(stage: str, match_count: int) -> list[tuple[str, str]]:
    """读取 ``(stage, match_count)`` 已存在的 ``(uid, homework_text)`` 快照行。"""

    from sqlalchemy import select

    from plugins.models import MajorHWSnapshot
    from plugins.utils import async_session_factory

    async with async_session_factory() as session:
        stmt = select(MajorHWSnapshot).where(
            MajorHWSnapshot.stage == stage,
            MajorHWSnapshot.match_count == match_count,
        )
        rows = list((await session.execute(stmt)).scalars().all())
    return [(row.uid, row.homework_text) for row in rows]


def prefix_matches(finished_matches: list, match_count: int) -> list:
    chronological = list(reversed(finished_matches))
    return list(reversed(chronological[:match_count]))


def load_stage_for_cpp(stage_path: Path) -> tuple[list[str], list[int], dict[str, str], bool]:
    data = json.loads(stage_path.read_text(encoding="utf-8-sig"))
    names: list[str] = []
    seeds: list[int] = []
    alias_to_name: dict[str, str] = {}
    for team_name, team_data in data["teams"].items():
        names.append(team_name)
        seeds.append(int(team_data["seed"]))
        alias_to_name[team_name] = team_name
        for alias in team_data.get("alias", []):
            alias_to_name[str(alias)] = team_name
    force_bo3 = data.get("match_format") == "bo3" or bool(data.get("all_bo3", False))
    return names, seeds, alias_to_name, force_bo3


def load_win_matrix_for_cpp(matrix_path: Path, team_names: list[str]) -> list[list[float]]:
    with matrix_path.open(newline="", encoding="utf-8") as file:
        rows = {row["Team"]: row for row in csv.DictReader(file)}
    matrix: list[list[float]] = []
    for team in team_names:
        row_values: list[float] = []
        for opponent in team_names:
            if team == opponent:
                row_values.append(0.0)
            else:
                value = rows[team][opponent]
                row_values.append(float(value))
        matrix.append(row_values)
    return matrix


def resolve_team_name(raw_name: str, alias_to_name: dict[str, str]) -> str:
    from plugins.major_hw.gen_win_matrix import resolve_team_name as resolve

    return resolve(raw_name, alias_to_name)


def write_cpp_input(
    path: Path,
    team_names: list[str],
    seeds: list[int],
    matrix: list[list[float]],
    current_matches: list,
    alias_to_name: dict[str, str],
    iterations: int,
    threads: int,
    force_bo3: bool,
) -> None:
    team_to_idx = {team: idx for idx, team in enumerate(team_names)}
    with path.open("w", encoding="utf-8", newline="\n") as file:
        file.write(f"{iterations} {threads} {1 if force_bo3 else 0} {len(team_names)}\n")
        for idx, (team, seed) in enumerate(zip(team_names, seeds)):
            file.write(f"{idx} {seed}\n")
        for row in matrix:
            file.write(" ".join(f"{value:.10f}" for value in row) + "\n")
        chronological = list(reversed(current_matches))
        file.write(f"{len(chronological)}\n")
        for match in chronological:
            winner = resolve_team_name(str(match[0]), alias_to_name)
            loser = resolve_team_name(str(match[1]), alias_to_name)
            file.write(f"{team_to_idx[winner]} {team_to_idx[loser]}\n")


def run_cpp_simulator(
    input_path: Path,
    output_path: Path,
    simulator_path: Path,
    team_names: list[str],
) -> None:
    subprocess.run(
        [str(simulator_path), str(input_path), str(output_path)],
        cwd=REPO_ROOT,
        check=True,
    )
    text = output_path.read_text(encoding="utf-8")
    lines = text.splitlines()
    if len(lines) < 1 or lines[0] != "# major_hw_result_format=2":
        raise ValueError(f"unexpected simulator output header in {output_path}")
    teams_header = f"# teams: {json.dumps(team_names, ensure_ascii=False, separators=(',', ':'))}"
    if len(lines) < 2 or not lines[1].startswith("# teams:"):
        lines.insert(1, teams_header)
    else:
        lines[1] = teams_header
    output_path.write_text("\n".join(lines) + "\n", encoding="utf-8", newline="\n")


def atomic_replace(target: Path, content: bytes) -> None:
    """先写同目录临时文件再 ``os.replace``，避免半截 ``result.txt``。"""

    tmp_path = target.with_name(target.name + ".tmp")
    tmp_path.write_bytes(content)
    os.replace(tmp_path, target)


async def recalculate(args: argparse.Namespace) -> None:
    import plugins.major_hw as major_hw
    from plugins.major_hw.gen_win_matrix import gen_win_matrix
    from plugins.major_hw.simulate import simulate
    from plugins.major_hw.verify import evaluate_combination, parse_simulation_results
    from plugins.utils import local_storage

    raw_matches = await local_storage.get(f"hltvresult{major_hw.config.major_event_id}", default="[]")
    finished_matches = json.loads(raw_matches)
    total_count = len(finished_matches)
    max_count = total_count
    if args.max_match_count is not None:
        max_count = min(max_count, args.max_match_count)
    target_counts = (
        parse_match_counts(args.match_counts, max_count)
        if args.match_counts
        else list(range(max_count + 1))
    )

    members = await major_hw.db.get_all_hw(major_hw.major_stage_name)
    team_names, seeds, alias_to_name, force_bo3 = load_stage_for_cpp(major_hw.teamfile)
    simulator_path = REPO_ROOT / "tools" / ("major_simulator.exe" if os.name == "nt" else "major_simulator")
    cpp_available = simulator_path.exists()
    if args.engine == "cpp" and not cpp_available:
        raise FileNotFoundError(f"missing compiled simulator: {simulator_path}")

    print(
        f"stage={major_hw.major_stage_name} event={major_hw.config.major_event_id} "
        f"members={len(members)} snapshots={total_count} "
        f"targets={target_counts} engine={args.engine} cpp_available={cpp_available}"
    )

    for match_count in target_counts:
        current_matches = prefix_matches(finished_matches, match_count)
        engine = choose_simulation_engine(args.engine, match_count, cpp_available)
        output_path = major_hw.simulation_result_dir / f".recalc-{match_count}.txt"
        output_path.parent.mkdir(parents=True, exist_ok=True)
        print(f"recalculating match_count={match_count} engine={engine} matches={len(current_matches)}")

        await asyncio.to_thread(
            gen_win_matrix,
            str(major_hw.teamfile),
            current_matches,
            True,
        )
        if engine == "python":
            # 精确遍历可行时由 simulate 内部自动选择 total_weight = 1.0 的加权遍历。
            await asyncio.to_thread(
                simulate,
                major_hw.teamfile,
                output_path,
                current_matches,
                True,
            )
        else:
            matrix = load_win_matrix_for_cpp(Path("win_matrix.csv"), team_names)
            cpp_input = (
                Path("data") / "major_simulations" / major_hw.major_stage_name / f"{match_count}.input.txt"
            )
            cpp_input.parent.mkdir(parents=True, exist_ok=True)
            write_cpp_input(
                cpp_input,
                team_names,
                seeds,
                matrix,
                current_matches,
                alias_to_name,
                args.iterations,
                args.threads,
                force_bo3,
            )
            await asyncio.to_thread(run_cpp_simulator, cpp_input, output_path, simulator_path, team_names)

        try:
            results, total_weight = parse_simulation_results(str(output_path))
            result_bytes = output_path.read_bytes()

            source_rows: list[tuple[str, str]]
            if args.preserve_existing_snapshots:
                source_rows = await load_existing_homework_rows(
                    major_hw.major_stage_name, match_count
                )
            else:
                source_rows = []
            if not source_rows:
                # 历史无快照（或未开启保留）时回退到当前全员列表。
                source_rows = [
                    (member.uid, major_hw.homework_teams_text(member.teams))
                    for member in members
                ]

            homework_rows: list[tuple[str, str, float, float]] = []
            for uid, homework_text in source_rows:
                combo = parse_homework_text(homework_text)
                _, winrate, expval = evaluate_combination(combo, results)
                homework_rows.append((uid, homework_text, winrate, expval))

            is_latest = match_count == total_count
            if is_latest:
                # 只有最新场次才原子替换根目录 result.txt 并更新当前作业表。
                atomic_replace(Path(major_hw.file_path), result_bytes)
                for uid, _, winrate, expval in homework_rows:
                    await major_hw.db.set_uid_val(uid, major_hw.major_stage_name, winrate, expval)

            result_path = major_hw.save_simulation_result_file(match_count, result_bytes)

            latest_match_id = None
            if current_matches and len(current_matches[0]) >= 4:
                latest_match_id = str(current_matches[0][3])

            await major_hw.db.save_simulation_snapshot(
                stage=major_hw.major_stage_name,
                event_id=major_hw.config.major_event_id,
                match_count=match_count,
                latest_match_id=latest_match_id,
                total_weight=total_weight,
                homework_rows=homework_rows,
                result_size=len(result_bytes),
            )
            print(
                f"saved match_count={match_count} total_weight={total_weight:g} "
                f"homework_rows={len(homework_rows)} file={result_path}"
                f"{' (latest: updated result.txt)' if is_latest else ''}"
            )
        finally:
            output_path.unlink(missing_ok=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Recalculate every saved Major stage result prefix.")
    parser.add_argument("--database", default=None, help="Override cs_database; omit to use .env.prod")
    parser.add_argument("--major-name", default=None, help="Override major_name; omit to use env config")
    parser.add_argument("--major-stage", default=None, help="Override major_stage; omit to use env config")
    parser.add_argument("--event-id", default=None, type=int, help="Override major_event_id; omit to use env config")
    parser.add_argument("--max-match-count", type=int)
    parser.add_argument(
        "--match-counts",
        default=None,
        help="Only recalculate these match counts, e.g. 17,18,19,20 or 17-20",
    )
    parser.add_argument(
        "--engine",
        choices=ENGINE_CHOICES,
        default="auto",
        help="auto: exact Python enumeration when affordable, otherwise C++ Monte Carlo",
    )
    parser.add_argument(
        "--preserve-existing-snapshots",
        action="store_true",
        help="Reuse existing (stage, match_count) snapshot rows; never fabricate early snapshots for later joiners",
    )
    parser.add_argument("--iterations", type=int, default=int(os.getenv("MAJOR_SIMULATION_ITERATIONS", "200000")))
    parser.add_argument("--threads", type=int, default=int(os.getenv("MAJOR_SIMULATION_CORES", "8")))
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    init_nonebot(args)
    asyncio.run(recalculate(args))


if __name__ == "__main__":
    main()
