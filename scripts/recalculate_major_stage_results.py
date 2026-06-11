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


def init_nonebot(args: argparse.Namespace) -> None:
    nonebot.init(
        driver=os.getenv("DRIVER", "~fastapi+~httpx"),
        superusers=set(json.loads(os.getenv("SUPERUSERS", "[]"))),
        cs_database=args.database,
        major_name=args.major_name,
        major_stage=args.major_stage,
        major_event_id=args.event_id,
        cs_group_list=json.loads(os.getenv("CS_GROUP_LIST", "[]")),
        cs_season_id=os.getenv("CS_SEASON_ID", "S21"),
        cs_last_season_id=os.getenv("CS_LAST_SEASON_ID", "S20"),
        cs_mysteam_id=int(os.getenv("CS_MYSTEAM_ID", "0")),
        cs_wmtoken=os.getenv("CS_WMTOKEN", "debug"),
        cs_ai_url=os.getenv("CS_AI_URL", "http://127.0.0.1"),
        cs_ai_api_key=os.getenv("CS_AI_API_KEY", "debug"),
        cs_ai_model=os.getenv("CS_AI_MODEL", "debug"),
        cs_botid=int(os.getenv("CS_BOTID", "0")),
        onebot_access_token=os.getenv("ONEBOT_ACCESS_TOKEN", "debug"),
    )
    nonebot.load_plugin(Path("plugins") / "models")
    nonebot.load_plugin(Path("plugins") / "utils")
    nonebot.load_plugin(Path("plugins") / "major_hw")


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
    if raw_name in alias_to_name:
        return alias_to_name[raw_name]
    lowered = raw_name.lower()
    for alias, team_name in alias_to_name.items():
        if alias.lower() == lowered:
            return team_name
    raise ValueError(f"unknown team name in finished matches: {raw_name}")


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


async def recalculate(args: argparse.Namespace) -> None:
    import plugins.major_hw as major_hw
    from plugins.major_hw.gen_win_matrix import gen_win_matrix
    from plugins.major_hw.verify import evaluate_combination, parse_simulation_results
    from plugins.utils import local_storage

    raw_matches = await local_storage.get(f"hltvresult{major_hw.config.major_event_id}", default="[]")
    finished_matches = json.loads(raw_matches)
    max_count = len(finished_matches)
    if args.max_match_count is not None:
        max_count = min(max_count, args.max_match_count)

    members = await major_hw.db.get_all_hw(major_hw.major_stage_name)
    team_names, seeds, alias_to_name, force_bo3 = load_stage_for_cpp(major_hw.teamfile)
    simulator_path = REPO_ROOT / "tools" / ("major_simulator.exe" if os.name == "nt" else "major_simulator")
    if not simulator_path.exists():
        raise FileNotFoundError(f"missing compiled simulator: {simulator_path}")

    print(
        f"stage={major_hw.major_stage_name} event={major_hw.config.major_event_id} "
        f"members={len(members)} snapshots=0..{max_count}"
    )

    for match_count in range(max_count + 1):
        current_matches = prefix_matches(finished_matches, match_count)
        print(f"recalculating match_count={match_count} matches={current_matches}")

        await asyncio.to_thread(
            gen_win_matrix,
            str(major_hw.teamfile),
            current_matches,
            True,
        )
        matrix = load_win_matrix_for_cpp(Path("win_matrix.csv"), team_names)
        cpp_input = Path("data") / "major_simulations" / major_hw.major_stage_name / f"{match_count}.input.txt"
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
        run_cpp_simulator(cpp_input, Path(major_hw.file_path), simulator_path, team_names)

        results, total_weight = parse_simulation_results(major_hw.file_path)
        result_bytes = Path(major_hw.file_path).read_bytes()
        result_path = major_hw.save_simulation_result_file(match_count, result_bytes)

        homework_rows: list[tuple[str, str, float, float]] = []
        for member in members:
            teams = json.loads(member.teams)
            combo = {
                "3-0": teams[:2],
                "3-1/3-2": teams[2:8],
                "0-3": teams[8:],
            }
            _, winrate, expval = evaluate_combination(combo, results)
            homework_rows.append((
                member.uid,
                major_hw.homework_teams_text(member.teams),
                winrate,
                expval,
            ))
            if match_count == max_count:
                await major_hw.db.set_uid_val(member.uid, major_hw.major_stage_name, winrate, expval)

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
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Recalculate every saved Major stage result prefix.")
    parser.add_argument("--database", required=True)
    parser.add_argument("--major-name", required=True)
    parser.add_argument("--major-stage", required=True)
    parser.add_argument("--event-id", required=True, type=int)
    parser.add_argument("--max-match-count", type=int)
    parser.add_argument("--iterations", type=int, default=int(os.getenv("MAJOR_SIMULATION_ITERATIONS", "200000")))
    parser.add_argument("--threads", type=int, default=int(os.getenv("MAJOR_SIMULATION_CORES", "8")))
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    init_nonebot(args)
    asyncio.run(recalculate(args))


if __name__ == "__main__":
    main()
