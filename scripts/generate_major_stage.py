from __future__ import annotations

import argparse
import asyncio
import importlib.util
import json
import sys
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))


def load_rating_module():
    module_path = REPO_ROOT / "plugins" / "major_hw" / "gen_win_matrix.py"
    spec = importlib.util.spec_from_file_location("major_stage_rating", module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"failed to load {module_path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


rating_module = load_rating_module()


def load_json(path: Path) -> Any:
    with path.open(encoding="utf-8-sig") as file:
        return json.load(file)


def dump_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="\n") as file:
        json.dump(data, file, ensure_ascii=False, indent=2)
        file.write("\n")


def normalize_matches(data: Any) -> list[tuple[str, str, str, str]]:
    if isinstance(data, dict):
        data = data.get("games", data.get("matches", data.get("results", [])))
    if not isinstance(data, list):
        raise ValueError("match results must be a JSON list, or an object with games/matches/results")

    matches: list[tuple[str, str, str, str]] = []
    for index, item in enumerate(data):
        if not isinstance(item, (list, tuple)) or len(item) < 2:
            raise ValueError(f"match #{index + 1} must contain at least winner and loser")
        winner = str(item[0])
        loser = str(item[1])
        score = str(item[2]) if len(item) > 2 else "1:0"
        match_id = str(item[3]) if len(item) > 3 else ""
        matches.append((winner, loser, score, match_id))
    return matches


async def load_storage_matches(event_id: int) -> list[tuple[str, str, str, str]]:
    import nonebot

    nonebot.init()
    nonebot.load_plugin(Path("plugins") / "models")
    nonebot.load_plugin(Path("plugins") / "utils")

    from plugins.utils import local_storage

    raw = await local_storage.get(f"hltvresult{event_id}", default="[]")
    return normalize_matches(json.loads(raw))


def build_alias_map(teams) -> dict[str, str]:
    alias2full: dict[str, str] = {}
    for team in teams:
        alias2full[team.name] = team.name
        for alias in team.alias:
            alias2full[alias] = team.name
    return alias2full


def format_rating(value: float) -> int:
    return int(round(value))


def generate_stage(args: argparse.Namespace) -> dict[str, Any]:
    previous_path = Path(args.previous)
    template_path = Path(args.template)

    previous_data = load_json(previous_path)
    next_data = load_json(template_path)
    previous_teams = rating_module.load_teams(previous_path)
    system_names = rating_module.load_system_names(previous_path)
    alias2full = build_alias_map(previous_teams)

    rolled_teams = rating_module.apply_finished_matches_to_ratings(
        previous_teams,
        system_names,
        args.matches,
        alias2full,
        newest_first=args.newest_first,
    )
    rolled_by_name = {team.name: team for team in rolled_teams}
    previous_team_data = previous_data.get("teams", {})

    next_data.setdefault("systems", previous_data.get("systems", {}))
    next_teams = next_data.get("teams")
    if not isinstance(next_teams, dict):
        raise ValueError("template must contain a teams object")

    updated: list[str] = []
    kept: list[str] = []
    for team_name, team_data in next_teams.items():
        if team_name in rolled_by_name:
            rolled = rolled_by_name[team_name]
            for idx, system_name in enumerate(system_names):
                team_data[system_name] = format_rating(rolled.rating[idx])
            if "alias" not in team_data:
                team_data["alias"] = previous_team_data.get(team_name, {}).get("alias", [])
            updated.append(team_name)
            continue

        missing = [system for system in system_names if system not in team_data]
        if missing:
            missing_text = ", ".join(missing)
            raise ValueError(f"{team_name} is not in previous stage and is missing: {missing_text}")
        team_data.setdefault("alias", [])
        kept.append(team_name)

    print(f"updated_from_previous={len(updated)}: {', '.join(updated)}")
    print(f"kept_from_template={len(kept)}: {', '.join(kept)}")
    return next_data


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate a new Major stage JSON by carrying rolled ratings from the previous stage.",
    )
    parser.add_argument("--previous", required=True, help="Previous stage asset JSON path.")
    parser.add_argument("--template", required=True, help="Next stage template JSON path.")
    parser.add_argument("--output", required=True, help="Output JSON path.")
    parser.add_argument("--results", help="Finished match results JSON path.")
    parser.add_argument("--from-storage", type=int, help="Read hltvresult{event_id} from bot storage.")
    parser.add_argument(
        "--oldest-first",
        dest="newest_first",
        action="store_false",
        help="Treat result JSON as oldest-to-newest. Default matches hltvresult storage: newest-to-oldest.",
    )
    parser.set_defaults(newest_first=True)
    args = parser.parse_args()

    if bool(args.results) == bool(args.from_storage):
        parser.error("pass exactly one of --results or --from-storage")
    return args


def main() -> None:
    args = parse_args()
    if args.results:
        args.matches = normalize_matches(load_json(Path(args.results)))
    else:
        args.matches = asyncio.run(load_storage_matches(args.from_storage))

    output = generate_stage(args)
    dump_json(Path(args.output), output)
    print(f"wrote={Path(args.output)}")


if __name__ == "__main__":
    main()
