from __future__ import annotations

import argparse
import asyncio
import gzip
import json
import os
from pathlib import Path
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


async def apply_results(args: argparse.Namespace) -> None:
    import plugins.major_hw as major_hw
    from plugins.major_hw.verify import evaluate_combination, parse_simulation_results
    from plugins.utils import local_storage

    result_dir = Path(args.result_dir)
    raw_matches = await local_storage.get(f"hltvresult{major_hw.config.major_event_id}", default="[]")
    finished_matches = json.loads(raw_matches)
    current_match_count = len(finished_matches)

    members = await major_hw.db.get_all_hw(major_hw.major_stage_name)
    print(
        f"stage={major_hw.major_stage_name} event={major_hw.config.major_event_id} "
        f"members={len(members)} current_match_count={current_match_count}"
    )

    for gzip_path in sorted(result_dir.glob("*.txt.gz"), key=lambda path: int(path.name.split(".", 1)[0])):
        match_count = int(gzip_path.name.split(".", 1)[0])
        if args.max_match_count is not None and match_count > args.max_match_count:
            continue

        result_bytes = gzip.decompress(gzip_path.read_bytes())
        temp_path = result_dir / f".apply-{match_count}.txt"
        temp_path.write_bytes(result_bytes)
        try:
            results, total_weight = parse_simulation_results(str(temp_path))
        finally:
            temp_path.unlink(missing_ok=True)

        current_matches = prefix_matches(finished_matches, match_count)
        latest_match_id = None
        if current_matches and len(current_matches[0]) >= 4:
            latest_match_id = str(current_matches[0][3])

        homework_rows: list[tuple[str, str, float, float]] = []
        for member in members:
            teams = json.loads(member.teams)
            combo = {
                "3-0": teams[:2],
                "3-1/3-2": teams[2:8],
                "0-3": teams[8:],
            }
            _, winrate, expval = evaluate_combination(combo, results)
            homework_text = major_hw.homework_teams_text(member.teams)
            homework_rows.append((member.uid, homework_text, winrate, expval))
            if match_count == current_match_count:
                await major_hw.db.set_uid_val(member.uid, major_hw.major_stage_name, winrate, expval)

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
            f"applied match_count={match_count} total_weight={total_weight:g} "
            f"homework_rows={len(homework_rows)} file={gzip_path}"
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Apply precomputed Major simulation result files to the database.")
    parser.add_argument("--database", required=True)
    parser.add_argument("--major-name", required=True)
    parser.add_argument("--major-stage", required=True)
    parser.add_argument("--event-id", required=True, type=int)
    parser.add_argument("--result-dir", required=True)
    parser.add_argument("--max-match-count", type=int)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    init_nonebot(args)
    asyncio.run(apply_results(args))


if __name__ == "__main__":
    main()
