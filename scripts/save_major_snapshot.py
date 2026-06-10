from __future__ import annotations

import asyncio
import json
from pathlib import Path
import sys

import nonebot


REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

nonebot.init()
nonebot.load_plugin(Path("plugins") / "models")
nonebot.load_plugin(Path("plugins") / "utils")
nonebot.load_plugin(Path("plugins") / "major_hw")

import plugins.major_hw as major_hw
from plugins.utils import local_storage
from plugins.major_hw.verify import parse_simulation_results


async def main() -> None:
    finished_matches = json.loads(
        await local_storage.get(f"hltvresult{major_hw.config.major_event_id}", default="[]")
    )
    results, total_weight = parse_simulation_results(major_hw.file_path)
    major_hw.results = results

    homework_rows: list[tuple[str, float, float]] = []
    members = await major_hw.db.get_all_hw(major_hw.major_stage_name)
    for member in members:
        calc_result = await major_hw.calc_val(member.uid)
        if calc_result is not None:
            winrate, expval = calc_result
            homework_rows.append((member.uid, winrate, expval))

    latest_match_id = None
    if finished_matches and len(finished_matches[0]) >= 4:
        latest_match_id = str(finished_matches[0][3])

    await major_hw.db.save_simulation_snapshot(
        stage=major_hw.major_stage_name,
        event_id=major_hw.config.major_event_id,
        match_count=len(finished_matches),
        latest_match_id=latest_match_id,
        total_weight=total_weight,
        result_path=major_hw.file_path,
        homework_rows=homework_rows,
    )
    print(
        f"saved stage={major_hw.major_stage_name} "
        f"match_count={len(finished_matches)} homework_rows={len(homework_rows)}"
    )


if __name__ == "__main__":
    asyncio.run(main())
