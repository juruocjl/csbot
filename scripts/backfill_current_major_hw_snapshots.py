from __future__ import annotations

import asyncio
import json
from pathlib import Path
import sys
import time

import nonebot
from sqlalchemy import delete, select


REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

nonebot.init()
nonebot.load_plugin(Path("plugins") / "models")
nonebot.load_plugin(Path("plugins") / "utils")

from plugins.models import MajorHW, MajorHWSnapshot, MajorSimulationSnapshot
from plugins.utils import async_session_factory, local_storage


def homework_teams_text(teams_json: str) -> str:
    teams = json.loads(teams_json)
    if len(teams) == 10:
        normalized = {
            "3-0": sorted(teams[:2]),
            "3-1/3-2": sorted(teams[2:8]),
            "0-3": sorted(teams[8:]),
        }
    else:
        normalized = teams
    return json.dumps(normalized, ensure_ascii=False, separators=(",", ":"))


async def main() -> None:
    config = nonebot.get_driver().config
    stage = f"{config.major_name}-{config.major_stage}"
    event_id = int(config.major_event_id)
    finished_matches = json.loads(await local_storage.get(f"hltvresult{event_id}", default="[]"))
    match_count = len(finished_matches)
    latest_match_id = None
    if finished_matches and len(finished_matches[0]) >= 4:
        latest_match_id = str(finished_matches[0][3])

    created_at = int(time.time())
    async with async_session_factory() as session:
        async with session.begin():
            stmt = select(MajorHW).where(MajorHW.stage == stage)
            result = await session.execute(stmt)
            members = list(result.scalars().all())

            existing_snapshot = await session.get(MajorSimulationSnapshot, (stage, match_count))
            total_weight = existing_snapshot.total_weight if existing_snapshot else 0.0

            await session.execute(
                delete(MajorHWSnapshot)
                .where(MajorHWSnapshot.stage == stage)
                .where(MajorHWSnapshot.match_count == match_count)
            )
            await session.merge(MajorSimulationSnapshot(
                stage=stage,
                match_count=match_count,
                event_id=event_id,
                latest_match_id=latest_match_id,
                created_at=created_at,
                total_weight=total_weight,
                result_size=0,
                result_gzip=b"",
            ))
            for member in members:
                await session.merge(MajorHWSnapshot(
                    stage=stage,
                    match_count=match_count,
                    uid=member.uid,
                    homework_text=homework_teams_text(member.teams),
                    created_at=created_at,
                    winrate=member.winrate,
                    expval=member.expval,
                ))

    print(f"backfilled stage={stage} match_count={match_count} homework_rows={len(members)}")


if __name__ == "__main__":
    asyncio.run(main())
