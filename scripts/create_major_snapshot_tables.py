from __future__ import annotations

import asyncio
from pathlib import Path
import sys

import nonebot
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine


REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

nonebot.init()
nonebot.load_plugin(Path("plugins") / "models")

from plugins.models import MajorHWSnapshot, MajorSimulationSnapshot


async def main() -> None:
    database_url = nonebot.get_driver().config.cs_database
    engine = create_async_engine(database_url, pool_pre_ping=True, pool_recycle=3600)
    async with engine.begin() as connection:
        await connection.run_sync(MajorSimulationSnapshot.__table__.create, checkfirst=True)
        await connection.run_sync(MajorHWSnapshot.__table__.create, checkfirst=True)
        await connection.execute(text(
            "ALTER TABLE major_hw_snapshots "
            "ADD COLUMN IF NOT EXISTS homework_text TEXT NOT NULL DEFAULT ''"
        ))
        await connection.execute(text("""
            DO $$
            BEGIN
                IF EXISTS (
                    SELECT 1
                    FROM information_schema.columns
                    WHERE table_name = 'major_hw_snapshots'
                      AND column_name = 'teams_hash'
                ) THEN
                    UPDATE major_hw_snapshots
                    SET homework_text = teams_hash
                    WHERE homework_text = '';
                END IF;

                IF EXISTS (
                    SELECT 1
                    FROM pg_constraint
                    WHERE conrelid = 'major_hw_snapshots'::regclass
                      AND conname = 'major_hw_snapshots_pkey'
                ) THEN
                    ALTER TABLE major_hw_snapshots DROP CONSTRAINT major_hw_snapshots_pkey;
                END IF;
                ALTER TABLE major_hw_snapshots
                    ADD CONSTRAINT major_hw_snapshots_pkey
                    PRIMARY KEY (stage, match_count, uid, homework_text);
            END $$;
        """))
        await connection.execute(text(
            "ALTER TABLE major_hw_snapshots DROP COLUMN IF EXISTS teams_hash"
        ))
    await engine.dispose()
    print("major snapshot tables are ready")


if __name__ == "__main__":
    asyncio.run(main())
