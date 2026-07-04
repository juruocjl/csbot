from __future__ import annotations

import asyncio
from pathlib import Path
import sys
import time

import nonebot


REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

nonebot.init()
nonebot.load_plugin(Path("plugins") / "models")
nonebot.load_plugin(Path("plugins") / "utils")
nonebot.load_plugin(Path("plugins") / "chat_history")

from plugins.chat_history import db as chat_history_db


def main_progress(started: float):
    def progress(stage: str, current: int, total: int | None) -> None:
        elapsed = time.monotonic() - started
        if total:
            print(f"{stage} {current}/{total} elapsed={elapsed:.1f}s", flush=True)
        else:
            print(f"{stage} {current}/? elapsed={elapsed:.1f}s", flush=True)

    return progress


async def main() -> None:
    started = time.monotonic()
    print("chat history group rebuild: rebuilding lexicons, chunks, and spans", flush=True)
    await chat_history_db.rebuild_all_group_indexes(progress_callback=main_progress(started))
    elapsed = time.monotonic() - started
    print(f"chat history group rebuild complete in {elapsed:.1f}s", flush=True)


if __name__ == "__main__":
    asyncio.run(main())
