from __future__ import annotations

import asyncio
import os
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


def _progress_bar(current: int, total: int | None, width: int = 28) -> str:
    if not total:
        return "[" + "." * width + "]"
    current = min(current, total)
    filled = int(width * current / total)
    return "[" + "#" * filled + "." * (width - filled) + "]"


def _make_progress_printer(started: float):
    last_print: dict[str, float] = {}

    def print_progress(stage: str, current: int, total: int | None) -> None:
        now = time.monotonic()
        done = total is not None and current >= total
        if not done and current != 0 and now - last_print.get(stage, 0) < 1:
            return
        last_print[stage] = now
        if total:
            percent = current / total * 100
            progress = f"{current}/{total} {percent:5.1f}%"
        else:
            progress = f"{current}/?"
        elapsed = now - started
        print(f"{stage:12} {_progress_bar(current, total)} {progress} elapsed={elapsed:.1f}s", flush=True)

    return print_progress


async def main() -> None:
    batch_size = int(os.getenv("CHAT_HISTORY_BACKFILL_BATCH_SIZE", "1000"))
    started = time.monotonic()
    progress = _make_progress_printer(started)
    print(f"chat history backfill: indexing messages, batch_size={batch_size}", flush=True)
    indexed = await chat_history_db.rebuild_all_message_indexes(
        batch_size=batch_size,
        progress_callback=progress,
    )
    print("chat history backfill: rebuilding chunks and spans", flush=True)
    await chat_history_db.rebuild_all_group_indexes(progress_callback=progress)
    elapsed = time.monotonic() - started
    print(f"chat history backfill complete: indexed {indexed} messages in {elapsed:.1f}s")


if __name__ == "__main__":
    asyncio.run(main())
