"""Read-only Douyu smoke check; does not load the bot, access DBs, or send messages."""

import argparse
import asyncio
import importlib.util
import json
from pathlib import Path
import sys

import aiohttp


SPEC = importlib.util.spec_from_file_location(
    "douyu_status", Path(__file__).parents[1] / "plugins" / "live_watcher" / "douyu.py",
)
DOUYU = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = DOUYU
SPEC.loader.exec_module(DOUYU)


async def probe(liveids, expectations):
    failed = False
    async with aiohttp.ClientSession() as session:
        for liveid in liveids:
            try:
                result = await DOUYU.fetch_douyu_status(session, liveid)
                state = "replay" if result.is_replay else "live" if result.islive else "offline"
                print(json.dumps({"liveid": liveid, "nickname": result.nickname,
                                  "state": state, "islive": result.islive}, ensure_ascii=False))
                if liveid in expectations and state != expectations[liveid]:
                    print(f"unexpected state for {liveid}: expected {expectations[liveid]}", file=sys.stderr)
                    failed = True
            except Exception as error:
                print(f"{liveid}: {type(error).__name__}: {error}", file=sys.stderr)
                failed = True
    return int(failed)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("liveids", nargs="+")
    parser.add_argument("--expect", action="append", default=[], metavar="dy_ID=live|offline|replay")
    args = parser.parse_args()
    expectations = {}
    for item in args.expect:
        liveid, separator, state = item.partition("=")
        if not separator or liveid not in args.liveids or state not in {"live", "offline", "replay"}:
            parser.error("--expect requires a requested room and live/offline/replay")
        expectations[liveid] = state
    raise SystemExit(asyncio.run(probe(args.liveids, expectations)))
