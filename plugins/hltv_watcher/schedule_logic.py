"""Provider parsing shared by the schedule, discovery and result notifications."""
from __future__ import annotations

import json
from typing import Any, Mapping

from .logic import team_ref

# Explicit search aliases; substring matches are never used to bind a team.
ALIASES = {"tyloo": ("天禄",), "lvg": ("Lynn Vision",),
           "bcgame": ("bcg", "BC.Game"), "navi": ("Natus Vincere",)}



def number(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def score(value: Any) -> int | None:
    parsed = number(value, -1)
    return parsed if parsed >= 0 else None


def parse_schedule_matches(event_id: int, rows: Any) -> list[dict]:
    matches = {}
    for raw in rows if isinstance(rows, list) else []:
        if not isinstance(raw, Mapping):
            continue
        event = raw.get("tt_info") or {}
        if not isinstance(event, Mapping):
            continue
        if event.get("id") != f"csgo_tt_{event_id}":
            continue  # Empty tournaments can return featured matches from other events.
        info, state = raw.get("mc_info") or {}, raw.get("state") or {}
        if not isinstance(info, Mapping) or not isinstance(state, Mapping):
            continue
        match_id = str(info.get("id") or "")
        if not match_id:
            continue
        status = {0: "upcoming", 1: "live", 2: "finished"}.get(number(state.get("status"), -1), "unknown")
        score1, score2 = score(state.get("t1_score")), score(state.get("t2_score"))
        if status == "finished" and (score1 is None or score2 is None):
            status = "unknown"  # Incomplete results must never generate a fabricated 0:0 notification.
        sides = []
        for side in ("t1_info", "t2_info"):
            source = info.get(side) or {}
            sides.append({**(team_ref(source) or {"external_id": None, "logo": ""}),
                          "name": str(source.get("disp_name") or "待定")})
        maps = [{"name": str(item.get("map_name") or "待定"),
                 "status": {0: "upcoming", 1: "live", 2: "finished"}.get(number(item.get("status"), -1), "unknown"),
                 "score1": score(item.get("t1_score")), "score2": score(item.get("t2_score"))}
                for item in state.get("bout_states") or [] if isinstance(item, Mapping)]
        matches[match_id] = {"id": match_id, "event_id": event_id,
            "event_name": str(event.get("disp_name") or ""), "stage": str(info.get("tt_stage_desc") or ""),
            "start_ts": number(info.get("plan_ts")), "format": number(info.get("format")),
            "status": status, "team1": sides[0], "team2": sides[1],
            "score1": score1 if status in {"live", "finished"} else None,
            "score2": score2 if status in {"live", "finished"} else None,
            "maps": maps}
    return list(matches.values())


def result_key(match: dict) -> str:
    return json.dumps([match["team1"].get("team_id"), match["team2"].get("team_id"),
                       match["score1"], match["score2"], match["maps"]], sort_keys=True)


def legacy_results(matches: list[dict]) -> list[tuple[str, str, str, str]]:
    rows = []
    for match in sorted(matches, key=lambda m: (m["start_ts"], m["id"])):
        if match["status"] != "finished":
            continue
        a, b, sa, sb = match["team1"]["name"], match["team2"]["name"], match["score1"], match["score2"]
        if sb > sa:
            a, b, sa, sb = b, a, sb, sa
        rows.append((a, b, f"{sa}:{sb}", match["id"]))
    return rows
