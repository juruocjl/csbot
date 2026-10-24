"""Pure helpers for the 5E event watcher.

The functions in this module deliberately do not import nonebot, aiohttp or the
database layer.  They are covered by ``checks/hltv_watcher_logic_test.py`` with
recorded API payloads, so the notification rules can be verified without
touching 5EPlay.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Iterable, Mapping, Sequence
from zoneinfo import ZoneInfo


SHANGHAI = ZoneInfo("Asia/Shanghai")

# 5EPlay event grades: 1 = Major, 2 = S, 7 = S+.  Grade labels are used as a
# fallback for payloads where ``grade`` is missing or has a new value.
MAJOR_TIER_GRADES = frozenset({"1", "2", "7"})
MAJOR_TIER_LABEL_PARTS = ("major", "s级", "s+")

DEFAULT_FOLLOWED_TEAMS = ("TYLOO", "LVG", "FaZe", "BC.Game")

# A just-finished event is kept for a short while so the last result can still
# be picked up, then it is removed from the watch list.
FINISHED_EVENT_GRACE_SECONDS = 2 * 60 * 60

# Look back far enough to catch events that started before the current window
# but are still running.  A one-day lookback silently dropped long events such
# as a four-day C-tier event that started before the watcher was deployed.
DISCOVERY_LOOKBACK_DAYS = 30
DISCOVERY_LOOKAHEAD_DAYS = 30

Match = tuple[str, str, str, str]


def compact(value: str) -> str:
    """Case-fold and strip punctuation/spaces for tolerant name matching."""
    return "".join(ch for ch in str(value).casefold() if ch.isalnum())


def discovery_time_window(now: datetime) -> tuple[str, str]:
    """Return the 5E event-list time window as ``start_end`` strings."""
    start = (now - timedelta(days=DISCOVERY_LOOKBACK_DAYS)).strftime(
        "%Y-%m-%d 00:00:00"
    )
    end = (now + timedelta(days=DISCOVERY_LOOKAHEAD_DAYS)).strftime(
        "%Y-%m-%d 23:59:59"
    )
    return start, end


def parse_event_id(value: Any) -> int | None:
    match = re.fullmatch(r"csgo_tt_(\d+)", str(value or "").strip())
    if match is None:
        return None
    event_id = int(match.group(1))
    return event_id if event_id > 0 else None


def parse_event_time(value: Any) -> int:
    """Parse the China-local timestamps returned by 5EPlay into epoch seconds."""
    text = str(value or "").strip()
    if not text:
        return 0
    parsed: datetime | None = None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
        try:
            parsed = datetime.strptime(text, fmt)
            break
        except ValueError:
            continue
    if parsed is None:
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError:
            return 0
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=SHANGHAI)
    return int(parsed.timestamp())


@dataclass(frozen=True)
class EventInfo:
    event_id: int
    name: str
    grade: str = ""
    grade_label: str = ""
    special_grade_label: str = ""
    status: str = ""
    start_ts: int = 0
    end_ts: int = 0
    teams: tuple[str, ...] = ()

    @property
    def tier_label(self) -> str:
        return self.special_grade_label or self.grade_label or "赛事"

    @property
    def is_major_tier(self) -> bool:
        label = f"{self.grade_label} {self.special_grade_label}".casefold()
        if self.grade in MAJOR_TIER_GRADES:
            return True
        return any(part in label for part in MAJOR_TIER_LABEL_PARTS)

    def to_dict(self) -> dict[str, Any]:
        return {
            "event_id": self.event_id,
            "name": self.name,
            "grade": self.grade,
            "grade_label": self.grade_label,
            "special_grade_label": self.special_grade_label,
            "status": self.status,
            "start_ts": self.start_ts,
            "end_ts": self.end_ts,
            "teams": list(self.teams),
        }

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> "EventInfo":
        event_id = int(raw.get("event_id") or 0)
        if event_id <= 0:
            raise ValueError("event_id 必须为正整数")
        teams = raw.get("teams") or []
        return cls(
            event_id=event_id,
            name=str(raw.get("name") or "").strip(),
            grade=str(raw.get("grade") or "").strip(),
            grade_label=str(raw.get("grade_label") or "").strip(),
            special_grade_label=str(raw.get("special_grade_label") or "").strip(),
            status=str(raw.get("status") or "").strip(),
            start_ts=int(raw.get("start_ts") or 0),
            end_ts=int(raw.get("end_ts") or 0),
            teams=tuple(str(team).strip() for team in teams if str(team).strip()),
        )


def event_from_api(raw: Mapping[str, Any]) -> EventInfo | None:
    """Convert one item from ``csgo_event_list_v1`` into a compact event."""
    basic = raw.get("basic_info") or {}
    if not isinstance(basic, Mapping):
        return None
    event_id = parse_event_id(basic.get("id"))
    if event_id is None:
        return None
    teams: list[str] = []
    for item in raw.get("teams") or []:
        if not isinstance(item, Mapping):
            continue
        name = str(item.get("name") or "").strip()
        if name and name not in teams:
            teams.append(name)
    return EventInfo(
        event_id=event_id,
        name=str(basic.get("disp_name") or "").strip(),
        grade=str(basic.get("grade") or "").strip(),
        grade_label=str(basic.get("grade_label") or "").strip(),
        special_grade_label=str(basic.get("special_grade_label") or "").strip(),
        status=str(basic.get("status") or "").strip(),
        start_ts=parse_event_time(basic.get("start_time")),
        end_ts=parse_event_time(basic.get("end_time")),
        teams=tuple(teams),
    )


def parse_event_list(payload: Mapping[str, Any]) -> list[EventInfo]:
    data = payload.get("data")
    if not isinstance(data, Mapping):
        return []
    items = data.get("items")
    if not isinstance(items, list):
        return []
    events: list[EventInfo] = []
    for item in items:
        if not isinstance(item, Mapping):
            continue
        event = event_from_api(item)
        if event is not None:
            events.append(event)
    return events


def events_to_json(events: Iterable[EventInfo]) -> str:
    return json.dumps(
        [event.to_dict() for event in events],
        ensure_ascii=False,
        separators=(",", ":"),
    )


def events_from_json(raw: str | None) -> dict[int, EventInfo]:
    if not raw:
        return {}
    parsed = json.loads(raw)
    if not isinstance(parsed, list):
        raise ValueError("赛事缓存必须是 JSON 数组")
    events: dict[int, EventInfo] = {}
    for item in parsed:
        if not isinstance(item, Mapping):
            continue
        event = EventInfo.from_dict(item)
        events[event.event_id] = event
    return events


def parse_finished_matches(
    event_id: int,
    raw_matches: Any,
) -> tuple[str, list[Match]]:
    """Parse ``event_session_list`` rows into finished results, oldest first.

    The endpoint may return featured matches from other events when the
    requested event has no sessions yet.  Rows whose ``tt_info.id`` does not
    match ``event_id`` are ignored so a result cannot be filed under the wrong
    event.
    """
    if not isinstance(raw_matches, list):
        return "", []

    expected_event = f"csgo_tt_{event_id}"
    title = ""
    results: list[Match] = []
    for raw_match in raw_matches:
        if not isinstance(raw_match, Mapping):
            continue
        tt_info = raw_match.get("tt_info")
        if not isinstance(tt_info, Mapping) or tt_info.get("id") != expected_event:
            continue
        if not title:
            title = str(tt_info.get("disp_name") or "").strip()

        state = raw_match.get("state")
        if not isinstance(state, Mapping):
            continue
        try:
            status = int(state.get("status"))
        except (TypeError, ValueError):
            continue
        if status != 2:
            continue

        mc_info = raw_match.get("mc_info")
        if not isinstance(mc_info, Mapping):
            continue
        team1_info = mc_info.get("t1_info") or {}
        team2_info = mc_info.get("t2_info") or {}
        if not isinstance(team1_info, Mapping) or not isinstance(team2_info, Mapping):
            continue
        team1 = str(team1_info.get("disp_name") or "").strip()
        team2 = str(team2_info.get("disp_name") or "").strip()
        if not team1 or not team2:
            continue

        try:
            team1_score = int(state.get("t1_score"))
            team2_score = int(state.get("t2_score"))
        except (TypeError, ValueError):
            continue

        if team2_score > team1_score:
            team1, team2 = team2, team1
            team1_score, team2_score = team2_score, team1_score

        match_id = str(mc_info.get("id") or "").strip()
        if not match_id:
            continue
        results.append((team1, team2, f"{team1_score}:{team2_score}", match_id))

    results.reverse()
    return title, results


def is_followed_team(team: str, followed: Iterable[str]) -> bool:
    """Match ``BC.Game``/``bcg`` and similar spelling differences.

    A followed term matches when it equals the team name after normalization or
    is contained in it.  The latter covers the short ``bcg`` alias used in the
    configuration for ``BC.Game``.
    """
    normalized = compact(team)
    if not normalized:
        return False
    for value in followed:
        term = compact(str(value))
        if term and (normalized == term or term in normalized):
            return True
    return False


def event_has_followed_team(event: EventInfo, followed: Iterable[str]) -> bool:
    return any(is_followed_team(team, followed) for team in event.teams)


def qualifies_for_watch(event: EventInfo, followed: Iterable[str]) -> bool:
    return event.is_major_tier or event_has_followed_team(event, followed)


def event_is_active(
    event: EventInfo,
    now_ts: int,
    grace_seconds: int = FINISHED_EVENT_GRACE_SECONDS,
) -> bool:
    """Keep an event while it is running and briefly after it ends.

    The short grace period lets the next poll pick up the final result even if
    5EPlay has already flipped the event status to ``past``.
    """
    if event.end_ts and event.end_ts + grace_seconds < now_ts:
        return False
    if event.status == "past" and not event.end_ts:
        return False
    return True


def _is_stage_child(parent: EventInfo, child: EventInfo) -> bool:
    """True when ``child`` looks like a stage sub-event of ``parent``.

    Majors are published twice by 5EPlay: once as the overall event and once
    per stage (``... 第1阶段``).  Watching both would poll the same matches twice
    and can duplicate notifications.  Only stage-marked children are collapsed;
    separate qualifiers and similarly named events are kept.
    """
    child_name = child.name.casefold()
    if "阶段" not in child.name and "stage" not in child_name:
        return False
    parent_name = compact(parent.name)
    child_compact = compact(child.name)
    if len(parent_name) < 4 or len(child_compact) < 4:
        return False
    if not (
        child_compact.startswith(parent_name)
        or parent_name.startswith(child_compact)
    ):
        return False
    if parent.start_ts and parent.end_ts:
        if child.start_ts and child.start_ts > parent.end_ts:
            return False
        if child.end_ts and child.end_ts < parent.start_ts:
            return False
    return True


def select_watch_events(
    events: Iterable[EventInfo],
    followed: Iterable[str],
    now_ts: int,
    grace_seconds: int = FINISHED_EVENT_GRACE_SECONDS,
) -> list[EventInfo]:
    """Return active events that should be watched, without stage duplicates."""
    candidates = [
        event
        for event in events
        if event_is_active(event, now_ts, grace_seconds)
        and qualifies_for_watch(event, followed)
    ]
    # Keep the widest event of a family first so its stage children are dropped.
    candidates.sort(key=lambda event: (-(event.end_ts - event.start_ts), event.start_ts, event.event_id))
    selected: list[EventInfo] = []
    for event in candidates:
        if any(_is_stage_child(parent, event) for parent in selected):
            continue
        selected.append(event)
    selected.sort(key=lambda event: (event.start_ts, event.event_id))
    return selected


def should_poll_event(event: EventInfo, now_ts: int) -> bool:
    """Only live events (and recently finished or overdue events) poll.

    Upcoming events are still kept in the watch list, but polling their match
    endpoint before the event starts would only waste requests.  The discovery
    pass notices the ``live`` transition within half an hour.  Recently finished
    events are polled for a short grace period so the last result is not lost
    when the event status flips to ``past`` between two polls.
    """
    recently_finished = bool(
        event.end_ts
        and event.end_ts <= now_ts
        and event.end_ts + FINISHED_EVENT_GRACE_SECONDS >= now_ts
    )
    if event.end_ts and event.end_ts + FINISHED_EVENT_GRACE_SECONDS < now_ts:
        return False
    if event.status == "past":
        return recently_finished
    if event.status == "live":
        return True
    if event.status == "upcoming" and event.start_ts:
        return event.start_ts <= now_ts
    return False


def notify_matches(
    event: EventInfo,
    matches: Sequence[Match],
    followed: Iterable[str],
) -> list[Match]:
    """Apply the notification rules for one event."""
    if event.is_major_tier:
        return list(matches)
    return [
        match
        for match in matches
        if is_followed_team(match[0], followed) or is_followed_team(match[1], followed)
    ]


def build_message(event: EventInfo, matches: Sequence[Match]) -> str:
    title = event.name or f"赛事 {event.event_id}"
    tag = f"【{event.tier_label}】" if event.tier_label else ""
    lines = [f"{tag}{title} 结果有更新"]
    lines.extend(f"{match[0]} vs {match[1]} {match[2]}" for match in matches)
    return "\n".join(lines)


def split_message(text: str, limit: int = 1500) -> list[str]:
    """Split a long notification on line boundaries for OneBot message limits."""
    chunks: list[str] = []
    current: list[str] = []
    size = 0
    for line in text.split("\n"):
        extra = len(line) + (1 if current else 0)
        if current and size + extra > limit:
            chunks.append("\n".join(current))
            current = [line]
            size = len(line)
        else:
            current.append(line)
            size += extra
    if current:
        chunks.append("\n".join(current))
    return chunks
