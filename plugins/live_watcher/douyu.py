"""Read Douyu's own room metadata; never infer live status from HTML titles."""

from __future__ import annotations

from dataclasses import dataclass
import json
import re

import aiohttp


HEADERS = {"User-Agent": "Mozilla/5.0", "Referer": "https://www.douyu.com/"}
TIMEOUT = aiohttp.ClientTimeout(total=10)
REPLAY_TITLE = re.compile(r"回放|重播|录播|轮播", re.IGNORECASE)


@dataclass(frozen=True)
class DouyuStatus:
    islive: int
    nickname: str
    is_replay: bool


def _integer(value: object, field: str) -> int:
    if type(value) is int:
        return value
    if isinstance(value, str) and value.isascii() and value.isdigit():
        return int(value)
    raise ValueError(f"invalid Douyu {field}")


def parse_room(data: object, room_id: str) -> DouyuStatus:
    room = data.get("room") if isinstance(data, dict) else None
    if not isinstance(room, dict):
        raise ValueError("missing Douyu room metadata")
    if _integer(room.get("room_id"), "room_id") != int(room_id):
        raise ValueError("Douyu room identity mismatch")
    nickname, title = room.get("nickname"), room.get("room_name")
    if not isinstance(nickname, str) or not nickname.strip():
        raise ValueError("missing Douyu nickname")
    if not isinstance(title, str):
        raise ValueError("missing Douyu room title")
    show_status = _integer(room.get("show_status"), "show_status")
    video_loop = _integer(room.get("videoLoop"), "videoLoop")
    if show_status not in (1, 2) or video_loop not in (0, 1):
        raise ValueError("unknown Douyu live/replay status")
    # Some manually rebroadcast events have videoLoop=0 but explicitly say 重播.
    is_replay = show_status == 1 and (video_loop == 1 or bool(REPLAY_TITLE.search(title)))
    return DouyuStatus(int(show_status == 1 and not is_replay), nickname.strip(), is_replay)


def resolve_room_id(html: str, requested_id: str) -> str:
    match = re.search(
        r'<script\b[^>]*\bid=[\"\']vike_pageContext[\"\'][^>]*>(.*?)</script\s*>',
        html, re.DOTALL | re.IGNORECASE,
    )
    if match is None:
        raise ValueError("missing Douyu mobile room metadata")
    try:
        room = json.loads(match[1])["pageProps"]["room"]["roomInfo"]["roomInfo"]
        actual = _integer(room["rid"], "rid")
        vip_id = _integer(room.get("vipId", 0), "vipId")
    except (KeyError, TypeError) as error:
        raise ValueError("invalid Douyu mobile room metadata") from error
    if actual <= 0 or int(requested_id) not in (actual, vip_id):
        raise ValueError("Douyu mobile room identity mismatch")
    return str(actual)


async def _fetch_room(session: aiohttp.ClientSession, room_id: str) -> DouyuStatus:
    async with session.get(
        f"https://www.douyu.com/betard/{room_id}", headers=HEADERS, timeout=TIMEOUT,
    ) as response:
        response.raise_for_status()
        return parse_room(await response.json(), room_id)


async def fetch_douyu_status(session: aiohttp.ClientSession, liveid: str) -> DouyuStatus:
    match = re.fullmatch(r"dy_([1-9][0-9]*)", liveid)
    if match is None:
        raise ValueError("invalid Douyu room id")
    requested_id = match[1]
    try:
        return await _fetch_room(session, requested_id)
    except (aiohttp.ClientError, TimeoutError, ValueError) as error:
        # betard requires the real ID. Resolve vanity IDs using the official
        # mobile page, whose isLive flag also counts replays and must not be used.
        async with session.get(
            f"https://m.douyu.com/{requested_id}", headers=HEADERS, timeout=TIMEOUT,
        ) as response:
            response.raise_for_status()
            actual_id = resolve_room_id(await response.text(), requested_id)
        if actual_id == requested_id:
            raise ValueError("Douyu live/replay metadata unavailable") from error
        return await _fetch_room(session, actual_id)
