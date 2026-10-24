"""5EPlay tournament API client used by the event watcher."""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta
from typing import Any, Mapping

import aiohttp
from nonebot import logger, require

from .logic import SHANGHAI, EventInfo, parse_event_list, parse_finished_matches

get_session = require("utils").get_session


EVENT_LIST_URL = "https://app.5eplay.com/api/csgo/tournament/csgo_event_list_v1"
EVENT_SESSION_URL = "https://app.5eplay.com/api/tournament/event_session_list"

REQUEST_TIMEOUT_SECONDS = 20
DISCOVERY_DAYS_BEFORE = 1
DISCOVERY_DAYS_AFTER = 30
MAX_DISCOVERY_PAGES = 2

REQUEST_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "zh-CN,zh;q=0.9",
    "Origin": "https://event.5eplay.com",
    "Referer": "https://event.5eplay.com/",
}


class FiveEApiError(RuntimeError):
    def __init__(self, message: str, status: int | None = None):
        super().__init__(message)
        self.status = status


async def _request_json(
    method: str,
    url: str,
    *,
    params: Mapping[str, Any] | None = None,
    data: Mapping[str, Any] | None = None,
) -> Mapping[str, Any]:
    session = get_session()
    try:
        async with session.request(
            method,
            url,
            params=params,
            json=data,
            headers=REQUEST_HEADERS,
            timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT_SECONDS),
        ) as response:
            if response.status != 200:
                raise FiveEApiError(
                    f"5E API 返回 HTTP {response.status}: {url}", response.status
                )
            try:
                payload = await response.json(content_type=None)
            except (aiohttp.ContentTypeError, ValueError) as exc:
                raise FiveEApiError(f"5E API 返回了非 JSON 内容: {url}") from exc
    except asyncio.TimeoutError as exc:
        raise FiveEApiError(f"5E API 请求超时: {url}") from exc
    except aiohttp.ClientError as exc:
        raise FiveEApiError(f"5E API 网络错误: {url}: {exc}") from exc

    if not isinstance(payload, Mapping):
        raise FiveEApiError(f"5E API 返回结构异常: {url}")
    if payload.get("success") is False or payload.get("status") is False:
        message = payload.get("message") or payload.get("error") or "未知错误"
        raise FiveEApiError(f"5E API 返回失败: {url}: {message}")
    return payload


def _discovery_payload(page_token: str, now: datetime) -> dict[str, Any]:
    start = (now - timedelta(days=DISCOVERY_DAYS_BEFORE)).strftime(
        "%Y-%m-%d 00:00:00"
    )
    end = (now + timedelta(days=DISCOVERY_DAYS_AFTER)).strftime(
        "%Y-%m-%d 23:59:59"
    )
    return {
        "tournaments_options": {
            "cursor": "",
            "player_id": "",
            "time_value": f"{start}_{end}",
            "team_id": "",
            "time_type": "self",
            "tt_series": [],
            "grade": [],
            "tt_bonus": [],
            "page_token": page_token,
        }
    }


async def fetch_events(now: datetime | None = None) -> list[EventInfo]:
    """Discover events in a bounded window around now.

    The API is only asked for the current/upcoming window (one day back and 30
    days ahead), not the full event history.  At most two pages are read.
    """
    now = now or datetime.now(SHANGHAI)
    events: dict[int, EventInfo] = {}
    page_token = ""
    for page in range(MAX_DISCOVERY_PAGES):
        payload = await _request_json(
            "POST",
            EVENT_LIST_URL,
            data=_discovery_payload(page_token, now),
        )
        for event in parse_event_list(payload):
            events[event.event_id] = event

        data = payload.get("data")
        next_token = data.get("page_token") if isinstance(data, Mapping) else None
        if not next_token:
            break
        if page + 1 == MAX_DISCOVERY_PAGES:
            logger.warning(
                "5E 赛事列表还有更多分页，本轮只读取前 %s 页", MAX_DISCOVERY_PAGES
            )
            break
        page_token = str(next_token)

    if not events:
        raise FiveEApiError("5E 赛事列表为空，保留上一轮监视列表")
    return list(events.values())


async def get_matches(
    event_id: int, fallback_title: str = ""
) -> tuple[str, list[tuple[str, str, str, str]]]:
    """Return finished matches for one event, oldest first.

    The endpoint can return featured matches from other events when the
    requested event has no sessions yet.  Those rows are filtered out by
    ``tt_info.id`` so a result cannot be filed under the wrong event.
    """
    payload = await _request_json(
        "GET",
        EVENT_SESSION_URL,
        params={"tt_id": f"csgo_tt_{event_id}", "game_type": 1},
    )
    data = payload.get("data")
    raw_matches = data.get("matches") if isinstance(data, Mapping) else None
    title, results = parse_finished_matches(event_id, raw_matches)
    return title or fallback_title, results
