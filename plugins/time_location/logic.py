from __future__ import annotations

from datetime import datetime, timezone
from zoneinfo import ZoneInfo


WEEKDAYS = ("周一", "周二", "周三", "周四", "周五", "周六", "周日")


class UnknownLocationError(ValueError):
    pass


def select_locations(
    locations: dict[str, str],
    query: str,
) -> list[tuple[str, str]]:
    """Select configured locations by exact name, IANA zone, or name keyword."""
    items = list(locations.items())
    normalized_query = query.strip().casefold()
    if not normalized_query:
        return items

    exact = [
        item
        for item in items
        if normalized_query in (item[0].casefold(), item[1].casefold())
    ]
    if exact:
        return exact

    matches = [
        item
        for item in items
        if normalized_query in item[0].casefold()
        or normalized_query in item[1].casefold()
    ]
    if matches:
        return matches

    raise UnknownLocationError(query.strip())


def format_location_time(
    name: str,
    zone_name: str,
    now: datetime | None = None,
) -> str:
    """Render one location using IANA rules, including historical/future DST."""
    instant = now or datetime.now(timezone.utc)
    if instant.tzinfo is None:
        raise ValueError("now must be timezone-aware")

    local_time = instant.astimezone(ZoneInfo(zone_name))
    return f"{name}：{local_time:%m-%d %H:%M} {WEEKDAYS[local_time.weekday()]}"
