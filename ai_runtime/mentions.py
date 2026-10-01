"""Stable identity markers with a bounded, untrusted at-time display label."""
from __future__ import annotations

import re

MAX_AT_NAME_LENGTH = 100


def at_display_name(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    name = re.sub(r"[\x00-\x1f\x7f]", " ", value).strip()
    return name[:MAX_AT_NAME_LENGTH] or None


def render_at(uid: object, display_name: object = None) -> str:
    """Keep the QQ marker for identity; the name is only a historical label."""
    target = str(uid)
    if target == "all":
        return "[at:all]"
    name = at_display_name(display_name)
    if not name:
        return f"[at:{target}]"
    # A nickname must not introduce another apparent internal reference.
    name = name.replace("[", "［").replace("]", "］")
    return f"@{name}([at:{target}])"
