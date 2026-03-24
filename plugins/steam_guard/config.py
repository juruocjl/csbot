from pydantic import BaseModel, Field


class Config(BaseModel):
    cs_steam_guard_enable_group_list: list[int] = Field(default_factory=list)
    cs_steam_guard_target_user: str = ""
    cs_steam_guard_ban_after: int = 3
    cs_steam_guard_ban_duration: int = 600
    cs_steam_guard_warn_cooldown_seconds: int = 300
    cs_steam_guard_monitor_cache_seconds: int = 30
