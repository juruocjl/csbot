from pydantic import BaseModel, Field


def _default_time_locations() -> dict[str, str]:
    return {
        "中国（北京）": "Asia/Shanghai",
        "美国东部（纽约）": "America/New_York",
        "美国中部（芝加哥）": "America/Chicago",
        "美国西部（洛杉矶）": "America/Los_Angeles",
    }


class Config(BaseModel):
    """Configuration for the time-location command."""

    cs_time_locations: dict[str, str] = Field(default_factory=_default_time_locations)
    cs_time_auto_delete_seconds: int = 60
