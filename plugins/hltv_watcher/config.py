from pydantic import BaseModel, Field

class Config(BaseModel):
    """Plugin Config Here"""
    cs_group_list: list[int]
    cs_event_group_list: list[int] = Field(default_factory=list)
    cs_schedule_auto_delete_seconds: int = Field(default=90, ge=0)
