from pydantic import BaseModel, Field


class Config(BaseModel):
    """Plugin Config Here"""
    cs_event_group_list: list[int | str] = Field(default_factory=list)
