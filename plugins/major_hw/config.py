from pydantic import BaseModel

class Config(BaseModel):
    """Plugin Config Here"""
    major_name: str
    major_stage: str
    major_event_id: int
    major_rating_event_id_list: list[int] | None = None
    cs_group_list: list[int]
