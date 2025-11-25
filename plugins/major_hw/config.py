from pydantic import BaseModel
from typing import List

class Config(BaseModel):
    """Plugin Config Here"""
    major_stage: str
    major_event_id: int
    major_teams: List[str]
    cs_group_list: List[int]