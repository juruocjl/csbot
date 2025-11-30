from pydantic import BaseModel
from typing import List

class Config(BaseModel):
    """Plugin Config Here"""
    major_stage: str
    major_all_stages: List[str]
    major_event_id: int
    cs_group_list: List[int]