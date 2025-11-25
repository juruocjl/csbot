from pydantic import BaseModel
from typing import List

class Config(BaseModel):
    """Plugin Config Here"""
    hltv_event_id_list: List[int]
    cs_group_list: List[int]