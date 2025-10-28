from pydantic import BaseModel
from typing import List


class Config(BaseModel):
    """Plugin Config Here"""
    cs_live_list: List[str]
    cs_group_list: List[int]
