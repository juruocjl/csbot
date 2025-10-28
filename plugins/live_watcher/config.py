from pydantic import BaseModel
from typing import List


class Config(BaseModel):
    """Plugin Config Here"""
    live_watch_list: List[str]
    cs_group_list: List[int]
