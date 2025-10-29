from pydantic import BaseModel
from typing import List


class Config(BaseModel):
    """Plugin Config Here"""
    cs_group_list: List[int]
