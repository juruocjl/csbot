from pydantic import BaseModel
from typing import List


class Config(BaseModel):
    """Plugin Config Here"""
    cs_ai_url: str
    cs_ai_api_key: str
    cs_ai_model: str
    cs_group_list: List[int]