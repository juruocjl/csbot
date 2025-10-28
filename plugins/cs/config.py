from pydantic import BaseModel
from typing import List


class Config(BaseModel):
    """Plugin Config Here"""
    cs_botid: int
    cs_mysteam_id: int
    cs_wmtoken: str
    cs_season_id: str
    cs_last_season_id: str
    cs_ai_url: str
    cs_ai_api_key: str
    cs_ai_model: str
    cs_live_list: List[str]
    cs_group_list: List[int]
    cs_csqaq_api: str