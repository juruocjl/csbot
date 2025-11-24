from pydantic import BaseModel
from typing import List

class Config(BaseModel):
    """Plugin Config Here"""
    major_stage: str
    major_teams: List[str]