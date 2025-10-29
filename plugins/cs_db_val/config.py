from pydantic import BaseModel


class Config(BaseModel):
    """Plugin Config Here"""
    cs_season_id: str
    cs_last_season_id: str
