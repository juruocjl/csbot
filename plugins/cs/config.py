from pydantic import BaseModel


class Config(BaseModel):
    """Plugin Config Here"""
    cs_watch_stage_auto_delete_seconds: int = 600
