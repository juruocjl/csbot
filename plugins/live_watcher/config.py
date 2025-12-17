from pydantic import BaseModel


class Config(BaseModel):
    """Plugin Config Here"""
    live_watch_list: list[str]
    cs_group_list: list[int]