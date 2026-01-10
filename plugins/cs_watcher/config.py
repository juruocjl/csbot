from pydantic import BaseModel


class Config(BaseModel):
    """Plugin Config Here"""
    cs_steamkey: str
    cs_proxy: str | None = None
    cs_group_list: list[int]