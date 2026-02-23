from pydantic import BaseModel


class Config(BaseModel):
    """Plugin Config Here"""
    cs_group_list: list[int]
    cs_fudu_delay: int = 5
