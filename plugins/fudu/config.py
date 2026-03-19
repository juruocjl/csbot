from pydantic import BaseModel
from pydantic import Field


class Config(BaseModel):
    """Plugin Config Here"""
    cs_group_list: list[int]
    cs_fudu_ban_group_list: list[int] = Field(default_factory=list)
    cs_fudu_delay: int = 10
    cs_fudu_rank_delete_delay: int = 60
