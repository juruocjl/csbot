from pydantic import BaseModel

class Config(BaseModel):
    """Plugin Config Here"""
    hltv_event_id_list: list[int]
    cs_group_list: list[int]