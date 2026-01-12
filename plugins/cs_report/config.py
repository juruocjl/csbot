from pydantic import BaseModel


class Config(BaseModel):
    """Plugin Config Here"""
    cs_group_list: list[int]

    cs_domain: str = "https://cs.example.com"  # CS服务器域名