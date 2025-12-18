from pydantic import BaseModel


class Config(BaseModel):
    """Plugin Config Here"""
    auto_delete_delay: int
    auto_delete_uid: list[str]