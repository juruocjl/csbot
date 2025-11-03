from pydantic import BaseModel


class Config(BaseModel):
    """Plugin Config Here"""
    ts_ip: str
    ts_pswd: str
