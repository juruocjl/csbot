from pydantic import BaseModel


class Config(BaseModel):
    """Plugin Config Here"""
    auth_code_valid_seconds: int = 3600  # 验证码有效期，单位秒
