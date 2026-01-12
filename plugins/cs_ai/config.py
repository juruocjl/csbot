from pydantic import BaseModel


class Config(BaseModel):
    """Plugin Config Here"""
    cs_ai_url: str
    cs_ai_api_key: str
    cs_ai_model: str
    cs_ai_enable_thinking: bool = False
    cs_domain: str = "https://cs.example.com"  # CS服务器域名
