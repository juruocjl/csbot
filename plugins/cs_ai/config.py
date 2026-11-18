from pydantic import BaseModel


class Config(BaseModel):
    """Plugin Config Here"""
    cs_ai_url: str = "https://api.openai.com/v1"
    cs_ai_api_key: str
    cs_ai_model: str = "gpt-6.2-sol"
    cs_ai_output_guard_enabled: bool = True
    cs_ai_guard_model: str = ""
    cs_ai_enable_thinking: bool = True
    cs_tavily_api_key: str = ""
    cs_domain: str = "https://cs.example.com"  # CS服务器域名
    cs_ai_engine: str = "dsh"
    cs_ai_vision: bool = True
    cs_ai_openai_proxy_url: str = "http://127.0.0.1:7890"
    cs_ai_image_model: str = "gpt-image-2.6-sunburst"
    cs_ai_search_url: str = ""
    cs_ai_search_api_key: str = ""
