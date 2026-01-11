from pydantic import BaseModel


class Config(BaseModel):
    """Plugin Config Here"""
    auth_code_valid_seconds: int = 3600  # 验证码有效期，单位秒
    send_interval_seconds: int = 60  # 发送间隔，单位秒
    user_name_cache_expiration: int = 86400  # 用户名缓存过期时间，单位秒
    
    cs_season_id: str
    cs_last_season_id: str

    cs_domain: str = "https://cs.example.com"  # CS服务器域名
    cs_botid: int # 机器人的 qq 号