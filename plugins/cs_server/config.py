from pydantic import BaseModel, Field


class Config(BaseModel):
    """Plugin Config Here"""
    auth_code_valid_seconds: int = 3600  # 验证码有效期，单位秒
    send_interval_seconds: int = 60  # 发送间隔，单位秒
    user_name_cache_expiration: int = 86400  # 用户名缓存过期时间，单位秒
    
    cs_season_id: str
    cs_last_season_id: str

    cs_domain: str = "https://cs.example.com"  # CS服务器域名
    cs_steam_monitor_url: str = "http://127.0.0.1:5555/api/friends/status"  # Steam 在线状态监控接口 URL
    cs_botid: int # 机器人的 qq 号
    mute_api_token: str | None = Field(
        None,
        description="来自 .env 的管理员认证 token",
        env="MUTE_API_TOKEN",
    )