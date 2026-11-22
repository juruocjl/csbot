from pydantic import BaseModel


class Config(BaseModel):
    """Plugin Config Here"""

    cs_group_list: list[str] = []
    cs_domain: str = "https://cs.example.com"
    cs_birthday_enabled: bool = True
    cs_birthday_self_cooldown_seconds: int = 86400
    cs_birthday_pregen_hour: int = 23
    cs_birthday_pregen_minute: int = 50
    cs_birthday_timezone: str = "Asia/Shanghai"
