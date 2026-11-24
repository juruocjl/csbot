from pydantic import BaseModel


class Config(BaseModel):
    """Plugin Config Here"""
    # Direct Telnet fallback only. Optional so the GCP host can drop the TS
    # password entirely and run through TS_PROXY_URL alone; the direct branch
    # refuses to run while either value is empty.
    ts_ip: str = ""
    ts_pswd: str = ""
    ts_delete_delay: int = 300
    # Edge query proxy on cgserver (single async HTTP request per command).
    ts_proxy_url: str = ""
    ts_proxy_token: str = ""
    ts_proxy_timeout: float = 10.0
