import nonebot
from nonebot.adapters.onebot.v11 import Adapter as OneBotAdapter
from pathlib import Path

from nonebot import logger
from nonebot.log import default_format
logger.add("app.log", level="INFO", format=default_format, rotation="1 week")
logger.add("err.log", level="ERROR", format=default_format, rotation="1 week")


nonebot.init()
 
driver = nonebot.get_driver()
driver.register_adapter(OneBotAdapter)
 
nonebot.load_plugin(Path("plugins") / "utils")
nonebot.load_plugin(Path("plugins") / "major_hw")
nonebot.load_plugin(Path("plugins") / "pic")
nonebot.load_plugin(Path("plugins") / "allmsg")
nonebot.load_plugin(Path("plugins") / "ts")
nonebot.load_plugin(Path("plugins") / "market")
nonebot.load_plugin(Path("plugins") / "market2")
nonebot.load_plugin(Path("plugins") / "admin_tool")
nonebot.load_plugin(Path("plugins") / "live_watcher")
nonebot.load_plugin(Path("plugins") / "cs_img")
nonebot.load_plugin(Path("plugins") / "cs_db_upd")
nonebot.load_plugin(Path("plugins") / "cs_db_val")
nonebot.load_plugin(Path("plugins") / "cs_ai")
nonebot.load_plugin(Path("plugins") / "cs_report")
nonebot.load_plugin(Path("plugins") / "cs")
nonebot.load_plugin(Path("plugins") / "small_funcs")

nonebot.load_plugin("nonebot_plugin_memes")

nonebot.load_from_toml("pyproject.toml")

if __name__ == "__main__":
    nonebot.run()