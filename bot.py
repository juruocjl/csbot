import nonebot
from nonebot.adapters.onebot.v11 import Adapter as OneBotAdapter
from pathlib import Path
from nonebot import logger
from nonebot.log import default_format
logger.add("app.log", level="INFO", format=default_format, rotation="1 week")


nonebot.init()
 
driver = nonebot.get_driver()
driver.register_adapter(OneBotAdapter)
 
nonebot.load_plugin(Path("plugins") / "utils")
nonebot.load_plugin(Path("plugins") / "ts")
nonebot.load_plugin(Path("plugins") / "pic")
nonebot.load_plugin(Path("plugins") / "allmsg")
nonebot.load_plugin(Path("plugins") / "restart")
nonebot.load_plugin(Path("plugins") / "admintool")
nonebot.load_plugin(Path("plugins") / "cs")
 
nonebot.load_from_toml("pyproject.toml")

if __name__ == "__main__":
    nonebot.run()