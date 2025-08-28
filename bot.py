import nonebot
from nonebot.adapters.qq import Adapter as QQAdapter
from pathlib import Path

nonebot.init()
 
driver = nonebot.get_driver()
driver.register_adapter(QQAdapter)
 
nonebot.load_plugin(Path("./plugins/cs"))
 
nonebot.load_from_toml("pyproject.toml")

if __name__ == "__main__":
    nonebot.run()