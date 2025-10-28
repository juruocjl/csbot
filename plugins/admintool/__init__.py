from nonebot import get_plugin_config
from nonebot.plugin import PluginMetadata
from nonebot import on_command
from nonebot.permission import SUPERUSER
from nonebot import logger

from .config import Config

__plugin_meta__ = PluginMetadata(
    name="admintool",
    description="",
    usage="",
    config=Config,
)

config = get_plugin_config(Config)

import subprocess

def git_pull(repo_path: str = ".") -> str:
    try:
        result = subprocess.run(
            ["git", "pull"],
            cwd=repo_path,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        
        output_lines = result.stdout.strip().splitlines()

        logger.info(output_lines)

        if result.returncode == 0:
            assert(output_lines)
            if output_lines:
                summary = output_lines[-1]
                return f"成功：{summary}"
        else:
            error_msg = output_lines[-1] if output_lines else "未知错误"
            return f"失败：{error_msg}"
    
    except Exception as e:
        return f"执行出错：{str(e)}"


pull = on_command("pull", priority=10, block=True, permission=SUPERUSER)

@pull.handle()
async def pull_function():
    await pull.finish(git_pull())