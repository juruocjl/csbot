from nonebot import get_plugin_config
from nonebot.plugin import PluginMetadata
from nonebot import on_command
from nonebot.permission import SUPERUSER

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
        
        if result.returncode == 0:
            output_lines = result.stdout.strip().splitlines()
            assert(output_lines)
            if output_lines:
                summary = output_lines[-1]
                return f"成功：{summary}"
        else:
            error_lines = result.stderr.strip().splitlines()
            error_msg = error_lines[-1] if error_lines else "未知错误"
            return f"失败：{error_msg}"
    
    except Exception as e:
        return f"执行出错：{str(e)}"


pull = on_command("pull", priority=10, block=True, permission=SUPERUSER)

@pull.handle()
async def pull_function():
    await pull.finish(git_pull())