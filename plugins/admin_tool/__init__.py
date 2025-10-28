from nonebot import get_plugin_config
from nonebot.plugin import PluginMetadata
from nonebot import on_command
from nonebot.permission import SUPERUSER
from nonebot import logger

from .config import Config

__plugin_meta__ = PluginMetadata(
    name="admin_tool",
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
            stderr=subprocess.STDOUT,
            text=True
        )
        
        detailed_lines = [line.strip() for line in result.stdout.splitlines() if line.strip()]
        
        if result.returncode == 0:
            if not detailed_lines:
                summary = "本地已同步（无输出）"
            elif "Already up to date" in detailed_lines[-1]:
                summary = "已是最新版本，无需更新"
            elif "Fast-forward" in detailed_lines:
                stats_line = next((line for line in detailed_lines if "files changed" in line or "file changed" in line), None)
                if stats_line:
                    summary = f"更新成功：{stats_line}"
                else:
                    summary = "更新成功（无详细统计）"
            else:
                summary = f"操作成功：{detailed_lines[-1]}"
        else:
            summary = f"操作失败：{detailed_lines[-1] if detailed_lines else '未知错误'}"
        
        return summary
    
    except Exception as e:
        err_msg = f"执行出错：{str(e)}"
        return err_msg


pull = on_command("pull", priority=10, block=True, permission=SUPERUSER)

@pull.handle()
async def pull_function():
    await pull.finish(git_pull())