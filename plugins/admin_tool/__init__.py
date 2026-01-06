from nonebot import get_plugin_config
from nonebot.plugin import PluginMetadata
from nonebot.adapters.onebot.v11 import Message, MessageEvent, MessageSegment
from nonebot import on_command
from nonebot.params import CommandArg
from nonebot.permission import SUPERUSER
from nonebot import require

from sqlalchemy import text

require("nonebot_plugin_apscheduler")
from nonebot_plugin_apscheduler import scheduler

require("utils")
from ..utils import async_session_factory

from .config import Config

import subprocess
import re

__plugin_meta__ = PluginMetadata(
    name="admin_tool",
    description="",
    usage="",
    config=Config,
)

config = get_plugin_config(Config)


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
sql = on_command("sql", priority=10, block=True, permission=SUPERUSER)

@pull.handle()
async def pull_function():
    await pull.finish(git_pull())

async def query(sql: str):
    async with async_session_factory() as session:
        async with session.begin():
            cursor = await session.execute(text(sql))
            return cursor.fetchall()

@sql.handle()
async def sql_function(args: Message = CommandArg()):
    await sql.finish(await query(args.extract_plain_text()))
