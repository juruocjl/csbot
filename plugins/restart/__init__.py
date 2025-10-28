from nonebot import get_plugin_config
from nonebot.plugin import PluginMetadata
from nonebot import get_driver, on_command
from nonebot.permission import SUPERUSER

import inspect
import socket
from pathlib import Path
from shutil import which
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from uvicorn.server import Server

driver = get_driver()

from .config import Config

__plugin_meta__ = PluginMetadata(
    name="restart",
    description="",
    usage="",
    config=Config,
)

config = get_plugin_config(Config)

def _restart():
    from os import execlp
    nb = which("nb")
    py = which("python")
    if nb:
        execlp(nb, nb, "run")
    elif py:
        from sys import argv
        if argv and Path(argv[0]).exists():
            execlp(py, py, argv[0])
        if Path("bot.py").exists():
            execlp(py, py, "bot.py")
    raise Exception("cannot restart")


def _uvicorn_getserver() -> "Server":
    from uvicorn.server import Server
    fis = inspect.getouterframes(inspect.currentframe())
    svrs = (fi.frame.f_locals.get("server", None) for fi in fis[::-1])
    server, *_ = (s for s in svrs if isinstance(s, Server))
    return server


def _uvicorn_getsocket() -> list[socket.socket]:
    fis = inspect.getouterframes(inspect.currentframe())
    skvars = (fi.frame.f_locals.get("sockets", None) for fi in fis[::-1])
    try:
        socks, *_ = (
            s for s in skvars
            if isinstance(s, list) and all(isinstance(x, socket.socket) for x in s)
        )
        return socks
    except Exception:
        return []


def _none_stop():
    if TYPE_CHECKING:
        from nonebot.drivers.none import Driver as NoneDriver
        assert isinstance(driver, NoneDriver)
    driver.exit()


def _uvicorn_stop():
    server = _uvicorn_getserver()
    server.should_exit = True


stop_m = on_command("stop", aliases={"shutdown", "停机"}, permission=SUPERUSER)
restart_m = on_command("restart", aliases={"reboot", "重启"}, permission=SUPERUSER)


@stop_m.handle()
async def do_stop():
    if "fastapi" in driver.type or "quart" in driver.type:
        _uvicorn_stop()
    if "none" in driver.type:
        _none_stop()


@restart_m.handle()
async def do_restart():
    import atexit
    if "none" in driver.type:
        atexit.register(_restart)
        _none_stop()
    if "fastapi" in driver.type or "quart" in driver.type:
        server = _uvicorn_getserver()
        server.force_exit = True
        await server.shutdown(_uvicorn_getsocket())
        try:
            _restart()
        except Exception:
            await do_stop()