from nonebot import get_plugin_config
from nonebot.plugin import PluginMetadata
from nonebot.adapters.onebot.v11 import Message, MessageEvent, MessageSegment
from nonebot import on_command
from nonebot import require
from nonebot import get_bot
from nonebot import logger

from bs4 import BeautifulSoup
from curl_cffi.requests import AsyncSession
import json

scheduler = require("nonebot_plugin_apscheduler").scheduler

localstorage = require("utils").localstorage
event_update = require("major_hw").event_update

from .config import Config

__plugin_meta__ = PluginMetadata(
    name="hltv_watcher",
    description="",
    usage="",
    config=Config,
)

config = get_plugin_config(Config)


def parse_matches_by_score(html: str) -> list[tuple[str, str]]:
    soup = BeautifulSoup(html, 'html.parser')

    title = soup.find("div", class_="event-hub-title").get_text(strip=True)

    results = []

    for match in soup.find_all("div", class_="result-con"):
        # 1. 获取按顺序排列的两个队伍名称
        # find_all 会按照文档流的顺序返回元素，所以 teams[0] 是左边队伍，teams[1] 是右边队伍
        teams = match.find_all("div", class_="team")
        
        # 2. 获取比分区域的 span
        score_cell = match.find("td", class_="result-score")
        if not score_cell:
            continue
        
        # 这里通常有两个 span：左边分数和右边分数
        score_spans = score_cell.find_all("span")

        # 确保数据完整性（既有两支队伍，也有两个分数 span）
        assert(len(teams) == 2 and len(score_spans) == 2)
        name_left = teams[0].get_text(strip=True)
        name_right = teams[1].get_text(strip=True)
        
        # 3. 判断哪个 span 有 'score-won' 类
        left_classes = score_spans[0].get("class", [])
        right_classes = score_spans[1].get("class", [])

        left_score = score_spans[0].get_text(strip=True)
        right_score = score_spans[1].get_text(strip=True)

        if "score-won" in left_classes:
            # 左边分数的 span 赢了 -> 左边队伍赢
            results.append((name_left, name_right, f"{left_score}:{right_score}"))
        elif "score-won" in right_classes:
            # 右边分数的 span 赢了 -> 右边队伍赢
            results.append((name_right, name_left, f"{right_score}:{left_score}"))
        else:
            raise(RuntimeError("No win team"))
    return title, results

updategame = on_command("更新比赛", priority=10, block=True)

@scheduler.scheduled_job("cron", minute="*/5", id="hltv")
async def update_events():
    bot = get_bot()
    async with AsyncSession(impersonate="chrome110") as s:
        for event in config.hltv_event_id_list:
            logger.info(f"start get {event}")
            r = await s.get("https://www.hltv.org/results?event={event}")
            title, res = parse_matches_by_score(r.text)
            oldres = json.loads(localstorage.get(f"hltvresult{event}", default="[]"))
            if len(res) != len(oldres):
                text = title + " 结果有更新"
                for i in range(len(oldres), len(res)):
                    text += f"\n{res[i][0]} {res[i][2]} {res[i][1]}"
                for groupid in config.cs_group_list:
                    await bot.send_msg(
                        message_type="group",
                        group_id=groupid,
                        message=text
                    )
                localstorage.set(f"hltvresult{event}", json.dumps(res))
                event_update(event)

@updategame.handle()
async def updategame_function():
    await update_events()