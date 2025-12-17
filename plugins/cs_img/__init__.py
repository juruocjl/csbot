from nonebot import get_plugin_config
from nonebot.plugin import PluginMetadata
from nonebot import require

require("utils")
from ..utils import output, path_to_file_url, screenshot_html_to_png

require("cs_db_upd")
from ..cs_db_upd import MatchStatsPW, SteamBaseInfo, SteamDetailInfo

import os
from pathlib import Path
import tempfile
from io import BytesIO
from unicodedata import normalize
import datetime

from .config import Config

__plugin_meta__ = PluginMetadata(
    name="cs_img",
    description="",
    usage="",
    config=Config,
)

config = get_plugin_config(Config)


with open(Path("assets") / "data.html", 'r', encoding='utf-8') as file:
    data_content = file.read()

with open(Path("assets") / "rank.html", 'r', encoding='utf-8') as file:
    rank_content = file.read().split("<!--SPLIT--->")

with open(Path("assets") / "matches.html", 'r', encoding='utf-8') as file:
    matches_content = file.read().split("<!--SPLIT--->")




def get_elo_info(pvpScore, seasonId = "S21"):
    if int(seasonId[1:]) >= 21:
        pool = "S"
        color = "#87CEFA"
        arc = 0
        if pvpScore == 0:
            pool = "?"
        elif pvpScore <= 1000:
            pool = "D"
            arc = pvpScore / 1000
        elif pvpScore <= 1150:
            pool = "C"
            arc = (pvpScore - 1000) / 150
        elif pvpScore <= 1300:
            pool = "C+"
            arc = (pvpScore - 1150) / 150
        elif pvpScore <= 1450:
            pool = "C+"
            arc = (pvpScore - 1300) / 150
            color = "#FFDF00"
        elif pvpScore <= 1600:
            pool = "B"
            arc = (pvpScore - 1450) / 150
        elif pvpScore <= 1750:
            pool = "B+"
            arc = (pvpScore - 1600) / 150
        elif pvpScore <= 1900:
            pool = "B+"
            arc = (pvpScore - 1750) / 150
            color = "#FFDF00"
        elif pvpScore <= 2050:
            pool = "A"
            arc = (pvpScore - 1900) / 150
        elif pvpScore <= 2200:
            pool = "A+"
            arc = (pvpScore - 2050) / 150
        elif pvpScore <= 2400:
            pool = "A+"
            arc = (pvpScore - 2200) / 200
            color = "#FFDF00"
        
        return pool, color, arc
    else:
        pool = "S"
        color = "#87CEFA"
        arc = 0
        if pvpScore == 0:
            pool = "?"
        elif pvpScore <= 1000:
            pool = "D"
            arc = pvpScore / 1000
        elif pvpScore <= 1200:
            pool = "D+"
            arc = (pvpScore - 1000) / 200
        elif pvpScore <= 1400:
            pool = "C"
            arc = (pvpScore - 1200) / 200
        elif pvpScore <= 1600:
            pool = "C+"
            arc = (pvpScore - 1400) / 200
        elif pvpScore <= 1800:
            pool = "B"
            arc = (pvpScore - 1600) / 200
        elif pvpScore <= 2000:
            pool = "B+"
            arc = (pvpScore - 1800) / 200
        elif pvpScore <= 2200:
            pool = "A"
            arc = (pvpScore - 2000) / 200
        elif pvpScore <= 2400:
            pool = "A+"
            arc = (pvpScore - 2200) / 200
        
        return pool, color, arc


async def gen_rank_image2(datas: list[tuple[int, tuple[float, int]]], min_value: float, max_value: float, title: str, format: str):
    html = rank_content[0]
    sum: float = 0
    zeroscore = - min_value / (max_value - min_value)
    for (steamid, value) in datas:
        score = (value[0] - min_value) / (max_value - min_value)
        temp_html = rank_content[1]
        temp_html = temp_html.replace('_AVATAR_', path_to_file_url(os.path.join("avatar", f"{steamid}.png")))
        temp_html = temp_html.replace('_COLOR_', red_to_green_color(score))
        if value[0] >= 0:
            temp_html = temp_html.replace('_LEN_', f"{round(500 * (score - zeroscore))}")
            temp_html = temp_html.replace('_LEFTPX_', f"{round(500 * zeroscore) + 10}")
        else:
            temp_html = temp_html.replace('_LEN_', f"{round(500 * (zeroscore - score))}")
            temp_html = temp_html.replace('_LEFTPX_', f"{round(500 * score) + 10}")

        if len(value) == 1:
            temp_html = temp_html.replace('_VALUE_', output(value[0], format))
        else:
            temp_html = temp_html.replace('_VALUE_', f"{output(value[0], format)} <span style='font-size:20px;'>{value[1]}场</span>")
        html += temp_html
        sum += value[0]
    html += rank_content[2]
    avg = sum / len(datas)
    score = (avg - min_value) / (max_value - min_value)
    html = html.replace("_AVG_", output(avg, format))
    html = html.replace("_AVGPOS_", f"{round(score * 500) + 98}")
    html = html.replace("_AVGLEN_", f"{round(len(datas) * 90) + 40}")
    html = html.replace("_TITLE_", title)
    with tempfile.NamedTemporaryFile(mode='w', encoding='utf-8', suffix=".html", dir="temp", delete=False) as temp_file:
        temp_file.write(html)
        temp_file.close()
        img = await screenshot_html_to_png(path_to_file_url(temp_file.name), 850, 200 + len(datas) * 90)
        os.remove(temp_file.name)
    return BytesIO(img)

async def gen_matches_image(datas: list[MatchStatsPW], steamid: str, name: str):
    green = "#4CAF50"
    red = "#F44336"
    gray = "#9E9E9E"
    html = matches_content[0]
    html = html.replace("_avatar_", path_to_file_url(os.path.join("avatar", f"{steamid}.png")))
    html = html.replace("_name_", normalize('NFKC', name))
    for match in datas:
        temp_html = matches_content[1]
        myScore = match.score1 if match.team == 1 else match.score2
        opScore = match.score2 if match.team == 1 else match.score1
        Result = 2 if match.team == match.winTeam else (1 if (match.winTeam != 1 and match.winTeam != 2) else 0)
        temp_html = temp_html.replace("_SCORERESULT_", ["负", "平", "胜"][Result])
        temp_html = temp_html.replace("_TIME_", datetime.datetime.fromtimestamp(match.timeStamp).strftime("%m-%d %H:%M"))
        temp_html = temp_html.replace("_SCORE1_", f"{myScore}")
        temp_html = temp_html.replace("_SCORE2_", f"{opScore}")
        temp_html = temp_html.replace("_SCORECOLOR_", [red, gray, green][Result])
        temp_html = temp_html.replace("_MAP_", match.mapName)
        temp_html = temp_html.replace("_TYPE_", match.mode)
        temp_html = temp_html.replace("_RT_",f"{match.pwRating: .2f}")
        temp_html = temp_html.replace("_RTCOLOR_", green if match.pwRating > 1 else red)
        temp_html = temp_html.replace("_K_", f"{match.kill}")
        temp_html = temp_html.replace("_D_", f"{match.death}")
        temp_html = temp_html.replace("_A_", f"{match.assist}")
        temp_html = temp_html.replace("_WE_", f"{match.we: .1f}")
        temp_html = temp_html.replace("_WECOLOR_", green if match.we > 8 else red)
        temp_html = temp_html.replace("_GROUPDISPLAY_", "inline" if match.isgroup else "none")
        pool, color, arc = get_elo_info(match.pvpScore, match.seasonId)
        temp_html = temp_html.replace("_POOLCOLOR_", color)
        temp_html = temp_html.replace("_ARC_", f"{113.1 * (1 - arc)}")
        temp_html = temp_html.replace("_POOL_", pool)
        temp_html = temp_html.replace("_DELTA_", f"{match.pvpScoreChange:+}")
        html += temp_html
    html += matches_content[2]

    with tempfile.NamedTemporaryFile(mode='w', encoding='utf-8', suffix=".html", dir="temp", delete=False) as temp_file:
        temp_file.write(html)
        temp_file.close()
        img = await screenshot_html_to_png(path_to_file_url(temp_file.name), 570, 100 + len(datas) * 80)
        os.remove(temp_file.name)
    return BytesIO(img)

async def gen_stats_image(baseinfo: SteamBaseInfo, detailinfo: SteamDetailInfo):
    html = data_content
    html = html.replace("_avatar_", path_to_file_url(os.path.join("avatar", f"{baseinfo.steamid}.png")))
    html = html.replace("_name_", normalize('NFKC', baseinfo.name))
    html = html.replace("_WE_", f"{detailinfo.we: .1f}")
    html = html.replace("_Rating_",f"{detailinfo.pwRating: .2f}")
    html = html.replace("_ELO_", f"{detailinfo.pvpScore}")
    html = html.replace("_cnt_", f"{detailinfo.cnt}")
    html = html.replace("_winRate_", f"{detailinfo.winRate * 100 : .0f}%")
    html = html.replace("_RWS_", f"{detailinfo.rws: .2f}")
    html = html.replace("_ADR_", f"{detailinfo.damagePerRound: .2f}")
    html = html.replace("_KAST_", f"{detailinfo.kastPerRound * 100: .0f}%")
    html = html.replace("_headShotRatio_", f"{detailinfo.headshotRate * 100 : .0f}%")
    html = html.replace("_entryKillRatio_", f"{detailinfo.firstRate * 100 : .0f}%")
    html = html.replace("_vs1WinRate_", f"{detailinfo.v1WinPercentage * 100 : .0f}%")

    html = html.replace("_FP_", f"{detailinfo.firePowerScore}")
    html = html.replace("_FPL_", f"{round((detailinfo.firePowerScore / 100) * 320)}")
    html = html.replace("_FPC_", red_to_green_color(detailinfo.firePowerScore / 100))

    html = html.replace("_MS_", f"{detailinfo.marksmanshipScore}")
    html = html.replace("_MSC_", red_to_green_color(detailinfo.marksmanshipScore / 100))

    html = html.replace("_FR_", f"{detailinfo.firstScore}")
    html = html.replace("_FRC_", red_to_green_color(detailinfo.firstScore / 100))

    html = html.replace("_FU_", f"{detailinfo.followUpShotScore}")
    html = html.replace("_FUC_", red_to_green_color(detailinfo.followUpShotScore / 100))

    html = html.replace("_vN_", f"{detailinfo.oneVnScore}")
    html = html.replace("_vNC_", red_to_green_color(detailinfo.oneVnScore / 100))

    html = html.replace("_IT_", f"{detailinfo.itemScore}")
    html = html.replace("_ITC_", red_to_green_color(detailinfo.itemScore / 100))

    html = html.replace("_SN_", f"{detailinfo.sniperScore}")
    html = html.replace("_SNC_", red_to_green_color(detailinfo.sniperScore / 100))
    
    html = html.replace("_LastTime_", datetime.datetime.fromtimestamp(baseinfo.lasttime).strftime("%y-%m-%d %H:%M"))

    pool, color, arc = get_elo_info(detailinfo.pvpScore)
    
    html = html.replace("_COLOR_", color)
    html = html.replace("_ARC_", f"{226.2 * (1 - arc)}")
    html = html.replace("_POOL_", pool)
    with tempfile.NamedTemporaryFile(mode='w', encoding='utf-8', suffix=".html", dir="temp", delete=False) as temp_file:
        temp_file.write(html)
        temp_file.close()
        img = await screenshot_html_to_png(path_to_file_url(temp_file.name), 480, 700)
        os.remove(temp_file.name)
    return BytesIO(img)

def red_to_green_color(score):
    red = 1.0 - score
    green = score
    blue = 0.2
    
    return f"rgb({round(red*255)},{round(green*255)},{round(blue*255)})"
