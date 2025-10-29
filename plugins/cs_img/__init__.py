from nonebot import get_plugin_config
from nonebot.plugin import PluginMetadata
from nonebot import require

output = require("utils").output

import os
from pathlib import Path
from pyppeteer import launch
import asyncio
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


def path_to_file_url(path):
    absolute_path = os.path.abspath(path)
    
    if os.name == 'nt':
        absolute_path = '/' + absolute_path.replace('\\', '/')
    return 'file://' + absolute_path

async def screenshot_html_to_png(url, width, height):
    browser = await launch(headless=True, args=['--no-sandbox', '--disable-setuid-sandbox'])
    page = await browser.newPage()
    await page.setViewport({'width': width, 'height': height})
    await page.goto(url)
    await asyncio.sleep(1)
    image = await page.screenshot()
    await browser.close()
    return image

def get_elo_info(pvpScore, seasonId = 21):
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

async def gen_rank_image1(datas, min_value, max_value, title, format):
    html = rank_content[0]
    sum = 0
    for (steamid, value) in datas:
        score = (value[0] - min_value) / (max_value - min_value)
        temp_html = rank_content[1]
        temp_html = temp_html.replace('_AVATAR_', path_to_file_url(os.path.join("avatar", f"{steamid}.png")))
        temp_html = temp_html.replace('_COLOR_', red_to_green_color(score))
        temp_html = temp_html.replace('_LEN_', f"{round(500 * score)}")
        temp_html = temp_html.replace('_LEFTPX_', f"10")
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

async def gen_rank_image2(datas, min_value, max_value, title, format):
    html = rank_content[0]
    sum = 0
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

async def gen_matches_image(datas, steamid, name):
    green = "#4CAF50"
    red = "#F44336"
    gray = "#9E9E9E"
    html = matches_content[0]
    html = html.replace("_avatar_", path_to_file_url(os.path.join("avatar", f"{steamid}.png")))
    html = html.replace("_name_", normalize('NFKC', name))
    for match in datas:
        (mid,steamid,seasonId,mapName,team,winTeam,score1,score2,pwRating,we,timeStamp,kill,death,assist,duration,mode,pvpScore,pvpStars,pvpScoreChange,pvpMvp,isgroup,greenMatch,entryKill,headShot,headShotRatio,flashTeammate,flashSuccess,twoKill,threeKill,fourKill,fiveKill,vs1,vs2,vs3,vs4,vs5,dmgArmor,dmgHealth,adpr,rws,teamId,throwsCnt,snipeNum,firstDeath) = match
        temp_html = matches_content[1]
        myScore = score1 if team == 1 else score2
        opScore = score2 if team == 1 else score1
        Result = 2 if team == winTeam else (1 if (winTeam != 1 and winTeam != 2) else 0)
        temp_html = temp_html.replace("_SCORERESULT_", ["负", "平", "胜"][Result])
        temp_html = temp_html.replace("_TIME_", datetime.datetime.fromtimestamp(timeStamp).strftime("%m-%d %H:%M"))
        temp_html = temp_html.replace("_SCORE1_", f"{myScore}")
        temp_html = temp_html.replace("_SCORE2_", f"{opScore}")
        temp_html = temp_html.replace("_SCORECOLOR_", [red, gray, green][Result])
        temp_html = temp_html.replace("_MAP_", mapName)
        temp_html = temp_html.replace("_TYPE_", mode)
        temp_html = temp_html.replace("_RT_",f"{pwRating: .2f}")
        temp_html = temp_html.replace("_RTCOLOR_", green if pwRating > 1 else red)
        temp_html = temp_html.replace("_K_", f"{kill}")
        temp_html = temp_html.replace("_D_", f"{death}")
        temp_html = temp_html.replace("_A_", f"{assist}")
        temp_html = temp_html.replace("_WE_", f"{we: .1f}")
        temp_html = temp_html.replace("_WECOLOR_", green if we > 8 else red)
        temp_html = temp_html.replace("_GROUPDISPLAY_", "inline" if isgroup else "none")
        pool, color, arc = get_elo_info(pvpScore, seasonId)
        temp_html = temp_html.replace("_POOLCOLOR_", color)
        temp_html = temp_html.replace("_ARC_", f"{113.1 * (1 - arc)}")
        temp_html = temp_html.replace("_POOL_", pool)
        temp_html = temp_html.replace("_DELTA_", f"{pvpScoreChange:+}")

        html += temp_html
    html += matches_content[2]

    with tempfile.NamedTemporaryFile(mode='w', encoding='utf-8', suffix=".html", dir="temp", delete=False) as temp_file:
        temp_file.write(html)
        temp_file.close()
        img = await screenshot_html_to_png(path_to_file_url(temp_file.name), 570, 100 + len(datas) * 80)
        os.remove(temp_file.name)
    return BytesIO(img)

async def gen_stats_image(result):
    (steamid, _, name, pvpScore, cnt, kd, winRate, pwRating, avgWe, kills, deaths, assists, rws, adr, headShotRatio, entryKillRatio, vs1WinRate, lasttime, _) = result
    html = data_content
    html = html.replace("_avatar_", path_to_file_url(os.path.join("avatar", f"{steamid}.png")))
    html = html.replace("_name_", normalize('NFKC', name))
    html = html.replace("_WE_", f"{avgWe: .1f}")
    html = html.replace("_Rating_",f"{pwRating: .2f}")
    html = html.replace("_ELO_", f"{pvpScore}")
    html = html.replace("_cnt_", f"{cnt}")
    html = html.replace("_winRate_", f"{winRate * 100 : .0f}%")
    html = html.replace("_RWS_", f"{rws: .2f}")
    html = html.replace("_ADR_", f"{adr: .2f}")
    html = html.replace("_KD_", f"{kd: .2f}")
    html = html.replace("_headShotRatio_", f"{headShotRatio * 100 : .0f}%")
    html = html.replace("_entryKillRatio_", f"{entryKillRatio * 100 : .0f}%")
    html = html.replace("_vs1WinRate_", f"{vs1WinRate * 100 : .0f}%")
    html = html.replace("_avgK_", "nan" if cnt == 0 else f"{kills / cnt : .2f}")
    html = html.replace("_avgD_", "nan" if cnt == 0 else f"{deaths / cnt : .2f}")
    html = html.replace("_avgA_", "nan" if cnt == 0 else f"{assists / cnt : .2f}")
    html = html.replace("_LastTime_", datetime.datetime.fromtimestamp(lasttime).strftime("%y-%m-%d %H:%M"))

    pool, color, arc = get_elo_info(pvpScore)
    
    html = html.replace("_COLOR_", color)
    html = html.replace("_ARC_", f"{226.2 * (1 - arc)}")
    html = html.replace("_POOL_", pool)
    with tempfile.NamedTemporaryFile(mode='w', encoding='utf-8', suffix=".html", dir="temp", delete=False) as temp_file:
        temp_file.write(html)
        temp_file.close()
        img = await screenshot_html_to_png(path_to_file_url(temp_file.name), 480, 560)
        os.remove(temp_file.name)
    return BytesIO(img)

def red_to_green_color(score):
    red = 1.0 - score
    green = score
    blue = 0.2
    
    return f"rgb({round(red*255)},{round(green*255)},{round(blue*255)})"
