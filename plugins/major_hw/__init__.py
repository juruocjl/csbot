from nonebot import get_plugin_config
from nonebot.plugin import PluginMetadata
from nonebot.adapters.onebot.v11 import Message, MessageEvent, GroupMessageEvent, MessageSegment
from nonebot.params import CommandArg
from nonebot.permission import SUPERUSER
from nonebot.adapters import Bot
from nonebot import get_bot
from nonebot import on_command
from nonebot import require
from nonebot import logger

get_cursor = require("utils").get_cursor
localstorage = require("utils").localstorage

import json
import asyncio
from pathlib import Path

from .gen_win_matrix import gen_win_matrix
from .simulate import simulate
from .verify import parse_simulation_results, evaluate_combination

from .config import Config

__plugin_meta__ = PluginMetadata(
    name="major_hw",
    description="",
    usage="",
    config=Config,
)

config = get_plugin_config(Config)

class DataManager:
    def __init__(self):
        cursor = get_cursor()
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS major_hw (
            uid TEXT,
            stage TEXT,
            teams TEXT,
            winrate REAL,
            expval REAL,
            PRIMARY KEY (uid, stage)
        )
        """)
    def add_hw(self, uid: str, stage: str, teams: str):
        cursor = get_cursor()
        cursor.execute("""
        INSERT OR REPLACE INTO major_hw (uid, stage, teams, winrate, expval) 
        VALUES (?, ?, ?, ?, ?)
        """, (uid, stage, teams, 0.0, 0.0))
    def get_uid_hw(self, uid: str, stage: str):
        cursor = get_cursor()
        cursor.execute("SELECT * FROM major_hw WHERE uid = ? AND stage = ?", (uid, stage))
        return cursor.fetchone()
    def set_uid_val(self, uid: str, stage: str, new_winrate: float, new_expval: float):
        cursor = get_cursor()
        cursor.execute("UPDATE major_hw SET winrate = ? WHERE uid = ? AND stage = ?", (new_winrate, uid, stage))
        cursor.execute("UPDATE major_hw SET expval = ? WHERE uid = ? AND stage = ?", (new_expval, uid, stage))
    def get_all_hw(self, stage: str):
        cursor = get_cursor()
        cursor.execute("SELECT * FROM major_hw WHERE stage = ?", (stage, ))
        return cursor.fetchall()

db = DataManager()

teamfile = Path(".") / "assets" / f"{config.major_stage}.json"
major_teams = []
try:
    with open(teamfile, "r") as f:
        data = json.load(f)
        major_teams = list(data['teams'].keys())
except:
    logger.fail("fail to load teams")
file_path = "result.txt"


logger.info(f"{config.major_stage}, {major_teams}")
results, total_simulations = parse_simulation_results(file_path)
logger.info(f"已加载 {total_simulations} 个模拟结果")


hwhelp = on_command("作业帮助", priority=10, block=True)
hwadd = on_command("做作业", priority=10, block=True)
hwsee = on_command("查看作业", priority=10, block=True)
hwrank = on_command("作业排名", priority=10, block=True)
hwupd = on_command("更新作业", priority=10, block=True, permission=SUPERUSER)
simupd = on_command("更新模拟", priority=10, block=True, permission=SUPERUSER)
hwout = on_command("作业导出", priority=10, block=True, permission=SUPERUSER)

@hwhelp.handle()
async def hwhelp_funtion():
    await hwhelp.finish(f"""当前状态 {config.major_stage}
/做作业 team30,team30,team31/team32,team31/team32,team31/team32,team31/team32,team31/team32,team31/team32,team03,team03
添加作业，可用队伍 {major_teams}
/查看作业 [@某人]
查看自己或某人作业概率
/作业排名
查看当前作业排名
""")

def calc_val(uid: str):
    if res := db.get_uid_hw(uid, config.major_stage):
        teams = json.loads(res[2])
        combo = {
            '3-0': teams[: 2],
            '3-1/3-2': teams[2: 8],
            '0-3': teams[8: ]
        }
        correct_counts, prob_ge5, expected_value = evaluate_combination(combo, results)
        db.set_uid_val(uid, config.major_stage, prob_ge5, expected_value)
        return prob_ge5, expected_value

@hwadd.handle()
async def hwadd_function(message: MessageEvent, arg: Message = CommandArg()):
    uid = message.get_user_id()
    teams = []
    for team in arg.extract_plain_text().split(','):
        team = team.strip()
        ok = False
        for nowteam in major_teams:
            if team.lower() == nowteam.lower():
                teams.append(nowteam)
                ok = True
                break
        if not ok:
            await hwadd.finish(f"未知队伍 {team}")
    logger.info(teams)
    if len(teams) != 10 or len(set(teams)) != 10:
        await hwadd.finish("请输入十只不同队伍")
    db.add_hw(uid, config.major_stage, json.dumps(teams))
    await hwadd.send("成功添加预测，开始计算概率")
    prob_ge5, expected_value = calc_val(uid)
    await hwadd.finish(f">= 5 的概率 = {prob_ge5:.6f}，正确数期望 = {expected_value:.6f}")

@hwsee.handle()
async def hwsee_function(message: MessageEvent):
    uid = message.get_user_id()
    for seg in message.get_message():
        if seg.type == "at" and seg.data['qq'] != 'all':
            uid = seg.data['qq']
    if res := db.get_uid_hw(uid, config.major_stage):
        prob_ge5, expected_value = res[3:]
        teams = json.loads(res[2])
        text = f"3-0 {teams[:2]}\n"
        text += f"3-1/3-2 {teams[2:8]}\n"
        text += f"0-3 {teams[8:]}\n"
        text += f">= 5 的概率 = {prob_ge5:.6f}，正确数期望 = {expected_value:.6f}"
        await hwsee.finish(text)

async def getcard(bot: Bot, gid: str, uid: str):
    info = await bot.get_group_member_info(group_id=gid, user_id=uid, no_cache=False)
    if info["card"]:
        return info["card"]
    return info["nickname"]

@hwrank.handle()
async def hwrank_function(bot: Bot, message: GroupMessageEvent):
    gid = message.get_session_id().split('_')[1]
    res = db.get_all_hw(config.major_stage)
    res = sorted(res, key=lambda x: x[3], reverse=True)
    text = f"{config.major_stage} 作业排行"
    for member in res:
        try:
            uid = member[0]
            name = await getcard(bot, gid, uid)
            text+= f"\n{name} 通过率 {member[3]}"
        except:
            logger.info(f"fail to get {member[0]}")
    await hwrank.finish(text)

@hwupd.handle()
async def hwupd_function():
    global results
    await hwupd.send("开始读取模拟结果")

    results, total_simulations = parse_simulation_results(file_path)
    logger.info(f"已加载 {total_simulations} 个模拟结果")

    res = db.get_all_hw(config.major_stage)
    await hwupd.send("开始重新计算所有作业")
    for member in res:
        calc_val(member[0])
    await hwupd.finish(f"成功计算 {len(res)} 份作业")

@simupd.handle()
async def calc_simulate():
    await asyncio.to_thread(gen_win_matrix, str(teamfile), json.loads(localstorage.get(f"hltvresult{config.major_event_id}", default="[]")))
    await asyncio.to_thread(simulate, teamfile)
    await simupd.finish("结果模拟完成")

async def event_update(event_id):
    if event_id == config.major_event_id:
        logger.info(f"{event_id} updated")
        bot = get_bot()
        
        for groupid in config.cs_group_list:
            await bot.send_msg(
                message_type="group",
                group_id=groupid,
                message="开始重新模拟"
            )
        
        await asyncio.to_thread(gen_win_matrix, str(teamfile), json.loads(localstorage.get(f"hltvresult{config.major_event_id}", default="[]")))
        await asyncio.to_thread(simulate, teamfile)

        for groupid in config.cs_group_list:
            await bot.send_msg(
                message_type="group",
                group_id=groupid,
                message="新结果模拟完成"
            )
        
        global results
        results, total_simulations = parse_simulation_results(file_path)
        logger.info(f"已加载 {total_simulations} 个模拟结果")

        res = db.get_all_hw(config.major_stage)
        for member in res:
            calc_val(member[0])
        
        for groupid in config.cs_group_list:
            await bot.send_msg(
                message_type="group",
                group_id=groupid,
                message=f"成功计算 {len(res)} 份作业"
            )

@hwout.handle()
async def hwout_function():
    res = db.get_all_hw(config.major_stage)
    out = []
    for member in res:
        out.append(await getcard(member[0]), json.loads(member[2]))
    await hwout.finish(json.dumps({
        'stage': config.major_stage,
        'games': json.loads(localstorage.get(f"hltvresult{config.major_event_id}", default="[]")),
        'homework': out
    }))