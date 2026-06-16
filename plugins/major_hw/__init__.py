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

require("models")
from ..models import MajorHW, MajorHWSnapshot, MajorSimulationSnapshot

require("utils")
from ..utils import async_session_factory
from ..utils import local_storage
from ..utils import getcard

from thefuzz import process
from unicodedata import normalize
import json
import asyncio
import gzip
import time
from pathlib import Path
from sqlalchemy import update, select

from .gen_win_matrix import gen_win_matrix
from .simulate import simulate
from .verify import parse_simulation_results, evaluate_combination
from .playoff_homework import validate_playoff_bracket

from .config import Config

__plugin_meta__ = PluginMetadata(
    name="major_hw",
    description="",
    usage="",
    config=Config,
)

config = get_plugin_config(Config)


class DataManager:
    async def add_hw(self, uid: str, stage: str, teams: str):
        """
        对应 INSERT OR REPLACE
        注意：原逻辑会将 winrate 和 expval 重置为 0.0，
        session.merge 会完全覆盖旧记录，行为一致。
        """
        async with async_session_factory() as session:
            async with session.begin():
                new_hw = MajorHW(
                    uid=uid, 
                    stage=stage, 
                    teams=teams, 
                    winrate=0.0, 
                    expval=0.0
                )
                await session.merge(new_hw)

    async def get_uid_hw(self, uid: str, stage: str) -> MajorHW | None:
        async with async_session_factory() as session:
            # 复合主键查询：session.get 接收一个元组 (uid, stage)
            result = await session.get(MajorHW, (uid, stage))
            return result  # 返回的是 MajorHW 对象，可以直接用 result.teams 访问

    async def set_uid_val(self, uid: str, stage: str, new_winrate: float, new_expval: float):
        """
        合并了原来的两条 UPDATE 语句，一次性更新
        """
        async with async_session_factory() as session:
            async with session.begin():
                # 使用 update 语句直接更新，效率更高
                stmt = (
                    update(MajorHW)
                    .where(MajorHW.uid == uid, MajorHW.stage == stage)
                    .values(winrate=new_winrate, expval=new_expval)
                )
                await session.execute(stmt)

    async def get_all_hw(self, stage: str) -> list[MajorHW]:
        async with async_session_factory() as session:
            stmt = select(MajorHW).where(MajorHW.stage == stage)
            result = await session.execute(stmt)
            return list(result.scalars().all())

    async def save_simulation_snapshot(
        self,
        stage: str,
        event_id: int,
        match_count: int,
        latest_match_id: str | None,
        total_weight: float,
        homework_rows: list[tuple[str, str, float, float]],
        result_size: int = 0,
    ) -> None:
        created_at = int(time.time())
        async with async_session_factory() as session:
            async with session.begin():
                await session.merge(MajorSimulationSnapshot(
                    stage=stage,
                    match_count=match_count,
                    event_id=event_id,
                    latest_match_id=latest_match_id,
                    created_at=created_at,
                    total_weight=total_weight,
                    result_size=result_size,
                    result_gzip=b"",
                ))
                for uid, homework_text, winrate, expval in homework_rows:
                    await session.merge(MajorHWSnapshot(
                        stage=stage,
                        match_count=match_count,
                        uid=uid,
                        homework_text=homework_text,
                        created_at=created_at,
                        winrate=winrate,
                        expval=expval,
                    ))

db = DataManager()

major_stage_name = f"{config.major_name}-{config.major_stage}"
simulation_result_dir = Path("data") / "major_simulations" / major_stage_name

teamfile = Path(".") / "assets" / f"{major_stage_name}.json"
major_teams = []
alias2full = {}
playoff_matchups: list[list[str]] = []
try:
    with open(teamfile, "r", encoding="utf-8") as f:
        data = json.load(f)
        major_teams = list(data['teams'].keys())
        playoff_matchups = data.get("matchups", [])
        for team_name, team_data in data['teams'].items():
            alias2full[team_name] = team_name
            for alias in team_data['alias']:
                alias2full[alias] = team_name
except:
    logger.error("fail to load teams")


def get_name(wuzzyname):
    # 模糊匹配得到准确名称
    match, _ = process.extractOne(wuzzyname, alias2full.keys())
    return alias2full[match]

file_path = "result.txt"
_simulation_update_task: asyncio.Task | None = None
_simulation_update_queue: list[list[tuple[str, str, str, str]]] = []


logger.info(f"{major_stage_name}, {major_teams}")
results = {}
total_simulations = 0.0
try:
    results, total_simulations = parse_simulation_results(file_path)
    logger.info(f"已加载 {total_simulations} 个模拟结果")
except:
    logger.error("未能加载模拟结果")


def homework_teams_text(teams_json: str) -> str:
    teams = json.loads(teams_json)
    if len(teams) == 10:
        normalized = {
            "3-0": sorted(teams[:2]),
            "3-1/3-2": sorted(teams[2:8]),
            "0-3": sorted(teams[8:]),
        }
    else:
        normalized = teams
    return json.dumps(normalized, ensure_ascii=False, separators=(",", ":"))


def save_simulation_result_file(match_count: int, result_bytes: bytes) -> Path:
    simulation_result_dir.mkdir(parents=True, exist_ok=True)
    path = simulation_result_dir / f"{match_count}.txt.gz"
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    tmp_path.write_bytes(gzip.compress(result_bytes, compresslevel=9))
    tmp_path.replace(path)
    return path


async def save_current_homework_snapshot(uid: str, teams_json: str, winrate: float, expval: float) -> None:
    finished_matches = json.loads(await local_storage.get(f"hltvresult{config.major_event_id}", default="[]"))
    latest_match_id = None
    if finished_matches and len(finished_matches[0]) >= 4:
        latest_match_id = str(finished_matches[0][3])
    await db.save_simulation_snapshot(
        stage=major_stage_name,
        event_id=config.major_event_id,
        match_count=len(finished_matches),
        latest_match_id=latest_match_id,
        total_weight=total_simulations,
        homework_rows=[(uid, homework_teams_text(teams_json), winrate, expval)],
    )


hwhelp = on_command("作业帮助", priority=10, block=True)
hwadd = on_command("做作业", priority=10, block=True)
hwsee = on_command("查看作业", priority=10, block=True)
hwrank = on_command("作业排名", priority=10, block=True)
hwdetail = on_command("作业详情", priority=10, block=True)
allrank = on_command("赛事作业结果", priority=10, block=True)
hwupd = on_command("更新作业", priority=10, block=True, permission=SUPERUSER)
simupd = on_command("更新模拟", priority=10, block=True, permission=SUPERUSER)
hwout = on_command("作业导出", priority=10, block=True, permission=SUPERUSER)

def to_emoji(val):
    if val == 1.0:
        return "✅"
    elif val == 0.0:
        return "❌"
    else:
        return "❔"

@hwhelp.handle()
async def hwhelp_funtion():
    if config.major_stage == "playoffs":
        await hwhelp.finish(f"""当前状态 {major_stage_name}
/做作业 Aurora,FURIA,Spirit,Vitality,Aurora,Spirit,Spirit
添加淘汰赛作业，格式为 4强x4,2强x2,冠军x1。作业必须是合法锦标赛结果：4强每场八强赛选一队，2强两个半区各选一队，冠军来自2强。
八强对阵 {playoff_matchups}
可用队伍 {major_teams}
/查看作业 [@某人]
查看自己或某人作业概率
/作业排名
查看当前作业排名
/赛事作业结果
查看赛事整体结果""")
    else:
        await hwhelp.finish(f"""当前状态 {major_stage_name}
/做作业 team30,team30,team31/team32,team31/team32,team31/team32,team31/team32,team31/team32,team31/team32,team03,team03
添加作业，可用队伍 {major_teams}
/查看作业 [@某人]
查看自己或某人作业概率
/作业排名
查看当前作业排名
/作业详情 [@某人]
查看自己或某人的正确率变化截图
/赛事作业结果
查看赛事整体结果""")

async def calc_val(uid: str) -> tuple[float, float] | None:
    if config.major_stage == "playoffs":
        result: list[tuple[str, str, str, str]] = json.loads(await local_storage.get(f"hltvresult{config.major_event_id}", default="[]"))
        result.reverse()
        playoff_start = 33
        if res := await db.get_uid_hw(uid, major_stage_name + "-quad"):
            teams = json.loads(res.teams)
            win_teams = [get_name(team1) for team1, _, _, _ in result[playoff_start:playoff_start + 4]]
            loss_teams = [get_name(team2) for _, team2, _, _ in result[playoff_start:playoff_start + 4]]
            if len(result) >= 37:
                correct = float(sum(1 for team in teams if team in win_teams))
            else:
                correct = float('nan')
            prob_ge2 = float("nan")
            if sum(1 for team in teams if team in loss_teams) > 2:
                prob_ge2 = 0.0
            if sum(1 for team in teams if team in win_teams) >= 2:
                prob_ge2 = 1.0
            await db.set_uid_val(uid, major_stage_name + "-quad", prob_ge2, correct)
        if res := await db.get_uid_hw(uid, major_stage_name + "-semi"):
            teams = json.loads(res.teams)
            win_teams = [get_name(team1) for team1, _, _, _ in result[playoff_start + 4:playoff_start + 6]]
            loss_teams = [get_name(team2) for _, team2, _, _ in result[playoff_start:playoff_start + 6]]
            if len(result) >= 39:
                correct = sum(1 for team in teams if team in win_teams)
            else:
                correct = float('nan')
            prob_ge1 = float("nan")
            if sum(1 for team in teams if team in loss_teams) > 1:
                prob_ge1 = 0.0
            if sum(1 for team in teams if team in win_teams) >= 1:
                prob_ge1 = 1.0
            await db.set_uid_val(uid, major_stage_name + "-semi", prob_ge1, correct)
        if res := await db.get_uid_hw(uid, major_stage_name + "-final"):
            teams = json.loads(res.teams)
            win_teams = [get_name(team1) for team1, _, _, _ in result[playoff_start + 6:playoff_start + 7]]
            loss_teams = [get_name(team2) for _, team2, _, _ in result[playoff_start:playoff_start + 7]]
            if len(result) >= 40:
                correct = sum(1 for team in teams if team in win_teams)
            else:
                correct = float('nan')
            prob_ge1 = float("nan")
            if sum(1 for team in teams if team in loss_teams) > 0:
                prob_ge1 = 0.0
            if sum(1 for team in teams if team in win_teams) >= 1:
                prob_ge1 = 1.0
            await db.set_uid_val(uid, major_stage_name + "-final", prob_ge1, correct)
    else:
        if res := await db.get_uid_hw(uid, major_stage_name):
            teams = json.loads(res.teams)
            combo = {
                '3-0': teams[: 2],
                '3-1/3-2': teams[2: 8],
                '0-3': teams[8: ]
            }
            correct_counts, prob_ge5, expected_value = evaluate_combination(combo, results)
            await db.set_uid_val(uid, major_stage_name, prob_ge5, expected_value)
            return prob_ge5, expected_value
    return None

@hwadd.handle()
async def hwadd_function(message: MessageEvent, arg: Message = CommandArg()):
    if len(major_teams) == 0:
        await hwadd.finish("当前阶段不可做作业")
    uid = message.get_user_id()
    teams = []
    for team in normalize('NFKC', arg.extract_plain_text()).split(','):
        teams.append(get_name(team))
    await hwadd.send(f"模糊匹配得到队伍 {teams}，请仔细核对")
    if config.major_stage == "playoffs":
        if len(teams) != 7:
            await hwadd.finish("请输入 7 个队伍：4强x4,2强x2,冠军x1")
        quad = teams[:4]
        semi = teams[4:6]
        final = teams[6:]
        if error := validate_playoff_bracket(quad, semi, final, playoff_matchups):
            await hwadd.finish(f"作业不是合法锦标赛结果：{error}")
        await db.add_hw(uid, major_stage_name + "-quad", json.dumps(quad))
        await db.add_hw(uid, major_stage_name + "-semi", json.dumps(semi))
        await db.add_hw(uid, major_stage_name + "-final", json.dumps(final))
        await calc_val(uid)
        await hwadd.finish("成功添加淘汰赛作业")
    else:
        if len(teams) != 10 or len(set(teams)) != 10:
            await hwadd.finish("请输入十只不同队伍")
        teams_json = json.dumps(teams)
        await db.add_hw(uid, major_stage_name, teams_json)
        await hwadd.send("成功添加预测，开始计算概率")
        calc_result = await calc_val(uid)
        assert calc_result is not None
        prob_ge5, expected_value = calc_result
        await save_current_homework_snapshot(uid, teams_json, prob_ge5, expected_value)
        await hwadd.finish(f">= 5 的概率 = {prob_ge5:.6f}，正确数期望 = {expected_value:.6f}")

@hwsee.handle()
async def hwsee_function(message: MessageEvent):
    uid = message.get_user_id()
    for seg in message.get_message():
        if seg.type == "at" and seg.data['qq'] != 'all':
            uid = seg.data['qq']
    if config.major_stage == "playoffs":
        if quad := await db.get_uid_hw(uid, major_stage_name + "-quad"):
            if semi := await db.get_uid_hw(uid, major_stage_name + "-semi"):
                if final := await db.get_uid_hw(uid, major_stage_name + "-final"):
                    text = f"{to_emoji(quad.winrate)}四强：{json.loads(quad.teams)}\n"
                    text += f"{to_emoji(semi.winrate)}决赛：{json.loads(semi.teams)}\n"
                    text += f"{to_emoji(final.winrate)}冠军：{json.loads(final.teams)}\n"
                    await hwsee.finish(text.strip())
    else:
        if res := await db.get_uid_hw(uid, major_stage_name):
            prob_ge5, expected_value = res.winrate, res.expval
            teams = json.loads(res.teams)
            text = f"3-0 {teams[:2]}\n"
            text += f"3-1/3-2 {teams[2:8]}\n"
            text += f"0-3 {teams[8:]}\n"
            text += f">= 5 的概率 = {prob_ge5:.6f}，正确数期望 = {expected_value:.6f}"
            await hwsee.finish(text)
    await hwsee.finish("该用户未提交作业")

@hwrank.handle()
async def hwrank_function(bot: Bot, message: GroupMessageEvent):
    gid = message.get_session_id().split('_')[1]
    screenshot = await _get_major_rank_screenshot(gid)
    if screenshot:
        await hwrank.finish(MessageSegment.image(screenshot))

    res = await db.get_all_hw(major_stage_name)
    res = sorted(res, key=lambda x: x.winrate, reverse=True)
    text = f"{major_stage_name} 作业排行"
    for member in res:
        try:
            uid = member.uid
            name = await getcard(bot, gid, uid)
            text+= f"\n{name} 通过率 {member.winrate}"
        except:
            logger.info(f"fail to get {member.uid}")
    await hwrank.finish(text)

@hwdetail.handle()
async def hwdetail_function(bot: Bot, message: GroupMessageEvent):
    gid = message.get_session_id().split('_')[1]
    uid = message.get_user_id()
    for seg in message.get_message():
        if seg.type == "at" and seg.data['qq'] != 'all':
            uid = seg.data['qq']

    screenshot = await _get_major_detail_screenshot(gid, uid)
    if screenshot:
        await hwdetail.finish(MessageSegment.image(screenshot))
    await hwdetail.finish("生成作业详情截图失败，请稍后重试")

@hwupd.handle()
async def hwupd_function():
    global results
    await hwupd.send("开始读取模拟结果")

    results, total_simulations = parse_simulation_results(file_path)
    logger.info(f"已加载 {total_simulations} 个模拟结果")

    res = await db.get_all_hw(major_stage_name)
    await hwupd.send("开始重新计算所有作业")
    for member in res:
        await calc_val(member.uid)
    await hwupd.finish(f"成功计算 {len(res)} 份作业")

@simupd.handle()
async def calc_simulate():
    bot = get_bot()
    if _simulation_update_task is not None and not _simulation_update_task.done():
        await _enqueue_major_simulation(bot)
        await simupd.finish("已有模拟正在进行，已加入队列")
    await _enqueue_major_simulation(bot)
    await simupd.finish("已开始后台模拟")


async def _send_major_groups(bot: Bot, message: str):
    for groupid in config.cs_group_list:
        await bot.send_msg(
            message_type="group",
            group_id=groupid,
            message=message,
        )


async def _get_major_rank_screenshot(group_id: str | int) -> bytes | None:
    try:
        from ..cs_server import db as server_db
        from ..cs_server import get_screenshot

        token = await server_db.get_bot_token(str(group_id))
        return await get_screenshot("/major-homework", token, width=760)
    except Exception:
        logger.exception("failed to create major homework ranking screenshot")
        return None


async def _get_major_detail_screenshot(group_id: str | int, uid: str | int) -> bytes | None:
    try:
        from ..cs_server import db as server_db
        from ..cs_server import get_screenshot

        token = await server_db.get_bot_token(str(group_id))
        return await get_screenshot(f"/major-homework/user/{uid}", token, width=1040)
    except Exception:
        logger.exception("failed to create major homework detail screenshot")
        return None


async def _send_major_rank_groups(bot: Bot, title: str):
    for groupid in config.cs_group_list:
        screenshot = await _get_major_rank_screenshot(groupid)
        if screenshot:
            await bot.send_msg(
                message_type="group",
                group_id=groupid,
                message=Message([MessageSegment.text(title + "\n"), MessageSegment.image(screenshot)]),
            )
        else:
            await bot.send_msg(
                message_type="group",
                group_id=groupid,
                message=title + "\n生成作业排名截图失败，请稍后使用 /作业排名 重试",
            )


async def _run_major_simulation_once(bot: Bot, finished_matches: list[tuple[str, str, str, str]] | None = None):
    global results, total_simulations

    await _send_major_groups(bot, "开始重新模拟")
    if finished_matches is None:
        finished_matches = json.loads(await local_storage.get(f"hltvresult{config.major_event_id}", default="[]"))
    await asyncio.to_thread(gen_win_matrix, str(teamfile),
                            finished_matches,
                            newest_first=True)
    await asyncio.to_thread(simulate, teamfile, "result.txt", finished_matches, True)
    await _send_major_groups(bot, "新结果模拟完成")

    results, total_simulations = parse_simulation_results(file_path)
    logger.info(f"已加载 {total_simulations} 个模拟结果")

    res = await db.get_all_hw(major_stage_name)
    homework_rows: list[tuple[str, str, float, float]] = []
    for member in res:
        calc_result = await calc_val(member.uid)
        if calc_result is not None:
            prob_ge5, expected_value = calc_result
            homework_rows.append((member.uid, homework_teams_text(member.teams), prob_ge5, expected_value))
    latest_match_id = None
    if finished_matches and len(finished_matches[0]) >= 4:
        latest_match_id = str(finished_matches[0][3])
    result_bytes = Path(file_path).read_bytes()
    save_simulation_result_file(len(finished_matches), result_bytes)
    await db.save_simulation_snapshot(
        stage=major_stage_name,
        event_id=config.major_event_id,
        match_count=len(finished_matches),
        latest_match_id=latest_match_id,
        total_weight=total_simulations,
        homework_rows=homework_rows,
        result_size=len(result_bytes),
    )
    logger.info(
        f"已保存 {major_stage_name} 第 {len(finished_matches)} 场后的模拟快照，"
        f"作业快照 {len(homework_rows)} 条"
    )
    await _send_major_rank_groups(bot, f"成功计算 {len(res)} 份作业，当前作业排名")


async def _run_queued_major_simulation(bot: Bot):
    global _simulation_update_task

    try:
        while _simulation_update_queue:
            finished_matches = _simulation_update_queue.pop(0)
            try:
                await _run_major_simulation_once(bot, finished_matches)
            except Exception:
                logger.exception("major simulation failed")
                await _send_major_groups(bot, "新结果模拟失败，请查看日志")
                return
            if _simulation_update_queue:
                await _send_major_groups(bot, "检测到后续赛果，继续按下一场结果重新模拟")
    finally:
        _simulation_update_task = None


async def _enqueue_major_simulation(bot: Bot, finished_matches: list[tuple[str, str, str, str]] | None = None):
    global _simulation_update_task

    if finished_matches is None:
        finished_matches = json.loads(await local_storage.get(f"hltvresult{config.major_event_id}", default="[]"))
    _simulation_update_queue.append(list(finished_matches))

    if _simulation_update_task is not None and not _simulation_update_task.done():
        await _send_major_groups(bot, "模拟正在进行，已记录新赛果快照，完成后会按顺序再算一次")
        return

    _simulation_update_task = asyncio.create_task(_run_queued_major_simulation(bot))

async def event_update(event_id, finished_matches: list[tuple[str, str, str, str]] | None = None):

    if event_id == config.major_event_id:
        logger.info(f"{event_id} updated")
        bot = get_bot()
        
        if config.major_stage == "playoffs":
            res = await db.get_all_hw(major_stage_name + "-quad")
            for member in res:
                await calc_val(member.uid)
            await _send_major_rank_groups(bot, f"成功计算 {len(res)} 份作业，当前作业排名")
        else:
            await _enqueue_major_simulation(bot, finished_matches)

major_all_stages = [f"{config.major_name}-{x}" for x in ["stage1", "stage2", "stage3", "playoffs-quad", "playoffs-semi", "playoffs-final"]]

@allrank.handle()
async def allrank_function(bot: Bot, message: GroupMessageEvent):
    gid = message.get_session_id().split('_')[1]
    res: dict[str, dict[str, float]] = {}
    for stage in major_all_stages:
        allres = await db.get_all_hw(stage)
        for member in allres:
            if member.uid not in res:
                res[member.uid] = {}
            res[member.uid][stage] = member.winrate
    text = ""
    for uid, data in res.items():
        right = 0
        wrong = 0
        for stage in major_all_stages:
            if data.get(stage, float("nan")) == 1.0:
                right += 1
            elif data.get(stage, float("nan")) == 0.0:
                wrong += 1
        if right == 6:
            text += "💎 "
        elif right >= 3 and wrong != 0:
            text += "🥇 "
        elif wrong > 3:
            text += "🥈 "
        else:
            text += "❔ "
        for stage in major_all_stages:
            text += to_emoji(data.get(stage, float("nan")))
        text += " "
        text += await getcard(bot, gid, uid)
        text += "\n"
    await allrank.finish(text)

@hwout.handle()
async def hwout_function(bot: Bot, args: Message = CommandArg()):

    params = args.extract_plain_text().strip().split()
    gid = params[0]
    stage = params[1] if len(params) > 1 else major_stage_name
    eventid = params[2] if len(params) > 2 else config.major_event_id

    res = await db.get_all_hw(stage)
    out = []
    for member in res:
        out.append({'nickname': await getcard(bot, gid, member.uid), 'teams': json.loads(member.teams)})
    await hwout.finish(json.dumps({
        'stage': stage,
        'games': json.loads(await local_storage.get(f"hltvresult{eventid}", default="[]")),
        'homework': out
    }))
