from ai_runtime.image_archive import image_segment
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

from unicodedata import normalize
from dataclasses import dataclass, field
import json
import re
import asyncio
import gzip
import time
from pathlib import Path
from typing import Sequence
from sqlalchemy import update, select

from .gen_win_matrix import gen_win_matrix, resolve_team_name
from .simulate import simulate
from .verify import parse_simulation_results, evaluate_combination
from .playoff_homework import validate_playoff_bracket
from . import chart as hw_chart
from .virtual_users import (
    get_registry,
    register_virtual_user,
    resolve_virtual_user,
    virtual_display_name,
)
from .stars import (
    add_starred_uids,
    assign_homework_ranks,
    get_starred_uids,
    list_starred_uids,
    remove_starred_uids,
)

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

    async def get_stage_snapshots(self, stage: str) -> list[MajorHWSnapshot]:
        """取某阶段全部作业概率快照，按 uid / 场次 / 创建时间升序排列。

        同一 uid 同一 match_count 可能因「赛中改作业」（homework_text 属于复合主键）
        而存在多条记录，去重交给 ``chart.build_series`` 按 created_at 取最新。
        """
        async with async_session_factory() as session:
            stmt = (
                select(MajorHWSnapshot)
                .where(MajorHWSnapshot.stage == stage)
                .order_by(
                    MajorHWSnapshot.uid,
                    MajorHWSnapshot.match_count,
                    MajorHWSnapshot.created_at,
                )
            )
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
    # 统一名称解析：精确/大小写/去标点优先，最后模糊匹配兜底
    return resolve_team_name(wuzzyname, alias2full)


def _validate_loaded_results(parsed_results, parsed_total: float):
    """忽略切换阶段后遗留的旧 result.txt，避免队伍集合不一致时抛 KeyError。"""
    if config.major_stage != "playoffs" and major_teams:
        team_to_bit = getattr(parsed_results, "team_to_bit", {})
        if team_to_bit and set(team_to_bit.keys()) != set(major_teams):
            logger.warning(
                f"result.txt 队伍列表与当前阶段 {major_stage_name} 不匹配，已忽略陈旧模拟结果"
            )
            return {}, 0.0
    return parsed_results, parsed_total


file_path = "result.txt"
_simulation_update_task: asyncio.Task | None = None
_simulation_update_queue: list[list[tuple[str, str, str, str]]] = []


logger.info(f"{major_stage_name}, {major_teams}")
results = {}
total_simulations = 0.0
try:
    results, total_simulations = _validate_loaded_results(*parse_simulation_results(file_path))
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
hwchart = on_command("作业走势", aliases={"作业折线图", "作业曲线"}, priority=10, block=True)
hwmine = on_command("我的作业走势", aliases={"我的作业折线图", "我的作业曲线"}, priority=10, block=True)
allrank = on_command("赛事作业结果", priority=10, block=True)
hwupd = on_command("更新作业", priority=10, block=True, permission=SUPERUSER)
simupd = on_command("更新模拟", priority=10, block=True, permission=SUPERUSER)
hwout = on_command("作业导出", priority=10, block=True, permission=SUPERUSER)
hwstreamer = on_command("主播作业", priority=10, block=True, permission=SUPERUSER)
hwstar = on_command("作业打星", aliases={"打星"}, priority=10, block=True, permission=SUPERUSER)

HWSTREAMER_USAGE = """/主播作业 <主播名或uid> <队伍1,队伍2,...>
录入或更新当前阶段的主播作业（复用模糊匹配与合法性校验）
/主播作业 列表
查看已登记的虚拟主播
/主播作业 添加 <uid> <名称> <头像URL> [别名1,别名2]
热注册新主播（写入 local_storage，不改默认配置）"""

HWSTAR_USAGE = """用法：
/作业打星 列表 —— 查看当前赛事打🌟名单（未购通行证，不占名次）
/作业打星 @某人 [更多@ / QQ号 / 主播名] —— 加入打🌟名单
/作业打星 取消 @某人 [更多@ / QQ号 / 主播名] —— 移出打🌟名单
同一届 Major 的 stage1/stage2/stage3/playoffs 共享名单，不同赛事彼此隔离。"""

STAR_LIST_TOKENS = {"列表", "list", "ls"}
STAR_ADD_TOKENS = {"添加", "add"}
STAR_REMOVE_TOKENS = {"取消", "移除", "删除", "del", "remove", "rm"}


async def _major_member_name(bot: Bot, gid: str, uid: str) -> str:
    """排行/导出显示名：虚拟主播优先用配置昵称，普通用户回退群名片。"""
    virtual_name = await virtual_display_name(uid)
    if virtual_name:
        return virtual_name
    try:
        return await getcard(bot, gid, uid)
    except Exception:
        logger.info(f"fail to get {uid}")
        return str(uid)


async def _major_text_target(arg: Message) -> str | None:
    """命令文本参数解析为虚拟 uid；非虚拟用户返回 None。"""
    text = normalize("NFKC", arg.extract_plain_text()).strip() if arg else ""
    if not text:
        return None
    user = await resolve_virtual_user(text)
    return user.uid if user else None



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
查看赛事整体结果
/作业打星 列表
（管理员）查看当前赛事打🌟名单（未购通行证，不占名次）
/作业打星 @某人 [更多目标]
（管理员）加入打🌟名单；/作业打星 取消 @某人 移出""")
    else:
        await hwhelp.finish(f"""当前状态 {major_stage_name}
/做作业 team30,team30,team31/team32,team31/team32,team31/team32,team31/team32,team31/team32,team31/team32,team03,team03
添加作业，可用队伍 {major_teams}
/查看作业 [@某人|主播名]
查看自己、某人或虚拟主播的作业概率
/作业排名
查看当前作业排名
/作业详情 [@某人|主播名]
查看自己、某人或虚拟主播的正确率变化截图
/作业走势 [@某人|我|主播名] [log]
看全员或指定玩家通过率随每场结束的折线图（可混用 @ / 我 / 主播名，log/对数切对数坐标）
/我的作业走势 [@某人|主播名] [log]
只看自己的走势；附带 @ 或主播名可一键对比
/赛事作业结果
查看赛事整体结果
/主播作业 <主播名或uid> <队伍1,队伍2,...>
（管理员）录入或更新虚拟主播作业
/作业打星 列表
（管理员）查看当前赛事打🌟名单（未购通行证，不占名次）
/作业打星 @某人 [更多目标]
（管理员）加入打🌟名单；/作业打星 取消 @某人 移出""")

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
async def hwsee_function(message: MessageEvent, arg: Message = CommandArg()):
    uid = message.get_user_id()
    for seg in message.get_message():
        if seg.type == "at" and seg.data['qq'] != 'all':
            uid = seg.data['qq']
    text = normalize('NFKC', arg.extract_plain_text()).strip() if arg else ""
    if text:
        target_uid = await _major_text_target(arg)
        if target_uid is None:
            await hwsee.finish(f"未找到虚拟主播 {text}，可用 /主播作业 列表 查看")
        uid = target_uid
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
        await hwrank.finish(image_segment(screenshot))

    res = await db.get_all_hw(major_stage_name)
    res = sorted(res, key=lambda x: x.winrate, reverse=True)
    starred_uids = await get_starred_uids(config.major_name)
    ranks = assign_homework_ranks([member.uid for member in res], starred_uids)
    text = f"{major_stage_name} 作业排行"
    for member, (starred, rank) in zip(res, ranks):
        name = await _major_member_name(bot, gid, member.uid)
        prefix = "🌟 " if starred else f"{rank}. "
        text += f"\n{prefix}{name} 通过率 {member.winrate}"
    await hwrank.finish(text)

@hwdetail.handle()
async def hwdetail_function(bot: Bot, message: GroupMessageEvent, arg: Message = CommandArg()):
    gid = message.get_session_id().split('_')[1]
    uid = message.get_user_id()
    for seg in message.get_message():
        if seg.type == "at" and seg.data['qq'] != 'all':
            uid = seg.data['qq']
    text = normalize('NFKC', arg.extract_plain_text()).strip() if arg else ""
    if text:
        target_uid = await _major_text_target(arg)
        if target_uid is None:
            await hwdetail.finish(f"未找到虚拟主播 {text}，可用 /主播作业 列表 查看")
        uid = target_uid

    screenshot = await _get_major_detail_screenshot(gid, uid)
    if screenshot:
        await hwdetail.finish(image_segment(screenshot))
    await hwdetail.finish("生成作业详情截图失败，请稍后重试")

CHART_PLAYOFF_HINT = (
    "当前为 {stage}（淘汰赛）阶段，暂不支持作业走势折线图；"
    "可用 /作业排名 或 /作业详情 查看概率变化。"
)
CHART_EMPTY_HINT = "当前阶段暂无作业概率快照，等下一场比赛结束或重新提交作业后再试。"
CHART_SELF_EMPTY_HINT = "你当前阶段暂无作业概率快照，等下一场比赛结束或重新提交作业后再试。"
CHART_TARGET_EMPTY_HINT = "指定玩家当前阶段暂无作业概率快照，等下一场比赛结束或重新提交作业后再试。"
CHART_FAILED_HINT = "生成作业走势折线图失败，请稍后重试。"
CHART_UNKNOWN_TARGET_HINT = "未识别目标：{targets}（支持 @群友、我、虚拟主播名或 log/线性）"
CHART_PARTIAL_HINT = "未找到 {targets} 的作业概率快照，已跳过并继续出图。"

CHART_LOG_TOKENS = {"log", "对数", "对数坐标", "log10", "logscale"}
CHART_LINEAR_TOKENS = {"linear", "线性", "线性坐标", "lin"}
CHART_SELF_TOKENS = {"我", "me", "self", "自己"}
CHART_TOKEN_SPLIT = re.compile(r"[\s,，、]+")
CHART_QQ_UID = re.compile(r"^\d{5,13}$")


@dataclass
class ChartRequest:
    """``/作业走势`` 与 ``/我的作业走势`` 的统一参数解析结果。"""

    scale: str = hw_chart.SCALE_LINEAR
    uids: list[str] = field(default_factory=list)
    unknown: list[str] = field(default_factory=list)


def chart_text_tokens(arg: Message | None) -> list[str]:
    """命令参数的纯文本 token（NFKC 规范化，按空白/中英文逗号/顿号切分）。"""

    text = normalize("NFKC", arg.extract_plain_text()).strip() if arg else ""
    return [token for token in CHART_TOKEN_SPLIT.split(text) if token]


def parse_chart_scale(arg: Message | None) -> str:
    """解析 ``/作业走势`` 的可选坐标模式参数，默认线性。"""

    for token in chart_text_tokens(arg):
        lowered = token.casefold()
        if lowered in CHART_LOG_TOKENS:
            return hw_chart.SCALE_LOG
        if lowered in CHART_LINEAR_TOKENS:
            return hw_chart.SCALE_LINEAR
    return hw_chart.SCALE_LINEAR


def chart_at_targets(arg: Message | None) -> list[str]:
    """只从命令参数 ``arg`` 里取 ``@``，绝不读整条消息。

    触发机器人的前置 ``@bot`` 不在 ``CommandArg()`` 里；若从
    ``message.get_message()`` 取 ``at`` 会把它误当成要画图的玩家。
    """

    targets: list[str] = []
    for segment in arg or []:
        if segment.type != "at":
            continue
        qq = str(segment.data.get("qq") or "").strip()
        if not qq or qq == "all":
            continue
        targets.append(qq)
    return targets


async def parse_chart_request(arg: Message | None, sender_uid: str) -> ChartRequest:
    """统一解析坐标参数与目标人（``@`` / 我 / 虚拟主播名 / 纯数字 uid）。"""

    request = ChartRequest()
    seen: set[str] = set()

    def add(uid: str) -> None:
        uid = str(uid).strip()
        if not uid or uid in seen:
            return
        seen.add(uid)
        request.uids.append(uid)

    for qq in chart_at_targets(arg):
        add(qq)

    for token in chart_text_tokens(arg):
        lowered = token.casefold()
        if lowered in CHART_LOG_TOKENS:
            request.scale = hw_chart.SCALE_LOG
            continue
        if lowered in CHART_LINEAR_TOKENS:
            request.scale = hw_chart.SCALE_LINEAR
            continue
        if lowered in CHART_SELF_TOKENS:
            add(sender_uid)
            continue
        virtual_user = await resolve_virtual_user(token)
        if virtual_user is not None:
            add(virtual_user.uid)
            continue
        if CHART_QQ_UID.match(token):
            add(token)
            continue
        request.unknown.append(token)
    return request


def parse_star_action(arg: Message | None) -> tuple[str, list[str], list[str]]:
    """解析 ``/作业打星`` 参数：``(action, at_uids, text_tokens)``。

    ``action`` 为 ``list`` / ``add`` / ``remove``；只从 ``CommandArg()`` 取 ``@``，
    绝不读整条消息，避免把前置 ``@bot`` 当成目标。
    """

    at_uids = chart_at_targets(arg)
    tokens = chart_text_tokens(arg)
    action = "add"
    if tokens:
        head = tokens[0].casefold()
        if head in STAR_LIST_TOKENS:
            action = "list"
            tokens = tokens[1:]
        elif head in STAR_REMOVE_TOKENS:
            action = "remove"
            tokens = tokens[1:]
        elif head in STAR_ADD_TOKENS:
            action = "add"
            tokens = tokens[1:]
    return action, at_uids, tokens


async def parse_star_targets(
    at_uids: Sequence[str],
    tokens: Sequence[str],
) -> tuple[list[str], list[str]]:
    """把 ``@`` / QQ 号 / 虚拟主播名解析成 UID 列表，返回 ``(uids, unknown)``。"""

    uids: list[str] = []
    unknown: list[str] = []
    seen: set[str] = set()

    def add(uid: str) -> None:
        value = str(uid).strip()
        if value and value not in seen:
            seen.add(value)
            uids.append(value)

    for qq in at_uids:
        add(qq)
    for token in tokens:
        virtual_user = await resolve_virtual_user(token)
        if virtual_user is not None:
            add(virtual_user.uid)
            continue
        if CHART_QQ_UID.match(token):
            add(token)
            continue
        unknown.append(token)
    return uids, unknown


def _star_gid(message: MessageEvent) -> str | None:
    parts = str(message.get_session_id()).split("_")
    if len(parts) > 1 and parts[0] == "group":
        return parts[1]
    return None


async def _star_member_lines(bot: Bot, gid: str | None, uids: list[str]) -> list[str]:
    lines: list[str] = []
    for uid in uids:
        name = await _major_member_name(bot, gid or "", uid)
        lines.append(f"🌟 {name}（{uid}）" if name != uid else f"🌟 {uid}")
    return lines


async def _star_list_text(bot: Bot, gid: str | None, event_name: str) -> str:
    uids = await list_starred_uids(event_name)
    if not uids:
        return f"{event_name} 暂无打🌟成员。\n{HWSTAR_USAGE}"
    lines = await _star_member_lines(bot, gid, uids)
    return f"{event_name} 打🌟名单（{len(uids)} 人，未购通行证，不占名次）：\n" + "\n".join(lines) + f"\n{HWSTAR_USAGE}"


@hwstar.handle()
async def hwstar_function(bot: Bot, message: MessageEvent, arg: Message = CommandArg()):
    event_name = config.major_name
    action, at_uids, tokens = parse_star_action(arg)
    gid = _star_gid(message)

    if action == "list":
        await hwstar.finish(await _star_list_text(bot, gid, event_name))
    if not at_uids and not tokens:
        await hwstar.finish(await _star_list_text(bot, gid, event_name))

    targets, unknown = await parse_star_targets(at_uids, tokens)
    if unknown:
        await hwstar.finish(f"未识别目标：{'、'.join(unknown)}\n{HWSTAR_USAGE}")
    if not targets:
        await hwstar.finish(HWSTAR_USAGE)

    if action == "remove":
        removed, current = await remove_starred_uids(event_name, targets)
        if removed:
            lines = await _star_member_lines(bot, gid, removed)
            head = f"已移出打🌟名单 {len(removed)} 人：\n" + "\n".join(lines)
        else:
            head = "目标均不在当前打🌟名单内"
    else:
        added, current = await add_starred_uids(event_name, targets)
        if added:
            lines = await _star_member_lines(bot, gid, added)
            head = f"已加入打🌟名单 {len(added)} 人：\n" + "\n".join(lines)
        else:
            head = "目标均已在当前打🌟名单内"

    if current:
        lines = await _star_member_lines(bot, gid, current)
        tail = f"当前名单（{len(current)} 人）：\n" + "\n".join(lines)
    else:
        tail = "当前名单为空"
    await hwstar.finish(f"{head}\n{tail}")


async def _major_chart_names(bot: Bot, gid: str, uids: list[str]) -> dict[str, str]:
    """并发解析展示名：主播用配置昵称、群友用群名片；单个失败只回退 uid。"""

    unique = list(dict.fromkeys(str(uid) for uid in uids if str(uid)))
    if not unique:
        return {}
    resolved = await asyncio.gather(
        *(_major_member_name(bot, gid, uid) for uid in unique), return_exceptions=True
    )
    return {
        uid: uid if isinstance(name, BaseException) or not name else str(name)
        for uid, name in zip(unique, resolved)
    }


def chart_empty_hint(requested: list[str], sender_uid: str) -> str:
    """按「全员 / 只看自己 / 指定他人」给出不同措辞，避免误导。"""

    if not requested:
        return CHART_EMPTY_HINT
    if requested == [sender_uid]:
        return CHART_SELF_EMPTY_HINT
    return CHART_TARGET_EMPTY_HINT


async def send_major_chart(
    matcher,
    bot: Bot,
    message: GroupMessageEvent,
    request: ChartRequest,
    *,
    include_self: bool = False,
):
    """``/作业走势`` / ``/我的作业走势`` 共用的筛选、渲染与发送逻辑。"""

    if config.major_stage == "playoffs":
        await matcher.finish(CHART_PLAYOFF_HINT.format(stage=major_stage_name))

    sender_uid = str(message.get_user_id())
    requested = list(request.uids)
    if include_self and sender_uid not in requested:
        requested.insert(0, sender_uid)

    if request.unknown:
        await matcher.finish(
            CHART_UNKNOWN_TARGET_HINT.format(targets="、".join(request.unknown))
        )

    snapshots = await db.get_stage_snapshots(major_stage_name)
    if not snapshots:
        await matcher.finish(chart_empty_hint(requested, sender_uid))

    gid = message.get_session_id().split('_')[1]
    available = {str(snapshot.uid) for snapshot in snapshots}
    selected = [uid for uid in requested if uid in available]
    missing = [uid for uid in requested if uid not in available]
    if requested and not selected:
        await matcher.finish(chart_empty_hint(requested, sender_uid))

    rows = (
        [snapshot for snapshot in snapshots if str(snapshot.uid) in set(selected)]
        if requested
        else snapshots
    )
    names = await _major_chart_names(
        bot, gid, [*selected, *missing] if requested else sorted(available)
    )
    # 部分目标没有快照：先提示跳过了谁，再照常给其余目标出图，不中断。
    if missing:
        await matcher.send(
            CHART_PARTIAL_HINT.format(targets="、".join(names.get(uid, uid) for uid in missing))
        )

    series = hw_chart.build_series(
        rows,
        names,
        starred_uids=await get_starred_uids(config.major_name),
    )
    if not series:
        await matcher.finish(chart_empty_hint(requested, sender_uid))

    finished_matches = json.loads(
        await local_storage.get(f"hltvresult{config.major_event_id}", default="[]")
    )
    try:
        png_bytes = await asyncio.to_thread(
            hw_chart.render_homework_chart,
            series,
            finished_matches,
            stage_name=major_stage_name,
            scale=request.scale,
            resolver=get_name,
        )
    except Exception:
        logger.exception("failed to render major homework chart")
        await matcher.finish(CHART_FAILED_HINT)
    await matcher.finish(image_segment(png_bytes))


@hwchart.handle()
async def hwchart_function(bot: Bot, message: GroupMessageEvent, arg: Message = CommandArg()):
    request = await parse_chart_request(arg, str(message.get_user_id()))
    await send_major_chart(hwchart, bot, message, request)


@hwmine.handle()
async def hwmine_function(bot: Bot, message: GroupMessageEvent, arg: Message = CommandArg()):
    request = await parse_chart_request(arg, str(message.get_user_id()))
    await send_major_chart(hwmine, bot, message, request, include_self=True)

@hwstreamer.handle()
async def hwstreamer_function(message: MessageEvent, arg: Message = CommandArg()):
    text = normalize('NFKC', arg.extract_plain_text()).strip()
    if not text:
        await hwstreamer.finish(HWSTREAMER_USAGE)

    head = text.split(None, 1)[0]

    if head == "列表":
        users = (await get_registry()).all()
        if not users:
            await hwstreamer.finish("暂无已登记的虚拟主播，可用 /主播作业 添加 <uid> <名称> <头像URL> [别名] 登记")
        lines = [
            f"{user.name}（{user.uid}）头像 {user.avatar or 'QQ 默认'} 别名 {list(user.aliases)}"
            for user in users
        ]
        await hwstreamer.finish("已登记虚拟主播：\n" + "\n".join(lines))

    if head == "添加":
        parts = text.split(None, 4)
        if len(parts) < 4:
            await hwstreamer.finish("格式：/主播作业 添加 <uid> <名称> <头像URL> [别名1,别名2]")
        _, streamer_uid, streamer_name, streamer_avatar = parts[:4]
        aliases = [
            item.strip()
            for item in re.split(r"[,，;；|/]+", parts[4] if len(parts) > 4 else "")
            if item.strip()
        ]
        try:
            user = await register_virtual_user(streamer_uid, streamer_name, streamer_avatar, aliases)
        except ValueError as error:
            await hwstreamer.finish(f"登记失败：{error}")
        await hwstreamer.finish(
            f"已登记虚拟主播 {user.name}（{user.uid}），头像 {user.avatar or 'QQ 默认'}，别名 {list(user.aliases)}"
        )

    if len(major_teams) == 0:
        await hwstreamer.finish("当前阶段不可做作业")
    target, _, teams_text = text.partition(" ")
    if not teams_text.strip():
        await hwstreamer.finish(HWSTREAMER_USAGE)
    user = await resolve_virtual_user(target)
    if user is None:
        await hwstreamer.finish(f"未找到虚拟主播 {target}，请先 /主播作业 添加 <uid> <名称> <头像URL> 登记")

    teams = [get_name(team) for team in teams_text.split(",")]
    await hwstreamer.send(f"{user.name} 模糊匹配得到队伍 {teams}，请仔细核对")
    if config.major_stage == "playoffs":
        if len(teams) != 7:
            await hwstreamer.finish("请输入 7 个队伍：4强x4,2强x2,冠军x1")
        quad = teams[:4]
        semi = teams[4:6]
        final = teams[6:]
        if error := validate_playoff_bracket(quad, semi, final, playoff_matchups):
            await hwstreamer.finish(f"作业不是合法锦标赛结果：{error}")
        await db.add_hw(user.uid, major_stage_name + "-quad", json.dumps(quad))
        await db.add_hw(user.uid, major_stage_name + "-semi", json.dumps(semi))
        await db.add_hw(user.uid, major_stage_name + "-final", json.dumps(final))
        await calc_val(user.uid)
        await hwstreamer.finish(f"成功添加 {user.name} 的淘汰赛作业")
    else:
        if len(teams) != 10 or len(set(teams)) != 10:
            await hwstreamer.finish("请输入十只不同队伍")
        teams_json = json.dumps(teams)
        await db.add_hw(user.uid, major_stage_name, teams_json)
        await hwstreamer.send("成功添加预测，开始计算概率")
        calc_result = await calc_val(user.uid)
        assert calc_result is not None
        prob_ge5, expected_value = calc_result
        await save_current_homework_snapshot(user.uid, teams_json, prob_ge5, expected_value)
        await hwstreamer.finish(
            f"{user.name} >= 5 的概率 = {prob_ge5:.6f}，正确数期望 = {expected_value:.6f}"
        )


@hwupd.handle()
async def hwupd_function():
    global results
    await hwupd.send("开始读取模拟结果")

    results, total_simulations = _validate_loaded_results(*parse_simulation_results(file_path))
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
        return await get_screenshot("/major-homework", token, width=820)
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
                message=Message([MessageSegment.text(title + "\n"), image_segment(screenshot)]),
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
    starred_uids = await get_starred_uids(config.major_name)
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
        text += await _major_member_name(bot, gid, uid)
        if str(uid).strip() in starred_uids:
            text += " 🌟"
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
        out.append({'nickname': await _major_member_name(bot, gid, member.uid), 'teams': json.loads(member.teams)})
    await hwout.finish(json.dumps({
        'stage': stage,
        'games': json.loads(await local_storage.get(f"hltvresult{eventid}", default="[]")),
        'homework': out
    }))
