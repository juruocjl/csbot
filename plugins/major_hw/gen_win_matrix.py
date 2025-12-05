from dataclasses import dataclass
from functools import lru_cache
from typing import TYPE_CHECKING, Tuple, List, Dict
import json
import sys
import csv
from pathlib import Path
from nonebot import logger
from fuzzywuzzy import process

if TYPE_CHECKING:
    from pathlib import Path

# 评分系统权重配置
VRS_WEIGHT = 0.3  # Valve评分系统权重
HLTV_WEIGHT = 0.7  # HLTV评分系统权重
SIGMA = 500
HLTV_EXP = 0.1


@dataclass(frozen=True)
class Team:
    """
    队伍类，存储队伍的基本信息和评分
    Attributes:
        id: 队伍唯一标识符
        name: 队伍名称
        seed: 种子排名
        rating: 评分元组 (VRS评分, HLTV评分)
    """
    id: int
    name: str
    seed: int
    alias: List[str]
    rating: Tuple[int, ...]


    def __str__(self) -> str:
        return str(self.name)

    def __hash__(self) -> int:
        return self.id


@lru_cache(maxsize=None)
def win_probability(
    a: Team,
    b: Team,
    sigma: Tuple[int, ...] = (SIGMA, SIGMA),
    hltv_exp: float = HLTV_EXP,
) -> float:
    """
    计算队伍a战胜队伍b的概率

    Args:
        a: 队伍a
        b: 队伍b
        sigma: Elo公式的sigma
        hltv_exp: HLTV 比值的指数（原本固定为 0.67）
    """
    # 获取两支队伍的VRS和HLTV评分
    v1, h1 = a.rating[0], a.rating[1]
    v2, h2 = b.rating[0], b.rating[1]

    # 使用Elo公式计算VRS胜率
    d = sigma[0]
    p_vrs = 1 / (1 + 10 ** ((v2 - v1) / d))

    # ⭐ HLTV 指数已可调
    p_hltv = 1 / (1 + (h2 / h1) ** hltv_exp)

    # 加权平均胜率
    p = VRS_WEIGHT * p_vrs + HLTV_WEIGHT * p_hltv
    p /= (VRS_WEIGHT + HLTV_WEIGHT) if (VRS_WEIGHT + HLTV_WEIGHT) > 0 else 1

    return p


def calculate_win_matrix(
    teams: List[Team],
    sigma: Tuple[int, ...] = (SIGMA, SIGMA),
    hltv_exp: float = HLTV_EXP,
) -> Dict[str, Dict[str, float]]:
    """
    计算所有队伍之间的胜率矩阵
    """
    win_matrix = {}

    for team1 in teams:
        win_matrix[team1.name] = {}
        for team2 in teams:
            if team1 != team2:
                win_matrix[team1.name][team2.name] = win_probability(
                    team1, team2, sigma, hltv_exp
                )

    return win_matrix


def print_win_matrix(win_matrix: Dict[str, Dict[str, float]], teams: List[Team]) -> None:
    """
    打印胜率矩阵
    """
    print("胜率矩阵（行队名 vs. 列队名 -> 行队名获胜概率）:")

    COLUMN_WIDTH = 10  # 每列固定10个字符宽度

    # 打印表头
    header = "队伍".center(COLUMN_WIDTH)
    for team in teams:
        team_name = team.name
        if len(team_name) > COLUMN_WIDTH - 2:
            team_name = team_name[:COLUMN_WIDTH - 3] + "..."
        header += team_name.center(COLUMN_WIDTH)
    print(header)

    # 分隔线
    separator = "-" * COLUMN_WIDTH
    for _ in teams:
        separator += "-" * COLUMN_WIDTH
    print(separator)

    # 每一行
    for team1 in teams:
        team1_name = team1.name
        if len(team1_name) > COLUMN_WIDTH - 2:
            team1_name = team1_name[:COLUMN_WIDTH - 3] + "..."
        row = team1_name.center(COLUMN_WIDTH)

        for team2 in teams:
            if team1 == team2:
                row += "-".center(COLUMN_WIDTH)
            else:
                win_rate = win_matrix[team1.name][team2.name]
                row += f"{win_rate:.2f}".center(COLUMN_WIDTH)
        print(row)


def load_teams(file_path: str | Path) -> List[Team]:
    """从JSON文件加载队伍数据"""
    with open(file_path) as file:
        data = json.load(file)

    teams = []
    for i, (team_name, team_data) in enumerate(data["teams"].items()):
        rating = tuple(
            (eval(sys_v))(team_data[sys_k])  # noqa: S307
            for sys_k, sys_v in data["systems"].items()
        )
        teams.append(
            Team(
                id=i,
                name=team_name,
                seed=team_data["seed"],
                alias=team_data["alias"],
                rating=rating
            )
        )

    return teams


def save_win_matrix_to_csv(win_matrix: Dict[str, Dict[str, float]], teams: List[Team], file_path: str) -> None:
    """
    将胜率矩阵输出为 CSV 文件，方便在 Excel 打开
    """
    with open(file_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)

        # 写表头
        header = ["Team"] + [team.name for team in teams]
        writer.writerow(header)

        # 写每一行
        for team1 in teams:
            row = [team1.name]
            for team2 in teams:
                if team1 == team2:
                    row.append("-")
                else:
                    row.append(f"{win_matrix[team1.name][team2.name]:.4f}")
            writer.writerow(row)


def gen_win_matrix(file_path : Path | str, finish_match : List[Tuple[str, str, str, str]]):

    teams = load_teams(file_path)

    alias2full = {}
    for team in teams:
        alias2full[team.name] = team.name
        for alias in team.alias:
            alias2full[alias] = team.name
    def get_name(wuzzyname):
        # 模糊匹配得到准确名称
        match, _ = process.extractOne(wuzzyname, alias2full.keys())
        return alias2full[match]
    # ⭐ 现在你可以在这里调节 HLTV 指数
    win_matrix = calculate_win_matrix(teams, hltv_exp=HLTV_EXP)

    for teama, teamb, _, _ in finish_match:
        teama = get_name(teama)
        teamb = get_name(teamb)
        win_matrix[teama][teamb] = 1
        win_matrix[teamb][teama] = 0
        

    # print_win_matrix(win_matrix, teams)
    save_win_matrix_to_csv(win_matrix, teams, "win_matrix.csv")
    logger.info("胜率矩阵已保存为 win_matrix.csv")

