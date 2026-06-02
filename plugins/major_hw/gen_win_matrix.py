from dataclasses import dataclass, replace
from functools import lru_cache
import json
import sys
import csv
from pathlib import Path
from nonebot import logger
from thefuzz import process


# 评分系统权重配置
VRS_WEIGHT = 0.3  # Valve评分系统权重
HLTV_WEIGHT = 0.7  # HLTV评分系统权重
SIGMA = 500
HLTV_EXP = 0.1
RATING_K_FACTORS = {
    "valve": 80.0,
    "value": 80.0,
    "hltv": 35.0,
}
MIN_RATING = 1.0
RECENT_WEIGHT_MIN = 0.85
RECENT_WEIGHT_MAX = 2.0
UPSET_BONUS_SCALE = 3.0
UPSET_BONUS_CAP = 2.25


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
    alias: list[str]
    rating: tuple[float, ...]


    def __str__(self) -> str:
        return str(self.name)

    def __hash__(self) -> int:
        return self.id


@lru_cache(maxsize=None)
def win_probability(
    a: Team,
    b: Team,
    sigma: tuple[int, ...] = (SIGMA, SIGMA),
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
    v1, h1 = a.rating[0], max(a.rating[1], MIN_RATING)
    v2, h2 = b.rating[0], max(b.rating[1], MIN_RATING)

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
    teams: list[Team],
    sigma: tuple[int, ...] = (SIGMA, SIGMA),
    hltv_exp: float = HLTV_EXP,
) -> dict[str, dict[str, float]]:
    """
    计算所有队伍之间的胜率矩阵
    """
    win_matrix: dict[str, dict[str, float]] = {}

    for team1 in teams:
        win_matrix[team1.name] = {}
        for team2 in teams:
            if team1 != team2:
                win_matrix[team1.name][team2.name] = win_probability(
                    team1, team2, sigma, hltv_exp
                )

    return win_matrix


def print_win_matrix(win_matrix: dict[str, dict[str, float]], teams: list[Team]) -> None:
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


def load_teams(file_path: str | Path) -> list[Team]:
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


def load_system_names(file_path: str | Path) -> list[str]:
    """Return rating system names in the same order as Team.rating."""
    with open(file_path) as file:
        data = json.load(file)
    return list(data["systems"].keys())


def parse_score_weight(score: str) -> float:
    """Give clean BO3 wins a small boost while keeping BO1/map scores neutral."""
    try:
        winner_score, loser_score = (int(part) for part in score.split(":", 1))
    except ValueError:
        return 1.0

    if 0 <= loser_score < winner_score <= 3:
        return 1.0 + max(0, winner_score - loser_score - 1) * 0.15
    return 1.0


def expected_from_rating(
    winner_rating: float,
    loser_rating: float,
    system_name: str,
) -> float:
    if system_name.lower() == "hltv":
        winner_rating = max(winner_rating, MIN_RATING)
        loser_rating = max(loser_rating, MIN_RATING)
        return 1 / (1 + (loser_rating / winner_rating) ** HLTV_EXP)
    return 1 / (1 + 10 ** ((loser_rating - winner_rating) / SIGMA))


def apply_finished_matches_to_ratings(
    teams: list[Team],
    system_names: list[str],
    finish_match: list[tuple[str, str, str, str]],
    alias2full: dict[str, str],
    newest_first: bool = False,
) -> list[Team]:
    """Recalculate live ratings from finished match results before simulation."""
    team_by_name = {team.name: team for team in teams}
    matches = list(reversed(finish_match)) if newest_first else list(finish_match)

    def get_name(wuzzyname):
        match, _ = process.extractOne(wuzzyname, alias2full.keys())
        return alias2full[match]

    match_count = len(matches)
    for match_index, (winner_raw, loser_raw, score, _) in enumerate(matches):
        winner_name = get_name(winner_raw)
        loser_name = get_name(loser_raw)
        winner = team_by_name[winner_name]
        loser = team_by_name[loser_name]
        winner_ratings = list(winner.rating)
        loser_ratings = list(loser.rating)
        score_weight = parse_score_weight(score)
        recent_weight = (
            RECENT_WEIGHT_MAX
            if match_count <= 1
            else RECENT_WEIGHT_MIN
            + (RECENT_WEIGHT_MAX - RECENT_WEIGHT_MIN) * match_index / (match_count - 1)
        )

        for idx, system_name in enumerate(system_names):
            k_factor = RATING_K_FACTORS.get(system_name.lower())
            if k_factor is None:
                continue
            expected = expected_from_rating(
                winner_ratings[idx],
                loser_ratings[idx],
                system_name,
            )
            upset_weight = min(
                UPSET_BONUS_CAP,
                1 + max(0.0, 0.5 - expected) * UPSET_BONUS_SCALE,
            )
            delta = k_factor * score_weight * recent_weight * upset_weight * (1 - expected)
            winner_ratings[idx] = max(MIN_RATING, winner_ratings[idx] + delta)
            loser_ratings[idx] = max(MIN_RATING, loser_ratings[idx] - delta)

        team_by_name[winner_name] = replace(winner, rating=tuple(winner_ratings))
        team_by_name[loser_name] = replace(loser, rating=tuple(loser_ratings))

    return [team_by_name[team.name] for team in teams]


def save_win_matrix_to_csv(win_matrix: dict[str, dict[str, float]], teams: list[Team], file_path: str) -> None:
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


def gen_win_matrix(
    file_path: Path | str,
    finish_match: list[tuple[str, str, str, str]],
    newest_first: bool = False,
):

    teams = load_teams(file_path)
    system_names = load_system_names(file_path)

    alias2full = {}
    for team in teams:
        alias2full[team.name] = team.name
        for alias in team.alias:
            alias2full[alias] = team.name
    def get_name(wuzzyname):
        # 模糊匹配得到准确名称
        match, _ = process.extractOne(wuzzyname, alias2full.keys())
        return alias2full[match]
    teams = apply_finished_matches_to_ratings(
        teams,
        system_names,
        finish_match,
        alias2full,
        newest_first=newest_first,
    )
    win_probability.cache_clear()

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

