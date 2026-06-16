from __future__ import annotations

PLAYOFF_CATEGORIES = ["4强", "2强", "冠军"]
PLAYOFF_CATEGORY_SLOTS = {
    "4强": 4,
    "2强": 2,
    "冠军": 1,
}
PLAYOFF_STATUS_LABELS = {
    "correct": "对",
    "wrong": "错",
    "pending": "待定",
}


def playoff_category_status(
    teams: list[str],
    category: str,
    winners: dict[str, list[str]],
    eliminated: set[str],
) -> str:
    slots = PLAYOFF_CATEGORY_SLOTS[category]
    required = (slots + 1) // 2
    picked = teams[:slots]
    if len(picked) < slots:
        return "pending"

    winner_set = set(winners.get(category, []))
    picked_set = set(picked)
    correct_count = len(picked_set & winner_set)
    eliminated_count = sum(1 for team in picked if team in eliminated)
    possible_count = slots - eliminated_count

    if correct_count >= required:
        return "correct"
    if len(winner_set) >= slots:
        return "wrong"
    if possible_count < required:
        return "wrong"
    return "pending"


def validate_playoff_bracket(
    quad: list[str],
    semi: list[str],
    final: list[str],
    matchups: list[list[str]],
) -> str | None:
    if len(quad) != 4 or len(set(quad)) != 4:
        return "4强必须是四支不同队伍"
    if len(semi) != 2 or len(set(semi)) != 2:
        return "2强必须是两支不同队伍"
    if len(final) != 1:
        return "冠军必须是一支队伍"
    if len(matchups) != 4 or any(len(matchup) != 2 for matchup in matchups):
        return "淘汰赛对阵配置不完整"

    matchup_sets = [set(matchup) for matchup in matchups]
    for index, matchup in enumerate(matchup_sets, start=1):
        picked = matchup & set(quad)
        if len(picked) != 1:
            return f"4强必须在第 {index} 场八强赛 {list(matchup)} 中选择且只选择一支队伍"

    upper_half = matchup_sets[0] | matchup_sets[1]
    lower_half = matchup_sets[2] | matchup_sets[3]
    if not set(semi).issubset(set(quad)):
        return "2强必须从你选择的4强队伍中产生"
    if len(set(semi) & upper_half) != 1 or len(set(semi) & lower_half) != 1:
        return "2强必须从两个半区各选择一支队伍"

    if final[0] not in semi:
        return "冠军必须从你选择的2强队伍中产生"
    return None
