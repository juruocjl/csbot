"""S 段位等效分换算的纯逻辑模块。

完美天梯在 ``MatchStatsPW`` 中把 S 段位玩家统一存为 ``pvpScore = 2401``，
升星信息单独放在 ``pvpStars``。为了在排名与分数变化折线图中体现升星差异，
本模块在展示/排序层提供等效分与段位标注的换算，不修改数据库表结构，也不
回写原始 ``pvpScore`` / ``pvpStars`` 字段值。

仅依赖 Python 标准库，便于独立单元测试。
"""

from __future__ import annotations

S_RANK_MIN_SCORE = 2401
S_STAR_EQUIVALENT_POINTS = 25


def calc_effective_pvp_score(pvp_score: int | float | None, pvp_stars: int | float | None = 0) -> int:
    """把天梯原始分折算为可用于排序/展示的等效分。

    - ``pvp_score`` 为 ``None`` 或 ``<= 0``（未定段等异常数据）时返回 ``0``；
    - 达到 S 段位（``>= 2401``）时按 ``pvp_score + 25 * max(pvp_stars, 0)`` 折算，
      因此 2401 / 0 星 -> 2401、2401 / 1 星 -> 2426、2401 / 2 星 -> 2451；
    - 非 S 段位原样返回，避免影响 2400 及以下的正常分值；
      周末联赛等 ``pvp_score > 2401`` 且 ``pvp_stars == 0`` 的数据保持原值。
    """
    if pvp_score is None:
        return 0
    score = int(pvp_score)
    if score <= 0:
        return 0
    stars = max(int(pvp_stars or 0), 0)
    if score >= S_RANK_MIN_SCORE:
        return score + S_STAR_EQUIVALENT_POINTS * stars
    return score


def format_s_rank_badge_label(pvp_score: int | float | None, pvp_stars: int | float | None = 0) -> str | None:
    """生成 S 段位的段位附加标注（如 ``S ★2``），非标准 S 段位返回 ``None``。

    仅标准 S 段位（``pvp_score == 2401``，或 ``pvp_score > 2401`` 且 ``pvp_stars > 0``）
    生成标注；周末联赛 ``pvp_score > 2401`` 且 0 星、以及非 S 段位不加标注。
    """
    if pvp_score is None:
        return None
    score = int(pvp_score)
    stars = max(int(pvp_stars or 0), 0)
    if score == S_RANK_MIN_SCORE or (score > S_RANK_MIN_SCORE and stars > 0):
        return f"S ★{stars}"
    return None
