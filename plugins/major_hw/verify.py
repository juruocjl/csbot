
from collections import defaultdict
import re


def parse_simulation_results(file_path: str) -> tuple[dict[tuple[frozenset[str], frozenset[str], frozenset[str]], float], float]:
    """
    解析模拟结果文件，返回每个组合及其出现频率的字典
    格式: {('3-0': set, '3-1/3-2': set, '0-3': set): frequency}
    """
    results: dict[tuple[frozenset[str], frozenset[str], frozenset[str]], float] = defaultdict(float)
    total_simulations = 0.0
    number = r"[0-9]+(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?"
    pattern = rf"3-0: (.*?) \| 3-1/3-2: (.*?) \| 0-3: (.*?): ({number})/{number}"

    def parse_team_set(raw: str) -> frozenset[str]:
        return frozenset(t.strip() for t in raw.split(',') if t.strip())

    with open(file_path, 'r') as file:
        for line in file:
            match = re.match(pattern, line)
            if match:
                three_zero = parse_team_set(match.group(1))
                three_one_two = parse_team_set(match.group(2))
                zero_three = parse_team_set(match.group(3))
                count = float(match.group(4))

                key = (three_zero, three_one_two, zero_three)
                results[key] += count
                total_simulations += count

    return results, total_simulations


def evaluate_combination(combo: dict, results: dict) -> tuple:
    """
    评估组合在模拟结果中的表现

    Args:
        combo: 要评估的组合
        results: 模拟结果字典

    Returns:
        tuple: (正确数列表, 正确数>=5的概率, 正确数期望)
    """
    correct_counts = []
    total_weight = sum(results.values())
    if total_weight <= 0:
        return correct_counts, 0.0, 0.0

    ge5_weight = 0.0
    expected_total = 0.0
    for (three_zero, three_one_two, zero_three), count in results.items():
        correct = 0
        correct += len(set(combo['3-0']) & set(three_zero))
        correct += len(set(combo['3-1/3-2']) & set(three_one_two))
        correct += len(set(combo['0-3']) & set(zero_three))
        if correct >= 5:
            ge5_weight += count
        expected_total += correct * count

    prob_ge5 = ge5_weight / total_weight
    expected_value = expected_total / total_weight
    return correct_counts, prob_ge5, expected_value
