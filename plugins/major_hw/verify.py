
from collections import defaultdict
import re
import numpy as np

def parse_simulation_results(file_path: str) -> tuple[dict[tuple[frozenset[str], frozenset[str], frozenset[str]], int], int]:
    """
    解析模拟结果文件，返回每个组合及其出现频率的字典
    格式: {('3-0': set, '3-1/3-2': set, '0-3': set): frequency}
    """
    results: dict[tuple[frozenset[str], frozenset[str], frozenset[str]], int] = defaultdict(int)
    total_simulations = 0
    pattern = r"3-0: (.*?) \| 3-1/3-2: (.*?) \| 0-3: (.*?): (\d+)/\d+"

    with open(file_path, 'r') as file:
        for line in file:
            match = re.match(pattern, line)
            if match:
                three_zero = set(t.strip() for t in match.group(1).split(','))
                three_one_two = set(t.strip() for t in match.group(2).split(','))
                zero_three = set(t.strip() for t in match.group(3).split(','))
                count = int(match.group(4))

                key = (frozenset(three_zero), frozenset(three_one_two), frozenset(zero_three))
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
    total_simulations = sum(results.values())

    for (three_zero, three_one_two, zero_three), count in results.items():
        correct = 0
        correct += len(set(combo['3-0']) & set(three_zero))
        correct += len(set(combo['3-1/3-2']) & set(three_one_two))
        correct += len(set(combo['0-3']) & set(zero_three))
        correct_counts.extend([correct] * count)

    correct_counts_np = np.array(correct_counts)
    prob_ge5 = np.mean(correct_counts_np >= 5)
    expected_value = np.mean(correct_counts_np)

    return correct_counts, prob_ge5, expected_value
