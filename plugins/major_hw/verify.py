
from collections import defaultdict
import json
import re


class SimulationResults(defaultdict):
    def __init__(self):
        super().__init__(float)
        self.team_to_bit: dict[str, int] = {}


def _mask_from_names(names, team_to_bit: dict[str, int]) -> int:
    mask = 0
    for name in names:
        mask |= team_to_bit[name]
    return mask


def parse_simulation_results(file_path: str) -> tuple[SimulationResults, float]:
    """
    解析模拟结果文件，返回每个组合及其出现频率的字典
    格式: {('3-0': set, '3-1/3-2': set, '0-3': set): frequency}
    """
    results = SimulationResults()
    total_simulations = 0.0
    number = r"[0-9]+(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?"
    pattern = rf"3-0: (.*?) \| 3-1/3-2: (.*?) \| 0-3: (.*?): ({number})/{number}"
    mask_pattern = rf"m ([0-9a-fA-F]+) ([0-9a-fA-F]+) ([0-9a-fA-F]+): ({number})/{number}"

    def parse_team_set(raw: str) -> frozenset[str]:
        return frozenset(t.strip() for t in raw.split(',') if t.strip())

    with open(file_path, 'r') as file:
        for line in file:
            if line.startswith("# teams:"):
                team_names = json.loads(line.split(":", 1)[1].strip())
                results.team_to_bit = {team: 1 << idx for idx, team in enumerate(team_names)}
                continue
            mask_match = re.match(mask_pattern, line)
            if mask_match:
                three_zero = int(mask_match.group(1), 16)
                three_one_two = int(mask_match.group(2), 16)
                zero_three = int(mask_match.group(3), 16)
                count = float(mask_match.group(4))

                key = (three_zero, three_one_two, zero_three)
                results[key] += count
                total_simulations += count
                continue

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
    team_to_bit = getattr(results, "team_to_bit", {})
    combo_masks = None
    if team_to_bit:
        combo_masks = (
            _mask_from_names(combo['3-0'], team_to_bit),
            _mask_from_names(combo['3-1/3-2'], team_to_bit),
            _mask_from_names(combo['0-3'], team_to_bit),
        )
    for (three_zero, three_one_two, zero_three), count in results.items():
        if isinstance(three_zero, int):
            if combo_masks is None:
                raise ValueError("numeric simulation results require a # teams header")
            correct = 0
            correct += (combo_masks[0] & three_zero).bit_count()
            correct += (combo_masks[1] & three_one_two).bit_count()
            correct += (combo_masks[2] & zero_three).bit_count()
        else:
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
