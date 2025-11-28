import sys
from pathlib import Path
import json
import os
from tqdm import tqdm
import matplotlib.cm as cm
import matplotlib as mpl
import matplotlib.pyplot as plt
import numpy as np

sys.path.append(f"{os.getcwd()}/plugins/major_hw")
from gen_win_matrix import gen_win_matrix
from simulate import simulate
from verify import parse_simulation_results, evaluate_combination


filepath = Path(sys.argv[1])

if not filepath.exists():
    print("file not exists")
    exit()

with open(filepath, 'r') as f:
    data = json.load(f)


info_path = Path(".") / "assets" / f"{data['stage']}.json"

games = data['games']
games.reverse()

x_name = [""]
x_name.extend([f"{game[0]} {game[2]} {game[1]}" for game in games])

for i in range(len(games) + 1):
    print(f"{i}/{len(games)}")
    out_path = Path(".") / "temp" / f"{data['stage']}-{i}.txt"
    if not out_path.exists():
        gen_win_matrix(info_path, games[:i])
        simulate(info_path, out_path)


members = data['homework']
for i in range(len(members)):
    members[i]['winrate'] = []

for i in tqdm(range(len(games) + 1)):
    results, total_simulations = parse_simulation_results(Path(".") / "temp" / f"{data['stage']}-{i}.txt")
    for j in range(len(members)):
        teams = members[j]['teams']
        combo = {
            '3-0': teams[: 2],
            '3-1/3-2': teams[2: 8],
            '0-3': teams[8: ]
        }
        correct_counts, prob_ge5, expected_value = evaluate_combination(combo, results)
        members[j]['winrate'].append(prob_ge5)

with open(Path(".") / "temp" / f"{data['stage']}-{len(data['games'])}", "w") as f:
    json.dump(members, f)

x_indices = list(range(len(x_name)))

plt.rcParams['font.family'] = 'WenQuanYi Micro Hei'
plt.rcParams['axes.unicode_minus'] = False   # 解决负号 '-' 显示为方块的问题
plt.rcParams['axes.prop_cycle'] = mpl.cycler(color=cm.get_cmap('tab20').colors)

plt.figure(figsize=(12, 6))

for member in members:
    nickname = member["nickname"]
    winrate_data = member["winrate"]

    assert(len(winrate_data) == len(x_name))
    plt.plot(x_indices, winrate_data, label=nickname, marker='o')


plt.xlabel("赛程") 
plt.ylabel("通过率")
plt.title(f"作业通过率变化图")
# plt.legend(title="昵称") 
plt.legend(
    title="昵称",
    # 锚点设置在 X 轴的 1.02 位置，Y 轴的 1.0 位置 (右上角外侧)
    bbox_to_anchor=(1.02, 1),
    # 将图例框的左上角与锚点对齐
    loc='upper left',
    # 确保图例完全位于坐标轴外部
    borderaxespad=0. 
)
plt.grid(True)

plt.xticks(x_indices, x_name, rotation=45, ha='right')
plt.ylim(0, 1)

plt.tight_layout()
plt.savefig("result.png")