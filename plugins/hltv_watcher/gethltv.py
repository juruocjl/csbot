from bs4 import BeautifulSoup
from curl_cffi.requests import AsyncSession


def parse_matches_by_score(html: str) -> list[tuple[str, str]]:
    soup = BeautifulSoup(html, 'html.parser')

    title = soup.find("div", class_="event-hub-title").get_text(strip=True)

    results = []

    for match in soup.find_all("div", class_="result-con"):
        # 1. 获取按顺序排列的两个队伍名称
        # find_all 会按照文档流的顺序返回元素，所以 teams[0] 是左边队伍，teams[1] 是右边队伍
        teams = match.find_all("div", class_="team")
        
        # 2. 获取比分区域的 span
        score_cell = match.find("td", class_="result-score")
        if not score_cell:
            continue
        
        # 这里通常有两个 span：左边分数和右边分数
        score_spans = score_cell.find_all("span")

        # 确保数据完整性（既有两支队伍，也有两个分数 span）
        assert(len(teams) == 2 and len(score_spans) == 2)
        name_left = teams[0].get_text(strip=True)
        name_right = teams[1].get_text(strip=True)
        
        # 3. 判断哪个 span 有 'score-won' 类
        left_classes = score_spans[0].get("class", [])
        right_classes = score_spans[1].get("class", [])

        left_score = score_spans[0].get_text(strip=True)
        right_score = score_spans[1].get_text(strip=True)

        if "score-won" in left_classes:
            # 左边分数的 span 赢了 -> 左边队伍赢
            results.append((name_left, name_right, f"{left_score}:{right_score}"))
        elif "score-won" in right_classes:
            # 右边分数的 span 赢了 -> 右边队伍赢
            results.append((name_right, name_left, f"{right_score}:{left_score}"))
        else:
            raise(RuntimeError("No win team"))
    return title, results

async def get_matches(event):
    async with AsyncSession(impersonate="chrome110") as s:
        r = await s.get(f"https://www.hltv.org/results?event={event}")
        return parse_matches_by_score(r.text)
