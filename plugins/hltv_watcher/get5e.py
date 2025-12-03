from nonebot import require

get_session = require("utils").get_session

async def get_matches(event):
    url = f"https://app.5eplay.com/api/tournament/event_session_list?tt_id=csgo_tt_{event}&game_type=1"
    async with get_session().get(url) as result:
        data = await result.json()
        title = ""
        res = []
        for match in data['data']['matches']:
            if int(match['state']['status']) == 2:
                title = match['tt_info']['disp_name']
                team1 = match['mc_info']['t1_info']['disp_name']
                team2 = match['mc_info']['t2_info']['disp_name']
                team1_score = int(match['state']['t1_score'])
                team2_score = int(match['state']['t2_score'])
                if team2_score > team1_score:
                    team1, team2 = team2, team1
                    team1_score, team2_score = team2_score, team1_score
                res.append((team1, team2, f"{team1_score}:{team2_score}"))
        res.reverse()
        return title, res 