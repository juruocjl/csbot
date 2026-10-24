"""Offline tests for event discovery, notification rules and 5E parsing."""

import importlib.util
import json
from pathlib import Path
import sys
import unittest
from datetime import datetime, timedelta


LOGIC_PATH = Path(__file__).parents[1] / "plugins" / "hltv_watcher" / "logic.py"
SPEC = importlib.util.spec_from_file_location("hltv_watcher_logic", LOGIC_PATH)
assert SPEC and SPEC.loader
LOGIC = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = LOGIC
SPEC.loader.exec_module(LOGIC)


NOW = int(datetime(2026, 10, 24, 12, 0, tzinfo=LOGIC.SHANGHAI).timestamp())


def event(
    event_id,
    name,
    *,
    grade="5",
    grade_label="C级赛事",
    special_grade_label="",
    status="live",
    start="2026-10-24 00:00:00",
    end="2026-10-24 23:59:59",
    teams=(),
):
    return LOGIC.EventInfo(
        event_id=event_id,
        name=name,
        grade=grade,
        grade_label=grade_label,
        special_grade_label=special_grade_label,
        status=status,
        start_ts=LOGIC.parse_event_time(start),
        end_ts=LOGIC.parse_event_time(end),
        teams=tuple(teams),
    )


def match(event_id, match_id, team1, team2, s1, s2, status=2):
    return {
        "mc_info": {
            "id": match_id,
            "t1_info": {"disp_name": team1},
            "t2_info": {"disp_name": team2},
        },
        "state": {"status": str(status), "t1_score": str(s1), "t2_score": str(s2)},
        "tt_info": {"id": f"csgo_tt_{event_id}", "disp_name": "测试赛事"},
    }


class EventParsingTest(unittest.TestCase):
    def test_event_list_payload(self):
        payload = {
            "success": True,
            "data": {
                "items": [
                    {
                        "basic_info": {
                            "id": "csgo_tt_9434",
                            "disp_name": "RLG特斯拉杯 2026",
                            "grade": "4",
                            "grade_label": "B级赛事",
                            "special_grade_label": "",
                            "status": "upcoming",
                            "start_time": "2026-10-27 19:00:00",
                            "end_time": "2026-10-29 19:00:00",
                        },
                        "teams": [{"name": "FaZe"}, {"name": "B8"}, {"name": "FaZe"}],
                    },
                    {"basic_info": {"id": "not-an-event"}},
                ]
            },
        }

        events = LOGIC.parse_event_list(payload)

        self.assertEqual(len(events), 1)
        parsed = events[0]
        self.assertEqual(parsed.event_id, 9434)
        self.assertEqual(parsed.name, "RLG特斯拉杯 2026")
        self.assertEqual(parsed.teams, ("FaZe", "B8"))
        self.assertEqual(parsed.start_ts, LOGIC.parse_event_time("2026-10-27 19:00:00"))
        self.assertFalse(parsed.is_major_tier)

    def test_major_and_s_tiers(self):
        self.assertTrue(event(1, "Major", grade="1", grade_label="Major").is_major_tier)
        self.assertTrue(event(2, "S", grade="2", grade_label="S级赛事").is_major_tier)
        self.assertTrue(event(3, "S+", grade="7", grade_label="S+").is_major_tier)
        self.assertTrue(
            event(4, "label only", grade="", grade_label="S级赛事").is_major_tier
        )
        # A qualifier can mention Major in its name without being a Major.
        self.assertFalse(
            event(
                5,
                "PGL 新加坡 Major 2026 资格赛",
                grade="6",
                grade_label="资格赛B级",
            ).is_major_tier
        )

    def test_discovery_windows_cover_live_and_future_events(self):
        windows = LOGIC.discovery_time_windows(
            datetime(2026, 10, 24, 12, tzinfo=LOGIC.SHANGHAI)
        )
        starts = [LOGIC.parse_event_time(start) for start, _ in windows]
        ends = [LOGIC.parse_event_time(end) for _, end in windows]

        # One window must reach back over a long-running event; another must
        # still cover upcoming Major/S events.
        self.assertLessEqual(
            min(starts), NOW - int(timedelta(days=10).total_seconds())
        )
        self.assertGreaterEqual(
            max(ends), NOW + int(timedelta(days=25).total_seconds())
        )
        self.assertEqual(len(windows), 2)

    def test_event_time_and_json_round_trip(self):
        parsed = event(8050, "PGL 布加勒斯特大师赛 2026", teams=["TYLOO"])
        restored = LOGIC.events_from_json(LOGIC.events_to_json([parsed]))
        self.assertEqual(restored[8050], parsed)
        self.assertEqual(
            LOGIC.parse_event_time("2026-10-24 00:00:00"),
            int(datetime(2026, 10, 24, tzinfo=LOGIC.SHANGHAI).timestamp()),
        )
        self.assertEqual(LOGIC.events_from_json(None), {})

    def test_invalid_metadata_is_rejected(self):
        for raw in ("not-json", "{}", '[{"event_id":0}]'):
            with self.subTest(raw=raw):
                with self.assertRaises((ValueError, json.JSONDecodeError)):
                    LOGIC.events_from_json(raw)


class TeamMatchingTest(unittest.TestCase):
    def test_followed_team_aliases(self):
        followed = ["TYLOO", "LVG", "FaZe", "bcg"]
        for team in ("TYLOO", "tyloo", "L.V.G", "FaZe", "FaZe Clan", "BC.Game", "BC.Game Academy"):
            with self.subTest(team=team):
                self.assertTrue(LOGIC.is_followed_team(team, followed))
        for team in ("G2", "B8", "NIP"):
            with self.subTest(team=team):
                self.assertFalse(LOGIC.is_followed_team(team, followed))

    def test_empty_followed_list(self):
        self.assertFalse(LOGIC.is_followed_team("TYLOO", []))
        self.assertFalse(LOGIC.is_followed_team("", ["TYLOO"]))


class WatchListTest(unittest.TestCase):
    def test_active_events_are_selected_and_finished_removed(self):
        events = [
            event(8050, "PGL 布加勒斯特大师赛 2026", grade="2", grade_label="S级赛事"),
            event(
                9435,
                "虚空秋季杯 2026",
                teams=["LVG", "CW"],
                end="2026-10-24 23:59:59",
            ),
            event(
                9001,
                "已结束的比赛",
                teams=["TYLOO"],
                status="past",
                start="2026-10-20 00:00:00",
                end="2026-10-22 23:59:59",
            ),
            event(
                9434,
                "RLG特斯拉杯 2026",
                grade="4",
                grade_label="B级赛事",
                status="upcoming",
                start="2026-10-27 19:00:00",
                end="2026-10-29 19:00:00",
                teams=["FaZe"],
            ),
            event(9002, "无关比赛", teams=["G2"], end="2026-10-26 23:59:59"),
        ]

        selected = LOGIC.select_watch_events(
            events, ["TYLOO", "LVG", "FaZe", "bcg"], NOW
        )

        self.assertEqual([item.event_id for item in selected], [8050, 9435, 9434])

    def test_major_stage_children_are_collapsed(self):
        parent = event(
            8302,
            "PGL 新加坡 Major 2026",
            grade="1",
            grade_label="Major",
            status="upcoming",
            start="2026-11-24 00:00:00",
            end="2026-12-13 23:59:59",
        )
        stage1 = event(
            9303,
            "PGL 新加坡 Major 2026 第1阶段",
            grade="1",
            grade_label="Major",
            status="upcoming",
            start="2026-11-24 00:00:00",
            end="2026-11-27 23:59:59",
        )
        qualifier = event(
            9400,
            "PGL 新加坡 Major 2026 资格赛",
            grade="6",
            grade_label="资格赛B级",
            status="upcoming",
            start="2026-11-10 00:00:00",
            end="2026-11-12 23:59:59",
            teams=["TYLOO"],
        )

        selected = LOGIC.select_watch_events([stage1, qualifier, parent], ["TYLOO"], NOW)

        self.assertEqual([item.event_id for item in selected], [9400, 8302])

    def test_poll_only_live_or_overdue_upcoming(self):
        live = event(1, "live", status="live")
        upcoming = event(2, "upcoming", status="upcoming", start="2026-11-02 00:00:00", end="2026-11-08 23:59:59")
        overdue = event(3, "overdue", status="upcoming", start="2026-10-24 00:00:00", end="2026-10-25 23:59:59")
        recent_past = event(4, "recent past", status="past", start="2026-10-24 08:00:00", end="2026-10-24 11:00:00")
        old_past = event(5, "old past", status="past", start="2026-10-24 01:00:00", end="2026-10-24 03:00:00")

        self.assertTrue(LOGIC.should_poll_event(live, NOW))
        self.assertFalse(LOGIC.should_poll_event(upcoming, NOW))
        self.assertTrue(LOGIC.should_poll_event(overdue, NOW))
        self.assertTrue(LOGIC.should_poll_event(recent_past, NOW))
        self.assertFalse(LOGIC.should_poll_event(old_past, NOW))

    def test_recently_finished_event_gets_final_result_grace(self):
        recent = event(6, "刚结束", teams=["TYLOO"], status="past", start="2026-10-24 08:00:00", end="2026-10-24 11:00:00")
        old = event(7, "早结束", teams=["TYLOO"], status="past", start="2026-10-24 01:00:00", end="2026-10-24 03:00:00")

        selected = LOGIC.select_watch_events([recent, old], ["TYLOO"], NOW)

        self.assertEqual([item.event_id for item in selected], [6])


class NotificationRuleTest(unittest.TestCase):
    def test_major_sends_everything(self):
        major = event(8302, "Major", grade="1", grade_label="Major")
        matches = [
            ("A", "B", "2:0", "m1"),
            ("TYLOO", "C", "2:1", "m2"),
        ]
        self.assertEqual(LOGIC.notify_matches(major, matches, ["TYLOO"]), matches)

    def test_non_major_only_sends_followed_teams(self):
        regular = event(9434, "B级赛事", grade="4", grade_label="B级赛事")
        matches = [
            ("A", "B", "2:0", "m1"),
            ("TYLOO", "C", "2:1", "m2"),
            ("D", "BC.Game", "0:2", "m3"),
        ]
        notified = LOGIC.notify_matches(regular, matches, ["TYLOO", "bcg"])
        self.assertEqual([item[3] for item in notified], ["m2", "m3"])

    def test_message_and_split(self):
        major = event(8302, "PGL 新加坡 Major 2026", grade="1", grade_label="Major")
        message = LOGIC.build_message(major, [("TYLOO", "G2", "2:0", "m1")])
        self.assertIn("【Major】PGL 新加坡 Major 2026 结果有更新", message)
        self.assertIn("TYLOO vs G2 2:0", message)
        self.assertEqual(LOGIC.split_message(message), [message])
        long_message = "\n".join(["x" * 100] * 30)
        self.assertGreater(len(LOGIC.split_message(long_message, limit=500)), 1)


class MatchParsingTest(unittest.TestCase):
    def test_finished_matches_only_and_foreign_events_ignored(self):
        raw = [
            match(9430, "csgo_mc_1", "Team A", "Team B", 2, 1),
            match(8245, "csgo_mc_2", "Other", "Event", 2, 0),
            match(9430, "csgo_mc_3", "Team C", "Team D", 0, 0, status=0),
        ]

        title, results = LOGIC.parse_finished_matches(9430, raw)

        self.assertEqual(title, "测试赛事")
        self.assertEqual(results, [("Team A", "Team B", "2:1", "csgo_mc_1")])

    def test_winner_is_first_and_results_are_oldest_first(self):
        raw = [
            match(9430, "newer", "Loser", "Winner", 1, 2),
            match(9430, "older", "Winner", "Loser", 2, 0),
        ]

        _, results = LOGIC.parse_finished_matches(9430, raw)

        self.assertEqual(
            results,
            [
                ("Winner", "Loser", "2:0", "older"),
                ("Winner", "Loser", "2:1", "newer"),
            ],
        )

    def test_non_list_payload_is_empty(self):
        self.assertEqual(LOGIC.parse_finished_matches(9430, None), ("", []))


if __name__ == "__main__":
    unittest.main()
