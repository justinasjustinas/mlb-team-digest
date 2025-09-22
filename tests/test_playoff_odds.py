import playoff_odds as po


def sample_standings():
    return [
        {"team_id": 1, "team_name": "A", "league": "L", "division": "East", "w": 60, "l": 40},
        {"team_id": 2, "team_name": "B", "league": "L", "division": "East", "w": 58, "l": 42},
        {"team_id": 3, "team_name": "C", "league": "L", "division": "East", "w": 54, "l": 46},
        {"team_id": 4, "team_name": "D", "league": "L", "division": "Central", "w": 62, "l": 38},
        {"team_id": 5, "team_name": "E", "league": "L", "division": "Central", "w": 52, "l": 48},
        {"team_id": 6, "team_name": "F", "league": "L", "division": "West", "w": 55, "l": 45},
        {"team_id": 7, "team_name": "G", "league": "L", "division": "West", "w": 53, "l": 47},
        {"team_id": 8, "team_name": "H", "league": "L", "division": "West", "w": 49, "l": 51},
    ]


def test_division_leader_outpaces_rival():
    standings = sample_standings()
    leader_prob = po.estimate_playoff_odds(1, standings=standings)
    rival_prob = po.estimate_playoff_odds(2, standings=standings)

    assert leader_prob is not None and rival_prob is not None
    assert leader_prob > 60
    assert leader_prob >= rival_prob - 15


def test_wildcard_keeps_competitive_team_alive():
    standings = sample_standings()
    wildcard_prob = po.estimate_playoff_odds(7, standings=standings)
    bubble_prob = po.estimate_playoff_odds(8, standings=standings)

    assert wildcard_prob is not None and bubble_prob is not None
    assert wildcard_prob > bubble_prob
    assert bubble_prob < 50


def test_missing_team_returns_none():
    assert po.estimate_playoff_odds(999, standings=sample_standings()) is None


def test_clinched_division_reaches_full_probability():
    standings = [
        {"team_id": 11, "team_name": "Leader", "league": "L", "division": "East", "w": 95, "l": 60},
        {"team_id": 12, "team_name": "Chaser", "league": "L", "division": "East", "w": 85, "l": 70},
        {"team_id": 13, "team_name": "Spoiler", "league": "L", "division": "East", "w": 70, "l": 85},
        {"team_id": 14, "team_name": "WestLeader", "league": "L", "division": "West", "w": 94, "l": 61},
        {"team_id": 15, "team_name": "WestChaser", "league": "L", "division": "West", "w": 88, "l": 67},
        {"team_id": 16, "team_name": "WestSpoiler", "league": "L", "division": "West", "w": 80, "l": 75},
    ]

    assert po.estimate_playoff_odds("Leader", standings=standings) == 100


def test_locked_in_wildcard_reaches_full_probability():
    standings = [
        {"team_id": 21, "team_name": "EastLeader", "league": "L", "division": "East", "w": 95, "l": 60},
        {"team_id": 22, "team_name": "EastSecond", "league": "L", "division": "East", "w": 92, "l": 63},
        {"team_id": 23, "team_name": "EastThird", "league": "L", "division": "East", "w": 88, "l": 67},
        {"team_id": 24, "team_name": "WestLeader", "league": "L", "division": "West", "w": 90, "l": 65},
        {"team_id": 25, "team_name": "WestSecond", "league": "L", "division": "West", "w": 87, "l": 68},
        {"team_id": 26, "team_name": "WestThird", "league": "L", "division": "West", "w": 70, "l": 85},
    ]

    assert po.estimate_playoff_odds("WestSecond", standings=standings) == 100


def test_eliminated_wildcard_team_falls_to_zero():
    standings = [
        {"team_id": 31, "team_name": "EastLeader", "league": "L", "division": "East", "w": 96, "l": 58},
        {"team_id": 32, "team_name": "EastSecond", "league": "L", "division": "East", "w": 88, "l": 66},
        {"team_id": 33, "team_name": "EastThird", "league": "L", "division": "East", "w": 87, "l": 67},
        {"team_id": 34, "team_name": "WestLeader", "league": "L", "division": "West", "w": 92, "l": 62},
        {"team_id": 35, "team_name": "WestSecond", "league": "L", "division": "West", "w": 86, "l": 68},
        {"team_id": 36, "team_name": "WestThird", "league": "L", "division": "West", "w": 70, "l": 84},
    ]

    assert po.estimate_playoff_odds("WestThird", standings=standings) == 0
