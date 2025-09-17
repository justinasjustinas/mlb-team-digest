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
