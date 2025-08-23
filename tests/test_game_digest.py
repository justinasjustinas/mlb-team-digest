
import pytest

import game_digest

def test_ip_to_float_prefers_outs():
    # When outs is provided, it's used (rounded to 2 decimals)
    assert game_digest.ip_to_float("6.0", 18) == 6.0
    assert game_digest.ip_to_float("6.1", 19) == pytest.approx(6.33, rel=1e-3)
    assert game_digest.ip_to_float(None, 2) == pytest.approx(0.67, rel=1e-3)

def test_ip_to_float_from_string_when_no_outs():
    assert game_digest.ip_to_float("6.0", None) == 6.0
    assert game_digest.ip_to_float("6.1", None) == 6.33
    assert game_digest.ip_to_float("0.2", None) == 0.67
    assert game_digest.ip_to_float(None, None) == 0.0


def test_hitter_score_formula():
    # hits=2 (1 single + 1 HR), 1 HR, 1 2B, 0 3B, 1 BB, 1 HBP, 1 SB, 3 RBI, 2 R
    b = dict(hits=2, homeRuns=1, doubles=1, triples=0, baseOnBalls=1, hitByPitch=1, stolenBases=1, rbi=3, runs=2)
    # singles = max(H - HR - 2B - 3B, 0) = max(2 - 1 - 1 - 0, 0) = 0
    expected = 5*1 + 3*(1+0) + 2*(1+1+1) + 1*0 + 1.5*3 + 1.0*2
    assert game_digest.hitter_score(b) == expected


def test_team_names_and_which_side():
    feed = {"gameData": {"teams": {"away": {"id": 119, "name": "Dodgers"}, "home": {"id": 112, "name": "Cubs"}}}}
    assert game_digest.team_names(feed) == ("Dodgers", "Cubs")
    assert game_digest.which_side(feed, 119) == "away"
    assert game_digest.which_side(feed, 112) == "home"
    with pytest.raises(SystemExit):
        game_digest.which_side(feed, 161)  # Yankees not in this game


def test_get_team_boxscore():
    feed = {"liveData": {"boxscore": {"teams": {"away": {"team": {"id": 119, "name": "Dodgers"}}}}}}
    assert game_digest.get_team_boxscore(feed, "away") == {"team": {"id": 119, "name": "Dodgers"}}
    assert game_digest.get_team_boxscore(feed, "home") == {}


def test_batting_line_and_top_hitters():
    # players must have stats.batting
    batter_good = {"person": {"fullName": "John Doe"}, "stats": {"batting": {"hits": 2, "atBats": 4, "homeRuns": 1, "rbi": 3, "baseOnBalls": 1}}}
    batter_bad = {"person": {"fullName": "Bad Hitter"}, "stats": {"batting": {"hits": 0, "atBats": 4}}}
    team_box = {"players": {"X": batter_good, "Y": batter_bad}}

    # batting_line reads directly from a batting dict
    line = game_digest.batting_line(batter_good["stats"]["batting"])
    assert "2-for-4" in line and "1 HR" in line and "3 RBI" in line and "1 BB" in line

    hitters = game_digest.top_hitters(team_box, n=1)
    assert len(hitters) == 1
    name, score, line = hitters[0]
    assert name == "John Doe"
    assert "2-for-4" in line


def test_best_pitcher():
    # players must have stats.pitching
    pA = {"person": {"fullName": "Pitcher A"}, "stats": {"pitching": {"inningsPitched": "5.0", "outs": 15, "strikeOuts": 4, "earnedRuns": 1, "hits": 3, "baseOnBalls": 1, "homeRuns": 0}}}
    pB = {"person": {"fullName": "Pitcher B"}, "stats": {"pitching": {"inningsPitched": "2.0", "outs": 6, "strikeOuts": 1, "earnedRuns": 0, "hits": 1, "baseOnBalls": 0, "homeRuns": 0}}}
    team_box = {"players": {"A": pA, "B": pB}}
    name, score, line = game_digest.best_pitcher(team_box)
    assert name in {"Pitcher A", "Pitcher B"}
    assert "IP" in line and "ER" in line and "K" in line


def test_linescore_defaults_and_values():
    assert game_digest.linescore({"liveData": {"linescore": {"teams": {"away": {"runs": 3}, "home": {"runs": 2}}}}}) == (3, 2)
    # missing teams/runs should default to 0,0
    assert game_digest.linescore({"liveData": {"linescore": {"teams": {}}}}) == (0, 0)


def test_choose_mvp():
    top_hitter = ("Great Hitter", 15.0, "3-for-4, 2 HR, 5 RBI")
    best_pitcher = ("Great Pitcher", 20.0, "9.0 IP, 10 K, 0 ER")
    name, score, line, role = game_digest.choose_mvp(top_hitter, best_pitcher)
    assert name == "Great Pitcher" and role == "Pitcher"


def test_digest_one_game_prints(capsys):
    feed = {
        "gameData": {"teams": {"away": {"id": 119, "name": "Dodgers"}, "home": {"id": 112, "name": "Cubs"}}},
        "liveData": {
            "linescore": {"teams": {"away": {"runs": 5}, "home": {"runs": 3}}},
            "boxscore": {"teams": {"away": {"players": {}}, "home": {"players": {}}}},
        },
    }
    game_digest.digest_one_game(feed, 119)  # Dodgers (away)
    out = capsys.readouterr().out
    assert "Dodgers" in out and "Cubs" in out and ("📣" in out or "-" in out)
