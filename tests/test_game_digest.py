# tests/test_game_digest.py
import json
import sys
from pathlib import Path
from typing import Any, Dict, List

import pytest

import game_digest as gd


# --------------------------
# Test data helpers
# --------------------------

TEAM_ID = 112  # Cubs (away in our fixture)
GAME_DATE = "2025-08-23"
GAME_ID = 111

def _summary_row() -> Dict[str, Any]:
    return {
        "game_id": GAME_ID,
        "game_date": GAME_DATE,
        "home_team_id": 108,
        "home_team": "Los Angeles Angels",
        "away_team_id": 112,
        "away_team": "Chicago Cubs",
        "home_runs": 1,
        "away_runs": 12,
        "status_abstract": "Final",
        "status_detailed": "Final",
    }

def _linescore_row(extra_innings: bool = False) -> Dict[str, Any]:
    # 9-inning example; set inning 10 to mark extras, if requested
    ls = {
        "game_id": GAME_ID,
        "game_date": GAME_DATE,
        "home_team_id": 108,
        "away_team_id": 112,
        "total_home": 1,
        "total_away": 12,
    }
    away = [2,0,3,0,0,0,3,4,0] + [None]*6
    home = [0,0,0,1,0,0,0,0,0] + [None]*6
    if extra_innings:
        away[9] = 1  # 10th inning (index 9)
    for i in range(1, 16):
        ls[f"away_inn_{i}"] = away[i-1]
        ls[f"home_inn_{i}"] = home[i-1]
    return ls

def _players_rows() -> List[Dict[str, Any]]:
    # Two batters, one pitcher — all for team 112 (away)
    return [
        {
            "game_id": GAME_ID, "game_date": GAME_DATE,
            "team_side": "away", "team_id": 112, "team_name": "Chicago Cubs",
            "player_id": 10001, "player_name": "Away Slugger", "primary_pos": "LF",
            "ab": 5, "r": 3, "h": 3, "doubles": 0, "triples": 0, "hr": 2, "rbi": 5,
            "bb": 1, "so": 1, "sb": 1, "cs": 0, "sf": 0, "sh": 0,
            "outs": 0, "ip_str": "0.0", "er": 0, "k": 0, "h_allowed": 0,
            "bb_allowed": 0, "hr_allowed": 0, "bf": 0, "pitches": 0, "strikes": 0,
            "hbp": 0, "wp": 0,
        },
        {
            "game_id": GAME_ID, "game_date": GAME_DATE,
            "team_side": "away", "team_id": 112, "team_name": "Chicago Cubs",
            "player_id": 10002, "player_name": "Away TableSetter", "primary_pos": "2B",
            "ab": 4, "r": 2, "h": 2, "doubles": 1, "triples": 0, "hr": 0, "rbi": 1,
            "bb": 1, "so": 0, "sb": 0, "cs": 0, "sf": 0, "sh": 0,
            "outs": 0, "ip_str": "0.0", "er": 0, "k": 0, "h_allowed": 0,
            "bb_allowed": 0, "hr_allowed": 0, "bf": 0, "pitches": 0, "strikes": 0,
            "hbp": 0, "wp": 0,
        },
        {
            "game_id": GAME_ID, "game_date": GAME_DATE,
            "team_side": "away", "team_id": 112, "team_name": "Chicago Cubs",
            "player_id": 20002, "player_name": "Away Pitcher", "primary_pos": "P",
            "ab": 0, "r": 0, "h": 0, "doubles": 0, "triples": 0, "hr": 0, "rbi": 0,
            "bb": 0, "so": 0, "sb": 0, "cs": 0, "sf": 0, "sh": 0,
            "outs": 21, "ip_str": "7.0", "er": 1, "k": 8, "h_allowed": 4,
            "bb_allowed": 1, "hr_allowed": 0, "bf": 26, "pitches": 95, "strikes": 62,
            "hbp": 0, "wp": 0,
        },
    ]


# --------------------------
# Unit tests: pure helpers
# --------------------------

def test_parse_date_or_today_with_explicit_value():
    d = gd.parse_date_or_today("2025-08-23")
    assert str(d) == "2025-08-23"

def test_outs_to_ip_str():
    assert gd.outs_to_ip_str(0) == "0.0"
    assert gd.outs_to_ip_str(3) == "1.0"
    assert gd.outs_to_ip_str(7) == "2.1"


# --------------------------
# JSON mode
# --------------------------

def test_load_from_json_and_make_digest(tmp_path: Path, capsys):
    # Arrange: write three JSON files the way mlb_ingest.py outputs them
    indir = tmp_path / "out_debug"
    indir.mkdir(parents=True, exist_ok=True)
    (indir / f"{GAME_ID}_summary.json").write_text(json.dumps(_summary_row(), indent=2))
    (indir / f"{GAME_ID}_linescore.json").write_text(json.dumps(_linescore_row(), indent=2))
    (indir / f"{GAME_ID}_players.json").write_text(json.dumps({"players": _players_rows()}, indent=2))

    triples = gd.load_from_json(TEAM_ID, gd.dt.date.fromisoformat(GAME_DATE), str(indir))
    assert len(triples) == 1
    s, l, players = triples[0]

    # Build digest
    digest = gd.make_digest_for_game(TEAM_ID, s, l, players)
    assert digest["game_id"] == GAME_ID
    assert digest["team_id"] == TEAM_ID
    assert digest["team_runs"] == 12 and digest["opponent_runs"] == 1
    assert digest["result"] == "W"

    # Team batting totals (3+2 hits, 3+2 runs, HR=2, RBI=5+1=6, BB=2, SO=1, SB=1)
    body = digest["body_markdown"]
    assert "R 5 • H 5 • HR 2 • RBI 6 • BB 2 • SO 1 • SB 1" in body
    assert "Away Slugger (2)" in body  # HR list appears

    # Pitching totals and star line
    assert "Team: 7.0 IP, 8 K, 1 ER, 4 H, 1 BB" in body
    assert "Away Pitcher: 7.0 IP, 8 K, 1 ER, 4 H, 1 BB" in body

    # Drive main() in JSON mode to ensure prints work and no BQ write is attempted
    rc = gd.main(["--team", str(TEAM_ID), "--date", GAME_DATE, "--output", "json", "--json_indir", str(indir)])
    assert rc == 0
    out = capsys.readouterr().out
    assert "Final:" in out and "Top Batters" in out and "Pitching" in out


def test_format_linescore_str_marks_extras():
    lrow = _linescore_row(extra_innings=True)
    ls = gd.LinescoreRow(
        game_id=lrow["game_id"], game_date=lrow["game_date"],
        home_team_id=lrow["home_team_id"], away_team_id=lrow["away_team_id"],
        totals=(lrow["total_home"], lrow["total_away"]),
        innings_home=[lrow.get(f"home_inn_{i}") for i in range(1, 16)],
        innings_away=[lrow.get(f"away_inn_{i}") for i in range(1, 16)],
    )
    s = gd.format_linescore_str(ls, team_is_home=False)
    assert "(+)" in s


# --------------------------
# BQ mode (read-only)
# --------------------------

class FakeBQClient:
    def __init__(self, project="fake"):
        self.project = project

def _fake_bq_query_factory():
    """Return a bq_query stub that returns per-sql results."""
    calls = {"q1": 0, "q2": 0, "q3": 0}

    def _bq_query(client, sql: str, params: Dict[str, Any]):
        # q1: summaries
        if "game_summaries" in sql:
            calls["q1"] += 1
            return iter([_summary_row()])
        # q2: linescore
        if "game_linescore" in sql:
            calls["q2"] += 1
            return iter([_linescore_row()])
        # q3: players
        if "game_boxscore_players" in sql:
            calls["q3"] += 1
            return iter(_players_rows())
        raise AssertionError(f"Unexpected SQL: {sql}")

    return _bq_query, calls

def test_load_from_bq_and_main_no_write(monkeypatch, capsys):
    # Patch client + query so no real BQ is used
    monkeypatch.setattr(gd, "make_bq_client", lambda project=None: FakeBQClient())
    fake_query, calls = _fake_bq_query_factory()
    monkeypatch.setattr(gd, "bq_query", fake_query)

    triples = gd.load_from_bq(TEAM_ID, gd.dt.date.fromisoformat(GAME_DATE))
    assert len(triples) == 1
    s, l, players = triples[0]
    assert s.game_id == GAME_ID
    assert len(players) == 3

    # Drive main() in BQ mode with --no_write (so we don't need bigquery package)
    rc = gd.main(["--team", str(TEAM_ID), "--date", GAME_DATE, "--output", "bq", "--no_write"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "Final:" in out and "Top Batters" in out and "Pitching" in out

    # Ensure our fake was hit properly
    assert calls["q1"] >= 1 and calls["q2"] >= 1 and calls["q3"] >= 1
