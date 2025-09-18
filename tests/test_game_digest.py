# tests/test_game_digest.py
import json
import os
import re
import sys
import datetime as dt
from pathlib import Path
from typing import Any, Dict, List

import pytest

# Make repo root importable
sys.path.append(".")
import game_digest as mod  # <-- rename if your module name differs


# ------------------------
# Helpers
# ------------------------
def _mk_linescore(game_id: int, innings_away: List[int], innings_home: List[int]):
    rows = []
    for i, runs in enumerate(innings_away, start=1):
        rows.append({"game_id": game_id, "is_home": False, "inning_num": i, "runs": runs})
    for i, runs in enumerate(innings_home, start=1):
        rows.append({"game_id": game_id, "is_home": True, "inning_num": i, "runs": runs})
    return rows


def _mk_players(game_id: int, team_id: int, team_name: str):
    # one batter + one pitcher — already “derived” as if coming from mlb_ingest
    batter = {
        "role": "batter",
        "game_id": game_id,
        "team_id": team_id,
        "team_name": team_name,
        "AB": 4, "H": 2, "HR": 1, "RBI": 3,
        "AVG": 0.500, "OBP": 0.600, "SLG": 1.250, "OPS": 1.850,
        "BAT_SCORE": 12.5,
        "name": "Star Batter",
    }
    pitcher = {
        "role": "pitcher",
        "game_id": game_id,
        "team_id": team_id,
        "team_name": team_name,
        "started": True,
        "outs": 18, "IP": 6.0, "ERA": 1.50, "WHIP": 0.83, "SO": 7, "HR": 0,
        "PITCH_SCORE": 38.0,
        "name": "Ace Pitcher",
        "player_id": 2000,
    }
    return [batter, pitcher]


# ------------------------
# Unit tests: helpers
# ------------------------
def test_fmt_rate():
    assert mod.fmt_rate(0.375) == ".375"
    assert mod.fmt_rate(0.3751, 3) == ".375"
    assert mod.fmt_rate(1.2345, 2) == "1.23"
    assert mod.fmt_rate(0.75, leading_zero=True) == "0.750"


def test_parse_date_prefers_arg_and_falls_back_to_today():
    assert mod.parse_date("2025-08-23") == "2025-08-23"
    # None -> today in BASEBALL_TZ; we just assert format
    d = mod.parse_date(None)
    assert re.fullmatch(r"\d{4}-\d{2}-\d{2}", d)


def test_is_our_row_helpers():
    s = {
        "home_team_id": 1, "home_team_name": "Home",
        "away_team_id": 2, "away_team_name": "Away",
    }
    assert mod.is_our_game_row(s, "1")
    assert mod.is_our_game_row(s, "Away")
    assert not mod.is_our_game_row(s, "Nope")

    r = {"team_id": 2, "team_name": "Away"}
    assert mod.is_our_team_row(r, "2")
    assert mod.is_our_team_row(r, "away")
    assert not mod.is_our_team_row(r, "other")


def test_pick_top_batter_and_pitching_helpers():
    batters = [
        {"BAT_SCORE": 5, "HR": 0, "RBI": 1, "H": 1, "name": "B1"},
        {"BAT_SCORE": 7, "HR": 1, "RBI": 1, "H": 2, "name": "B2"},  # best
    ]
    pitchers = [
        {"PITCH_SCORE": 30, "outs": 18, "ERA": 2.0, "WHIP": 1.1, "name": "Starter", "player_id": 1, "started": True},
        {"PITCH_SCORE": 42, "outs": 6, "ERA": 0.0, "WHIP": 0.5, "name": "Closer", "player_id": 2, "started": False},
        {"PITCH_SCORE": 25, "outs": 3, "ERA": 3.0, "WHIP": 1.4, "name": "Mop-up", "player_id": 3, "started": False},
    ]
    assert mod.pick_top_batter(batters)["name"] == "B2"
    starter = mod.pick_starting_pitcher(pitchers)
    assert starter["name"] == "Starter"
    assert mod.pick_top_relief_pitcher(pitchers, starter)["name"] == "Closer"


def test_pick_top_relief_pitcher_excludes_starter_without_id():
    pitchers = [
        {"PITCH_SCORE": 60, "outs": 15, "ERA": 1.8, "WHIP": 0.9, "name": "Starter", "started": True},
        {"PITCH_SCORE": 55, "outs": 9, "ERA": 2.0, "WHIP": 1.0, "name": "Setup", "started": False},
    ]
    starter = mod.pick_starting_pitcher(pitchers)
    assert starter["name"] == "Starter"
    top_relief = mod.pick_top_relief_pitcher(pitchers, starter)
    assert top_relief["name"] == "Setup"


def test_format_ip_value_prefers_outs_and_uses_baseball_style():
    row_with_outs = {"outs": 20, "IP": 6.6667}
    assert mod.format_ip_value(row_with_outs) == "6.2"

    row_without_outs = {"IP": 2.3333}
    assert mod.format_ip_value(row_without_outs) == "2.1"

    row_with_string = {"IP": "6.1"}
    assert mod.format_ip_value(row_with_string) == "6.1"

    row_with_int = {"IP": 5}
    assert mod.format_ip_value(row_with_int) == "5.0"

    row_with_bad_ip = {"IP": "n/a"}
    assert mod.format_ip_value(row_with_bad_ip) == "n/a"

    assert mod.format_ip_value({}) == "0.0"


# ------------------------
# JSON path
# ------------------------
def test_build_from_json_happy_path(monkeypatch, tmp_path: Path, capsys):
    # Arrange files
    game_id = 123456
    date_iso = "2025-08-23"
    home_id, away_id = 10, 112
    home_name, away_name = "Los Angeles Angels", "Chicago Cubs"
    status = "Final"

    summary = {
        "game_id": game_id,
        "game_date": date_iso,
        "home_team_id": home_id, "home_team_name": home_name,
        "away_team_id": away_id, "away_team_name": away_name,
        "home_score": 1, "away_score": 5,
        "status": status,
    }
    lines = _mk_linescore(game_id, innings_away=[2,0,3,0,0,0,0,0,0], innings_home=[0,0,0,1,0,0,0,0,0])
    players = _mk_players(game_id, team_id=away_id, team_name=away_name)
    players.append(
        {
            "role": "pitcher",
            "game_id": game_id,
            "team_id": away_id,
            "team_name": away_name,
            "started": False,
            "outs": 5,
            "IP": 5 / 3,
            "ERA": 0.00,
            "WHIP": 0.60,
            "SO": 4,
            "HR": 0,
            "PITCH_SCORE": 55.0,
            "name": "Setup Reliever",
            "player_id": 2001,
        }
    )

    # Write triplet
    (tmp_path / f"{game_id}_summary.json").write_text(json.dumps(summary), encoding="utf-8")
    (tmp_path / f"{game_id}_linescore.json").write_text(json.dumps(lines), encoding="utf-8")
    (tmp_path / f"{game_id}_players.json").write_text(json.dumps(players), encoding="utf-8")

    # Point module to tmp dir
    monkeypatch.setenv("DIGEST_JSON_DIR", str(tmp_path))
    mod.JSON_DIR = str(tmp_path)
    monkeypatch.setattr(mod.playoff_odds, "estimate_playoff_odds", lambda team_id: 75)
    monkeypatch.setattr(mod.playoff_odds, "estimate_playoff_odds", lambda team_id: 75)

    # Act
    body, out_game_id, out_team_id = mod.build_from_json(team=away_name, date_iso=date_iso)

    # Assert
    assert out_game_id == game_id
    assert out_team_id == away_id
    assert body.startswith(f"## Final: {away_name} 5-1 {home_name}")
    assert "### Linescore" in body
    assert "Away: 2 0 3 0 0 0 0 0 0" in body
    assert "Home: 0 0 0 1 0 0 0 0 0" in body
    assert "### Top Batter for Chicago Cubs" in body
    assert "### Pitching for Chicago Cubs" in body
    assert "- SP Ace Pitcher" in body
    assert "6.0 IP" in body
    assert "- RP Setup Reliever" in body
    assert "1.2 IP" in body
    assert "### Chicago Cubs postseason odds: 75%" in body


def test_find_summary_for_errors(monkeypatch, tmp_path: Path):
    # No directory
    monkeypatch.setenv("DIGEST_JSON_DIR", str(tmp_path / "missing"))
    mod.JSON_DIR = str(tmp_path / "missing")
    with pytest.raises(FileNotFoundError):
        mod.find_summary_for("Team", "2025-08-23")

    # Directory exists but no summaries
    (tmp_path / "data").mkdir()
    monkeypatch.setenv("DIGEST_JSON_DIR", str(tmp_path / "data"))
    mod.JSON_DIR = str(tmp_path / "data")
    with pytest.raises(FileNotFoundError):
        mod.find_summary_for("Team", "2025-08-23")


def test_main_json_flow(monkeypatch, tmp_path: Path, capsys):
    # Prepare same as happy path
    game_id = 777000
    date_iso = "2025-08-23"
    home_id, away_id = 108, 112
    home_name, away_name = "Los Angeles Angels", "Chicago Cubs"

    summary = {
        "game_id": game_id,
        "game_date": date_iso,
        "home_team_id": home_id, "home_team_name": home_name,
        "away_team_id": away_id, "away_team_name": away_name,
        "home_score": 1, "away_score": 12,
        "status": "Final",
    }
    lines = _mk_linescore(game_id, innings_away=[2,0,3,0,0,0,3,4,0], innings_home=[0,0,0,1,0,0,0,0,0])
    players = _mk_players(game_id, team_id=away_id, team_name=away_name)
    players.append(
        {
            "role": "pitcher",
            "game_id": game_id,
            "team_id": away_id,
            "team_name": away_name,
            "started": False,
            "outs": 4,
            "IP": 4 / 3,
            "ERA": 0.00,
            "WHIP": 0.75,
            "SO": 3,
            "HR": 0,
            "PITCH_SCORE": 52.0,
            "name": "High-Leverage Reliever",
            "player_id": 2003,
        }
    )

    (tmp_path / f"{game_id}_summary.json").write_text(json.dumps(summary), encoding="utf-8")
    (tmp_path / f"{game_id}_linescore.json").write_text(json.dumps(lines), encoding="utf-8")
    (tmp_path / f"{game_id}_players.json").write_text(json.dumps(players), encoding="utf-8")

    monkeypatch.setenv("DIGEST_JSON_DIR", str(tmp_path))
    mod.JSON_DIR = str(tmp_path)
    monkeypatch.setattr(mod.playoff_odds, "estimate_playoff_odds", lambda team_id: 75)

    argv = ["prog", "--team", away_name, "--date", date_iso, "--output", "json"]
    monkeypatch.setattr(sys, "argv", argv)
    rc = mod.main()
    assert rc == 0

    out = capsys.readouterr().out
    assert out.startswith(f"## Final: {away_name} 12-1 {home_name}")
    assert "### Top Batter for Chicago Cubs" in out
    assert "### Pitching for Chicago Cubs" in out
    assert "- SP Ace Pitcher" in out
    assert "6.0 IP" in out
    assert "- RP High-Leverage Reliever" in out
    assert "1.1 IP" in out
    assert "### Chicago Cubs postseason odds: 75%" in out


# ------------------------
# BigQuery path
# ------------------------
def test_build_from_bq_and_main_bq_flow(monkeypatch, capsys):
    project = "p"; dataset = "d"; team = "Chicago Cubs"
    game_id = 999001
    home = {"id": 108, "name": "Los Angeles Angels"}
    away = {"id": 112, "name": "Chicago Cubs"}

    # Fake bq_query that returns rows based on the SQL
    def fake_bq_query(client, sql: str, params=None):
        if "FROM `p.d.game_summaries`" in sql:
            return [{
                "game_id": game_id,
                "game_date": "2025-08-23",
                "home_team_id": home["id"], "home_team_name": home["name"],
                "away_team_id": away["id"], "away_team_name": away["name"],
                "home_score": 1, "away_score": 5, "status": "Final",
            }]
        if "FROM `p.d.game_linescore`" in sql:
            return [
                {"is_home": False, "inning_num": 1, "runs": 2},
                {"is_home": False, "inning_num": 2, "runs": 0},
                {"is_home": False, "inning_num": 3, "runs": 3},
                {"is_home": True,  "inning_num": 1, "runs": 0},
                {"is_home": True,  "inning_num": 2, "runs": 0},
                {"is_home": True,  "inning_num": 3, "runs": 0},
            ]
        if "FROM `p.d.game_boxscore_players`" in sql:
            players = _mk_players(game_id, team_id=away["id"], team_name=away["name"])
            players.append(
                {
                    "role": "pitcher",
                    "game_id": game_id,
                    "team_id": away["id"],
                    "team_name": away["name"],
                    "started": False,
                    "outs": 4,
                    "IP": 4 / 3,
                    "ERA": 0.00,
                    "WHIP": 0.75,
                    "SO": 3,
                    "HR": 0,
                    "PITCH_SCORE": 52.0,
                    "name": "High-Leverage Reliever",
                    "player_id": 2003,
                }
            )
            return players
        raise AssertionError(f"Unexpected SQL: {sql}")

    # Capture digests written
    writes: List[Dict[str, Any]] = []

    # Minimal fake client (only needs .project because we patched bq_query)
    client = type("FakeClient", (), {"project": project})()

    # Monkeypatch the BQ functions the script calls
    monkeypatch.setattr(mod, "bq_query", fake_bq_query)
    monkeypatch.setattr(mod, "bq_client_or_none", lambda proj: client)
    monkeypatch.setattr(mod, "bq_write_digest", lambda c, p, d, row: writes.append(row))
    monkeypatch.setattr(mod.playoff_odds, "estimate_playoff_odds", lambda team_id: 75)

    # Run main in bq mode
    argv = [
        "prog",
        "--team", team,
        "--date", "2025-08-23",
        "--output", "bq",
        "--bq_project", project,
        "--bq_dataset", dataset,
    ]
    import sys as _sys
    monkeypatch.setattr(_sys, "argv", argv)

    rc = mod.main()
    assert rc == 0

    assert len(writes) == 1
    row = writes[0]
    assert row["game_id"] == game_id
    assert row["team_id"] == away["id"]
    assert row["team_name"] == team
    assert row["game_date"] == "2025-08-23"
    assert row["created_at"].endswith("Z")
    assert "## Final: Chicago Cubs 5-1 Los Angeles Angels" in row["digest_md"]
    assert "### Pitching for Chicago Cubs" in row["digest_md"]
    assert "SP Ace Pitcher" in row["digest_md"]
    assert "6.0 IP" in row["digest_md"]
    assert "RP High-Leverage Reliever" in row["digest_md"]
    assert "1.1 IP" in row["digest_md"]
    assert "### Chicago Cubs is 75% likely to make it to Playoffs this year" in row["digest_md"]


# ------------------------
# Env / output mode
# ------------------------
def test_is_cloud_env_and_default_output(monkeypatch):
    for k in ["K_SERVICE","CLOUD_RUN_JOB","GAE_ENV","GOOGLE_CLOUD_PROJECT","BQ_PROJECT"]:
        monkeypatch.delenv(k, raising=False)
    assert mod.is_cloud_env() is False
    assert mod.default_output_mode() == "json"

    monkeypatch.setenv("GOOGLE_CLOUD_PROJECT", "x")
    assert mod.is_cloud_env() is True
    assert mod.default_output_mode() == "bq"