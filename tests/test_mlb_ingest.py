
import json
import re
import sys
from pathlib import Path

import pytest

import mlb_ingest 

def test_today_chicago_date_str_format():
    date_str = mlb_ingest.today_chicago_date_str()
    assert re.match(r"^\d{4}-\d{2}-\d{2}$", date_str)

def test_extract_status_from_feed_prefers_detailed_state():
    feed = {"gameData": {"status": {"detailedState": "Final", "abstractGameState": "Live"}}}
    assert mlb_ingest.extract_status_from_feed(feed) == "Final"

def test_extract_status_from_feed_fallback_abstract_state():
    feed = {"gameData": {"status": {"abstractGameState": "Live"}}}
    assert mlb_ingest.extract_status_from_feed(feed) == "Live"

def test_extract_official_date_from_feed_prefers_official():
    feed = {"gameData": {"datetime": {"officialDate": "2024-04-01"}}}
    assert mlb_ingest.extract_official_date_from_feed(feed) == "2024-04-01"

def test_extract_official_date_from_feed_fallback_datetime(monkeypatch):
    feed = {"gameData": {"datetime": {"dateTime": "2024-04-02T18:00:00Z"}}}
    assert mlb_ingest.extract_official_date_from_feed(feed) == "2024-04-02"

def test_extract_official_date_from_feed_ultimate_fallback(monkeypatch):
    # Force today_chicago_date_str to a known value for determinism
    monkeypatch.setattr(mlb_ingest, "today_chicago_date_str", lambda: "2099-01-01")
    feed = {"gameData": {"datetime": {}}}  # no officialDate, no dateTime
    assert mlb_ingest.extract_official_date_from_feed(feed) == "2099-01-01"

def test_describe_matchup():
    game = {
        "teams": {
            "home": {"team": {"name": "Chicago Cubs"}},
            "away": {"team": {"name": "St. Louis Cardinals"}},
        }
    }
    assert mlb_ingest.describe_matchup(game) == "St. Louis Cardinals at Chicago Cubs"

def test_save_json_creates_dirs(tmp_path: Path):
    obj = {"hello": "world"}
    out = tmp_path / "raw" / "2024-04-01" / "game_123.json"
    mlb_ingest.save_json(obj, out)
    assert out.exists()
    with out.open("r", encoding="utf-8") as f:
        data = json.load(f)
    assert data == obj

def test_fetch_feed_live_calls_fetch_json(monkeypatch):
    captured = {}
    def fake_fetch(url: str, params=None, **kwargs):
        captured["url"] = url
        return {"ok": True}
    monkeypatch.setattr(mlb_ingest, "fetch_json", fake_fetch)
    mlb_ingest.fetch_feed_live(123456)
    assert mlb_ingest.BASE_FEED.format(gamePk=123456) == captured["url"]

def test_get_game_pks_for_team_on_date_filters(monkeypatch):
    # Construct a fake schedule response with multiple games, some not involving the team
    TEAM = 112
    other = 999
    schedule = {
        "dates": [{
            "games": [
                {"gamePk": 1, "teams": {"home": {"team": {"id": TEAM}}, "away": {"team": {"id": other}}}},
                {"gamePk": 2, "teams": {"home": {"team": {"id": other}}, "away": {"team": {"id": TEAM}}}},
                {"gamePk": 3, "teams": {"home": {"team": {"id": other}}, "away": {"team": {"id": other}}}},
            ]
        }]
    }

    def fake_fetch(url: str, params=None, **kwargs):
        assert url == mlb_ingest.BASE_SCHEDULE
        assert params["teamId"] == TEAM
        return schedule

    monkeypatch.setattr(mlb_ingest, "fetch_json", fake_fetch)
    games = mlb_ingest.get_game_pks_for_team_on_date(TEAM, "2024-04-01")
    assert [g["gamePk"] for g in games] == [1, 2]


def test_get_game_pks_for_team_on_date_no_dates(monkeypatch):
    monkeypatch.setattr(mlb_ingest, "fetch_json", lambda *a, **k: {"dates": []})
    assert mlb_ingest.get_game_pks_for_team_on_date(112, "2024-01-01") == []

def test_main_documents_team_id_behavior(monkeypatch, tmp_path, capsys):
    game = {
        "gamePk": 424242,
        "teams": {"home": {"team": {"name": "Cubs"}}, "away": {"team": {"name": "Cards"}}},
        "status": {"detailedState": "Scheduled"},
    }

    # Track what team_id is passed to get_game_pks_for_team_on_date
    called = {"team_id": None}
    def fake_get_games(team_id: int, date_str: str):
        called["team_id"] = team_id
        return [game]

    # Avoid real network and sleeping
    feed = {"gameData": {"status": {"detailedState": "Final"}, "datetime": {"officialDate": "2024-04-01"}}}
    monkeypatch.setattr(mlb_ingest, "get_game_pks_for_team_on_date", fake_get_games)
    monkeypatch.setattr(mlb_ingest, "fetch_feed_live", lambda pk: feed)
    monkeypatch.setattr(mlb_ingest, "save_json", lambda obj, path: None)

    argv = ["mlb_ingest.py", "--team", "999", "--date", "2024-04-01", "--outdir", str(tmp_path)]
    monkeypatch.setattr(sys, "argv", argv)
    mlb_ingest.main()

    out = capsys.readouterr().out
    # Confirms printed team_id is the constant (documenting current behavior)
    assert f"team_id={mlb_ingest.TEAM_ID}" in out
    # Confirms the function also received the constant, not 999
    assert called["team_id"] == mlb_ingest.TEAM_ID


@pytest.mark.parametrize("max_wait_min", [0, 1, 2, 3])
def test_main_stops_on_final_without_sleep(monkeypatch, tmp_path, capsys, max_wait_min):
    game = {"gamePk": 7, "teams": {}, "status": {"detailedState": "In Progress"}}
    monkeypatch.setattr(mlb_ingest, "get_game_pks_for_team_on_date", lambda tid, d: [game])

    calls = {"count": 0}
    def fake_fetch(pk):
        calls["count"] += 1
        return {"gameData": {"status": {"detailedState": "Final"}, "datetime": {"officialDate": "2024-04-01"}}}

    monkeypatch.setattr(mlb_ingest, "fetch_feed_live", fake_fetch)
    monkeypatch.setattr(mlb_ingest, "save_json", lambda obj, path: None)
    monkeypatch.setattr(sys, "argv", ["mlb_ingest.py", "--team", str(mlb_ingest.TEAM_ID), "--date", "2024-04-01", "--outdir", str(tmp_path), "--wait", "--max-wait-min", str(max_wait_min)])
    # Make sleep a no-op just in case
    monkeypatch.setattr(mlb_ingest.time, "sleep", lambda s: None)

    mlb_ingest.main()
    # Only one poll because it is Final immediately
    assert calls["count"] == 1
