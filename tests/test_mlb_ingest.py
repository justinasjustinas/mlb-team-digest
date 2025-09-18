# tests/test_mlb_ingest.py
import json
import math
import sys
from typing import Any, Dict, List

import pytest

# ---- Bring the module under test into the path (adjust if needed)
sys.path.append(".")
import mlb_ingest as mod  # noqa: E402


# ------------------------
# Helpers / sample payloads
# ------------------------
def sample_feed(
    *,
    game_pk: int = 776618,
    game_date: str = "2025-08-23",
    detailed: str = "Final",
    abstract: str = "Final",
    venue_tz: str = "America/Los_Angeles",
    game_time_utc: str = "2025-08-24T01:38:00Z",
    home_id: int = 108,
    home_name: str = "Los Angeles Angels",
    away_id: int = 112,
    away_name: str = "Chicago Cubs",
    home_runs: int = 1,
    away_runs: int = 12,
    innings_home=(0, 0, 0, 1, 0, 0, 0, 0, 0),
    innings_away=(2, 0, 3, 0, 0, 0, 3, 4, 0),
) -> Dict[str, Any]:
    innings = []
    for i in range(max(len(innings_home), len(innings_away))):
        innings.append({
            "home": {"runs": innings_home[i] if i < len(innings_home) else None},
            "away": {"runs": innings_away[i] if i < len(innings_away) else None},
        })

    return {
        "gamePk": game_pk,
        "gameData": {
            "datetime": {
                "originalDate": game_date,
                "dateTime": game_time_utc,
                "timeZone": {"id": venue_tz},
            },
            "status": {
                "detailedState": detailed,
                "abstractGameState": abstract,
            },
            "teams": {
                "home": {"id": home_id, "name": home_name},
                "away": {"id": away_id, "name": away_name},
            },
            "venue": {"timeZone": {"id": venue_tz}},
        },
        "liveData": {
            "linescore": {
                "teams": {"home": {"runs": home_runs}, "away": {"runs": away_runs}},
                "innings": innings,
            },
            "boxscore": {
                "teams": {
                    "home": {
                        "players": {
                            "ID1": {
                                "person": {"id": 1001, "fullName": "Home Batter"},
                                "position": {"abbreviation": "1B"},
                                "stats": {
                                    "batting": {
                                        "atBats": 4, "runs": 1, "hits": 2,
                                        "doubles": 1, "triples": 0, "homeRuns": 0,
                                        "rbi": 1, "baseOnBalls": 0, "strikeOuts": 1,
                                        "stolenBases": 0, "caughtStealing": 0,
                                        "sacFlies": 0, "sacBunts": 0,
                                    },
                                    "pitching": {
                                        "outs": 0, "inningsPitched": "0.0",
                                        "earnedRuns": 0, "strikeOuts": 0, "hits": 0,
                                        "baseOnBalls": 0, "homeRuns": 0,
                                        "battersFaced": 0, "pitchesThrown": 0,
                                        "strikes": 0, "hitByPitch": 0, "wildPitches": 0,
                                    },
                                },
                            }
                        }
                    },
                    "away": {
                        "players": {
                            "ID2": {
                                "person": {"id": 2002, "fullName": "Away Pitcher"},
                                "position": {"abbreviation": "P"},
                                "gameStatus": {"isStarter": True},
                                "stats": {
                                    "batting": {
                                        "atBats": 0, "runs": 0, "hits": 0,
                                        "doubles": 0, "triples": 0, "homeRuns": 0,
                                        "rbi": 0, "baseOnBalls": 0, "strikeOuts": 0,
                                        "stolenBases": 0, "caughtStealing": 0,
                                        "sacFlies": 0, "sacBunts": 0,
                                    },
                                    "pitching": {
                                        "outs": 21, "inningsPitched": "7.0",
                                        "earnedRuns": 1, "strikeOuts": 8, "hits": 4,
                                        "baseOnBalls": 1, "homeRuns": 0, "battersFaced": 26,
                                        "pitchesThrown": 95, "strikes": 62, "hitByPitch": 0,
                                        "wildPitches": 0,
                                    },
                                },
                            }
                        }
                    },
                }
            }
        },
    }


def make_schedule_payload_from_feed(feed: Dict[str, Any]) -> Dict[str, Any]:
    """Transform the sample_feed into /schedule-like structure used by find_final_games_for_team."""
    g = {
        "gamePk": feed["gamePk"],
        "status": {"detailedState": feed["gameData"]["status"]["detailedState"]},
        "teams": {
            "home": {
                "team": {
                    "id": feed["gameData"]["teams"]["home"]["id"],
                    "name": feed["gameData"]["teams"]["home"]["name"],
                },
                "score": feed["liveData"]["linescore"]["teams"]["home"]["runs"],
            },
            "away": {
                "team": {
                    "id": feed["gameData"]["teams"]["away"]["id"],
                    "name": feed["gameData"]["teams"]["away"]["name"],
                },
                "score": feed["liveData"]["linescore"]["teams"]["away"]["runs"],
            },
        },
    }
    return {"dates": [{"games": [g]}]}


def make_linescore_from_feed(feed: Dict[str, Any]) -> Dict[str, Any]:
    return feed["liveData"]["linescore"]


def make_boxscore_from_feed(feed: Dict[str, Any]) -> Dict[str, Any]:
    return feed["liveData"]["boxscore"]


# ------------------------
# Unit tests: pure helpers
# ------------------------
def test_parse_ip_to_outs():
    assert mod.parse_ip_to_outs(None) == 0
    assert mod.parse_ip_to_outs(2) == 6  # int means "innings", convert to outs
    assert mod.parse_ip_to_outs("7.0") == 21
    assert mod.parse_ip_to_outs("6.1") == 19
    assert mod.parse_ip_to_outs("6.2") == 20
    assert mod.parse_ip_to_outs("abc") == 0


def test_compute_batting_metrics_simple():
    row = {
        "AB": 4, "H": 2, "BB": 0, "HBP": 0, "SF": 0,
        "HR": 0, "doubles": 1, "triples": 0, "R": 1, "RBI": 1, "SB": 0,
    }
    out = mod.compute_batting_metrics(dict(row))
    # AVG = 2/4 = 0.5; OBP = (2+0+0)/(4+0+0+0)=0.5; TB = 1*1 + 2*1 + 3*0 + 4*0 = 3; SLG=3/4=0.75; OPS=1.25
    assert math.isclose(out["AVG"], 0.5)
    assert math.isclose(out["OBP"], 0.5)
    assert math.isclose(out["SLG"], 0.75)
    assert math.isclose(out["OPS"], 1.25)
    # BAT_SCORE_RAW = 7.0 -> scaled 0..100 with LO=0, HI=20 => 35.0
    assert math.isclose(out["BAT_SCORE"], 35.0, rel_tol=1e-3)


def test_compute_pitching_metrics_simple():
    row = {"IP": "7.0", "ER": 1, "H": 4, "BB": 1, "HR": 0, "SO": 8}
    out = mod.compute_pitching_metrics(dict(row))
    # IP = 7.0 -> outs=21, ip=7.0; ERA = round(9/7, 2) = 1.29; WHIP = round(5/7, 2) = 0.71
    assert out["outs"] == 21
    assert math.isclose(out["IP"], 7.0)
    assert math.isclose(out["ERA"], round(9.0 / 7.0, 2), rel_tol=1e-6)
    assert math.isclose(out["WHIP"], round(5.0 / 7.0, 2), rel_tol=1e-6)
    # Raw PITCH_SCORE = 45 -> scaled LO=-10, HI=40 => >100, clamp to 100
    assert math.isclose(out["PITCH_SCORE"], 100.0, rel_tol=1e-6)


# ------------------------
# Flattening tests
# ------------------------
def test_flatten_linescore_from_feed():
    feed = sample_feed()
    ls = make_linescore_from_feed(feed)
    rows = mod.flatten_linescore(feed["gamePk"], ls)
    # Two rows per inning; we provided 9 innings -> 18 rows
    assert len(rows) == 18
    # Check first inning away/home values
    assert rows[0] == {"game_id": feed["gamePk"], "is_home": False, "inning_num": 1, "runs": 2}
    assert rows[1] == {"game_id": feed["gamePk"], "is_home": True,  "inning_num": 1, "runs": 0}


def test_flatten_game_summary_from_schedule_like():
    feed = sample_feed()
    sched = make_schedule_payload_from_feed(feed)
    g = sched["dates"][0]["games"][0]
    summary = mod.flatten_game_summary(g, feed["gameData"]["datetime"]["originalDate"])
    assert summary["game_id"] == feed["gamePk"]
    assert summary["home_team_id"] == feed["gameData"]["teams"]["home"]["id"]
    assert summary["away_team_id"] == feed["gameData"]["teams"]["away"]["id"]
    assert summary["home_score"] == feed["liveData"]["linescore"]["teams"]["home"]["runs"]
    assert summary["away_score"] == feed["liveData"]["linescore"]["teams"]["away"]["runs"]
    assert summary["status"] == "Final"


def test_flatten_boxscore_creates_batter_and_pitcher_rows():
    feed = sample_feed()
    box = make_boxscore_from_feed(feed)
    rows = mod.flatten_boxscore(feed["gamePk"], box)
    # One batter (home), one pitcher (away)
    roles = sorted([r["role"] for r in rows])
    assert roles == ["batter", "pitcher"]
    # Derived metrics exist
    batter = [r for r in rows if r["role"] == "batter"][0]
    pitcher = [r for r in rows if r["role"] == "pitcher"][0]
    assert "BAT_SCORE" in batter
    assert "PITCH_SCORE" in pitcher


# ------------------------
# I/O path: local JSON
# ------------------------
def test_write_json_triplet_and_main_json_flow(monkeypatch, tmp_path, capsys):
    feed = sample_feed()
    schedule_payload = make_schedule_payload_from_feed(feed)
    linescore_payload = make_linescore_from_feed(feed)
    boxscore_payload = make_boxscore_from_feed(feed)

    # Stub network fetches
    def fake_http_get_json(url, params=None, timeout=25):
        if url.endswith("/schedule"):
            return schedule_payload
        if "/linescore" in url:
            return linescore_payload
        if "/boxscore" in url:
            return boxscore_payload
        raise AssertionError(f"Unexpected URL: {url}")

    monkeypatch.setattr(mod, "http_get_json", fake_http_get_json)

    # Force JSON output directory
    monkeypatch.setenv("DIGEST_JSON_DIR", str(tmp_path))
    # The module captured JSON_DIR at import-time; override it on the module.
    mod.JSON_DIR = str(tmp_path)

    # Run CLI main with json mode
    argv = ["prog", "--team", "112", "--date", "2025-08-23", "--output", "json"]
    monkeypatch.setattr(sys, "argv", argv)
    rc = mod.main()
    assert rc == 0

    # Files are written
    base = tmp_path / str(feed["gamePk"])
    assert (base.with_name(f"{feed['gamePk']}_summary.json")).exists()
    assert (base.with_name(f"{feed['gamePk']}_linescore.json")).exists()
    assert (base.with_name(f"{feed['gamePk']}_players.json")).exists()

    # Index print
    out = capsys.readouterr().out

    def _extract_last_json_object_from_stdout(stdout: str) -> dict:
        start = stdout.rfind("{")
        end = stdout.rfind("}")
        assert start != -1 and end != -1 and end >= start
        return json.loads(stdout[start:end+1])

    payload = _extract_last_json_object_from_stdout(out)
    assert payload["written"] == 1

# ------------------------
# I/O path: BigQuery (mocked)
# ------------------------
def test_main_bq_flow(monkeypatch):
    feed = sample_feed()
    schedule_payload = make_schedule_payload_from_feed(feed)
    linescore_payload = make_linescore_from_feed(feed)
    boxscore_payload = make_boxscore_from_feed(feed)

    def fake_http_get_json(url, params=None, timeout=25):
        if url.endswith("/schedule"):
            return schedule_payload
        if "/linescore" in url:
            return linescore_payload
        if "/boxscore" in url:
            return boxscore_payload
        raise AssertionError(f"Unexpected URL: {url}")

    monkeypatch.setattr(mod, "http_get_json", fake_http_get_json)

    # Fake BQ client + capture writes
    class FakeBQClient:
        project = "proj"

    writes: List[tuple[str, List[Dict[str, Any]]]] = []

    def fake_bq_client_or_none(project):
        return FakeBQClient()

    def fake_bq_ensure_dataset(client, dataset_id, location):
        # no-op
        return None

    def fake_bq_write_rows(client, table_fqn, rows):
        writes.append((table_fqn, rows))

    monkeypatch.setattr(mod, "bq_client_or_none", fake_bq_client_or_none)
    monkeypatch.setattr(mod, "bq_ensure_dataset", fake_bq_ensure_dataset)
    monkeypatch.setattr(mod, "bq_write_rows", fake_bq_write_rows)

    argv = [
        "prog",
        "--team", "112",
        "--date", "2025-08-23",
        "--output", "bq",
        "--bq_project", "myproj",
        "--bq_dataset", "mlb",
    ]
    monkeypatch.setattr(sys, "argv", argv)
    rc = mod.main()
    assert rc == 0

    # Three tables should be written: summaries, linescore, players
    assert len(writes) == 3
    tables = [t for t, _ in writes]
    assert f"myproj.mlb.game_summaries" in tables
    assert f"myproj.mlb.game_linescore" in tables
    assert f"myproj.mlb.game_boxscore_players" in tables

    # Basic sanity on row counts
    tbl_to_rows = {t: r for t, r in writes}
    assert len(tbl_to_rows["myproj.mlb.game_summaries"]) == 1
    assert len(tbl_to_rows["myproj.mlb.game_linescore"]) == 18  # 9 innings * (home+away)
    assert len(tbl_to_rows["myproj.mlb.game_boxscore_players"]) == 2  # 1 batter + 1 pitcher


# ------------------------
# Env / defaults
# ------------------------
def test_is_cloud_env_and_default_output_mode(monkeypatch):
    # Clear
    for k in ["K_SERVICE", "CLOUD_RUN_JOB", "GAE_ENV", "GOOGLE_CLOUD_PROJECT", "BQ_PROJECT"]:
        monkeypatch.delenv(k, raising=False)
    assert mod.is_cloud_env() is False
    assert mod.default_output_mode() == "json"

    # Any of these set => cloud
    monkeypatch.setenv("GOOGLE_CLOUD_PROJECT", "x")
    assert mod.is_cloud_env() is True
    assert mod.default_output_mode() == "bq"
