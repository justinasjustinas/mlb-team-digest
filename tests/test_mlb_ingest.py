# tests/test_mlb_ingest.py
import json
import sys
import types
from pathlib import Path
from typing import Any, Dict

import pytest

import mlb_ingest


# --------------------------
# Helpers / fixtures
# --------------------------

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
    innings_home = (0, 0, 0, 1, 0, 0, 0, 0, 0),
    innings_away = (2, 0, 3, 0, 0, 0, 3, 4, 0),
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
                "teams": {
                    "home": {"runs": home_runs},
                    "away": {"runs": away_runs},
                },
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
                                        "baseOnBalls": 0, "homeRuns": 0, "battersFaced": 0,
                                        "pitchesThrown": 0, "strikes": 0, "hitByPitch": 0, "wildPitches": 0,
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
                                        "pitchesThrown": 95, "strikes": 62, "hitByPitch": 0, "wildPitches": 0,
                                    },
                                },
                            }
                        }
                    },
                }
            }
        },
    }


# --------------------------
# Unit tests (pure helpers)
# --------------------------

def test_decide_sink_prefers_cli_and_env(monkeypatch):
    monkeypatch.delenv("OUTPUT_SINK", raising=False)
    monkeypatch.delenv("K_SERVICE", raising=False)
    assert mlb_ingest.decide_sink(None) == "json"

    monkeypatch.setenv("OUTPUT_SINK", "bq")
    assert mlb_ingest.decide_sink(None) == "bq"

    assert mlb_ingest.decide_sink("json") == "json"


def test_parse_date_or_today_with_explicit_value():
    d = mlb_ingest.parse_date_or_today("2025-08-23")
    assert str(d) == "2025-08-23"


def test_summarize_game_minimal_fields():
    feed = sample_feed()
    row = mlb_ingest.summarize_game(feed)
    assert row["game_id"] == 776618
    assert row["game_date"] == "2025-08-23"
    assert row["home_team_id"] == 108
    assert row["away_team_id"] == 112
    assert row["home_team"] == "Los Angeles Angels"
    assert row["away_team"] == "Chicago Cubs"
    assert row["home_runs"] == 1
    assert row["away_runs"] == 12
    assert row["status_detailed"] == "Final"
    assert row["status_abstract"] == "Final"
    assert row["game_time_utc"] == "2025-08-24T01:38:00Z"
    assert row["venue_tz"] == "America/Los_Angeles"
    assert "ingested_at_utc" in row


def test_summarize_linescore_builds_flat_innings():
    feed = sample_feed()
    ls = mlb_ingest.summarize_linescore(feed)
    assert ls["game_id"] == 776618
    assert ls["game_date"] == "2025-08-23"
    assert ls["home_team_id"] == 108
    assert ls["away_team_id"] == 112
    assert ls["total_home"] == 1
    assert ls["total_away"] == 12
    assert ls["home_inn_1"] == 0
    assert ls["away_inn_1"] == 2
    assert ls["home_inn_4"] == 1
    assert ls["away_inn_7"] == 3
    for i in range(1, 16):
        assert f"home_inn_{i}" in ls and f"away_inn_{i}" in ls


def test_iter_player_rows_minimal_projection():
    feed = sample_feed()
    rows = mlb_ingest.iter_player_rows(feed)
    assert len(rows) == 2
    away = next(r for r in rows if r["player_id"] == 2002)
    assert away["team_side"] == "away"
    assert away["team_id"] == 112
    assert away["team_name"] == "Chicago Cubs"
    assert away["primary_pos"] == "P"
    assert away["game_date"] == "2025-08-23"
    assert away["outs"] == 21
    assert away["ip_str"] == "7.0"
    assert away["er"] == 1
    assert away["k"] == 8
    assert away["h_allowed"] == 4
    assert away["bb_allowed"] == 1
    assert away["hr_allowed"] == 0
    assert away["bf"] == 26
    assert away["pitches"] == 95
    assert away["strikes"] == 62
    assert away["hbp"] == 0
    assert away["wp"] == 0
    for k in ("ab","r","h","doubles","triples","hr","rbi","bb","so","sb","cs","sf","sh"):
        assert k in away


# --------------------------
# Main / I/O behavior
# --------------------------

def test_main_json_writes_only_for_final(monkeypatch, tmp_path):
    # Make schedule return two games: one Final, one Live
    schedule = {
        "dates": [{
            "games": [
                {"gamePk": 111},
                {"gamePk": 222},
            ]
        }]
    }

    def fake_http(url, params=None, **_):
        if "schedule" in url:
            return schedule
        if "111" in url:
            return sample_feed(game_pk=111, detailed="Final", abstract="Final")
        if "222" in url:
            return sample_feed(game_pk=222, detailed="In Progress", abstract="Live")
        raise AssertionError(f"Unexpected URL: {url}")

    monkeypatch.setenv("OUTPUT_SINK", "json")
    monkeypatch.delenv("K_SERVICE", raising=False)
    monkeypatch.setattr(mlb_ingest, "http_get_json", fake_http)

    outdir = tmp_path / "out"
    args = ["--team", "112", "--date", "2025-08-23", "--json_outdir", str(outdir)]
    rc = mlb_ingest.main(args)
    assert rc == 0

    want = {
        outdir / "111_summary.json",
        outdir / "111_linescore.json",
        outdir / "111_players.json",
    }
    got = set(outdir.glob("*.json"))
    assert want.issubset(got)
    assert not any(p.name.startswith("222_") for p in got)

    summary = json.loads((outdir / "111_summary.json").read_text())
    assert summary["status_abstract"] == "Final"


def test_write_json_writes_file(monkeypatch, tmp_path: Path):
    # write_json does not create parent dirs by itself; main() does that.
    # So the test should prepare the parent directory.
    obj = {"hello": "world"}
    out = tmp_path / "subdir" / "example.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    mlb_ingest.write_json(obj, str(out))
    assert out.exists()
    assert json.loads(out.read_text()) == obj


# --------------------------
# BigQuery stubs (module + client)
# --------------------------

# Minimal fake bigquery module so write_bq_all can import schema classes
class _BQ_SchemaField:
    def __init__(self, name, field_type): self.name, self.field_type = name, field_type

class _BQ_Table:
    def __init__(self, table_id, schema=None): self.table_id, self.schema = table_id, schema

class _BQ_Dataset:
    def __init__(self, ds_id): self.dataset_id = ds_id

class _BQ_LoadJobConfig:
    def __init__(self, write_disposition=None): self.write_disposition = write_disposition

class _BQ_QueryJobConfig:
    def __init__(self, query_parameters=None): self.query_parameters = query_parameters or []

class _BQ_ArrayQueryParameter:
    def __init__(self, name, typ, values): self.name, self.typ, self.values = name, typ, values

class _BQ_FakeLoadJob:
    def result(self): return None

class FakeClient:
    def __init__(self):
        self.project = "fake"
        self.created_tables = []
        self.loaded = []
        self.queries = []

    def get_dataset(self, dataset_id): raise Exception("not found")
    def create_dataset(self, ds, exists_ok=False): return ds

    def get_table(self, table): raise Exception("not found")
    def create_table(self, table):
        self.created_tables.append(table.table_id)
        return table

    def load_table_from_json(self, rows, destination, job_config=None):
        self.loaded.append((destination, list(rows)))
        return _BQ_FakeLoadJob()

    # used by bq_delete_by_game_ids()
    def query(self, q, job_config=None):
        self.queries.append((q, job_config))
        return _BQ_FakeLoadJob()


def _install_fake_bigquery(monkeypatch):
    google = types.ModuleType("google")
    cloud = types.ModuleType("google.cloud")
    bq = types.ModuleType("google.cloud.bigquery")
    bq.SchemaField = _BQ_SchemaField
    bq.Table = _BQ_Table
    bq.Dataset = _BQ_Dataset
    bq.LoadJobConfig = _BQ_LoadJobConfig
    bq.QueryJobConfig = _BQ_QueryJobConfig
    bq.ArrayQueryParameter = _BQ_ArrayQueryParameter

    google.cloud = cloud
    cloud.bigquery = bq

    monkeypatch.setitem(sys.modules, "google", google)
    monkeypatch.setitem(sys.modules, "google.cloud", cloud)
    monkeypatch.setitem(sys.modules, "google.cloud.bigquery", bq)


def test_write_bq_all_uses_client_and_appends(monkeypatch):
    # Install fake bigquery module & patch client factory
    _install_fake_bigquery(monkeypatch)
    fake = FakeClient()
    monkeypatch.setattr(mlb_ingest, "bq_client", lambda project=None: fake)

    summaries = [mlb_ingest.summarize_game(sample_feed(game_pk=999))]
    linescore = [mlb_ingest.summarize_linescore(sample_feed(game_pk=999))]
    players = mlb_ingest.iter_player_rows(sample_feed(game_pk=999))

    mlb_ingest.write_bq_all(summaries, linescore, players, project="fake", dataset_location="EU")

    # Three loads happened
    assert len(fake.loaded) == 3
    dests = [d for (d, _) in fake.loaded]
    assert any("game_summaries" in d for d in dests)
    assert any("game_linescore" in d for d in dests)
    assert any("game_boxscore_players" in d for d in dests)

    # Delete-by-game_id query was issued for upsert behavior
    assert any("DELETE FROM" in q for (q, _) in fake.queries)
