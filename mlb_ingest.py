#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import sys
import time
from typing import Any, Dict, List, Optional

import requests  # pip install requests
# Local runs need: pip install pytz
# BigQuery writes need: pip install google-cloud-bigquery

# =============================
# Config / Defaults
# =============================
BASEBALL_TZ = "America/New_York"  # league slate keyed to Eastern
SCHEDULE_URL = "https://statsapi.mlb.com/api/v1/schedule"
LIVE_FEED_TMPL = "https://statsapi.mlb.com/api/v1.1/game/{gamePk}/feed/live"

# BigQuery tables (dataset.table). Override via env if desired.
BQ_SUMMARIES = os.getenv("BQ_SUMMARIES", "mlb.game_summaries")
BQ_LINESCORE = os.getenv("BQ_LINESCORE", "mlb.game_linescore")
BQ_PLAYERS   = os.getenv("BQ_PLAYERS",   "mlb.game_boxscore_players")
BQ_PROJECT   = os.getenv("BQ_PROJECT")            # optional
BQ_LOCATION  = os.getenv("BQ_LOCATION", "EU")     # when creating datasets

# =============================
# Small utils
# =============================
def log(msg: str) -> None:
    print(msg, flush=True)

def is_cloud_run() -> bool:
    return "K_SERVICE" in os.environ  # set by Cloud Run

def decide_sink(cli_output: Optional[str]) -> str:
    if cli_output in ("bq", "json"):
        return cli_output
    env = os.getenv("OUTPUT_SINK")
    if env in ("bq", "json"):
        return env
    return "bq" if is_cloud_run() else "json"

def parse_date_or_today(date_str: Optional[str]) -> dt.date:
    import pytz  # ensure installed locally
    if date_str:
        return dt.datetime.strptime(date_str, "%Y-%m-%d").date()
    tz = pytz.timezone(BASEBALL_TZ)
    return dt.datetime.now(tz).date()

def http_get_json(url: str, params: Dict[str, Any] | None = None, retries: int = 3, timeout: int = 20) -> Dict[str, Any]:
    params = params or {}
    backoff = 1.6
    for attempt in range(retries):
        try:
            r = requests.get(url, params=params, timeout=timeout)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            if attempt == retries - 1:
                raise
            sleep = backoff ** attempt
            log(f"HTTP error {e}; retrying in {sleep:.1f}s …")
            time.sleep(sleep)

def _safe_int(x: Any) -> Optional[int]:
    try:
        return int(x) if x is not None else None
    except Exception:
        return None

# Final detection (only write when final)
FINAL_PREFIXES = ("Final", "Game Over", "Completed")  # detailedState often begins with these
def is_final(detailed: Optional[str], abstract: Optional[str]) -> bool:
    if abstract and abstract.lower() in ("final", "completed"):
        return True
    if detailed:
        return any(detailed.startswith(p) for p in FINAL_PREFIXES)
    return False

# =============================
# Fetch / Transform
# =============================
def fetch_game_ids_for(team_id: int, game_date: dt.date) -> List[int]:
    """Schedule call to discover game IDs for the team on the date."""
    params = {"sportId": 1, "teamId": team_id, "date": game_date.isoformat()}
    data = http_get_json(SCHEDULE_URL, params=params)
    ids: List[int] = []
    for d in data.get("dates", []):
        for g in d.get("games", []):
            if g.get("gamePk"):
                ids.append(int(g["gamePk"]))
    return ids

def fetch_live_feed(game_id: int) -> Dict[str, Any]:
    return http_get_json(LIVE_FEED_TMPL.format(gamePk=game_id))

def summarize_game(feed: Dict[str, Any]) -> Dict[str, Any]:
    """
    mlb.game_summaries — only fields the digest/scheduling needs.
    Columns:
      game_id, game_date,
      home_team_id, home_team,
      away_team_id, away_team,
      home_runs, away_runs,
      status_detailed, status_abstract,
      game_time_utc, venue_tz,
      ingested_at_utc
    """
    gd = feed.get("gameData", {}) or {}
    ld = feed.get("liveData", {}) or {}

    teams_meta = gd.get("teams", {}) or {}
    home_meta = teams_meta.get("home", {}) or {}
    away_meta = teams_meta.get("away", {}) or {}

    status_block = gd.get("status", {}) or {}
    status_detailed = status_block.get("detailedState")
    status_abstract = status_block.get("abstractGameState")  # e.g., "Live", "Final", "Preview"

    # Linescore totals (may be absent pregame)
    ls = (ld.get("linescore", {}) or {})
    t_home = (ls.get("teams", {}).get("home", {}) or {})
    t_away = (ls.get("teams", {}).get("away", {}) or {})

    # Scheduling helpers (UTC start + venue tz)
    dt_block = (gd.get("datetime", {}) or {})
    game_time_rfc3339 = dt_block.get("dateTime")  # e.g., "2025-08-23T18:05:00Z"
    venue_tz = ((gd.get("venue", {}) or {}).get("timeZone", {}) or {}).get("id") \
               or (dt_block.get("timeZone", {}) or {}).get("id")

    return {
        "game_id": _safe_int(feed.get("gamePk")),
        "game_date": dt_block.get("originalDate"),
        "home_team_id": _safe_int(home_meta.get("id")),
        "home_team": home_meta.get("name"),
        "away_team_id": _safe_int(away_meta.get("id")),
        "away_team": away_meta.get("name"),
        "home_runs": _safe_int(t_home.get("runs")),
        "away_runs": _safe_int(t_away.get("runs")),
        "status_detailed": status_detailed,
        "status_abstract": status_abstract,
        "game_time_utc": game_time_rfc3339,
        "venue_tz": venue_tz,
        "ingested_at_utc": dt.datetime.utcnow().isoformat(timespec="seconds") + "Z",
    }

def summarize_linescore(feed: Dict[str, Any]) -> Dict[str, Any]:
    """
    mlb.game_linescore — minimal fields.
    Columns:
      game_id, game_date, home_team_id, away_team_id,
      home_inn_1..15, away_inn_1..15, total_home, total_away, ingested_at_utc
    """
    gd = feed.get("gameData", {}) or {}
    ld = feed.get("liveData", {}) or {}

    teams_meta = gd.get("teams", {}) or {}
    home_meta = teams_meta.get("home", {}) or {}
    away_meta = teams_meta.get("away", {}) or {}

    ls = ld.get("linescore", {}) or {}
    innings = ls.get("innings", []) or []

    row: Dict[str, Any] = {
        "game_id": _safe_int(feed.get("gamePk")),
        "game_date": (gd.get("datetime", {}) or {}).get("originalDate"),
        "home_team_id": _safe_int(home_meta.get("id")),
        "away_team_id": _safe_int(away_meta.get("id")),
        "total_home": _safe_int((ls.get("teams", {}).get("home", {}) or {}).get("runs")),
        "total_away": _safe_int((ls.get("teams", {}).get("away", {}) or {}).get("runs")),
        "ingested_at_utc": dt.datetime.utcnow().isoformat(timespec="seconds") + "Z",
    }
    for i in range(1, 16):  # flatten up to 15 innings
        inn = innings[i - 1] if i - 1 < len(innings) else {}
        row[f"home_inn_{i}"] = _safe_int((inn.get("home", {}) or {}).get("runs"))
        row[f"away_inn_{i}"] = _safe_int((inn.get("away", {}) or {}).get("runs"))
    return row

def iter_player_rows(feed: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    mlb.game_boxscore_players — exactly the batting/pitching fields needed.
    Columns:
      game_id, game_date, team_side, team_id, team_name,
      player_id, player_name, primary_pos,
      Batting: ab,r,h,doubles,triples,hr,rbi,bb,so,sb,cs,sf,sh
      Pitching: outs,ip_str,er,k,h_allowed,bb_allowed,hr_allowed,bf,pitches,strikes,hbp,wp
      ingested_at_utc
    """
    gd = feed.get("gameData", {}) or {}
    game_date = (gd.get("datetime", {}) or {}).get("originalDate")

    teams_meta = gd.get("teams", {}) or {}
    home_meta = teams_meta.get("home", {}) or {}
    away_meta = teams_meta.get("away", {}) or {}

    ld = feed.get("liveData", {}) or {}
    box = (ld.get("boxscore", {}) or {}).get("teams", {}) or {}

    out_rows: List[Dict[str, Any]] = []

    for side in ("home", "away"):
        team_meta = home_meta if side == "home" else away_meta
        team_id = team_meta.get("id")
        team_name = team_meta.get("name")

        players = (box.get(side, {}) or {}).get("players", {}) or {}
        for pdata in players.values():
            person = pdata.get("person", {}) or {}
            pos = (pdata.get("position", {}) or {}).get("abbreviation")
            batting = (pdata.get("stats", {}) or {}).get("batting", {}) or {}
            pitching = (pdata.get("stats", {}) or {}).get("pitching", {}) or {}

            out_rows.append({
                "game_id": _safe_int(feed.get("gamePk")),
                "game_date": game_date,
                "team_side": side,                      # "home" | "away"
                "team_id": _safe_int(team_id),
                "team_name": team_name,
                "player_id": _safe_int(person.get("id")),
                "player_name": person.get("fullName"),
                "primary_pos": pos,

                # Batting
                "ab": _safe_int(batting.get("atBats")),
                "r": _safe_int(batting.get("runs")),
                "h": _safe_int(batting.get("hits")),
                "doubles": _safe_int(batting.get("doubles")),
                "triples": _safe_int(batting.get("triples")),
                "hr": _safe_int(batting.get("homeRuns")),
                "rbi": _safe_int(batting.get("rbi")),
                "bb": _safe_int(batting.get("baseOnBalls")),
                "so": _safe_int(batting.get("strikeOuts")),
                "sb": _safe_int(batting.get("stolenBases")),
                "cs": _safe_int(batting.get("caughtStealing")),
                "sf": _safe_int(batting.get("sacFlies")),
                "sh": _safe_int(batting.get("sacBunts")),

                # Pitching
                "outs": _safe_int(pitching.get("outs")),
                "ip_str": pitching.get("inningsPitched"),
                "er": _safe_int(pitching.get("earnedRuns")),
                "k": _safe_int(pitching.get("strikeOuts")),
                "h_allowed": _safe_int(pitching.get("hits")),
                "bb_allowed": _safe_int(pitching.get("baseOnBalls")),
                "hr_allowed": _safe_int(pitching.get("homeRuns")),
                "bf": _safe_int(pitching.get("battersFaced")),
                "pitches": _safe_int(pitching.get("pitchesThrown")),
                "strikes": _safe_int(pitching.get("strikes")),
                "hbp": _safe_int(pitching.get("hitByPitch")),
                "wp": _safe_int(pitching.get("wildPitches")),

                "ingested_at_utc": dt.datetime.utcnow().isoformat(timespec="seconds") + "Z",
            })
    return out_rows

# =============================
# JSON sink (local)
# =============================
def write_json(obj: Dict[str, Any], path: str) -> None:
    import pathlib
    pathlib.Path(path).write_text(json.dumps(obj, indent=2))
    log(f"💾 Wrote JSON to {path}")

# =============================
# BigQuery sink (Cloud Run / local BQ)
# =============================
def bq_client(project: Optional[str] = None):
    try:
        from google.cloud import bigquery
    except Exception as e:
        raise RuntimeError("Missing google-cloud-bigquery. Add it to your env or container.") from e
    return bigquery.Client(project=project) if project else bigquery.Client()

def bq_ensure_dataset(client, dataset_id: str, location: Optional[str] = None) -> None:
    from google.cloud import bigquery
    try:
        client.get_dataset(dataset_id)
    except Exception:
        ds = bigquery.Dataset(f"{client.project}.{dataset_id}")
        if location:
            ds.location = location
        client.create_dataset(ds, exists_ok=True)

def bq_ensure_table(client, full_table_id: str, schema) -> None:
    try:
        client.get_table(full_table_id)
    except Exception:
        from google.cloud import bigquery
        table = bigquery.Table(full_table_id, schema=schema)
        client.create_table(table)
        log(f"Created table {full_table_id}")

def bq_delete_by_game_ids(client, table_fq: str, game_ids: List[int]) -> None:
    if not game_ids:
        return
    from google.cloud import bigquery
    q = f"DELETE FROM `{table_fq}` WHERE game_id IN UNNEST(@ids)"
    job = client.query(q, job_config=bigquery.QueryJobConfig(
        query_parameters=[bigquery.ArrayQueryParameter("ids", "INT64", game_ids)]
    ))
    job.result()

def bq_load_rows(client, full_table_id: str, rows: List[Dict[str, Any]]) -> None:
    if not rows:
        return
    from google.cloud import bigquery
    job = client.load_table_from_json(
        rows,
        destination=full_table_id,
        job_config=bigquery.LoadJobConfig(write_disposition="WRITE_APPEND"),
    )
    job.result()
    log(f"✅ Appended {len(rows)} row(s) to {full_table_id}")

def write_bq_all(
    summaries: List[Dict[str, Any]],
    linescores: List[Dict[str, Any]],
    player_rows: List[Dict[str, Any]],
    project: Optional[str] = None,
    dataset_location: Optional[str] = None,
) -> None:
    from google.cloud import bigquery

    client = bq_client(project)

    def fq(table_spec: str) -> tuple[str, str]:
        ds, tbl = table_spec.split(".", 1)
        return f"{client.project}.{ds}.{tbl}", ds

    tbl_summaries, ds_summaries = fq(BQ_SUMMARIES)
    tbl_linescore, ds_linescore = fq(BQ_LINESCORE)
    tbl_players,   ds_players   = fq(BQ_PLAYERS)

    # Ensure datasets (use EU for europe-west4)
    bq_ensure_dataset(client, ds_summaries, dataset_location)
    bq_ensure_dataset(client, ds_linescore, dataset_location)
    bq_ensure_dataset(client, ds_players,   dataset_location)

    # Schemas (minimal)
    SUMMARIES_SCHEMA = [
        bigquery.SchemaField("game_id", "INTEGER"),
        bigquery.SchemaField("game_date", "DATE"),
        bigquery.SchemaField("home_team_id", "INTEGER"),
        bigquery.SchemaField("home_team", "STRING"),
        bigquery.SchemaField("away_team_id", "INTEGER"),
        bigquery.SchemaField("away_team", "STRING"),
        bigquery.SchemaField("home_runs", "INTEGER"),
        bigquery.SchemaField("away_runs", "INTEGER"),
        bigquery.SchemaField("status_detailed", "STRING"),
        bigquery.SchemaField("status_abstract", "STRING"),
        bigquery.SchemaField("game_time_utc", "TIMESTAMP"),
        bigquery.SchemaField("venue_tz", "STRING"),
        bigquery.SchemaField("ingested_at_utc", "TIMESTAMP"),
    ]

    LINESCORE_SCHEMA = [
        bigquery.SchemaField("game_id", "INTEGER"),
        bigquery.SchemaField("game_date", "DATE"),
        bigquery.SchemaField("home_team_id", "INTEGER"),
        bigquery.SchemaField("away_team_id", "INTEGER"),
        bigquery.SchemaField("total_home", "INTEGER"),
        bigquery.SchemaField("total_away", "INTEGER"),
        *[bigquery.SchemaField(f"home_inn_{i}", "INTEGER") for i in range(1, 16)],
        *[bigquery.SchemaField(f"away_inn_{i}", "INTEGER") for i in range(1, 16)],
        bigquery.SchemaField("ingested_at_utc", "TIMESTAMP"),
    ]

    PLAYERS_SCHEMA = [
        bigquery.SchemaField("game_id", "INTEGER"),
        bigquery.SchemaField("game_date", "DATE"),
        bigquery.SchemaField("team_side", "STRING"),
        bigquery.SchemaField("team_id", "INTEGER"),
        bigquery.SchemaField("team_name", "STRING"),
        bigquery.SchemaField("player_id", "INTEGER"),
        bigquery.SchemaField("player_name", "STRING"),
        bigquery.SchemaField("primary_pos", "STRING"),
        # Batting
        bigquery.SchemaField("ab", "INTEGER"),
        bigquery.SchemaField("r", "INTEGER"),
        bigquery.SchemaField("h", "INTEGER"),
        bigquery.SchemaField("doubles", "INTEGER"),
        bigquery.SchemaField("triples", "INTEGER"),
        bigquery.SchemaField("hr", "INTEGER"),
        bigquery.SchemaField("rbi", "INTEGER"),
        bigquery.SchemaField("bb", "INTEGER"),
        bigquery.SchemaField("so", "INTEGER"),
        bigquery.SchemaField("sb", "INTEGER"),
        bigquery.SchemaField("cs", "INTEGER"),
        bigquery.SchemaField("sf", "INTEGER"),
        bigquery.SchemaField("sh", "INTEGER"),
        # Pitching
        bigquery.SchemaField("outs", "INTEGER"),
        bigquery.SchemaField("ip_str", "STRING"),
        bigquery.SchemaField("er", "INTEGER"),
        bigquery.SchemaField("k", "INTEGER"),
        bigquery.SchemaField("h_allowed", "INTEGER"),
        bigquery.SchemaField("bb_allowed", "INTEGER"),
        bigquery.SchemaField("hr_allowed", "INTEGER"),
        bigquery.SchemaField("bf", "INTEGER"),
        bigquery.SchemaField("pitches", "INTEGER"),
        bigquery.SchemaField("strikes", "INTEGER"),
        bigquery.SchemaField("hbp", "INTEGER"),
        bigquery.SchemaField("wp", "INTEGER"),
        bigquery.SchemaField("ingested_at_utc", "TIMESTAMP"),
    ]

    # Ensure tables
    bq_ensure_table(client, tbl_summaries, SUMMARIES_SCHEMA)
    bq_ensure_table(client, tbl_linescore, LINESCORE_SCHEMA)
    bq_ensure_table(client, tbl_players,   PLAYERS_SCHEMA)

    # Upsert behavior: delete existing rows for these game_ids, then append new
    final_ids = [r["game_id"] for r in summaries if r.get("game_id") is not None]
    if final_ids:
        bq_delete_by_game_ids(client, tbl_summaries, final_ids)
        bq_delete_by_game_ids(client, tbl_linescore, final_ids)
        bq_delete_by_game_ids(client, tbl_players,   final_ids)

    # Load rows
    bq_load_rows(client, tbl_summaries, summaries)
    bq_load_rows(client, tbl_linescore, linescores)
    bq_load_rows(client, tbl_players,   player_rows)

# =============================
# CLI
# =============================
def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="Ingest MLB data (minimal fields for game_digest).")
    ap.add_argument("--team", type=int, required=True, help="MLB team ID (e.g., 112 for Cubs).")
    ap.add_argument("--date", type=str, help="YYYY-MM-DD; defaults to 'today' in America/New_York.")
    ap.add_argument("--output", choices=["bq", "json"], help="Override sink. Default: CloudRun=bq, local=json")
    ap.add_argument("--json_outdir", default="data", help="Where to write JSON files locally.")
    args = ap.parse_args(argv)

    game_date = parse_date_or_today(args.date)
    sink = decide_sink(args.output)

    log(f"🔎 Fetching games for team={args.team} on {game_date} ({BASEBALL_TZ}) …")
    game_ids = fetch_game_ids_for(args.team, game_date)
    if not game_ids:
        log("No games found.")
        return 0

    summaries: List[Dict[str, Any]] = []
    linescores: List[Dict[str, Any]] = []
    players: List[Dict[str, Any]] = []

    for gid in game_ids:
        log(f"📥 Pulling live feed for game {gid} …")
        feed = fetch_live_feed(gid)

        s = summarize_game(feed)
        l = summarize_linescore(feed)
        p = iter_player_rows(feed)

        # Only write when FINAL
        if is_final(s.get("status_detailed"), s.get("status_abstract")):
            summaries.append(s)
            linescores.append(l)
            players.extend(p)
            log(f"🟢 FINAL: game_id={s['game_id']} {s.get('away_team')} @ {s.get('home_team')}  "
                f"{s.get('away_runs')}–{s.get('home_runs')}")
        else:
            log(f"🟡 NOT FINAL yet: game_id={s.get('game_id')} "
                f"status_detailed={s.get('status_detailed')} status_abstract={s.get('status_abstract')} — skipping write")

        if sink == "json":
            # For local inspection: drop JSON files only for FINAL games (keeps it clean)
            if summaries and summaries[-1]["game_id"] == s["game_id"]:
                os.makedirs(args.json_outdir, exist_ok=True)
                write_json(s, os.path.join(args.json_outdir, f"{gid}_summary.json"))
                write_json(l, os.path.join(args.json_outdir, f"{gid}_linescore.json"))
                write_json({"players": p}, os.path.join(args.json_outdir, f"{gid}_players.json"))

    if sink == "bq":
        if summaries:
            write_bq_all(
                summaries=summaries,
                linescores=linescores,
                player_rows=players,
                project=BQ_PROJECT,
                dataset_location=BQ_LOCATION,
            )
        else:
            log("⏭️ No FINAL games this run — nothing written to BigQuery.")

    log("✅ Done.")
    return 0

if __name__ == "__main__":
    sys.exit(main())
