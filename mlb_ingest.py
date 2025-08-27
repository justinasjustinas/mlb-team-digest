#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
mlb_ingest.py — Ingest MLB data for a team/date, compute derived metrics,
and either write per-game JSON triplets locally OR write rows to BigQuery.

CLI (backward-style):
  --team  (id like 112 OR exact team name like "Chicago Cubs")
  --date  (YYYY-MM-DD, optional; defaults to eastern 'today')
  --output bq|json  (auto-detect: cloud->bq, local->json)

Local JSON outputs under DIGEST_JSON_DIR or ./data:
  {gamePk}_summary.json   # single object
  {gamePk}_linescore.json # list
  {gamePk}_players.json   # list (batters + pitchers with derived metrics)

BigQuery tables (dataset defaults to 'mlb'):
  - game_summaries
  - game_linescore
  - game_boxscore_players
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import time
from typing import Any, Dict, List, Optional

import requests  # pip install requests

BASEBALL_TZ   = os.getenv("BASEBALL_TZ", "America/New_York")
BQ_PROJECT    = os.getenv("BQ_PROJECT", os.getenv("GOOGLE_CLOUD_PROJECT", ""))
BQ_DATASET    = os.getenv("BQ_DATASET", "mlb")
BQ_LOCATION   = os.getenv("BQ_LOCATION", "US")
JSON_DIR      = os.getenv("DIGEST_JSON_DIR", "data")

API = "https://statsapi.mlb.com/api/v1"

# ---------------
# Env autodetect
# ---------------
def is_cloud_env() -> bool:
    return bool(os.getenv("K_SERVICE") or os.getenv("CLOUD_RUN_JOB") or os.getenv("GAE_ENV") or os.getenv("GOOGLE_CLOUD_PROJECT") or os.getenv("BQ_PROJECT"))

def default_output_mode() -> str:
    return "bq" if is_cloud_env() else "json"

# ---------------
# Utils
# ---------------
def log(msg: str) -> None:
    print(msg, flush=True)

def safe_div(n: float, d: float) -> float:
    return float(n)/float(d) if d else 0.0

def parse_date(date_str: Optional[str]) -> dt.date:
    if date_str:
        return dt.datetime.strptime(date_str, "%Y-%m-%d").date()
    try:
        import pytz  # type: ignore
        tz = pytz.timezone(BASEBALL_TZ)
        return dt.datetime.now(tz).date()
    except Exception:
        return dt.date.today()

def is_intish(x: str) -> bool:
    try:
        int(x); return True
    except Exception:
        return False

def innings_from_outs(outs: int) -> float:
    whole = outs // 3
    rem = outs % 3
    return float(whole) + rem/10.0

def parse_ip_to_outs(ip_val: Any) -> int:
    if ip_val is None:
        return 0
    if isinstance(ip_val, int):
        return ip_val if ip_val >= 3 else ip_val*3
    try:
        f = float(ip_val)
        whole = int(f); dec = round((f - whole)*10)
        return whole*3 + min(max(dec,0),2)
    except Exception:
        return 0

# ---------------
# Derived metrics
# ---------------
def compute_batting_metrics(row: Dict[str, Any]) -> Dict[str, Any]:
    AB  = int(row.get("AB", 0) or 0)
    H   = int(row.get("H", 0) or 0)
    BB  = int(row.get("BB", 0) or 0)
    HBP = int(row.get("HBP", 0) or 0)
    SF  = int(row.get("SF", 0) or 0)
    HR  = int(row.get("HR", 0) or 0)
    D2  = int(row.get("doubles", row.get("2B", row.get("Doubles", 0))) or 0)
    D3  = int(row.get("triples", row.get("3B", row.get("Triples", 0))) or 0)
    R   = int(row.get("R", 0) or 0)
    RBI = int(row.get("RBI", 0) or 0)
    SB  = int(row.get("SB", 0) or 0)

    singles = max(H - D2 - D3 - HR, 0)
    TB = singles + 2*D2 + 3*D3 + 4*HR

    AVG = safe_div(H, AB)
    OBP = safe_div(H + BB + HBP, AB + BB + HBP + SF)
    SLG = safe_div(TB, AB)
    OPS = OBP + SLG

    BAT_SCORE = 5*HR + 3*(D2 + D3) + 2*(BB + HBP + SB) + singles + 1.5*RBI + 1.0*R

    row.update({
        "AVG": AVG, "OBP": OBP, "SLG": SLG, "OPS": OPS,
        "BAT_SCORE": float(BAT_SCORE),
    })
    return row

def compute_pitching_metrics(row: Dict[str, Any]) -> Dict[str, Any]:
    outs = int(row.get("outs") or parse_ip_to_outs(row.get("IP")))
    ip   = outs/3.0
    ER = float(row.get("ER", 0) or 0)
    H  = float(row.get("H", 0) or 0)
    BB = float(row.get("BB", 0) or 0)
    HR = float(row.get("HR", 0) or 0)
    SO = float(row.get("SO", row.get("K", 0)) or 0)

    ERA  = safe_div(ER*9.0, ip) if ip else 0.0
    WHIP = safe_div(H + BB, ip) if ip else 0.0
    PITCH_SCORE = 6*ip + 3*SO - 4*ER - 2*(H + BB) - 3*HR

    row.update({
        "outs": outs,
        "IP": ip,  # convenience
        "ERA": ERA, "WHIP": WHIP, "SO": SO,
        "PITCH_SCORE": float(PITCH_SCORE),
    })
    return row

# ---------------
# MLB API
# ---------------
class HTTPGetError(Exception):
    """Raised when http_get_json fails after retries."""


def http_get_json(
    url: str,
    params: Dict[str, Any] | None = None,
    timeout: int = 25,
    retries: int = 3,
    backoff: float = 0.5,
) -> Dict[str, Any]:
    for attempt in range(retries + 1):
        try:
            r = requests.get(url, params=params or {}, timeout=timeout)
            r.raise_for_status()
            return r.json()
        except requests.exceptions.RequestException as e:
            log(f"HTTP GET failed for {url}: {e}")
            if attempt == retries:
                raise HTTPGetError(f"Failed to fetch {url}") from e
            time.sleep(backoff * (2 ** attempt))

def find_final_games_for_team(date_iso: str, team: str) -> List[Dict[str, Any]]:
    sched = http_get_json(f"{API}/schedule", {"date": date_iso, "sportId": 1})
    games = []
    for d in sched.get("dates", []):
        for g in d.get("games", []):
            st = (g.get("status", {}) or {}).get("detailedState", "")
            if str(st).lower() != "final":
                continue
            home = g.get("teams", {}).get("home", {}).get("team", {}) or {}
            away = g.get("teams", {}).get("away", {}).get("team", {}) or {}
            if is_intish(team):
                if str(home.get("id")) == team or str(away.get("id")) == team:
                    games.append(g)
            else:
                if str(home.get("name", "")).lower() == team.lower() or str(away.get("name", "")).lower() == team.lower():
                    games.append(g)
    return games

def fetch_linescore(game_pk: int) -> Dict[str, Any]:
    return http_get_json(f"{API}/game/{game_pk}/linescore")

def fetch_boxscore(game_pk: int) -> Dict[str, Any]:
    return http_get_json(f"{API}/game/{game_pk}/boxscore")

# ---------------
# Flattening
# ---------------
def flatten_game_summary(g: Dict[str, Any], date_iso: str) -> Dict[str, Any]:
    game_pk = int(g["gamePk"])
    home = g["teams"]["home"]["team"]
    away = g["teams"]["away"]["team"]
    return {
        "game_id": game_pk,
        "game_date": date_iso,
        "home_team_id": int(home["id"]),
        "home_team_name": home["name"],
        "away_team_id": int(away["id"]),
        "away_team_name": away["name"],
        "home_score": int(g["teams"]["home"]["score"]),
        "away_score": int(g["teams"]["away"]["score"]),
        "status": "Final",
    }

def flatten_linescore(game_pk: int, ls: Dict[str, Any]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    innings = ls.get("innings", []) or []
    for i, inn in enumerate(innings, start=1):
        ah = inn.get("away", {}) or {}
        hh = inn.get("home", {}) or {}
        out.append({"game_id": game_pk, "is_home": False, "inning_num": i, "runs": int(ah.get("runs", 0) or 0)})
        out.append({"game_id": game_pk, "is_home": True,  "inning_num": i, "runs": int(hh.get("runs", 0) or 0)})
    return out

def _bat_row(game_pk:int, team_side:str, team_block:Dict[str,Any], player:Dict[str,Any]) -> Optional[Dict[str,Any]]:
    stats = (player.get("stats", {}) or {}).get("batting", {}) or {}
    if not stats:
        return None

    # Keep if they actually batted (AB>0) or contributed in any way:
    meaningful_keys = ["hits","doubles","triples","homeRuns","rbi","runs","baseOnBalls","hitByPitch","sacFlies","stolenBases"]
    if not (stats.get("atBats", 0) > 0 or any(stats.get(k, 0) > 0 for k in meaningful_keys)):
        return None

    row = {
        "role": "batter",
        "game_id": game_pk,
        "team_id": int(team_block.get("team",{}).get("id",0)),
        "team_name": team_block.get("team",{}).get("name"),
        "is_home": team_side == "home",
        "player_id": int(player.get("person",{}).get("id",0)),
        "name": player.get("person",{}).get("fullName"),
        "started": bool(player.get("gameStatus",{}).get("isCurrentBatter") or False),
        "batting_order": player.get("battingOrder"),
        # raw
        "AB": stats.get("atBats", 0),
        "H": stats.get("hits", 0),
        "BB": stats.get("baseOnBalls", 0),
        "HBP": stats.get("hitByPitch", 0),
        "SF": stats.get("sacFlies", 0),
        "HR": stats.get("homeRuns", 0),
        "doubles": stats.get("doubles", 0),
        "triples": stats.get("triples", 0),
        "R": stats.get("runs", 0),
        "RBI": stats.get("rbi", 0),
        "SB": stats.get("stolenBases", 0),
    }
    return compute_batting_metrics(row)

def _pit_row(game_pk:int, team_side:str, team_block:Dict[str,Any], player:Dict[str,Any]) -> Optional[Dict[str,Any]]:
    stats = (player.get("stats", {}) or {}).get("pitching", {}) or {}
    if not stats:
        return None

    ip_str = stats.get("inningsPitched")  # e.g., "6.1"
    row = {
        "role": "pitcher",
        "game_id": game_pk,
        "team_id": int(team_block.get("team",{}).get("id",0)),
        "team_name": team_block.get("team",{}).get("name"),
        "is_home": team_side == "home",
        "player_id": int(player.get("person",{}).get("id",0)),
        "name": player.get("person",{}).get("fullName"),
        "started": bool(player.get("gameStatus",{}).get("isCurrentPitcher") or False),
        # raw
        "IP": float(ip_str) if isinstance(ip_str, str) and ip_str.replace('.', '', 1).isdigit() else ip_str,
        "ER": stats.get("earnedRuns", 0),
        "H": stats.get("hits", 0),
        "BB": stats.get("baseOnBalls", 0),
        "HR": stats.get("homeRuns", 0),
        "SO": stats.get("strikeOuts", 0),
    }

    # Decide if we keep this pitcher row:
    outs = row.get("IP")
    outs = outs if isinstance(outs, (int,float)) else 0
    outs = int(outs * 3) if isinstance(outs, float) else outs
    has_outs = outs > 0
    meaningful_keys = ["earnedRuns","hits","baseOnBalls","homeRuns","strikeOuts"]
    has_stats = any(stats.get(k, 0) > 0 for k in meaningful_keys)

    if not (has_outs or has_stats):
        return None

    return compute_pitching_metrics(row)

def flatten_boxscore(game_pk: int, box: Dict[str, Any]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for team_side in ["away", "home"]:
        team_block = box.get("teams", {}).get(team_side, {}) or {}
        for pid, player in (team_block.get("players", {}) or {}).items():
            br = _bat_row(game_pk, team_side, team_block, player)
            if br: out.append(br)
            pr = _pit_row(game_pk, team_side, team_block, player)
            if pr: out.append(pr)
    return out

# ---------------
# BigQuery I/O
# ---------------
def bq_client_or_none(project: str):
    try:
        from google.cloud import bigquery  # type: ignore
        return bigquery.Client(project=project or None)
    except Exception:
        return None

def bq_ensure_dataset(client, dataset_id: str, location: str) -> None:
    from google.cloud import bigquery  # type: ignore
    ds_ref = bigquery.Dataset(f"{client.project}.{dataset_id}")
    try:
        client.get_dataset(ds_ref)
    except Exception:
        ds_ref.location = location
        client.create_dataset(ds_ref)

def bq_write_rows(client, table_fqn: str, rows: List[Dict[str, Any]]) -> None:
    if not rows:
        return
    from google.cloud import bigquery  # type: ignore
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
        autodetect=True,
    )
    job = client.load_table_from_json(rows, table_fqn, job_config=job_config)
    job.result()

# ---------------
# Local JSON writer
# ---------------
def ensure_dir(p: str | os.PathLike) -> None:
    os.makedirs(p, exist_ok=True)

def write_json_triplet(game_pk: int, summary: dict, lines: list[dict], players: list[dict], out_dir: str = JSON_DIR) -> tuple[str,str,str]:
    ensure_dir(out_dir)
    base = os.path.join(out_dir, str(game_pk))
    p_summary = f"{base}_summary.json"
    p_lines   = f"{base}_linescore.json"
    p_players = f"{base}_players.json"
    with open(p_summary, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)
    with open(p_lines, "w", encoding="utf-8") as f:
        json.dump(lines, f, indent=2)
    with open(p_players, "w", encoding="utf-8") as f:
        json.dump(players, f, indent=2)
    return p_summary, p_lines, p_players

# ---------------
# CLI
# ---------------
def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--team", required=True, help="Team id (e.g., 112) or exact name (e.g., 'Chicago Cubs')")
    ap.add_argument("--date", help="YYYY-MM-DD (defaults to BASEBALL_TZ today)")
    ap.add_argument("--output", choices=["bq","json"], default=default_output_mode())
    ap.add_argument("--bq_project", default=BQ_PROJECT)
    ap.add_argument("--bq_dataset", default=BQ_DATASET)
    args = ap.parse_args()

    date_iso = parse_date(args.date).isoformat()
    games = find_final_games_for_team(date_iso, args.team)
    if not games:
        log(f"No FINAL games for team={args.team} on {date_iso}.")
        return 0

    summaries: List[Dict[str, Any]] = []
    lines_all: List[Dict[str, Any]] = []
    players_all: List[Dict[str, Any]] = []

    for g in games:
        game_pk = int(g["gamePk"])
        summaries.append(flatten_game_summary(g, date_iso))
        ls = fetch_linescore(game_pk)
        lines = flatten_linescore(game_pk, ls)
        lines_all.extend(lines)
        box = fetch_boxscore(game_pk)
        players = flatten_boxscore(game_pk, box)
        players_all.extend(players)

        if args.output == "json":
            ps, pl, pp = write_json_triplet(game_pk, summaries[-1], lines, players, out_dir=JSON_DIR)
            log(f"Wrote JSON: {ps}, {pl}, {pp}")

    if args.output == "json":
        # Also print a small index for convenience
        print(json.dumps({"written": len(games)}, indent=2))
        return 0

    # BigQuery path
    client = bq_client_or_none(args.bq_project)
    if client is None:
        log("BigQuery client unavailable; set GOOGLE_CLOUD_PROJECT/BQ_PROJECT or use --output json.")
        return 2

    bq_ensure_dataset(client, args.bq_dataset, BQ_LOCATION)
    bq_write_rows(client, f"{args.bq_project}.{args.bq_dataset}.game_summaries", summaries)
    bq_write_rows(client, f"{args.bq_project}.{args.bq_dataset}.game_linescore", lines_all)
    bq_write_rows(client, f"{args.bq_project}.{args.bq_dataset}.game_boxscore_players", players_all)
    log(f"✅ Wrote {len(summaries)} summaries, {len(lines_all)} lines rows, {len(players_all)} player rows to BQ.")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
