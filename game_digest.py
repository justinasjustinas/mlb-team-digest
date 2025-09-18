#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import re

from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import playoff_odds

# =============================
# Config / Defaults
# =============================
BASEBALL_TZ   = os.getenv("BASEBALL_TZ", "America/New_York")
BQ_PROJECT    = os.getenv("BQ_PROJECT", os.getenv("GOOGLE_CLOUD_PROJECT", ""))
BQ_DATASET    = os.getenv("BQ_DATASET", "mlb")
JSON_DIR      = os.getenv("DIGEST_JSON_DIR", "data")

# =============================
# Env Detect
# =============================
def is_cloud_env() -> bool:
    return bool(os.getenv("K_SERVICE") or os.getenv("CLOUD_RUN_JOB") or os.getenv("GAE_ENV") or os.getenv("GOOGLE_CLOUD_PROJECT") or os.getenv("BQ_PROJECT"))

def default_output_mode() -> str:
    return "bq" if is_cloud_env() else "json"

# =============================
# Helpers
# =============================
def fmt_rate(x: float, places: int = 3, leading_zero: bool = False) -> str:
    s = f"{float(x):.{places}f}"
    if not leading_zero and s.startswith("0"):
        s = s[1:]
    return s

def parse_date(date_str: Optional[str]) -> str:
    if date_str:
        return date_str
    try:
        import pytz  # type: ignore
        tz = pytz.timezone(BASEBALL_TZ)
        return dt.datetime.now(tz).date().isoformat()
    except Exception:
        return dt.date.today().isoformat()

def is_our_game_row(row: Dict[str, Any], team: str) -> bool:
    return (str(row.get("home_team_id")) == str(team)) or (str(row.get("away_team_id")) == str(team)) or \
           (str(row.get("home_team_name","")).lower() == str(team).lower()) or \
           (str(row.get("away_team_name","")).lower() == str(team).lower()) or \
           (str(row.get("home_team","")).lower() == str(team).lower()) or \
           (str(row.get("away_team","")).lower() == str(team).lower())

def is_our_team_row(row: Dict[str, Any], team: str) -> bool:
    return (str(row.get("team_id")) == str(team)) or (str(row.get("team_name","")).lower() == str(team).lower())

# =============================
# Local JSON
# =============================
def load_json(path: str):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def find_summary_for(team: str, date_iso: str) -> Tuple[str, Dict[str, Any]]:
    root = Path(JSON_DIR)
    if not root.exists():
        raise FileNotFoundError(f"JSON dir not found: {JSON_DIR}")
    candidates = sorted(root.glob("*_summary.json"), key=lambda p: p.stat().st_mtime, reverse=True)
    if not candidates:
        raise FileNotFoundError(f"No *_summary.json found under {JSON_DIR}")
    for f in candidates:
        obj = load_json(str(f))
        summary = obj[0] if isinstance(obj, list) and obj else obj
        if not isinstance(summary, dict):
            continue
        if str(summary.get("game_date")) != date_iso:
            continue
        status = str(summary.get("status") or summary.get("status_detailed") or "").lower()
        if "final" not in status:
            continue
        if is_our_game_row(summary, team):
            game_pk = f.name.split("_", 1)[0]
            return game_pk, summary
    raise RuntimeError(f"No FINAL summary for team={team} date={date_iso} under {JSON_DIR}")

def build_from_json(team: str, date_iso: str) -> Tuple[str, Optional[int], Optional[int]]:
    game_pk, summary = find_summary_for(team, date_iso)
    p_lines   = Path(JSON_DIR) / f"{game_pk}_linescore.json"
    p_players = Path(JSON_DIR) / f"{game_pk}_players.json"
    if not p_lines.exists() or not p_players.exists():
        raise FileNotFoundError(f"Missing sibling JSON files for game {game_pk}: {p_lines}, {p_players}")

    lines = load_json(str(p_lines))
    players = load_json(str(p_players))

    game_id = int(summary.get("game_id", game_pk))
    away_line = [str(r["runs"]) for r in lines if (int(r.get("game_id", game_id))==game_id and not r.get("is_home"))]
    home_line = [str(r["runs"]) for r in lines if (int(r.get("game_id", game_id))==game_id and r.get("is_home"))]

    team_is_home = (str(summary.get("home_team_id")) == str(team)) or (str(summary.get("home_team_name","")).lower() == str(team).lower()) or (str(summary.get("home_team","")).lower() == str(team).lower())
    team_name = (
    (summary.get("home_team_name") or summary.get("home_team"))
    if team_is_home else
    (summary.get("away_team_name") or summary.get("away_team"))
)
    opp_name = (
    (summary.get("away_team_name") or summary.get("away_team"))
    if team_is_home else
    (summary.get("home_team_name") or summary.get("home_team"))
)
    team_id = int(summary["home_team_id"] if team_is_home else summary["away_team_id"])
    our_score = int(summary["home_score"] if team_is_home else summary["away_score"])
    opp_score = int(summary["away_score"] if team_is_home else summary["home_score"])

    our_players = [r for r in players if int(r.get("game_id", game_id))==game_id and is_our_team_row(r, team)]
    batters  = [r for r in our_players if (r.get("role") == "batter") or (r.get("AB") is not None)]
    pitchers = [r for r in our_players if (r.get("role") == "pitcher") or (r.get("outs") is not None) or (r.get("IP") is not None)]

    top_b = pick_top_batter(batters)
    sp = pick_starting_pitcher(pitchers)
    rp = pick_top_relief_pitcher(pitchers, sp)

    out: List[str] = []
    out.append(f"## Final: {team_name} {our_score}-{opp_score} {opp_name}")
    out.append("")
    out.append("### Linescore")
    out.append(f"Away: {' '.join(away_line)}")
    out.append(f"Home: {' '.join(home_line)}")
    out.append("")
    if top_b:
        name = top_b.get("name") or top_b.get("player_name") or top_b.get("fullName") or "Top Batter"
        out.append(f"### Top Batter for {team_name}")
        out.append(f"- {name}: {fmt_rate(float(top_b['BAT_SCORE']),2,True)} BAT_SCORE, "
                   f"{int(top_b.get('HR',0))} HR, {int(top_b.get('RBI',0))} RBI, "
                   f"{fmt_rate(float(top_b['AVG']))} AVG, {fmt_rate(float(top_b['OBP']))} OBP, "
                   f"{fmt_rate(float(top_b['SLG']))} SLG, {fmt_rate(float(top_b['OPS']))} OPS")
        out.append("")
    if sp or rp:
        out.append(f"### Pitching for {team_name}")
        if sp:
            name = sp.get("name") or sp.get("player_name") or sp.get("fullName") or "Starting Pitcher"
            out.append(
                f"- SP {name}: {fmt_rate(float(sp['PITCH_SCORE']),2,True)} PITCH_SCORE, "
                f"{format_ip_value(sp)} IP, {float(sp.get('ERA',0.0)):.2f} ERA, {float(sp.get('WHIP',0.0)):.2f} WHIP"
            )
        if rp:
            name = rp.get("name") or rp.get("player_name") or rp.get("fullName") or "Top Reliever"
            out.append(
                f"- RP {name}: {fmt_rate(float(rp['PITCH_SCORE']),2,True)} PITCH_SCORE, "
                f"{format_ip_value(rp)} IP, {float(rp.get('ERA',0.0)):.2f} ERA, {float(rp.get('WHIP',0.0)):.2f} WHIP"
            )
        out.append("")
    prob = playoff_odds.estimate_playoff_odds(team_id)
    if prob is not None:
        out.append(f"### {team_name} postseason odds: {prob}%")
        out.append("")
    return "\n".join(out), game_id, team_id

# =============================
# BigQuery
# =============================
def bq_client_or_none(project: str):
    try:
        from google.cloud import bigquery  # type: ignore
        return bigquery.Client(project=project or None)
    except Exception:
        return None

def bq_query(client, sql: str, params=None):
    # params: list of {"name": str, "type": "STRING|INT64|DATE", "value": any}
    from google.cloud import bigquery  # type: ignore
    job_config = bigquery.QueryJobConfig()

    if params:
        qps = []
        for p in params:
            name = p["name"]
            typ  = p["type"].upper()
            val  = p["value"]
            if typ == "INT64":
                qps.append(bigquery.ScalarQueryParameter(name, "INT64", int(val)))
            elif typ == "DATE":
                # Accept "YYYY-MM-DD" string
                qps.append(bigquery.ScalarQueryParameter(name, "DATE", str(val)))
            else:
                qps.append(bigquery.ScalarQueryParameter(name, "STRING", str(val)))
        job_config.query_parameters = qps

    rows = client.query(sql, job_config=job_config).result()
    return [dict(r) for r in rows]

def bq_write_digest(client, project: str, dataset: str, row: Dict[str, Any]) -> None:
    from google.cloud import bigquery  # type: ignore
    table = f"{project}.{dataset}.game_digests"
    job = client.load_table_from_json([row], table)
    job.result()


_BASEBALL_IP_RE = re.compile(r"^\s*(\d+)(?:\.(\d))?\s*$")


def _coerce_outs(value: Any) -> Optional[int]:
    try:
        outs = int(round(float(value)))
    except Exception:
        return None
    return outs if outs >= 0 else None


def _parse_ip_to_outs(ip_val: Any) -> Optional[int]:
    if ip_val is None:
        return None

    if isinstance(ip_val, (int, float)):
        try:
            outs = int(round(float(ip_val) * 3.0))
        except Exception:
            return None
        return outs if outs >= 0 else None

    if isinstance(ip_val, str):
        ip_str = ip_val.strip()
        if not ip_str:
            return None

        m = _BASEBALL_IP_RE.match(ip_str)
        if m:
            innings = int(m.group(1))
            remainder = int(m.group(2) or 0)
            remainder = max(0, min(remainder, 2))
            return innings * 3 + remainder

        try:
            outs = int(round(float(ip_str) * 3.0))
        except Exception:
            return None
        return outs if outs >= 0 else None

    return None


def format_ip_value(row: Dict[str, Any]) -> str:
    """Format innings pitched using baseball-style tenths (e.g., 6.2 == 6⅔)."""

    outs = _coerce_outs(row.get("outs"))
    if outs is None:
        outs = _parse_ip_to_outs(row.get("IP"))

    if outs is None:
        raw = row.get("IP")
        return str(raw) if raw is not None else "0.0"

    innings = outs // 3
    remainder = outs % 3
    return f"{innings}.{remainder}"


def pick_top_batter(rows: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    rows = [r for r in rows if r.get("BAT_SCORE") is not None]
    if not rows: return None
    rows.sort(key=lambda r: float(r["BAT_SCORE"]), reverse=True)
    return rows[0]

def pick_starting_pitcher(rows: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    pitchers = [r for r in rows if r.get("PITCH_SCORE") is not None]
    if not pitchers:
        return None

    def start_key(row: Dict[str, Any]) -> Tuple[int, float]:
        outs = int(row.get("outs") or 0)
        score = float(row.get("PITCH_SCORE") or 0.0)
        return outs, score

    starters = [r for r in pitchers if r.get("started")]
    if starters:
        return sorted(starters, key=start_key, reverse=True)[0]

    return sorted(pitchers, key=start_key, reverse=True)[0]


def pick_top_relief_pitcher(rows: List[Dict[str, Any]], starter: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    pitchers = [r for r in rows if r.get("PITCH_SCORE") is not None]
    if not pitchers:
        return None

    starter_id = starter.get("player_id") if starter else None
    reliefs = [r for r in pitchers if not r.get("started")]
    if starter_id is not None:
        reliefs = [r for r in reliefs if r.get("player_id") != starter_id]
    if starter is not None:
        reliefs = [r for r in reliefs if r is not starter]

    if not reliefs:
        reliefs = [r for r in pitchers if r.get("player_id") != starter_id] if starter_id is not None else pitchers
        if starter is not None:
            reliefs = [r for r in reliefs if r is not starter]

    if not reliefs:
        return None

    return sorted(reliefs, key=lambda r: float(r.get("PITCH_SCORE") or 0.0), reverse=True)[0]

def build_from_bq(client, project: str, dataset: str, team: str, date_iso: str) -> Tuple[str, Optional[int], Optional[int]]:
    games = bq_query(
    client,
    f"""
    SELECT *
    FROM `{project}.{dataset}.game_summaries`
    WHERE game_date = @date AND LOWER(status) = 'final'
    """,
    params=[{"name": "date", "type": "DATE", "value": date_iso}],
)
    g = games[0]; game_id = g["game_id"]

    lines = bq_query(
    client,
    f"""
    SELECT is_home, inning_num, runs
    FROM `{project}.{dataset}.game_linescore`
    WHERE game_id = @game_id
    ORDER BY is_home, inning_num
    """,
    params=[{"name": "game_id", "type": "INT64", "value": game_id}],
)
    away_line = [str(r["runs"]) for r in lines if not r["is_home"]]
    home_line = [str(r["runs"]) for r in lines if r["is_home"]]

    box = bq_query(
    client,
    f"""
    SELECT *
    FROM `{project}.{dataset}.game_boxscore_players`
    WHERE game_id = @game_id
      AND (CAST(team_id AS STRING) = @team OR LOWER(team_name) = LOWER(@team))
    """,
    params=[
        {"name": "game_id", "type": "INT64", "value": game_id},
        {"name": "team", "type": "STRING", "value": str(team)},
    ],
)
    batters  = [r for r in box if (r.get("role") == "batter") or (r.get("AB") is not None)]
    pitchers = [r for r in box if (r.get("role") == "pitcher") or (r.get("outs") is not None) or (r.get("IP") is not None)]

    top_b = pick_top_batter(batters)
    sp = pick_starting_pitcher(pitchers)
    rp = pick_top_relief_pitcher(pitchers, sp)

    our_is_home = (str(g.get("home_team_id")) == str(team)) or ((g.get("home_team_name","")).lower() == str(team).lower())
    team_name = g["home_team_name"] if our_is_home else g["away_team_name"]
    opp_name  = g["away_team_name"] if our_is_home else g["home_team_name"]
    team_id = int(g["home_team_id"] if our_is_home else g["away_team_id"])
    our_score = int(g["home_score"] if our_is_home else g["away_score"])
    opp_score = int(g["away_score"] if our_is_home else g["home_score"])

    out: List[str] = []
    out.append(f"## Final: {team_name} {our_score}-{opp_score} {opp_name}")
    out.append("")
    out.append("### Linescore")
    out.append(f"Away: {' '.join(away_line)}")
    out.append(f"Home: {' '.join(home_line)}")
    out.append("")
    if top_b:
        name = top_b.get("name") or top_b.get("player_name") or top_b.get("fullName") or "Top Batter"
        out.append(f"### Top Batter for {team_name}")
        out.append(f"- {name}: {fmt_rate(float(top_b['BAT_SCORE']),2,True)} BAT_SCORE, "
                   f"{int(top_b.get('HR',0))} HR, {int(top_b.get('RBI',0))} RBI, "
                   f"{fmt_rate(float(top_b['AVG']))} AVG, {fmt_rate(float(top_b['OBP']))} OBP, "
                   f"{fmt_rate(float(top_b['SLG']))} SLG, {fmt_rate(float(top_b['OPS']))} OPS")
        out.append("")
    if sp or rp:
        out.append(f"### Pitching for {team_name}")
        if sp:
            name = sp.get("name") or sp.get("player_name") or sp.get("fullName") or "Starting Pitcher"
            out.append(
                f"- SP {name}: {fmt_rate(float(sp['PITCH_SCORE']),2,True)} PITCH_SCORE, "
                f"{format_ip_value(sp)} IP, {float(sp.get('ERA',0.0)):.2f} ERA, {float(sp.get('WHIP',0.0)):.2f} WHIP"
            )
        if rp:
            name = rp.get("name") or rp.get("player_name") or rp.get("fullName") or "Top Reliever"
            out.append(
                f"- RP {name}: {fmt_rate(float(rp['PITCH_SCORE']),2,True)} PITCH_SCORE, "
                f"{format_ip_value(rp)} IP, {float(rp.get('ERA',0.0)):.2f} ERA, {float(rp.get('WHIP',0.0)):.2f} WHIP"
            )
        out.append("")
    prob = playoff_odds.estimate_playoff_odds(team_id)
    if prob is not None:
        out.append(f"### {team_name} is {prob}% likely to make it to Playoffs this year")
        out.append("")
    return "\n".join(out), int(game_id), team_id

# =============================
# CLI
# =============================
def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--team", required=True)
    p.add_argument("--date")
    p.add_argument("--output", choices=["bq","json"], default=default_output_mode())
    p.add_argument("--bq_project", default=BQ_PROJECT)
    p.add_argument("--bq_dataset", default=BQ_DATASET)
    args = p.parse_args()

    date_iso = parse_date(args.date)

    if args.output == "json":
        body, game_id, team_id = build_from_json(args.team, date_iso)
        print(body)
        return 0

    client = bq_client_or_none(args.bq_project)
    if client is None:
        raise SystemExit("BigQuery client unavailable. Set GOOGLE_CLOUD_PROJECT/BQ_PROJECT or use --output json.")
    body, game_id, team_id = build_from_bq(client, args.bq_project, args.bq_dataset, args.team, date_iso)
    print(body)

    # write to digests
    row = {
        "game_id": game_id,
        "team_id": team_id,
        "team_name": str(args.team),
        "game_date": date_iso,
        "digest_md": body,
        "created_at": dt.datetime.utcnow().isoformat(timespec="seconds") + "Z",
    }
    bq_write_digest(client, args.bq_project, args.bq_dataset, row)

    payload = {
        "severity": "INFO",
        "component": "game_digest",
        "event": "digest_written",
        "team_id": row["team_id"],
        "team_name": row["team_name"],
        "game_id": row["game_id"],
        "game_date": row["game_date"],
        "digest_md": row["digest_md"],
        "created_at": row["created_at"],
    }
    print(json.dumps(payload, ensure_ascii=False), flush=True)
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
