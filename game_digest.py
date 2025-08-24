#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import sys
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

# =============================
# Config / Defaults
# =============================
BASEBALL_TZ = "America/New_York"

BQ_PROJECT = os.getenv("BQ_PROJECT")  # optional override
BQ_LOCATION = os.getenv("BQ_LOCATION", "EU")

BQ_SUMMARIES = os.getenv("BQ_SUMMARIES", "mlb.game_summaries")
BQ_LINESCORE = os.getenv("BQ_LINESCORE", "mlb.game_linescore")
BQ_PLAYERS   = os.getenv("BQ_PLAYERS",   "mlb.game_boxscore_players")
BQ_DIGESTS   = os.getenv("BQ_DIGESTS",   "mlb.game_digests")  # where we’ll write digest rows

DEFAULT_JSON_INDIR = os.getenv("JSON_INDIR", "out_debug")

# =============================
# Tiny helpers
# =============================
def log(msg: str) -> None:
    print(msg, flush=True)

def parse_date_or_today(s: Optional[str]) -> dt.date:
    if s:
        return dt.datetime.strptime(s, "%Y-%m-%d").date()
    # league day in US/Eastern
    try:
        from zoneinfo import ZoneInfo
        return dt.datetime.now(ZoneInfo(BASEBALL_TZ)).date()
    except Exception:
        return dt.date.today()

def outs_to_ip_str(outs: Optional[int]) -> str:
    if outs is None:
        return "0.0"
    return f"{outs // 3}.{outs % 3}"

def safe_int(x: Any) -> Optional[int]:
    try:
        return int(x) if x is not None else None
    except Exception:
        return None

def ensure_list(x):
    return x if isinstance(x, list) else [x]

# =============================
# Data structures
# =============================
@dataclass
class SummaryRow:
    game_id: int
    game_date: str
    home_team_id: int
    home_team: str
    away_team_id: int
    away_team: str
    home_runs: Optional[int]
    away_runs: Optional[int]
    status_abstract: Optional[str]
    status_detailed: Optional[str]

@dataclass
class LinescoreRow:
    game_id: int
    game_date: str
    home_team_id: int
    away_team_id: int
    totals: Tuple[Optional[int], Optional[int]]
    innings_home: List[Optional[int]]
    innings_away: List[Optional[int]]

@dataclass
class PlayerRow:
    game_id: int
    game_date: str
    team_side: str
    team_id: int
    team_name: str
    player_id: int
    player_name: str
    primary_pos: Optional[str]
    # batting
    ab: Optional[int]; r: Optional[int]; h: Optional[int]; doubles: Optional[int]
    triples: Optional[int]; hr: Optional[int]; rbi: Optional[int]; bb: Optional[int]
    so: Optional[int]; sb: Optional[int]; cs: Optional[int]; sf: Optional[int]; sh: Optional[int]
    # pitching
    outs: Optional[int]; ip_str: Optional[str]; er: Optional[int]; k: Optional[int]
    h_allowed: Optional[int]; bb_allowed: Optional[int]; hr_allowed: Optional[int]
    bf: Optional[int]; pitches: Optional[int]; strikes: Optional[int]
    hbp: Optional[int]; wp: Optional[int]

# =============================
# BQ IO
# =============================
def make_bq_client(project: Optional[str] = None):
    from google.cloud import bigquery
    return bigquery.Client(project=project) if project else bigquery.Client()

def bq_query(client, sql: str, params: Dict[str, Tuple[str, Any]]):
    """
    params: dict of name -> (type, value)
    """
    from google.cloud import bigquery
    qparams = []
    for name, (typ, val) in params.items():
        qparams.append(bigquery.ScalarQueryParameter(name, typ, val))
    cfg = bigquery.QueryJobConfig(query_parameters=qparams)
    return client.query(sql, job_config=cfg).result()

def bq_ensure_dataset(client, dataset_id: str, location: Optional[str] = None):
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
        client.create_table(bigquery.Table(full_table_id, schema=schema))
        log(f"Created table {full_table_id}")

def bq_write_digests(client, digests: List[Dict[str, Any]], table_spec: str, dataset_location: Optional[str] = None) -> None:
    """
    table_spec: dataset.table (e.g., mlb.game_digests)
    """
    if not digests:
        return
    from google.cloud import bigquery

    ds, tbl = table_spec.split(".", 1)
    full = f"{client.project}.{ds}.{tbl}"

    # ensure dataset+table
    bq_ensure_dataset(client, ds, dataset_location)

    DIGEST_SCHEMA = [
        bigquery.SchemaField("game_id", "INT64"),
        bigquery.SchemaField("game_date", "DATE"),
        bigquery.SchemaField("team_id", "INT64"),
        bigquery.SchemaField("team_name", "STRING"),
        bigquery.SchemaField("opponent_id", "INT64"),
        bigquery.SchemaField("opponent_name", "STRING"),
        bigquery.SchemaField("is_home", "BOOL"),
        bigquery.SchemaField("team_runs", "INT64"),
        bigquery.SchemaField("opponent_runs", "INT64"),
        bigquery.SchemaField("result", "STRING"),
        bigquery.SchemaField("title", "STRING"),
        bigquery.SchemaField("body_markdown", "STRING"),
        bigquery.SchemaField("created_at_utc", "TIMESTAMP"),
    ]
    bq_ensure_table(client, full, DIGEST_SCHEMA)

    # upsert by game_id+team_id (delete then insert)
    ids = [(d["game_id"], d["team_id"]) for d in digests]
    q = f"""
    DELETE FROM `{full}`
    WHERE (game_id, team_id) IN UNNEST(@pairs)
    """
    pairs = [{"f0_": int(g), "f1_": int(t)} for (g, t) in ids]
    cfg = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter("pairs", "STRUCT<game_id INT64, team_id INT64>", pairs)
        ]
    )
    client.query(q, job_config=cfg).result()

    client.load_table_from_json(
        digests,
        destination=full,
        job_config=bigquery.LoadJobConfig(write_disposition="WRITE_APPEND"),
    ).result()
    log(f"✅ Wrote {len(digests)} digest row(s) to {full}")

# =============================
# Load from BQ
# =============================
def load_from_bq(team_id: int, game_date: dt.date) -> List[Tuple[SummaryRow, LinescoreRow, List[PlayerRow]]]:
    client = make_bq_client(BQ_PROJECT)

    # 1) final game summaries for team/date
    q1 = f"""
    SELECT *
    FROM `{client.project}.{BQ_SUMMARIES}`
    WHERE game_date = @d
      AND @team IN (home_team_id, away_team_id)
      AND LOWER(status_abstract) = 'final'
    ORDER BY game_id
    """
    rows = list(bq_query(client, q1, {"d": ("DATE", game_date), "team": ("INT64", team_id)}))
    if not rows:
        log("No final games found in summaries.")
        return []

    results: List[Tuple[SummaryRow, LinescoreRow, List[PlayerRow]]] = []

    for r in rows:
        s = SummaryRow(
            game_id=r["game_id"],
            game_date=str(r["game_date"]),
            home_team_id=r["home_team_id"],
            home_team=r["home_team"],
            away_team_id=r["away_team_id"],
            away_team=r["away_team"],
            home_runs=r["home_runs"],
            away_runs=r["away_runs"],
            status_abstract=r["status_abstract"],
            status_detailed=r["status_detailed"],
        )
        # 2) linescore
        q2 = f"""
        SELECT *
        FROM `{client.project}.{BQ_LINESCORE}`
        WHERE game_id = @g
        """
        lr = list(bq_query(client, q2, {"g": ("INT64", s.game_id)}))
        if not lr:
            log(f"Warning: no linescore for game_id={s.game_id}")
            continue
        lrow = lr[0]
        lines = LinescoreRow(
            game_id=lrow["game_id"],
            game_date=str(lrow["game_date"]),
            home_team_id=lrow["home_team_id"],
            away_team_id=lrow["away_team_id"],
            totals=(lrow["total_home"], lrow["total_away"]),
            innings_home=[lrow.get(f"home_inn_{i}") for i in range(1, 16)],
            innings_away=[lrow.get(f"away_inn_{i}") for i in range(1, 16)],
        )
        # 3) players (team only)
        q3 = f"""
        SELECT *
        FROM `{client.project}.{BQ_PLAYERS}`
        WHERE game_id = @g AND team_id = @t
        """
        pr = list(bq_query(client, q3, {"g": ("INT64", s.game_id), "t": ("INT64", team_id)}))
        players: List[PlayerRow] = []
        for p in pr:
            players.append(PlayerRow(
                game_id=p["game_id"], game_date=str(p["game_date"]),
                team_side=p["team_side"], team_id=p["team_id"], team_name=p["team_name"],
                player_id=p["player_id"], player_name=p["player_name"], primary_pos=p["primary_pos"],
                ab=p["ab"], r=p["r"], h=p["h"], doubles=p["doubles"], triples=p["triples"], hr=p["hr"],
                rbi=p["rbi"], bb=p["bb"], so=p["so"], sb=p["sb"], cs=p["cs"], sf=p["sf"], sh=p["sh"],
                outs=p["outs"], ip_str=p["ip_str"], er=p["er"], k=p["k"], h_allowed=p["h_allowed"],
                bb_allowed=p["bb_allowed"], hr_allowed=p["hr_allowed"], bf=p["bf"],
                pitches=p["pitches"], strikes=p["strikes"], hbp=p["hbp"], wp=p["wp"]
            ))
        results.append((s, lines, players))

    return results

# =============================
# Load from local JSON
# =============================
def _load_json(path: str) -> Any:
    with open(path, "r") as f:
        return json.load(f)

def load_from_json(team_id: int, game_date: dt.date, indir: str) -> List[Tuple[SummaryRow, LinescoreRow, List[PlayerRow]]]:
    """
    Expects files produced by mlb_ingest.py:
      {gid}_summary.json
      {gid}_linescore.json
      {gid}_players.json  (dict with key "players": [...])
    """
    import glob
    results: List[Tuple[SummaryRow, LinescoreRow, List[PlayerRow]]] = []

    for summary_path in glob.glob(os.path.join(indir, "*_summary.json")):
        try:
            sraw = _load_json(summary_path)
        except Exception:
            continue
        # filter by team/date and final
        if str(sraw.get("game_date")) != game_date.isoformat():
            continue
        if team_id not in (safe_int(sraw.get("home_team_id")), safe_int(sraw.get("away_team_id"))):
            continue
        if (sraw.get("status_abstract") or "").lower() != "final" and not str(sraw.get("status_detailed", "")).startswith(("Final","Game Over","Completed")):
            continue

        gid = safe_int(sraw.get("game_id"))
        if gid is None:
            continue

        # build SummaryRow
        s = SummaryRow(
            game_id=gid,
            game_date=str(sraw.get("game_date")),
            home_team_id=safe_int(sraw.get("home_team_id")) or 0,
            home_team=sraw.get("home_team"),
            away_team_id=safe_int(sraw.get("away_team_id")) or 0,
            away_team=sraw.get("away_team"),
            home_runs=safe_int(sraw.get("home_runs")),
            away_runs=safe_int(sraw.get("away_runs")),
            status_abstract=sraw.get("status_abstract"),
            status_detailed=sraw.get("status_detailed"),
        )
        # linescore+players paths
        base = os.path.basename(summary_path).replace("_summary.json", "")
        lraw = _load_json(os.path.join(indir, f"{base}_linescore.json"))
        praw = _load_json(os.path.join(indir, f"{base}_players.json"))

        lines = LinescoreRow(
            game_id=safe_int(lraw.get("game_id")) or 0,
            game_date=str(lraw.get("game_date")),
            home_team_id=safe_int(lraw.get("home_team_id")) or 0,
            away_team_id=safe_int(lraw.get("away_team_id")) or 0,
            totals=(safe_int(lraw.get("total_home")), safe_int(lraw.get("total_away"))),
            innings_home=[lraw.get(f"home_inn_{i}") for i in range(1, 16)],
            innings_away=[lraw.get(f"away_inn_{i}") for i in range(1, 16)],
        )

        players: List[PlayerRow] = []
        for p in ensure_list((praw or {}).get("players", [])):
            if safe_int(p.get("team_id")) != team_id:
                continue
            players.append(PlayerRow(
                game_id=safe_int(p.get("game_id")) or 0,
                game_date=str(p.get("game_date")),
                team_side=p.get("team_side"),
                team_id=safe_int(p.get("team_id")) or 0,
                team_name=p.get("team_name"),
                player_id=safe_int(p.get("player_id")) or 0,
                player_name=p.get("player_name"),
                primary_pos=p.get("primary_pos"),
                ab=safe_int(p.get("ab")), r=safe_int(p.get("r")), h=safe_int(p.get("h")),
                doubles=safe_int(p.get("doubles")), triples=safe_int(p.get("triples")), hr=safe_int(p.get("hr")),
                rbi=safe_int(p.get("rbi")), bb=safe_int(p.get("bb")), so=safe_int(p.get("so")),
                sb=safe_int(p.get("sb")), cs=safe_int(p.get("cs")), sf=safe_int(p.get("sf")), sh=safe_int(p.get("sh")),
                outs=safe_int(p.get("outs")), ip_str=p.get("ip_str"), er=safe_int(p.get("er")), k=safe_int(p.get("k")),
                h_allowed=safe_int(p.get("h_allowed")), bb_allowed=safe_int(p.get("bb_allowed")), hr_allowed=safe_int(p.get("hr_allowed")),
                bf=safe_int(p.get("bf")), pitches=safe_int(p.get("pitches")), strikes=safe_int(p.get("strikes")),
                hbp=safe_int(p.get("hbp")), wp=safe_int(p.get("wp")),
            ))

        results.append((s, lines, players))

    results.sort(key=lambda tup: tup[0].game_id)
    return results

# =============================
# Digest computation
# =============================
def format_linescore_str(ls: LinescoreRow, team_is_home: bool) -> str:
    # 9 innings display (extras collapsed as '...')
    home = ls.innings_home
    away = ls.innings_away
    nine_home = [x for x in home[:9]]
    nine_away = [x for x in away[:9]]

    ih = " ".join("-" if v is None else str(v) for v in nine_home)
    ia = " ".join("-" if v is None else str(v) for v in nine_away)

    extra = ""
    if any(v not in (None, 0) for v in home[9:] + away[9:]):
        extra = " (+)"

    return f"Away: {ia}\nHome: {ih}{extra}"

def make_digest_for_game(team_id: int, s: SummaryRow, l: LinescoreRow, players: List[PlayerRow]) -> Dict[str, Any]:
    # Identify home/away for our team
    is_home = (team_id == s.home_team_id)
    team_name = s.home_team if is_home else s.away_team
    opp_name  = s.away_team if is_home else s.home_team
    team_runs = s.home_runs if is_home else s.away_runs
    opp_runs  = s.away_runs if is_home else s.home_runs

    result = "W" if (team_runs or 0) > (opp_runs or 0) else ("L" if (team_runs or 0) < (opp_runs or 0) else "T")

    # Batting totals & leaders
    team_hitters = players  # already filtered to team
    H  = sum((p.h or 0) for p in team_hitters)
    R  = sum((p.r or 0) for p in team_hitters)
    HR = sum((p.hr or 0) for p in team_hitters)
    RBI= sum((p.rbi or 0) for p in team_hitters)
    BB = sum((p.bb or 0) for p in team_hitters)
    SO = sum((p.so or 0) for p in team_hitters)
    SB = sum((p.sb or 0) for p in team_hitters)

    # Top batters: sort by hits desc, HR desc, RBI desc
    hitters_sorted = sorted(
        team_hitters,
        key=lambda p: (p.h or 0, p.hr or 0, p.rbi or 0),
        reverse=True
    )
    top_batters = [p for p in hitters_sorted if (p.ab or 0) > 0][:3]

    # Pitching totals & highlight
    pitchers = [p for p in team_hitters if (p.pitches or 0) > 0 or (p.outs or 0) > 0]
    tot_outs = sum((p.outs or 0) for p in pitchers)
    tot_er   = sum((p.er or 0)   for p in pitchers)
    tot_k    = sum((p.k or 0)    for p in pitchers)
    tot_bb   = sum((p.bb_allowed or 0) for p in pitchers)
    tot_h    = sum((p.h_allowed or 0)  for p in pitchers)

    # pick a "star" pitcher: >= 9 outs, then by K desc, ER asc
    star_pitcher = None
    if pitchers:
        pitchers_candidate = [p for p in pitchers if (p.outs or 0) >= 9] or pitchers
        star_pitcher = sorted(pitchers_candidate, key=lambda p: ((p.k or 0), -(p.er or 0)), reverse=True)[0]

    # HR list
    hr_guys = [p for p in team_hitters if (p.hr or 0) > 0]
    hr_list = ", ".join(f"{p.player_name} ({p.hr})" for p in hr_guys)

    # Notables
    notables: List[str] = []
    for p in team_hitters:
        if (p.h or 0) >= 3 or (p.hr or 0) >= 2 or (p.rbi or 0) >= 4:
            line = f"{p.player_name}: {p.h or 0}H, {p.hr or 0}HR, {p.rbi or 0}RBI"
            notables.append(line)
    for p in pitchers:
        if ((p.k or 0) >= 7) or (((p.outs or 0) >= 18) and ((p.er or 0) <= 2)):
            line = f"{p.player_name}: {outs_to_ip_str(p.outs)} IP, {p.k or 0} K, {p.er or 0} ER"
            notables.append(line)

    # Title & body
    score_str = f"{team_runs}-{opp_runs}"
    title = f"{team_name} {result} {score_str} vs {opp_name}"

    ls_text = format_linescore_str(l, is_home)

    bat_lines = []
    for p in top_batters:
        bat_lines.append(
            f"- {p.player_name}: {p.h or 0} H, {p.hr or 0} HR, {p.rbi or 0} RBI, "
            f"{p.bb or 0} BB, {p.so or 0} K (AB {p.ab or 0})"
        )

    pit_lines = []
    if star_pitcher:
        pit_lines.append(f"- {star_pitcher.player_name}: {outs_to_ip_str(star_pitcher.outs)} IP, "
                         f"{star_pitcher.k or 0} K, {star_pitcher.er or 0} ER, "
                         f"{star_pitcher.h_allowed or 0} H, {star_pitcher.bb_allowed or 0} BB")

    body = []
    body.append(f"## Final: {team_name} {score_str} {result} {opp_name}")
    body.append("")
    body.append("### Linescore")
    body.append("```")
    body.append(ls_text)
    body.append("```")
    body.append("")
    body.append("### Team Totals (batting)")
    body.append(f"- R {R} • H {H} • HR {HR} • RBI {RBI} • BB {BB} • SO {SO} • SB {SB}")
    if hr_list:
        body.append(f"- Homers: {hr_list}")
    body.append("")
    if bat_lines:
        body.append("### Top Batters")
        body.extend(bat_lines)
        body.append("")
    body.append("### Pitching")
    body.append(f"- Team: {outs_to_ip_str(tot_outs)} IP, {tot_k} K, {tot_er} ER, {tot_h} H, {tot_bb} BB")
    if pit_lines:
        body.extend(pit_lines)
    if notables:
        body.append("")
        body.append("### Notables")
        for line in notables:
            body.append(f"- {line}")

    digest = {
        "game_id": s.game_id,
        "game_date": s.game_date,
        "team_id": team_id,
        "team_name": team_name,
        "opponent_id": s.away_team_id if is_home else s.home_team_id,
        "opponent_name": opp_name,
        "is_home": is_home,
        "team_runs": team_runs,
        "opponent_runs": opp_runs,
        "result": result,
        "title": title,
        "body_markdown": "\n".join(body),
        "created_at_utc": dt.datetime.now(dt.UTC).isoformat(timespec="seconds"),
    }
    return digest

# =============================
# CLI
# =============================
def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="Build a game digest from BigQuery (default) or local JSON.")
    ap.add_argument("--team", type=int, required=True, help="MLB team ID (e.g., 112).")
    ap.add_argument("--date", type=str, help="YYYY-MM-DD; defaults to 'today' in America/New_York.")
    # 'output' here selects the SOURCE of data (bq vs json)
    ap.add_argument("--output", choices=["bq", "json"], default="bq",
                    help="Where to READ data from: 'bq' (default) or 'json' (local debug files).")
    ap.add_argument("--json_indir", default=DEFAULT_JSON_INDIR, help="Input dir for local JSON files.")
    # BQ options (when reading from BQ; and for writing digests)
    ap.add_argument("--bq_project", help="Optional GCP project override for BigQuery client.")
    ap.add_argument("--bq_digests", default=BQ_DIGESTS, help="dataset.table for digest output (when writing).")
    ap.add_argument("--no_write", action="store_true", help="Do not write digest rows back to BigQuery.")
    args = ap.parse_args(argv)

    team_id = args.team
    game_date = parse_date_or_today(args.date)

    log(f"🧩 Building digest for team={team_id} date={game_date} (source={args.output})")

    triples: List[Tuple[SummaryRow, LinescoreRow, List[PlayerRow]]] = []
    if args.output == "bq":
        triples = load_from_bq(team_id, game_date)
    else:
        triples = load_from_json(team_id, game_date, args.json_indir)

    if not triples:
        log("No final games found. Nothing to do.")
        return 0

    digests: List[Dict[str, Any]] = []
    for (s, l, players) in triples:
        digests.append(make_digest_for_game(team_id, s, l, players))

    # Print to stdout (nice for logs)
    for d in digests:
        print("\n" + "="*80)
        print(d["title"])
        print("-"*80)
        print(d["body_markdown"])
        print("="*80 + "\n")

    # Write back to BQ if we sourced from BQ and not disabled
    if args.output == "bq" and not args.no_write:
        try:
            client = make_bq_client(args.bq_project or BQ_PROJECT)
            # Ensure dataset exists (table created inside writer)
            ds, _tbl = args.bq_digests.split(".", 1)
            bq_ensure_dataset(client, ds, BQ_LOCATION)
            bq_write_digests(client, digests, args.bq_digests, BQ_LOCATION)
        except Exception as e:
            log(f"❌ Failed to write digest to BigQuery: {e}")
            return 2

    return 0


if __name__ == "__main__":
    sys.exit(main())
