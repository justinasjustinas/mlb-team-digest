#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Dict, List, Tuple

def load_json(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)

def find_saved_jsons(outdir: Path, date: str) -> List[Path]:
    """Return all saved game JSONs under data/raw/<date>/ (sorted)."""
    folder = outdir / "raw" / date
    files = sorted(folder.glob("game_*.json"))
    if not files:
        raise SystemExit(f"No game_*.json found under {folder}")
    return files

def which_side(feed: Dict[str, Any], team_id: int) -> str:
    home_id = feed.get("gameData", {}).get("teams", {}).get("home", {}).get("id")
    away_id = feed.get("gameData", {}).get("teams", {}).get("away", {}).get("id")
    if team_id == home_id:
        return "home"
    if team_id == away_id:
        return "away"
    raise SystemExit(f"Team {team_id} not in this game (home={home_id}, away={away_id}).")

def linescore(feed: Dict[str, Any]) -> Tuple[int, int]:
    ls = feed.get("liveData", {}).get("linescore", {}).get("teams", {}) or {} ## Guard if linescore's missing (e.g. suspended)
    away = int((ls.get("away") or {}).get("runs") or 0)
    home = int((ls.get("home") or {}).get("runs") or 0)
    return away, home

# The final score of the game
def team_names(feed: Dict[str, Any]) -> Tuple[str, str]:
    gd = feed.get("gameData", {})
    home = gd.get("teams", {}).get("home", {}).get("name", "Home")
    away = gd.get("teams", {}).get("away", {}).get("name", "Away")
    return away, home

# The total boxscore of your team
def get_team_boxscore(feed: Dict[str, Any], side: str) -> Dict[str, Any]:
    return (feed.get("liveData", {}).get("boxscore", {}).get("teams", {}).get(side, {}) or {})

# This function deals with the peculiarities of the inningsPitched (see section 8 in docs/mlb_api_reference.md)
def ip_to_float(ip_str: str | None, outs: int | None) -> float:
    if outs is not None:
        return round(outs / 3.0, 2)
    if not ip_str:
        return 0.0
    try:
        if "." in ip_str:
            whole, frac = ip_str.split(".")
            whole, frac = int(whole), int(frac)
            third = {0: 0.0, 1: 1/3, 2: 2/3}.get(frac, 0.0)
            return round(whole + third, 2)
        return float(ip_str)
    except Exception:
        return 0.0

# See docs/custom_metrics.md for more info on the hitter score
def hitter_score(b: Dict[str, Any]) -> float:
    H  = int(b.get("hits", 0) or 0)
    HR = int(b.get("homeRuns", 0) or 0)
    _2B = int(b.get("doubles", 0) or 0)
    _3B = int(b.get("triples", 0) or 0)
    BB = int(b.get("baseOnBalls", 0) or 0)
    HBP = int(b.get("hitByPitch", 0) or 0)
    SB = int(b.get("stolenBases", 0) or 0)
    RBI = int(b.get("rbi", 0) or 0)
    R = int(b.get("runs", 0) or 0)
    singles = max(H - HR - _2B - _3B, 0)
    return 5*HR + 3*(_2B + _3B) + 2*(BB + HBP + SB) + 1*singles + 1.5*RBI + 1.0*R

# See docs/custom_metrics.md for more info on the pitcher score
def pitcher_score(p: Dict[str, Any]) -> float:
    ER = int(p.get("earnedRuns", 0) or 0)
    SO = int(p.get("strikeOuts", 0) or 0)
    H  = int(p.get("hits", 0) or 0)
    BB = int(p.get("baseOnBalls", 0) or 0)
    HR = int(p.get("homeRuns", 0) or 0)
    outs = p.get("outs")
    ip_str = p.get("inningsPitched")
    IP = ip_to_float(ip_str, outs if isinstance(outs, int) else None)
    return 6*IP + 3*SO - 4*ER - 2*(H + BB) - 3*HR

def batting_line(b: Dict[str, Any]) -> str:
    H  = int(b.get("hits", 0) or 0)
    AB = int(b.get("atBats", 0) or 0)
    HR = int(b.get("homeRuns", 0) or 0)
    RBI = int(b.get("rbi", 0) or 0)
    BB = int(b.get("baseOnBalls", 0) or 0)
    parts = [f"{H}-for-{AB}"]
    if HR: parts.append(f"{HR} HR")
    if RBI: parts.append(f"{RBI} RBI")
    if BB: parts.append(f"{BB} BB")
    return ", ".join(parts)

def top_hitters(team_box: Dict[str, Any], n: int = 3) -> List[Tuple[str, float, str]]:
    players = team_box.get("players", {})
    rows: List[Tuple[str, float, str]] = []
    for p in players.values():
        name = p.get("person", {}).get("fullName", "Unknown")
        b = p.get("stats", {}).get("batting", {})
        if not b: 
            continue
        score = hitter_score(b)
        rows.append((name, score, batting_line(b)))
    rows.sort(key=lambda x: x[1], reverse=True)
    return rows[:n]

def best_pitcher(team_box: Dict[str, Any]) -> Tuple[str, float, str]:
    players = team_box.get("players", {})
    best = ("None", -1e9, "")
    for p in players.values():
        name = p.get("person", {}).get("fullName", "Unknown")
        pit = p.get("stats", {}).get("pitching", {})
        if not pit: 
            continue
        score = pitcher_score(pit)
        ip = ip_to_float(pit.get("inningsPitched"), pit.get("outs") if isinstance(pit.get("outs"), int) else None)
        line = f"{ip} IP, {pit.get('earnedRuns',0)} ER, {pit.get('strikeOuts',0)} K, {pit.get('hits',0)} H, {pit.get('baseOnBalls',0)} BB"
        if score > best[1]:
            best = (name, score, line)
    return best

def choose_mvp(h_top: Tuple[str, float, str], p_best: Tuple[str, float, str]) -> Tuple[str, float, str, str]:
    return (p_best[0], p_best[1], p_best[2], "Pitcher") if p_best[1] > h_top[1] else (h_top[0], h_top[1], h_top[2], "Hitter")

def digest_one_game(feed: Dict[str, Any], team_id: int) -> None:
    away_name, home_name = team_names(feed)
    away_runs, home_runs = linescore(feed)
    side = which_side(feed, team_id)

    score_str = f"{away_name} {away_runs} — {home_name} {home_runs}"
    print(f"\n📣 {score_str}")
    print(f"Team: {home_name if side=='home' else away_name} ({side})\n")

    team_box = get_team_boxscore(feed, side)
    hitters = top_hitters(team_box, n=3)
    pitcher = best_pitcher(team_box)
    h1 = hitters[0] if hitters else ("None", 0.0, "")
    mvp_name, mvp_score, mvp_line, mvp_role = choose_mvp(h1, pitcher)

    print("Top hitters:")
    for name, score, line in hitters:
        print(f"  • {name}: {line}  [score {score:.1f}]")

    print(f"\nBest pitcher:\n  • {pitcher[0]}: {pitcher[2]}  [score {pitcher[1]:.1f}]")
    print(f"\n🏆 Team MVP: {mvp_name} ({mvp_role}) — {mvp_line}  [score {mvp_score:.1f}]\n")

def main() -> None:
    ap = argparse.ArgumentParser(description="Print a digest (+ MVP) for all saved games on a date, or a single JSON.")
    ap.add_argument("--json", help="Path to a saved feed/live JSON (process just this file).")
    ap.add_argument("--date", help="YYYY-MM-DD; if --json not given, process ALL game_*.json under data/raw/<date>/")
    ap.add_argument("--team", type=int, required=True, help="MLB teamId (e.g., Cubs=112)")
    ap.add_argument("--outdir", default="data", help="Base data directory (default: data)")
    args = ap.parse_args()

    if args.json:
        feeds = [load_json(Path(args.json))]
    else:
        if not args.date:
            raise SystemExit("Provide --json or --date")
        paths = find_saved_jsons(Path(args.outdir), args.date)
        feeds = [load_json(p) for p in paths]

    for feed in feeds:
        digest_one_game(feed, args.team)

if __name__ == "__main__":
    main()