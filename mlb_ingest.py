#!/usr/bin/env python
from __future__ import annotations

import argparse
import datetime as dt
import json
import time
from pathlib import Path
from typing import Any, Dict, List
from zoneinfo import ZoneInfo

import requests

BASE_SCHEDULE = "https://statsapi.mlb.com/api/v1/schedule"
BASE_FEED = "https://statsapi.mlb.com/api/v1.1/game/{gamePk}/feed/live"

TEAM_ID = 112 # Hardcoded for Cubs for now

# Just to make sure it works correctly when no --date is passed
CHI_TZ = ZoneInfo("America/Chicago")

def today_chicago_date_str() -> str:
    """Return today's date string in Chicago local time (YYYY-MM-DD)."""
    return dt.datetime.now(CHI_TZ).date().isoformat()

def fetch_json(url: str, params: Dict[str, Any] | None = None, *, timeout: int = 20) -> Dict[str, Any]:
    """GET JSON with a tiny bit of resilience."""
    for attempt in range(3):
        try:
            r = requests.get(url, params=params, timeout=timeout)
            r.raise_for_status()
            return r.json()
        except requests.RequestException as e:
            if attempt == 2:
                raise
            time.sleep(1.5 * (attempt + 1))
    raise RuntimeError("unreachable")

def get_game_pks_for_team_on_date(team_id: int, date_str: str) -> List[Dict[str, Any]]:
    """
    Return a list of game dicts for that team/date from the schedule API.
    Each item includes gamePk, status, home/away info, etc.
    """
    params = {"sportId": 1, "teamId": team_id, "date": date_str}
    data = fetch_json(BASE_SCHEDULE, params=params)
    dates = data.get("dates", [])
    if not dates:
        return []
    games = dates[0].get("games", [])
    # Keep only games where this team is home or away (the API already filters, but let's be explicit)
    out = []
    for g in games:
        teams = g.get("teams", {})
        if teams.get("home", {}).get("team", {}).get("id") == team_id or \
           teams.get("away", {}).get("team", {}).get("id") == team_id:
            out.append(g)
    return out

def fetch_feed_live(game_pk: int) -> Dict[str, Any]:
    url = BASE_FEED.format(gamePk=game_pk)
    return fetch_json(url)

def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)

def save_json(obj: Dict[str, Any], path: Path) -> None:
    ensure_dir(path.parent)
    with path.open("w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)

def extract_status_from_feed(feed: Dict[str, Any]) -> str:
    # Common fields used by StatsAPI
    return (
        feed.get("gameData", {})
            .get("status", {})
            .get("detailedState", "")
    ) or feed.get("gameData", {}).get("status", {}).get("abstractGameState", "")

def extract_official_date_from_feed(feed: Dict[str, Any]) -> str:
    # Prefer officialDate if present; fallback to date from gameDate
    gd = feed.get("gameData", {})
    official = gd.get("datetime", {}).get("officialDate")
    if official:
        return official  # already YYYY-MM-DD
    # fallback to gameDate (UTC ISO) → date
    game_date_iso = gd.get("datetime", {}).get("dateTime")
    if game_date_iso:
        try:
            return dt.datetime.fromisoformat(game_date_iso.replace("Z", "+00:00")).date().isoformat()
        except Exception:
            pass
    # ultimate fallback: today in Chicago
    return today_chicago_date_str()

def describe_matchup(game: Dict[str, Any]) -> str:
    teams = game.get("teams", {})
    home = teams.get("home", {}).get("team", {}).get("name", "Home")
    away = teams.get("away", {}).get("team", {}).get("name", "Away")
    return f"{away} at {home}"

def main():
    parser = argparse.ArgumentParser(
        description="Fetch MLB StatsAPI feed/live JSON for a team's game(s) on a date."
    )
    parser.add_argument("--team", type=int, required=True,
                        help="Team's numeric ID")
    parser.add_argument("--date", type=str, default=None,
                        help="Date in YYYY-MM-DD (defaults to today in America/Chicago)")
    parser.add_argument("--outdir", type=str, default="data",
                        help="Output directory for saved JSON (default: data)")
    parser.add_argument("--wait", action="store_true",
                        help="If set, poll until game is Final (checks every 2 minutes)")
    parser.add_argument("--max-wait-min", type=int, default=240,
                        help="Max minutes to wait when --wait is set (default 240)")
    args = parser.parse_args()

    # Resolve date (Chicago local)
    date_str = args.date or today_chicago_date_str()
    outdir = Path(args.outdir)

    print(f"🔎 Looking up schedule for team_id={TEAM_ID} on {date_str} (America/Chicago) …")
    games = get_game_pks_for_team_on_date(TEAM_ID, date_str)
    if not games:
        print("No games found for that team/date.")
        return

    for g in games:
        game_pk = g.get("gamePk")
        matchup = describe_matchup(g)
        gstatus = g.get("status", {}).get("detailedState", "Unknown")
        print(f"• gamePk={game_pk} — {matchup} — schedule status: {gstatus}")

        # Fetch feed/live. Optionally poll until Final.
        poll_count = 0
        max_polls = max(1, (args.max_wait_min * 60) // 120) if args.wait else 1 #REVIEW THIS!
        while True:
            feed = fetch_feed_live(game_pk)
            fstatus = extract_status_from_feed(feed)
            print(f"  ↳ feed/live status: {fstatus}")

            if (not args.wait) or (fstatus.lower() == "final"):
                # Save immediately if not waiting, or now that it's final.
                official_date = extract_official_date_from_feed(feed)
                out_path = outdir / "raw" / official_date / f"game_{game_pk}.json"
                save_json(feed, out_path)
                print(f"  💾 saved: {out_path}")
                break

            poll_count += 1
            if poll_count >= max_polls:
                print("  ⏰ reached max wait; not final yet. Try again later with --wait or rerun.")
                break

            print("  ⏳ not final yet; sleeping 120s …")
            time.sleep(120)

if __name__ == "__main__":
    main()