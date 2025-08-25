# MLB StatsAPI `feed/live` JSON — Practical Reference

> Targeted for **team-focused digests** (box score, MVP, trends).
> Endpoint: `https://statsapi.mlb.com/api/v1.1/game/{gamePk}/feed/live`
> Made by the author of this project (not MLB) and therefore should be considered unofficial and possibly not 100% accurate.

## 0) Top-level keys

- `gamePk` _(int)_ — unique game ID (use in filenames).
- `link` _(str)_ — API-relative path for this game.
- `metaData` _(obj)_ — time stamps, pretty print flags.
- `copyright` _(str)_.

---

## 1) `gameData` — identities, context, status

**Paths to use**

- `gameData.status.detailedState` _(str)_ — `"Final"`, `"In Progress"`, `"Scheduled"`.
- `gameData.status.abstractGameState` _(str)_ — `"Final"`, `"Live"`, `"Preview"`.
- `gameData.datetime.officialDate` _(YYYY-MM-DD)_ — the local game date (best for folder names).
- `gameData.datetime.dateTime` _(ISO UTC)_ — scheduled start time (e.g., `"2025-08-22T18:05:00Z"`).
- `gameData.teams.home.id` / `.name` — home team; same for `away`.
- `gameData.game.doubleHeader` _(str)_ — `"Y"` if part of a doubleheader (also see `gameNumber`).
- `gameData.venue` — park info (name, city, id).

**Notes**

- Prefer `officialDate` for daily folders.
- Use `status.detailedState` in your polling logic (`--wait`).

---

## 2) `liveData.linescore` — final/current score

**Paths**

- `liveData.linescore.teams.away.runs` _(int or null → treat as 0)_
- `liveData.linescore.teams.home.runs` _(int or null → treat as 0)_
- Also there: per-inning scores, hits, errors, left-on-base.

**Why you need it**

- Build the header: `"AwayName X — HomeName Y"`.

---

## 3) `liveData.boxscore` — team + player box stats

This is where you’ll get per-player **batting** and **pitching** lines.

**Team block**

- `liveData.boxscore.teams.home.team.id/name`
- `liveData.boxscore.teams.away.team.id/name`

**Players (by team)**

- `liveData.boxscore.teams.{home|away}.players` — dict keyed like `"IDXXXXXX"`.

Each player entry contains:

- `person.fullName`, `person.id`
- `position.abbreviation` _(e.g., "RF", "P")_
- `stats.batting` _(obj)_ — keys you’ll use:
  - `atBats` (AB), `hits` (H), `doubles` (2B), `triples` (3B), `homeRuns` (HR),
  - `baseOnBalls` (BB), `hitByPitch` (HBP), `runs` (R), `rbi` (RBI), `stolenBases` (SB),
  - `totalBases` (TB) sometimes present.
- `stats.pitching` _(obj)_ — keys you’ll use:
  - **`outs` (int)** — total outs recorded (precise; 3 outs = 1 inning).
  - **`inningsPitched` (str)** — human shorthand `"6.1" = 6⅓`, `"6.2" = 6⅔`.
  - `hits`, `runs`, `earnedRuns`, `baseOnBalls` (BB), `strikeOuts` (SO), `homeRuns` (HR).

**Innings pitched: `outs` vs `inningsPitched`**

- `outs` is **preferred** (precise integer). Convert to innings with `outs / 3`.
  - 19 outs → 6⅓ IP; 20 outs → 6⅔ IP; 21 outs → 7.0 IP.
- `inningsPitched` is a **string shorthand** for humans. Use as fallback.

**Safe access pattern (Python)**

```python
team_box = feed["liveData"]["boxscore"]["teams"][side]  # side = "home"|"away"
for p in team_box["players"].values():
    batting = p.get("stats", {}).get("batting", {})
    pitching = p.get("stats", {}).get("pitching", {})
```

---

## 4) `liveData.plays` — play-by-play (optional for V1)

- `liveData.plays.allPlays` — each play event with context.
- `result.event`, `about.isScoringPlay`, `about.inning`, runners before/after.
- If you later compute **WPA**/**RE24** or “Turning Point,” you’ll use this.

---

## 6) Minimal JSON path cheatsheet

```text
# Teams & status
gameData.status.detailedState
gameData.datetime.officialDate
gameData.teams.home.id / name
gameData.teams.away.id / name

# Score
liveData.linescore.teams.home.runs
liveData.linescore.teams.away.runs

# Boxscore teams
liveData.boxscore.teams.home.team.name
liveData.boxscore.teams.away.team.name

# Players (iterate values())
liveData.boxscore.teams.{home|away}.players

# Hitting stats per player
...players[PID].stats.batting.atBats
...players[PID].stats.batting.hits
...players[PID].stats.batting.homeRuns
...players[PID].stats.batting.baseOnBalls
...players[PID].stats.batting.hitByPitch
...players[PID].stats.batting.rbi
...players[PID].stats.batting.runs
...players[PID].stats.batting.doubles / triples

# Pitching stats per player
...players[PID].stats.pitching.outs
...players[PID].stats.pitching.inningsPitched
...players[PID].stats.pitching.earnedRuns
...players[PID].stats.pitching.strikeOuts
...players[PID].stats.pitching.baseOnBalls
...players[PID].stats.pitching.hits
...players[PID].stats.pitching.homeRuns
```

---

## 7) Data quality & nulls — tips

- Many numeric fields can be `null` → coerce with `or 0` before `int(...)`.
- Some players may have only batting or only pitching in a game (skip missing blocks).
- For postponed/suspended games, `status.detailedState` ≠ `"Final"` and stats may be incomplete.
- Doubleheaders: `gameData.game.doubleHeader == "Y"` and/or two distinct `gamePk`s on the same `officialDate`.

---

## 8) Example: Preference for outs with safe innings conversion (can see it in game_digest.py)

In baseball box scores, Innings Pitched (IP) is often given in two formats:

1. As total outs (e.g., outs = 19)
   → 19 outs = 6 innings + 1 out = 6⅓ innings.

2. As a shorthand string (e.g., "6.1" or "6.2")
   "6.1" means 6 innings + 1 out = 6⅓.
   "6.2" means 6 innings + 2 outs = 6⅔.

These don’t look like normal decimals (because .1 ≠ one tenth — it means one out, which is ⅓ of an inning).
Therefore, this function converts either representation into a decimal float you can do math with:

```python
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
```

---

## 9) Some additional fields to explore in the future

- Deep `gameData.players` bios (useful if you want handedness/positions later).
- `decisions` (W/L/S) — you can add this later for context in summaries.
- Umpires, weather, broadcasts — nice polish, not required.

---

## 10) Notes on usage / terms (summary)

- Use for **personal, non-commercial, non-bulk** purposes.
- Don’t redistribute raw JSON; fetch directly from the API when running locally.
- This summary was not made by MLB, and therefore it might not be 100% accurate.

---

### Appendix: Quick field-to-stat mapping

| Stat | Fields used (per batter)                                   |
| ---- | ---------------------------------------------------------- |
| AVG  | `hits`, `atBats`                                           |
| OBP  | `hits`, `baseOnBalls`, `hitByPitch`, `atBats`, `sacFlies?` |
| SLG  | `totalBases` (if present) **or** derive from 1B/2B/3B/HR   |
| OPS  | `OBP + SLG`                                                |

| Stat | Fields used (per pitcher)                    |
| ---- | -------------------------------------------- |
| IP   | `outs` (preferred) or `inningsPitched`       |
| ERA  | `earnedRuns`, `outs/inningsPitched`          |
| WHIP | `baseOnBalls`, `hits`, `outs/inningsPitched` |
| K    | `strikeOuts`                                 |
| BB   | `baseOnBalls`                                |
