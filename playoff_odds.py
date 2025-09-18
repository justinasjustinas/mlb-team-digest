#!/usr/bin/env python3
"""Estimate postseason odds for MLB teams.

This module fetches current season standings and applies a transparent
heuristic to approximate each club's chance of qualifying for the
postseason—either by winning its division or securing a wildcard berth.
The intent is to surface an intuitive, easy-to-maintain signal that can
be displayed inside the daily game digest.
"""
from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, Iterable, List, Optional, Sequence

import requests

STANDINGS_URL = (
    "https://statsapi.mlb.com/api/v1/standings?leagueId=103,104&season=2025"
    "&standingsTypes=regularSeason&hydrate=team(division,league),division,league"
)


@dataclass(frozen=True)
class TeamStanding:
    """Minimal representation of a team's record within the standings."""

    team_id: int
    team_name: str
    league: str
    division: str
    wins: int
    losses: int

    @property
    def games_played(self) -> int:
        return self.wins + self.losses

    @property
    def win_pct(self) -> float:
        return self.wins / self.games_played if self.games_played else 0.0


def fetch_standings() -> List[TeamStanding]:
    """Fetch raw standings data from MLB's public endpoint."""

    resp = requests.get(STANDINGS_URL, timeout=10)
    resp.raise_for_status()
    payload = resp.json()

    teams: List[TeamStanding] = []

    # Primary parsing path for the StatsAPI standings endpoint.
    for block in payload.get("records", []):
        league = block.get("league") or {}
        division = block.get("division") or {}
        for entry in block.get("teamRecords", []):
            enriched = dict(entry)
            enriched.setdefault("league", entry.get("league") or league)
            enriched.setdefault("division", entry.get("division") or division)
            normalized = _normalize_team(enriched, entry)
            if normalized:
                teams.append(normalized)

    if teams:
        return teams

    # Fallback path for the legacy bdfed endpoint shape.
    raw_records = payload.get("data") or payload.get("stats") or []
    for raw in raw_records:
        record = raw.get("stats") if isinstance(raw, dict) else None
        if isinstance(record, dict):
            record = record.get("standings", record)
        elif isinstance(record, list):
            record = record[0] if record else raw
        normalized = _normalize_team(record or raw, raw)
        if normalized:
            teams.append(normalized)

    return teams


def estimate_playoff_odds(
    team: str | int, standings: Optional[Sequence[Any]] = None
) -> Optional[int]:
    """Return an estimated postseason probability for ``team``.

    Parameters
    ----------
    team:
        Team identifier (name or numeric id).
    standings:
        Optional iterable of standings records. When omitted the standings
        are fetched from :data:`STANDINGS_URL`.

    Returns
    -------
    Optional[int]
        Integer percentage in the inclusive range [0, 100]. ``None`` is
        returned when the team cannot be found or the standings endpoint
        is unavailable.
    """

    try:
        records = list(standings) if standings is not None else fetch_standings()
    except Exception:
        return None

    normalized = [_normalize_team(obj) for obj in records]
    teams = [team for team in normalized if team]
    if not teams:
        return None

    lookup_key = str(team).lower()
    subject: Optional[TeamStanding] = None
    for entry in teams:
        if str(entry.team_id) == str(team) or entry.team_name.lower() == lookup_key:
            subject = entry
            break
    if subject is None:
        return None

    league_mates = [t for t in teams if t.league == subject.league]
    division_mates = [t for t in league_mates if t.division == subject.division]

    division_leader = _division_leader(division_mates)
    division_prob = _division_probability(subject, division_mates)

    if division_leader and division_leader.team_id == subject.team_id:
        wildcard_prob = 0.0
    else:
        wildcard_prob = _wildcard_probability(subject, league_mates)

    overall = 1.0 - (1.0 - division_prob) * (1.0 - wildcard_prob)
    return int(round(_clamp(overall, 0.0, 1.0) * 100))


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _normalize_team(record: Any, raw: Optional[Any] = None) -> Optional[TeamStanding]:
    """Convert API payloads or dictionaries into :class:`TeamStanding`."""

    if isinstance(record, TeamStanding):
        return record

    candidate = record or raw or {}

    team_info = candidate.get("team") if isinstance(candidate, dict) else None

    team_id_value = None
    if isinstance(candidate, dict):
        team_id_value = candidate.get("teamId") or candidate.get("team_id")
    if team_id_value is None and isinstance(team_info, dict):
        team_id_value = team_info.get("id")

    try:
        team_id = int(team_id_value)
    except Exception:
        return None

    team_name_value = ""
    if isinstance(candidate, dict):
        team_name_value = candidate.get("teamName") or candidate.get("team_name") or ""
    if not team_name_value and isinstance(team_info, dict):
        team_name_value = team_info.get("name", "")
    team_name = str(team_name_value).strip()

    league_value = None
    division_value = None
    if isinstance(candidate, dict):
        league_value = candidate.get("league")
        division_value = candidate.get("division")
    if league_value is None and isinstance(team_info, dict):
        league_value = team_info.get("league")
    if division_value is None and isinstance(team_info, dict):
        division_value = team_info.get("division")

    league = _extract_name(league_value)
    division = _extract_name(division_value)

    wins_value = None
    losses_value = None
    if isinstance(candidate, dict):
        wins_value = candidate.get("w") or candidate.get("wins")
        losses_value = candidate.get("l") or candidate.get("losses")
        if wins_value is None and "leagueRecord" in candidate:
            wins_value = candidate.get("leagueRecord", {}).get("wins")
        if losses_value is None and "leagueRecord" in candidate:
            losses_value = candidate.get("leagueRecord", {}).get("losses")

    try:
        wins = int(wins_value)
        losses = int(losses_value)
    except Exception:
        return None

    if not team_name or not league or not division:
        return None

    return TeamStanding(team_id, team_name, league, division, wins, losses)


def _extract_name(value: Any) -> str:
    if isinstance(value, dict):
        return str(value.get("name", "")).strip()
    if value is None:
        return ""
    return str(value).strip()


def _division_leader(teams: Iterable[TeamStanding]) -> Optional[TeamStanding]:
    ranked = sorted(teams, key=lambda t: (t.win_pct, t.wins), reverse=True)
    return ranked[0] if ranked else None


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def _games_back(trailing: TeamStanding, leader: TeamStanding) -> float:
    return ((leader.wins - trailing.wins) + (trailing.losses - leader.losses)) / 2.0


def _sigmoid(delta: float, scale: float = 2.0) -> float:
    scale = max(scale, 1e-6)
    return 1.0 / (1.0 + math.exp(-delta / scale))


def _division_probability(team: TeamStanding, peers: Sequence[TeamStanding]) -> float:
    if not peers:
        return 0.0

    ranked = sorted(peers, key=lambda t: (t.win_pct, t.wins), reverse=True)
    leader = ranked[0]

    if team.team_id == leader.team_id:
        if len(ranked) == 1:
            return 1.0
        runner_up = ranked[1]
        games_ahead = _games_back(runner_up, team)
        return _sigmoid(games_ahead, scale=1.5)

    games_back = _games_back(team, leader)
    return _sigmoid(-games_back, scale=1.5)


def _wildcard_probability(team: TeamStanding, league_mates: Sequence[TeamStanding]) -> float:
    if not league_mates:
        return 0.0

    divisions: dict[str, List[TeamStanding]] = {}
    for t in league_mates:
        divisions.setdefault(t.division, []).append(t)

    division_leaders = {
        leader.team_id
        for leader in (_division_leader(division) for division in divisions.values())
        if leader is not None
    }

    candidates = [t for t in league_mates if t.team_id not in division_leaders]
    candidates.sort(key=lambda t: (t.win_pct, t.wins), reverse=True)

    slots = 3
    if not candidates:
        return 0.0

    try:
        rank = next(i for i, entry in enumerate(candidates) if entry.team_id == team.team_id)
    except StopIteration:
        return 0.0

    if rank < slots:
        if len(candidates) <= slots:
            return 0.8
        next_best = candidates[slots]
        cushion = _games_back(next_best, team)
        return _sigmoid(cushion, scale=3.0)

    third_team = candidates[slots - 1]
    deficit = _games_back(team, third_team)
    return _sigmoid(-deficit, scale=3.0)
