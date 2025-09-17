# Postseason odds heuristic

The `playoff_odds` module powers the postseason percentage that now
appears in the game digest. It is intentionally lightweight: rather than
training a predictive model we rely on the freshest standings and a few
baseball-savvy rules of thumb so the number is easy to reason about and
can run anywhere we ship the digest.

## Data source

We query the public MLB Stats API endpoint used elsewhere in this
project (`https://bdfed.stitch.mlbinfra.com/bdfed/stats/team?...`). The
payload already contains league, division, wins and losses for each club;
no authentication or additional joins are required.

## Normalisation

The API mixes camelCase and snake_case field names, so the fetch layer
normalises each entry into a simple `TeamStanding` dataclass containing:

- `team_id` / `team_name`
- `league` and `division`
- `wins` and `losses`
- a derived winning percentage

Records missing any of the above are ignored to avoid producing noisy
results.

## Probability model

The estimator computes two probabilities and then combines them:

1. **Division path** – Teams are ranked by winning percentage inside
   their division. The leader's probability depends on the cushion to the
   runner-up (expressed in games ahead). Trailing clubs get a probability
   based on how many games back they sit. We run these margins through a
   sigmoid curve so small swings in the standings cause smooth changes
   in the probability rather than binary jumps.
2. **Wildcard path** – The same approach is applied league-wide once the
   three division leaders are removed. If a team occupies one of the
   three wildcard slots we look at the buffer to the next-best club; if
   it sits outside the cut line we look at the deficit to the third
   wildcard. As before, the margin is converted to a probability via a
   sigmoid.

The final postseason odds equal ``1 - (1 - P_division) * (1 -
P_wildcard)``, representing the chance the team reaches October either
through winning the division or via a wildcard berth. Division leaders
skip the wildcard calculation—they already have a direct path, so their
probability is simply the division estimate.

## Operational characteristics

- The HTTP call uses a ten second timeout and bubbles up any HTTP
  failures so callers can surface a graceful fallback.
- All math is deterministic and requires only wins/losses, which keeps
  the module testable with simple fixtures.
- The heuristics produce values in the 0–100 range while favouring teams
  that are leading or within striking distance, aligning with intuition
  without pretending to be a full simulation.
