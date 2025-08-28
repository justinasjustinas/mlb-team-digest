# Custom Metrics (MVP Selection)

This project includes **custom scoring formulas** for evaluating a player’s impact in a single game.  
These are not official MLB statistics — they’re heuristic formulas designed to identify the **MVP of your chosen team**.

---

## Batter Score (BAT_SCORE)

```python
BAT_SCORE = 5*HR + 3*(2B + 3B) + 2*(BB + HBP + SB) + 1*singles + 1.5*RBI + 1.0*R
```

**Weights**

- Home Run (HR): 5 points
- Double (2B) or Triple (3B): 3 points
- Walk (BB), Hit By Pitch (HBP), Stolen Base (SB): 2 points
- Single: 1 point
- Run Batted In (RBI): 1.5 points
- Run Scored (R): 1 point

**Rationale**

- HRs are the most impactful single event → highest weight.
- Extra-base hits (2B, 3B) show power.
- Walks/HBP show discipline, SB adds value on the bases.
- RBIs reflect run production.
- Runs scored capture direct contribution.
- Singles are useful but less powerful.

---

## Pitcher Score (PITCH_SCORE)

```python
PITCH_SCORE = 6*IP + 3*SO - 4*ER - 2*(H + BB) - 3*HR
```

**Weights**

- Innings Pitched (IP): +6 per inning
- Strikeouts (SO): +2 each
- Earned Runs (ER): −4 each
- Non-HR Hits (H − HR): −2 each
- Walks (BB): −1 each
- Home Runs Allowed (HR): −3 each

**Rationale**

- Going deep into a game (IP) adds strong value. A starting pitcher’s job is to record as many outs as possible while keeping runs low. Every inning pitched (IP) = 3 outs recorded. If a starter throws 6, 7, or more innings, that means the bullpen (relief pitchers) doesn’t have to cover as much, which is highly valuable to the team.
- Strikeouts prevent balls in play from turning into hits.
- Runs allowed (ER) carry a heavy penalty since they directly cost the team.
- Walks and non-HR hits are mildly negative, as they create baserunners.
- Home runs receive an additional penalty beyond being counted as a hit and an earned run, since they guarantee a run and cannot be mitigated by defense.

---

## Notes

- These weights are **arbitrary** and can be tuned.
- Future versions might explore:
  - Rethinking the philosophy in general, e.g. purely skill based OR what happened in the game (incl. luck) OR a combination of the two?
  - Possibly shrink toward average for tiny samples (short relief stints), so one mistake doesn’t make the score whiplash.
  - Turn it into a 0-100 SCORE.
  - And the list goes on...
