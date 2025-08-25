# Custom Metrics (MVP Selection)

This project includes **custom scoring formulas** for evaluating a player’s impact in a single game.  
These are not official MLB statistics — they’re heuristic formulas designed to identify the **MVP of your chosen team**.

---

## Batter Score (BAT_SCORE)

```python
score = 5*HR + 3*(2B + 3B) + 2*(BB + HBP + SB) + 1*singles + 1.5*RBI + 1.0*R
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
score = 6*IP + 3*SO - 4*ER - 2*(H + BB) - 3*HR
```

**Weights**

- Innings Pitched (IP): +6 per inning
- Strikeouts (SO): +3 each
- Earned Runs (ER): −4 each
- Hits + Walks (H, BB): −2 each
- Home Runs Allowed (HR): −3 each

**Rationale**

- Going deep into a game (IP) adds strong value.
- Strikeouts prevent bad luck from balls in play.
- ER, hits, walks, and HRs penalize mistakes.

---

## Notes

- These weights are **arbitrary** and can be tuned.
- Future versions might explore:
  - Using advanced metrics (WPA, OPS, FIP).
  - Adjusting weights based on league averages.
  - Including defensive contributions.
