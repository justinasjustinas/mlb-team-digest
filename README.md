## MLB Team Digest

Please note that I'm rather new to baseball, let alone baseball stats. Therefore, consider this **work-in-progress** project as a way to learn more about this fascinating sport and to play around with IaC, certain Cloud concepts, etc.

This project runs privately and delivers a simple digest of the latest game for a selected team. It is intended strictly for personal, educational use and does not redistribute MLB data.

## Next steps

- Setup a workflow to send an email with the game digest.
- Update README with deployment steps, variable setup, etc.
- Improve tests (datetime.datetime.utcnow() is deprecated).
- Add core metrics, namely AVG, OBP, SLG, OPS, ERA and WHIP.
- Add --beginner parameter to return a beginner-friendly digest.
- Let `--team` param accept names/abbreviations (nicer CLI).
- Figure out a smart way to perform data validation tests.
- And more...

## Derived metrics that we are (or will be) computing in this project

**Hitters**

- Implemented:
  - `hitter score = 5*HR + 3*(2B + 3B) + 2*(BB + HBP + SB) + 1*singles + 1.5*RBI + 1.0*R`
- To be implemented:
  - `AVG = H / AB` (guard `AB > 0`)
  - `OBP = (H + BB + HBP) / (AB + BB + HBP + SF)`
  - `SLG = TB / AB`
  - `OPS = OBP + SLG`

**Pitchers**

- Implemented:
  - `pitcher score = 6*IP + 3*SO - 4*ER - 2*(H + BB) - 3*HR`
- To be implemented:
  - `IP (float) = outs / 3` (or parse `inningsPitched`)
  - `ERA = (ER * 9) / IP` (guard `IP > 0`)
  - `WHIP = (BB + H) / IP` (guard `IP > 0`)

See docs/custom_metrics.md for the full overview of how custom metrics, such as hitter score and pitcher score are calculated.
See docs/glossary.md in case the above acronyms do not mean much to you yet.

## ⚠️ Disclaimer

This project uses MLB’s public Stats API (`statsapi.mlb.com`) to fetch baseball data.

- It is intended **solely for personal, educational, and non-commercial purposes**.
- **No raw MLB data is redistributed** in this repository. Each user of this project must fetch data directly from MLB’s API.
- MLB content and data are © MLB Advanced Media, L.P. All rights reserved.

If you use this project, please respect MLB’s terms: only individual, non-commercial, non-bulk use of the data is permitted.
