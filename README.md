WORK-IN-PROGRESS!

This project runs privately and delivers email digests to the project owner only. It is intended strictly for personal, educational use and does not redistribute MLB data.

## Common derived metrics computed in this project

**Hitters**

- `AVG = H / AB` (guard `AB > 0`)
- `OBP = (H + BB + HBP) / (AB + BB + HBP + SF)`
- `SLG = TB / AB`
- `OPS = OBP + SLG`

**Pitchers**

- `IP (float) = outs / 3` (or parse `inningsPitched`)
- `ERA = (ER * 9) / IP` (guard `IP > 0`)
- `WHIP = (BB + H) / IP` (guard `IP > 0`)

See docs/glossary.md in case the above acronyms do not mean much to you yet.

## ⚠️ Disclaimer

This project uses MLB’s public Stats API (`statsapi.mlb.com`) to fetch baseball data.

- It is intended **solely for personal, educational, and non-commercial purposes**.
- **No raw MLB data is redistributed** in this repository. Each user of this project must fetch data directly from MLB’s API.
- MLB content and data are © MLB Advanced Media, L.P. All rights reserved.

If you use this project, please respect MLB’s terms: only individual, non-commercial, non-bulk use of the data is permitted.
