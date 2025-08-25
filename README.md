# MLB Team Digest

A small pipeline that ingests **just-enough** MLB game data for a single team, stores it in BigQuery, then builds a compact, human‑readable **game digest** (and saves that back to BigQuery).

Please note that I'm rather new to baseball, let alone baseball stats. Therefore, consider this **work-in-progress** project as a way to learn more about this fascinating sport and to play around with IaC, certain Cloud concepts, etc.

---

## How it works (high level)

```
MLB Stats API  ──▶  Ingest (mlb_ingest.py)  ──▶  BigQuery tables
                                       │
                                       └──▶  JSON files (for local debugging)

BigQuery  ──▶  Digest (game_digest.py) ──▶  BigQuery: mlb.game_digests
                                         └──▶  Markdown printed to logs/stdout
```

- **Ingest (`mlb_ingest.py`)**

  - For a given `--team` (e.g., 112 for Cubs) and `--date` (YYYY‑MM‑DD, default: “today” in `America/New_York`), fetches the game(s).
  - If a game is **FINAL**, pulls minimal **summary**, **linescore**, and **boxscore players** data, then writes rows to BigQuery:
    - `mlb.game_summaries`
    - `mlb.game_linescore`
    - `mlb.game_boxscore_players`
  - On Cloud Run the default sink is **BigQuery**; locally the default sink is **JSON** (under `./data`). You can override via `--output bq|json`.

- **Digest (`game_digest.py`)**
  - Reads back the above three tables (or local JSON when `--output json`) for the team/date.
  - Computes a tidy markdown body: final score, linescore, team totals, top batters, pitching highlight, and a short “notables” list.
  - Prints the digest to stdout **and** (by default) writes one row per final game into `mlb.game_digests`.
  - You can disable writing with `--no_write` (useful locally).

---

## Example of final digest output

Note that this was just to test the whole setup. Calculated metrics, such as **AVG, OBP, SLG, OPS, ERA, WHIP**, will be added soon, which means the digest itself will change as well.

```
Chicago Cubs W 5-3 vs St. Louis Cardinals
--------------------------------------------------------------------------------
## Final: Chicago Cubs 5-3 W St. Louis Cardinals

### Linescore
Away: 1 0 0 1 0 0 0 1 0
Home: 0 0 2 0 0 1 2 0 -

### Team Totals (batting)
- R 5 • H 9 • HR 2 • RBI 5 • BB 3 • SO 7 • SB 1
- Homers: Seiya Suzuki (1), Ian Happ (1)

### Top Batters
- Seiya Suzuki: 3 H, 1 HR, 2 RBI, 0 BB, 1 K (AB 4)
- Ian Happ: 2 H, 1 HR, 3 RBI, 1 BB, 2 K (AB 4)

### Pitching
- Team: 9.0 IP, 10 K, 3 ER, 6 H, 2 BB
- Justin Steele: 7.0 IP, 8 K, 2 ER, 5 H, 1 BB

### Notables
- Seiya Suzuki: 3H, 1HR, 2RBI
- Justin Steele: 7.0 IP, 8 K, 2 ER
```

> Note: This is a representative example. Your actual output depends on the selected team/date and the underlying data, obviously.

---

## CLI quick reference

### Ingest

```bash
python mlb_ingest.py --team 112 --date 2025-08-23           # Local, writes JSON under ./data by default
python mlb_ingest.py --team 112 --date 2025-08-23 --output bq  # Force BigQuery sink locally
```

Flags:

- `--team <int>`: Team ID (e.g., 112 Cubs).
- `--date YYYY-MM-DD`: Defaults to “today” in `America/New_York`.
- `--output bq|json`: Override sink (default: CloudRun=bq, local=json).
- `--json_outdir <dir>`: Local JSON output directory (default: `data`).

Environment (optional):

- `BQ_PROJECT`: GCP project override for BigQuery client.
- `BQ_LOCATION`: BigQuery dataset location (default `EU`).
- `BQ_SUMMARIES` (default `mlb.game_summaries`)
- `BQ_LINESCORE` (default `mlb.game_linescore`)
- `BQ_PLAYERS` (default `mlb.game_boxscore_players`)

### Digest

```bash
python game_digest.py --team 112 --date 2025-08-23                  # Reads from BigQuery, writes to mlb.game_digests
python game_digest.py --team 112 --date 2025-08-23 --no_write       # Read BQ, do not write
python game_digest.py --team 112 --date 2025-08-23 --output json    # Read from local JSON files
```

Flags:

- `--team <int>` and `--date YYYY-MM-DD` as above.
- `--output bq|json`: **Source** to read from (`bq` default).
- `--json_indir <dir>`: Local JSON input directory (matches ingest’s `--json_outdir`).
- `--bq_project`: Optional GCP project override for BigQuery client.
- `--bq_digests` (default `mlb.game_digests`).
- `--no_write`: Don’t write back to BigQuery (still prints to stdout).

Environment (optional):

- `BQ_PROJECT`, `BQ_LOCATION`, `BQ_SUMMARIES`, `BQ_LINESCORE`, `BQ_PLAYERS`, `BQ_DIGESTS`

---

## Infrastructure (Terraform + GCP)

### What gets created

- **Artifact Registry** (Docker) for your images.
- **BigQuery dataset** (e.g., `mlb`) — tables are created by the code at first write.
- **Service Accounts** and **IAM** bindings so Cloud Run Jobs can pull images and read/write BigQuery.
- **Cloud Run Jobs**:
  - `mlb-ingest` (runs `mlb_ingest.py`)
  - `mlb-digest` (runs `game_digest.py`)
  - Env passed to jobs: `BQ_*` variables (location, table names, etc.).
- **Cloud Workflows**: `mlb-orchestrator` that, for a given team/date:
  - Gets the day’s games, **sleeps until each game’s scheduled start**, then sleeps an additional **90 minutes**.
  - Polls every ~15 minutes until the game is **FINAL**; then triggers **ingest** followed by **digest** for that game/date.
- **Cloud Scheduler**: one job per team ID that triggers the Workflow daily (default **08:00 America/New_York**) so the workflow can handle that day’s games. (Scheduler runs in a supported region such as `europe-west1` by default here.)

### Prereqs

- GCP project with billing enabled.
- Admin permissions to enable APIs and create resources.
- Installed CLIs: `gcloud`, `terraform`, and Docker (or Cloud Build).

### Enable required APIs (one time)

```bash
gcloud services enable   artifactregistry.googleapis.com   run.googleapis.com   workflows.googleapis.com   bigquery.googleapis.com   cloudscheduler.googleapis.com   cloudbuild.googleapis.com
```

### Configure Terraform

Edit `variables.tf` (or provide `-var` flags) for at least:

- `project_id`: your GCP project
- `region`: e.g., `europe-west4` (for Artifact Registry, Cloud Run Jobs, Workflows)
- `bq_location`: `EU` (recommended with `europe-west4`)
- `dataset_id`: e.g., `mlb`
- `repo_name`: e.g., `mlb-docker`
- `image_name`: e.g., `mlb`
- `team_ids`: list like `[112]`
- `scheduler_region`: **must be a Scheduler‑supported region**, default is `europe-west1`
- `scheduler_cron`: default `"0 8 * * *"`
- `scheduler_tz`: default `"America/New_York"`

### Build & push the image

You can use either Docker or Cloud Build. Example with Cloud Build (recommended):

```bash
# From repo root (Dockerfile + code present)
gcloud builds submit --tag   "europe-west4-docker.pkg.dev/${PROJECT_ID}/${REPO_NAME}/${IMAGE_NAME}:latest"
```

Terraform outputs `image_uri_hint` to help you get the correct AR path.

### Deploy infra

```bash
cd infra/terraform
terraform init
terraform apply -auto-approve   -var="project_id=${PROJECT_ID}"   -var="region=europe-west4"   -var="bq_location=EU"   -var="dataset_id=mlb"   -var="repo_name=${REPO_NAME}"   -var="image_name=${IMAGE_NAME}"   -var='team_ids=[112]'
```

This creates the Artifact Registry, BQ dataset, service accounts/IAM, Cloud Run Jobs, Cloud Workflow, and Cloud Scheduler jobs.

> Tip: If you change the image, re‑push the tag and **recreate** the Cloud Run Jobs (or update the image reference in Terraform and apply).

---

## Running things

### First local run (JSON‑only, no GCP needed)

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# 1) Ingest to JSON files
python mlb_ingest.py --team 112 --date 2025-08-23

# 2) Build digest from those JSON files (no writes)
python game_digest.py --team 112 --date 2025-08-23 --output json --no_write
```

You’ll see a nicely formatted digest printed to your terminal.

### Local run against BigQuery

Authenticate and set env, then write to BQ:

```bash
gcloud auth application-default login
export BQ_LOCATION=EU
export BQ_PROJECT=${PROJECT_ID}

# 1) Ingest (write to BQ)
python mlb_ingest.py --team 112 --date 2025-08-23 --output bq

# 2) Build digest from BQ and write to mlb.game_digests
python game_digest.py --team 112 --date 2025-08-23
```

### One‑off manual run in Cloud (after Terraform)

You can kick Cloud Run Jobs manually (good for smoke tests):

```bash
gcloud run jobs execute mlb-ingest --region europe-west4 --args="--team,112,--date,2025-08-23"
gcloud run jobs execute mlb-digest --region europe-west4 --args="--team,112,--date,2025-08-23"
```

### Automated daily flow

- At 08:00 **America/New_York**, Cloud Scheduler triggers the Workflow for each `team_id`.
- The Workflow sleeps until each scheduled game’s start, then +90m, polls until **FINAL**, runs **ingest** and then **digest** once per game. fileciteturn1file0

---

## Tables (BigQuery)

Created on first write by the code with minimal schemas:

- `mlb.game_summaries`: one row per game (date, teams, runs, statuses, timestamps).
- `mlb.game_linescore`: inning‑by‑inning (1–15), totals, timestamps.
- `mlb.game_boxscore_players`: per‑player hitting & pitching basic lines.
- `mlb.game_digests`: the final digest row per (team, game).

Set `BQ_*` env vars to change the dataset/table names.

---

## Notes & troubleshooting

- **Regions/locations**: Use a **Scheduler‑supported region** (here default `europe-west1`) and prefer `EU` for `BQ_LOCATION` when using `europe-west4` to avoid cross‑region latency/costs.
- **No FINAL games**: Ingest will log and skip BQ writes if nothing is final yet; that’s expected.
- **Local time**: All “today” logic uses `America/New_York` for baseball date computations.
- **Auth**: Locally, set ADC via `gcloud auth application-default login`. On Cloud Run Jobs the attached service account handles BQ access.
- **Changing teams**: Add IDs to `team_ids` in Terraform to create more Scheduler jobs, or run Jobs/Workflow on demand.

---

## Disclaimer

This uses MLB’s public Stats API (`statsapi.mlb.com`) for personal, educational, non‑commercial use. **No raw MLB data is redistributed**; each user fetches directly from the API. MLB content and data © MLB Advanced Media, L.P.
