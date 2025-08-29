# Infrastructure & IAM Overview

> Single-source-of-truth for what exists, which identities run what, and the **least‑privilege** IAM needed for CI/CD and runtime.

---

## 1) Components at a glance

```
┌────────────────────┐
│ GitHub Actions CI  │
└────────┬───────────┘
         │
         ▼
┌────────────────────┐
│ Artifact Registry  │   (step 1: build & push)
└────────┬───────────┘
         │
         ▼
┌────────────────────┐
│ Cloud Run Jobs     │   (step 2: rollout new image)
│  - mlb-ingest      │
│  - mlb-digest      │
└────────┬───────────┘
         │ invoked by
         ▼
┌────────────────────┐
│ Cloud Workflow     │   (triggered daily by Scheduler)
│  mlb-orchestrator  │
└────────┬───────────┘
         │
         ▼
┌────────────────────┐
│ BigQuery dataset   │
│        mlb         │
└────────────────────┘
```

Cloud Scheduler triggers the Workflow on a schedule:  
**Cron:** `0 8 * * *` (08:00 AM daily, America/New_York)  
**Region:** `europe-west1`

---

## 2) Service accounts (who runs what)

| Service Account   | Purpose                                                    | Used by / Attached to                             |
| ----------------- | ---------------------------------------------------------- | ------------------------------------------------- |
| **CI SA**         | Federated identity for CI via WIF; builds, pushes, deploys | GitHub Actions workflows (build, rollout, deploy) |
| **Workflow SA**   | Runtime identity for Cloud Workflow                        | Workflow `mlb-orchestrator`                       |
| **Ingest Job SA** | Runtime identity for job writing to BQ                     | Cloud Run Job `mlb-ingest`                        |
| **Digest Job SA** | Runtime identity for job reading from BQ                   | Cloud Run Job `mlb-digest`                        |
| **Runner Job SA** | Shared runtime SA if jobs are deployed with a common SA    | Cloud Run Jobs (as configured)                    |
| **Scheduler SA**  | Identity for Cloud Scheduler                               | Cloud Scheduler trigger                           |

---

## 3) IAM matrix (least-privilege)

### 3.1 Project-level roles

| Principal   | Roles                                              | Why                                            |
| ----------- | -------------------------------------------------- | ---------------------------------------------- |
| CI SA       | `roles/run.admin`, `roles/workflows.admin`         | Update Cloud Run jobs, deploy/update Workflows |
|             | `roles/artifactregistry.writer` _(scoped to repo)_ | Push images to Artifact Registry               |
| Workflow SA | _(optional)_ `roles/logging.logWriter`             | Allow Workflow to write logs                   |

---

### 3.2 Per-resource roles

| Target / Resource           | Member       | Role                                                              | Why                                                                   |
| --------------------------- | ------------ | ----------------------------------------------------------------- | --------------------------------------------------------------------- |
| Workflow resource           | Scheduler SA | `roles/workflows.invoker`                                         | Scheduler can trigger the workflow                                    |
| Cloud Run Job `mlb-ingest`  | Workflow SA  | `roles/runJobRunnerWithOverrides` _(custom role)_                 | Workflow calls `jobs.run` **with overrides**                          |
| Cloud Run Job `mlb-digest`  | Workflow SA  | `roles/runJobRunnerWithOverrides` _(custom role)_                 | Same as above                                                         |
| Job Runtime SAs             | CI SA        | `roles/iam.serviceAccountUser`                                    | CI can deploy/update jobs using these runtime SAs                     |
| Workflow SA                 | CI SA        | `roles/iam.serviceAccountUser`                                    | CI can set Workflow’s runtime SA during deploy                        |
| Job Runtime SA _(optional)_ | Workflow SA  | `roles/iam.serviceAccountUser` _(only if overridden at run time)_ | Needed only if workflow overrides the job’s runtime SA in run request |
| Artifact Registry repo      | CI SA        | `roles/artifactregistry.writer` _(scoped to repo)_                | Push images from CI                                                   |

> Instead of `roles/run.admin`, we defined a custom role `runJobRunnerWithOverrides` for the Workflow SA with only:
>
> - `run.jobs.get`
> - `run.jobs.run`
> - `run.jobs.runWithOverrides`
> - `run.executions.get`
> - `run.executions.list`
> - `run.operations.get`
> - `run.operations.list`

---

### 3.3 BigQuery dataset-level IAM

| Dataset | Member        | Role                        | Why                           |
| ------- | ------------- | --------------------------- | ----------------------------- |
| `mlb`   | Ingest Job SA | `roles/bigquery.dataEditor` | Write game data               |
| `mlb`   | Digest Job SA | `roles/bigquery.dataViewer` | Read data to assemble digests |

---

### 3.4 Workload Identity Federation (for GitHub)

- **Provider (WIF_PROVIDER)**: workload identity provider configured for GitHub repo
- **Trust binding on CI SA**: `roles/iam.workloadIdentityUser` with principalSet filter for repo

```bash
PROJECT_NUMBER="<your-project-number>"
POOL_ID="<your-pool>"
PROVIDER_ID="<your-provider>"
REPO="<owner>/<repo>"   # e.g. justinas/mlb-team-digest

gcloud iam service-accounts add-iam-policy-binding \
  CI-SA \
  --role="roles/iam.workloadIdentityUser" \
  --member="principalSet://iam.googleapis.com/projects/${PROJECT_NUMBER}/locations/global/workloadIdentityPools/${POOL_ID}/attribute.repository/${REPO}"


---

## 4) Workflow logic (mlb-orchestrator)

- Compute target date (ET).
- Fetch team schedule; free large JSON immediately.
- For each game: sleep until start → +90 minutes → poll live feed every 15 minutes (max 12h).
- On `FINAL`: run `mlb-ingest` → **sleep 180s** → run `mlb-digest`.
- Memory‑safe assignments: null large vars after use.

---

## 5) GitHub Actions (CI/CD)

**Secrets / Vars**

- **Secrets**: `WIF_PROVIDER`, `WIF_SERVICE_ACCOUNT`
- **Variables**: `PROJECT_ID`, `REGION`, `REPO_NAME`, `IMAGE_NAME`

**Jobs**

1. **build-and-push**

   - Auth via OIDC → build with Buildx → push to Artifact Registry
   - Tags: `edge` on main; semver (`vX.Y.Z`) on tags

2. **rollout**

   - `gcloud run jobs update mlb-ingest --image ...`
   - `gcloud run jobs update mlb-digest --image ...`
   - Requires CI SA → `run.admin` + `serviceAccountUser` on job SAs

3. **deploy-cloud-workflows** (only if workflow YAML changed)
   - `gcloud workflows deploy mlb-orchestrator --source infra/workflows/mlb_orchestrator.yaml --service-account Workflow SA`

---

## 6) Common errors & fixes

| Error                              | Root cause                                   | Fix                           |
| ---------------------------------- | -------------------------------------------- | ----------------------------- |
| `iam.serviceaccounts.actAs denied` | CI SA missing `serviceAccountUser` on job SA | Grant that role               |
| `Memory usage limit exceeded`      | Large JSON held across loops                 | Null large vars immediately   |
| Digest ran before Ingest           | Parallel steps                               | Chain and add **180s** buffer |

---

## 7) Change management flow

1. PR merged to `main` → GitHub Action runs.
2. **build-and-push** pushes `:edge`.
3. **rollout** updates `mlb-ingest` and `mlb-digest`.
4. If workflow YAML changed → **deploy-cloud-workflows** redeploys `mlb-orchestrator`.
5. Cloud Scheduler triggers Workflow daily at 08:00 ET.

---

## 8) Permissions matrix (visual)

```

[ GitHub Actions CI SA ]
| artifactregistry.writer (repo-scoped)
| run.admin
| workflows.admin
| iam.serviceAccountUser on ---> [ Workflow SA ]
| iam.serviceAccountUser on ---> [ Ingest Job SA ]
| iam.serviceAccountUser on ---> [ Digest Job SA ]
|
+--> builds/pushes images ---> [ Artifact Registry ]
+--> updates/deploys -------> [ Cloud Run Jobs ] (mlb-ingest, mlb-digest)
+--> deploys/updates -------> [ Cloud Workflow ] (mlb-orchestrator)

[ Workflow SA ]
| (optional) logging.logWriter
| roles/runJobRunnerWithOverrides on:
| - Cloud Run Job: mlb-ingest
| - Cloud Run Job: mlb-digest
| (custom role includes: run.jobs.get, run.jobs.run, run.jobs.runWithOverrides,
| run.executions.get, run.executions.list,
| run.operations.get [ + optional run.operations.list ])
|
+--> runs jobs (with overrides) ---> [ Cloud Run Jobs ]
| (only if overriding runtime SA:)
| iam.serviceAccountUser on -------> [ Ingest/Digest Job Runtime SA ]

[ Ingest Job SA ]
| bigquery.dataEditor on dataset: mlb
+--> writes -----------------------> [ BigQuery dataset: mlb ]

[ Digest Job SA ]
| bigquery.dataViewer on dataset: mlb
+--> reads ------------------------> [ BigQuery dataset: mlb ]

[ Cloud Scheduler SA ] (if used)
| workflows.invoker
+--> triggers ---------------------> [ Cloud Workflow ]

```

---

## Note on Workflow Schedule

- The **mlb-orchestrator** workflow is triggered automatically via **Cloud Scheduler**.
- **Cron:** `0 8 * * *` (08:00 AM daily, America/New_York)
- **Region:** `europe-west1`
```
