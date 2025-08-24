terraform {
  required_providers {
    google      = { source = "hashicorp/google",      version = "~> 5.34" }
    google-beta = { source = "hashicorp/google-beta", version = "~> 5.34" }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

provider "google-beta" {
  project = var.project_id
  region  = var.region
}

data "google_project" "this" {}

# Enable APIs
resource "google_project_service" "apis" {
  for_each = toset([
    "artifactregistry.googleapis.com",
    "run.googleapis.com",
    "bigquery.googleapis.com",
    "cloudscheduler.googleapis.com",
    "cloudbuild.googleapis.com",
    "workflows.googleapis.com",
    "logging.googleapis.com",
  ])
  project             = var.project_id
  service             = each.key
  disable_on_destroy  = false
}

# Artifact Registry
resource "google_artifact_registry_repository" "repo" {
  location      = var.region
  repository_id = var.repo_name
  format        = "DOCKER"
  depends_on    = [google_project_service.apis]
}

# Cloud Build can push to the repo
resource "google_artifact_registry_repository_iam_member" "cb_writer" {
  location   = var.region
  repository = google_artifact_registry_repository.repo.repository_id
  role       = "roles/artifactregistry.writer"
  member     = "serviceAccount:${data.google_project.this.number}@cloudbuild.gserviceaccount.com"
}

# BigQuery dataset (tables are created by your code)
resource "google_bigquery_dataset" "mlb" {
  dataset_id                 = var.dataset_id
  location                   = var.bq_location
  description                = "MLB ingest dataset"
  delete_contents_on_destroy = false
  depends_on                 = [google_project_service.apis]
}

# Service Accounts
resource "google_service_account" "runner" {
  account_id   = "mlb-runner-sa"
  display_name = "MLB Cloud Run Job SA"
}

resource "google_service_account" "wf" {
  account_id   = "mlb-workflows-sa"
  display_name = "Workflows SA"
}

resource "google_service_account" "scheduler" {
  account_id   = "mlb-scheduler-sa"
  display_name = "Scheduler SA"
}

# IAM for runner SA (pull image + write/query BQ)
resource "google_project_iam_member" "runner_ar_reader" {
  role   = "roles/artifactregistry.reader"
  member = "serviceAccount:${google_service_account.runner.email}"
}

resource "google_project_iam_member" "runner_bq_jobuser" {
  role   = "roles/bigquery.jobUser"
  member = "serviceAccount:${google_service_account.runner.email}"
}

resource "google_project_iam_member" "runner_bq_dataeditor" {
  role   = "roles/bigquery.dataEditor"
  member = "serviceAccount:${google_service_account.runner.email}"
}

# IAM for Workflows SA (to run Cloud Run jobs) + logs
resource "google_project_iam_member" "wf_run_dev" {
  role   = "roles/run.developer"
  member = "serviceAccount:${google_service_account.wf.email}"
}

resource "google_project_iam_member" "wf_logs" {
  role   = "roles/logging.logWriter"
  member = "serviceAccount:${google_service_account.wf.email}"
}

# Workflows (ensure the filename below matches .yaml/.yml in infra/workflows/)
resource "google_workflows_workflow" "orchestrator" {
  name            = "mlb-orchestrator"
  region          = var.region
  description     = "Sleep until start+90m; run ingest every 15m until FINAL; then run digest once."
  service_account = google_service_account.wf.email
  source_contents = file("${path.module}/../workflows/mlb_orchestrator.yaml")
  depends_on      = [google_project_service.apis]
}

# Allow Scheduler SA to invoke the Workflow
resource "google_workflows_workflow_iam_member" "wf_invoker" {
  project  = var.project_id
  region   = var.region
  workflow = google_workflows_workflow.orchestrator.name
  role     = "roles/workflows.invoker"
  member   = "serviceAccount:${google_service_account.scheduler.email}"
}

# Single image used by both jobs
locals {
  image_uri = "${var.region}-docker.pkg.dev/${var.project_id}/${var.repo_name}/${var.image_name}"
}

# Ingest job (default CMD runs mlb_ingest.py)
resource "google_cloud_run_v2_job" "ingest" {
  provider = google-beta
  name     = "mlb-ingest"
  location = var.region

  template {
    template {
      service_account = google_service_account.runner.email

      containers {
        image = local.image_uri
        env { name = "OUTPUT_SINK", value = "bq" }
        env { name = "BQ_LOCATION", value = var.bq_location }
      }

      max_retries = 0
    }
  }

  depends_on = [google_artifact_registry_repository.repo]
}

# Digest job (same image, override command to run game_digest.py)
resource "google_cloud_run_v2_job" "digest" {
  provider = google-beta
  name     = "mlb-digest"
  location = var.region

  template {
    template {
      service_account = google_service_account.runner.email

      containers {
        image   = local.image_uri
        command = ["python", "game_digest.py"]
        env { name = "BQ_LOCATION", value = var.bq_location }
      }

      max_retries = 0
    }
  }

  depends_on = [google_artifact_registry_repository.repo]
}

# Cloud Scheduler jobs (one per team_id) -> invokes Workflow at 8am ET
resource "google_cloud_scheduler_job" "daily_team" {
  for_each  = toset([for t in var.team_ids : tostring(t)])

  name      = "run-mlb-orchestrator-${each.value}"
  region    = var.region
  schedule  = var.scheduler_cron
  time_zone = var.scheduler_tz

  http_target {
    uri         = "https://workflowexecutions.googleapis.com/v1/projects/${var.project_id}/locations/${var.region}/workflows/${google_workflows_workflow.orchestrator.name}/executions"
    http_method = "POST"

    oauth_token {
      service_account_email = google_service_account.scheduler.email
    }

    headers = { "Content-Type" = "application/json" }

    # Body: {"argument":"{\"team_ids\":[112]}"}
    body = base64encode(jsonencode({
      argument = jsonencode({ team_ids = [tonumber(each.value)] })
    }))
  }

  depends_on = [google_workflows_workflow_iam_member.wf_invoker]
}
