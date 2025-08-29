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
  project            = var.project_id
  service            = each.key
  disable_on_destroy = false
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
  access {
    iam_member = "serviceAccount:${google_service_account.ingest.email}"
    role       = "roles/bigquery.dataEditor"
  }
  access {
    iam_member = "serviceAccount:${google_service_account.digest.email}"
    role       = "roles/bigquery.dataViewer"
  }
  depends_on = [google_project_service.apis]
}

# Service Accounts
resource "google_service_account" "ingest" {
  account_id   = "mlb-ingest-sa"
  display_name = "Ingest Job SA"
}

resource "google_service_account" "digest" {
  account_id   = "mlb-digest-sa"
  display_name = "Digest Job SA"
}

resource "google_service_account" "wf" {
  account_id   = "mlb-workflows-sa"
  display_name = "Workflows SA"
}

resource "google_service_account" "scheduler" {
  account_id   = "mlb-scheduler-sa"
  display_name = "Scheduler SA"
}

resource "google_service_account" "ci" {
  account_id   = "mlb-ci-sa"
  display_name = "CI Service Account"
}

# IAM for job runtime SAs
resource "google_project_iam_member" "ingest_ar_reader" {
  project = var.project_id
  role    = "roles/artifactregistry.reader"
  member  = "serviceAccount:${google_service_account.ingest.email}"
}
resource "google_project_iam_member" "ingest_bq_jobuser" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.ingest.email}"
}
resource "google_project_iam_member" "digest_ar_reader" {
  project = var.project_id
  role    = "roles/artifactregistry.reader"
  member  = "serviceAccount:${google_service_account.digest.email}"
}
resource "google_project_iam_member" "digest_bq_jobuser" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.digest.email}"
}

resource "google_workflows_workflow" "orchestrator" {
  name            = "mlb-orchestrator"
  region          = var.region
  description     = "Sleep until start+90m; run ingest every 15m until FINAL; then run digest once."
  service_account = google_service_account.wf.email

  # Make sure this path/extension matches your file on disk:
  #   infra/workflows/mlb_orchestrator.yaml
  source_contents = file("${path.module}/../workflows/mlb_orchestrator.yaml")

  depends_on = [google_project_service.apis]
}


# IAM for Workflows SA (run Cloud Run) + logs
resource "google_project_iam_custom_role" "run_job_runner_with_overrides" {
  role_id     = "runJobRunnerWithOverrides"
  title       = "Run Job Runner With Overrides"
  description = "Minimal permissions to run Cloud Run jobs with overrides"
  permissions = [
    "run.jobs.get",
    "run.jobs.run",
    "run.jobs.runWithOverrides",
    "run.executions.get",
    "run.executions.list",
    "run.operations.get",
    "run.operations.list",
  ]
}

resource "google_project_iam_member" "wf_run_job_runner" {
  project = var.project_id
  role    = google_project_iam_custom_role.run_job_runner_with_overrides.name
  member  = "serviceAccount:${google_service_account.wf.email}"
}
resource "google_project_iam_member" "wf_logs" {
  project = var.project_id
  role    = "roles/logging.logWriter"
  member  = "serviceAccount:${google_service_account.wf.email}"
}

# Scheduler SA can invoke Workflows (project-level)
resource "google_project_iam_member" "scheduler_workflows_invoker" {
  project = var.project_id
  role    = "roles/workflows.invoker"
  member  = "serviceAccount:${google_service_account.scheduler.email}"
}

resource "google_service_account_iam_member" "scheduler_token_creator" {
  service_account_id = google_service_account.scheduler.name
  role               = "roles/iam.serviceAccountTokenCreator"
  member             = "serviceAccount:service-${data.google_project.this.number}@gcp-sa-cloudscheduler.iam.gserviceaccount.com"
}

# CI service account roles
resource "google_project_iam_member" "ci_run_admin" {
  project = var.project_id
  role    = "roles/run.admin"
  member  = "serviceAccount:${google_service_account.ci.email}"
}

resource "google_project_iam_member" "ci_workflows_admin" {
  project = var.project_id
  role    = "roles/workflows.admin"
  member  = "serviceAccount:${google_service_account.ci.email}"
}

resource "google_artifact_registry_repository_iam_member" "ci_writer" {
  location   = var.region
  repository = google_artifact_registry_repository.repo.repository_id
  role       = "roles/artifactregistry.writer"
  member     = "serviceAccount:${google_service_account.ci.email}"
}

# CI can impersonate runtime service accounts
resource "google_service_account_iam_member" "ci_wf_sa_user" {
  service_account_id = google_service_account.wf.name
  role               = "roles/iam.serviceAccountUser"
  member             = "serviceAccount:${google_service_account.ci.email}"
}

resource "google_service_account_iam_member" "ci_ingest_sa_user" {
  service_account_id = google_service_account.ingest.name
  role               = "roles/iam.serviceAccountUser"
  member             = "serviceAccount:${google_service_account.ci.email}"
}

resource "google_service_account_iam_member" "ci_digest_sa_user" {
  service_account_id = google_service_account.digest.name
  role               = "roles/iam.serviceAccountUser"
  member             = "serviceAccount:${google_service_account.ci.email}"
}

# Workload Identity Federation for GitHub
resource "google_iam_workload_identity_pool" "ci_pool" {
  provider                  = google-beta
  workload_identity_pool_id = var.wif_pool_id
  display_name              = "CI Pool"
}

resource "google_iam_workload_identity_pool_provider" "github" {
  provider                         = google-beta
  workload_identity_pool_id        = google_iam_workload_identity_pool.ci_pool.workload_identity_pool_id
  workload_identity_pool_provider_id = var.wif_provider_id
  display_name                     = "GitHub Provider"
  oidc {
    issuer_uri = "https://token.actions.githubusercontent.com"
  }
  attribute_mapping = {
    "google.subject"      = "assertion.sub"
    "attribute.repository" = "assertion.repository"
  }
}

resource "google_service_account_iam_member" "ci_wif_binding" {
  service_account_id = google_service_account.ci.name
  role               = "roles/iam.workloadIdentityUser"
  member             = "principalSet://iam.googleapis.com/projects/${data.google_project.this.number}/locations/global/workloadIdentityPools/${google_iam_workload_identity_pool.ci_pool.workload_identity_pool_id}/attribute.repository/${var.github_repository}"
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
      service_account = google_service_account.ingest.email

      containers {
        image = local.image_uri

        env {
          name  = "OUTPUT_SINK"
          value = "bq"
        }

        env {
          name  = "BQ_LOCATION"
          value = var.bq_location
        }
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
      service_account = google_service_account.digest.email

      containers {
        image   = local.image_uri
        command = ["python", "game_digest.py"]

        env {
          name  = "BQ_LOCATION"
          value = var.bq_location
        }
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
  region    = var.scheduler_region
  schedule  = var.scheduler_cron
  time_zone = var.scheduler_tz

  http_target {
    uri         = "https://workflowexecutions.googleapis.com/v1/projects/${var.project_id}/locations/${var.region}/workflows/${google_workflows_workflow.orchestrator.name}/executions"
    http_method = "POST"
    oauth_token { service_account_email = google_service_account.scheduler.email }
    headers = { "Content-Type" = "application/json" }
    body = base64encode(jsonencode({
    argument = jsonencode({ team_id = tonumber(each.value) })
  }))
  }

  depends_on = [
    google_project_iam_member.scheduler_workflows_invoker,
    google_workflows_workflow.orchestrator
  ]
}
