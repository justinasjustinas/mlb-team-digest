variable "project_id" {
  type = string
}

variable "region" {
  type    = string
  default = "europe-west4"
}

variable "scheduler_region" {
  type        = string
  default     = "europe-west1"  # Cloud Scheduler supported region
  description = "Region for Cloud Scheduler (use `gcloud scheduler locations list` to see what's available)."
}

variable "bq_location" {
  type    = string
  default = "EU"
}

variable "repo_name" {
  type    = string
  default = "mlb-team-digest"
}

variable "image_name" {
  type    = string
  default = "mlb-team-digest:latest"
}

variable "dataset_id" {
  type    = string
  default = "mlb"
}

variable "team_ids" {
  type    = list(number)
  default = [112]
}

variable "scheduler_cron" {
  type    = string
  default = "0 8 * * *"
}

variable "scheduler_tz" {
  type    = string
  default = "America/New_York"
}

# Workload Identity Federation
variable "wif_pool_id" {
  type    = string
  default = "mlb-ci-pool"
}

variable "wif_provider_id" {
  type    = string
  default = "mlb-github"
}

variable "github_repository" {
  type        = string
  description = "GitHub repository in owner/name format for OIDC trust"
  default     = "OWNER/REPO"
}
