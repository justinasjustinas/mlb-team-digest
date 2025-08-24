variable "project_id"     { type = string }
variable "region"         { type = string  default = "europe-west4" }
variable "bq_location"    { type = string  default = "EU" }
variable "repo_name"      { type = string  default = "mlb-team-digest" }
variable "ingest_image"   { type = string  default = "mlb-team-digest:latest" } # tag you push
variable "digest_image"   { type = string  default = "mlb-team-digest:latest" } # same image, different command
variable "dataset_id"     { type = string  default = "mlb" }
variable "team_ids"       { type = list(number) default = [112] }  # add teams here
variable "scheduler_cron" { type = string  default = "0 8 * * *" }
variable "scheduler_tz"   { type = string  default = "America/New_York" }
