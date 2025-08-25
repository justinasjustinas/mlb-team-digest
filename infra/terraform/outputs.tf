output "artifact_registry_repo" { value = google_artifact_registry_repository.repo.repository_id }
output "ingest_job_name"       { value = google_cloud_run_v2_job.ingest.name }
output "digest_job_name"       { value = google_cloud_run_v2_job.digest.name }
output "workflow_name"         { value = google_workflows_workflow.orchestrator.name }
output "scheduler_jobs"        { value = { for k, v in google_cloud_scheduler_job.daily_team : k => v.name } }
output "image_uri_hint"        { value = "${var.region}-docker.pkg.dev/${var.project_id}/${var.repo_name}/${var.image_name}" }
