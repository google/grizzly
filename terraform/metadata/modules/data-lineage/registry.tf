resource "google_artifact_registry_repository" "grizzly_repository" {
  location      = "us-central1"
  repository_id = "grizzly-repository"
  description   = "Grizzlly reposotiry"
  format        = "DOCKER"
  project       = var.gcp_project_id
}