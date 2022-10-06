locals {
  grizzly_data_line_docker = "${path.module}/../../../../grizzly_data_lineage"
  grizzly_registry_repo = "us-central1-docker.pkg.dev/${google_artifact_registry_repository.grizzly_repository.project}/${google_artifact_registry_repository.grizzly_repository.name}"
}

resource "null_resource" "build-data-lineage" {
  triggers = {
    timestamp = timestamp()
  }
  provisioner "local-exec" {
    command = "cd ${local.grizzly_data_line_docker} && gcloud builds submit --tag ${local.grizzly_registry_repo}/grizzly-data-lineage ."
  }
  depends_on = [
    google_artifact_registry_repository.grizzly_repository
  ]
}
