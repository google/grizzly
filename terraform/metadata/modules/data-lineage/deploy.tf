locals {
  grizzly_projects_string = "${join(",", [for s in var.managed_gcp_project_list : format("%s", s)])}"
  grizzly_data_line_app_engine = "${path.module}/../../../../grizzly_data_lineage/app-engine"
  grizzly_data_line_app_engine_deploy = "./gcp-app-deploy.sh ${local.grizzly_projects_string} ${local.grizzly_registry_repo}/grizzly-data-lineage ${var.gcp_project_id}"
  grizzly_data_line_app_engine_deploy_default = "${local.grizzly_data_line_app_engine_deploy} default"
  grizzly_data_line_app_engine_deploy_datalineage = "${local.grizzly_data_line_app_engine_deploy} data-lineage"
}

resource "google_app_engine_application" "app" {
  project     = var.gcp_project_id
  location_id = var.location_id

  depends_on = [
    google_artifact_registry_repository.grizzly_repository,
    null_resource.build-data-lineage
  ]
}

resource "null_resource" "deploy-data-lineage" {
  triggers = {
    timestamp = timestamp()
  }
  provisioner "local-exec" {
    command = "chmod -R 777 ${local.grizzly_data_line_app_engine} && cd ${local.grizzly_data_line_app_engine} && ${local.grizzly_data_line_app_engine_deploy_default} && ${local.grizzly_data_line_app_engine_deploy_datalineage}"
  }

  depends_on = [
    google_artifact_registry_repository.grizzly_repository,
    null_resource.build-data-lineage,
    google_app_engine_application.app
  ]
}