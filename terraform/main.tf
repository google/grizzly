/**
 * Copyright 2021 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

data "google_project" "project" {
  project_id = var.gcp_project_id
}

locals {
  gcp_project_id = data.google_project.project.project_id
  gcp_project_number = data.google_project.project.number
}

module "googleapi" {
  source = "./modules/googleapi"
  gcp_project_id = local.gcp_project_id
}

module "bq" {
  source = "./modules/bq"
  gcp_project_id = local.gcp_project_id
}

module "composer" {
  source = "./modules/composer"
  count = local.is_composer_v2 ? 0 : 1
  depends_on = [module.googleapi]
  composer_environment_name = var.composer_environment_name
  composer_image_version = var.composer_image_version
  composer_location = var.composer_location
  composer_node_zone = var.composer_node_zone
  gcp_project_id = local.gcp_project_id
  default_datacatalog_taxonomy_location = var.default_datacatalog_taxonomy_location
}

module "composer_v2" {
  source = "./modules/composer_v2"
  count = local.is_composer_v2 ? 1 : 0
  depends_on = [module.googleapi]
  composer_environment_name = var.composer_environment_name
  composer_image_version = var.composer_image_version
  composer_location = var.composer_location
  composer_node_zone = var.composer_node_zone
  gcp_project_id = local.gcp_project_id
  default_datacatalog_taxonomy_location = var.default_datacatalog_taxonomy_location
}

locals {
  airflow_uri = local.is_composer_v2 ? module.composer_v2[0].airflow_uri : module.composer[0].airflow_uri
  airflow_gcs_bucket = local.is_composer_v2 ? module.composer_v2[0].airflow_gcs_bucket : module.composer[0].airflow_gcs_bucket
  composer_location = local.is_composer_v2 ? module.composer_v2[0].composer_location : module.composer[0].composer_location
  composer_environment_name = local.is_composer_v2 ? module.composer_v2[0].composer_environment_name : module.composer[0].composer_environment_name
}

module "security" {
  source = "./modules/security"
  gcp_project_id = local.gcp_project_id
  gcp_project_number = local.gcp_project_number
  composer_environment_name = local.composer_environment_name
}

output "gcp_project_id" {
  value = local.gcp_project_id
}

output "gcp_project_number" {
  value = local.gcp_project_number
}

output "airflow_uri" {
  value = local.airflow_uri
}

output "airflow_gcs_bucket" {
  value = local.airflow_gcs_bucket
}

output "composer_location" {
  value = local.composer_location
}

output "composer_environment_name" {
  value = local.composer_environment_name
}
