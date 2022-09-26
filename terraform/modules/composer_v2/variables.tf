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

variable "gcp_project_id" {
  type = string
  description = "The ID of the GCP project."
}

variable "composer_environment_name" {
  type    = string
  description = "Name of the GCP Composer/Airflow environment."
}

variable "composer_image_version" {
  type    = string
  description = "Cloud Composer image with the required version of Airflow."
}

variable "composer_location" {
  type    = string
  description = "Compute Engine region where the Composer environment's GKE cluster is located."
}

variable "composer_node_zone" {
  type    = string
  description = "The Compute Engine zone in which to deploy the VMs running the Apache Airflow software."
}

#### Default parameters

variable "composer_node_count" {
  type    = string
  description = "The number of nodes in the Kubernetes Engine cluster that will be used to run your environment."
  default = 25
}

variable "composer_machine_type" {
  type    = string
  description = "The Compute Engine machine type used for cluster instances."
  default = "e2-highmem-2"
}

variable "default_datacatalog_taxonomy_location" {
  type    = string
  description = "Default loaction for search of DataCatalog Taxonomies. (us, europe, us-central1, etc.)"
}

variable "composer_oauth_scopes" {
  type    = list(string)
  description = "The set of Google API scopes to be made available on all node VMs."
  default = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/cloud-platform",
    "https://www.googleapis.com/auth/drive"
  ]
}

locals {
  airflow_variables = {
    ENVIRONMENT: "N/A",
    ETL_LOG_TABLE: "etl_log.composer_job_details",
    ETL_STAGE_DATASET: "etl_staging_composer",
    FORCE_OFF_HX_LOADING: "N",
    GCP_PROJECT_ID: var.gcp_project_id,
    HISTORY_TABLE_CONFIG: "{\n  \"dataset_id\": \"{{ target_dataset_id }}_hx\",\n  \"default_history_expiration\": 180,\n  \"table_id\": \"{{ target_table_id }}_hx\"\n}",
    # projects/{Config.GCP_PROJECT_ID}/locations/us/taxonomies
    DEFAULT_DATACATALOG_TAXONOMY_LOCATION: "projects/${var.gcp_project_id}/locations/${var.default_datacatalog_taxonomy_location}/taxonomies"
  }
}
