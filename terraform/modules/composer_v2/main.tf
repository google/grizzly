/**
 * Copyright 2022 Google LLC
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

resource "google_project_iam_member" "composer_v2_api_service_agent_extension" {
  project = var.gcp_project_id
  role    = "roles/composer.ServiceAgentV2Ext"
  member  = "serviceAccount:service-${var.gcp_project_number}@cloudcomposer-accounts.iam.gserviceaccount.com"
}

resource "google_composer_environment" "grizzly_airflow" {
  project = var.gcp_project_id
  name = var.composer_environment_name
  region = var.composer_location
  depends_on = [
    google_project_iam_member.composer_v2_api_service_agent_extension
  ]

  config {
    environment_size = "ENVIRONMENT_SIZE_MEDIUM"
    software_config {
      image_version = var.composer_image_version
      airflow_config_overrides = local.airflow_config_overrides
      pypi_packages = {
        geopandas = "==0.11.1"
        openpyxl = ""
      }
    }
    workloads_config {
      scheduler {
        cpu = 2
        memory_gb = 7.5
        storage_gb = 5
        count = 2
      }
      web_server {
        cpu = 2
        memory_gb = 4
        storage_gb = 4
      }
      worker {        
        cpu = 2
        memory_gb = 8
        storage_gb = 5
        min_count = 3
        max_count = 25
      }
    }
  }
}

locals {
  airflow_gcs_bucket = replace(google_composer_environment.grizzly_airflow.config[0].dag_gcs_prefix, "/dags", "")
  airflow_uri = google_composer_environment.grizzly_airflow.config[0].airflow_uri
  composer_location = var.composer_location
  composer_environment_name = var.composer_environment_name
}
