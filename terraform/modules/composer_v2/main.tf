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

resource "google_composer_environment" "grizzly_airflow" {
  project = var.gcp_project_id
  name = var.composer_environment_name
  region = var.composer_location

  config {
    environment_size = "ENVIRONMENT_SIZE_LARGE"
    software_config {
      image_version = var.composer_image_version
    }
  }
}

locals {
  airflow_gcs_bucket = replace(google_composer_environment.grizzly_airflow.config[0].dag_gcs_prefix, "/dags", "")
  airflow_uri = google_composer_environment.grizzly_airflow.config[0].airflow_uri
  composer_location = var.composer_location
  composer_environment_name = var.composer_environment_name
}
