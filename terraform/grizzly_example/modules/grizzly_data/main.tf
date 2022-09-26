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

# Get configuration of target GCP composer environment
data "google_composer_environment" "grizzly_airflow" {
    name = var.composer_environment_name
    project = var.gcp_project_id
    region = var.composer_location
}

locals {
  airflow_gcs_bucket = replace(data.google_composer_environment.grizzly_airflow.config[0].dag_gcs_prefix, "/dags", "")
  airflow_uri = data.google_composer_environment.grizzly_airflow.config[0].airflow_uri
}

resource "null_resource" "setup_grizzly_example_data" {
  provisioner "local-exec" {
    command = "gsutil -m cp -r ../../grizzly_example/gis_examples/building-footprint-usa* ${local.airflow_gcs_bucket}/data/imports/building-footprint-usa/"
  }
  depends_on = [
    data.google_composer_environment.grizzly_airflow
  ]
}