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

locals {
  airflow_plugin_folders = [ "grizzly", "operators", "templates"]
}

resource "null_resource" "setup_airflow_plugin" {
  for_each = toset(local.airflow_plugin_folders)
  triggers = {
    airflow_variable_ids = "airflow/plugins/${each.key}"
  }
  provisioner "local-exec" {
    command = "gsutil cp -R ../airflow/plugins/${each.key}/ ${local.airflow_gcs_bucket}/plugins/"
  }
  depends_on = [
    google_composer_environment.grizzly_airflow
  ]
}