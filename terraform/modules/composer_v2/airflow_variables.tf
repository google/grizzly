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
  create_airflow_variable_cmd = "gcloud beta composer environments run ${var.composer_environment_name} --location ${var.composer_location} --project ${var.gcp_project_id} variables -- set "
}

resource "null_resource" "setup_airflow_variables" {
  for_each = local.airflow_variables
  triggers = {
    airflow_variable_ids = "${each.key} ${each.value}"
  }
  provisioner "local-exec" {
    command = "${local.create_airflow_variable_cmd} ${each.key} '${each.value}'"
  }
  depends_on = [
    google_composer_environment.grizzly_airflow
  ]
}
