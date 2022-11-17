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
