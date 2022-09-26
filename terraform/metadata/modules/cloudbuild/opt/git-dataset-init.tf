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

/**
*resource "google_cloudbuild_trigger" "git-dataset-init-trigger" {
*  project = var.gcp_project_id
*  name = "git-dataset-init"
*  description = "Initializing gcp git-metadata grizzly objects"
*  substitutions = {
*    _ENVIRONMENT = "_ENVIRONMENT"
*  }
*  trigger_template {
*    branch_name = ".*"
*    repo_name   = var.framework_repository
*  }
*  build {
*    step {
*      name = "gcr.io/google.com/cloudsdktool/cloud-sdk"
*      env = [
*        "GRIZZLY_REPO=${var.repository_name}",
*        "GRIZZLY_FRAMEWORK_REPO=${var.framework_repository}",
*        "GRIZZLY_FRAMEWORK_PROJECT=${var.gcp_project_id}",
*        "ENVIRONMENT_CONFIG_FILE=/workspace/grizzly/ENVIRONMENT_CONFIGURATIONS.yml"
*      ]
*      args = [
*                      "bash",
*                      "-c",
*                      <<-EOT
*                          gcloud source repos clone $$GRIZZLY_FRAMEWORK_REPO
*                          --project=$$GRIZZLY_FRAMEWORK_PROJECT &&
*                          pip3 install -r /workspace/$$GRIZZLY_FRAMEWORK_REPO/requirements.txt &&
*                          gcloud source repos clone $$GRIZZLY_REPO
*                          --project=$$GRIZZLY_FRAMEWORK_PROJECT && cd /workspace/$$GRIZZLY_REPO &&
*                          git checkout -b $_ENVIRONMENT --track remotes/origin/$_ENVIRONMENT &&
*                          cd /workspace/scripts &&
*                          python3 ./git_dataset_init.py -e $_ENVIRONMENT -c
*                          $$ENVIRONMENT_CONFIG_FILE
*                        EOT
*      ]
*    }
*  }
*}
*/
