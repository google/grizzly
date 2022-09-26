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
*resource "google_cloudbuild_trigger" "deploy-composer-trigger" {
*  project = var.gcp_project_id
*  name = "deploy-composer"
*  description = "Deploy code to GCP Composer"
*  substitutions = {
*    _DOMAIN = "YOUR_DOMAIN"
*    _ENVIRONMENT = "dev"
*  }
*  trigger_template {
*    branch_name = "dev"
*    repo_name   = var.repository_name
*  }
*  build {
*    step {
*      name = "gcr.io/google.com/cloudsdktool/cloud-sdk"
*      env = [
*        "GRIZZLY_FRAMEWORK_REPO=${var.framework_repository}",
*        "GRIZZLY_FRAMEWORK_PROJECT=${var.gcp_project_id}",
*        "ENVIRONMENT_CONFIG_FILE=/workspace/ENVIRONMENT_CONFIGURATIONS.yml"
*      ]
*      args = [
*                      "bash",
*                      "-c",
*                      <<-EOT
*                        gcloud source repos clone $$GRIZZLY_FRAMEWORK_REPO --project=$$GRIZZLY_FRAMEWORK_PROJECT &&
*                        pip3 install -r /workspace/$$GRIZZLY_FRAMEWORK_REPO/requirements.txt &&
*                        GCP_ENVIRONMENT=$(python3 -c "import yaml;print(yaml.safe_load(open('$$ENVIRONMENT_CONFIG_FILE'))['$_ENVIRONMENT']['GCP_ENVIRONMENT'])") &&
*                        AIRFLOW_ENVIRONMENT=$(python3 -c "import yaml;print(yaml.safe_load(open('$$ENVIRONMENT_CONFIG_FILE'))['$_ENVIRONMENT']['AIRFLOW_ENVIRONMENT'])") &&
*                        AIRFLOW_LOCATION=$(python3 -c "import yaml;print(yaml.safe_load(open('$$ENVIRONMENT_CONFIG_FILE'))['$_ENVIRONMENT']['AIRFLOW_LOCATION'])")  &&
*                        cd /workspace/$$GRIZZLY_FRAMEWORK_REPO/scripts &&
*                        python3 ./deploy_composer.py -p "$$${GCP_ENVIRONMENT}" -l  "$$${AIRFLOW_LOCATION}" -c "$$${AIRFLOW_ENVIRONMENT}" -s /workspace/$_DOMAIN
*                      EOT
*      ]
*    }
*  }
*}
*/