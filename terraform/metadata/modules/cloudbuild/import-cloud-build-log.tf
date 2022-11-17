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

resource "google_cloudbuild_trigger" "import-cloud-build-log-trigger" {
  project = var.gcp_project_id
  name = "import-cloud-build-log"
  description = "Import Cloud Build trigger log into ETL log tables"
  substitutions = {
    _ENVIRONMENT = "dev"
  }
  source_to_build {
    uri = "https://source.developers.google.com/p/${var.gcp_project_id}/r/${var.repository_name}"
    ref = "refs/heads/dev"
    repo_type = "CLOUD_SOURCE_REPOSITORIES"
  }
  approval_config {
    approval_required = false
  }
  build {
    step {
      name = "gcr.io/google.com/cloudsdktool/cloud-sdk"
      env = [
        "GRIZZLY_FRAMEWORK_REPO=${var.framework_repository}",
        "GRIZZLY_FRAMEWORK_PROJECT=${var.gcp_project_id}",
        "ENVIRONMENT_CONFIG_FILE=/workspace/ENVIRONMENT_CONFIGURATIONS.yml"
      ]
      args = [
                      "bash",
                      "-c",
                      <<-EOT
                        gcloud source repos clone $$GRIZZLY_FRAMEWORK_REPO --project=$$GRIZZLY_FRAMEWORK_PROJECT &&
                        cd /workspace/$$GRIZZLY_FRAMEWORK_REPO &&
                        git checkout -b main origin/main &&
                        git pull &&
                        pip3 install -r /workspace/$$GRIZZLY_FRAMEWORK_REPO/requirements.txt &&
                        cd /workspace/$$GRIZZLY_FRAMEWORK_REPO/scripts &&
                        GCP_ENVIRONMENT=$(python3 -c "import yaml;print(yaml.safe_load(open('$$ENVIRONMENT_CONFIG_FILE'))['$_ENVIRONMENT']['GCP_ENVIRONMENT'])") &&
                        AIRFLOW_ENVIRONMENT=$(python3 -c "import yaml;print(yaml.safe_load(open('$$ENVIRONMENT_CONFIG_FILE'))['$_ENVIRONMENT']['AIRFLOW_ENVIRONMENT'])") &&
                        AIRFLOW_LOCATION=$(python3 -c "import yaml;print(yaml.safe_load(open('$$ENVIRONMENT_CONFIG_FILE'))['$_ENVIRONMENT']['AIRFLOW_LOCATION'])") &&
                        cd /workspace/$$GRIZZLY_FRAMEWORK_REPO/scripts &&
                        python3 ./import_cloud_build_log.py -e "$_ENVIRONMENT" -m "$$GRIZZLY_FRAMEWORK_PROJECT" -c "$$ENVIRONMENT_CONFIG_FILE" -f "/workspace/$$GRIZZLY_REPO" &&
                        python3 ./run_deploy_data_lineage_trigger.py -e "$_ENVIRONMENT" -m "$$GRIZZLY_FRAMEWORK_PROJECT"
                      EOT
      ]
    }
  }
}
