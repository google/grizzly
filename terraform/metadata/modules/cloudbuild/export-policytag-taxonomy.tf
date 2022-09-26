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

resource "google_cloudbuild_trigger" "export-policytag-taxonomy-trigger" {
  project = var.gcp_project_id
  name = "export-policytag-taxonomy"
  description = "Export DataCatalog Policy Tag taxonomy templates to GIT."
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
        "GRIZZLY_REPO=${var.repository_name}",
        "GRIZZLY_FRAMEWORK_PROJECT=${var.gcp_project_id}",
        "ENVIRONMENT_CONFIG_FILE=/workspace/ENVIRONMENT_CONFIGURATIONS.yml",
        "TEMPLATE_FOLDER=data-calalog-policytag-taxonomy-templates"
      ]
      args = [
        "bash",
        "-c",
        <<-EOT
            cd /tmp &&
            gcloud source repos clone $$GRIZZLY_FRAMEWORK_REPO --project=$$GRIZZLY_FRAMEWORK_PROJECT &&
            cd ./$$GRIZZLY_FRAMEWORK_REPO &&
            git checkout -b main origin/main &&
            pip3 install -r /tmp/$$GRIZZLY_FRAMEWORK_REPO/requirements.txt &&
            cd /tmp &&
            gcloud source repos clone $$GRIZZLY_REPO --project=$$GRIZZLY_FRAMEWORK_PROJECT &&
            cd /tmp/$$GRIZZLY_REPO &&
            git checkout -b $BRANCH_NAME --track remotes/origin/$BRANCH_NAME &&
            GCP_ENVIRONMENT=$(python3 -c "import yaml;print(yaml.safe_load(open('$$ENVIRONMENT_CONFIG_FILE'))['$BRANCH_NAME']['GCP_ENVIRONMENT'])") &&
            AIRFLOW_LOCATION=$(python3 -c "import yaml;print(yaml.safe_load(open('$$ENVIRONMENT_CONFIG_FILE'))['$BRANCH_NAME']['AIRFLOW_LOCATION'])") &&
            echo "Working with gcp_project_id=[$$${GCP_ENVIRONMENT}] location=[$$${AIRFLOW_LOCATION}]" &&
            cd /tmp/$$GRIZZLY_FRAMEWORK_REPO/scripts &&
            python3 ./export_datacatalog_policytag_taxonomy.py -p "$$${GCP_ENVIRONMENT}" -l "$$${AIRFLOW_LOCATION}" -s /tmp/$$GRIZZLY_REPO/$$TEMPLATE_FOLDER &&
            cd /tmp/$$GRIZZLY_REPO/$$TEMPLATE_FOLDER &&
            git config --global user.name "Cloud build" &&
            CURRENT_USER=`gcloud auth list --filter=status:ACTIVE --format="value(account)"` &&
            git config --global user.email "$$${CURRENT_USER}" &&
            git add . &&
            git status &&
            git commit -m "Export DataCatalog Policy Tags taxonomy to repo." &&
            git push
        EOT
      ]
    }
  }
}

