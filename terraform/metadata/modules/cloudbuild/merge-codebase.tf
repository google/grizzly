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

resource "google_cloudbuild_trigger" "merge-codebase-trigger" {
  project = var.gcp_project_id
  name = "merge-codebase"
  description = "Merge codebase between branches"
  substitutions = {
    _DOMAIN = "your_domain"
    _TARGET_BRANCH = "uat"
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
        "GRIZZLY_REPO=${var.repository_name}",
        "GRIZZLY_PROJECT=${var.gcp_project_id}",
      ]
      args = [
                      "bash",
                      "-c",
                      <<-EOT
                        apt-get -y install rsync &&
                        grizzly_tmp_dir=$(mktemp -d -t grizzly-XXXXXXXXXX) &&
                        cd "$$${grizzly_tmp_dir}" &&
                        gcloud source repos clone $$GRIZZLY_REPO . --project=$$GRIZZLY_PROJECT &&
                        git checkout -b $_TARGET_BRANCH origin/$_TARGET_BRANCH &&
                        git pull &&
                        echo "Merging domain [$_DOMAIN] from [$BRANCH_NAME] to [$_TARGET_BRANCH]"
                        rsync -r -p -t --delete --force /workspace/$_DOMAIN/ ./$_DOMAIN &&
                        git config --global user.name "Cloud build" &&
                        CURRENT_USER=`gcloud auth list --filter=status:ACTIVE --format="value(account)"` &&
                        git config --global user.email "$$${CURRENT_USER}" &&
                        git add . &&
                        git commit -m "Code merge domain [$_DOMAIN] from [$BRANCH_NAME] to [$_TARGET_BRANCH]" &&
                        git push
                      EOT
      ]
    }
  }
}
