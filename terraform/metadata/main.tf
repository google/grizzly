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

data "google_project" "project" {
  project_id = var.gcp_project_id
}

locals {
  gcp_project_id = data.google_project.project.project_id
  gcp_project_number = data.google_project.project.number
}

module "googleapi" {
  source = "./modules/googleapi"
  gcp_project_id = local.gcp_project_id
}

module "security" {
  source = "./modules/security"
  depends_on = [module.googleapi]
  gcp_project_id = local.gcp_project_id
  gcp_project_number = local.gcp_project_number
  managed_gcp_project_list = var.managed_gcp_project_list
}

module "cloudbuild" {
  source = "./modules/cloudbuild"
  depends_on = [module.security]
  gcp_project_id = local.gcp_project_id
  repository_name = var.repository_name
}

output "gcp_project_id" {
  value = local.gcp_project_id
}

output "gcp_project_number" {
  value = local.gcp_project_number
}
