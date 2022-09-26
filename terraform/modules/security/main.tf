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
  iam_roles = [
    # Editor
    {
      role: "roles/editor"
      members: [
        "serviceAccount:${var.gcp_project_number}-compute@developer.gserviceaccount.com"
      ]
    },
    # BigQuery Data Owner
    {
      role: "roles/bigquery.dataOwner"
      members: [
        "serviceAccount:${var.gcp_project_number}-compute@developer.gserviceaccount.com"
      ]
    },
    # Fine-Grained Reader
    {
      role: "roles/datacatalog.categoryFineGrainedReader"
      members: [
        "serviceAccount:${var.gcp_project_number}-compute@developer.gserviceaccount.com"
      ]
    },
    # Data Catalog Tag Editor
    {
      role: "roles/datacatalog.tagEditor"
      members: [
        "serviceAccount:${var.gcp_project_number}-compute@developer.gserviceaccount.com"
      ]
    },
    # Data Catalog TagTemplate User
    {
      role: "roles/datacatalog.tagTemplateUser"
      members: [
        "serviceAccount:${var.gcp_project_number}-compute@developer.gserviceaccount.com"
      ]
    },
    # Cloud Composer v2 API Service Agent Extension
    {
      role: "roles/composer.ServiceAgentV2Ext"
      members: [
        "serviceAccount:service-${var.gcp_project_number}@cloudcomposer-accounts.iam.gserviceaccount.com"
      ]
    },
  ]
}

locals {
  # flatten ensures that this local value is a flat list of objects, rather
  # than a dict of list of objects.
  iam_mapping = flatten([
    for role_members in local.iam_roles : [
      for principal in role_members.members : {
        role = role_members.role
        principal = principal
      }
    ]
  ])
}

resource "google_project_iam_member" "project" {
  for_each = {
    for mapping in local.iam_mapping : "${mapping.role}.${mapping.principal}" => mapping
  }
  project = var.gcp_project_id
  role    = each.value.role
  member  = each.value.principal
}
