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
  iam_metadata_roles = [
    # Cloud Build Service Agent
    {
      role: "roles/cloudbuild.serviceAgent"
      members: [
        "serviceAccount:service-${var.gcp_project_number}@gcp-sa-cloudbuild.iam.gserviceaccount.com"
      ]
    },
    # Source Repository Writer
    {
      role: "roles/source.writer"
      members: [
        "serviceAccount:${var.gcp_project_number}@cloudbuild.gserviceaccount.com"
      ]
    },
    # Kubernetes Engine Admin
    {
      role: "roles/container.admin"
      members: [
        "serviceAccount:${var.gcp_project_number}@cloudbuild.gserviceaccount.com"
      ]
    },
    # Container Registry Service Agent
    {
      role: "roles/containerregistry.ServiceAgent"
      members: [
        "serviceAccount:service-${var.gcp_project_number}@containerregistry.iam.gserviceaccount.com"
      ]
    },
    # Editor
    {
      role: "roles/owner"
      members: [
        "serviceAccount:${var.gcp_project_number}@cloudservices.gserviceaccount.com"
      ]
    },
    # Storage Object Admin
    {
      role: "roles/storage.objectAdmin"
      members: [
        "serviceAccount:${var.gcp_project_number}@cloudbuild.gserviceaccount.com"
      ]
    },
    # DLP Administrator
    {
      role: "roles/dlp.admin"
      members: [
        "serviceAccount:${var.gcp_project_number}@cloudbuild.gserviceaccount.com"
      ]
    },
    # Service Account User
    {
      role: "roles/iam.serviceAccountUser"
      members: [
        "serviceAccount:${var.gcp_project_number}@cloudbuild.gserviceaccount.com"
      ]
    },
    # Data Catalog TagTemplate Owner
    {
      role: "roles/datacatalog.tagTemplateOwner"
      members: [
        "serviceAccount:${var.gcp_project_number}@cloudbuild.gserviceaccount.com"
      ]
    },
    # Data Catalog Policy Tag Admin
    {
      role: "roles/datacatalog.categoryAdmin"
      members: [
        "serviceAccount:${var.gcp_project_number}@cloudbuild.gserviceaccount.com"
      ]
    },
  ]
}

# grant access to build service account on all managed projects
locals {
  iam_managed_roles = [
    # BigQuery Admin
    {
      role: "roles/bigquery.admin"
      members: [
        "serviceAccount:${var.gcp_project_number}@cloudbuild.gserviceaccount.com"
      ]
    },
    # BigQuery Data Owner
    {
      role: "roles/bigquery.dataOwner"
      members: [
        "serviceAccount:${var.gcp_project_number}@cloudbuild.gserviceaccount.com"
      ]
    },
    # Composer Administrator
    {
      role: "roles/composer.admin"
      members: [
        "serviceAccount:${var.gcp_project_number}@cloudbuild.gserviceaccount.com"
      ]
    },
    # Kubernetes Engine Admin
    {
      role: "roles/container.admin"
      members: [
        "serviceAccount:${var.gcp_project_number}@cloudbuild.gserviceaccount.com"
      ]
    },
    # Storage Object Admin
    {
      role: "roles/storage.objectAdmin"
      members: [
        "serviceAccount:${var.gcp_project_number}@cloudbuild.gserviceaccount.com"
      ]
    },
    # DLP Administrator
    {
      role: "roles/dlp.admin"
      members: [
        "serviceAccount:${var.gcp_project_number}@cloudbuild.gserviceaccount.com"
      ]
    },
    # Dataflow Admin
    {
      role: "roles/dataflow.admin"
      members: [
        "serviceAccount:${var.gcp_project_number}@cloudbuild.gserviceaccount.com"
      ]
    },
    # Dataflow Developer
    {
      role: "roles/dataflow.developer"
      members: [
        "serviceAccount:${var.gcp_project_number}@cloudbuild.gserviceaccount.com"
      ]
    },
    # Dataflow Worker
    {
      role: "roles/dataflow.worker"
      members: [
        "serviceAccount:${var.gcp_project_number}@cloudbuild.gserviceaccount.com"
      ]
    },
    # Service Account User
    {
      role: "roles/iam.serviceAccountUser"
      members: [
        "serviceAccount:${var.gcp_project_number}@cloudbuild.gserviceaccount.com"
      ]
    },
    # Data Catalog TagTemplate Owner
    {
      role: "roles/datacatalog.tagTemplateOwner"
      members: [
        "serviceAccount:${var.gcp_project_number}@cloudbuild.gserviceaccount.com"
      ]
    },
    # Cloud Build Service Account
    {
      role: "roles/cloudbuild.builds.builder"
      members: [
        "serviceAccount:${var.gcp_project_number}@cloudbuild.gserviceaccount.com"
      ]
    },
    # Data Catalog Policy Tag Admin
    {
      role: "roles/datacatalog.categoryAdmin"
      members: [
        "serviceAccount:${var.gcp_project_number}@cloudbuild.gserviceaccount.com"
      ]
    }
  ]
}

locals {
  # flatten ensures that this local value is a flat list of objects, rather
  # than a dict of list of objects.
  iam_metadata_mapping = flatten([
    for role_members in local.iam_metadata_roles : [
      for principal in role_members.members : {
        project = var.gcp_project_id
        role = role_members.role
        principal = principal
      }
    ]
  ])
}

locals {
  # flatten ensures that this local value is a flat list of objects, rather
  # than a dict of list of objects.
  iam_mapping = flatten([
    for project_id in var.managed_gcp_project_list : [
      for role_members in local.iam_managed_roles : [
        for principal in role_members.members : {
          project = project_id
          role = role_members.role
          principal = principal
        }
      ]
    ]
  ])
}

# apply metadata project roles
resource "google_project_iam_member" "managed_projects_iam" {
  for_each = {
    for mapping in local.iam_mapping : "${mapping.project}.${mapping.role}.${mapping.principal}" => mapping
  }
  project = each.value.project
  role    = each.value.role
  member  = each.value.principal
}

# apply metadata project roles
resource "google_project_iam_member" "gcp_project_iam" {
  for_each = {
    for mapping in local.iam_metadata_mapping : "${mapping.project}.${mapping.role}.${mapping.principal}" => mapping
  }
  project = var.gcp_project_id
  role    = each.value.role
  member  = each.value.principal
}
