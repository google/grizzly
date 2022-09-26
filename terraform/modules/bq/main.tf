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

resource "google_bigquery_dataset" "dataset" {
  project = var.gcp_project_id
  dataset_id = "etl_log"
  description = "ETL Logs for Grizzly (GCP Composer/Airflow)."
}

resource "google_bigquery_dataset" "etl_staging" {
  project = var.gcp_project_id
  dataset_id = "etl_staging_composer"
  default_table_expiration_ms = 259200000
  description = "Staging dataset for GCP Composer."
}

resource "google_bigquery_table" "etl_log_table" {
  project = var.gcp_project_id
  dataset_id = google_bigquery_dataset.dataset.dataset_id
  table_id = "composer_job_details"
  deletion_protection=false

  time_partitioning {
    type = "DAY"
    field = "job_start_timestamp"
  }

  labels = {
    env = "etl_log_table"
  }

  schema = <<EOF
[
    {
    "name": "job_id",
    "type": "INTEGER"
    },
    {
    "name": "job_name",
    "type": "STRING"
    },
    {
    "name": "job_start_timestamp",
    "type": "TIMESTAMP"
    },
    {
    "name": "job_end_timestamp",
    "type": "TIMESTAMP"
    },
    {
    "name": "job_status",
    "type": "STRING"
    },
    {
    "name": "job_write_mode",
    "type": "STRING"
    },
    {
    "name": "job_schedule_interval",
    "type": "STRING"
    },
    {
    "name": "job_parameter_file",
    "type": "STRING"
    },
    {
    "name": "subject_area",
    "type": "STRING"
    },
    {
    "name": "target_table",
    "type": "STRING"
    },
    {
    "name": "target_hx_loading_indicator",
    "type": "STRING"
    },
    {
    "name": "stage_loading_query",
    "type": "STRING"
    },
    {
    "name": "source_table",
    "type": "STRING",
    "mode": "REPEATED"
    },
    {
    "name": "job_step",
    "type": "RECORD",
    "mode": "REPEATED",
    "fields": [
        {
        "name": "job_step_id",
        "type": "INTEGER"
        },
        {
        "name": "job_step_name",
        "type": "STRING"
        },
        {
        "name": "job_step_start_time",
        "type": "TIMESTAMP"
        },
        {
        "name": "job_step_end_time",
        "type": "TIMESTAMP"
        },
        {
        "name": "job_step_status",
        "type": "STRING"
        },
        {
        "name": "total_bytes_billed",
        "type": "INTEGER"
        },
        {
        "name": "total_bytes_processed",
        "type": "INTEGER"
        }
    ]
      },
      {
        "name": "subject_area_build_id",
        "type": "STRING"
     },
      {
        "name": "job_build_id",
        "type": "STRING"
     },
      {
        "name": "target_audit_indicator",
        "type": "STRING"
     }
]
EOF
}
