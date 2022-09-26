# Copyright 2021 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Configuration of Airflow environment.

Read Grizzly environment specific variables from Airflow environment variables.

Typical usage example:

config = Config()
gcp_project_id = config.GCP_PROJECT_ID
"""

import json
from airflow.configuration import conf
from airflow.models import Variable


class Config:
  """Airflow variables used for Grizzly configuration."""
  GCP_PROJECT_ID = Variable.get('GCP_PROJECT_ID')
  ENVIRONMENT = Variable.get('ENVIRONMENT')
  ETL_STAGE_DATASET = Variable.get('ETL_STAGE_DATASET')
  ETL_LOG_TABLE = Variable.get('ETL_LOG_TABLE', 'etl_log.composer_job_details')
  HISTORY_TABLE_CONFIG = json.loads(Variable.get('HISTORY_TABLE_CONFIG'))
  FORCE_OFF_HX_LOADING = Variable.get('FORCE_OFF_HX_LOADING', 'N').upper()
  DEFAULT_DATACATALOG_TAXONOMY_LOCATION = Variable.get(
      'DEFAULT_DATACATALOG_TAXONOMY_LOCATION')
  GS_BUCKET = Variable.get(
      'GS_BUCKET',
      conf.get('logging',
               'remote_base_log_folder'
              ).replace('/logs', '').replace('gs://', '')
  )
