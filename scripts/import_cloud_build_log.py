# Copyright 2022 Google LLC
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

"""Import cloud build history into BQ table.

Load information form GCP build logs into BQ log table
etl_log.cb_trigger_execution.

Args:
  env: Environment name (dev, uat, prod)
  metadata_project_id: GCP project id where GIT repository located.
  env_config_file: Environment configuration file.
  folder: File folder.

Example:
  python3 ./import_cloud_build_log.py -e <ENVIRONMENT>
    -m <METADATA_PROJECT_ID>
    -c <ENV_CONFIG_FILE>
    -f <FOLDER_PATH>
"""

import argparse
import json
import sys
from typing import List

from deployment_utils import BQUtils
from deployment_utils import ImportToolConfig
import google.auth.transport.requests
from google.auth.transport.urllib3 import AuthorizedHttp
from grizzly.forecolor import ForeColor
from grizzly_git.config import CB_TRIGGER_EXECUTION_TABLE_NAME
from grizzly_git.config import CB_TRIGGER_EXECUTION_TABLE_SCHEMA
from grizzly_git.config import GIT_DATASET
from grizzly_git.config import SQL_MERGE_TMP_TO_CB_TRIGGERS_EXECUTION
from grizzly_git.config import SQL_MERGE_TO_SUBJECT_AREA_BUILD
from grizzly_git.config import STAGE_DATASET
from grizzly_git.config import SUBJECT_AREA_BUILD_TABLE_NAME
from grizzly_git.config import SUBJECT_AREA_BUILD_TABLE_SCHEMA


def get_auth_http() -> AuthorizedHttp:
  """Return AuthorizedHttp for gcp cloud access."""

  scopes = ['https://www.googleapis.com/auth/cloud-platform']

  credentials, _ = google.auth.default(scopes=scopes)
  auth_req = google.auth.transport.requests.Request()
  credentials.refresh(auth_req)

  return AuthorizedHttp(credentials)


def get_cloud_build_rows(base_url: str) -> List[any]:
  """Convert REST API data from Cloud Build API into BQ rows.

  Args:
    base_url (str): Base part of GCP RestAPI url used for data extraction.

  Returns:
    (List[any]): List of rows to be inserted into BQ table.
  """

  authed_http = get_auth_http()

  def get_rows(url: str):
    r = authed_http.urlopen(method='get', url=url)
    response = json.loads(r.data)
    rows = []

    builds = response.get('builds', None)

    if not builds:
      return [], None

    for bid in response['builds']:
      if 'startTime' not in bid:
        continue
      bid_id = bid['id']
      status = bid['status']
      start_time = bid['startTime']

      finish_time = bid.get('finishTime', None)

      subject_area, trigger_name, commit_sha, short_sha = [None]*4
      substitutions = bid.get('substitutions', None)
      if substitutions:
        subject_area = substitutions.get('_DOMAIN', None)

        if subject_area:
          subject_area = subject_area.split('/')[-1].upper()

        trigger_name = substitutions.get('TRIGGER_NAME', None)
        commit_sha = substitutions.get('COMMIT_SHA', None)
        short_sha = substitutions.get('SHORT_SHA', None)

      # subject_area = subject_area.upper() if subject_area else subject_area.

      rows.append({'id': bid_id,
                   'status': status,
                   'start_time': start_time,
                   'finish_time': finish_time,
                   'trigger_name': trigger_name,
                   'commit_sha': commit_sha,
                   'short_sha': short_sha,
                   'subject_area': subject_area})

    next_url = None
    if 'nextPageToken' in response:
      next_url = f'{base_url}?pageToken={response["nextPageToken"]}'

    return rows, next_url

  ret_rows = []

  url = base_url
  while url:
    rows, url = get_rows(url=url)
    ret_rows += rows

  return ret_rows


def cb_triggers_execution_data_proc(
    bq_utils: BQUtils,
    metadata_project_id: str) -> None:
  """Insert Cloud Build Trigger historical data into BQ table.

  Args:
    bq_utils (BQUtils): Instance of BQUtils used for table data uploading.
    metadata_project_id (str): GCP project id with GIT repository.
  """

  table_name = f'{GIT_DATASET}.{CB_TRIGGER_EXECUTION_TABLE_NAME}'
  tmp_table_name = f'{STAGE_DATASET}.{CB_TRIGGER_EXECUTION_TABLE_NAME}'

  bq_utils.create_table(table_name=table_name,
                        table_schema=CB_TRIGGER_EXECUTION_TABLE_SCHEMA)

  tmp_table = bq_utils.create_temp_table(
      table_name=tmp_table_name,
      table_schema=CB_TRIGGER_EXECUTION_TABLE_SCHEMA)

  url = 'https://cloudbuild.googleapis.com/v1/projects/'
  url += f'{metadata_project_id}/builds'

  cloud_build_rows = get_cloud_build_rows(base_url=url)

  tmp_table_name = '{}.{}.{}'.format(
      tmp_table.project,
      tmp_table.dataset_id,
      tmp_table.table_id)

  bq_utils.bq_client.insert_rows_json(table=tmp_table_name,
                                      json_rows=cloud_build_rows)

  sql_merge = SQL_MERGE_TMP_TO_CB_TRIGGERS_EXECUTION
  sql_merge = sql_merge.format(GIT_DATASET=GIT_DATASET,
                               tmp_table=tmp_table_name)

  bq_utils.bq_client.query(query=sql_merge).result()


def subject_area_build_data_proc(bq_utils: BQUtils) -> None:
  """Insert data into SUBJECT_AREA_BUILD table.

  Args:
    bq_utils (BQUtils): Instance of BQUtils used for table data uploading.
  """
  table_name = f'{GIT_DATASET}.{SUBJECT_AREA_BUILD_TABLE_NAME}'
  bq_utils.create_table(table_name=table_name,
                        table_schema=SUBJECT_AREA_BUILD_TABLE_SCHEMA)

  sql_merge = SQL_MERGE_TO_SUBJECT_AREA_BUILD.format(GIT_DATASET=GIT_DATASET)
  bq_utils.bq_client.query(query=sql_merge).result()


def main(args: argparse.Namespace) -> None:
  """Implement the command line interface described in the module doc string."""
  env = args.env.lower()
  env_config_file = args.env_config_file
  metadata_project_id = args.metadata_project_id

  config = ImportToolConfig(env=env, env_config_file=env_config_file)

  bq_utils = BQUtils(gcp_project_id=config.gcp_environment_target)

  cb_triggers_execution_data_proc(bq_utils=bq_utils,
                                  metadata_project_id=metadata_project_id)
  subject_area_build_data_proc(bq_utils=bq_utils)

if __name__ == '__main__':
  try:
    # Construct the argument parser
    ap = argparse.ArgumentParser(
        description='Script uploads Cloud Build logs into BQ log table.')
    # Add the arguments to the parser
    ap.add_argument(
        '-e',
        '--env',
        dest='env',
        required=True,
        help='Env of GCP project.')
    ap.add_argument(
        '-m',
        '--metadata_project_id',
        dest='metadata_project_id',
        required=True,
        help='Metadata project id')
    ap.add_argument(
        '-c',
        '--env_config_file',
        dest='env_config_file',
        required=True,
        help='Environment file')
    ap.add_argument(
        '-f',
        '--folder',
        dest='folder',
        required=True,
        help='File folder')

    arguments = ap.parse_args()
    main(args=arguments)

  except:
    print(f'{ForeColor.RED}Unexpected error:{ForeColor.RESET}',
          sys.exc_info()[1])
    raise
