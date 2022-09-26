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

"""Import taxonomy and access from GCP into set of tables.

Import taxonomy and access from GCP into set of tables.

Example:
  python3 ./import_taxonomy.py
    -e <ENVIRONMENT>
    -c <ENV_CONFIG_FILE>
    -f <ETL_SOURCE_PATH> [-l]
"""

import argparse
import json
import os
import sys
from typing import Any, List

from deployment_utils import BQUtils
from deployment_utils import ImportToolConfig
from deployment_utils import Utils
import google.auth.transport.requests
from google.auth.transport.urllib3 import AuthorizedHttp
from google.cloud import bigquery
from grizzly.forecolor import ForeColor


PROJECT_TAXONOMY_TABLE_NAME = 'project_taxonomy'
PROJECT_TAXONOMY_TABLE_SCHEMA = [
    bigquery.SchemaField('project_id', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('dataset_name', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('table_name', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('column_name', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('taxonomy_name', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('policy_tag_name', 'STRING', mode='NULLABLE')
]


def create_table(project_id: str) -> None:
  """Create Project Taxonomy table."""

  bq_utils = BQUtils(gcp_project_id=project_id)
  bq_utils.create_table(table_name=f'etl_log.{PROJECT_TAXONOMY_TABLE_NAME}',
                        table_schema=PROJECT_TAXONOMY_TABLE_SCHEMA)


def get_authorized_http() -> AuthorizedHttp:
  """Get AuthorizedHttp for the session."""

  scopes = ['https://www.googleapis.com/auth/cloud-platform']

  credentials, _ = google.auth.default(scopes=scopes)
  auth_req = google.auth.transport.requests.Request()
  credentials.refresh(auth_req)

  auth_http = AuthorizedHttp(credentials)
  return auth_http


def get_taxonomy(project_id: str,
                 dataset_name: str,
                 table_name: str) -> List[Any]:
  """Get list of taxonomies."""

  base_bq_api_url = 'https://datacatalog.googleapis.com/v1/{api_call}'

  authed_http = get_authorized_http()

  api_call = 'https://content-bigquery.googleapis.com/bigquery/v2/projects/'
  api_call += '{project_Id}/datasets/{dataset_name}/tables/{table_name}'

  api_call = api_call.format(
      project_Id=project_id,
      dataset_name=dataset_name,
      table_name=table_name)
  r = authed_http.urlopen(method='get', url=api_call)
  response = json.loads(r.data)

  ret = []
  for fld in response['schema']['fields']:
    if 'policyTags' in fld:
      column_name = fld['name']
      full_tax = fld['policyTags']['names'][0]
      try:
        tax_ind = full_tax.index('taxonomies')
      except KeyError:
        tax_ind = None

      policy_tag_ind = full_tax.index('policyTags')
      pr_loc = full_tax[0:tax_ind+10]
      pr_log_tax = full_tax[0:policy_tag_ind+10]
      tax_id = full_tax[tax_ind+11:policy_tag_ind-1]
      p_tag_id = full_tax[policy_tag_ind+11:]

      t = authed_http.urlopen(method='get',
                              url=base_bq_api_url.format(api_call=pr_loc))
      response = json.loads(t.data)

      for tn in response['taxonomies']:

        if tax_id in tn['name']:
          taxonomy_name = tn['displayName']
          url = base_bq_api_url.format(api_call=pr_log_tax)
          t = authed_http.urlopen(method='get', url=url)

          response = json.loads(t.data)

          for pt in response['policyTags']:
            if p_tag_id in pt['name']:
              policy_tag_name = pt['displayName']
              ret.append((project_id,
                          dataset_name,
                          table_name,
                          column_name,
                          taxonomy_name,
                          policy_tag_name))
  return ret


def insert_taxonomies(taxonomies_list: List[Any],
                      project_id: str,
                      ) -> None:
  """Insert  list of taxonomies into BQ table."""

  template_sql = """
     insert into etl_log.project_taxonomy
     (project_id, dataset_name, table_name,
      column_name, taxonomy_name, policy_tag_name)
  values
     ('{project_id}','{dataset_name}', '{table_name}',
      '{column_name}','{taxonomy_name}','{policy_tag_name}')"""

  for taxonomy in taxonomies_list:

    project_id = taxonomy[0]
    dataset_name = taxonomy[1]
    table_name = taxonomy[2]
    column_name = taxonomy[3]
    taxonomy_name = taxonomy[4]
    policy_tag_name = taxonomy[5]

    sql = template_sql.format(
        project_id=project_id,
        dataset_name=dataset_name,
        table_name=table_name,
        column_name=column_name,
        taxonomy_name=taxonomy_name,
        policy_tag_name=policy_tag_name)

    deployment_cmd = f'bq query --project_id {project_id}'
    deployment_cmd += f' --use_legacy_sql=false "{sql}"'

    err = os.system(deployment_cmd)
    if err != 0:
      print(f'Error during inserting data: {deployment_cmd}')


def main(args: argparse.Namespace) -> None:
  """Import data catalog."""

  Utils.auth()
  env = args.env.lower()
  env_config_file = args.env_config_file
  config = ImportToolConfig(env=env, env_config_file=env_config_file)

  client = Utils.get_bq_client(project=config.gcp_environment_target)

  create_table(project_id=config.gcp_environment_target)

  datasets = client.list_datasets()
  for dataset in datasets:
    dataset_id = dataset.dataset_id
    tables = client.list_tables(dataset_id)

    for table in tables:
      project_id = table.project
      dataset_name = table.dataset_id
      table_name = table.table_id

      table_taxonomies = get_taxonomy(
          project_id=project_id,
          dataset_name=dataset_name,
          table_name=table_name)

      insert_taxonomies(taxonomies_list=table_taxonomies,
                        project_id=project_id)


if __name__ == '__main__':

  try:
    # Construct the argument parser
    ap = argparse.ArgumentParser(
        description='Script used for SYNC/MERGE of DLP templates')
    # Add the arguments to the parser
    ap.add_argument(
        '-e',
        '--env',
        dest='env',
        required=True,
        help='Env of GCP project.')
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
