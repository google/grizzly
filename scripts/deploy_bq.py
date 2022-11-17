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

"""Execute BigQuery scripts and deploy BigQuery objects.

Apply BQ scripts defined in SCOPE.yml file.

Args:
  source_path: directory with BQ code to be deployed,
    string argument passed in the command line
  gcp_project_id: target GCP project,
    string argument passed in the command line
  scope_file: Scope file with a list of objects to be deployed,
    default = 'SCOPE.yml',

Example:
  python3 ./deploy_bq.py -p <GCP_PROJECT_ID>
    --scope <SCOPE_FILE_NAME>
    -s <SQL_CODE_SOURCE_PATH>
"""


import argparse
import os
import pathlib
import sys
import time

from google.cloud import bigquery
from grizzly.forecolor import ForeColor
from grizzly.grizzly_exception import GrizzlyException
import yaml


def main(args: argparse.Namespace) -> None:
  """Implement the command line interface described in the module doc string."""
  source_path = pathlib.Path(args.source_path)
  gcp_project_id = args.gcp_project_id
  gcp_location = args.gcp_location
  bqclient = bigquery.Client(project=gcp_project_id, location=gcp_location)
  scope_file = source_path / args.scope_file
  if '.yml' not in args.scope_file:
    scope_file = scope_file.with_suffix('.yml')
  scope_config = yaml.safe_load(scope_file.read_text())
  # get list of files to be deployed
  file_list = [
      source_path / f
      for f in scope_config['deployment_scope']
  ]
  print('List of files to be deployed:')
  for f in file_list:
    print(f'  - {f}')
  # deploy files
  for f in file_list:
    query = f.read_text()
    print(f'Executing: {f}')
    # Wait for query to finish.
    query_job = bqclient.query(query)
    time.sleep(10)
    run_time = 10
    while query_job.running():
      print(f'Waiting for job to complete : {f.name}, {query_job.job_id}, ({run_time})')
      run_time += 10
      time.sleep(10)

    # Check if job had errors.
    ex = query_job.exception()
    if ex:
      raise GrizzlyException(ex)
  return


if __name__ == '__main__':
  try:
    # Construct the argument parser
    ap = argparse.ArgumentParser(description=__doc__)
    # Add the arguments to the parser
    ap.add_argument(
        '-p',
        '--project',
        dest='gcp_project_id',
        required=True,
        help='Target GCP project.')
    ap.add_argument(
        '-l',
        '--location',
        dest='gcp_location',
        required=False,
        default=None,
        help='Default location for jobs / datasets / tables. US by default')
    ap.add_argument(
        '--scope',
        dest='scope_file',
        default='SCOPE.yml',
        help='Scope file with a list of BQ objects to be deployed.')
    ap.add_argument(
        '-s',
        '--source_path',
        required=True,
        help='Directory with BQ code to be deployed')

    main(ap.parse_args())
  except:
    print(f'{ForeColor.RED}Unexpected error:{ForeColor.RESET}',
          sys.exc_info()[1])
    raise
