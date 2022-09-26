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

"""Deploy ETL code to GCP Composer environment.

Application generates deployment files in [/tmp/...] folder.
DAG py file is generated automatically on a base of ETL scope and sql files used
Once all referenced files are prepared in [/tmp/...] folder application performs
upload into correspondent [gs://.../DAG/] and [gs://.../data/ETL/<DOMAIN>/]
folder linked with GCP Composer environment.

Example:
  python3 ./deploy_composer.py -p <GCP_PROJECT_ID>
    -l <GCP_COMPOSER_LOCATION>
    -c <GCP_COMPOSER_INSTANCE_NAME>
    -s <ETL_SOURCE_PATH> [-l]
"""
import argparse
import pathlib
import sys
from grizzly.composer_environment import ComposerEnvironment
from grizzly.scope import Scope

CURRENT_PATH = pathlib.Path(__file__).resolve().parent
TEMPLATE_PATH = CURRENT_PATH / 'templates'


def main(args: argparse.Namespace) -> None:
  """Implement the command line interface described in the module doc string."""
  gcp_composer_environment = ComposerEnvironment(
      project_id=args.gcp_project_id,
      location=args.gcp_location,
      environment_name=args.gcp_composer_env_name)
  source_path = pathlib.Path(args.source_path)
  deployment_scope = Scope(source_path)
  deployment_scope.generate_stagging_files()
  deployment_scope.generate_DAG_file(TEMPLATE_PATH / 'dag.py.jinja2')
  gcp_composer_environment.publish_scope(deployment_scope)
  return


if __name__ == '__main__':
  try:
    # Construct the argument parser
    ap = argparse.ArgumentParser(
        description=__doc__
    )
    # Add the arguments to the parser
    ap.add_argument(
        '-p',
        '--project',
        dest='gcp_project_id',
        required=True,
        help='Target GCP project')
    ap.add_argument(
        '-l',
        '--location',
        dest='gcp_location',
        required=True,
        help='GCP Composer environment location.')
    ap.add_argument(
        '-c',
        '--composer_environment',
        dest='gcp_composer_env_name',
        required=True,
        help='GCP Composer environment name')
    ap.add_argument(
        '-e',
        '--env',
        required=False,
        help='The stage(environment) used: dev|uat|prod')
    ap.add_argument(
        '-s',
        '--source_path',
        required=True,
        help='Directory with pipeline to be deployed')
    ap.add_argument(
        '--local',
        required=False,
        dest='local',
        action='store_true',
        help=('Generate DAG in a temporary folder without deployment on the '
              'environment.')
    )
    main(ap.parse_args())
  except:
    print('Unexpected error:', sys.exc_info()[1])
    raise
