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

"""Deploy ETL code to GCP Composer environment with git data.

Application generates deployment files in [/tmp/...] folder.
DAG py file is generated automatically on a base of ETL scope and sql files used
Once all referenced files are prepared in [/tmp/...] folder application performs
upload into correspondent [gs://.../DAG/] and [gs://.../data/ETL/<DOMAIN>/]
folder linked with GCP Composer environment.

Example:
  python3 ./deploy_composer_git.py -p <GCP_ENVIRONMENT>
"""

import argparse
import pathlib
import sys

from deployment_utils import BQUtils
from deployment_utils import ImportToolConfig

from load_data_lineage import DataLineage_Utils

CURRENT_PATH = pathlib.Path(__file__).resolve().parent
TEMPLATE_PATH = CURRENT_PATH / "templates"

def main(args: argparse.Namespace):
  """Implement the command line interface described in the module doc string."""

  env = args.env.lower()
  env_config_file = args.env_config_file

  config = ImportToolConfig(env=env, env_config_file=env_config_file)
  bq_utils = BQUtils(gcp_project_id=config.gcp_environment_target)

  DataLineage_Utils.clean_up_data_lineage_history(bq_utils=bq_utils)

if __name__ == "__main__":

  try:
    # Construct the argument parser
    ap = argparse.ArgumentParser(
        description="Script used for "
                    "deploy ETL code on GCP Composer environment."
    )

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

    arguments = ap.parse_args()
    main(args=arguments)
  except:
    print("Unexpected error:", sys.exc_info()[1])
    raise
