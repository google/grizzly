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
    -l <GCP_COMPOSER_LOCATION>
    -c <GCP_COMPOSER_INSTANCE_NAME>
    -d <DOMAIN_NAME>
    -bid <BUILD_ID>
    -x <COMMIT_SHA>
    [-l]
"""

import argparse
import pathlib
import sys

from grizzly.composer_environment import ComposerEnvironment

from deployment_utils import BQUtils

def main(args: argparse.Namespace):
  """Implement the command line interface described in the module doc string."""

  gcp_composer_environment = ComposerEnvironment(
      project_id=args.gcp_project_id,
      location=args.gcp_location,
      environment_name=args.gcp_composer_env_name)

  gcp_composer_environment.delete_domain(domain=args.domain_name.upper())

if __name__ == "__main__":

  try:
    # Construct the argument parser
    ap = argparse.ArgumentParser(
        description="Script used for "
                    "deploy ETL code on GCP Composer environment."
    )

    # Add the arguments to the parser
    ap.add_argument(
        "-p",
        "--project",
        dest="gcp_project_id",
        required=True,
        help="Target GCP project")
    ap.add_argument(
        "-l",
        "--location",
        dest="gcp_location",
        required=True,
        help="GCP Composer environment location.")
    ap.add_argument(
        "-c",
        "--composer_environment",
        dest="gcp_composer_env_name",
        required=True,
        help="GCP Composer environment name")
    ap.add_argument(
        "-d",
        "--domain_name",
        required=True,
        help="Domain name for deleting")
    ap.add_argument(
        "-bid",
        "--build_id",
        required=True,
        help="Id of build"
        )
    ap.add_argument(
        "-x",
        "--commit_sha",
        required=True,
        dest="commit_sha",
        help="Commit SHA of the build"
    )
    ap.add_argument(
        "--local",
        required=False,
        dest="local",
        action="store_true",
        help="Generate DAG in temporary folder "
             "without deployment on environment."
    )

    arguments = ap.parse_args()
    main(args=arguments)
  except:
    print("Unexpected error:", sys.exc_info()[1])
    raise
