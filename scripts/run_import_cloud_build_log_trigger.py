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

"""Module for running CB triggers during install process.

   Is invoked by apply_grizzly_terraform.sh

   Typical usage example:
   python3 ./run_build_triggers.py project-id branch-name
"""

import argparse
import sys
from typing import Dict, Any, List


from deployment_utils import Trigger
from google.cloud.devtools import cloudbuild_v1
from grizzly.forecolor import ForeColor

def main(args: argparse.Namespace) -> None:

  client = cloudbuild_v1.CloudBuildClient()

  trigger = Trigger(
    client=client,
    gcp_project=args.metadata_project_id,
    branch=args.env,
    trigger_name="import-cloud-build-log",
    substitutions = {"_ENVIRONMENT":args.env}
  )

  trigger.run_trigger()

 
if __name__ == '__main__':

  try:
    ap = argparse.ArgumentParser(
        description='Script used for Import Cloud Log')
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

    arguments = ap.parse_args()
    main(args=arguments)

  except:
    print(f'{ForeColor.RED}Unexpected error:{ForeColor.RESET}',
          sys.exc_info()[1])
    raise

