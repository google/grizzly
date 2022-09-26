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

"""Deploys Dataflow job of inbound pub/sub.

  Script used by Cloud Build to deploy dataflow jobs via DeploymentTool.

  Typical usage example:

  python3 ./deploy_dataflow_inbound_pubsub.py -s $_SCOPE_FILE -e $BRANCH_NAME
      -d $_DOMAIN -c $$CFG
"""

import argparse
import sys

from deployment_configuration import DeploymentToolConfig
from deployment_dataflow_tool import DeploymentTool
from grizzly.forecolor import ForeColor


def main(args: argparse.Namespace) -> None:
  """Deploys Dataflow job via DeploymentTool.

  Args:
    args: Command input arguments. next arguments are supported.
      environment - Environment name, string argument passed in the commandline.
      domain - Domain name to be deployed, string argument passed in the
        commandline.
      env_config_file - Environment file, string argument passed in the
        commandline.
      scope_file - Scope file with a list of objects to be deployed, default
        value is 'SCOPE.yml'.
  """
  scope_file = args.scope_file
  environment = args.environment.lower()
  domain = args.domain.lower()
  env_config_file = args.env_config_file

  config = DeploymentToolConfig(
      scope_file=scope_file,
      domain=domain,
      environment=environment,
      env_config_file=env_config_file)

  deploy_tool = DeploymentTool(config=config)
  deploy_tool.deploy()


if __name__ == '__main__':
  try:
    # Construct the argument parser
    ap = argparse.ArgumentParser(description=__doc__)
    # Add the arguments to the parser
    ap.add_argument(
        '-s',
        '--scope',
        dest='scope_file',
        default='SCOPE.yaml',
        help='Scope file with a list of Dataflow objects to be deployed.')
    ap.add_argument(
        '-e',
        '--environment',
        dest='environment',
        required=True,
        help='Environment name')
    ap.add_argument(
        '-d',
        '--domain',
        dest='domain',
        required=True,
        help='Domain name to be deployed')
    ap.add_argument(
        '-c',
        '--env_config_file',
        dest='env_config_file',
        required=True,
        help='Environment file')

    main(ap.parse_args())
  except:
    print(f'{ForeColor.RED}Unexpected error:{ForeColor.RESET}',
          sys.exc_info()[1])
    raise
