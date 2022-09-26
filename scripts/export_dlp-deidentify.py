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
"""Export DLP deidentify.

  Script used by Cloud Build to export data catalog templates as protos
  to a Repo. Script used for SYNC/MERGE of DLP templates.

  Typical usage example:

  python3 ./export_dlp-deidentify.py
    -s $BRANCH_NAME
    -c $$CFG
    -f /workspace/$$REPO
"""
import argparse
import glob
import pathlib
import sys
from typing import Dict, Union

from deployment_utils import Utils as DeploymentUtils
from git import Repo
import google
import google.auth.transport.requests
import google.cloud.dlp
from google.cloud.dlp_v2.types import DeidentifyTemplate

import grizzly.cloud_platform_auth as Auth
from grizzly.forecolor import ForeColor
import yaml

dlp_client = google.cloud.dlp_v2.DlpServiceClient()
FILES_FOLDER = 'dlp-deidentify-templates'


class ExportToolConfig:
  """Class containing configuration data for the Export job.

  Attributes:
    env_source (str): Source GCP project.
    env_config_file (pathlib.Path): Environment file.
    gcp_environment_source (str): Environment configuration from environment
      config file.
    parent_source (str): Location of parent source.
    source_template_id (str): Template id from environment config file.
  """

  def __init__(self,
               env_source: str,
               env_config_file: Union[str, pathlib.Path]) -> None:
    """Initializes ExportToolConfig."""

    self.env_source = env_source
    if isinstance(env_config_file, str):
      self.env_config_file = pathlib.Path(env_config_file)
    environment_config = yaml.safe_load(
        self.env_config_file.read_text())[self.env_source]
    self.gcp_environment_source = environment_config['GCP_ENVIRONMENT']
    self.parent_source = (
        f'projects/{self.gcp_environment_source}/locations/global')
    self.source_template_id = (
        f'projects/{self.gcp_environment_source}/deidentifyTemplates/' +
        '{template_id}')


def get_dic_deidentify_templates(parent: str) -> Dict[str, DeidentifyTemplate]:
  """Gets DeidentifyTemplates for a specific parent resource.

  Args:
    parent (str): Parent resource name.

  Returns:
    (Dict[str, DeidentifyTemplate]): A dictionary with keys of template names
      and values of template definition.
  """
  templates = dlp_client.list_deidentify_templates(parent=parent)
  res = {str(x.name).split('/')[-1]: x for x in templates}
  return res


def main(args: argparse.Namespace) -> None:
  """Export DLP deidentify.

  Exports DLP deindentify as protos to a Repo. Script used for
  SYNC/MERGE of DLP templates.

  Args:
    args (argparse.Namespace): Input arguments.
  """
  env_source = args.env_source.lower()
  env_config_file = args.env_config_file
  folder = args.folder

  config = ExportToolConfig(
      env_source=env_source,
      env_config_file=env_config_file)

  Auth.auth()

  source_dlp = get_dic_deidentify_templates(config.parent_source)

  repo = Repo(folder)
  print(f'Repo folder: {folder}')

  dest_folder = f'{folder}/{FILES_FOLDER}'
  pathlib.Path(dest_folder).mkdir(parents=True, exist_ok=True)

  files = glob.glob(f'{dest_folder}/*.gcp_proto')
  print('Removing deleted files from git repo')
  for f in files:
    print(f'Remove: {f}')
    repo.index.remove([f], working_tree=True)

  print(f'Deleted files: {files}')

  pathlib.Path(dest_folder).mkdir(parents=True, exist_ok=True)

  for name, template in source_dlp.items():
    file_name = DeploymentUtils.proto_save(
        obj=template,
        class_message=DeidentifyTemplate,
        file_name=f'{name}.gcp_proto',
        path=dest_folder)
    repo.git.add([file_name])
    print(file_name)

  repo.index.commit('Export Templates')
  origin = repo.remote(name='origin')
  origin.push()


if __name__ == '__main__':
  try:
    # Construct the argument parser
    ap = argparse.ArgumentParser(
        description='Script used for SYNC/MERGE of DLP templates')
    # Add the arguments to the parser
    ap.add_argument(
        '-s',
        '--env_source',
        dest='env_source',
        required=True,
        help='Source GCP project.')
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

    args_values = ap.parse_args()
    main(args=args_values)
  except:
    print(f'{ForeColor.RED}Unexpected error:{ForeColor.RESET}',
          sys.exc_info()[1])
    raise
