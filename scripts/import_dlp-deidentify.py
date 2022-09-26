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
"""Import DLP deidentify templates.

  Script used by Cloud Build to import DLP deidentify templates to a project.
  Script used for SYNC/MERGE of DLP templates.

  Typical usage example:

  python3 ./import_dlp-deidentify.py -t $BRANCH_NAME -m $_MODE
    -c $$CFG -f /workspace/$$GRIZZLY_REPO
"""

import argparse
import enum
import pathlib
import sys
from typing import Dict, Union

from deployment_utils import Utils as DeploymentUtils
import google
import google.auth.transport.requests
import google.cloud.dlp
from google.cloud.dlp_v2.types import DeidentifyTemplate
from google.cloud.dlp_v2.types import dlp

import grizzly.cloud_platform_auth as Auth
from grizzly.forecolor import ForeColor
from grizzly.grizzly_exception import GrizzlyException
import yaml

dlp_client = google.cloud.dlp_v2.DlpServiceClient()
FILES_FOLDER = 'dlp-deidentify-templates'


class Mode(enum.Enum):
  merge = 1
  sync = 2


class ImportToolConfig:
  """Class containing configuration data for the Import job.

  Attributes:
    env_target (str): Target GCP project
    env_config_file (pathlib.Path): Environment file
    gcp_environment_source (str): Environment configuration from environment
      config file.
    parent_target (str): location of parent target
    target_template_id (str): template id from environment config file
  """

  def __init__(self,
               env_target: str,
               env_config_file: Union[str, pathlib.Path]) -> None:
    """Initializes ImportToolConfig.

    Args:
      env_target (str): Target GCP project.
      env_config_file (Union[str, pathlib.Path]): Environment file
    """
    self.env_target = env_target
    if isinstance(env_config_file, str):
      self.env_config_file = pathlib.Path(env_config_file)
    environment_config = yaml.safe_load(
        self.env_config_file.read_text())[self.env_target]
    self.gcp_environment_source = environment_config['GCP_ENVIRONMENT']
    self.parent_target = (
        f'projects/{self.gcp_environment_source}/locations/global')
    self.target_template_id = (
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


def delete_templates(config: ImportToolConfig,
                     templates: Dict[str, DeidentifyTemplate]) -> None:
  """Deletes set of DLP Deindentify templates from project.

  Args:
    config (ImportToolConfig): ImportToolConfig object containing configuration
      data.
    templates (Dict[str, DeidentifyTemplate]): A dictionary of templates to
      delete. Keys are template names. Values are templates.

  Raises:
    (GrizzlyException): Exception in case if templates were already removed.
  """
  print('Templates for deleting:')
  print(templates)
  for template_id, _ in templates.items():
    try:
      del_request = dlp.DeleteDeidentifyTemplateRequest()
      del_request.name = config.target_template_id.format(
          template_id=template_id)
      dlp_client.delete_deidentify_template(request=del_request)
    except google.api_core.exceptions.NotFound as e:
      raise GrizzlyException(
          'Template is not found or has been already deleted.') from e


def create_templates(config: ImportToolConfig,
                     templates: Dict[str, DeidentifyTemplate]) -> None:
  """Creates set of DLP Deidentify templates for project.

  Args:
    config (ImportToolConfig): ImportToolConfig object containing configuration
      data.
    templates (Dict[str, DeidentifyTemplate]): A dictionary of templates to
      create. Keys are template names. Values are templates.
  """
  print('Templates for creation:')
  print(templates)

  for template_id, template in templates.items():
    target_template = dlp.CreateDeidentifyTemplateRequest()
    target_template.deidentify_template = template
    target_template.template_id = template_id
    target_template.parent = config.parent_target
    dlp_client.create_deidentify_template(request=target_template)


def sync_templates(config: ImportToolConfig,
                   source_templates: Dict[str, DeidentifyTemplate],
                   target_templates: Dict[str, DeidentifyTemplate]) -> None:
  """Syncs the DLP Deidentify templates between source and target.

  Args:
    config (ImportToolConfig): ImportToolConfig object containing configuration
      data.
    source_templates (Dict[str, DeidentifyTemplate]): Dict with keys of template
      names and values of definitions.
    target_templates (Dict[str, DeidentifyTemplate]): Dict with keys of template
      names and values of definitions.
  """
  # delete if found template in target
  target_templates_deleting = {
      k: v for k, v in source_templates.items() if k in target_templates
  }
  delete_templates(config=config, templates=target_templates_deleting)
  # create template in target
  create_templates(config=config, templates=source_templates)


def load_source_proto(folder: str) -> Dict[str, DeidentifyTemplate]:
  """Loads source protos from a folder.

  Args:
    folder (str): source folder

  Returns:
    (Dict[str, DeidentifyTemplate]): A dictionary with keys of tag names and
      values of tag definitions.
  """
  path = f'/{folder}/{FILES_FOLDER}/'
  templates = DeploymentUtils.proto_load(
      path,
      '*.gcp_proto',
      proto_class=DeidentifyTemplate)
  templates = {v.name.split('/')[-1]: v for _, v in templates.items()}
  return templates


def main(args: argparse.Namespace) -> None:
  """Import DLP Deidentify templates.

  Imports DLP deidentify templates to a project. Script used for SYNC/MERGE of
  DLP templates.

  Args:
    args (argparse.Namespace): input arguments
  """
  env_target = args.env_target.lower()
  mode = args.mode.lower()
  env_config_file = args.env_config_file
  folder = args.folder
  Auth.auth()
  config = ImportToolConfig(
      env_target=env_target,
      env_config_file=env_config_file)

  source_dic = load_source_proto(folder=folder)
  target_dic = get_dic_deidentify_templates(config.parent_target)

  mode = Mode[mode]

  if mode == Mode.sync:
    delete_templates(config=config,
                     templates=target_dic)
    target_dic.clear()

  sync_templates(config=config,
                 source_templates=source_dic,
                 target_templates=target_dic)


if __name__ == '__main__':
  try:
    # Construct the argument parser
    ap = argparse.ArgumentParser(
        description='Script used for SYNC/MERGE of DLP templates')
    # Add the arguments to the parser
    ap.add_argument(
        '-t',
        '--env_target',
        dest='env_target',
        required=True,
        help='Target GCP project.')
    ap.add_argument(
        '-m',
        '--mode',
        dest='mode',
        required=True,
        help='mode: SYNC MERGE')
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
