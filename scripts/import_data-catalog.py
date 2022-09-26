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
"""Import data catalog.

  Script used by Cloud Build to import data catalog templates to a project.
  Script used for SYNC/MERGE of DLP templates.

  Typical usage example:

  python3 ./import_data-catalog.py -t $BRANCH_NAME -m $_MODE
    -c $$CFG -f /workspace/$$GRIZZLY_REPO
"""
import argparse
import enum
import pathlib
import subprocess
import sys
from typing import Dict, List, TypeVar, Union

from deployment_utils import Utils as DeploymentUtils
from google.cloud import datacatalog_v1
from google.cloud.datacatalog_v1.types import datacatalog
from google.cloud.datacatalog_v1.types import TagTemplate

import grizzly.cloud_platform_auth as Auth
from grizzly.forecolor import ForeColor
from grizzly.grizzly_exception import GrizzlyException
import yaml

TSearchCatalogPager = TypeVar('TSearchCatalogPager')
client = datacatalog_v1.DataCatalogClient()
FILES_FOLDER = 'data-catalog-templates'


class Mode(enum.Enum):
  merge = 1
  sync = 2


class ImportToolConfig:
  """Class containing configuration data for the Import job.

  Attributes:
    env_target (str): Target GCP project
    env_config_file (pathlib.Path): Environment file
    gcp_environment_target (str): environment configuration of target
    parent_target (str): location of parent target
  """

  def __init__(self,
               env_target: str,
               env_config_file: Union[str, pathlib.Path]) -> None:
    """Initializes ImportToolConfig.

    Args:
      env_target (str): Target GCP project
      env_config_file (Union[str, pathlib.Path]): Environment file

    Returns:
      None
    """
    self.env_target = env_target
    if isinstance(env_config_file, str):
      self.env_config_file = pathlib.Path(env_config_file)
    environment_config = yaml.safe_load(
        self.env_config_file.read_text())[self.env_target]
    self.gcp_environment_target = environment_config['GCP_ENVIRONMENT']
    self.parent_target = (f'projects/{self.gcp_environment_target}/locations'
                          '/{location}')


def _search(project_id: str, search_string: str) -> TSearchCatalogPager:
  """Searches data catalog for resources that match a query.

  Args:
    project_id (str): ID of GCP project to search in.
    search_string (str): Query string to search

  Returns:
    (TSearchCatalogPager): SearchCatalog response message
  """
  scope = datacatalog_v1.types.SearchCatalogRequest.Scope()
  scope.include_project_ids.append(project_id)
  scope.include_public_tag_templates = False

  datacatalog_cli = datacatalog_v1.DataCatalogClient()
  request: datacatalog_v1.types.SearchCatalogRequest = (
      datacatalog_v1.types.SearchCatalogRequest())
  request.query = search_string
  request.page_size = 5
  request.scope = scope

  return datacatalog_cli.search_catalog(request=request)


def get_dic_tag_templates(project_id: str) -> Dict[str, TagTemplate]:
  """Gets DLP tag templates for a specific project.

  Args:
    project_id: ID of GCP project.

  Returns:
    A dictionary with keys of template names and values of template definition.
  """
  search_results = _search(
      project_id=project_id,
      search_string=f'projectId:{project_id}, type=tag_template')

  tags_names_set = set()
  for page in search_results.pages:
    for result in page.results:
      tags_names_set.add(result.relative_resource_name)

  tags_dic = {}
  for tag in tags_names_set:
    request = datacatalog.GetTagTemplateRequest()
    request.name = tag
    tag_response = client.get_tag_template(request=request)
    tags_dic[tag] = tag_response

  res = {str(v.name).split('/')[-1]: v for _, v in tags_dic.items()}

  return res


def run_command(arguments: List[str]) -> str:
  """Constructs and runs gcloud composer command.

  Args:
    arguments: list where each entry is a word in a gcloud composer command

  Returns:
    Result of gcloud composer command.

  Raises:
    GrizzlyException: An error occurred in case if bash command failed.
  """
  cmd_result = subprocess.run(
      arguments,
      check=False,
      stdout=subprocess.PIPE,
      stderr=subprocess.PIPE)
  print(cmd_result)
  if cmd_result.returncode != 0:
    raise GrizzlyException(
        f'{ForeColor.RED}{cmd_result.stderr.decode("utf-8")}{ForeColor.RESET}')
  return cmd_result.stdout.decode('utf-8')


def delete_templates(config: ImportToolConfig, templates) -> None:
  """Deletes set of DLP templates from project.

  Args:
    config:
      ImportToolConfig object containing configuration data
    templates:
      A dictionary of templates to delete. Keys are template names. Values are
      templates.

  Returns:
    None
  """
  for name, template in templates.items():
    location = template.name.split('/')[3]
    composer_cmd = [
        'gcloud', 'data-catalog', 'tag-templates', 'delete', name, '--location',
        location, '--quiet', '--force', '--project',
        config.gcp_environment_target
    ]
    run_command(composer_cmd)


def sync_templates(config: ImportToolConfig,
                   source_templates: Dict[str, TagTemplate],
                   target_templates: Dict[str, TagTemplate],
                   is_already_deleted: bool = False) -> None:
  """Syncs the DLP templates between source and target.

  Args:
    config: ImportToolConfig object containing configuration data
    source_templates: Dict with keys of template names and values of definitions
    target_templates: Dict with keys of template names and values of definitions
    is_already_deleted: If false, deletes templates before create

  Returns:
    None
  """
  # delete if found template in target

  target_templates_deleting = {
      k: v for k, v in source_templates.items() if k in target_templates
  }

  if not is_already_deleted:
    print('Deleting target tags for sync:')
    print(target_templates_deleting.keys())
    delete_templates(config=config, templates=target_templates_deleting)

  # create template in target
  create_templates(config=config, templates=source_templates)


def create_templates(config: ImportToolConfig,
                     templates: Dict[str, TagTemplate]) -> None:
  """Creates set of DLP templates for project.

  Args:
    config:
      ImportToolConfig object containing configuration data
    templates:
      A dictionary of templates to create. Keys are template names. Values are
      templates.

  Returns:
    None
  """
  print('Templates for creating:')
  print(templates.keys())

  for template_id, template in templates.items():

    target_template = datacatalog.CreateTagTemplateRequest()
    target_template.tag_template = template
    target_template.tag_template_id = template_id

    location = template.name.split('/')[3]
    target_template.parent = config.parent_target.format(location=location)

    client.create_tag_template(request=target_template)


def load_source_tags(folder: str) -> Dict[str, TagTemplate]:
  """Loads source tags as protos from a folder.

  Args:
    folder: source folder

  Returns:
    A dictionary with keys of tag names and values of tag definitions.
  """

  path = f'/{folder}/{FILES_FOLDER}/'
  templates = DeploymentUtils.proto_load(
      path, '*.gcp_proto', proto_class=TagTemplate)

  templates = {v.name.split('/')[-1]: v for _, v in templates.items()}

  return templates


def main(args: argparse.Namespace) -> None:
  """Import data catalog.

  Imports data catalog templates to a project. Script used for SYNC/MERGE of
  DLP templates.

  Args:
    args: command input parameters.
      env_target - target GCP project
      mode - how the script should handle existing data (SYNC or MERGE)
      env_config_file - environment configuration file
      folder - repo folder

  Returns:
    None
  """
  env_target = args.env_target.lower()
  mode = args.mode.lower()
  env_config_file = args.env_config_file
  folder = args.folder

  config = ImportToolConfig(env_target=env_target,
                            env_config_file=env_config_file)

  Auth.auth()

  source_tags = load_source_tags(folder=folder)
  target_tags = get_dic_tag_templates(project_id=config.gcp_environment_target)

  mode = Mode[mode]

  is_already_deleted = False
  if mode == Mode.sync:
    print('Deleting target tags for mode SYNC:')
    print(target_tags.keys())
    delete_templates(config=config, templates=target_tags)
    is_already_deleted = True

  sync_templates(
      config=config,
      source_templates=source_tags,
      target_templates=target_tags,
      is_already_deleted=is_already_deleted)


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

    main(ap.parse_args())
  except:
    print(f'{ForeColor.RED}Unexpected error:{ForeColor.RESET}',
          sys.exc_info()[1])
    raise
