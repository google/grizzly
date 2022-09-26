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
"""Export data catalog.

  Script used by Cloud Build to export data catalog templates as protos
  to a repo. Script used for SYNC/MERGE of DLP templates.

  Typical usage example:

  python3 ./export_data-catalog.py -s $BRANCH_NAME -c $$CFG -f /workspace/$$REPO
"""

import argparse
import glob
import pathlib
import sys
from typing import Dict, TypeVar, Union

from deployment_utils import Utils as DeploymentUtils
from git import Repo
from google.cloud import datacatalog_v1
from google.cloud.datacatalog_v1.types import datacatalog
from google.cloud.datacatalog_v1.types.tags import TagTemplate

import grizzly.cloud_platform_auth as Auth
from grizzly.forecolor import ForeColor
import yaml

TSearchCatalogPager = TypeVar('TSearchCatalogPager')
client = datacatalog_v1.DataCatalogClient()

FILES_FOLDER = 'data-catalog-templates'


class ExportToolConfig:
  """Class containing configuration data for the Export job.

  Attributes:
    env_source (str): Source GCP project.
    env_config_file (pathlib.Path): Environment file
    gcp_environment_source (str): environment configuration of source
  """

  def __init__(self,
               env_source: str,
               env_config_file: Union[str, pathlib.Path]) -> None:
    """Initializes ExportToolConfig.

    Args:
      env_source (str): Source GCP project.
      env_config_file (Union[str, pathlib.Path]): Environment file

    Returns:
      None
    """

    self.env_source = env_source
    if isinstance(env_config_file, str):
      self.env_config_file = pathlib.Path(env_config_file)
    environment_config = yaml.safe_load(
        self.env_config_file.read_text())[self.env_source]
    self.gcp_environment_source = environment_config['GCP_ENVIRONMENT']


def _search(project_id: str, search_string: str) -> TSearchCatalogPager:
  """Searches data catalog for resources that match a query.

  Args:
    project_id (str): ID of GCP project to search in.
    search_string (str): Query string to search

  Returns:
    SearchCatalog response message
  """

  print('Search tag templates')
  print(f'Project: {project_id}')
  print(f'Search string: {search_string}')

  # Searching project in datacatalog
  scope = datacatalog_v1.types.SearchCatalogRequest.Scope()
  scope.include_project_ids.append(project_id)
  scope.include_public_tag_templates = False

  datacatalog_data = datacatalog_v1.DataCatalogClient()
  request_dc: datacatalog_v1.types.SearchCatalogRequest = (
      datacatalog_v1.types.SearchCatalogRequest())
  request_dc.query = search_string
  request_dc.page_size = 5
  request_dc.scope = scope

  return datacatalog_data.search_catalog(request=request_dc)


def get_dic_tag_templates(project_id: str) -> Dict[str, TagTemplate]:
  """Gets DLP tag templates for a specific project.

  Args:
    project_id (str): ID of GCP project.

  Returns:
    (Dict[str, TagTemplate]): A dictionary with keys of template names and
      values of template definition.
  """
  search_results = _search(
      project_id=project_id,
      search_string=f'projectId:{project_id}, type=tag_template')

  print('Found templates')
  print(search_results)

  tags_names_set = set()
  for page in search_results.pages:
    for result in page.results:
      tags_names_set.add(result.relative_resource_name)

  tags_dic = {}
  for tag in tags_names_set:
    request_dc = datacatalog.GetTagTemplateRequest()
    request_dc.name = tag
    tag_response = client.get_tag_template(request=request_dc)
    tags_dic[tag] = tag_response

  res = {str(v.name).split('/')[-1]: v for _, v in tags_dic.items()}

  return res


def main(args: argparse.Namespace) -> None:
  """Export data catalog.

  Exports data catalog templates as protos to a Repo. Script used for
  SYNC/MERGE of DLP templates.

  Args:
    args (argparse.Namespace): arguments

  Returns:
    None
  """
  env_source = args.env_source.lower()
  env_config_file = args.env_config_file
  folder = args.folder

  config = ExportToolConfig(env_source=env_source,
                            env_config_file=env_config_file)

  Auth.auth()

  source_tags = get_dic_tag_templates(project_id=config.gcp_environment_source)

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

  for name, template in source_tags.items():
    file_name = DeploymentUtils.proto_save(
        obj=template,
        class_message=TagTemplate,
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
