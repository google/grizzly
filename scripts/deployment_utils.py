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

"""Module contains functionality for work with PROTO files.

Classes and methods from this module are used in import/export scripts for
export GCP configurations into PROTO file stored in GIT and for import
configurations from GIT proto files into GCP.

Typical usage example:
  from deployment_utils import Utils as DeploymentUtils
  file_name = DeploymentUtils.proto_save(
      obj=template,
      class_message=TagTemplate,
      file_name=f'{name}.gcp_proto',
      path=dest_folder)
"""

import datetime
import glob
import os
from typing import Any, Dict, List

import google.auth.transport.requests
from google.cloud.devtools import cloudbuild_v1
from google.cloud import bigquery
from google.cloud.bigquery import Table
import proto
import yaml


class Utils:
  """Methods for work with files in PROTO format.

  This class is used by deployment scripts responsible for upload and deployment
  of DataCatalog and DLP configurations.
  """

  @classmethod
  def remove_files(cls, path: str, files_filter: str) -> List[Any]:
    """Remove files in folder.

    Args:
      path (str): Path to folder to be cleaned.
      files_filter (str): File  mask.

    Returns:
      (List[Any]): List of deleted files.
    """
    deleted_files = []

    files = glob.glob(f'{path}/{files_filter}')
    for f in files:
      os.remove(f)
      print(f'Removed file: {f}')
      deleted_files.append(f)

    return deleted_files

  @classmethod
  def proto_save(cls, obj: proto.Message, class_message: Any,
                 file_name: str, path: str) -> str:
    """Save proto file into GIT repository.

    Args:
      obj (proto.Message): GCP object definition in PROTO format.
      class_message (Any): Reference to GCP class used.
      file_name (str): Target file name.
      path (str): Target file location.

    Returns:
      (str): Full path to file generated.
    """

    f = open(f'{path}/{file_name}', 'w+')
    json_data = class_message.to_json(obj)
    f.write(json_data)
    f.close()

    return f'{path}/{file_name}'

  @classmethod
  def proto_load(cls, path: str, file_filter: str,
                 proto_class: Any) -> Dict[str, Any]:
    """Load file from GIT in proto format into GCP.

    Args:
      path (str): Path to files to be loaded.
      file_filter (str): File names mask.
      proto_class: Reference to GCP class used.

    Returns:
      (Dict[str, Any]): Object in PROTO format loaded from file.
    """
    proto_messages = {}

    path = f'{path}/'

    for filename in glob.glob(os.path.join(path, file_filter)):
      with open(os.path.join(os.getcwd(), filename), 'r') as f:
        message: proto_class = proto_class.from_json(f.read())
        proto_messages[filename] = message

    return proto_messages

  @classmethod
  def yaml_load(cls, path: str, file_filter: str) -> Dict[str, Any]:
    """Load yaml file and return it as Dictionary.

    Args:
      path (str): Path to files to be loaded.
      file_filter (str): File names mask.

    Returns:
      (Dict[str, Any]): Content of files loaded as dictionary.
    """
    files = cls.get_files_by_filter(path=path, file_filter=file_filter)
    res = {filename: cls.parse_yml_file(filename) for filename in files}
    return res

  @classmethod
  def get_files_by_filter(cls, path: str, file_filter: str) -> List[Any]:
    """Return files by path and filter.

    Args:
      path (str): Path to files to be loaded.
      file_filter (str): File names mask.

    Returns:
      (List[Any]): List of files in folder.
    """
    walk_dir = path

    print('walk_dir = ' + walk_dir)

    # If your current working directory may change during
    # script execution, it's recommended to
    # immediately convert program arguments to an absolute path.
    # Then the variable root below will
    # be an absolute path as well. Example:
    walk_dir = os.path.abspath(walk_dir)
    print(f'walk_dir (absolute) = {walk_dir}/{file_filter}')

    files = glob.glob(f'{walk_dir}/{file_filter}', recursive=True)

    return files

  @classmethod
  def files_load(cls, path: str, file_filter: str) -> Dict[str, Any]:
    """Load files.

    Generate a dictionary of content of files in folder.

    Args:
      path (str): Path to files to be loaded.
      file_filter (str): File names mask.

    Returns:
      (Dict[str, Any]): Dictionary with files content.
    """

    files = cls.get_files_by_filter(path=path, file_filter=file_filter)

    res = {filename: open(filename, 'rb').read().decode('utf-8')
           for filename in files}

    return res

  @classmethod
  def parse_yml_file(cls, yml_file):
    """Parse yaml fiiles."""

    with open(yml_file) as f:
      yml_values = yaml.safe_load(f.read())
    return yml_values

  @classmethod
  def auth(cls, scopes=None):
    """GCP Authentication."""

    if not scopes:
      scopes = ['https://www.googleapis.com/auth/cloud-platform']

    credentials, _ = google.auth.default(scopes=scopes)

    auth_req = google.auth.transport.requests.Request()
    credentials.refresh(auth_req)

  @classmethod
  def get_bq_client(cls, project):
    """Return instance of BQ client for GCP project."""
    return bigquery.Client(project=project)


class BQUtils:
  """Provides basic functionality to work with BQ.

  Attributes:
    gcp_project_id (string): GCP project id.
    bq_client (Any): Instance of BQ Client.
  """

  def __init__(self, gcp_project_id: str) -> None:
    """Initialize BQUtils item.

    Args:
      gcp_project_id (str): GCP project.
    """
    Utils.auth()
    self.gcp_project_id = gcp_project_id
    self.bq_client = Utils.get_bq_client(project=self.gcp_project_id)

  def create_temp_table(self,
                        table_name: str,
                        table_schema: Any) -> Table:
    """Create temorary table.

    Created temporary table for requested target table.

    Args:
      table_name (str): Target table name.
      table_schema (Any): Definition of BQ table schema.

    Returns:
      (Table): Instance of BQ table created.
    """
    suffix: str = datetime.datetime.now().strftime('%m%d%Y%H%M%S')
    table_name: str = f'{table_name}_{suffix}'
    ret: Table = self.create_table(table_name=table_name,
                                   table_schema=table_schema)
    return ret

  def create_table(self,
                   table_name: str,
                   table_schema: Any,
                   exists_ok: bool = True) -> Table:
    """Create BQ table.

    Args:
      table_name (str): Target table name.
      table_schema (Any): Definition of BQ table schema.
      exists_ok (bool): If True, ignore "already exists" errors when creating
        the table.

    Returns:
      (Table): Instance of BQ table created.
    """

    table_name_ref = f'{self.gcp_project_id}.{table_name}'
    table: Table = bigquery.Table(table_ref=table_name_ref,
                                  schema=table_schema)
    ret: Table = self.bq_client.create_table(table=table,
                                             exists_ok=exists_ok)
    return ret

  def create_tables(self,
                    table_name: str,
                    tmp_table_name: str,
                    table_schema,
                    tmp_table_schema = None,
                    exists_ok: bool = True):
    """Create tables tmp & traditional."""

    table = self.create_table(table_name=table_name,
                              table_schema=table_schema,
                              exists_ok=exists_ok)

    if tmp_table_schema:
      tmp_table = self.create_temp_table(table_name=tmp_table_name,
                                        table_schema=tmp_table_schema)
    else:
      tmp_table = self.create_temp_table(table_name=tmp_table_name,
                                        table_schema=table_schema)

    return table, tmp_table             
            


class ImportToolConfig:
  """Import tool configuration class.

  Attributes:
    env (str): Environment name (dev, uat, prod).
    env_config_file (str): Path to ENVIRONMENT_CONFIGURATION.yml file.
    gcp_environment_target (Any): Parameter value from ENVIRONMENT_CONFIGURATION
      file.
  """

  def __init__(self,
               env,
               env_config_file,
               env_tag_name: str = 'GCP_ENVIRONMENT') -> None:
    """Initializing class.

    Args:
      env (str): environment name
      env_config_file (str): config file name with environments
      env_tag_name (str): the tag name with information about GCP Environment
    """
    self.env = env
    self.env_config_file = env_config_file

    self.gcp_environment_target = self._get_environment_configuration(
        env_configs_file=env_config_file,
        environment=env,
        env_tag_name=env_tag_name)

  def _get_environment_configuration(self,
                                     env_configs_file: str,
                                     environment: str,
                                     env_tag_name: str) -> Any:
    """Getting the environment configuration.

    Parse ENVIRONMENT_CONFIGURATIONS.yml file and return its content as a
    dictionary.

    Args:
      env_configs_file (str): Path to environment configuration file.
      environment (str): Environment name (dev, uat, prod).
        Reference to environment to be used.
      env_tag_name (str): Parameter from environment configuration file to be
        extracted for given environment name.

    Returns:
      (Any): Value of parameter from environment configuration file.
    """
    env_config = Utils.parse_yml_file(f'{env_configs_file}')
    return env_config[environment][env_tag_name]


class Trigger:
  """Represents Cloud Build trigger configuration.

  Configures, runs, and monitors CB trigger run.

  Attributes:
    gcp_project (str): GCP project id.
    branch (str): branch name to run trigger on.
    trigger_name (str): name of the trigger to run.
    substitutions (Dict[str, Any], default - None): dictionary of substitutions
      for trigger. Keys are the names of substituted variables,
      and variables are substitutions.

    _request (RunBuildTriggerRequest): request that will be passed to a client
    _failed (bool): whether the run has failed.
    _cancelled (bool): whether the run was cancelled.
    _operation (cloudbuild_v1.operation.Operation): operation object returned
      by CB client. Contains the status and, eventually, the result.
  """

  def __init__(self, 
               client,
               gcp_project: str,
               branch: str, trigger_name: str,
               substitutions: Dict[str, Any] = None) -> None:
    """Initialize trigger."""
    self.client = client
    self.gcp_project = gcp_project
    self.branch = branch
    self.trigger_name = trigger_name
    self.substitutions = substitutions

    self._operation = None

    # construct request
    full_trigger_name = 'projects/{}/locations/global/triggers/{}'.format(
        self.gcp_project, self.trigger_name)

    self._request = cloudbuild_v1.RunBuildTriggerRequest({
        'name': full_trigger_name,
        'project_id': self.gcp_project,
        'source': {
            'project_id': self.gcp_project,
            'branch_name': self.branch,
            'substitutions': self.substitutions,
        }
    })

  def run_trigger(self):
    """Runs and monitors the status of CB trigger."""
    self._operation = self.client.run_build_trigger(request=self._request)

    return self._operation

  def __str__(self):
    """Formatting for printing."""
    return 'Trigger {} on branch {} with substitutions {}'.format(
        self.trigger_name, self.branch, self.substitutions)
