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

"""Functionality for work with Grizzly environment configuration."""
import pathlib

from grizzly.composer_environment import ComposerEnvironment
import yaml


class DeploymentToolConfig:
  """Grizzly environment configuration.

  Atrributes:
    gcp_project_id (str): GCP Project Id.
    airflow_location (str): Compute Engine region in which composer environment
      was created.
    airflow_environmnet (str): GCP Composer environment name.
    gcp_bucket (str): Google Storage bucket used by GCP Composer environment.
    project_path (str): Domain folder.
    scope_file_path (str): Full SCOPE file path.
  """

  def __init__(self,
               scope_file: str,
               domain: str,
               environment: str,
               env_config_file: str) -> None:
    """Init deployment tools configuration.

    Args:
      scope_file (str): Scope file with a list of objects to be deployed,
        default value is 'SCOPE.yml'.
      domain (str): Domain name to be deployed, string argument passed in the
        commandline.
      environment (str): Environment name, string argument passed in the
        commandline.
      env_config_file (str): Environment file, string argument passed in the
        commandline.
    """
    env_config_file_path = pathlib.Path(env_config_file)
    # Path to folder with ENVIRONMENT_CONFIGURATIONS.yml file
    project_root_folder_path = env_config_file_path.parent
    environment_config = yaml.safe_load(
        env_config_file_path.read_text())[environment]

    self.gcp_project_id = environment_config['GCP_ENVIRONMENT']
    self.airflow_location = environment_config['AIRFLOW_LOCATION']
    self.airflow_environmnet = environment_config['AIRFLOW_ENVIRONMENT']

    composer = ComposerEnvironment(
        project_id=self.gcp_project_id,
        location=self.airflow_location,
        environment_name=self.airflow_environmnet)

    self.gcp_bucket = composer.gs_bucket

    self.project_path = project_root_folder_path / domain
    self.scope_file_path = project_root_folder_path / domain / scope_file
