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

"""Implementation of functionality for work with GCP Composer environments.

Implementation of functionality for shell commands execution and for grizzly
DAG-domain code deployment to target GCP Composer environment.

Typical usage example:
  gcp_composer_environment = ComposerEnvironment(
      project_id='my_gcp_project',
      location='us-central1',
      environment_name='dev')
  deployment_scope = Scope('my_grizzly_project_path')
  deployment_scope.generate_stagging_files()
  deployment_scope.generate_DAG_file(TEMPLATE_PATH / 'dag.py.jinja2')
  gcp_composer_environment.publish_scope(deployment_scope)
"""

import subprocess
from typing import Any, List
from grizzly.forecolor import ForeColor
from grizzly.grizzly_exception import GrizzlyException
from grizzly.scope import Scope
import yaml


class ComposerEnvironment():
  """Interract with GCP composer and retrieve Environment details.

  Attributes:
    project_id (string): GCP project Id.
    location (string): Compute Engine region in which composer environment was
      created.
    environment_name(string): GCP Composer environment name.
    details (dictionary): GCP Composer environment details.
    dag_folder (string): GCP Composer DAGs folder.
    gs_bucket (string): Google Storage bucket used by GCP Composer environment.
    etl_folder (string): Reference to folder with ETL configuration files.
      This folder is stored in a bucket defined in gs_bucket.
  """

  def __init__(self, project_id: str, location: str,
               environment_name: str) -> None:
    """Initialize the instance of of composer environment.

    Args:
      project_id (string): GCP project Id.
      location (string): Compute Engine region in which composer environment was
        created.
      environment_name (string): GCP Composer environment name.
    """
    self.project_id = project_id
    self.location = location
    self.environment_name = environment_name
    # get details about a Cloud Composer environment
    composer_cmd = [
        'gcloud', 'composer', 'environments', 'describe', '--project',
        project_id, environment_name, '--location', location
    ]
    cmd_result = self.run_command(composer_cmd)
    self.details = yaml.safe_load(cmd_result)
    self.dag_folder = self.details['config']['dagGcsPrefix']
    self.gs_bucket = self.dag_folder.replace('gs://', '').replace('/dags', '')
    self.etl_folder = f'gs://{self.gs_bucket}/data/ETL'
    return

  def __getattr__(self, name: str) -> Any:
    """Get GCP Composer environment property(attribute).

    Args:
      name (string): Name of GCP Composer environment property.

    Returns:
      (Any) GCP Composer environment property.
    """
    return self.details['config'][name]

  def run_command(self, arguments: List[str]) -> str:
    """Run bash command with arguments and return command output.

    Run gcloud composer command. Return command output.

    Args:
      arguments (List[string]): List of strings with definition of command to
        be executed.
        For example: ['gcloud', 'composer', 'environments', 'storage', 'data'].

    Returns:
      (string) Return command output as a text.

    Raises:
      GrizzlyException: An error occurred in case if bash command failed.
    """
    cmd_result = subprocess.run(
        arguments,
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE)
    if cmd_result.returncode != 0:
      raise GrizzlyException(
          f'{ForeColor.RED}{cmd_result.stderr.decode("utf-8")}{ForeColor.RESET}'
      )
    return cmd_result.stdout.decode('utf-8')

  def publish_scope(self, scope: Scope) -> None:
    """Publish ETL configuration files into GCP Composer environment.

    Files will be published into gs://{Airflow_Bucket}/data/ETL

    Args:
      scope (grizzly.scope.Scope): Instance of Scope that contains ETL
        definition for DAG with all yml, sql, json, etc. files to be deployed on
        GCP Composer environment.
    """
    print(f'Copying files from {scope.temp_path} to {self.etl_folder}')
    # Clean-up target GS location from previous instalations
    composer_cmd = [
        'gcloud', 'composer', 'environments', 'storage', 'data', 'delete',
        f'ETL/{scope.config.domain_name}', '--project', self.project_id,
        '--environment', self.environment_name, '--location', self.location,
        '--quiet'
    ]
    cmd_result = self.run_command(composer_cmd)
    print(cmd_result)
    # Copy files from TEMP to GS_Bucket
    composer_cmd = [
        'gcloud', 'composer', 'environments', 'storage', 'data', 'import',
        f'--source=/{scope.temp_path}', '--project', self.project_id,
        '--environment', self.environment_name, '--location', self.location,
        '--destination=ETL'
    ]
    cmd_result = self.run_command(composer_cmd)
    print(cmd_result)
    # Copy DAG file to GS_Bucket
    dag_file_name = scope.config.domain_name + '.py'
    composer_cmd = [
        'gcloud', 'composer', 'environments', 'storage', 'dags', 'import',
        f'--source=/{scope.temp_path / dag_file_name}', '--project',
        self.project_id, '--environment', self.environment_name, '--location',
        self.location
    ]
    cmd_result = self.run_command(composer_cmd)
    print(cmd_result)
    return

  def publish_file(self, domain: str, file: str) -> None:
    """Publish ETL configuration files into GCP Composer environment.

    Files published into gs://{Airflow_Bucket}/data/ETL

    Args:
      domain (str): domain of destination
      file (str): file name for copy
    """
    print(f'Copying files from {file} to {self.etl_folder}')
    # Copy files from TEMP to GS_Bucket
    composer_cmd = [
        'gcloud', 'composer', 'environments', 'storage', 'data', 'import',
        f'--source=/{file}', '--project', self.project_id,
        '--environment', self.environment_name, '--location', self.location,
        f'--destination=ETL/{domain}'
    ]
    cmd_result = self.run_command(composer_cmd)
    print(cmd_result)
    return
