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

"""Wrapper module for deploying a Dataflow job.

  Uses DeploymentToolConfig to define the dataflow job to be deployed.

  Typical usage example:

  config = DeploymentToolConfig()
  deploy_tool = DeploymentTool(config=config)
  deploy_tool.deploy()
"""

import os
import pathlib
import subprocess
import time
from typing import Any, Dict

from deployment_configuration import DeploymentToolConfig
from google.api_core.exceptions import NotFound
from google.cloud.bigquery.client import Client as BQClient
from google.cloud.bigquery.schema import SchemaField
from google.cloud.bigquery.table import Table
from grizzly.forecolor import ForeColor
from grizzly.grizzly_exception import GrizzlyException
import yaml


class DeploymentTool:
  """Wrapper class for deploying a Dataflow Job.

  Attributes:
    config (DeploymentToolConfig): DeploymentToolConfig defines the dataflow
      job to be deployed.
    client (google.cloud.bigquery.client.Client): BigQuery client
  """

  def __init__(self, config: DeploymentToolConfig) -> None:
    """Initializes DeploymentTool.

    Args:
      config (DeploymentToolConfig): DeploymentToolConfig defines the dataflow
        job to be deployed.
    """
    self.config = config
    self.client: BQClient = BQClient(project=config.gcp_project_id)

  def _is_table_exists(self, deployment_params: Dict[str, Any]) -> bool:
    """Checks if BigQuery Table exists.

    Args:
      deployment_params (Dict[str, Any]): dictionary from yaml file

    Returns:
      True or False
    """
    try:
      self.client.get_table(deployment_params['target_table_name'])
    except NotFound as ex:
      return False
    except Exception as ex:
      raise ex
    return True

  def _create_table(self, deployment_params: Dict[str, Any]) -> None:
    """Creates BigQuery Table.

    Args:
      deployment_params (Dict[str, Any]): Dictionary from yaml file.
    """
    target_table_name = deployment_params['target_table_name']
    print(f'Creating table {target_table_name}')

    fields = []
    for name, value in deployment_params['fields'].items():
      nullable = 'NULLABLE' if value['is_nullable'] else 'REQUIRED'
      field = SchemaField(
          name=name, field_type=value['field_type'], mode=nullable)
      fields.append(field)

    table = Table(f'{self.config.gcp_project_id}.{target_table_name}', fields)

    self.client.create_table(table)

  def _is_config_table_differ(self, deployment_params: Dict[str, Any]) -> bool:
    """Checks if existing table matches desired table defined in config.

    Args:
      deployment_params (Dict[str, Any]): Dictionary from yaml file.
    Returns:
      True/False of table metadata comparison
    """
    table = self.client.get_table(deployment_params['target_table_name'])
    print(table.schema)

    existing_fields = [
        f'{f.name.lower(),str(f.is_nullable).lower(),f.field_type.lower()}'
        for f in table.schema
    ]
    existing_fields.sort(reverse=False)
    existing_fields_str = '|'.join(existing_fields)

    print('Existing fields:')
    print(existing_fields_str)

    fields = deployment_params['fields']
    desirable_fields = [
        f'{k.lower(),str(v["is_nullable"]).lower(),v["field_type"].lower()}'
        for k, v in fields.items()
    ]
    desirable_fields.sort(reverse=False)
    desirable_fields_str = '|'.join(desirable_fields)

    print('Desirable fields:')
    print(desirable_fields_str)

    return existing_fields_str == desirable_fields_str

  def _deploy_bq_table(self, deployment_params: Dict[str, Any]) -> None:
    """Creates table if it does not exist or does not match desired schema.

    Args:
      deployment_params (Dict[str, Any]): Dictionary from yaml file.
    """
    target_table_name = deployment_params['target_table_name']

    if self._is_table_exists(deployment_params):

      if not self._is_config_table_differ(deployment_params):
        print('Existing and desirable fields '
              'are NOT identical, changes are needed.')
        self._create_table(deployment_params)

      else:
        print('Existing and desirable fields are identical, '
              'no changes are needed.')

    else:
      print(f'Table {target_table_name} is not found.')
      self._create_table(deployment_params)

  def _get_topic_name(self, deployment_params: Dict[str, Any]) -> str:
    """Gets topic from deployment_params dictionary.

    Args:
      deployment_params (Dict[str, Any]): Dictionary from yaml file.

    Returns:
      (str): A string containing the topic name.
    """
    topic = deployment_params['topic']
    return topic

  def _get_data_flow_job(self, name: str) -> Dict[str, Any]:
    """Lists job and status of a specific Dataflow job.

    Args:
      name (str): name of dataflow job of interest

    Returns:
      An object listing dataflow jobs in the current project
    """
    check_cmd = f"""
                  gcloud dataflow jobs list \
                  --format=yaml \
                  --project={self.config.gcp_project_id} \
                  --status=active
                  --region=us-central1 \
                  --filter="name={name}"
                 """

    print('Executing command:')
    print(check_cmd)
    result = subprocess.run(
        check_cmd, capture_output=True, text=True, shell=True, check=False)
    return yaml.safe_load(result.stdout)

  def _cancel_data_flow_job(self, job: Dict[str, Any]) -> None:
    """Cancels a dataflow job.

    Args:
      job (Dict[str, Any]): An object returned from _get_data_flow_job
    """
    if (job and job['state'] == 'Running'):
      print(f'Trying to cancel the job = {job["id"]}')
      cancel_cmd = f"""
                  gcloud dataflow jobs cancel {job["id"]} \
                  --project={self.config.gcp_project_id} \
                  --region=us-central1
                 """
      print(cancel_cmd)
      result = subprocess.run(
          cancel_cmd, capture_output=True, text=True, shell=True, check=False)
      print('Result:')
      print(result)

      print('Awaiting termination dataflow 5 minutes...')
      time.sleep(300)

    elif (job and job['state'] != 'Running'):
      print(f'Job id = { job["id"]} has already state = {job["state"]}')

  def deploy(self) -> None:
    """Deploys a DeploymentTool job to Dataflow.

    Raises:
      (GrizzlyException): Dataflow deploy has failed
    """
    # get files for deployment
    scope_config = yaml.safe_load(self.config.scope_file_path.read_text())

    print('scope_config -> ')
    print(scope_config)

    file_list = [
        f'{self.config.project_path}/{f}.yml'
        for f in scope_config['SUBSCRIBE']
    ]
    print('List of files to be deployed:')

    for f in file_list:
      print(f'  - {f}')

    # deploy files

    temporary_bucket_folder = f'{self.config.gcp_bucket}/'
    temporary_bucket_folder += f'{scope_config["temporary_bucket_folder"]}'

    for f in file_list:
      name = f.split('/')[-1].replace('.yml', '')
      deployment_params = yaml.safe_load(pathlib.Path(f).read_text())
      print(deployment_params)

      self._deploy_bq_table(deployment_params)

      input_topic = self._get_topic_name(deployment_params)

      job = self._get_data_flow_job(name)

      if not job:
        print('not found dataflow jobs')
      else:
        print('Found dataflow jobs:')
        print(job)

      # trying to cancel the job
      self._cancel_data_flow_job(job=job)

      print('Deployment new dataflow job')
      source_pubsub_to_bq = (
          'gs://dataflow-templates-us-central1/latest/PubSub_to_BigQuery')
      dataflow_cmd_parameters = (
          f'inputTopic={input_topic},'
          f'outputTableSpec={self.config.gcp_project_id}:'
          f'{deployment_params["target_table_name"]}'
      )
      deployment_cmd = f"""
          gcloud config set project {self.config.gcp_project_id} &&
          gcloud dataflow jobs run {name} \
          --gcs-location {source_pubsub_to_bq} \
          --region us-central1 \
          --staging-location gs://{temporary_bucket_folder}/dataflow-{name} \
          --parameters {dataflow_cmd_parameters}
          """

      print(f'Executing: {deployment_cmd}')
      err = os.system(deployment_cmd)
      if err != 0:
        raise GrizzlyException(
            f'{ForeColor.RED}Sorry, Could not execute bq deploy'
            f'for {f}{ForeColor.RESET}.'
        )
