# Copyright 2021 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the 'License');
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an 'AS IS' BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Grizzly DLP operator module.

  This module contains DLPTaskConfig and GrizzlyDLPOperator.
  DLPTaskConfig contains configuration parameters parsed from yaml file and
  Airflow task config. It is used only inside GrizzlyDLPOperator.
  GrizzlyDLPOperator is a custom Airflow operator that can create DLP jobs.
  It is inherited from airflow.models.BaseOperator.

  Typical usage example:

  dlp_task = GrizzlyDLPOperator(
      dag=main_dag
      ,task_id='task.id'
      ,config_file_ref='config.yml'
      ,execution_timeout=timedelta(seconds=1200)
  )
"""

import datetime
import enum
import json
import pathlib
from typing import Any, Dict

from airflow.exceptions import AirflowSkipException
from airflow.models import BaseOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.hooks.dlp import CloudDLPHook
from google.protobuf import timestamp_pb2
from grizzly.etl_action import check_custom_schedule
from grizzly.etl_action import parse_table
from grizzly.execution_log import ExecutionLog
from grizzly.task_instance import TaskInstance
import jinja2
import pendulum


TEMPLATE_FOLDER = pathlib.Path('/home/airflow/gcs/plugins/templates')


class DLPTaskConfig(TaskInstance):
  """Represents DLP task configuration yaml file.

  Also contains StorageType Enum and constants:
    ATTEMPT_PREFIX (str): the prefix that goes before attempt number in job
      name.
    DATETIME_FORMAT (str): the formatting string for job timestamp in the name.

  Attributes:
    execution_date (pendulum.Datetime): current execution date retrieved from
      the task context.
    name (str): name of the DLP job, which is derived from the task name.
    id (str): id of the DLP job. Consists of name, timestamp, and attempt
      number.
    template_name (str): name of the DLP inspection template.
    timestamp_field (str, None): the field for additive scanning of the data.
      If not specified, additive scans will not be performed. If the storage
      type is BigQuery, will represent the name of the timestamp field for
      additive scans. If the storage type is Cloud Storage, presence of this
      parameter in config will indicate the need of additive scans, however,
      the actual parameter value will be ignored.
    target_table (Dict): BigQuery table where the results of the DLP job will be
      saved.
    storage_type (StorageType): type of the source storage. Can be BigQuery or
      Cloud Storage.
    source_table (Dict): source table name if the storage type is BigQuery.
    source_gs (str): source Cloud Storage URL if the storage type is
      Cloud Storage.
  """
  ATTEMPT_PREFIX = '_attempt'
  DATETIME_FORMAT = '%m-%d-%Y--%H-%M-%S'

  class StorageType(enum.Enum):
    BIGQUERY = 'BIGQUERY'
    CLOUD_STORAGE = 'CLOUD_STORAGE'

  def __init__(self, task_config_path: str, context: Any) -> None:
    """Initialize the DLP config.

    Args:
      task_config_path (str): path to the task config file.
      context (Any): Instance of GrizzlyOperator executed.
    """
    super().__init__(task_config_path, context)
    self._dlp_inspect_config = self.get_raw_config_value('dlp_inspect_config')
    self.execution_date = self.get_context_value('execution_date')

    # generate the task name by removing the dots, which are not allowed in the
    # DLP task id
    self.name = self._context['task'].task_id.replace('.', '-')
    self.id = self.name + '_' + self.execution_date.strftime(
        self.DATETIME_FORMAT
    )
    self.template_name = self._dlp_inspect_config['template_name']

    self.timestamp_field = self._dlp_inspect_config.get('timestamp_field', None)
    self.target_table = parse_table(self._raw_config['target_table_name'])

    # convert storage type to custom Enum class
    self.storage_type = self.StorageType[
        self._dlp_inspect_config['storage_type'].upper()
    ]
    if self.storage_type == self.StorageType.BIGQUERY:
      # retrieve the source table details if BIGQUERY
      self.source_table = parse_table(
          self._dlp_inspect_config['source_table_name'])
    else:
      # otherwise, retrieve the source_gs
      self.source_gs = self._dlp_inspect_config['source_gs_url']

  def append_attempt_number(self, n: int) -> None:
    """Add attempt number to the task ID.

    Only adds it if it is greater than 1.
    Before the attempt number, ATTEMPT_PREFIX will be placed.

    Args:
      n (int): attempt number.
    Returns:
      None
    """
    if n != 1:
      self.id += f'{self.ATTEMPT_PREFIX}{n}'


class GrizzlyDLPOperator(BaseOperator):
  """Implementation of Grizzly DLP Airflow operator.

  Create an inspect DLP job.

  Attributes:
    _config_file_ref (str): path to the task yaml config file
    config (DLPTaskConfig): task config.
    dlp_hook (airflow.providers.google.cloud.hooks.dlp. CloudDLPHook): hook to
      interact with DLP API.
    bq_hook (airflow.providers.google.cloud.hooks.bigquery.BigQueryHook):
      Interact with BigQuery. This hook uses the Google Cloud Platform
      connection.
    bq_client (google.cloud.bigquery.Client): BigQuery client object.
    etl_log (grizzly.execution_log.ExecutionLog): Instance of ExecutionLog
      with detailed information about task execution. This object is used for
      dumping of execution logs into [etl_log.composer_job_details] table.
  """

  def __init__(self, config_file_ref: str, *args: Any, **kwargs: Any) -> None:
    """Initialize the Operator."""
    super(GrizzlyDLPOperator, self).__init__(*args, **kwargs)
    self._config_file_ref = config_file_ref
    # Attributes below be initialized until execute is called
    self.task_config = None
    self.dlp_hook = None
    self.bq_hook = None
    self.bq_client = None
    self.etl_log = None

  def is_first_run(self) -> bool:
    """Checks if a job with the same config has run before.

    Returns:
      (bool): True if this job is the first one of its kind.
    """
    def get_creation_time_from_id(job_id: str) -> datetime.datetime:
      """Helper function to parse creation timestamp from job ID.

      Args:
        job_id (str): job ID.
      Returns:
        (datetime.datetime): job creation timestamp.
      """
      # remove name and '_' separating character from job id
      ts_string = job_id.split(self.task_config.name + '_')[1]
      # remove the attempt number from job id
      ts_string = ts_string.split(self.task_config.ATTEMPT_PREFIX)[0]
      # parse datetime from ts_string based on format specified in config
      return datetime.datetime.strptime(ts_string,
                                        self.task_config.DATETIME_FORMAT)

    # get all DLP jobs using the hook
    jobs = self.dlp_hook.list_dlp_jobs(
        project_id=self.task_config.gcp_project_id)
    # extract the job IDs and keep them only if they are related to the current
    # job
    job_ids = [
        j.name for j in jobs if j.name.split('/')[-1].startswith(
            'i-' + self.task_config.name
        )
    ]
    # get a sorted list of job creation times
    creation_times = sorted([get_creation_time_from_id(n) for n in job_ids])

    if creation_times:
      # if other related job exist, compare current time with the oldest time
      # this is needed in case user wants to rerun the first job in airflow
      # after other runs have occurred
      return get_creation_time_from_id(self.task_config.id) <= creation_times[0]
    else:
      # if this is the only job of its kind, return True
      return True

  def check_attempt_number(self) -> int:
    """Get the attempt number.

    Checks if this job is a rerun of already created job and gets attempt
      number.

    Returns:
      (int): attempt number.
    """
    def get_attempt_number(job_id: str) -> int:
      """Helper function to parse attempt number from job ID.

      Args:
        job_id (str): job ID.
      Returns:
        (int): attempt number.
      """
      if self.task_config.ATTEMPT_PREFIX in job_id:
        # if the ATTEMPT_PREFIX is present in job_id, extract attempt number
        return int(job_id.split(self.task_config.ATTEMPT_PREFIX)[-1])
      else:
        # absence of ATTEMPT_PREFIX means that this is the first attempt
        return 1

    # get all DLP jobs using the hook
    jobs = self.dlp_hook.list_dlp_jobs(
        project_id=self.task_config.gcp_project_id)
    # extract the attempt numbers and keep them only if they are related to the
    # current job
    attempts = [get_attempt_number(j.name) for j in jobs if
                j.name.split('/')[-1].startswith('i-' + self.task_config.id)]
    # if attempts are not empty, calculate current attempt number, otherwise
    # return 1
    return max(attempts) + 1 if attempts else 1

  def create_timespan_config(self) -> Dict[str, Any]:
    """Creates timespan config.

    Returns:
      (Dict[str, Any]): timespan config.
    """
    def timestamp_dict_from_datetime(
        date_time: pendulum.DateTime) -> Dict[str, Any]:
      """Helper function to create timestamp dict from datetime.

      Args:
        date_time (pendulum.DateTime): datetime to convert.
      Returns:
        (Dict[str, Any]): timestamp dict.
      """
      timestamp = timestamp_pb2.Timestamp()
      timestamp.FromDatetime(date_time)
      return {'seconds': timestamp.seconds, 'nanos': timestamp.nanos}

    execution_date: pendulum.DateTime = self.task_config.execution_date
    prev_execution_date: pendulum.DateTime = self.task_config.get_context_value(
        'prev_execution_date'
    )
    timespan_config = {'timestamp_field': self.task_config.timestamp_field}

    if prev_execution_date is not None and not self.is_first_run():
      # define start_time only if this is not a first run
      timespan_config['start_time'] = timestamp_dict_from_datetime(
          prev_execution_date
      )
    timespan_config['end_time'] = timestamp_dict_from_datetime(execution_date)

    return timespan_config

  def create_dlp_job_config(self) -> Dict[str, Any]:
    """Creates DLP inspect job config.

    Returns:
      (Dict[str, Any]): DLP inspect job config.
    """
    # load jinja template
    template_path = TEMPLATE_FOLDER / 'inspect_job.jinja2'
    loader = jinja2.FileSystemLoader(template_path.parent)
    jinja_env = jinja2.Environment(loader=loader)
    jinja_template = jinja_env.get_template(template_path.name)

    # add attempt number to name if necessary
    self.task_config.append_attempt_number(self.check_attempt_number())

    jinja_args = {'job': self.task_config}
    # if timestamp_field defined in config, create timespan config
    if self.task_config.timestamp_field is not None:
      jinja_args['timespan'] = self.create_timespan_config()

    inspect_config_json = jinja_template.render(**jinja_args)
    self.log.info(inspect_config_json)

    inspect_config = json.loads(inspect_config_json)
    return inspect_config

  def execute(self, context: Any) -> None:
    """The code to execute when the runner calls the operator.

    Args:
      context: Airflow context that can be used for access Airflow config
        values.
    """
    self.task_config = DLPTaskConfig(self._config_file_ref, context)
    self.dlp_hook = CloudDLPHook()
    self.bq_hook = BigQueryHook(use_legacy_sql=False)
    self.bq_client = self.bq_hook.get_client()
    self.etl_log = ExecutionLog(self.task_config)

    try:
      check_custom_schedule(execution_context=self,
                            task_config=self.task_config)
      self.etl_log.add_step('create_dlp_inspect_job')
      self.dlp_hook.create_dlp_job(
          project_id=self.task_config.gcp_project_id,
          inspect_job=self.create_dlp_job_config(),
          job_id=self.task_config.id,
          wait_until_finished=False
      )
      self.etl_log.finish_step(job_step_status='SUCCESS', total_bytes_billed=0,
                               total_bytes_processed=0)
    except AirflowSkipException as ae:
      self.etl_log.log_flush(execution_context=self, status='SKIPPED')
      raise ae
    except Exception as ae:
      self.etl_log.log_flush(execution_context=self, status='FAILED')
      raise ae
    finally:
      self.etl_log.log_flush(execution_context=self, status='SUCCESS')

