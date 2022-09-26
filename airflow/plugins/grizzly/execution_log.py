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

"""Functionality for work with Grizzly execution log.

  Library contains decorators and classes to work with logs to support
  auditability. Saves technical information about the ETL step into BQ table
  [etl_log.composer_job_details].

  Typical usage example:

  @etl_step
  def function1 (...)
"""

import functools
import inspect
from typing import Any, Callable, Optional, Union
from grizzly.config import Config as GrizzlyConfig
from grizzly.grizzly_typing import TGrizzlyDLPOperator
from grizzly.grizzly_typing import TGrizzlyOperator
from grizzly.grizzly_typing import TGrizzlyTaskConfig
from grizzly.grizzly_typing import TQueryJob
import pendulum


def etl_step(func: Callable[..., Any]) -> Callable[..., Any]:
  """Decorator for logging of etl steps.

  Decoreator reads BQ job execution statistic and prepared ETL job step details.
  This decorator requires next parameters to be passed to ETL step function or
  method:
  etl_log (): Instance of ExecutionLog.
  job_step_name (string): Name of ETL step.

  Args:
    func (Callable[..., Any]): function to be wrapped by decorator.

  Returns:
    (Callable[..., Any]): Job execution statistics returned by func.
  """

  @functools.wraps(func)
  def wrapper(*args, **kwargs) -> TQueryJob:
    # get job_step_name from function parameters use default
    # if was not specified during call
    job_step_name = kwargs.get(
        'job_step_name',
        inspect.signature(func).parameters['job_step_name'].default)
    kwargs['etl_log'].add_step(job_step_name)
    job_stat = func(*args, **kwargs)
    # update status for latest inserted step
    total_bytes_billed = getattr(job_stat, 'total_bytes_billed', None)
    total_bytes_processed = getattr(job_stat, 'total_bytes_processed', None)
    kwargs['etl_log'].finish_step(
        job_step_status='SUCCESS',
        total_bytes_billed=total_bytes_billed,
        total_bytes_processed=total_bytes_processed,
    )
    return job_stat

  return wrapper


class ExecutionLog():
  """Grizzly execution log record.

  During execution of Grizzly Operator each instance of ExecutionLog updated
  with execution statistic.
  Each execution of Grizzly task generates row in [etl_log.composer_job_details]
  BQ table.

  Attributes:
    job_start_timestamp (string): Time when task was started.
    job_id (int): DAG run Id. All tasks executed during DAG run will have
      the same job_id.
    job_name (string): Airflow task name.
    job_end_timestamp (string): Time when  Airflow task completed or failed.
    job_status (string): Airflow Task execution status. Could be SKIPPED,
      FAILED or SUCCESS.
    job_write_mode (string): Grizzly write mode. [job_write_mode] parameter from
      task YML file.
    job_schedule_interval (string): DAG/Domain schedulte interval. Contains
      information from [schedule_interval] parameter from SCOPE.yml or from
      task YML file.
    job_parameter_file (string): Information from task YML file. If yml file
      contains any JINJA2 templates they will be stored in parsed form.
      Information here stored in a form used by Grizzly operator.
    job_step: List of ETL job steps with job  execution statistic.
    subject_area (string): DAG name. This value is equal to your Domain name.
    target_table (string): Target table name. Attribute [target_table_name] from
      task YML file.
    target_hx_loading_indicator (string): Target history table loading
      indicator. Could be Y or N.
    stage_loading_query (string): Data loading query. Query from file referenced
      in attribute [stage_loading_query] in task YML file.
    source_table (list[string]): list of source table used in
      stage_loading_query. Only table names wrapped with ` or [] are considered
      as source tables.
    job_build_id (string): Airflow task name.
    target_audit_indicator (string): Target audit table loading
      indicator. Could be Y or N.
    subject_area_build_id (string): Subject area build id.
  """

  def __init__(self, task_config: TGrizzlyTaskConfig) -> None:
    """Init instance of ExecutionLog."""
    self.job_start_timestamp = str(pendulum.now())
    self.job_id = task_config.get_context_value('dag_run').id
    self.job_name = task_config.get_context_value('task').task_id
    self.job_end_timestamp = None,
    self.job_status = None,
    self.job_write_mode = task_config.job_write_mode
    self.job_schedule_interval = task_config.schedule_interval
    self.job_parameter_file = task_config._task_config_file.read_text()
    self.subject_area = task_config.get_context_value('dag').safe_dag_id
    self.target_table = task_config.target_table_name
    self.target_hx_loading_indicator = task_config.target_hx_loading_indicator
    self.stage_loading_query = task_config.stage_loading_query
    self.source_table = task_config._source_tables

    self.job_build_id = self.job_name
    self.target_audit_indicator = task_config.target_audit_indicator

    self.subject_area_build_id = None
    if task_config._build:
      self.subject_area_build_id = task_config._build['commit_id']

    self.job_step = []

  def add_step(self, job_step_name: str) -> None:
    """Add new step into a step list."""
    self.job_step.append({
        'job_step_id': len(self.job_step),
        'job_step_name': job_step_name,
        'job_step_start_time': str(pendulum.now()),
        'job_step_end_time': None,
        'job_step_status': None,
        'total_bytes_billed': None,
        'total_bytes_processed': None,
    })

  def finish_step(self,
                  job_step_status: str,
                  total_bytes_billed: int,
                  total_bytes_processed: int,
                  job_step_id: Optional[int] = None) -> None:
    """Complete step and put step data into etl_log table."""
    if not job_step_id:
      job_step_id = len(self.job_step) - 1
    self.job_step[job_step_id].update({
        'job_step_end_time': str(pendulum.now()),
        'job_step_status': job_step_status,
        'total_bytes_billed': total_bytes_billed,
        'total_bytes_processed': total_bytes_processed,
    })

  def log_flush(self, execution_context: Union[TGrizzlyOperator,
                                               TGrizzlyDLPOperator],
                status: str) -> None:
    """Write ExecutionLog into [etl_log.composer_job_details] table.

    All not completed job steps are marked as FAILED.

    Args:
      execution_context (TGrizzlyOperator, TGrizzlyDLPOperator):
      Instance of Custom Operator executed.
      status (string): Task execution status.
    """
    execution_context.log.info(
        f'ETL: DAG run_id=[{self.job_id}]. Writing ETL log')
    self.job_end_timestamp = str(pendulum.now())
    self.job_status = status
    for i, step in enumerate(self.job_step):
      if step['job_step_status'] is None:
        self.job_step[i].update({
            'job_step_end_time': str(pendulum.now()),
            'job_step_status': 'FAILED'
        })
    etl_log_table_ref = execution_context.bq_client.get_table(
        GrizzlyConfig.ETL_LOG_TABLE)
    execution_context.bq_client.insert_rows(etl_log_table_ref, [vars(self)])
