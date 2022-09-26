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

"""Common library of a ETL functions.

Typical usage example:
  prepare_value_for_sql(values=values)
"""

import datetime
import json
import pathlib
from typing import Any, Dict, List, Optional, Union

from airflow.exceptions import AirflowSkipException
from airflow.providers.google.cloud.hooks.bigquery import _split_tablename
import croniter
from google.cloud import bigquery
from grizzly.config import Config
from grizzly.execution_log import etl_step
from grizzly.grizzly_typing import TCopyJob
from grizzly.grizzly_typing import TExecutionLog
from grizzly.grizzly_typing import TGrizzlyDLPOperator
from grizzly.grizzly_typing import TGrizzlyOperator
from grizzly.grizzly_typing import TGrizzlyTaskConfig
from grizzly.grizzly_typing import TQueryJob
import jinja2
import pendulum


def prepare_value_for_sql(value: Any) -> str:
  """Preparing value for INSERT INTO SELECT statement.

  This function is required for correct insertion of TIMESTAMP, ARRAY and STRUCT
  from python datetime, list and dictionary.

  Args:
    value (Any): value to be inserted in SELECT query

  Returns:
    (str): processsed data
  """
  if value is None:
    value = 'NULL'
  elif isinstance(value, str):
    value = json.dumps(value)
  elif type(value) in [type(datetime.datetime.now()), type(pendulum.now())]:
    value = "TIMESTAMP('{0}')".format(str(value))
  elif isinstance(value, list):
    value = '[' + ', '.join([prepare_value_for_sql(i) for i in value]) + ']'
  elif isinstance(value, dict):
    value = 'STRUCT(' + ', '.join([
        '{0} AS {1}'.format(prepare_value_for_sql(v), k)
        for k, v in value.items()
    ]) + ')'
  else:
    value = str(value)
  return value


def dry_run(execution_context: TGrizzlyOperator,
            sql: str,
            use_legacy_sql: bool = False) -> TQueryJob:
  """Perform dry-run of the query and return some basic statistic.

  This method could be used for understanding of query structure before it
  executed. For example you can understand is it multistep SQL or single step
  SQL or estimate query cost.

  Args:
    execution_context (TGrizzlyOperator): Instance of GrizzlyOperator executed.
    sql (string): BigQuery sql for dry-run.
    use_legacy_sql (bool, optional): Define BQ dialect to be used.
      Defaults to False.

  Returns:
    (TQueryJob): BQ QueryJob with job statistic details for Dry Run.
  """
  job_config = bigquery.QueryJobConfig(
      dry_run=True, use_query_cache=False, use_legacy_sql=use_legacy_sql)
  # Start the query, passing in the extra configuration.
  query_job = execution_context.bq_client.query(
      query=sql,
      job_config=job_config,
  )  # Make an API request.
  return query_job


def run_bq_query(execution_context: TGrizzlyOperator,
                 sql: str,
                 destination_table: Optional[str] = None,
                 write_disposition: str = 'WRITE_EMPTY',
                 use_legacy_sql: str = False) -> TQueryJob:
  """Execute BigQuery query and return job execution statistic.

  Args:
    execution_context (TGrizzlyOperator): Instance of GrizzlyOperator executed.
    sql (string): BigQuery script to be executed.
    destination_table (string, optional): Destination table for query results.
      If not defined method will just execute query without writing of query
      output. Defaults to None.
    write_disposition (str): BigQuery write disposition for
      destination_table. Next values are supported: [WRITE_APPEND, WRITE_EMPTY,
      WRITE_TRUNCATE]. Defaults to 'WRITE_EMPTY'.
    use_legacy_sql (bool): [description]. Defaults to False.

  Returns:
    (TQueryJob): BQ QueryJob with job statistic details.
  """
  job_id = execution_context.bq_cursor.run_query(
      sql=sql,
      use_legacy_sql=use_legacy_sql,
      allow_large_results=True,
      destination_dataset_table=destination_table,
      write_disposition=write_disposition)
  job_stat = execution_context.bq_client.get_job(job_id)
  return job_stat


def create_view(execution_context: TGrizzlyOperator,
                task_config: TGrizzlyTaskConfig) -> TQueryJob:
  """Generate VIEW from SELECT query.

  View generated on a base of JINJA2 template stored in file
  [templates/create_view.sql.jinja2]. Template uses SELECT query from
  task_config.stage_loading_query for view generation.
  View name is equal to task_config.target_table_name.

  Args:
    execution_context (TGrizzlyOperator):  Instance of GrizzlyOperator executed.
    task_config (TGrizzlyTaskConfig): Task configuration contains parsed and
     pre-proccessed information from task YML file.

  Returns:
    (TQueryJob): BQ QueryJob with job statistic details.
  """
  template_folder = pathlib.Path('/home/airflow/gcs/plugins/templates')
  view_template = template_folder / 'create_view.sql.jinja2'
  view_query = jinja2.Template(view_template.read_text()).render(
      view_name=task_config.target_table_name, task_config=task_config)
  return run_bq_query(
      execution_context,
      sql=view_query,
      use_legacy_sql=task_config.is_legacy_sql)


def check_custom_schedule(execution_context: Union[TGrizzlyOperator,
                                                   TGrizzlyDLPOperator],
                          task_config: Union[TGrizzlyTaskConfig]) -> None:
  """Check custom schedule.

  Custom task schedule should be less frequent than DAG schedule.
  Task skipped in case if it's no time for execution. For example if DAG was
  scheduled for execution every 1 Hr
  but task scheduled for execution every 2 Hr  then each second task execution
  will be skipped.

  Args:
    execution_context (TGrizzlyOperator, TGrizzlyDLPOperator): Instance of
      GrizzlyOperator executed.
    task_config (TGrizzlyTaskConfig): Task configuration
      contains parsed and pre-proccessed information from task YML file.

  Raises:
    (AirflowSkipException): Raise Airflow skip exception if it's no planned task
      execution accordingly to CRON schedule.
  """
  # proceed only for task with custom schedule defined
  if not task_config.is_custom_schedule:
    return
  else:
    schedule_interval = task_config.schedule_interval
    cron = croniter.croniter(schedule_interval, pendulum.now())
    previous_planned_etl_run = pendulum.instance(
        cron.get_prev(datetime.datetime))
    current_dag_execution_date = task_config.get_context_value(
        parameter_name='execution_date')
    previous_dag_execution_date = task_config.get_context_value(
        parameter_name='prev_execution_date')
    execution_context.log.info(
        'Previous planned ETL run for task({}) = {}'.format(
            schedule_interval,
            previous_planned_etl_run
        )
    )
    execution_context.log.info(
        f'Current execution_date = {current_dag_execution_date}')
    execution_context.log.info(
        f'Previous execution_date = {previous_dag_execution_date}')
    # no previous execution or ETL run planned for
    # (previous_DAG_run .. current_DAG_run]
    if not (previous_dag_execution_date is None or
            (previous_planned_etl_run > previous_dag_execution_date and
             previous_planned_etl_run <= current_dag_execution_date)):
      raise AirflowSkipException


def copy_table(execution_context: TGrizzlyOperator,
               source_table_name: str,
               target_table_name: str,
               write_disposition: str) -> TCopyJob:
  """Copy data between tables.

  Args:
    execution_context (TGrizzlyOperator): Instance of GrizzlyOperator executed.
    source_table_name (str): Source table name.
    target_table_name (str): Target table name.
    write_disposition (str): BigQuery write disposition for
      target_table_name. Next values are supported: [WRITE_APPEND, WRITE_EMPTY,
      WRITE_TRUNCATE]

  Returns:
    (TCopyJob): BQ CopyJob with job statistic details.
  """
  job_id = execution_context.bq_cursor.run_copy(
      source_project_dataset_tables=source_table_name,
      destination_project_dataset_table=target_table_name,
      write_disposition=write_disposition,
      create_disposition='CREATE_IF_NEEDED')
  job_stat = execution_context.bq_client.get_job(job_id)
  return job_stat


def run_bq_query_list(execution_context: TGrizzlyOperator,
                      query_list_parameter_name: Optional[str] = None,
                      query_list: Union[List[str], str, None] = None,
                      query_names: Union[List[str], str, None] = None,
                      etl_log: Optional[TExecutionLog] = None,
                      job_step_name: str = 'run_query',
                      message: str = 'ETL: Run query') -> None:
  """Execute queries from a list.

  Query list could be defined in parameter of task YML file
  [query_list_parameter_name] or provided as a list in [query_list].

  Args:
    execution_context(TGrizzlyOperator): Instance of GrizzlyOperator executed.
    query_list_parameter_name (string, optional): Name of task YML file
      parameter with a list of references to query files to be executed.
    query_list (list[string], str, optional): List of queries to be executed.
    query_names (list[string], str, optional): List of query names. Names from
      this list will be stored in etl_log table for each query executed.
      Will be transformed to list of empty strings if not provided.
      If provided as string will be transformed to a list with one string item.
    etl_log (TExecutionLog): ExecutionLog object
      used by etl_step decorator. ExecutionLog instance used for logging of
      each query run.
    job_step_name (str): Value to be stored in job_step.job_step_name
      attribute of etl log table. Used by etl_log decorator.
    message (string, optional): Debug message to be printed to Airflow task
      execution log. Defaults to None.
  """
  if query_list_parameter_name:
    query_list = getattr(execution_context.task_config,
                         query_list_parameter_name, [])
    query_names = execution_context.task_config._raw_config.get(
        query_list_parameter_name, [])

  if not query_list:
    # if query list is empty or None exit from function
    return

  if isinstance(query_names, str):
    # transform to list with 1 item
    query_names = [query_names]
  elif query_names is None:
    # transform to list of emptyy strings
    query_names = [''] * len(query_list)

  # define wrapper for query execution.
  # we need it for decoration in case of ETL logged execution
  # pylint: disable=unused-argument
  def run_query_wrapper(execution_context, sql, etl_log, job_step_name):
    return run_bq_query(execution_context=execution_context, sql=sql)

  # if etl_log was defined run decoratted function
  if etl_log is not None:
    run_func = etl_step(run_query_wrapper)
  else:
    run_func = run_query_wrapper

  # iterate and perform action
  for qname, query in zip(query_names, query_list):
    execution_context.log.info(f'{message} [{qname}]')
    jsn = f'{job_step_name} [{qname}]'
    run_func(execution_context, sql=query, etl_log=etl_log,
             job_step_name=jsn)
  return


@etl_step
def load_history_table(execution_context: TGrizzlyOperator,
                       target_table: Union[str, Dict[str, str]],
                       etl_log: TExecutionLog,
                       job_step_name: str = 'load_history_table') -> TQueryJob:
  """Load data into history table.

  If the history table does not exist, it will be created automatically.
  History table name is calculated on a base of target_table name.
  History data loaded as INSERT INTO SELECT statement from target_table table.

  Args:
    execution_context (TGrizzlyOperator): Instance of GrizzlyOperator executed.
    target_table (string): Target table name. This name is used for calculation
      of history table name.
    etl_log (TExecutionLog): ExecutionLog object
      used by etl_step decorator.
    job_step_name (str): Value to be stored in job_step.job_step_name
      attribute of etl log table. Used by etl_log decorator.

  Returns:
    (TQueryJob): BQ Job with statistic of query execution.
  """
  if isinstance(target_table, str):
    target_table = parse_table(target_table)
  # get a name of history table
  history_table = parse_table(execution_context.task_config.history_table_name)
  history_dataset_id = history_table['dataset_id']
  history_table_id = history_table['table_id']
  history_expiration = int(
      Config.HISTORY_TABLE_CONFIG['default_history_expiration'])
  # if history table does not exist generate it
  if not (execution_context.bq_hook.table_exists(
      project_id=target_table['project_id'],
      dataset_id=history_dataset_id,
      table_id=history_table_id)):
    execution_context.log.info(
        'ETL: Generate history table: [{}.{}]'.format(
            history_dataset_id,
            history_table_id
        )
    )
    hx_table_schema_definition = execution_context.bq_cursor.get_schema(
        dataset_id=target_table['dataset_id'],
        table_id=target_table['table_id'])['fields']
    hx_table_schema_definition.insert(0, {
        'name': 'create_time',
        'type': 'TIMESTAMP',
        'mode': 'NULLABLE'
    })
    hx_table_schema_definition.insert(1, {
        'name': 'job_process_id',
        'type': 'INTEGER',
        'mode': 'NULLABLE'
    })
    # get default expiration in days
    partitioning_schema = {
        'type': 'DAY',
        'expirationMs': f'{history_expiration*86400000}'
    }
    execution_context.bq_cursor.create_empty_table(
        project_id=target_table['project_id'],
        dataset_id=history_dataset_id,
        table_id=history_table_id,
        schema_fields=hx_table_schema_definition,
        time_partitioning=partitioning_schema)

  execution_context.log.info(
      'ETL: Load data to history table: [{}.{}]'.format(
          history_dataset_id,
          history_table_id
      )
  )
  copy_history_query = (
      'SELECT CURRENT_TIMESTAMP() as create_time, {0:d} as '
      'job_process_id, * FROM {1}').format(
          etl_log.job_id,
          f"{target_table['dataset_id']}.{target_table['table_id']}")
  return run_bq_query(
      execution_context=execution_context,
      destination_table=f'{history_dataset_id}.{history_table_id}',
      sql=copy_history_query,
      write_disposition='WRITE_APPEND',
  )


def parse_table(table_name: str) -> Dict[str, str]:
  """Parse input table name and return a dictionary.

  Args:
    table_name: table name to be parsed.

  Returns:
    (Dict[str, str]): Dictionary with file name structure.
    {
        "project_id": "",
        "dataset_id": "",
        "table_id": ""
    }
  """
  project_id, dataset_id, table_id = _split_tablename(
      default_project_id=Config.GCP_PROJECT_ID, table_input=table_name)
  return {
      'project_id': project_id,
      'dataset_id': dataset_id,
      'table_id': table_id
  }


def execute_bq_template(execution_context: TGrizzlyOperator,
                        task_config: TGrizzlyTaskConfig,
                        template_name: str,
                        data: Optional[Any] = None) -> TQueryJob:
  """Execute BigQuery Template query and return job execution statistic.

  Args:
    execution_context(TGrizzlyOperator): Instance of GrizzlyOperator executed.
    task_config (TGrizzlyTaskConfig): Task configuration
    template_name (str): name of template of bq command
    data (any, optional): data used in generating template

  Returns:
    (TQueryJob): BQ QueryJob with job statistic details
  """

  template_folder = pathlib.Path('/home/airflow/gcs/plugins/templates')
  exec_template = template_folder / template_name

  exec_query = jinja2.Template(exec_template.read_text()).render(
      task_config=task_config,
      execution_context=execution_context,
      data=data)

  return run_bq_query(
      execution_context=execution_context,
      sql=exec_query,
      use_legacy_sql=task_config.is_legacy_sql)
