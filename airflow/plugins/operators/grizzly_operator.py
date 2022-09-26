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

"""Grizzly operator module.

  Grizzly operator contains set of basic functions to work: create_view,
  load_data, etc. The operator is inherited from
  airflow.models.baseoperator.BaseOperator

  Typical usage example:
  products = GrizzlyOperator(
      dag=main_dag
      ,task_id='products'
      ,config_file_ref='data/ETL/products.yml'
      ,execution_timeout=timedelta(seconds=1200)
  )
"""

from typing import Any, Optional
from airflow.exceptions import AirflowException
from airflow.exceptions import AirflowSkipException
from airflow.models.baseoperator import BaseOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from grizzly.bq_table_security import BQTableSecurity
from grizzly.data_catalog_tag import DataCatalogTag
import grizzly.etl_action
from grizzly.etl_factory import ETLFactory
from grizzly.execution_log import etl_step
from grizzly.execution_log import ExecutionLog
from grizzly.grizzly_typing import TCopyJob
from grizzly.grizzly_typing import TExecutionLog
from grizzly.grizzly_typing import TGrizzlyTaskConfig
from grizzly.grizzly_typing import TQueryJob
import grizzly.task_instance


class GrizzlyOperator(BaseOperator):
  """Implementation of Grizzly Airflow operator.

  Upload data into target BigQuery table.

  Attributes:
    bigquery_conn_id (string): Airflow connection Id.
      Default value [bigquery_default].

    bq_cursor (google.cloud.bigquery.dbapi.Cursor): DB-API Cursor to Google
      BigQuery.
    config_file_ref (string): Reference to task YML file with configuration.
    task_config (grizzly.task_instance.TaskInstance): Task configuration
      contains parsed and pre-proccessed information from task YML file.
    bq_hook (airflow.providers.google.cloud.hooks.bigquery.BigQueryHook):
      Interact with BigQuery. This hook uses the Google Cloud Platform
      connection.
    bq_client (google.cloud.bigquery.Client): BigQuery client object.
    tags (grizzly.data_catalog_tag.DataCatalogTag): This object is used for
      setup DataCatalog Tags and Column Level security (DataCatalog Taxonomy).
    table_access (grizzly.bq_table_security.BQTableSecurity): Object is used
      for work with Table and Row Level security.
    etl_log (grizzly.execution_log.ExecutionLog): Instance of ExecutionLog
      with detailed information about task execution. This object is used for
      dumping of execution logs into [etl_log.composer_job_details] table.
  """

  WRITE_APPEND = 'WRITE_APPEND'
  WRITE_TRUNCATE = 'WRITE_TRUNCATE'
  WRITE_EMPTY = 'WRITE_EMPTY'
  CREATE_VIEW = 'CREATE_VIEW'
  UPDATE = 'UPDATE'
  DELETE = 'DELETE'
  RUN_QUERY = 'RUN_QUERY'
  EXPORT_DATA = 'EXPORT_DATA'
  MERGE = 'MERGE'

  INSERT_MODE_LIST = [WRITE_APPEND, WRITE_TRUNCATE, WRITE_EMPTY, MERGE]

  def __init__(self,
               config_file_ref: Optional[str] = None,
               bigquery_conn_id: str = 'bigquery_default',
               delegate_to: Optional[Any] = None,
               location: Optional[Any] = None,
               *args: Any,
               **kwargs: Any) -> None:
    """Initialize Airflow Task Instance for GrizzlyOperator."""
    super(GrizzlyOperator, self).__init__(*args, **kwargs)
    self.bigquery_conn_id = bigquery_conn_id
    self.bq_cursor = None
    self.config_file_ref = config_file_ref

  @etl_step
  def create_view(self,
                  task_config: TGrizzlyTaskConfig,
                  etl_log: TExecutionLog,
                  job_step_name: str = 'create_view') -> TQueryJob:
    """Create VIEW from SELECT statement.

    Args:
      task_config (grizzly.task_instance.TaskInstance): Task configuration.
      etl_log (grizzly.execution_log.ExecutionLog): ExecutionLog object
        used by etl_step decorator.
      job_step_name (str, optional): Value to be stored in
        [job_step.job_step_name] attribute of etl log table.
        Used by etl_log decorator.
        Defaults to 'create_view'.

    Returns:
      (google.cloud.bigquery.QueryJob): BigQuery job execution statistic.
    """
    self.log.info('ETL: Create VIEW [%s]', task_config.target_table_name)
    job_stat = grizzly.etl_action.create_view(
        execution_context=self, task_config=task_config)
    return job_stat

  @etl_step
  def copy_staging_to_target(
      self,
      task_config: TGrizzlyTaskConfig,
      etl_log: TExecutionLog,
      job_step_name: str = 'load_staging_to_target'
  ) -> TCopyJob:
    """Copy staging data into target table.

    Args:
      task_config (grizzly.task_instance.TaskInstance): Task configuration.
      etl_log (grizzly.execution_log.ExecutionLog): ExecutionLog object
        used by etl_step decorator.
      job_step_name (str, optional): Value to be stored in
        [job_step.job_step_name] attribute of etl log table.
        Used by etl_log decorator.
        Defaults to 'load_staging_to_target'.

    Returns:
      (google.cloud.bigquery.CopyJob): BigQuery job execution statistic.
      For details check:
        https://cloud.google.com/bigquery/docs/reference/v2/jobs
    """
    self.log.info(
        'ETL: Copy staging table: [{}]=>[{}]'.format(
            task_config.staging_table_name,
            task_config.target_table_name
        )
    )
    job_stat = grizzly.etl_action.copy_table(
        execution_context=self,
        source_table_name=task_config.staging_table_name,
        target_table_name=task_config.target_table_name,
        write_disposition=task_config.job_write_mode)
    return job_stat

  @etl_step
  def run_query(self,
                query: str,
                etl_log: TExecutionLog,
                is_legacy_sql: str = False,
                job_step_name: Optional[str] = None,
                msg: Optional[str] = None) -> TQueryJob:
    """Run BigQuery query.

    Args:
      query (string): BQ query to be executed.
      etl_log (grizzly.execution_log.ExecutionLog): ExecutionLog object
        used by etl_step decorator.
      is_legacy_sql (bool, optional): Define BQ dialect to be used for query
        execution. Defaults to False.
      job_step_name (str, optional): Value to be stored in
        [job_step.job_step_name] attribute of etl log table.
        Used by etl_log decorator.
        Defaults to None.
      msg (string, optional): Debug message to be printed to Airflow task
        execution log.
        Defaults to None.

    Returns:
        (google.cloud.bigquery.QueryJob): BigQuery job execution statistic.
    """
    self.log.info(msg)
    job_stat = grizzly.etl_action.run_bq_query(
        execution_context=self, sql=query, use_legacy_sql=is_legacy_sql)
    return job_stat

  @etl_step
  def merge_data(self,
                 task_config: TGrizzlyTaskConfig,
                 etl_log: TExecutionLog,
                 job_step_name: Optional[str] = 'merge_staging_to_target',
                 msg: Optional[str] = None) -> TQueryJob:
    """Run BigQuery for Merge query.

    Args:
      task_config (grizzly.task_instance.TaskInstance): Task configuration.
      etl_log (grizzly.execution_log.ExecutionLog): ExecutionLog object
        used by etl_step decorator.
      job_step_name (str, optional): Value to be stored in
        [job_step.job_step_name] attribute of etl log table.
        Used by etl_log decorator.
        Defaults to None.
      msg (string, optional): Debug message to be printed to Airflow task
        execution log.
        Defaults to None.

    Returns:
        (google.cloud.bigquery.QueryJob): BigQuery job execution statistic.
    """

    self.log.info(msg)
    job_stat = ETLFactory.merge_data(
        execution_context=self,
        task_config=task_config,
        write_disposition=task_config.job_write_mode)

    return job_stat

  @etl_step
  def load_data(self,
                task_config: TGrizzlyTaskConfig,
                etl_log: TExecutionLog,
                job_step_name: Optional[str] = None,
                msg: Optional[str] = None) -> TQueryJob:
    """Load data into staging or into target table.

    Method executes ETLFactory.upload_data for perform ETL action on a base of
    task configuration defined in YML file.

    Args:
      task_config (grizzly.task_instance.TaskInstance): Task configuration.
      etl_log (grizzly.execution_log.ExecutionLog): ExecutionLog object
        used by etl_step decorator.
      job_step_name (str, optional): Value to be stored in
        [job_step.job_step_name] attribute of etl log table.
        Used by etl_log decorator.
        Defaults to None.
      msg (string, optional): Debug message to be printed to Airflow task
        execution log.
        Defaults to None.

    Raises:
      AirflowException: Raise error if job_step_name not in ('load_target_table'
        'load_staging_table')

    Returns:
      (google.cloud.bigquery.QueryJob): BigQuery job execution statistic.
        For details check:
        https://cloud.google.com/bigquery/docs/reference/v2/jobs
    """
    self.log.info(msg)
    if job_step_name == 'load_target_table':
      target_table_name = task_config.target_table_name
      write_disposition = task_config.job_write_mode
    elif job_step_name == 'load_staging_table':
      target_table_name = task_config.staging_table_name
      write_disposition = 'WRITE_TRUNCATE'
    else:
      raise AirflowException(f'[{job_step_name}] is not supported')
    job_stat = ETLFactory.upload_data(
        execution_context=self,
        task_config=task_config,
        target_table=target_table_name,
        write_disposition=write_disposition)
    return job_stat

  @etl_step
  def export_data(
      self,
      task_config: TGrizzlyTaskConfig,
      etl_log: TExecutionLog,
      job_step_name: str = 'export_data',
      msg: Optional[str] = None) -> TQueryJob:
    """Export data into staging or into the external or internal target.

    Args:
      task_config (grizzly.task_instance.TaskInstance): Task configuration.
      etl_log (grizzly.execution_log.ExecutionLog): ExecutionLog object
        used by etl_step decorator.
      job_step_name (str, optional): Value to be stored in
        [job_step.job_step_name] attribute of etl log table.
        Used by etl_log decorator.
        Defaults to None.
      msg (string, optional): Debug message to be printed to Airflow task
        execution log.
        Defaults to None.

    Returns:
      (google.cloud.bigquery.ExtractJob): BigQuery job execution statistic.
        For details check:
        https://cloud.google.com/bigquery/docs/reference/v2/jobs
    """
    self.log.info(msg)

    write_disposition = task_config.job_write_mode

    job_stat = ETLFactory.export_data(
        execution_context=self,
        task_config=task_config,
        write_disposition=write_disposition)
    return job_stat

  def check_data_quality(self,
                         task_config: TGrizzlyTaskConfig,
                         etl_log: TExecutionLog) -> None:
    """Execute quality check queries.

    List of Data Quality check queries defined in [job_data_quality_query]
    parameter inside task definition YML file.

    Args:
      task_config (grizzly.task_instance.TaskInstance): Task configuration.
      etl_log (grizzly.execution_log.ExecutionLog): ExecutionLog object
        used by etl_step decorator inside run_bq_query_list method.
    """
    grizzly.etl_action.run_bq_query_list(
        execution_context=self,
        query_list_parameter_name='job_data_quality_query',
        etl_log=etl_log,
        job_step_name='data_quality_check',
        message='ETL: Run Data Quality Check')

  def run_pre_etl(self,
                  task_config: TGrizzlyTaskConfig,
                  etl_log: TExecutionLog) -> None:
    """Execute Pre ETL scripts.

    List of Pre ETL queries defined in [pre_etl_scripts]
    parameter inside task definition YML file.

    Args:
      task_config (grizzly.task_instance.TaskInstance): Task configuration.
      etl_log (grizzly.execution_log.ExecutionLog): ExecutionLog object
        used by etl_step decorator inside run_bq_query_list method.
    """
    grizzly.etl_action.run_bq_query_list(
        execution_context=self,
        query_list_parameter_name='pre_etl_scripts',
        etl_log=etl_log,
        job_step_name='pre_etl_script',
        message='ETL: Run pre ETL scripts')

  def run_post_etl(self,
                   task_config: TGrizzlyTaskConfig,
                   etl_log: TExecutionLog) -> None:
    """Execute Post ETL scripts.

    List of Post ETL queries defined in [post_etl_scripts]
    parameter inside task definition YML file.

    Args:
      task_config (grizzly.task_instance.TaskInstance): Task configuration.
      etl_log (grizzly.execution_log.ExecutionLog): ExecutionLog object
        used by etl_step decorator inside run_bq_query_list method.
    """
    grizzly.etl_action.run_bq_query_list(
        execution_context=self,
        query_list_parameter_name='post_etl_scripts',
        etl_log=etl_log,
        job_step_name='post_etl_script',
        message='ETL: Run post ETL scripts')

  def run_access_scripts(self, target_table: str) -> None:
    """Run access scripts for ROW LEVEL and TABLE LEVEL security.

    Method performs actions only if [access_scripts] was defined in yml files.

    Args:
      target_table (string): Target table name.
    """
    # if access scripts are not defined do nothing
    if self.task_config.access_scripts:
      self.log.info(f'ETL: Running access scripts on table [{target_table}]')
      # Pass RAW access_scripts from task yml file
      # We need to build script on fly
      self.table_access.run_bq_access_scripts(
          target_table=target_table,
          etl_log=self.etl_log,
          job_step_name=f'run_access_scripts [{target_table}]')
    return

  def execute(self, context: Any) -> None:
    """The code to execute when the runner calls the operator.

    Args:
      context: Airflow context that can be used for access Airflow config
        values.
    """
    dag_run_id = context['dag_run'].id
    self.task_config = grizzly.task_instance.TaskInstance(
        self.config_file_ref, context)
    # open BQ connection
    self.bq_hook = BigQueryHook(
        bigquery_conn_id=self.bigquery_conn_id,
        use_legacy_sql=False,
    )
    self.bq_cursor = self.bq_hook.get_conn().cursor()
    self.bq_client = self.bq_hook.get_client()
    # prepare etl_log
    self.etl_log = ExecutionLog(self.task_config)
    # set up DataCatalog column policy tags
    self.tags = DataCatalogTag(
        execution_context=self,
        column_policy_tags=self.task_config.column_policy_tags,
        datacatalog_tags=self.task_config.data_catalog_tags)
    # setup BQ table security object
    self.table_access = BQTableSecurity(
        execution_context=self,
        raw_access_scripts=self.task_config._raw_config.get(
            'access_scripts', []))
    try:
      # ETL. Check custom schedule and skip task in case of importance
      grizzly.etl_action.check_custom_schedule(
          execution_context=self, task_config=self.task_config)
      # ETL. Run pre ETL scripts
      self.run_pre_etl(self.task_config, self.etl_log)
      # ETL. CREATE_VIEW
      if self.task_config.job_write_mode == self.CREATE_VIEW:
        self.create_view(task_config=self.task_config, etl_log=self.etl_log)
      elif self.task_config.job_write_mode == self.EXPORT_DATA:
        self.export_data(task_config=self.task_config, etl_log=self.etl_log)
      elif self.task_config.job_write_mode == self.RUN_QUERY:
        # ETL. RUN_QUERY
        self.run_query(
            query=self.task_config.stage_loading_query,
            is_legacy_sql=self.task_config.is_legacy_sql,
            etl_log=self.etl_log,
            job_step_name='run_bq_query',
            msg=f'ETL: Run BQ query: {self.task_config.stage_loading_query}')
      elif self.task_config.job_write_mode in [
          *self.INSERT_MODE_LIST, self.UPDATE, self.DELETE
      ]:
        # INSERT, UPDATE, DELETE data
        # ETL. Load history table data
        is_target_table_exists = self.bq_hook.table_exists(
            **grizzly.etl_action.parse_table(
                self.task_config.target_table_name))
        if (self.task_config.target_hx_loading_indicator == 'Y' and
            is_target_table_exists):
          grizzly.etl_action.load_history_table(
              execution_context=self,
              target_table=self.task_config.target_table_name,
              etl_log=self.etl_log)
          self.run_access_scripts(self.task_config.history_table_name)
          self.tags.set_column_policy_tags(
              self.task_config.history_table_name
          )  # setup column policy tags on history table
        if self.task_config.job_write_mode in self.INSERT_MODE_LIST:
          # if job_data_quality_query was not defined then load dirrectly into
          # target table.
          # Upload upstream data into target table
          if not self.task_config.job_data_quality_query:
            self.load_data(
                task_config=self.task_config,
                etl_log=self.etl_log,
                job_step_name='load_target_table',
                msg='ETL: Loading data into target table [{}]'.format(
                    self.task_config.target_table_name
                )
            )
            self.run_access_scripts(self.task_config.target_table_name)
            # setup column policy tags on target table
            self.tags.set_column_policy_tags(self.task_config.target_table_name)
            # setup table  tags on target table
            self.tags.set_table_tags(self.task_config.target_table_name)
          else:
            # load into staging table, perform quality check, copy to target
            # ETL. Upload upstream data into staging table
            self.load_data(
                task_config=self.task_config,
                etl_log=self.etl_log,
                job_step_name='load_staging_table',
                msg='ETL: Loading data into staging table [{}]'.format(
                    self.task_config.staging_table_name
                )
            )
            self.run_access_scripts(self.task_config.staging_table_name)
            self.tags.set_column_policy_tags(
                self.task_config.staging_table_name
            )  # setup column policy tags on staging table
            # ETL. Validate quality of upstream data
            self.check_data_quality(
                task_config=self.task_config, etl_log=self.etl_log)
            # ETL. Insert into target table
            self.copy_staging_to_target(
                task_config=self.task_config, etl_log=self.etl_log)
            self.run_access_scripts(self.task_config.target_table_name)
            # setup column policy tags on target table
            self.tags.set_column_policy_tags(self.task_config.target_table_name)
            # setup table  tags on target table
            self.tags.set_table_tags(self.task_config.target_table_name)
        elif self.task_config.job_write_mode in [self.UPDATE, self.DELETE]:
          # ETL. UPDATE / DELETE
          self.run_query(
              query=self.task_config.stage_loading_query,
              is_legacy_sql=self.task_config.is_legacy_sql,
              etl_log=self.etl_log,
              job_step_name=f'{self.task_config.job_write_mode}_of_target',
              msg=f'ETL: {self.task_config.job_write_mode} the table.')
          self.run_access_scripts(self.task_config.target_table_name)
          # setup column policy tags on target table
          self.tags.set_column_policy_tags(self.task_config.target_table_name)
          # setup table  tags on target table
          self.tags.set_table_tags(self.task_config.target_table_name)
        pass
      # ETL. Run post ETL queries
      self.run_post_etl(self.task_config, self.etl_log)
    except AirflowSkipException as ae:
      self.etl_log.log_flush(execution_context=self, status='SKIPPED')
      raise ae
    except Exception as ae:
      self.etl_log.log_flush(execution_context=self, status='FAILED')
      raise ae
    else:
      self.etl_log.log_flush(execution_context=self, status='SUCCESS')
