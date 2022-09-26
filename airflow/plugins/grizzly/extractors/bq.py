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

"""Implementation of BigQuery extract class.

ExtractorBQ is inherited from grizzly.extractors.base_extractor.BaseExtractor
Only load method implemented. exctact and load methods are inherited from
BaseExtractor and they just bypass default empty objects.
Instance of this class created and used by grizzly.etl_factory.ETLFactory
For more insights check implementation of base_extractor and etl_factory.
"""

from typing import Any

from airflow.exceptions import AirflowException
import grizzly.etl_action
from grizzly.etl_audit import ETLAudit
from grizzly.extractors.base_extractor import BaseExtractor


class ExtractorBQ(BaseExtractor):
  """Implementation of ETL for BQ to BQ data loading.

  Attributes:
    task_config (TGrizzlyTaskConfig): Task configuration with parsed and
      pre-proccessed information from task YML file.
    target_table (string): Name of a table where query execution results should
      be stored.
    execution_context (TGrizzlyOperator): Instance of GrizzlyOperator executed.
    write_disposition (string): BQ write disposition WRITE_APPEND, WRITE_EMPTY,
      WRITE_TRUNCATE. In case if etl_factory use EtractorBQ for staging table it
      will be WRITE_TRUNCATE.
      If it executed for table defined in [target_table_name] attribute of task
      YML file this class attribute will be  equal to [job_write_mode] attribute
      of task YML file.
    job_stat (QueryJob): Job execution statistics.
  """

  def load(self, data: Any) -> None:
    """Load result of BQ script execution into BQ table.

    BQ script defined in [stage_loading_query] attribute of task YML file.
    In case if [stage_loading_query] contains multistep SQL result of last
    statement will be copied from BigQuery internal temporary table into table
    defined in [self.target_table]

    Args:
      data: Recieve default empty object from transform method.
      This parameter her only for method interface compatibility purpose and
      does not affect any calculations.

    Raises:
        AirflowException: Incorrect query type.
    """

    is_audit_supported = False
    if self.task_config.job_write_mode in ETLAudit.supported_write_mode:
      is_audit_supported = True

    # perform DRY_RUN for understanding is it BQ SCRIPT or just SELECT
    dry_run_job = grizzly.etl_action.dry_run(
        execution_context=self.execution_context,
        sql=self.task_config.stage_loading_query,
        use_legacy_sql=self.task_config.is_legacy_sql)
    if dry_run_job.statement_type not in ['SELECT', 'SCRIPT']:
      raise AirflowException(
          'Incorrect query. Support only SELECT and BQ SCRIPTs.')

    if dry_run_job.statement_type == 'SELECT':
      destination_table = self.target_table
    else:
      destination_table = None

    sql_exec = None

    if  self.task_config.target_audit_indicator == 'Y' and is_audit_supported:
      self._execute_audit_columns()
      sql_exec = ETLAudit.get_sql_statement(
          execution_context=self.execution_context,
          task_config=self.task_config,
          sql=sql_exec)

    if not sql_exec:
      sql_exec = self.task_config.stage_loading_query

    self.job_stat = grizzly.etl_action.run_bq_query(
        execution_context=self.execution_context,
        sql=sql_exec,
        destination_table=destination_table,
        write_disposition=self.write_disposition,
        use_legacy_sql=self.task_config.is_legacy_sql)

    # for scripts perform copy of cached from last resultset
    if dry_run_job.statement_type == 'SCRIPT':
      # get temporary (cached) table from last SELECT statement in query
      temporary_table = '{}.{}'.format(
          self.job_stat.destination.dataset_id,
          self.job_stat.destination.table_id
      )
      grizzly.etl_action.copy_table(
          execution_context=self.execution_context,
          source_table_name=temporary_table,
          target_table_name=self.target_table,
          write_disposition=self.write_disposition)

  def _execute_audit_columns(self):
    """Execute DDL for adding audit columns."""

    sql = ETLAudit.get_column_statement(
        execution_context=self.execution_context,
        task_config=self.task_config)

    self.job_stat = grizzly.etl_action.run_bq_query(
        execution_context=self.execution_context,
        sql=sql,
        use_legacy_sql=self.task_config.is_legacy_sql)
