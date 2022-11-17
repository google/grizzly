# Copyright 2022 Google LLC
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

"""ETL Factory module.

ETL Factory module dynamically executes functionality from other classes.
Provides two main implementation: upload_data & export_data

Typical usage example:

job_stat = ETLFactory.export_data(
      execution_context = self,
      task_config = task_config,
      write_disposition = write_disposition)
"""


import importlib
from typing import Any, Dict, Optional
from airflow.exceptions import AirflowException
from google.api_core.exceptions import NotFound
from grizzly.etl_action import execute_bq_template
from grizzly.grizzly_typing import TGrizzlyOperator
from grizzly.grizzly_typing import TGrizzlyTaskConfig
from grizzly.grizzly_typing import TQueryJob


sys_columns = {
    'sys_kyes': 'string',
    'sys_hash': 'string',
    'sys_act_ts': 'timestamp',
    'sys_act_id': 'string'
}


class ETLMerge:
  """Implementation of ETL methods for data merging.

  Attributes:
    execution_context (TGrizzlyOperator): Instance of GrizzlyOperator
      executed.
    bq_client (Any): BigQuery client.
    task_config (TGrizzlyTaskConfig): Task configuration
      contains parsed and pre-processed information from task YML file.
    write_disposition (string): Write disposition WRITE_APPEND, WRITE_EMPTY
      etc.
    stage_table (str): Placeholder. Name of the staging table
  """

  def __init__(self,
               execution_context: TGrizzlyOperator,
               task_config: TGrizzlyTaskConfig,
               write_disposition: str) -> None:

    self.execution_context = execution_context
    self.bq_client = self.execution_context.bq_client
    self.task_config = task_config
    self.write_disposition = write_disposition
    self.stage_table: str = None

  def _create_sys_columns(self,
                          target_table_name: str) -> None:
    """Create SYS columns for MERGE statement.

    Args:
      target_table_name (string): Target table name.
    """

    target_columns = self._get_columns(target_table_name)
    creating_columns_plan = {name: dt for name, dt in sys_columns.items()
                             if name not in target_columns}
    print(creating_columns_plan)

  def merge(self) -> None:
    """Perform MERGE operation."""

    target_table_name = self.task_config.target_table_name
    staging_table_name = self.task_config.staging_table_name

    if self._is_table_exists(target_table_name):
      target_schema = self._get_schema(target_table_name)
      source_schema = self._get_schema(staging_table_name)

      data = {'target': {'schema': target_schema},
              'source': {'schema': source_schema}}

      execute_bq_template(execution_context=self.execution_context,
                          task_config=self.task_config,
                          template_name='merge_sys_columns.sql.jinja2',
                          data=data)

    else:
      execute_bq_template(execution_context=self.execution_context,
                          task_config=self.task_config,
                          template_name='ctas_sys_columns.sql.jinja2')

  def _is_table_exists(self, table_name: str) -> bool:
    """Checking table if it exists.

    Args:
      table_name (string): Table name.

    Returns:
      Boolean - True if table exists, False if not
    """

    try:
      self.bq_client.get_table(table_name)
      return True
    except NotFound as ex:
      print(ex)
      return False
    except Exception as ex:
      raise ex

    return True

  def _get_columns(self, table_name: str) -> Dict[str, Any]:
    """Return columns of table.

    Args:
      table_name (string): Table name.

    Returns:
      (dict): Dictionary of the table schema.
    """

    table = self.bq_client.get_table(table_name)

    existing_fields = {
        f.name.lower(): [str(f.is_nullable).lower(), f.field_type.lower()]
        for f in table.schema
    }

    return existing_fields

  def _get_schema(self, table_name: str) -> Dict[str, Any]:
    """Return table schema.

    Args:
      table_name (string): Table name.

    Returns:
      Dictionary of the table schema
    """

    table = self.bq_client.get_table(table_name)
    return table.schema


class ETLFactory:
  """Implementation of ETL methods for data loading and export."""

  @classmethod
  def upload_data(cls,
                  execution_context: TGrizzlyOperator,
                  task_config: TGrizzlyTaskConfig,
                  target_table: Optional[str],
                  write_disposition: str) -> Optional[TQueryJob]:
    """Perform ETL operation. Load data from data source into target BQ table.

    Class method implements Class Factory pattern.
    Method supports different data sources. Or user can create own custom Python
    implementation of data extractor.
    Each extractor implementation should be inherited from BaseExtractor and
    could contain implementation of 'extract', 'transform', 'load' methods

    Args:
      execution_context (TGrizzlyOperator): Instance of GrizzlyOperator
        executed.
      task_config (TGrizzlyTaskConfig): Task configuration
        contains parsed and pre-processed information from task YML file.
      target_table (string, optional): Target table name.
      write_disposition (string): Write disposition WRITE_APPEND, WRITE_EMPTY
        etc.

    Raises:
        AirflowException: Error raised in case if was provided not supported
        data source.

    Returns:
        (TQueryJob, None): Job execution statistics.
    """

    # get upstream data source_type. If not defined use BQ as source
    source_type = task_config.source_type
    source_extractor = {
        'bq': 'grizzly.extractors.bq.ExtractorBQ',
        'bq_scripting': 'grizzly.extractors.bq.ExtractorBQ',
        'trix': 'grizzly.extractors.gsheet.ExtractorGSheet',
        'gsheet': 'grizzly.extractors.gsheet.ExtractorGSheet',
        'spanner': 'grizzly.extractors.spanner.ExtractorSpanner',
        'mysql': 'grizzly.extractors.mysql.ExtractorMySQL',
        'shapefile': 'grizzly.extractors.shapefile.ExtractorShapefile',
        'csv': 'grizzly.extractors.csv_url.ExtractorCSV',
        'excel': 'grizzly.extractors.excel_url.ExtractorExcel',
        'custom': task_config.source_extractor
    }

    if source_type not in source_extractor:
      raise AirflowException(
          ('Incorrect source_type was provided. ',
           f'[{source_type}] has no implementation.')
      )

    module_name, class_name = source_extractor[source_type].rsplit('.', 1)
    extractor_class = getattr(importlib.import_module(module_name), class_name)
    extractor = extractor_class(
        execution_context=execution_context,
        task_config=task_config,
        target_table=target_table,
        write_disposition=write_disposition)

    class_name = str(type(extractor))
    execution_context.log.info(f'ETL: Extracting data with [{class_name}]')

    # execute ETL process
    for e in extractor.extract():
      # stop further processing in case of empty resultset
      # e['metadata'] is None in case if extract step was skipped.
      # For example for BQ
      if e['metadata'] is not None and not e['rows']:
        break
      # transform upstream data
      e = extractor.transform(e)
      # perform loading of transformed data
      extractor.load(e)

    return getattr(extractor, 'job_stat', None)

  @classmethod
  def merge_data(cls,
                 execution_context: TGrizzlyOperator,
                 task_config: TGrizzlyTaskConfig,
                 write_disposition: str) -> None:
    """Merge data.

    Args:
      execution_context (TGrizzlyOperator): Instance of GrizzlyOperator
        executed.
      task_config (TGrizzlyTaskConfig): Task configuration
        contains parsed and pre-processed information from task YML file.
      write_disposition (string): Write disposition WRITE_APPEND, WRITE_EMPTY
        etc. This parameter is used for compatibility. Processing of this
        parameter should be implemented inside exporter class in case of
        importance.
    """

    etl_merge = ETLMerge(execution_context, task_config, write_disposition)
    etl_merge.merge()

  @classmethod
  def export_data(cls,
                  execution_context: TGrizzlyOperator,
                  task_config: TGrizzlyTaskConfig,
                  write_disposition: str) -> Optional[TQueryJob]:
    """Exports data to external storage (GS for example).

    Args:
      execution_context (TGrizzlyOperator): Instance of GrizzlyOperator
        executed.
      task_config (TGrizzlyTaskConfig): Task configuration
        contains parsed and pre-processed information from task YML file.
      write_disposition (string): Write disposition WRITE_APPEND, WRITE_EMPTY
        etc. This parameter is used for compatibility. Processing of this
        parameter should be implemented inside exporter class in case of
        importance.

    Returns:
        (TQueryJob, None): Job execution statistics
    """

    # get upstream data source_type. If not defined use BQ as source
    export_type = task_config.export_type
    target_exporter = {
        'files': 'grizzly.exporters.exporter_files.ExporterFiles',
        'custom': task_config.source_extractor
    }

    module_name, class_name = target_exporter[export_type].rsplit('.', 1)
    module = importlib.import_module(module_name)
    exporter_class = getattr(module, class_name)

    exporter = exporter_class(
        execution_context=execution_context,
        task_config=task_config,
        write_disposition=write_disposition)

    class_name = str(type(exporter))
    execution_context.log.info(f'ETL: Extracting data with [{class_name}]')

    # execute ETL process
    for e in exporter.extract():

      # stop further processing in case of empty resultset
      if e['metadata'] is not None and not e['rows']:
        break

      # transform upstream data
      e = exporter.transform(e)

      # perform loading of transformed data
      exporter.load(e)

    return getattr(exporter, 'job_stat', None)
