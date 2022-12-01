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

"""Implementation of BigQuery extract class.

ExtractorBQ is inherited from grizzly.extractors.base_extractor.BaseExtractor
Only load method implemented. Extract and load methods are inherited from
BaseExtractor, and they just bypass default empty objects.
Instance of this class created and used by grizzly.etl_factory.ETLFactory
For more insights check implementation of base_extractor and etl_factory.
"""

import pathlib
from typing import Any, Dict, Generator, Optional

from airflow.exceptions import AirflowException
import geopandas as gpd
import grizzly.etl_action
from grizzly.etl_audit import ETLAudit
from grizzly.extractors.base_url_extractor import BaseURLExtractor


class ExtractorShapefile(BaseURLExtractor):
  """Implementation of Shapefile to BQ data loading.

  Attributes:
    task_config (TGrizzlyTaskConfig): Task configuration with parsed and
      pre-processed information from task YML file.
    target_table (string): Name of a table where query execution results should
      be stored.
    execution_context (TGrizzlyOperator): Instance of GrizzlyOperator executed.
    write_disposition (string): BQ write disposition WRITE_APPEND, WRITE_EMPTY,
      WRITE_TRUNCATE. In case if etl_factory use ExtractorBQ for staging table
      it will be WRITE_TRUNCATE.
      If it executed for table defined in [target_table_name] attribute of task
      YML file this class attribute will be  equal to [job_write_mode] attribute
      of task YML file.
    job_stat (QueryJob): Job execution statistics.
  """

  def transform(self, data: Dict[str, Any]) -> Dict[str, Any]:
    """Transform input shapefile into CSV.

    Method returns data converted to CSV instead of original file.

    Args:
      data (Dict[str, Any]): Dictionary yielded by extract method

    Returns:
      (Dict[str, Any]): Transformed data.
    """
    self.execution_context.log.info(
        f'Transformation of [{self.data_url}] to CSV')
    converted_data_file = pathlib.Path(self.data_path, 'data.csv')
    shape_file = pathlib.Path(data['rows'][0])
    if shape_file.suffix == '.zip':
      shape_file_path = f'zip://{shape_file}'
    else:
      shape_file_path = shape_file

    sdf = gpd.read_file(shape_file_path)
    sdf.to_csv(converted_data_file, index=False)
    data['rows'][0] = str(converted_data_file)
    data['metadata']['source_format'] = 'CSV'
    return data

  def _execute_audit_columns(self):
    """Execute DDL for adding audit columns."""

    sql = ETLAudit.get_column_statement(
        execution_context=self.execution_context,
        task_config=self.task_config)

    self.job_stat = grizzly.etl_action.run_bq_query(
        execution_context=self.execution_context,
        sql=sql,
        use_legacy_sql=self.task_config.is_legacy_sql)
