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

ExtractorCSV is inherited from
grizzly.extractors.base_url_extractor.BaseURLExtractor
Only transform method implemented. Extract and load methods are inherited from
BaseURLExtractor.
Instance of this class created and used by grizzly.etl_factory.ETLFactory
For more insights check implementation of base_extractor and etl_factory.
"""

from typing import Any, Dict
from grizzly.extractors.base_url_extractor import BaseURLExtractor


class ExtractorCSV(BaseURLExtractor):
  """Implementation of CSV to BQ data loading.

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
    """Transform CSV file.

    Method bypass CSV file loaded by load method.

    Args:
      data (Dict[str, Any]): Dictionary yielded by extract method

    Returns:
      (Dict[str, Any]): Transformed data.
    """
    self.execution_context.log.info(f'Read CSV file [{self.data_url}].')
    data['metadata']['source_format'] = 'CSV'
    return data
