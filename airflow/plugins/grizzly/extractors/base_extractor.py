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

"""Definition of base class for all data extractors plugins.

  BaseExtractor provides functionality for creation of data extraction plugins.

  Typical usage example:

  class ExtractorNew(BaseExtractor):
  ...
"""


from typing import Any, Dict, Generator, Optional
from grizzly.grizzly_typing import TGrizzlyOperator
from grizzly.grizzly_typing import TGrizzlyTaskConfig


class BaseExtractor:
  """Base class for data extraction plugins.

  Base extractor defines next method that could be optionally overwritten in
  implementation classes.
  extract - this method extract data from source.
  transform - performs data transformation actions.
  load - load data into target table.

  Attributes:
    task_config (TGrizzlyTaskConfig): Task configuration with
      parsed and pre-proccessed information from task YML file.
    target_table (string): Name of a ETL target table.
    execution_context (TGrizzlyOperator): Instance of GrizzlyOperator executed.
    write_disposition (string): BQ write disposition WRITE_APPEND, WRITE_EMPTY,
      WRITE_TRUNCATE. In case if etl_factory use EtractorBQ for staging table it
      will be WRITE_TRUNCATE. If it executed for table defined in
      [target_table_name] attribute of task YML file this class attribute will
      be  equal to [job_write_mode] attribute of task YML file. Also in case if
      data loaded by chunk not as one piece this attribute could be used for
      orchestration when new portion should be loaded after truncate
      (1st portion of data) or appended to target tables.
    job_stat (Any): Object or dictionary with job execution statistic.
  """

  def __init__(self,
               execution_context: Optional[TGrizzlyOperator] = None,
               task_config: Optional[TGrizzlyTaskConfig] = None,
               target_table: Optional[str] = None,
               write_disposition: Optional[str] = None,
               *args: Any,
               **kwargs: Any) -> None:
    """Init basic attributes for data extractors."""
    self.task_config = task_config
    self.target_table = target_table
    self.execution_context = execution_context
    self.write_disposition = write_disposition
    self.job_stat = None

  def extract(self) -> Generator[Dict[str, Any], None, None]:
    """Basic implementation of generator for data extraction.

    In case if implementation of this method is skipped in child class then
    empty basic resultset is used in further steps. For example in BQ-To-BQ
    case could be implemented only one load method that will put result of
    query to target table directly.
    Implementation of extract method should yield chunks of data for further
    proccessing in transform and load method.

    Yields:
      (Dict[str, Any]): return input data for transform and load steps.
        It's mandatory to return next keys [metadata, rows].
        Example of data:
        {
          'metadata': [
                      {'name': 'id', 'type': 'INT64', 'mode': 'NULLABLE'},
                      {'name': 'value', 'type': 'STRING', 'mode': 'NULLABLE'}
                    ],
          'rows': [
                      ['1', 'Aa'],
                      ['2', 'Bb'],
                      ['3', 'Cc']
                    ]
        }
    """
    # return placeholder with 1 fake row
    yield {'metadata': None, 'rows': []}

  def transform(self, data: Dict[str, Any]) -> Any:
    """Basic implementation of data transformation.

    Basic implementation just bypass data recieved from extract method.

    Args:
      data (Dict[str, Any]): Dictionary yielded by extract method

    Returns:
      (Any): Transformed data.
    """
    return data

  def load(self, data: Any) -> None:
    """Basic implementation of load method.

    In case if you implement this method in your child class you should
    implement data loading into target BQ table.
    Optionally after data loading you can update self.job_stat with job
    execution statistic for further logging in etl_log table.

    Args:
      data (Any): Data that was returned by transform method for uploading into
        BQ target table.
    """
    pass
