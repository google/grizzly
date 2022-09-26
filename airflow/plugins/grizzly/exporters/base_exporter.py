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

"""BaseExporter for export plugins.

  BaseExporter provides functionality for creating export plugins.

  Typical usage example:

  class ExporterFiles(BaseExporter):
  ...
"""
from typing import Any, Dict, Generator, Optional
from grizzly.grizzly_typing import TGrizzlyOperator
from grizzly.grizzly_typing import TGrizzlyTaskConfig


class BaseExporter:
  """BaseExporter abstract class for export plugins.

  The BaseExporter abstract class provide option to create custom export plugins
  for any type of sources and target. Class contains three basic functions:
   extract, transform, load.

  Attributes:
    execution_context: GrizzlyOperator instance of the current execution context
      task_config: TaskInstance instance of the current task
    write_disposition: string with type of write disposition
      write_append, write_truncate, etc.
    job_stat (Any): Job execution statistic.
  """

  def __init__(self,
               execution_context: Optional[TGrizzlyOperator] = None,
               task_config: Optional[TGrizzlyTaskConfig] = None,
               write_disposition: Optional[str] = None) -> None:
    """Initialize instance of BaseExporter."""
    self.task_config = task_config
    self.execution_context = execution_context
    self.write_disposition = write_disposition
    self.job_stat = None

  def extract(self) -> Generator[Dict[str, Any], None, None]:
    """Extract data from the source and return rows.

    Yields:
      Extracted data in a format
      {'metadata': None, 'rows': []}
    """

    # return placeholder with 1 fake row
    yield {'metadata': None, 'rows': []}

  def transform(self, data: Dict[str, Any]) -> Any:
    """Transform data recieved from extract method.

    Args:
      data: Dictionary yielded by extract method

    Returns:
      Transformed data.
    """
    return data

  def load(self, data: Any) -> None:
    """Load data recieved from transform method.

    Args:
      data: Transformed data.
    """
    pass
