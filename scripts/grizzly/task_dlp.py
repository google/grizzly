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

"""DLP task module.

  DLPTask represents yaml config of a DLP inspect job.
  It is inherited from grizzly.task.Task.

  Typical usage example:

  DLPTask(task_file, self.source_path)
"""

from typing import List

from grizzly.task import Task


class DLPTask(Task):
  """Representation of DLP task YML file.

  This class overwrites the _get_source_tables to return the table which will be
  inspected by DLP.
  """

  def __init__(self, *args, **kwargs) -> None:
    """Initialize DLPTask."""
    super().__init__(*args, **kwargs)
    # replace the lowercase operator name with camel case class name
    self.operator_type = "GrizzlyDLPOperator"

  def _get_source_tables(self) -> List[str]:
    """Get the list of source tables.

    DLP task can have one source table: the inspect table, if the storage
    type is BigQuery. Or it can have zero source tables, if it scans Cloud
    Storage.

    Returns:
      (List[str]): list of source tables
    """
    # if source_table is defined, return it. Otherwise, return an empty list
    dlp_config = self.raw_config['dlp_inspect_config']
    source_table = dlp_config.get('source_table_name', None)
    if source_table:
      return [source_table]
    return []
