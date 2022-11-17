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

"""Task instance class."""

import pathlib
from typing import Dict
import yaml

from sql_graph.exceptions import ParsingError


class Task:
  """Class representing a Grizzly task instance.

    Attributes:
      task_id (str): task id string.
      files (Dict[str, str]): dictionary of all task files, where paths are
        the keys, and values are file contents.
      config_path (pathlib.Path): path object of the .yml config file.
      target_table_name (str): name of the target table.
      job_write_mode (str): job write mode of the query.
      descriptions (Dict): dictionary with table and column descriptions.
      query (str): stage loading query of the task.
  """

  def _find_config_file(self) -> pathlib.Path:
    """Looks up the confing file in the files list.

    Config file is the one and only one file that ends with a .yml.

    Returns:
      pathlib.Path: path object of the config file.
    """
    for path in self.files:
      if path.endswith(self.task_id + ".yml"):
        return pathlib.Path(path)
    raise ParsingError(f"Config file was not found for task {self.task_id}")

  def _get_query(self) -> str:
    """Returns the query string if it is present.

    Returns:
      str: query string.
    """
    if "stage_loading_query" in self.raw_config:
      query_path = pathlib.Path(self.raw_config["stage_loading_query"])
      query_path = self.config_path.parent / query_path
      query = self.files[str(query_path)]
      query = query.replace(r"\n", "\n").replace("\\\"", "\"")[1:-1]
      return query
    return ""

  def __init__(self, task_id: str, files: Dict[str, str]) -> None:
    self.task_id = task_id
    self.files = files

    self.config_path = self._find_config_file()
    self.raw_config = yaml.safe_load(self.files[str(self.config_path)])
    self.target_table_name = self.raw_config.get("target_table_name", None)
    self.job_write_mode = self.raw_config.get("job_write_mode", None)
    self.descriptions = self.raw_config.get("descriptions", None)
    self.query = self._get_query()
