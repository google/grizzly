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

"""Definition of base class for all grizzly items.

Base class is used for definition of Scope and Task classes.
Also it contains methods common for all grizzly items.

Typical usage example:
class Task(ComposerItem):
  ...
"""
import pathlib


class ComposerItem():
  """Base class for all GCP Composer deployable items like DAGs and Tasks.

  Attributes:
    source_path (string): Reference to root project(domain) location. This path
      is passed as input for deploy_composer.py and used for calculation of
      absolute passes for all files referenced in SCOPE.yml or inside task yml.
    file_ref (string): Reference to configuration YML file.
      It could be SCOPE.yml that defines DAG or task yml file.
  """

  def __init__(
      self,
      source_path: pathlib.Path,
      file_ref: pathlib.Path,
  ) -> None:
    """Initialize composer item.

    It could be item that represent ETL DAG-scope (SCOPE.yml) or ETL
    task-item (table loading).

    Args:
      source_path (pathlib.Path): Reference to configuration YML file.
        It could be SCOPE.yml that defines DAG or task yml file.
      file_ref (pathlib.Path): [description]
    """
    self.source_path = source_path
    self.file_ref = file_ref

  def _normalize_file_name(
      self,
      file_name: str,
      file_extention: str
  ) -> pathlib.Path:
    """Normalize file names.

    If file name was defined in YML file without extension this method will
    add it. For example references to queries could be defined without .sql
    file extension. This method will automatically add .sql to file name in
    case of importance for correct work with referenced file.

    Args:
      file_name (string): File name to be verified and adjusted.
      file_extention (string): File extension. File extention depends from YML
        parameteter. Some parameters require SQL, other YML extension. If user
        did not defined file reference with extention then default extension
        will be applied.

    Returns:
        (pathlib.Path) Reference to file.
    """
    normalized_file_name = self.source_path / file_name
    if normalized_file_name.suffix != file_extention:
      normalized_file_name = normalized_file_name.parent / (
          f'{normalized_file_name.name}{file_extention}')
    return normalized_file_name
