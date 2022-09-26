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

"""Definintion of Task class.

Task class represent task YML configuration file.
Instance of Task also contains information about all files referenced in task
YML file. This information is used for generation of Airflow task dependencies
and for definition of list of files to be copied to target GCP environment.

Typical usage example:
task_instance = Task(task_file, self.source_path)
"""

import pathlib
import re
from typing import List, Set
from grizzly.composer_item import ComposerItem
import yaml


class Task(ComposerItem):
  """Representation of task YML file.

  Each item in SCOPE.yml is parsed into a separate task.

  Attributes:
    task_id (string): Task name. It is equal to file name without extention.
    file_id (pathlib.Path): Reference to file YML file.
    files (list(pathlib.Path)): List of files to be deployed for coreect work of
      task. This list contains files defined in next task YML attributes
      [doc_md, stage_loading_query, job_data_quality_query, pre_etl_scripts,
      post_etl_scripts, data_catalog_tags, access_scripts] plus task YML file
      itself. If file reference was define without file extention then
      correspondent file extention will be used.
    raw_config (dict): Content of task YML file loaded as dictionary.
    target_table_name (string): Target table name defined in [target_table_name]
      attribute of task YML file.
    schedule_interval (string): Custom schedule interval that could be defined
      in [schedule_interval] attribute of task YML: file.
    trigger_rule (string): Custom Airflow task Trigger Rule that could be
      defined in [trigger_rule] attribute of task YML file. List of supported
      trigger rules available here
      https://airflow.apache.org/docs/apache-airflow/stable/concepts/dags.html?highlight=trigger_rule#trigger-rules
    parent_tasks (list(string)): List of parent tasks defined in [parent_tasks]
      attribute of task YML file. If this attribute defined it will override
      default calculation of parent tasks on a base of table used in BQ query
      defined in [stage_loading_query]
    doc_md (pathlib.Path): Reference to MD file with task documentation. More
      details about Airflow task documeentation available here:
      https://airflow.apache.org/docs/apache-airflow/stable/concepts/dags.html?highlight=trigger_rule#dag-task-documentation
    source_tables (list(string)): List of source tables used in query defined in
      [stage_loading_query] attribute of task YML file.
  """

  def __init__(self, file_path: pathlib.Path,
               source_path: pathlib.Path) -> None:
    super().__init__(source_path, file_path)
    self.task_id = file_path.stem
    # task_id is equal to filename without extention
    self.file_id = file_path.relative_to(source_path)
    self.files = {file_path}
    # read YML file
    self.raw_config = yaml.safe_load(file_path.read_text())
    self.target_table_name = self.raw_config.get('target_table_name', None)
    self.schedule_interval = self.raw_config.get('schedule_interval', None)
    self.operator_type = self.raw_config.get('operator_type', 'GrizzlyOperator')
    self.trigger_rule = None
    if 'trigger_rule' in self.raw_config:
      self.trigger_rule = self.raw_config['trigger_rule'].lower()
    self.parent_tasks = None
    if 'parent_tasks' in self.raw_config:
      if isinstance(self.raw_config['parent_tasks'], str):
        self.parent_tasks = [self.raw_config['parent_tasks']]
      else:
        self.parent_tasks = self.raw_config['parent_tasks']
    if 'doc_md' in self.raw_config:
      self.doc_md = self._normalize_file_name(self.raw_config['doc_md'], '.md')
    # Get list of files to be deployed
    etl_files = [('doc_md', '.md'), ('stage_loading_query', '.sql'),
                 ('job_data_quality_query', '.sql'),
                 ('pre_etl_scripts', '.sql'), ('post_etl_scripts', '.sql'),
                 ('data_catalog_tags', '.json'), ('access_scripts', '.sql')]
    for f in etl_files:
      parameter_name, file_type = f
      self.files.update(self._get_files_by_reference(parameter_name, file_type))
    # Get a list of source tables
    self.source_tables = self._get_source_tables()
    return

  def _get_source_tables(self) -> List[str]:
    """Parse SQL query and return a list of source tables.

    Parsing performed by regular expression and only tables wrapped with ` or []
    are considered as parent tables.

    Returns:
      (list[str]): List of parent BigQuery table used in task.
    """
    target_table = self.raw_config.get('target_table_name', None)
    stage_loading_query = ''
    if 'stage_loading_query' in self.raw_config:
      stage_loading_query = self._normalize_file_name(
          self.raw_config['stage_loading_query'], '.sql').read_text()
    # remove all comments from SQL code for cleanup it.
    # Need to cleanup as comment could contain reference for some tables.
    sql_text = re.sub(r'(((/\*)+?[\w\W]+?(\*/)+))|(--.*)|(#.*)', ' ',
                      stage_loading_query)
    # return a list of all used source(parent) tables.
    # Support LEGACY and Standard SQL syntax
    parent_table_list = re.findall(r'[`\[.](\w+\.\w+)[`\]]', sql_text)
    # remove duplicates and target table itself
    parent_table_list = list(dict.fromkeys(parent_table_list))
    if target_table in parent_table_list:
      parent_table_list.remove(
          target_table
      )  # required for correct work of UPDATE and DELETE etl mode
    return parent_table_list

  def _get_files_by_reference(self, parameter_name: str,
                              file_extention: str) -> Set[pathlib.Path]:
    """Return files defined in Task parameters.

    File could be defined as a string value or as a List.

    Args:
      parameter_name (str): Name of parameter from task YML file to be analysed.
      file_extention (str): File extention to be used in case if it was not
        defined in parameter file reference.

    Returns:
      Set of files defined in task YML file parameter.
    """
    resultset = set()
    if parameter_name in self.raw_config:
      # transform string to list element in case if only 1 file defined
      if isinstance(self.raw_config[parameter_name], str):
        fl = [self.raw_config[parameter_name]]
      else:
        fl = self.raw_config[parameter_name]
      resultset = {self._normalize_file_name(f, file_extention) for f in fl}
    return resultset
