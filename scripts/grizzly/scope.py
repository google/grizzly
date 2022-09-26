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

"""Definintion of Scope class.

Scope class represent SCOPE YML configuration file with information about tasks
included into domain's DAG.
This information is used for generation of DAG's py and DAG's yml files with
DAG configuration for further deploiment on GCP Composer

Typical usage example:
deployment_scope = Scope(source_path)
"""
import pathlib
import shutil
import tempfile

from grizzly.composer_item import ComposerItem
from grizzly.task import Task
from grizzly.task_dlp import DLPTask
import jinja2
import yaml


class ScopeConfig():
  """SCOPE.yml file raw and parsed content.

  Atrributes:
    scope_file (Dictionary): Dictionary with content of SCOPE.yml file.
    etl_scope (List[string]): List of ETL tasks to be executed by Airflow during
      DAG execution.
    domain_name (string): DAG/domain name. Application uses source_path folder
    name if not defined
    schedule_interval (string): DAG execution schedule in CRON format.
    execution_timeout_per_table: Task execution timeout in seconds.
      If you want a task to have a  maximum runtime, set its execution_timeout
      attribute that is the maximum permissible runtime in seconds. If any task
      runs longer, Airflow will kick in and fail the task with a timeout
      exception. Default value 20 minutes.
    doc_md (string): Reference to md file. It's possible to add documentation
      or notes to your DAGs objects that are visible in the web interface
    tags (List[string]): List of DAG's tags. Tags could be used in Airflow UI
      for filtering purpose.
  """

  def __init__(self, scope_file: pathlib.Path) -> None:
    """Init instance of ScopeConfig with information from SCOPE.yml file.

    Args:
        scope_file (pathlib.Path): Path to SCOPE.yml file with definition of ETL
          used for DAG(domain) generation.
    """
    raw_config = yaml.safe_load(scope_file.read_text())
    self.raw_config = raw_config
    # MANDATORY parameters
    self.etl_scope = raw_config['etl_scope']
    # OPTIONAL parameters
    # If not defined get domain_name as a name of folder with SCOPE.yml file
    self.domain_name = raw_config.get('domain_name',
                                      scope_file.parent.name.upper())
    self.schedule_interval = raw_config.get('schedule_interval', None)
    self.execution_timeout_per_table = raw_config.get(
        'execution_timeout_per_table', 1200)
    # It's possible to add documentation or notes to your DAGs & task
    # objects that are visible in the web interface
    self.doc_md = raw_config.get('doc_md', None)
    # Add tags to DAGs and use them for filtering in the UI
    self.tags = None
    if 'tags' in raw_config:
      self.tags = [raw_config['tags']] if isinstance(
          raw_config['tags'], str) else raw_config['tags']


class Scope(ComposerItem):
  """Represent ETL Scope data.

  Instance of object contains parsed SCOPE.yml files with all referenced files
  from [etl_scope] and [doc_md] attributes.
  ETL Scope data extracted from SCOPE.yml file that located in the folder
  provided in source_path program input parameter.

  SCOPE.yml file supports next parameters:
    schedule_interval: (optional) define DAG execution schedule in CRON format.
      Could be empty.
    domain_name: (optional) define domain name. Application uses source_path
      folder name if not defined
    execution_timeout_per_table: (optional) Task execution timeout in seconds.
      If you want a task to have a  maximum runtime, set its execution_timeout
      attribute that is the maximum permissible runtime in seconds. If any task
      runs longer, Airflow will kick in and fail the task with a timeout
      exception.
    etl_scope: Define a list of ETL tasks to be executed by Airflow during DAG
      execution.
    doc_md: (optional) Reference to md file. It's possible to add documentation
      or notes to your DAGs objects that are visible in the web interface
    tags: (optional) Add tags to DAGs and use it for filtering in the UI.

  Atrributes:
    scope_file (pathlib.Path): Reference to SCOPE.yml file. This file should be
      placed inside [source_path] dirrectory.
    config (ScopeConfig): Instance of ScopeConfig with raw and parsed content of
      SCOPE.yml file.
    temp_path (pathlib.Path): Reference to temp folder. During deployment
      application generates all required files (py, yml, sql, json, md, etc.) in
      /tmp/AIRFLOW.{something}/{SCOPE.yml:domain_name} folder.
    files (Set[pathlib.Path]): Set of files to be deployed to Airflow.
      This set contains:
        - [domain_name].py file with DAG definition. This file is generated
          by application.
        - [domain_name].yml file with definition of all dependencies between
          tasks. This file contains all parent BQ tables for all task
          from the SCOPE.
        - SCOPE.yml
        - All yml files referenced in [etl_scope].
        - md file referenced in [doc_md].
        - sql, md, json files referenced in task yml files.
    tasks (Dict[grizzly.task.Task]): Dictionary with all tasks from scope.
      Dictionary has next structure:
      {"task_id": task_instance}
      Ordinary task_id match with target table name.
    scope_tables (Dict[List[string]]): Dependencies in DAG files.
      Dictionary contains refference from target table name to task_id where
      this table defined as source.
      It has next structure:
        {'table_name': [task_id_1, task_id_2]}
  """

  def __init__(self, source_path: pathlib.Path) -> None:
    """Init instance of Scope with data from SCOPE.yml file.

    File references will be extended with file extention.

    Args:
        source_path (pathlib.Path): Reference to domain folder. This path is
          passed as [source_path] input parameter for application.
    """
    self.scope_file = source_path / 'SCOPE.yml'
    super().__init__(source_path, self.scope_file)
    self.config = ScopeConfig(self.scope_file)
    self.temp_path = pathlib.Path(
        tempfile.mkdtemp(prefix='AIRFLOW.')) / self.config.domain_name
    self.temp_path.mkdir()
    # Get list of files to be deployed
    self.files = {self.scope_file}
    # DAG documentation .md file
    if self.config.doc_md is not None:
      self.config.doc_md = self._normalize_file_name(self.config.doc_md, '.md')
      self.files.add(self.config.doc_md)
    # Load task .yml files
    self.tasks = dict()
    # we use self.scope_tables dict for build dependencies in DAG files
    # it contains a reference from Table name to task_id where
    # this table defined as source
    # {'table_name': [task_id_1, task_id_2]}
    self.scope_tables = dict()
    for f in self.config.etl_scope:
      task_file = self._normalize_file_name(f, '.yml')
      # check if we need to create DLPTask
      # DLP jobs will have operator_type specified
      # otherwise create regular Task
      operator_type = yaml.safe_load(task_file.read_text()).get('operator_type')
      if operator_type == 'grizzly_dlp_operator':
        task_instance = DLPTask(task_file, self.source_path)
      else:
        task_instance = Task(task_file, self.source_path)
      # self.tasks.append(task_instance)
      self.tasks[task_instance.task_id] = task_instance
      self.files.update(task_instance.files)
      # link tables with files defined in scope
      # only if target_table_name was defined in YML
      if task_instance.target_table_name:
        if task_instance.target_table_name not in self.scope_tables:
          self.scope_tables[task_instance.target_table_name] = [
              task_instance.task_id]
        else:
          self.scope_tables[task_instance.target_table_name].append(
              task_instance.task_id)
    return

  def generate_stagging_files(self) -> None:
    """Copy deploment files into [/tmp] folder.

    All files references in domain scope (yml, sql, md, json, etc.) will be
    preloaded first into [/tmp] folder. Also method generates {domain_name}.yml
    file with additional domain related information.
    For example {domain_name}.yml contains information about table dependencies.
    """
    domain_name = self.config.domain_name
    print(f'Files for [{domain_name}] will be generated in {self.temp_path}')
    for f in self.files:
      target_file = self.temp_path / f.relative_to(self.source_path)
      # create target temp folder in case of importance
      target_file.parent.mkdir(parents=True, exist_ok=True)
      shutil.copyfile(f, target_file)
    # generate file with derived attributes
    derived_attributes = {}
    # get source_tables
    source_tables = {
        t: ti.source_tables for t, ti in self.tasks.items() if ti.source_tables
    }
    if source_tables:
      derived_attributes['bq_source_tables'] = source_tables
    (self.temp_path / f'{domain_name}.yml').write_text(
        yaml.dump(derived_attributes))
    return

  def generate_DAG_file(self, dag_template: pathlib.Path) -> None:
    """Generate DAG file for requested scope.

    DAG generated on a base of JINJA2 template [./templates/dag.py.jinja2]

    Args:
        dag_template (pathlib.Path): Reference to DAG template.
    """
    domain_name = self.config.domain_name
    loader = jinja2.FileSystemLoader(dag_template.parent)
    jinja_env = jinja2.Environment(loader=loader)
    # build dependencies
    dependencies = []
    for task_id, task_instance in self.tasks.items():  # iterate tasks
      if task_instance.parent_tasks:
        # get a list of parent tasks from parameter
        gen = (t for t in task_instance.parent_tasks)
      else:
        # check source tables used in task
        # iterate source table used inside task
        # if source table is part of SCOPE then pickup source tasks for it
        gen = (
            t
            for t in task_instance.source_tables
            if t in self.scope_tables
        )
      for st in gen:
        # Adjusts child_task trigger_rule to none_failed
        # if any source task has custom schedule defined.
        if any(self.tasks[t].schedule_interval
               for t in self.scope_tables.get(st, [st])):
          task_instance.trigger_rule = 'none_failed'
        # generate string in format task_id_2 << [task_id_1_1, task_id_1_2]
        # next code prevent reference task as own parent
        # pylint: disable=cell-var-from-loop
        parent_tasks_list = [
            x
            for x in self.scope_tables.get(st, [st])
            if x != task_id
        ]
        item_parents = {
            'child_task':
                task_id.replace('.', '_'),
            'parent_tasks':
                # Transform ['bas_a.t1', 'bas_b.t2'] => "[bas_a_t1, bas_b_t2]"
                str(parent_tasks_list).replace('\'', '').replace('.', '_'),
            'label':
                task_instance.trigger_rule
        }
        dependencies.append(item_parents)
    jinja_template = jinja_env.get_template(dag_template.name)
    dag_file = jinja_template.render(
        dag_config=self.config,
        tasks=list(self.tasks.values()),
        dependencies=dependencies)
    (self.temp_path / f'{domain_name}.py').write_text(dag_file)
    return
