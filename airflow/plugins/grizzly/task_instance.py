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

"""Task instance module.

  TaskInstance is used as an additional level on the Airflow task instance
  to add more functionality and configuration to Grizzly platform.
  It represent Task Instance configuration on a base of task YML file.

  Typical usage example:

  task_config = grizzly.task_instance.TaskInstance(config_file_ref, context)
"""

import json
import pathlib
from typing import Any, Optional
from typing import List
from grizzly.config import Config as GrizzlyConfig
from grizzly.etl_action import parse_table
import jinja2
import yaml


class TaskInstance():
  """Represent configuration from task YML file.

  If some configurations were not defined in task YML this class could be used
  for retrieve of default values. All default value calculation should be placed
  only in this class.

  Attributes:
    _default_attribute_values (dict): Default configuration values. In case if
      some parameter was not defined in YML file default value could be recieved
      by __getattr__ from this attribute.
      Each YML file attribute has correspondent attribute in TaskInstance class.
    _load_as_sql (list(string)): List of attributes defined in task YML file
      to be loaded as SQL text file.
    _load_as_json (list(string)): List of attributes defined in task YML file
      to be loaded as dictionary from referenced JSON file.
    _load_as_yml (list(string)): List of attributes defined in task YML file
      to be loaded as dictionary from referenced YML file.
    _load_as_list_of_sql (list(string)): List of attributes defined in task YML
      file to be loaded as list of references to SQL text files.
    _context (GrizzlyOperator): Instance of GrizzlyOperator executed.
      Airflow task execution context that can be used to read the Airflow task
      and DAG configurations.
    _task_config_file (pathlib.Path): Reference to task YML file with
      configurations.
    _task_config_folder (pathlib.Path): Reference to folder that contain task
      YML file.
    gcp_project_id (string): Name of GCP project.
    _raw_config (dict): Content of task YML file.
    schedule_interval (string): Schedule interval. Task could have custom
      schedule interval defined in attribute [schedule_interval] that could
      override DAG schedule defined in SCOPE.yml file. If defined Task schedule
      should beless frequent than DAG schedule.
    is_custom_schedule (bool): True if custom schedule interval was defined in
      task YML file.
    target_hx_loading_indicator (string): Define importance of history table
      generation. If Airflow variable FORCE_OFF_HX_LOADING set to 'Y' this
      parameter will be forced to 'N'.
    _source_tables (list(string)): List of source tables used in BQ query.
      This list is generated during domain installation process and stored in
      data/ETL/{dag_name}/{dag_name}.yml file.
    staging_table_name (string): Staging table name. It has format
      {DATASET}.{TABLE_NAME}_{DAG_RUN_ID}
      Staging dataset is configured in Airflow variable [ETL_STAGE_DATASET].
    history_table_name (string): History table name. Table name template is
      configured in Airflow variable [HISTORY_TABLE_CONFIG].
    _build (dict): Build information from *_BUILD.yml
  """
  BASE_GS_PATH = pathlib.Path('/home/airflow/gcs')

  _load_as_sql = ['stage_loading_query']
  _load_as_json = ['data_catalog_tags']
  _load_as_yml = [
  ]  # just placeholder for future YML files refferenced by task config
  _load_as_list_of_sql = [
      'job_data_quality_query', 'pre_etl_scripts', 'post_etl_scripts',
      'access_scripts'
  ]

  _default_attribute_values = {
      '_source_tables': [],
      'target_table_name': None,
      'source_gsheet_id': None,
      'parent_tasks': None,
      'job_write_mode': None,
      'stage_loading_query': None,
      'target_hx_loading_indicator': 'N',  # Value adjusted inside self.__init__
      'use_legacy_sql': 'N',
      'job_data_quality_query': [],
      # schedule_interval Value adjusted inside
      # self.__init__ with default value from DAG
      'schedule_interval': None,
      'source_type': 'bq',
      'source_extractor': None,
      'source_config': None,
      'trigger_rule': None,
      'mysql_connection_id': None,
      'source_columns': None,
      'column_policy_tags': None,
      'data_catalog_tags': None,
      'staging_table_name': None,
      'history_table_name': None,
      'pre_etl_scripts': [],
      'post_etl_scripts': [],
      'access_scripts': [],
      # Exporter
      'export_type': 'files',
      'export_config': None,
      'notification_pubsub': None,
      # Audit
      'target_audit_indicator': 'N'
    }

  def __init__(self, task_config_path: str, context: Any) -> None:
    """Initialize the task instance.

    Setup task configuration from file referenced in task_config_path and task
    execution context.

    Args:
      task_config_path (string): Reference to task config YML file to be
        loaded.
      context (Any): Instance of GrizzlyOperator executed.
        Airflow task execution context that can be used to read the task
        configuration.
    """
    self._context = context
    dag_name = self._context['dag'].safe_dag_id
    self._task_config_file = self.BASE_GS_PATH / task_config_path
    self._task_config_folder = self._task_config_file.parent
    # setup config parameters
    self.gcp_project_id = GrizzlyConfig.GCP_PROJECT_ID

    # get task config from YML file
    self._raw_config = yaml.safe_load(self._task_config_file.read_text())
    self._raw_config = self.get_value_from_file(
        file=str(self._task_config_file), file_format='yml')

    # get schedule_interval from scope in case if it not defined per task
    if 'schedule_interval' not in self._raw_config:
      self.schedule_interval = self._context['dag'].schedule_interval
      self.is_custom_schedule = False
    else:
      self.is_custom_schedule = True

    # check for force turn off history
    if GrizzlyConfig.FORCE_OFF_HX_LOADING.upper() == 'Y':
      self.target_hx_loading_indicator = 'N'
    else:
      self.target_hx_loading_indicator = self._raw_config.get(
          'target_hx_loading_indicator', 'N').upper()

    # extract a list of parent tables
    dag_table_list_file = (
        self.BASE_GS_PATH / f'data/ETL/{dag_name}/{dag_name}.yml')
    task_name = self._context['task'].task_id
    self._source_tables = yaml.safe_load(dag_table_list_file.read_text()).get(
        'bq_source_tables', dict()).get(task_name, [])

    # extract build information from *_BUILD.yml
    build_file = (
        self.BASE_GS_PATH / f'data/ETL/{dag_name}/{dag_name}_BUILD.yml')
    self._build = None
    if build_file.exists():
      self._build = yaml.safe_load(build_file.read_text())

    # staging & history table name
    if 'target_table_name' in self._raw_config:
      target_table_name = self._raw_config['target_table_name']
      target_table = parse_table(target_table_name)
      # staging table name
      self.staging_table_name = '{DATASET}.{TABLE_NAME}_{DAG_RUN_ID}'.format(
          DATASET=GrizzlyConfig.ETL_STAGE_DATASET,
          TABLE_NAME=target_table_name.replace('.', '_'),
          DAG_RUN_ID=self._context['dag_run'].id)
      # get a name of history table
      history_dataset_id = jinja2.Template(
          GrizzlyConfig.HISTORY_TABLE_CONFIG['dataset_id']).render(
              target_dataset_id=target_table['dataset_id'])
      history_table_id = jinja2.Template(
          GrizzlyConfig.HISTORY_TABLE_CONFIG['table_id']).render(
              target_table_id=target_table['table_id'])
      self.history_table_name = f'{history_dataset_id}.{history_table_id}'

    # setup attributes from task config files
    for attr, val in self._raw_config.items():
      if attr not in self.__dict__:
        setattr(self, attr, val)
    # Enrich values with referenced files content
    # after loading of all other attributes
    for attr, val in self._raw_config.items():
      if attr in self._load_as_sql:
        val = self.get_value_from_file(val, 'sql')
      elif attr in self._load_as_json:
        val = self.get_value_from_file(val, 'json')
      elif attr in self._load_as_yml:
        val = self.get_value_from_file(val, 'yml')
      elif attr in self._load_as_list_of_sql:
        val = self.get_value_from_list(val, 'sql')
      else:
        continue
      setattr(self, attr, val)
    return

  @property
  def is_legacy_sql(self) -> bool:
    """Return True if use_legacy_sql was defined as True or Y in task YML file."""
    if self.use_legacy_sql in ['Y', 'y', 'True', 'true', True]:
      return True
    else:
      return False

  def __getattr__(self, name: str) -> Any:
    """Return default task instance attribute values.

    Return default attribute value in case if it was not defind in task YML
    or if it was not precalculated inside init method.
    Look up for default value in _default_attribute_values dictionary.

    Args:
      name (str): Attribute name

    Returns:
      (Any): Default attribute value.
    """
    return self._default_attribute_values[name]

  def get_value_from_file(self, file: str, file_format: str, **kwargs) -> Any:
    """Enrich task attribute with data from referenced file.

    In case if file was defined as Jinja template it will be rendered.

    Args:
      file (string): Reference to file to be loaded.
      file_format (string): File type. This attribute affects how file will be
        loaded.
        Will it be laoded as text or as object for JSON and YML.
      **kwargs (dict): Additional parameters to be passed to JINJA2 template
        renderer.

    Returns:
      (Any): File context as text or as dictionary.
    """
    file_format_executor = {
        'text': (lambda x: x),
        'sql': (lambda x: x),
        'json': (json.loads),
        'yml': (yaml.safe_load),
    }
    file_path = self._task_config_folder / file
    file_text_raw = file_path.read_text()
    if file_format in file_format_executor:
      # parse file as Jinja2 template.
      # Use task_config instance as Jinja variable
      parsed_data = jinja2.Template(file_text_raw).render(
          task_instance=self,
          grizzly_config=GrizzlyConfig,
          **kwargs)
      return file_format_executor[file_format](parsed_data)
    else:  # return raw text
      return file_text_raw

  def get_value_from_list(self, file_list: List[str], file_format: str,
                          **kwargs) -> List[Any]:
    """Parse a list of files and return a list of files content.

    Apply [get_value_from_file] method for each file in a list.

    Args:
      file_list (List[str] or string): List of files to be parsed. If parameter
        defined as string it's equal to a list with one item.
      file_format (str): File type. This attribute affects how file will be
        loaded. Will it be loaded as text for SQL or as object for JSON and YML.
      **kwargs (dict): Used for bypass of additional parameters for Jinja2
        template render.

    Returns:
      List[Any]: Files content as a list of values. One value per one file.
    """
    result = []
    if isinstance(file_list, str):
      file_list = [file_list]
    for f in file_list:
      result.append(self.get_value_from_file(f, file_format, **kwargs))
    return result

  def get_context_value(self, parameter_name: str) -> Optional[Any]:
    """Return value from Airflow context."""
    return self._context[parameter_name]

  def get_raw_config_value(
      self,
      parameter_name: str,
      default_value: Optional[Any] = None) -> Optional[Any]:
    """Return value from _raw_config."""
    return self._raw_config.get(parameter_name, default_value)
