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

"""Definition of base class for URL data extractors plugins.

  BaseURLExtractor provides functionality for creation of data extraction
  plugins for web based data source.

  Typical usage example:

  class ExtractorShapeFiles(BaseURLExtractor):
  ...
"""

import pathlib
import shutil
import tempfile
from typing import Any, Dict, Generator, Optional

from airflow.exceptions import AirflowException
from grizzly.config import Config as GrizzlyConfig
from grizzly.extractors.base_extractor import BaseExtractor
from grizzly.grizzly_typing import TGrizzlyOperator
from grizzly.grizzly_typing import TGrizzlyTaskConfig

import requests


class BaseURLExtractor(BaseExtractor):
  """Implementation of ETL for BQ to BQ data loading.

  Attributes:
    task_config (TGrizzlyTaskConfig): Task configuration with parsed and
      pre-proccessed information from task YML file.
    target_table (string): Name of a table where query execution results should
      be stored.
    execution_context (TGrizzlyOperator): Instance of GrizzlyOperator executed.
    write_disposition (string): BQ write disposition WRITE_APPEND, WRITE_EMPTY,
      WRITE_TRUNCATE. In case if etl_factory use EtractorBQ for staging table it
      will be WRITE_TRUNCATE.
      If it executed for table defined in [target_table_name] attribute of task
      YML file this class attribute will be  equal to [job_write_mode] attribute
      of task YML file.
    job_stat (QueryJob): Job execution statistics.
    data_url (str): Source Data URL.
    dag_name (str): Airflow DAG name.
    task_name (str): Airflow task name.
    data_path (pathlib.Path): Stores the data that tasks produce and use.
      This folder is mounted on all worker nodes.
  """

  COMPOSER_DATA_FOLDER = pathlib.Path('/home/airflow/gcs/data/imports/')

  def __init__(self,
               execution_context: Optional[TGrizzlyOperator] = None,
               task_config: Optional[TGrizzlyTaskConfig] = None,
               target_table: Optional[str] = None,
               write_disposition: Optional[str] = None,
               *args: Any,
               **kwargs: Any) -> None:
    """Initialize instance of extractor.

    Perform initial configuration and setup GSheet client.

    Args:
      execution_context (TGrizzlyOperator): Instance of GrizzlyOperator
        executed.
      task_config (TGrizzlyTaskConfig): Task configuration with
        parsed and pre-proccessed information from task YML file.
      target_table (string): Name of a table where GSheet data should be stored.
      write_disposition (string): BQ write disposition WRITE_APPEND, WRITE_EMPTY
        WRITE_TRUNCATE. In case if etl_factory use EtractorBQ for staging table
        it will be WRITE_TRUNCATE. If it executed for table defined in
        [target_table_name] attribute of task YML file this class attribute will
        be  equal to [job_write_mode] attribute of task YML file.
      *args (list): Optional attributes.
      **kwargs (dict): Optional attributes.


    Raises:
      AirflowException: Raise exception in case if GSheet Id reference was not
        defined in task YML file.
    """
    super().__init__(execution_context, task_config, target_table,
                     write_disposition, *args, **kwargs)
    self.data_url = self.task_config.source_data_url
    self.dag_name = self.task_config.dag_name
    self.task_name = self.task_config.task_name
    self.data_path = pathlib.Path(self.COMPOSER_DATA_FOLDER,
                                  self.dag_name,
                                  self.task_name)
    # generate directory if it does not exists
    self.data_path.mkdir(parents=True, exist_ok=True)

  def extract(self) -> Generator[Dict[str, Any], None, None]:
    """Basic implementation of generator for URL files data extraction.

    In case if implementation of this method is skipped in child class then
    empty basic resultset is used in further steps.

    Yields:
      (Dict[str, Any]): return input data for transform and load steps.
        It's mandatory to return next keys [metadata, rows].
        Example of data:
        {
          'metadata': {
              'url': 'https://somewhere/file.xyz',
              'content_type': 'application/zip'
          },
          'rows': [
            '/home/airflow/gcs/data/imports/task_name/datafile.xyz'
          ]
        }
    """
    self.execution_context.log.info(f'URL to be loaded: [{self.data_url}]')
    headers = {'User-Agent': 'Mozilla/5.0'}
    web_response = requests.get(self.data_url, headers=headers, stream=True)
    file_name = None

    if web_response.status_code != 200:
      raise AirflowException(web_response.text)

    with tempfile.NamedTemporaryFile(prefix=f'{self.task_name}.',
                                     dir=self.data_path) as tmp:
      for chunk in web_response.iter_content(chunk_size=1048576):
        if chunk:  # filter out keep-alive new chunks
          if not file_name:
            file_name = web_response.headers.get('Content-Disposition', None)
            if file_name:
              file_name = file_name.replace('attachment; filename=', '')
              file_name = file_name.replace('"', '')
            else:
              file_name = self.data_url.split('/')[-1]
          tmp.write(chunk)
      tmp.flush()
      # when data loading completed copy to target file. This will help to
      # prevent situation when data accidentaly removed.
      shutil.copy2(tmp.name, str(self.data_path/file_name))
    content_type = web_response.headers.get('Content-Type', None)
    yield {
        'metadata': {
            'url': self.data_url,
            'content_type': content_type
        },
        'rows': [str(self.data_path/file_name)]
    }

  def transform(self, data: Dict[str, Any]) -> Dict[str, Any]:
    """Basic implementation of data transformation.

    Transform loaded file into AVRO, CSV or Parquet format for further loading
    into BQ. This method should be implemented in child.
    Source data should be transformed into AVRO format.

    Args:
      data (Dict[str, Any]): Dictionary yielded by extract method

    Returns:
      (Dict[str, Any]): Transformed data.
    """
    return data

  def load(self, data: Dict[str, Any]) -> None:
    """Load data into BQ table.

    Args:
      data: Recieve default empty object from transform method.
        This parameter here only for method interface compatibility purpose and
        does not affect any calculations.

    Raises:
        AirflowException: Incorrect query type.
    """

    source_format = data['metadata'].get('source_format', 'CSV')
    rows_data_url = data['rows'][0].replace(
        '/home/airflow/gcs',
        f'gs://{GrizzlyConfig.GS_BUCKET}'
    )
    self.execution_context.log.info(f'Downloading data file {rows_data_url}')

    job_id = self.execution_context.bq_cursor.run_load(
        destination_project_dataset_table=self.target_table,
        source_uris=rows_data_url,
        source_format=source_format,
        write_disposition=self.write_disposition,
        skip_leading_rows=1,
        autodetect=True
    )
    self.job_stat = self.execution_context.bq_client.get_job(job_id)
