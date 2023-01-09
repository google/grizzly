import base64
import json
import pathlib
from datetime import datetime
from typing import Any
from typing import Dict
from typing import Generator
from typing import Optional

import google.cloud.exceptions
import pandas as pd
import requests
from airflow.exceptions import AirflowException
from airflow.models import Variable
from google.cloud import bigquery

import grizzly.etl_action
from grizzly.extractors.base_url_extractor import BaseURLExtractor
from grizzly.grizzly_typing import TGrizzlyOperator
from grizzly.grizzly_typing import TGrizzlyTaskConfig


class ExtractorWordpress(BaseURLExtractor):
  """Implementation of WordPress to BQ export.

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
      If wp_delta_field_name is used, then write_disposition must be
      WRITE_APPEND.
    job_stat (QueryJob): Job execution statistics.
    is_new_table_flag (bool): Flag that define should we create a new table or
      append data to existing. If [write_disposition] is WRITE_APPEND then flag
      inited as False else as True.  After upload of first chunk of data it
      changed to False.
    wp_header (str): WordPress authorization header. Is retrieved from an
      Airflow variable, the name of which is given by wp_credentials_var_name in
      task_config.
    wp_api_endpoint (str): url of the WP endpoint for data export requests.
    delta_loading (bool): whether the export will work in delta mode. This is
      determined by a presence or absence of wp_delta_field_name variable
      in the task_config.
    wp_delta_field_name (str): name of the field used as the timestamp for delta
      loading.
    last_update_timestamp (datetime): if the target_table already exists, this
      variable will be populated by the latest timestamp loaded into the target
      table.
  """

  CHUNK_SIZE = 100
  LATEST_DATETIME_QUERY = """
  SELECT
     MAX(CAST({delta_column_name} AS DATETIME)) AS latest 
  FROM `{target_table}`;
  """

  def _get_header(self) -> dict:
    """Generates the WP authorization header.

    Returns:
      dict: WP authorization header.
    """
    wp_credentials = Variable.get(
      self.task_config.wp_credentials_var_name
    )  # retrieves credentials from airflow
    wordpress_token = base64.b64encode(wp_credentials.encode())
    wordpress_header = {
      'Authorization': 'Basic ' + wordpress_token.decode('utf-8')
    }
    return wordpress_header

  def _target_table_exists(self) -> bool:
    """Helper method to check for the existence of the target_table in BQ.

    Returns:
      bool: if the table exists.
    """
    target_table_parsed = grizzly.etl_action.parse_table(
      self.task_config.target_table_name)
    dataset_ref = bigquery.DatasetReference(
      project=target_table_parsed['project_id'],
      dataset_id=target_table_parsed['dataset_id'])
    table_ref = dataset_ref.table(target_table_parsed['table_id'])
    try:
      self.execution_context.bq_client.get_table(table_ref)
      return True  # no exception means that table exists
    except google.cloud.exceptions.NotFound:
      return False

  def _get_last_update_timestamp(self) -> datetime:
    """Queries the target_table to get the latest loaded timestamp.

    Name of the timestamp field is determined by self.wp_delta_field_name.

    Returns:
      datetime: timestamp of the newest exported record in target_table.
    """
    self.execution_context.log.info(f'Retrieving last loaded timestamp.')
    if not self._target_table_exists():
      self.execution_context.log.info(
        f'No previous records found, will load all records.')
      return datetime.fromisoformat("1970-01-01T00:00:00")
    else:
      query = self.LATEST_DATETIME_QUERY.format(
        delta_column_name=self.wp_delta_field_name,
        target_table=self.target_table
      )
      result = self.execution_context.bq_client.query(query).result()
      ts = list(result)[0][0]
      self.execution_context.log.info(f'Last loaded timestamp: {ts}.')
      return ts

  def __init__(self,
               execution_context: Optional[TGrizzlyOperator] = None,
               task_config: Optional[TGrizzlyTaskConfig] = None,
               target_table: Optional[str] = None,
               write_disposition: Optional[str] = None,
               *args: Any,
               **kwargs: Any) -> None:
    super().__init__(execution_context, task_config, target_table,
                     write_disposition, *args, **kwargs)
    self.is_new_table_flag = (False if write_disposition == 'WRITE_APPEND'
                              else True)
    self.wp_header = self._get_header()
    self.wp_api_endpoint = task_config.wp_api_endpoint
    self.delta_loading = 'wp_delta_field_name' in self.task_config.__dict__
    if self.delta_loading:
      if self.write_disposition != "WRITE_APPEND":
        raise AirflowException("wp_delta_field_name parameter is only supported"
                               " in the WRITE_APPEND mode")
      self.execution_context.log.info(f'Loading in delta mode.')
      self.wp_delta_field_name = task_config.wp_delta_field_name
      self.last_update_timestamp = self._get_last_update_timestamp()

  def _make_get_request(self, request_url: str) -> requests.Response:
    """Wrapper method for get request and exception handling.

    Returns:
      response: response from WP.
    Raises:
      AirflowException: in case of the response code being not 200.
    """
    response = requests.get(request_url, headers=self.wp_header)
    if response.status_code != 200:
      raise AirflowException(f"Got response with code {response.status_code}: "
                             f"{response.text}")
    else:
      self.execution_context.log.info(
        f'Get request to {request_url} is successful.')
      return response

  def _get_page_count(self, url, page_size) -> int:
    """Makes a request to get the number of pages in the pagination.

    Returns:
      int: number of pages.
    """
    request_url = f"{url}&page=1&per_page={page_size}"
    response = self._make_get_request(request_url)
    pages_count = response.headers['X-WP-TotalPages']
    page_count = int(pages_count)
    self.execution_context.log.info(f'Total pages to load: {page_count}.')
    return page_count

  def _get_row_timestamp(self, row) -> datetime:
    """Extracts timestamp from row JSON.

    Returns:
      datetime: row timestamp.
    """
    row_timestamp_str = row.get(self.wp_delta_field_name)
    row_timestamp = datetime.fromisoformat(row_timestamp_str)
    return row_timestamp

  def _get_last_new_row_index(self, rows) -> int:
    """Finds the index of the last unloaded row.

    Returns:
      int: index.
    """
    last_row_timestamp = self._get_row_timestamp(rows[-1])
    # check the last row first.
    if last_row_timestamp > self.last_update_timestamp:
      # if it is unloaded, all the rows on the page will be loaded.
      return len(rows)
    else:
      # otherwise, scan rows one by one to find the last unloaded row.
      for ind in range(len(rows)):
        row_timestamp = self._get_row_timestamp(rows[ind])
        if row_timestamp <= self.last_update_timestamp:
          return ind

  def extract(self) -> Generator[Dict[str, Any], None, None]:
    """Extracts WP data from API using paginated requests.

    Yields:
      (Dict[str, Any]): return input data for transform and load steps.
      The data is the paginated response converted to JSON.
      If the delta loading is used, the data will only contain the records
      not loaded before.
    """
    base_request_url = self.wp_api_endpoint
    if self.delta_loading:
      # add sorting by wp_delta_field_name to the request
      base_request_url += f'&orderby={self.wp_delta_field_name}&order=desc'
    no_new_data_left = False

    total_pages = self._get_page_count(base_request_url, self.CHUNK_SIZE)
    for page_number in range(1, total_pages + 1):
      self.execution_context.log.info(f'Loading page number [{page_number}].')
      request_url = \
        f"{base_request_url}&page={page_number}&per_page={self.CHUNK_SIZE}"
      page_response = self._make_get_request(request_url)
      rows = page_response.json()
      if self.delta_loading:
        # check which rows need to be loaded
        last_new_index = self._get_last_new_row_index(rows)
        rows = rows[:last_new_index]
        no_new_data_left = last_new_index != len(rows)
      if len(rows) != 0:  # yield only not-empty lists
        yield {
          'metadata': {},
          'rows': rows
        }
      # if the index of the last unloaded row in delta mode was not at the end
      # of the page, we have reached the end of unloaded rows.
      if no_new_data_left:
        self.execution_context.log.info(
          f'Reached last loaded timestamp, stopping further loading.')
        break

  def transform(self, data: Dict[str, Any]) -> Any:
    """Converts JSON into a pandas DF and writes it into a GS bucket."""
    rows_df = pd.DataFrame(data['rows'])
    for col in rows_df.columns:
      if isinstance(rows_df[col][0], (dict, list)):
        rows_df[col] = [json.dumps(t) for t in rows_df[col]]

    converted_data_file = pathlib.Path(self.data_path, 'data.parquet')
    rows_df.to_parquet(converted_data_file)
    data['rows'] = [str(converted_data_file)]
    data['metadata']['source_format'] = 'PARQUET'
    return data

  def load(self, data: Dict[str, Any]):
    """Override of load method to account for is_new_table_flag."""
    # create staging table in case if it is the first run of the load method
    # in further ETL run append data to existing staging table.
    # this functionality is important for batch/chunk loads
    self.write_disposition = ('WRITE_TRUNCATE' if self.is_new_table_flag
                              else 'WRITE_APPEND')
    self.is_new_table_flag = False
    super(ExtractorWordpress, self).load(data)
