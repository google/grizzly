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

"""Implementation of BigQuery extract with DLP transformation.

ExtractorBQDlp is inherited from grizzly.extractors.base_extractor.BaseExtractor
Instance of this class created and used by grizzly.etl_factory.ETLFactory
For more insights check implementation of base_extractor and etl_factory.
"""


from typing import Any, Dict, Generator, Optional
from google.cloud import bigquery
from google.cloud.bigquery.job import QueryJob
import google.cloud.dlp
from google.cloud.dlp_v2.types import ContentItem
from grizzly.config import Config
import grizzly.etl_action
from grizzly.extractors.base_extractor import BaseExtractor
from grizzly.grizzly_typing import TGrizzlyOperator
from grizzly.grizzly_typing import TGrizzlyTableParsed
from grizzly.grizzly_typing import TGrizzlyTaskConfig


class QueryJobDLP:
  """Custom implementation of QueryJob DLP statistics.

  Attributes:
    query_job (QueryJob): BQ query job statistic.
    total_bytes_processed_acc (int): Bytes processed.
    total_bytes_billed_acc (int): Bytes billed.
  """

  def __init__(self, query_job: QueryJob) -> None:
    """Initialize instance of QueryJobDLP."""
    self.query_job = query_job
    self.total_bytes_processed_acc = 0
    self.total_bytes_billed_acc = 0

  def __getattr__(self, key: str) -> Any:
    """Get attribute of query_job by name."""
    return getattr(self.query_job, key)

  @property
  def total_bytes_processed(self) -> int:
    """Return total bytes processed."""
    res = self.query_job.total_bytes_processed

    if not res:
      res = 0

    res += self.total_bytes_processed_acc

    return res

  @property
  def total_bytes_billed(self) -> int:
    """Return total bytes billed."""

    res = self.query_job.total_bytes_billed

    if not res:
      res = 0

    res += self.total_bytes_billed_acc

    return res


class ExtractorBQDlp(BaseExtractor):
  """Implementation of ETL for BQ to BQ data loading with DLP transformation.

  Important notice. Data is streamed in target table. BigQuery data streaming
  requires up to 90 minutes for flush of streaming buffer. If you have
  WRITE_TRUNCATE configuration it could fail in case of frequent loading.
  As if you have unflushed buffer table fail to recreate.

  Attributes:
    task_config (TGrizzlyTaskConfig): Task configuration with
        parsed and pre-proccessed information from task YML file.
    target_table (string): Name of a table where query execution and DLP
      transformation results should be stored.
    execution_context (TGrizzlyOperator): Instance of GrizzlyOperator executed.
    write_disposition (string): BQ write disposition WRITE_APPEND, WRITE_EMPTY,
      WRITE_TRUNCATE. In case if ETL executed for staging table it will be
      WRITE_TRUNCATE. If ETL executed for table defined in [target_table_name]
      attribute of task YML file this class attribute will be equal to
      [job_write_mode] attribute of task YML file.
    job_stat (QueryJob): Job execution statistics for data extraction query
      defined in [stage_loading_query] attribute of task YML file.
    is_new_table_flag (bool): Flag that define should we create a new table or
      append data to existing. If [write_disposition] is WRITE_APPEND then flag
      inited as False else as True.  After upload of first chunk of data it
      changed to False.
    target_table_parsed (dict): Dictionary with parsed target table. Target
      table name is splitted on project_id, dataset and table name parts.
    dlp_client (google.cloud.dlp_v2.DlpServiceClient): DlpServiceClient instance
      for work with DLP API.
    dlp_config (dict): DLP configuration from [dlp_config] attrributee of task
      YML file.
    inspect_template_name (string): Reference to DLP inspect template to be used
      Example:
      projects/gcp-pipelines-prototype/locations/us/inspectTemplates/test
    deidentify_template_name (string): Reference to DLP deidentify template to
      be used.
      Example:
      projects/gcp-pipelines-prototype/locations/us/deidentifyTemplates/test
    headers (list(dict)): List of table headers. This list is used by DLP
    query_schema (dict): BigQuery table schema for query resultset.
    total_bytes_billed (int): Bytes billed.
    total_bytes_processed (int): Bytes proccessed.
  """

  def __init__(self,
               execution_context: Optional[TGrizzlyOperator] = None,
               task_config: Optional[TGrizzlyTaskConfig] = None,
               target_table: Optional[str] = None,
               write_disposition: Optional[str] = None,
               *args: Any,
               **kwargs: Any) -> None:
    """Initialize instance of ExtractorBQDlp.

    Perform initial configuration and setup DLP client.

    Args:
      execution_context (GrizzlyOperator): Instance of GrizzlyOperator executed.
      task_config (TGrizzlyTaskConfig): Task configuration with parsed and
        pre-proccessed information from task YML file.
      target_table (string): Name of a table where query execution and DLP
        transformation results should be stored.
      write_disposition (string): BQ write disposition WRITE_APPEND, WRITE_EMPTY
        WRITE_TRUNCATE. In case if etl_factory use EtractorBQ for staging table
        it will be WRITE_TRUNCATE. If it executed for table defined in
        [target_table_name] attribute of task YML file this class attribute will
        be  equal to [job_write_mode] attribute of task YML file.
      *args (list, optional): Optional aditional parameters.
      **kwargs (dict, optional): Optional aditional parameters.
    """
    super().__init__(execution_context, task_config, target_table,
                     write_disposition, *args, **kwargs)
    self.is_new_table_flag = (False if write_disposition == 'WRITE_APPEND'
                              else True)
    self.target_table_parsed = grizzly.etl_action.parse_table(
        task_config.target_table_name)
    self.dlp_client = google.cloud.dlp_v2.DlpServiceClient()
    self.dlp_config = self.task_config._raw_config['dlp_config']
    self.inspect_template_name = self.dlp_config.get('inspect_template_name',
                                                     None)
    self.inspect_template_name = self.inspect_template_name.replace(
        '{{ task_instance.gcp_project_id }}', Config.GCP_PROJECT_ID)
    self.deidentify_template_name = self.dlp_config.get(
        'deidentify_template_name', None)
    self.deidentify_template_name = self.deidentify_template_name.replace(
        '{{ task_instance.gcp_project_id }}', Config.GCP_PROJECT_ID)
    self.headers = []

    self.total_bytes_billed = 0
    self.total_bytes_processed = 0

  def _create_target_table(self,
                           target_table_parsed: TGrizzlyTableParsed,
                           schema: Any) -> None:
    """Create empty target table."""
    table_id = '{project_id}.{dataset_id}.{table_id}'.format(
        project_id=target_table_parsed['project_id'],
        dataset_id=target_table_parsed['dataset_id'],
        table_id=target_table_parsed['table_id']
    )
    self.execution_context.bq_cursor.run_table_delete(
        deletion_dataset_table=table_id, ignore_if_missing=True)
    self.execution_context.bq_client.create_table(
        table=bigquery.Table(table_id, schema=schema))

  def extract(self) -> Generator[Dict[str, Any], None, None]:
    """Extract source data.

    Execute source query defined in [stage_loading_query] attribute of task YML
    file. Page size is equal to [dlp_config.batch_size] attribute of task YML
    file.
    self.job_stat is used for extract table schema.
    Iterate pages in query result and yield chunks of data.
    If no data available just create empty table.

    Yields:
      (dict): Dictionary with table structure and chunk of data. Data used for
        further transformation and load purposes.
    """
    # Run query and keep resultset in BQ temporary table
    job_stat = grizzly.etl_action.run_bq_query(
        execution_context=self.execution_context,
        sql=self.task_config.stage_loading_query,
        use_legacy_sql=self.task_config.is_legacy_sql)

    self.job_stat = QueryJobDLP(job_stat)

    batch_size = self.dlp_config['batch_size']
    query_resultset = self.job_stat.result(page_size=batch_size)
    self.query_schema = query_resultset.schema
    # Define header list for DLP
    self.headers = [{'name': field.name} for field in self.query_schema]
    has_some_data = False
    for i, page_rows in enumerate(query_resultset.pages):
      has_some_data = True
      self.execution_context.log.info(f'Processing page number [{i}].')
      # transform page row objects to list of rows
      rows = [list(r) for r in page_rows]
      yield {'metadata': self.query_schema, 'rows': rows}

    # if no data available just create empty table
    if not has_some_data and self.is_new_table_flag:
      self._create_target_table(self.target_table_parsed, self.query_schema)
      self.is_new_table_flag &= False
      yield {'metadata': self.query_schema, 'rows': []}

  def transform(self, data: Dict[str, Any]) -> Any:
    """Apply DLP transformation of data.

    Args:
      data (dict): Chunk of query resultset data for further DLP transformation.

    Returns:
      (self.dlp_client.deidentify_content): Data after DLP deidentification.
    """
    # Prepare structured content to inspect
    # Need to use copy as ContentItem could update list by reference
    data_rows = data['rows'][:]

    rows = []

    for row in data_rows:
      values = [{'string_value':
                     str(col) if str(col) != 'None' else ''} for col in row]
      rows.append({'values': values})

    dlp_table = {'headers': self.headers[:], 'rows': rows}

    dlp_content_item = ContentItem(table=dlp_table)
    response = self.dlp_client.deidentify_content(
        parent=f'projects/{Config.GCP_PROJECT_ID}',
        deidentify_config=None,
        inspect_config=None,
        deidentify_template_name=self.deidentify_template_name,
        inspect_template_name=self.inspect_template_name,
        item=dlp_content_item)
    return response

  def load(self, data: Any) -> None:
    """Upload chunks of data into BQ table.

    Deidentified data streamed into target table.

    Args:
      data: Data after DLP deidentification.
    """
    # create table in case if it's a first run of load method
    # in further ETL run append data to existing staging table.
    # this functionality is required for batch/chunk uploading
    if self.is_new_table_flag:
      self._create_target_table(self.target_table_parsed, self.query_schema)
      self.is_new_table_flag &= False

    rows = []
    for row in data.item.table.rows:
      res_values = row.values
      inserted_row = {
          self.headers[i]['name']: v.string_value
          for i, v in enumerate(res_values)
      }
      rows.append(inserted_row)

    self.total_bytes_billed += data.overview.transformed_bytes
    self.total_bytes_processed += data.overview.transformed_bytes

    self.job_stat.total_bytes_billed_acc = self.total_bytes_billed
    self.job_stat.total_bytes_processed_acc = self.total_bytes_processed

    dataset_ref = bigquery.DatasetReference(
        self.target_table_parsed['project_id'],
        self.target_table_parsed['dataset_id'])
    table_ref = dataset_ref.table(self.target_table_parsed['table_id'])
    self.execution_context.bq_client.insert_rows_json(table_ref, rows)
