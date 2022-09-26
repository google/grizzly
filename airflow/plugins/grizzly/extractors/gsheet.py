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
"""Implementation of GSheet data extract.

ExtractorGSheet is inherited from
grizzly.extractors.base_extractor.BaseExtractor
Instance of this class created and used by grizzly.etl_factory.ETLFactory
For more insights check implementation of base_extractor and etl_factory.
"""

import datetime
import json
import re
import time
from typing import Any, Dict, List, Optional, Generator

from airflow.exceptions import AirflowException
from airflow.models import Variable
from googleapiclient.discovery import build
import grizzly.etl_action
from grizzly.extractors.base_extractor import BaseExtractor
from grizzly.grizzly_typing import TGrizzlyOperator
from grizzly.grizzly_typing import TGrizzlyTaskConfig
import numpy as np
import pandas as pd


class ExtractorGSheet(BaseExtractor):
  """Implementation of ETL for GSheet to BQ data loading.

  Parse spreadsheet_id in format '<gsheet_id>[/<RANGE_NAME>]'.
  If <RANGE_NAME> is not defined use target_table_name as a range
  Examples:
  1. 1nO3T-tYoKm38rCBQhGb_gsX5R2_HfcsTjYfBRYH-ofU/some_other
  2. '1nO3T-tYoKm38rCBQhGb_gsX5R2_HfcsTjYfBRYH-ofU/Job'!A1:B2
  3. 1nO3T-tYoKm38rCBQhGb_gsX5R2_HfcsTjYfBRYH-ofU
  Reference to GSheet should be defined in [source_gsheet_id] attribute of task
  YML file
    source_type: gsheet
    source_gsheet_id: 1FBE000067ZoOXeat4/'vn_calendar_hierarchy'
  or task YML file should define [source_config] with a name of Airflow variable
    source_type: gsheet
    source_config: source_config.common_parameters_kis

  Attributes:
    task_config (TGrizzlyTaskConfig): Task configuration with
        parsed and pre-proccessed information from task YML file.
    target_table (string): Name of a table where GSheet data should be stored.
    execution_context (TGrizzlyOperator): Instance of GrizzlyOperator executed.
    write_disposition (string): BQ write disposition WRITE_APPEND, WRITE_EMPTY,
      WRITE_TRUNCATE in case if etl_factory use ExtractorGSheet for staging
      table it will be WRITE_TRUNCATE. If it executed for table defined in
      [target_table_name] attribute of task YML file this class attribute will
      be  equal to [job_write_mode] attribute of task YML file.
    job_stat (QueryJob): Job execution statistics.
    is_new_table_flag (bool): Flag that define should we create a new table or
      append data to existing. If [write_disposition] is WRITE_APPEND then flag
      inited as False else as True.  After upload of first chunk of data it
      changed to False.
    gsheet_id (string): GSheet Id
    range_name (string): Gsheet range name. Could be defined as sheeet(page)
      name or as cells range in GSheet supported format for example
      <<Job!A1:B2>>.
    sheet (spreadsheets): Client for work with Google spreadsheets API.
  """

  def __init__(self,
               execution_context: Optional[TGrizzlyOperator] = None,
               task_config: Optional[TGrizzlyTaskConfig] = None,
               target_table: Optional[str] = None,
               write_disposition: Optional[str] = None,
               *args: Any,
               **kwargs: Any) -> None:
    """Initialize instance of ExtractorGSheet.

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
    self.target_table_parsed = grizzly.etl_action.parse_table(
        task_config.target_table_name)
    self.is_new_table_flag = (False if write_disposition == 'WRITE_APPEND'
                              else True)
    # if source_gsheet_id was defined in YML file
    if task_config.source_gsheet_id:
      spreadsheet_id = task_config.source_gsheet_id
    # gsheet was not configured in yml file check for
    # environment specific configuration
    elif task_config.source_config:
      source_config = json.loads(Variable.get(task_config.source_config))
      spreadsheet_id = source_config['source_gsheet_id']
    else:
      raise AirflowException(
          ('GSheet was not configured. Check '
           '[source_config or source_gsheet_id] parameter in your YML file.')
      )

    self.gsheet_id = spreadsheet_id
    self.range_name = None
    # parse format '<gsheet_id>[/<RANGE_NAME>]'
    if '/' in spreadsheet_id:
      self.gsheet_id, self.range_name = spreadsheet_id.split('/', 1)

    # if RANGE_NAME was not defined use target_table_name
    if not self.range_name:
      self.range_name = self.target_table_parsed['table_id']
    execution_context.log.info(
        (f'Extracting data from GSHEET_ID [{self.gsheet_id}], '
         f'RANGE_NAME [{self.range_name}].')
    )
    # Call the Sheets API
    self.sheet = build('sheets', 'v4', cache_discovery=False).spreadsheets()

  def __extract_row_value(self,
                          x: Dict[str, Any],
                          force_string: str = False) -> Dict[str, Any]:
    """Extract data from gsheet json response."""
    try:
      if 'stringValue' in x.get('effectiveValue', {}):
        val = x['effectiveValue']['stringValue']
      else:
        val = x.get('effectiveValue', {}).get('numberValue', None)

      cast_datetime = lambda arg: (
          datetime.datetime(1899, 12, 30) + datetime.timedelta(arg))
      cast_date = lambda arg: arg.strip() if 'stringValue' in x.get(
          'effectiveValue', {}) else cast_datetime(arg).date()
      defined_cell_format = x.get('userEnteredFormat',
                                  {}).get('numberFormat', {}).get('type', '')
      if x == {}:
        result = {'Type': None, 'Value': np.NaN}
      elif force_string:
        result = {
            'Type': 'STRING',
            'Value': np.NaN if val is None else x.get('formattedValue', np.NaN)
        }
      elif 'stringValue' in x.get('effectiveValue', {}):
        result = {'Type': 'STRING', 'Value': np.NaN if val is None else val}
      elif defined_cell_format == 'TEXT':
        result = {
            'Type':
                'STRING',
            'Value':
                x.get('effectiveValue', {}).get('stringValue',
                                                x.get('formattedValue', None))
        }
      elif defined_cell_format == 'DATE' and 'formattedValue' in x:
        result = {
            'Type': 'DATE',
            'Value': np.NaN if val is None else str(cast_date(val))
        }
      elif defined_cell_format == 'DATE_TIME' and 'formattedValue' in x:
        result = {
            'Type': 'TIMESTAMP',
            'Value': np.NaN if val is None else str(cast_datetime(val))
        }
      elif ('numberValue' in x.get('effectiveValue', {})
            and 'formattedValue' in x):
        result = {
            'Type': 'FLOAT64' if isinstance(val, float) else 'INT64',
            'Value': np.NaN if val is None else str(val)
        }
      elif 'formattedValue' in x:
        result = {'Type': 'STRING', 'Value': x['formattedValue']}
      else:
        result = {'Type': None, 'Value': np.NaN}
    except Exception as ex:
      self.execution_context.log.info(f'Parsed value: {x}')
      raise ex
    return result

  def __generate_sql_field(self,
                           value: Any,
                           metadata: Dict[str, str]) -> str:
    """Generate CAST string.

    Generate CAST string for transformation of GSheet cell data into BQ
    format for use in SELECT statement.

    Args:
      value (Any): Gsheet value.
      metadata (dict): Information about preffered BQ datatype and column name.

    Returns:
      (str): String with BQ CAST.
    """
    vfs = value
    # very ugly check
    if str(value) == 'nan':
      vfs = None
    result = 'CAST({value} AS {type}) AS `{name}`'.format(
        value=grizzly.etl_action.prepare_value_for_sql(vfs),
        type=metadata['type'],
        name=metadata['name']
    )
    return result

  def __get_header(self,
                   raw_header: List[Any]) -> List[Dict[str, Optional[str]]]:
    """Get table header from GSheet.

    Table header extracted from a 1st row in GSheet range.
    Method able to perform BQ column name maping to GSheet header.
    Column mapping could be defined in attribute [source_columns] of task YML
    file.

    Args:
      raw_header (List[Any]): 1st row in GSheet range contains definition of
        table headers

    Raises:
      AirflowException: YML Configuration contains column that does not exit in
        Gsheet

    Returns:
      List[Dict[str, Optional[str]]]: Table header.
    """
    column_ids = []
    # List used for duplicate counting. Columns before rename of duplicate
    columns_dup = []
    # List of raw column names.
    columns = []
    # Parse column names from raw data
    for column_id, column_name_raw in enumerate(raw_header):
      # Work only with columns with name defined
      if 'formattedValue' in column_name_raw:
        # replace all non alpha_numeric characters in column name with ''
        column_name_parsed = column_name_raw['formattedValue'].strip()
        column_name_adjusted = re.sub(r'\W', '', column_name_parsed)
        # Get a count of columns with a same name already proccessed.
        dub_count = columns_dup.count(column_name_adjusted.lower())
        # Put column name before duplicate naming resolved
        # Cast to lower as BQ column names are case insensetive
        columns_dup.append(column_name_adjusted.lower())
        if dub_count:
          # if column has duplicates in spreadsheet
          # remove duplicate by add number to the end of column name
          column_name_adjusted = f'{column_name_adjusted}{dub_count}'
          column_name_parsed = f'{column_name_parsed}.{dub_count}'
        # Column name to column id mapping
        column_ids.append((column_id, column_name_adjusted))
        columns.append(column_name_parsed)
    # get a list of column from config
    config_column_list = self.task_config.source_columns
    if config_column_list:  # only if list of columns define in config
      config_column_name_list = [c['source_name'] for c in config_column_list]
      # if column list defined raise error if column does not exist in a trix
      for ccn in config_column_name_list:
        if ccn not in columns:
          raise AirflowException(
              (f'Configuration contains column [{ccn}] that '
               'does not exit in Gsheet')
          )
      df_config = pd.DataFrame(config_column_list)
      df_sheet = pd.DataFrame([{
          'source_name': ci[1],
          'column_id': ci[0]
      } for ci in column_ids])
      # if columns defined. Exclude all not in list. LEFT JOIN
      df_merged = pd.merge(df_config, df_sheet, on='source_name', how='left')
      # if columns defined. Rename in case of importance
      if 'target_name' not in df_merged:
        df_merged['target_name'] = df_merged['source_name']
      else:
        df_merged['target_name'] = df_merged['target_name'].fillna(
            df_merged['source_name'])
      # replace all non alpha_numeric characters in column names with ''
      column_ids = [{
          'column_id': x['column_id'],
          'name': re.sub(r'\W', '', x['target_name']),
          'type': 'STRING' if x.get('force_string', '') == 'Y' else None
      } for x in df_merged.T.to_dict().values()]
    else:
      column_ids = [
          {
              'column_id': x[0],
              'name': x[1],
              'type': None
          }
          for x in column_ids
      ]
    return column_ids

  def extract(self) -> Generator[Dict[str, Any], None, None]:
    """Extract information from GSheet.

    Extract information from GSheet and yield chunks of data for further
    uploading into target BQ table.
    Chunk side is calculated dinamically on a base of size of data extracted
    from GSheet.
    extract method applies new column names on a base of configuration in
    ['source_columns'] attribute of task YML file.
    Empty rows and empty columns are ignored.
    Columns with duplicated names are renamed.
    Method automatically defines type of data on a base of information from
    GSheet.
    If different cells in a column have different types then data inserted in BQ
    target table as a string.

    Yields:
      (Dict[str, Any]): Dictionary with table structure and chunk of data. Data
        used for further transformation and load purposes.
    """

    attempt_number = 5
    time_delay = 10  # wait 10 seconds in case of failure
    while True:
      try:
        trix_data = self.sheet.get(
            spreadsheetId=self.gsheet_id,
            ranges=[self.range_name],
            fields=('sheets(data(rowData(values(userEnteredFormat/numberFormat,'
                    'userEnteredValue,effectiveValue,formattedValue)),'
                    'startColumn,startRow))')
        ).execute()
        break
      except:
        if attempt_number == 0:
          raise AirflowException('Could not extract raw data from GSheet.')
        else:
          attempt_number -= 1
          time.sleep(time_delay)
          continue

    # use rowdata from 1st range in resultset returned
    trix_data = trix_data['sheets'][0]['data'][0]['rowData']

    if len(trix_data) < 2:
      raise AirflowException('No data available on spreadsheet.')
    # get header from 1st row. Columns without names are excluded
    sheet_headers = self.__get_header(trix_data[0]['values'])
    force_string_columns = [
        h['name'] for h in sheet_headers if h['type'] == 'STRING'
    ]
    # get data starting from 2nd row
    frame_data = {c['name']: [] for c in sheet_headers}
    for data_row in trix_data[1:]:
      if 'values' not in data_row:
        continue
      row_len = len(data_row['values'])
      for si, header in enumerate(sheet_headers):
        c_id, c_name = header['column_id'], header['name']
        force_string = c_name in force_string_columns
        cell_value = self.__extract_row_value(
            data_row['values'][c_id] if c_id < row_len else {}, force_string)
        frame_data[c_name].append(cell_value['Value'])
        # Try to define type of data
        if sheet_headers[si]['type'] is None:
          sheet_headers[si]['type'] = cell_value['Type']
        elif (sheet_headers[si]['type'] == 'INT64' and
              cell_value['Type'] == 'FLOAT64'):
          sheet_headers[si]['type'] = 'FLOAT64'  # change it to float
        elif (sheet_headers[si]['type'] == 'FLOAT64' and
              cell_value['Type'] == 'INT64'):
          pass  # don't change float to int
        elif (sheet_headers[si]['type'] != cell_value['Type'] and
              cell_value['Type'] is not None):
          sheet_headers[si]['type'] = 'STRING'
    # create dataframe with trix data
    df = pd.DataFrame(frame_data)
    # define all columns without type as STRING
    for i in range(len(sheet_headers)):
      if sheet_headers[i]['type'] is None:
        sheet_headers[i]['type'] = 'STRING'
      # clean up id that are not used anymore
      del sheet_headers[i]['column_id']

    # remove empty rows from df
    df.dropna(how='all', inplace=True)

    self.total_rows = len(df)
    # in case of empty resultset just create table
    if self.total_rows == 0:
      # remove staging table then recreate it
      target_table_dic = grizzly.etl_action.parse_table(self.target_table)
      target_project_id = target_table_dic['project_id']
      target_ds_id = target_table_dic['dataset_id']
      target_table_id = target_table_dic['table_id']
      self.execution_context.bq_cursor.run_table_delete(
          deletion_dataset_table='{}.{}.{}'.format(target_project_id,
                                                   target_ds_id,
                                                   target_table_id),
          ignore_if_missing=True)
      self.execution_context.bq_cursor.create_empty_table(
          project_id=target_project_id,
          dataset_id=target_ds_id,
          table_id=target_table_id,
          schema_fields=sheet_headers)
    else:

      # prepare SQL
      field_count = len(sheet_headers)
      row_to_string_func = lambda x: 'STRUCT(' + ', '.join([
          self.__generate_sql_field(
              (x[i] if len(x[i:]) != 0 else None),
              sheet_headers[i])
          for i in range(field_count)
      ]) + ')'
      df['SQL_QUERY'] = df.apply(
          lambda x: row_to_string_func(x.astype(str)), axis=1)
      # div by 1Mb chunks
      df['SQL_LEN_CUMSUM'] = df['SQL_QUERY'].apply(len).cumsum() // 1000000
      min_chunk = int(df['SQL_LEN_CUMSUM'].min())
      max_chunk = int(df['SQL_LEN_CUMSUM'].max())

      self.current_load = 0
      for i in range(min_chunk, max_chunk + 1):
        rows = df.loc[df['SQL_LEN_CUMSUM'] == i, ['SQL_QUERY']].values.tolist()

        self.current_load += len(rows)
        yield {
            'metadata': sheet_headers,
            'rows': rows,
            'row_counter': self.current_load
        }

  def load(self, data: Any) -> None:
    """Upload GSheet data into BQ table.

    Parse input data. Transform it into SELECT query and write query result into
    target table. Loading of all chunks of data starting from 2nd one will
    append information to target table.

    Args:
      data (list[dict]): Prepared values for insertion into target table.
        String value like in format like
        <<STRUCT("Rudisha" as name, cast('12' as INT64) as val),
        STRUCT("Makhloufi" as name, cast('20' as INT64) as val)>>
    """
    rows_generator = (x[0] for x in data['rows'])
    rows = ',\n'.join(rows_generator)
    insert_sql = f'SELECT * FROM UNNEST([{rows}]) AS v'

    # create staging table in case if it first run of load method
    # in further ETL run append data to existing staging table.
    # this functionality is importante for batch/chunk loads
    write_disposition = ('WRITE_TRUNCATE' if self.is_new_table_flag
                         else 'WRITE_APPEND')
    self.is_new_table_flag &= False

    self.job_stat = grizzly.etl_action.run_bq_query(
        execution_context=self.execution_context,
        sql=insert_sql,
        destination_table=self.target_table,
        write_disposition=write_disposition)

    self.execution_context.log.info(
        f'!!! Loaded [{data["row_counter"]}] of [{self.total_rows}].')
