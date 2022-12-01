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

"""Implementation of BigQuery extract class.

ExtractorExcel is inherited from
grizzly.extractors.base_url_extractor.BaseURLExtractor
Instance of this class created and used by grizzly.etl_factory.ETLFactory
For more insights check implementation of base_extractor and etl_factory.
"""

import pathlib
import re
from typing import Any, Dict, List, Optional, Union

from airflow.exceptions import AirflowException
from grizzly.extractors.base_url_extractor import BaseURLExtractor
from grizzly.grizzly_typing import TGrizzlyOperator
from grizzly.grizzly_typing import TGrizzlyTaskConfig
import pandas as pd


class ExtractorExcel(BaseURLExtractor):
  """Implementation of Excel to BQ data loading.

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
    job_stat (QueryJob): Job execution statistics.
    sheet_name (Union[str, int]): Name of Excel sheet. The 1st sheet will be
      used if not defined in self.task_config.source_data_range
    header_row (int): Row (0-indexed) to use for the column labels
      of the parsed DataFrame.
    rows_to_load (Optional[int]): Number of rows to be loaded. Null = All
    usecols (Union[str, List[str], None]): Columns to be loaded.
      Could be defined as string Excel range or as a list of column names.
  """

  def __init__(self,
               execution_context: Optional[TGrizzlyOperator] = None,
               task_config: Optional[TGrizzlyTaskConfig] = None,
               target_table: Optional[str] = None,
               write_disposition: Optional[str] = None,
               *args: Any,
               **kwargs: Any) -> None:
    """Initialize instance of ExtractorExcel.

    Perform initial configuration and setup GSheet client.

    Args:
      execution_context (TGrizzlyOperator): Instance of GrizzlyOperator
        executed.
      task_config (TGrizzlyTaskConfig): Task configuration with
        parsed and pre-processed information from task YML file.
      target_table (string): Name of a table where GSheet data should be stored.
      write_disposition (string): BQ write disposition WRITE_APPEND, WRITE_EMPTY
        WRITE_TRUNCATE. In case if etl_factory use ExtractorBQ for staging table
        it will be WRITE_TRUNCATE. If it executed for table defined in
        [target_table_name] attribute of task YML file this class attribute will
        be  equal to [job_write_mode] attribute of task YML file.
      *args (list): Optional attributes.
      **kwargs (dict): Optional attributes.

    Raises:
      AirflowException: Raise exception in case if GSheet ID reference was not
        defined in task YML file.
    """
    super().__init__(execution_context, task_config, target_table,
                     write_disposition, *args, **kwargs)
    range_name: str = self.task_config.source_data_range
    # Defaults to 0: 1st sheet as a DataFrame
    self.sheet_name: Union[str, int] = 0
    # Row (0-indexed) to use for the column labels
    self.header_row: int = 0
    # Number of rows to parse. Default All
    self.rows_to_load: Optional[int] = None
    # Columns to be parsed. Default All
    self.usecols: Union[str, List[str], None] = None

    if range_name and ':' in range_name:  # If data range was defined
      try:
        # Get Sheet name and data range
        range_dict = re.match(
            r"""^((?P<sheet_name>('.+'))\!?)?
                   ((?P<start_column>[A-Za-z]+)   # Range start column
                   (?P<start_row>[0-9]*):         # Range start row
                   (?P<end_column>[A-Za-z]+)      # Range end column
                   (?P<end_row>[0-9]*))?          # Range end row
            """,
            range_name,
            re.VERBOSE).groupdict()
        if not range_dict['sheet_name'] and not range_dict['start_column']:
          raise AirflowException(
              'You should provide sheet name or range in [source_data_range].')
        if range_dict['sheet_name']:
          self.sheet_name = range_dict['sheet_name'].strip("'")
        if range_dict['start_row']:
          self.header_row = int(range_dict['start_row']) - 1
        else:
          self.header_row = 0
        if range_dict['end_row']:
          self.rows_to_load = int(range_dict['end_row']) - self.header_row + 1
        if range_dict['start_column']:
          self.usecols = (f'{range_dict["start_column"]}:'
                          f'{range_dict["end_column"]}')
      except:
        raise AirflowException(
            (f'Could not parse parameter source_data_range [{range_name}]. '
             "Supported formats ['sheet'!A1:D3 , 'sheet' , A1:D5]")
        ) from None
    elif range_name:  # sheet defined without range
      self.sheet_name = range_name.strip("'")
    # Column mapping could be defined in attribute [source_columns] of task YML
    # file.
    if self.task_config.source_columns:
      self.usecols = [c['source_name'] for c in self.task_config.source_columns]

  def transform(self, data: Dict[str, Any]) -> Dict[str, Any]:
    """Transform Excel file into CSV for further loading.

    Method reads Excel files and transform it into CSV format.

    Args:
      data (Dict[str, Any]): Dictionary yielded by extract method

    Returns:
      (Dict[str, Any]): Transformed data.
    """
    self.execution_context.log.info(
        (f'Read XLS file [{self.data_url}]. '
         f'Defined range [{self.task_config.source_data_range}], '
         f'header_row = [{self.header_row}], '
         f'rows_to_load = [{self.rows_to_load}], '
         f'usecols = [{self.usecols}] '
        )
    )
    type_cast_dict = dict()
    column_name_dict = dict()
    type_cast_dict = {
        c['source_name']: str
        for c in self.task_config.source_columns
        if c.get('force_string', '') == 'Y'
    }
    df = pd.read_excel(
        io=data['rows'][0],
        sheet_name=self.sheet_name,
        header=self.header_row,
        nrows=self.rows_to_load,
        usecols=self.usecols,
        dtype=type_cast_dict
    )
    # bulk column rename
    # replace all non alpha_numeric characters in column name with ''
    column_name_dict = {
        col: re.sub(r'\W', '_', col)
        for col in df.columns
    }
    # substitute column names with values from task config
    column_name_dict.update(
        {
            col['source_name']: col['target_name']
            for col in self.task_config.source_columns
            if 'target_name' in col
        }
    )
    # Rename columns
    df.rename(columns=column_name_dict,
              inplace=True)

    converted_data_file = pathlib.Path(self.data_path, 'data.parquet')
    df.to_parquet(converted_data_file)
    data['rows'][0] = str(converted_data_file)
    data['metadata']['source_format'] = 'PARQUET'
    return data
