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

from typing import Tuple

from sql_graph.exceptions import ColumnLookupError
from sql_graph.exceptions import ParsingLookupError
from sql_graph.exceptions import TableLookupError
from sql_graph.typing import TTable


def lookup_source_table_with_star_column(table: TTable) -> str:
  """Returns the name of table with SC if there is one and only one such table.

  Is used in cases where the column was not found among common sources by
  name. In such case, the table with star column (if there is one and only one)
  would be inferred as the source table. If there are multiple such tables,
  there would be more than one possibility for the source table because there
  is no information about what columns are masked by star column,
  so a TableLookupError would be raised.

  Args:
    table (TTable): table to search in.
  Returns:
     str: table name
  """
  sources_with_star_column = [t for t in table.get_sources()
                              if t.columns.star_column.initialized]
  if len(sources_with_star_column) == 1:
    return sources_with_star_column[0].name
  elif len(sources_with_star_column) > 1:
    raise TableLookupError("More than one table with star column")
  else:
    raise TableLookupError("No tables with star column")


def lookup_source_table_by_column(table: TTable, column_name: str) -> str:
  """Returns source column by column name.

  Searches common_sources for a table with the column that has the same name
  as given name, ard returns that column. Raises ColumnLookupError if not
  found.

  Args:
    table (TTable): table to search in.
    column_name (str): name of the column.
  Returns:
    TableColumn: column with the given name.
  """
  for table in table.get_sources():
    for column in table.columns:
      if column_name == column.name:
        return table.name
  else:
    raise ColumnLookupError(column_name)


def lookup_source(table: TTable, source: str) -> Tuple[str, str]:
  """Looks up source by a generic name.

  Tries all the lookup methods above.

  Args:
    table (TTable): table to search in.
    source (int, str): source column name or number.
  Returns:
     Tuple[str, str]: source table and column name.
  """
  if "." in source:
    table_name, column_name = source.rsplit(".", 1)
  else:
    column_name = source
    try:
      table_name = lookup_source_table_by_column(table, source)
    except ParsingLookupError:
      table_name = lookup_source_table_with_star_column(table)
  return table_name, column_name
