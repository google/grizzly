from typing import Tuple

from sql_graph.exceptions import ColumnLookupError
from sql_graph.exceptions import ParsingLookupError
from sql_graph.exceptions import TableLookupError
from sql_graph.typing import TTable


def lookup_external_source_table(table: TTable) -> str:
  """Returns the name external table if there is one and only one such table.

  Is used in cases where the column was not found among common sources by
  name. In such case, the only external table would be inferred as the source
  table. If there are multiple external tables, there would be more than one
  possibility for the source table because there is no information about
  external tables' columns, so a TableLookupError would be raised.

  Args:
    table (TTable): table to search in.
  Returns:
     str: table name
  """
  from sql_graph.parsing.tables import ExternalTable
  external_sources = [t for t in table.get_sources() if 
                      isinstance(t, ExternalTable)]
  if len(external_sources) == 1:
    return external_sources[0].name
  elif len(external_sources) > 1:
    raise TableLookupError("More than one external table")
  else:
    raise TableLookupError("No external tables")


def lookup_single_source_table(table: TTable) -> str:
  """Returns the table name if there is one and only one common source.

  If there is only one table in common_sources, then it is inferred to be the
  source for columns, source of which was not found by any other methods.
  Otherwise, raises TableLookupError.

  Args:
    table (TTable): table to search in.
  Returns:
     str: table name
  """
  if len(table.get_sources()) == 1:
    return table.get_sources()[0].name
  else:
    raise TableLookupError("Number of common sources is not equal one")


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
      try:
        table_name = lookup_external_source_table(table)
      except TableLookupError:
        table_name = lookup_single_source_table(table)
  return table_name, column_name
