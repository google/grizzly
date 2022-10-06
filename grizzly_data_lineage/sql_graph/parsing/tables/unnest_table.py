"""Unnest table class."""
from typing import Optional

from sql_graph.parsing.columns import TableColumn
from sql_graph.parsing.tables import Table
from sql_graph.typing import TTokenizedQuery
from sql_graph.typing import TableLocation
from sql_graph.typing import TokenizedJson


class UnnestTable(Table):
  """A subclass of the Table that represents unnest statement.

  Attributes:
    unnest_name (str): name of the unnest that can differ from table name.
      It could be used to name one of the unnest's columns.
  """

  # noinspection PyTypeChecker
  def _parse(self) -> None:
    """Override of the _parse method to parse unnest."""
    if "create_array" in self.table_info:
      if "create_struct" in self.table_info["create_array"]:
        for column in self.table_info["create_array"]["create_struct"]:
          c = TableColumn(name=column["name"], value=None, table=self)
          self.columns.add_column(c)
    if len(self.columns) == 0:
      if isinstance(self.table_info, str):
        column_name = self.table_info
      else:
        column_name = self.unnest_name
      c = TableColumn(name=column_name, value=None, table=self)
      self.columns.add_column(c)

  def __init__(self, name: str, table_info: TokenizedJson,
               unnest_name: Optional[str], location: TableLocation,
               query: TTokenizedQuery) -> None:
    self.unnest_name = unnest_name if unnest_name is not None else name
    super(UnnestTable, self).__init__(name, table_info["unnest"], location,
                                      query)

  def __repr__(self):
    """Representation of the class in print for debug purposes."""
    return f"<Unnest Table '{self.name}'>"

  def _get_label(self) -> str:
    """Override of the _get_label method."""
    return "Unnest"
