"""External table class."""
from sql_graph.parsing.tables import Table
from sql_graph.typing import TTokenizedQuery
from sql_graph.typing import TableLocation


class ExternalTable(Table):
  """A subclass of the Table that represents a table not defined in SQL fies."""

  def _parse(self) -> None:
    """Override of the _parse method to skip parsing"""
    pass

  def __init__(self, name: str, location: TableLocation,
               query: TTokenizedQuery) -> None:
    super(ExternalTable, self).__init__(name, table_info=None,
                                        location=location, query=query)
    # because nothing is known about external table, it will have a star column
    self.columns.star_column.initialized = True

  def __repr__(self):
    """Representation of the class in print for debug purposes."""
    return f"<External Table '{self.name}'>"
