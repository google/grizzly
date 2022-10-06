"""Star column class."""
from sql_graph.parsing.columns import TableColumn
from sql_graph.typing import TColumn
from sql_graph.typing import TTable


class StarColumn(TableColumn):
  """A special subclass of TableColumn that represents unknown columns.

  Will be present in all external tables and in some tables with SELECT *
    statement (i.e. when there is some uncertainty about table's contents).

  Attributes:
    initialized (bool): whether the star column was initialized by the
      ColumnContainer. Will determine the needs_serializing property.
  """

  def add_source(self, source: TColumn) -> None:
    """Override of add_source. Keeps track of the initialization status."""
    super(StarColumn, self).add_source(source)
    self.initialized = True

  def remove_source(self, source: TColumn) -> None:
    """Override of remove_source. Keeps track of the initialization status."""
    super(StarColumn, self).remove_source(source)
    self.initialized = len(self._sources) != 0

  def _parse(self) -> None:
    """Overwrite of parsing method to disable parsing."""
    pass

  def __init__(self, table: TTable) -> None:
    super(StarColumn, self).__init__(name="*", value=None, table=table)
    self.initialized = False

  def _get_serializing_id(self) -> str:
    """Override of _get_serializing_id method."""
    return f"{self._serializing_params.get_parent_id()}__*"

  @property
  def needs_serializing(self) -> bool:
    """Override of needs_serializing."""
    return self.initialized
