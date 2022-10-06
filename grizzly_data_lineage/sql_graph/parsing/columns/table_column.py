from sql_graph.parsing.columns import Column
from sql_graph.typing import TColumn
from sql_graph.typing import TTable
from sql_graph.typing import TokenizedJson


class TableColumn(Column):
  """Representation of regular column located in a table.

  Contains additional parsing logic in init method, as well as copy
    functionality. Also has a value attribute.

  Attributes:
    value (TokenizedJson): value of the column.
  """

  def _parse(self) -> None:
    """Override of _parse method."""
    self._parse_value_json(self.value)
    if self.value is None:
      self._parse_value_json(self.name)
      self.name = self.name.split(".")[-1]

  def __init__(self, name: str, value: TokenizedJson, table: TTable) -> None:
    super(TableColumn, self).__init__(name, table)
    self.value = value
    self._parse()

  def _get_serializing_id(self) -> str:
    """Overwrite of _get_serializing_id method."""
    return f"{self._serializing_params.get_parent_id()}.{self.name}"

  def copy(self, new_table: TTable) -> TColumn:
    """Copy column for another table.

    Replaces the table attribute and adds self as the source. This method is
    used in case of * syntax.

    Args:
      new_table (Table): new table that will contain new column object.

    Returns:
      Column: new column.
    """
    # new value will be parsed by new column, which will add this column
    # as a source
    new_value = f"{self.table.name}.{self.name}"
    c = TableColumn(name=self.name, value=new_value, table=new_table)
    return c
