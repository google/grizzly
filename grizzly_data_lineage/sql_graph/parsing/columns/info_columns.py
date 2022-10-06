"""Info columns that have column-like source tracking functional."""
from sql_graph.parsing.columns import Column
from sql_graph.typing import TTable
from sql_graph.typing import TokenizedJson


class InfoColumn(Column):
  """Subclass of Column to represent various information for a Table object.

  This class inherits from Column because it has similar logic for parsing and
  relinking to physical ancestors. It will be contained in Table object as a
  direct child.

  Attributes:
    _needs_serializing (bool): whether the instance will need to be serialized.
      Will be returned by needs_serializing property.
  """

  def __init__(self, table: TTable, name: str):
    super(InfoColumn, self).__init__(name=name, table=table)
    self._needs_serializing = False

  def __repr__(self) -> str:
    """Representation of the class in print for debug purposes."""
    return f"<{self.name} Info @ {self.table}>"

  def _get_serializing_id(self) -> str:
    """Overwrite of _get_serializing_id method."""
    return f"{self._serializing_params.get_parent_id()}__{self.name}"

  @property
  def needs_serializing(self) -> bool:
    """Overwrite of needs_serializing."""
    return self._needs_serializing


class JoinInfo(InfoColumn):
  """Subclass of InfoColumn for representing JOIN"""

  def __init__(self, table) -> None:
    super(JoinInfo, self).__init__(name="JOIN", table=table)
    self._join_infos = []

  def add_join(self, join_info: TokenizedJson) -> None:
    """Records information about a table join.

    Args:
      join_info (TokenizedJson): information about the join
    """
    if "on" in join_info:
      self._join_infos.append(join_info["on"])
      self._needs_serializing = True
    elif "using" in join_info:
      self._join_infos.append(join_info["using"])
      self._needs_serializing = True
    elif "cross join" in join_info or "join" in join_info:
      self._needs_serializing = True
    else:
      print(f"Unknown type of join: {join_info}")

  def parse_joins(self) -> None:
    """Parses recorded join_infos after table's sources are ready."""
    for join_info in self._join_infos:
      self._parse_value_json(join_info)


class WhereInfo(InfoColumn):
  """Subclass of InfoColumn for representing WHERE."""

  def __init__(self, table) -> None:
    super(WhereInfo, self).__init__(name="WHERE", table=table)

  def add_where(self, where_info: TokenizedJson) -> None:
    """Parses new where info for connections and marks instance for serializing.

    Args:
      where_info (TokenizedJson): information about the join
    """
    self._parse_value_json(where_info)
    self._needs_serializing = True
