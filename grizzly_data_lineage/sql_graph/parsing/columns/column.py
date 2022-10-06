"""Abstract column class."""
import abc
from typing import List

from sql_graph.exceptions import ParsingLookupError
from sql_graph.exceptions import UnknownScenario
from sql_graph.parsing.primitives import GridItem
from sql_graph.parsing.primitives.settings import COLUMN_VERTICAL_MARGIN
from sql_graph.parsing.primitives.settings import COLUMN_WIDTH
from sql_graph.typing import TColumn
from sql_graph.typing import TTable
from sql_graph.typing import TokenizedJson


class Column(GridItem, abc.ABC):
  """Abstract class that represents a Column.

  Inherits from GridItem. Will be displayed as a rectangle with a label.
  Has the ability to parse portions of Tokenized JSON related to it for
  connections. Also, can recalculate its sources to link to physical objects
  only.

  Class Attributes:
    IGNORED_TAGS (Set[str]): set of tags that will be ignored for column
      level lineage parsing.
    VIRTUAL_TABLE_TAGS (Set[str]): set of tags that will be treated as a virtual
      table if found in JSON value of the column.

  Attributes:
    name (str): name of the column to distinguish it from other columns.
      It will also be used for serializing label.
    table (Table): table object where the column is located
    _potential_source_names (List[str]): list of potential source column names.
      An attempt to add them again will be done during column recalculation.
    _sources_added_by_name (List[TColumn]): list of source columns that were
      looked up by their name. They will be removed and re-added during
      recalculation.
  """

  IGNORED_TAGS = {"literal", "count"}
  VIRTUAL_TABLE_TAGS = {"select", "select_distinct", "union", "union_all",
                        "unnest"}
  WIDTH = COLUMN_WIDTH
  VERTICAL_MARGIN = COLUMN_VERTICAL_MARGIN

  def _add_source_by_name(self, source_name: str):
    """Looks up the source by string name.

    Will search for a column with a given name in instance's table's sources.

    Args:
      source_name (str): name of the source column.
    """
    from sql_graph.parsing.tables import lookup_source
    table_name, column_name = lookup_source(self.table, source_name)
    source_table = self.table.namespace.get_table_by_name(table_name)
    source_column = source_table.columns[column_name]
    self.add_source(source_column)
    if source_column not in self._sources_added_by_name:
      self._sources_added_by_name.append(source_column)

  def _add_virtual_table(self, info: TokenizedJson) -> None:
    """Creates virtual table and adds its only column as a source.

    Args:
      info (TokenizedJson): tokenized JSON with info about virtual table.
    """
    table = self.table.namespace.create_select_table(name=None, table_info=info)
    # if table has only one column, add it as a source
    if len(table.columns) == 1:
      self.add_source(table.columns[0])
    else:
      raise UnknownScenario("Virtual table created from column SELECT statement"
                            f" in {self} cannot have more that one column.")

  def _parse_value_json(self, value_json: TokenizedJson) -> None:
    """Parses a tokenized JSON and adds all sources it can locate.

    Args:
      value_json (TokenizedJson): tokenized JSON representing column value.
    """
    if isinstance(value_json, dict):
      # if select is present, move its contents to a virtual table
      if any([t in self.VIRTUAL_TABLE_TAGS for t in value_json]):
        self._add_virtual_table(value_json)
      else:
        for key in value_json:
          # recursively parse all other keys if they are not ignored
          if key not in self.IGNORED_TAGS:
            self._parse_value_json(value_json[key])
    elif isinstance(value_json, list):
      # recursively parse all elements
      for item in value_json:
        self._parse_value_json(item)
    elif isinstance(value_json, str):
      # attempt to find a source by string or number
      self._potential_source_names.append(value_json)
      try:
        self._add_source_by_name(value_json)
      except ParsingLookupError:
        print(f"Could not lookup source from `{value_json}` "
              f"in value of {self}")
    elif value_json is None or isinstance(value_json, (int, float)):
      pass
    else:
      print(f"Don't know how to parse: {value_json} in value of {self}")

  def __init__(self, name: str, table: TTable) -> None:
    super(Column, self).__init__()
    self.name = name
    self.table = table
    self._potential_source_names: List[str] = []
    self._sources_added_by_name: List[TColumn] = []

  def __repr__(self):
    """Representation of the class in print for debug purposes."""
    return f"<Column '{self.name}' @ {self.table}>"

  def recalculate_sources(self) -> None:
    """Method used to recalculate sources if one of the source tables changed.

    Removes all sources added by name and re-attempts to add all sources
    from _potential_source_names.
    """
    for source in self._sources_added_by_name:
      self.remove_source(source)
    self._sources_added_by_name.clear()
    for source_name in self._potential_source_names:
      try:
        self._add_source_by_name(source_name)
      except ParsingLookupError:
        pass

  def replace_sources_from_table(self, old_table: TTable,
                                 new_table: TTable) -> None:
    """Replaces source columns from one table to another.

    Method is used during cycle breaking to relink this instance to the
    cycle breaker table columns.

    Args:
      old_table (Table): table that needs to be replaced.
      new_table (Table): cycle breaker table that will be used as a replacement.
    """
    for source_column in self.get_sources():
      if source_column.table == old_table:
        self.remove_source(source_column)
        self.add_source(new_table.columns[source_column.name])

  def relink_to_physical_ancestors(self) -> None:
    """Recalculates source list to only leave physical sources.

    If the source is not physical, replace it with the list of its sources.
    """
    super(Column, self).relink_to_physical_ancestors()
    physical_sources = []
    for source in self._sources:
      if source.table.physical:
        physical_sources.append(source)
      else:
        physical_sources.extend(source.get_sources())
    self._sources = physical_sources

  def _get_label(self) -> str:
    """Override of _get_label. Returns column name."""
    return self.name

  def _calculate_serializing_params(self) -> None:
    """Override of _calculate_serializing_params method."""
    super(Column, self)._calculate_serializing_params()
    for source in self._sources:
      self.add_connection(source)
