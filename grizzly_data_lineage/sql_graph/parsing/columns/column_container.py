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

"""Column container class."""
from collections import defaultdict
from typing import List, Dict, Union, Optional
from typing import Set

from sql_graph.exceptions import ColumnLookupError
from sql_graph.exceptions import UnknownScenario
from sql_graph.exceptions import UnsupportedTypeError
from sql_graph.parsing.columns import TableColumn
from sql_graph.parsing.columns import StarColumn
from sql_graph.parsing.primitives import Container
from sql_graph.parsing.primitives.settings import COLUMN_CONTAINER_CHILD_SPACING
from sql_graph.parsing.primitives.settings import \
  COLUMN_CONTAINER_HORIZONTAL_MARGIN
from sql_graph.parsing.primitives.settings import \
  COLUMN_CONTAINER_VERTICAL_MARGIN
from sql_graph.typing import TokenizedJson
from sql_graph.typing import TTable
from sql_graph.typing import TTableColumn


class ColumnContainer(Container):
  """Class for storage of columns in a table.

  Inherited from Container. Columns can be looked up by name or order number.
  using [] syntax.

  Attributes:
    table (Table): reference to the table that contains ColumnContainer
    star_column (StarColumn): column that represents missing info about the
      table. Can be initialized by external table or can be carried over from
      another table with initialized _star_column if SELECT * syntax is used.
    _column_dict (Dict[str, TableColumn): dictionary of columns with their names
      as keys. Is protected to prevent outside changes.
    _column_order_list (List[str]): list of column names in order. Is protected
      to prevent outside changes.
    _columns_by_source (Dict[Set[str]]): dict that keeps track of column names
      added from each source. Will be used during copy recalculation.
    _label (str): label of the ColumnContainer. May change depending on the
      parsing method called.
  """

  HORIZONTAL_MARGIN = COLUMN_CONTAINER_HORIZONTAL_MARGIN
  VERTICAL_MARGIN = COLUMN_CONTAINER_VERTICAL_MARGIN
  CHILD_SPACING = COLUMN_CONTAINER_CHILD_SPACING

  def __init__(self, table: TTable) -> None:
    super(ColumnContainer, self).__init__()
    self.table = table
    self.star_column = StarColumn(table=self.table)

    self._column_dict: Dict[str, TTableColumn] = {}
    self._column_order_list: List[str] = []
    self._columns_by_source: Dict[Set[str]] = defaultdict(set)

    self._label = "Columns"

  def __getitem__(self, key: Optional[Union[str, int]]) -> TTableColumn:
    """Method that allows dictionary_like item access with [].

    If column is not found using the key, will return _star_column if present.
    Otherwise, raises ColumnLookupError.

    Args:
      key (str, int, None): key to look for.
    Returns:
      TableColumn
    """
    try:
      if isinstance(key, str):  # get column by name
        return self._column_dict[key]
      elif isinstance(key, int):  # get column by order number
        column_name = self._column_order_list[key]
        return self._column_dict[column_name]
      elif key is None:  # treat None as lookup error
        raise ColumnLookupError(None)
      else:
        raise UnsupportedTypeError(type(key))
    except (KeyError, IndexError, ColumnLookupError) as e:
      if self.star_column.initialized:
        return self.star_column
      else:
        raise ColumnLookupError(key) from e

  def __repr__(self):
    """Representation of the class in print for debug purposes."""
    return f"<Column Container @ {self.table}>"

  def _validate_column_name(self, name: str) -> str:
    """Validates the table name to ensure it is unique and not empty.

    If the name is not unique, adds a number to the end of it. If it is empty,
    uses BQ logic to generate a unique name.

    Args:
      name (str): name to validate.
    Returns:
      str: validated name.
    """
    cnt = 0
    if name is None:
      name_template = "f{cnt}_"
      new_name = name_template.format(cnt=cnt)
    else:
      name_template = name + "_{cnt}"
      new_name = name
    while new_name in self._column_dict:
      cnt += 1
      new_name = name_template.format(cnt=cnt)
    return new_name

  def add_column(self, column: TTableColumn) -> None:
    """Adds a column to the dict and remembers its ordering and does validation.

    Args:
      column (Column): column to add.
    """
    column.name = self._validate_column_name(column.name)
    self._column_dict[column.name] = column
    self._column_order_list.append(column.name)
    # if column has just one source, record it in _columns_by_source
    if len(column.get_sources()) == 1:
      self._columns_by_source[column.get_sources()[0]].add(column.name)

  def _create_column(self, column_info: TokenizedJson) -> TTableColumn:
    """Method that extracts name and value from column info and creates column.

    Args:
      column_info (TokenizedJsonDict): column information JSON.
    Returns:
      TableColumn: created column
    """
    name = column_info.get("name")
    value = column_info.get("value")
    if isinstance(value, str):
      if name is None:
        name, value = value, None
    column = TableColumn(name, value, self.table)
    return column

  def _copy_columns_from_source(self, source_name: str) -> None:
    """Copies columns from a source with a given name.

    Used in case of * syntax.
    Also carries over information about _star_column, if it was initialized
    in source.

    Args:
      source_name (str): name of the source.
    """
    source = self.table.namespace.get_table_by_name(name=source_name)
    for column in source.columns:
      if isinstance(column, StarColumn):
        self.star_column.add_source(source=column)
      else:
        self.add_column(column.copy(self.table))

  def _copy_columns_from_all_sources(self) -> None:
    """Copies all columns from all sources.

    Used in case of * syntax.
    """
    for source in self.table.get_sources():
      self._copy_columns_from_source(source.name)

  def redo_copy(self, source_to_redo: TTable) -> bool:
    """Redoes copy if one of the sources has changed.

    Args:
      source_to_redo (Table): source that changed.
    Returns:
      bool: whether any new columns were added.
    """
    new_columns_added = False
    new_source = self.table.namespace.get_table_by_name(source_to_redo.name)
    self.star_column.remove_source(source_to_redo.columns.star_column)
    for column in new_source.columns:
      if isinstance(column, StarColumn):
        self.star_column.add_source(column)
      else:
        if column.name not in self._columns_by_source[new_source.name]:
          self.add_column(column.copy(self.table))
          new_columns_added = True
    return new_columns_added

  def parse_select(self, select_type: str, select_info: TokenizedJson) -> None:
    """Method that parses a select statement JSON.

    Args:
      select_type (str): type of the select.
      select_info (TokenizedJsonDict): JSON wih information about
        table's select.
    """
    def check_single_table_star_syntax(ci: TokenizedJson) -> bool:
      return isinstance(ci, dict) and "value" in ci \
             and isinstance(ci["value"], str) and ci["value"].endswith(".*")

    self._label = select_type
    if isinstance(select_info, (dict, str)):
      select_info = [select_info]
    for column_info in select_info:
      if isinstance(column_info, str) and column_info == "*":
        self._copy_columns_from_all_sources()
      elif check_single_table_star_syntax(column_info):
        source_name = column_info["value"][:-2]
        self._copy_columns_from_source(source_name)
      else:
        self.add_column(self._create_column(column_info))

  def parse_union(self, union_type: str, union_info: TokenizedJson) -> None:
    """Method that parses a union statement JSON.

    Args:
      union_type (str): type of the union.
      union_info (TokenizedJsonDict): JSON wih information about
        table's union.
    """
    self._label = union_type
    column_names = set()
    for table_info in union_info:
      virtual_table = self.table.namespace.create_select_table(
        name=None,
        table_info=table_info
      )
      self.table.add_source(virtual_table)
      column_names = column_names.union({c.name for c in virtual_table.columns})
    for column_name in sorted(column_names):
      column = TableColumn(name=column_name, value=None, table=self.table)
      for source_table in self.table.get_sources():
        try:
          column.add_source(source_table.columns[column_name])
        except ColumnLookupError:
          raise UnknownScenario(f"Column count mismatch "
                                f"while parsing {union_type} "
                                f"for table {self.table}")
      self.add_column(column)

  def get_all_references(self) -> Set[TTableColumn]:
    """Override of get_all_references to include star column."""
    references = super(ColumnContainer, self).get_all_references()
    references |= set(self.star_column.get_references())
    return references

  def _get_children(self) -> List[TTableColumn]:
    """Overwrite of the _get_children method."""
    children = [self._column_dict[t] for t in self._column_order_list]
    if self.star_column.initialized:
      children.insert(0, self.star_column)
    return children

  def _get_serializing_id(self) -> str:
    """Overwrite of _get_serializing_id method."""
    return f"{self._serializing_params.get_parent_id()}_column_container"

  def _get_label(self) -> str:
    """Override of _get_label."""
    return self._label
