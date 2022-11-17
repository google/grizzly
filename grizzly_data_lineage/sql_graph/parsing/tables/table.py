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

"""Abstract table class."""
import abc
from typing import Dict
from typing import List
from typing import Optional
from typing import Set

from sql_graph.exceptions import ParsingError
from sql_graph.exceptions import TableLookupError
from sql_graph.parsing.columns import Column
from sql_graph.parsing.columns import ColumnContainer
from sql_graph.parsing.columns import JoinInfo
from sql_graph.parsing.columns import WhereInfo
from sql_graph.parsing.primitives import Container
from sql_graph.parsing.primitives import TextInfoPanel
from sql_graph.parsing.primitives.settings import TABLE_CHILD_SPACING
from sql_graph.parsing.primitives.settings import TABLE_HORIZONTAL_MARGIN
from sql_graph.parsing.primitives.settings import TABLE_MAX_WIDTH_PENALTY
from sql_graph.parsing.primitives.settings import TABLE_VERTICAL_MARGIN
from sql_graph.parsing.utils.namespace import TableNamespace
from sql_graph.typing import TGridItem
from sql_graph.typing import TTokenizedQuery
from sql_graph.typing import TTable
from sql_graph.typing import TableLocation
from sql_graph.typing import TokenizedJson


class Table(Container, abc.ABC):
  """Abstract Class representing a basic SQL table. Subclass of the Container.

  Attributes:
    name (str): name of the table.
    table_info (TokenizedJson): JSON information about the table.
    location (TableLocation): location of the table. If table is located inside
      a graph namespace, it is considered physical. Otherwise, it can be located
      inside other table's namespace and will be virtual.
    namespace (TableNamespace): namespace of the table. All virtual tables that
      are present inside this table's definition will be stored here.
    columns (ColumnContainer): stores columns.
    join_info (JoinInfo): stores join information.
    where_info (WhereInfo): stores where information.
    text_info_panels (Dict[str, TextInfoPanel]): dictionary containing text info
      panels for the table instance.
  """

  HORIZONTAL_MARGIN = TABLE_HORIZONTAL_MARGIN
  VERTICAL_MARGIN = TABLE_VERTICAL_MARGIN
  CHILD_SPACING = TABLE_CHILD_SPACING
  MAX_WIDTH_PENALTY = TABLE_MAX_WIDTH_PENALTY

  def _add_source_by_name(self, source_name: str) -> None:
    """Looks up table by name and adds it.

    If table with given name was not found, will create an external table.

    Args:
      source_name (str): name of the table to add.
    """
    try:
      source = self.namespace.get_table_by_name(source_name)
    except TableLookupError:
      source = self.namespace.create_external_table(source_name)
    self.add_source(source)

  @abc.abstractmethod
  def _parse(self) -> None:
    """Abstract method that will perform table_info parsing."""
    pass

  def _get_name(self, name: Optional[str]) -> str:
    """Returns name for the table based on the passed name.

    If the name is None, will get next available anonymous name from the graph.
    """
    if name is not None:
      name = name.replace("..", ".")
    else:
      name = self.query.graph.get_next_anonymous_name()
    return name

  def __init__(self, name: Optional[str], table_info: TokenizedJson,
               location: TableLocation, query: TTokenizedQuery) -> None:
    super().__init__()
    self.location = location
    self.query = query
    self.name = self._get_name(name)
    self.table_info = table_info

    self.namespace = TableNamespace(self)
    self.columns = ColumnContainer(self)
    self.join_info = JoinInfo(self)
    self.where_info = WhereInfo(self)
    self.text_info_panels = {}

    self._parse()

  @property
  def physical(self) -> bool:
    """If table is located inside of graph's namespace, then it is physical."""
    from sql_graph import Graph
    return isinstance(self.location, Graph)

  def get_all_sources(self) -> Set[TTable]:
    """Override of get_all_sources to convert columns' sources to tables."""
    sources = super(Table, self).get_all_sources()
    result = set()
    for source in sources:
      if isinstance(source, Column):
        source = source.table
      if isinstance(source, Table) and source not in result:
        result.add(source)
    return result

  def get_all_table_references(self) -> Set[TTable]:
    """Modified version of get_all_references, which converts refs to tables."""
    references = super(Table, self).get_all_references()
    result = set()
    for reference in references:
      if isinstance(reference, Column):
        reference = reference.table
      if isinstance(reference, Table) and reference not in result:
        result.add(reference)
    return result

  def remove_source(self, source: TGridItem) -> None:
    """Override of remove_source to control for a special case.

    If a source, which is requested to be removed, it not a direct source, but
    a source from one of the table columns, the ParsingError will not be raised.
    """
    try:
      super(Table, self).remove_source(source)
    except ParsingError as e:
      if source in self.get_all_sources():
        return
      else:
        raise e

  def refresh_source(self, old_source: TTable) -> None:
    """Removes and adds source by name to refresh it."""
    self.remove_source(old_source)
    self._add_source_by_name(old_source.name)

  def recalculate(self) -> None:
    """Recalculates table in an event one of external tables gets redefined.

    All references will be relinked and copying using SELECT * syntax from this
    table will be redone. Recalculation will be propagated to tables that
    reference this table, if they gained additional columns from SELECT *
    copying recalculation.
    """
    all_references = list(self.get_all_references())
    all_references.sort(key=lambda x: isinstance(x, Table), reverse=True)
    tables_to_recalculate = []
    for reference in all_references:
      if isinstance(reference, Table):
        reference.refresh_source(self)
        star_column = self.columns.star_column
        if star_column in reference.columns.star_column.get_sources():
          columns_added = reference.columns.redo_copy(source_to_redo=self)
          if columns_added:
            tables_to_recalculate.append(reference)
      elif isinstance(reference, Column):
        reference.recalculate_sources()
    for table in tables_to_recalculate:
      table.recalculate()

  def relink_to_physical_ancestors(self) -> List[TTable]:
    """Recalculates common_source list to only leave physical sources.


    If the source is not physical, replace it with the list of its sources.
    Also, recursively calls this method its sources, columns, and column-like
    children.

    Returns:
      List[TTable]: list of physical sources, or self if this table is physical.
    """
    physical_sources = []
    for source in self.get_all_sources():
      if source.physical:
        physical_sources.append(source)
      else:
        physical_sources.extend(source.relink_to_physical_ancestors())
    self.replace_sources(physical_sources)
    super(Table, self).relink_to_physical_ancestors()
    return [self] if self.physical else self.get_sources()

  def _get_children(self) -> List[TGridItem]:
    """Override of the _get_children method to include info panels."""
    # noinspection PyTypeChecker
    return (list(self.text_info_panels.values())
            + [self.columns, self.join_info, self.where_info])

  def _get_serializing_id(self) -> str:
    """Override of _get_serializing_id method."""
    table_id = self.name
    if not self.physical:
      table_id = f"{self.location.serializing_params.id}__{table_id}"
    return table_id

  def _get_label(self) -> str:
    """Override of the _get_label method."""
    return self.name

  def _calculate_serializing_params(self) -> None:
    """Override of _calculate_serializing_params method."""
    self._serializing_params.query = self.query
    self.text_info_panels = {
      "domain": TextInfoPanel(name="domain_info", text=self.query.domain),
    }
    super()._calculate_serializing_params()
    self._serializing_params.data["tablePhysical"] = self.physical

    # stretch info panels to make them match ColumnContainer.
    for text_info_panel in self.text_info_panels.values():
      text_info_panel.horizontal_stretch(self.columns.serializing_params.width)
    self.join_info.horizontal_stretch(self.columns.serializing_params.width)
    self.where_info.horizontal_stretch(self.columns.serializing_params.width)
