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

"""Module containing namespace classes for table and graph."""
import abc
from collections import deque
from typing import Dict
from typing import Iterable
from typing import List
from typing import Optional

from sql_graph.exceptions import ParsingError
from sql_graph.exceptions import TableLookupError
from sql_graph.typing import TGraph
from sql_graph.typing import TTable
from sql_graph.typing import TTokenizedQuery
from sql_graph.typing import TableLocation
from sql_graph.typing import TokenizedJson


class Namespace(abc.ABC):
  """Abstract namespace class.

  Implements table management functionality and ensures virtual table isolation.

  Attributes:
    _master (TableLocation): master object which namespace is attached to.
      Can be either graph or table.
    _tables (Dict[str, TTable]): dictionary of tables with table names as keys
      and tables as values.
  """

  def __init__(self, master: TableLocation) -> None:
    self._master = master
    self._tables: Dict[str, TTable] = {}

  def _add_table(self, table: TTable) -> None:
    """Adds table object to the namespace."""
    if table.name in self._tables:
      raise ParsingError(f"Can't have tables with the same name "
                         f"inside a single namespace of {self._master}")
    else:
      self._tables[table.name] = table

  def _create_table_by_class(self, table_cls, **kwargs) -> TTable:
    """Creates table object with a given class and args, adds, and returns it.

    Args:
      table_cls: class of the table to create.
      **kwargs: keyword args for the table creation.
    Returns:
      Table: newly created table.
    """
    kwargs["location"] = self._master
    table = table_cls(**kwargs)
    self._add_table(table)
    return table

  def get_table_by_name(self, name: str) -> TTable:
    """Finds table by name.

    If table is not found in current namespace, tries to look in namespace
    of master's location. If master doesn't have a location, throws a
    TableLookupError.
    """
    name = name.replace("..", ".")
    if name in self._tables:
      return self._tables[name]
    elif hasattr(self._master, "location"):
      return self._master.location.namespace.get_table_by_name(name)
    else:
      raise TableLookupError(name)

  def get_tables(self, recursive=False) -> List[TTable]:
    """Returns a list with tables.

    Args:
      recursive (bool): if true, adds everything from namespaces of tables
        contained in current namespace.
    Returns:

    """
    table_list = list(self._tables.values())
    if recursive:
      for table in self._tables.values():
        table_list.extend(table.namespace.get_tables(recursive=True))
    return table_list

  def remove_table(self, table_name: str) -> None:
    """Removes table both from namespace and from memory.

    In addition, clears the namespace of the table in question, then
     cycles through table and children references and drops them.

    Args:
      table_name (str): name of the table to remove.
    """
    table = self._tables[table_name]
    del self._tables[table_name]
    table.namespace.clear()
    removal_queue = deque()
    removal_queue.append(table)
    while removal_queue:
      obj = removal_queue.popleft()
      for reference in obj.get_references():
        reference.remove_source(obj)
      if isinstance(obj, Iterable):
        for child in obj:
          removal_queue.append(child)
      del obj

  def clear(self) -> None:
    """Clears the namespace."""
    for table_name in list(self._tables.keys()):
      self.remove_table(table_name)


class TableNamespace(Namespace):
  """Implementation of the namespace for table.

  In addition to table management, supports alias management.

  Attributes:
    query (Query): query of the master table.
    graph (Graph): reference to the graph object. Used to add external tables.
    _aliases (Dict[str, str]): dictionary with aliases.
  """

  def __init__(self, master: TTable) -> None:
    super().__init__(master)
    self.query = master.query
    self.graph = master.query.graph
    self._aliases: Dict[str, str] = {}

  def _create_table_by_class(self, table_cls, **kwargs) -> TTable:
    """Override of _create_table_by_class to set query argument."""
    kwargs["query"] = self.query
    return super(TableNamespace, self)._create_table_by_class(table_cls,
                                                              **kwargs)

  def create_select_table(self, name: Optional[str],
                          table_info: TokenizedJson) -> TTable:
    """Creates and returns select table."""
    from sql_graph.parsing.tables import SelectTable
    return self._create_table_by_class(table_cls=SelectTable,
                                       name=name,
                                       table_info=table_info)

  def create_external_table(self, name: str) -> TTable:
    """Creates and returns external table inside graph's namespace."""
    return self.graph.namespace.create_external_table(name=name,
                                                      query=self.query)

  def create_unnest_table(self, unnest_name: Optional[str],
                          table_info: TokenizedJson) -> TTable:
    """Creates and returns unnest table."""
    from sql_graph.parsing.tables import UnnestTable
    return self._create_table_by_class(table_cls=UnnestTable,
                                       name=None,
                                       unnest_name=unnest_name,
                                       table_info=table_info)

  def register_alias(self, alias_name: str, table_name: str) -> None:
    """Registers new alias for master table.

    Args:
      alias_name (str): name of the alias.
      table_name (str): name of the table that is being aliased.
    """
    if alias_name in self._aliases:
      raise ParsingError(f"Alias with name {alias_name} "
                         f"for {self} already exists.")
    else:
      self._aliases[alias_name] = table_name

  def get_table_by_name(self, name: str) -> TTable:
    """Override of the get_table_by_name that checks dict with aliases."""
    name = self._aliases.get(name, name)
    return super(TableNamespace, self).get_table_by_name(name)


class GraphNamespace(Namespace):
  """Implementation of the namespace for a graph."""

  def __init__(self, graph: TGraph) -> None:
    super().__init__(graph)

  def _add_table(self, table: TTable) -> None:
    """Override of the _add_table with special logic for name collision.

    If table that is being added collides with an existing external table,
    triggers recalculation logic and replaces existing table.
    """
    try:
      super(GraphNamespace, self)._add_table(table)
    except ParsingError as e:
      from sql_graph.parsing.tables import ExternalTable
      existing_table = self._tables[table.name]
      if isinstance(existing_table, ExternalTable):
        self._tables[table.name] = table
        existing_table.recalculate()
        del existing_table
      else:
        raise e

  def create_physical_table(self, name: str,
                            table_info: TokenizedJson,
                            query: TTokenizedQuery) -> TTable:
    """Creates and returns physical select table."""
    from sql_graph.parsing.tables import SelectTable
    return self._create_table_by_class(table_cls=SelectTable,
                                       name=name,
                                       table_info=table_info,
                                       query=query)

  def create_external_table(self, name: str,
                            query: TTokenizedQuery) -> TTable:
    """Creates and returns external table."""
    from sql_graph.parsing.tables import ExternalTable
    return self._create_table_by_class(table_cls=ExternalTable,
                                       name=name,
                                       query=query)

  def create_cycle_breaker_table(self, source: TTable,
                                 target: TTable) -> TTable:
    """Creates and returns cycle breaker table."""
    from sql_graph.parsing.tables import CycleBreakerTable
    return self._create_table_by_class(table_cls=CycleBreakerTable,
                                       source=source,
                                       target=target)
