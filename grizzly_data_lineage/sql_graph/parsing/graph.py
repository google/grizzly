"""SQL parsing graph.

Module that contains main Graph class and a Query Class.

Example usage:
query_list = [Query(raw_query, target_table, domain), ...]
g = Graph(query_list)
g.remove_non_physical_tables()  # optional
g.break_cycles()
g.calculate_table_serializing_params()
"""
from typing import List, Optional
from typing import Set

from sql_graph.exceptions import ParsingError
from sql_graph.parsing.utils import GraphNamespace
from sql_graph.parsing.utils import TokenizedQuery

from sql_graph.typing import TQuery
from sql_graph.typing import TTable


class Graph:
  """Class representing a SQL graph.

  Attributes:
    _queries (List[Query]): a list of queries added to the graph.
    namespace (GraphNamespace): global namespace with physical tables.
    _anonymous_table_name_counter (int): counter of anonymous tables. Used for
      giving them placeholder names.
  """

  def __init__(self, queries: List[TQuery]) -> None:
    self._queries = queries
    self.namespace = GraphNamespace(self)
    self._anonymous_table_name_counter = 0

    for query in self._queries:
      tokenized_query = TokenizedQuery(query, graph=self)
      for tokenized_step in tokenized_query.get_tokenized_steps():
        try:
          self.namespace.create_physical_table(
            name=tokenized_step.table_name,
            table_info=tokenized_step.query,
            query=tokenized_query
          )
        except ParsingError as e:
          print(f"Failed to parse {tokenized_query} due to an error: {e}")

  def get_next_anonymous_name(self) -> str:
    """Returns placeholder name and increments the counter."""
    name = f"anonymous_table{self._anonymous_table_name_counter}_"
    self._anonymous_table_name_counter += 1
    return name

  def remove_non_physical_tables(self) -> None:
    """Starts relinking process and clears namespaces of physical tables."""
    for table in self.namespace.get_tables(recursive=False):
      table.relink_to_physical_ancestors()
    for table in self.namespace.get_tables(recursive=False):
      table.namespace.clear()

  def _find_cycles_relative_to_table(self, starting_table: TTable,
                                     table: Optional[TTable] = None,
                                     visited: Optional[Set[TTable]] = None):
    """Helper method that finds cycles using DFS.

    Args:
      starting_table (Table): initial table. If it will be encountered again
        over the course of DFS, then a cycle is found.
      table (Table or None): current table.
      visited (Set[TTable]): set with tables that were already visited.
    Yields:
      Pairs of tables which complete the cycle. One of them is always going to
        be starting table.
    """
    if table is None:
      table = starting_table
      visited = set()
    visited.add(table)
    for source_table in table.get_all_sources():
      if source_table == starting_table:
        yield starting_table, table
      if source_table not in visited:
        yield from self._find_cycles_relative_to_table(
          starting_table,
          source_table,
          visited
        )

  def break_cycles(self) -> None:
    """Finds cycles and creates Cycle Breaker tables to break them."""
    sorted_tables = sorted(self.namespace.get_tables(recursive=True),
                           key=lambda x: x.name)
    for table in sorted_tables:
      for source, target in self._find_cycles_relative_to_table(table):
        self.namespace.create_cycle_breaker_table(source, target)

  def calculate_table_serializing_params(self) -> None:
    """Calculates serializing params of tables in the correct order."""
    for table in self.namespace.get_tables(recursive=True):
      table.calculate_serializing_params()
