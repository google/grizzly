"""Abstract serializer class."""
import abc
from collections import deque
from typing import List

from sql_graph.parsing.primitives import Container
from sql_graph.serializing.table_layout import TopologicalLayout
from sql_graph.typing import TGraph, TGridItem


class Serializer(abc.ABC):
  """Abstract Serializer class with helper functions

  This class is responsible for calculating table positioning and formatting
  the serializing params.

  Attributes:
    graph (Graph): column level graph
    _physical (bool): whether to serialize only _physical tables
  """

  def __init__(self, graph: TGraph, physical: bool) -> None:
    self._graph = graph
    self._physical = physical

    if self._physical:
      graph.remove_non_physical_tables()
    graph.break_cycles()
    graph.calculate_table_serializing_params()

    self._tables_for_serializing = graph.namespace.get_tables(recursive=True)
    self._tables_for_serializing.sort(key=lambda x: x.name)
    TopologicalLayout(self._tables_for_serializing)

  def _get_object_list(self) -> List[TGridItem]:
    """Returns the list all objects that need to be serialized."""
    queue = deque(self._tables_for_serializing)
    objects = []
    while queue:
      obj = queue.popleft()
      objects.append(obj)
      if isinstance(obj, Container):
        for child in obj:
          if child.needs_serializing:
            queue.append(child)
    return objects

  def _get_connection_list(self):
    """Returns the list all connections that need to be serialized."""
    objects = self._get_object_list()
    return [c for obj in objects for c in obj.serializing_params.connections]

  @abc.abstractmethod
  def serialize(self):
    """Abstract method that will return formatted serialization result."""
    pass
