"""Connection class."""
from sql_graph.typing import TGridItem


class Connection:
  """Connection from one GridItem to another.

  Attributes:
    source (GridItem): source object of the connection.
    target (GridItem): target object of the connection.
    data (JsonDict): any other connection data. Will be used by ReactFlow.
  """

  def __init__(self, source: TGridItem, target: TGridItem) -> None:
    self.source = source
    self.target = target
    self.data = {}

  def __repr__(self) -> str:
    """Representation of the class in print for debug purposes."""
    return f"<Connection {self.source} -> {self.target}>"
