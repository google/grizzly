"""Coordinates class."""
import functools
from typing import Tuple

from sql_graph.typing import TCoordinates


@functools.total_ordering  # to support sorting
class Coordinates:
  """2D coordinates.

  Attributes:
    x (int): x coordinate.
    y (int): y coordinate.
    initialized (bool): whether coordinates were initialized.
  """

  def __init__(self, x: int = 0, y: int = 0, initialized: bool = True) -> None:
    self.x = x
    self.y = y
    self.initialized = initialized

  def __eq__(self, other: TCoordinates) -> bool:
    """Enables == operator to work with this class."""
    return self.x == other.x and self.y == other.y

  def __gt__(self, other: TCoordinates) -> bool:
    """Enables > operator to work with this class."""
    if self.x == other.x:
      return self.y > other.y
    return self.x > other.x

  def __repr__(self) -> str:
    """Representation of the class in print for debug purposes."""
    return f"<Coordinates ({self.x}, {self.y})>"

  def to_tuple(self) -> Tuple[int, int]:
    """Converts coordinates to a tuple."""
    return self.x, self.y
