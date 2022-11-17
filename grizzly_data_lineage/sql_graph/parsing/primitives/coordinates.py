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
