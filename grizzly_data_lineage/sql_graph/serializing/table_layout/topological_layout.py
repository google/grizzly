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

"""Topological Layout class."""
from collections import defaultdict
from collections import deque
from typing import Dict
from typing import List
from typing import Tuple

from sql_graph.parsing.primitives import Coordinates
from sql_graph.parsing.primitives.settings import MAX_PIXELS_PER_GRID_COLUMN
from sql_graph.parsing.primitives.settings import TABLE_X_SPACING
from sql_graph.parsing.primitives.settings import TABLE_Y_SPACING
from sql_graph.typing import TCoordinates
from sql_graph.typing import TTable


class TopologicalLayout:
  """
  Simple layout algorithm positions tables in topological order.

  Vertical positioning is semi-random.

  Attributes:
    _table_order (Dict[TTable, int]): topological order of the tables.
    _grid_col_heights (Dict[int, int]): current height for each column
      of the slots of the grid.
    _max_width (Dict[int, int]): maximum width of the tables
      for each column of the slots of the grid.
    _vertical_cnt (Dict[int, int]): count of last vertical position of the
      table for each column of slots of the grid.
    _x_coordinates (Dict[int, int]): x_coordinates of each column of slots.
    _y_coordinates (Dict[int, int]): y_coordinates of each row of slots.
  """

  def _table_topological_sort(self) -> None:
    """Method that sorts tables topologically.

    In the topological ordering, sources will always come before the references.
    This implementation of ordering, source will be places as close to its
    references as possible, thus reducing the crossing over of connections.
    """
    queue = deque(self._tables_for_serializing)
    while queue:
      table = queue.popleft()
      for source in table.get_all_sources():
        queue.append(source)
        self._table_order[source] = min(
          self._table_order[source],
          self._table_order[table] - 1
        )

  def _calculate_table_grid_slot_positions(self) -> None:
    """Method that calculates slot positions for each table.

    Relies on topological ordering.
    """
    # tables are sorted in the reverse of topological ordering
    # so the downstream tables will get the position earlier
    for table in sorted(
        self._tables_for_serializing,
        key=lambda t: (-self._table_order[t], t.name)
    ):
      # update the x position, so it would be to the left of all immediate
      # references
      for reference in table.get_all_table_references():
        self._grid_pos[table].x = min(
          self._grid_pos[reference].x - 1,
          self._grid_pos[table].x
        )

      table_height = table.serializing_params.height
      x = self._grid_pos[table].x
      # continue shifting the table to the left until one of the following:
      # table can be added to the column while satisfying height restrictions
      # or an empty column is reached
      while self._vertical_cnt[x] != 0 and self._grid_col_heights[
        x] + table_height > MAX_PIXELS_PER_GRID_COLUMN:
        x = self._grid_pos[table].x = self._grid_pos[table].x - 1

      # table y is the first unused y coordinate for current x
      self._grid_pos[table].y = self._vertical_cnt[x]
      self._grid_pos[table].initialized = True

      # update information for current x
      self._vertical_cnt[x] = self._grid_pos[table].y + 1
      self._grid_col_heights[x] += table_height + TABLE_Y_SPACING
      self._max_width[x] = max(
        self._max_width[x],
        table.serializing_params.width
      )

  def _calculate_table_grid_coordinates(self) -> None:
    """Method that calculates actual coordinates for all tables.

    Method takes into account tables' slot positions, width and height.
    """
    grid_information: Dict[Tuple[int, int]: TTable] = {}
    for table in self._tables_for_serializing:
      grid_pos = self._grid_pos[table]
      if grid_pos.initialized:
        grid_information[grid_pos.to_tuple()] = table
    for x, y in sorted(grid_information.keys()):
      table = grid_information[x, y]
      coord_x, coord_y = 0, 0
      if x - 1 in self._x_coordinates:
        # add _max_width of the last row and spacing
        coord_x = (self._x_coordinates[x - 1] + self._max_width[x - 1] +
                   TABLE_X_SPACING)
      if x in self._y_coordinates:
        coord_y = self._y_coordinates[x]
      table.set_coordinates(Coordinates(coord_x, coord_y))

      if x not in self._x_coordinates:
        self._x_coordinates[x] = coord_x
      self._y_coordinates[x] = (coord_y + table.serializing_params.height
                                + TABLE_Y_SPACING)

  def _center_columns_on_same_y(self) -> None:
    """Centers the columns vertically.

    Each column will have it's center at the same y coordinate """
    current_y_centers = {}
    for x, y in self._y_coordinates.items():
      self._y_coordinates[x] = y - TABLE_Y_SPACING
      current_y_centers[x] = self._y_coordinates[x] / 2
    desired_y_center = max(self._y_coordinates) / 2
    for table in self._tables_for_serializing:
      coord_x, coord_y = table.serializing_params.coordinates.to_tuple()
      x, y = self._grid_pos[table].to_tuple()
      coord_y += (desired_y_center - current_y_centers[x])
      table.set_coordinates(Coordinates(coord_x, coord_y))

  def __init__(self, tables_for_serializing: List[TTable]) -> None:
    self._tables_for_serializing = tables_for_serializing
    self._grid_pos: Dict[TTable, TCoordinates] = defaultdict(lambda:
                                                             Coordinates(0, 0))
    self._table_order: Dict[TTable, int] = defaultdict(lambda: 0)
    self._grid_col_heights: Dict[int, int] = defaultdict(lambda: 0)
    self._max_width: Dict[int, int] = defaultdict(lambda: 0)
    self._vertical_cnt: Dict[int, int] = defaultdict(lambda: 0)
    self._x_coordinates: Dict[int, int] = defaultdict(lambda: 0)
    self._y_coordinates: Dict[int, int] = defaultdict(lambda: 0)

    if self._tables_for_serializing:
      self._table_topological_sort()
      self._calculate_table_grid_slot_positions()
      self._calculate_table_grid_coordinates()
      self._center_columns_on_same_y()
