"""Topological Layout class."""
from collections import defaultdict
from collections import deque
from typing import Dict
from typing import List
from typing import Tuple

from sql_graph.parsing.primitives import Coordinates
from sql_graph.parsing.primitives.settings import TABLE_X_SPACING
from sql_graph.parsing.primitives.settings import TABLE_Y_SPACING
from sql_graph.typing import TCoordinates
from sql_graph.typing import TTable


class TopologicalLayout:
  """
  Simple layout algorithm positions tables in topological order.

  Vertical positioning is semi-random.

  Attributes:
    _vertical_cnt (Dict[int, int]): count of last vertical position of the
      table for each column of slots of the grid.
    _max_width (Dict[int, int]):  maximum width of the tables
      for each column of the slots of the grid.
    _x_coordinates (Dict[int, int]): x_coordinates of each column of slots.
    _y_coordinates (Dict[int, int]): y_coordinates of each row of slots.
  """

  def _calculate_table_grid_slot_positions(self) -> None:
    """Method that calculates slot positions for each table."""
    queue = deque(self._tables_for_serializing)
    while queue:
      table = queue.popleft()
      for source in table.get_all_sources():
        queue.append(source)
        self._grid_pos[source].x = min(
          self._grid_pos[source].x,
          self._grid_pos[table].x - 1
        )

    queue = deque(self._tables_for_serializing)
    while queue:
      table = queue.popleft()
      x = self._grid_pos[table].x
      # table y is the first unused y coordinate for current x
      self._grid_pos[table].y = self._vertical_cnt[x]
      self._grid_pos[table].initialized = True
      # update _max_width and _vertical_cnt for current x
      self._vertical_cnt[x] = self._grid_pos[table].y + 1
      self._max_width[x] = max(
        self._max_width[x],
        table.serializing_params.width
      )
      for source in table.get_all_sources():
        if self._grid_pos[source].x == x - 1:
          if not self._grid_pos[source].initialized:
            queue.append(source)

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
    self._vertical_cnt: Dict[int, int] = defaultdict(lambda: 0)
    self._max_width: Dict[int, int] = defaultdict(lambda: 0)
    self._x_coordinates: Dict[int, int] = defaultdict(lambda: 0)
    self._y_coordinates: Dict[int, int] = defaultdict(lambda: 0)

    if self._tables_for_serializing:
      self._calculate_table_grid_slot_positions()
      self._calculate_table_grid_coordinates()
      self._center_columns_on_same_y()
