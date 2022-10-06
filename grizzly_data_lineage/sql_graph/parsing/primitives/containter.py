"""Container class."""
import abc
from typing import List
from typing import Optional
from typing import Set

from sql_graph.parsing.primitives import Coordinates
from sql_graph.parsing.primitives import GridItem
from sql_graph.typing import TGridItem


class Container(GridItem, abc.ABC):
  """Subclass of GridItem that can contain other GridItems.

  Class Attributes:
    HORIZONTAL_MARGIN (int): horizontal margin of the instance (space between
      left or right border and contents of the GridItem). Will be overridden
      by child classes.
  """

  HORIZONTAL_MARGIN = 0
  VERTICAL_MARGIN = 0
  CHILD_SPACING = 0

  @abc.abstractmethod
  def _get_children(self) -> List[TGridItem]:
    """Abstract method that will return the children in a particular order.

    It is used for length calculation and iteration.
    """
    pass

  def __len__(self):
    """Enables len() function to work with the class."""
    return len(self._get_children())

  def __iter__(self):
    """Enables iteration over children to work with the class."""
    return iter(self._get_children())

  def get_all_sources(self) -> Set[TGridItem]:
    """Returns a set of unique sources from instance and all its children."""
    sources = set(self.get_sources())
    for child in self:
      if isinstance(child, Container):
        sources |= child.get_all_sources()
      else:
        sources |= set(child.get_sources())
    return sources

  def get_all_references(self) -> Set[TGridItem]:
    """Returns a set of unique references from instance and all its children."""
    references = set(self.get_references())
    for child in self:
      if isinstance(child, Container):
        references |= child.get_all_references()
      else:
        references |= set(child.get_references())
    return references

  def relink_to_physical_ancestors(self) -> None:
    """Override of parent's method that also calls it for all children."""
    super(Container, self).relink_to_physical_ancestors()
    for child in self:
      child.relink_to_physical_ancestors()

  def _calculate_next_child_coordinates(self, previous: Optional[
    TGridItem]) -> Coordinates:
    """Returns coordinates for next child based on the previous one."""
    # in case if previous is None, start with initial offset.
    if previous is None:
      return Coordinates(self.HORIZONTAL_MARGIN,
                         self.VERTICAL_MARGIN + self.CHILD_SPACING)
    # otherwise use previous y coordinate to calculate next set of coordinates
    else:
      prev_y = previous.serializing_params.coordinates.y
      prev_height = previous.serializing_params.height
      return Coordinates(
        x=self.HORIZONTAL_MARGIN,
        y=prev_y + prev_height + self.CHILD_SPACING
      )

  def _calculate_serializing_params(self):
    """Override of GridItem method that also calculates params for children."""
    super(Container, self)._calculate_serializing_params()
    max_x = 0  # maximum children x on the right
    last_y = 0  # last child y on the bottom
    previous = None
    for current in self:
      # calculate current child params, passing self as a parent
      current.calculate_serializing_params(parent=self)
      if not current.needs_serializing:
        continue
      coords = self._calculate_next_child_coordinates(previous)
      current.set_coordinates(coords)

      max_x = max(max_x, current.serializing_params.coordinates.x
                  + current.serializing_params.width)
      last_y = (current.serializing_params.coordinates.y
                + current.serializing_params.height)
      previous = current
    self._serializing_params.width = max_x + self.HORIZONTAL_MARGIN
    self._serializing_params.height = last_y + self.VERTICAL_MARGIN

    # call label wrapping again after the width was calculated
    label_height = self._wrap_label()
    self._serializing_params.height += label_height
    for child in self:
      # shift all children down
      child.vertical_shift(offset=label_height)

  @property
  def needs_serializing(self) -> bool:
    """Override of needs_serializing.

    Will return False for Containers with no children.
    """
    return len(self) > 0
