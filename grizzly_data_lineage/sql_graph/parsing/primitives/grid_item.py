import abc
from textwrap import wrap
from typing import List
from typing import Optional

from sql_graph.exceptions import SerializingParamsNotReady
from sql_graph.parsing.primitives import Connection
from sql_graph.parsing.primitives import SerializingParams
from sql_graph.parsing.primitives.settings import HORIZONTAL_PIXELS_PER_LETTER
from sql_graph.parsing.primitives.settings import VERTICAL_PIXELS_PER_LINE
from sql_graph.typing import TContainer
from sql_graph.typing import TCoordinates
from sql_graph.typing import TGridItem
from sql_graph.typing import TSerializingParams


class GridItem(abc.ABC):
  """Abstract class that represents basic object that can be visualized.

  Contains source management capabilities (source and reference tracking) as
  well as serializing capabilities (ID, label, and coordinates calculation).

  Class Attributes:
    WIDTH (int): starting width of the instance. Will be overridden by child
      classes.
    VERTICAL_MARGIN (int): vertical margin of the instance (space between top
      or bottom and contents of the GridItem). Will be overridden by child
      classes.

  Attributes:
    _sources (List[TGridItem]): list of the sources of the instance.
    _references (List[TGridItem]): list of the references, i.e. objects that
      have the instance as a source.
    _serializing_params (SerializingParams): object with params used for
      serialization and visualization. Is protected to prevent outside
      modification.
  """

  WIDTH = 0
  VERTICAL_MARGIN = 0

  def __init__(self) -> None:
    self._sources: List[TGridItem] = []
    self._references: List[TGridItem] = []
    self._serializing_params = SerializingParams(self)

  # SOURCE MANAGEMENT
  def register_reference(self, obj: TGridItem) -> None:
    """Adds a reference to the list.

    This method should only be called inside add_source.
    """
    if obj not in self._references:
      self._references.append(obj)

  def add_source(self, source: TGridItem) -> None:
    """Adds a source to the list.

    Also calls register_reference of the source.
    """
    if source not in self._sources:
      self._sources.append(source)
      source.register_reference(self)

  def drop_reference(self, obj: TGridItem) -> None:
    """Removes a reference from the list.

    This method should only be called inside remove_source.
    Reference must be present in the list or an error will occur.
    """
    self._references.remove(obj)

  def remove_source(self, source: TGridItem) -> None:
    """Removes a source from the list.

    Source must be present in the list or an error will occur.
    Also calls drop_reference of the source.
    """
    self._sources.remove(source)
    source.drop_reference(self)

  def get_sources(self) -> List[TGridItem]:
    """Returns a list of all sources of the instance.

    Child objects (if any) are not considered for this method.
    """
    return self._sources[:]

  def get_references(self):
    """Returns a list of all references of the instance.

    Child objects (if any) are not considered for this method.
    """
    return self._references[:]

  def relink_to_physical_ancestors(self) -> None:
    """Will recalculate instance's sources to make them physical."""
    pass

  # SERIALIZING
  @property
  def needs_serializing(self) -> bool:
    """A property that determines if the object needs to be serialized.

    Is true by default, but allows classes to set a condition to prevent this
    object from being included in the serialization. For example, Container
    class overwrites this to return False for empty objects.
    """
    return True

  @abc.abstractmethod
  def _get_serializing_id(self) -> str:
    """Abstract method that will return id for serializing."""
    pass

  @abc.abstractmethod
  def _get_label(self) -> str:
    """Abstract method that will return label for serializing."""
    pass

  def _wrap_label(self) -> int:
    """Splits label into multiple lines to make sure it fits into WIDTH.

    Returns:
      int: height of the new label in pixels.
    """
    if self._serializing_params.width != 0:
      # this method might be called several times
      # but the wrapping should only be performed after width was calculated
      max_width = self._serializing_params.width // HORIZONTAL_PIXELS_PER_LETTER
      label_lines = wrap(self._serializing_params.label, width=max_width)
      self._serializing_params.label = "\n".join(label_lines)
      label_height = len(label_lines) * VERTICAL_PIXELS_PER_LINE
      return label_height
    else:
      return 0

  def _calculate_serializing_params(self) -> None:
    """Protected method for serializing params calculation.

    This method will be overwritten by child classes to calculate their params
    in a specific way. ID will always be calculated first because it might be
    used by child objects to calculate their ID.
    """
    self._serializing_params.id = self._get_serializing_id()
    self._serializing_params.label = self._get_label()
    self._serializing_params.width = self.WIDTH
    label_height = self._wrap_label()
    self._serializing_params.height = 2 * self.VERTICAL_MARGIN + label_height
    self._serializing_params.ready = True

  def calculate_serializing_params(self,
                                   parent: Optional[TContainer] = None) -> None:
    """Public method that triggers calculation of serializing params.

    Args:
      parent (Container, None): optional parameter that indicates a parent
        of the object. If provided, it will be saved in SP and query will be
        copied from it.
    """
    if parent is not None:
      self._serializing_params.parent = parent
      self._serializing_params.query = parent.serializing_params.query
    self._calculate_serializing_params()

  @property
  def serializing_params(self) -> TSerializingParams:
    """A property that will return SP if they are ready.

    If SP are not ready raises a specific error. This is done to prevent outside
    access to SP that were not calculated yet.

    Returns:
      SerializingParams: SP of this objects
    """
    if self._serializing_params.ready:
      return self._serializing_params
    else:
      raise SerializingParamsNotReady(str(self))

  def set_coordinates(self, coordinates: TCoordinates) -> None:
    """Sets objects coordinates.

    It is separated from calculate_serializing_params because the coordinates
    are often calculated after calculation of other params.

    Args:
      coordinates (Coordinates): coordinates to be assigned.
    """
    self._serializing_params.coordinates = coordinates
    self._serializing_params.coordinates.initialized = True

  def horizontal_stretch(self, new_width: int) -> None:
    """Method that allows parent to stretch GridItem horizontally."""
    new_width = max(new_width, self._serializing_params.width)
    self._serializing_params.width = new_width

  def vertical_shift(self, offset: int) -> None:
    """Method that allows parent to shift GridItem vertically up or down."""
    self._serializing_params.coordinates.y += offset

  def add_connection(self, source: TGridItem) -> None:
    """Adds a new connection to the connections list.

    Args:
      source (GridItem): source GridItem object
    """
    self._serializing_params.connections.append(Connection(source, self))
    self._serializing_params.has_inbound_connection = True
    source.acknowledge_outbound_connection()

  def acknowledge_outbound_connection(self):
    """Method that sets has_outbound_connection to True.

    Method that is called by target's add_connection. This information is later
    used to determine node_type.
    """
    self._serializing_params.has_outbound_connection = True
