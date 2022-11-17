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

import abc
from typing import Any
from typing import List
from typing import Optional

from sql_graph.exceptions import ParsingError
from sql_graph.exceptions import SerializingParamsNotReady
from sql_graph.parsing.primitives import Connection
from sql_graph.parsing.primitives import SerializingParams
from sql_graph.parsing.primitives.settings import DESC_LETTER_WIDTH
from sql_graph.parsing.primitives.settings import DESC_LINE_HEIGHT
from sql_graph.parsing.primitives.settings import LABEL_LETTER_WIDTH
from sql_graph.parsing.primitives.settings import LABEL_LINE_HEIGHT
from sql_graph.parsing.primitives.settings import MAX_DESC_LINES
from sql_graph.parsing.utils.misc_utils import wrap_text
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
    MAX_WIDTH_PENALTY (int): increasing this parameter will decrease the
      available horizontal space for labels to allow other elements to be
      displayed, such as table drag handle.

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
  MAX_WIDTH_PENALTY = 0

  def __init__(self) -> None:
    self._sources: List[TGridItem] = []
    self._references: List[TGridItem] = []
    self._serializing_params = SerializingParams(self)

  # SOURCE MANAGEMENT
  def register_reference(self, reference: TGridItem) -> None:
    """Adds a reference to the list.

    This method should only be called inside add_source.
    """
    if reference not in self._references:
      self._references.append(reference)

  def add_source(self, source: TGridItem) -> None:
    """Adds a source to the list.

    Also calls register_reference of the source.
    """
    if source not in self._sources:
      self._sources.append(source)
      source.register_reference(self)

  def drop_reference(self, reference: TGridItem) -> None:
    """Removes a reference from the list.

    This method should only be called inside remove_source.
    Reference must be present in the list or an error will occur.
    """
    try:
      self._references.remove(reference)
    except ValueError as e:
      raise ParsingError(f"Attempted to remove reference {reference}, which "
                         f"was never added as a reference") from e

  def remove_source(self, source: TGridItem) -> None:
    """Removes a source from the list.

    Source must be present in the list or an error will occur.
    Also calls drop_reference of the source.
    """
    try:
      self._sources.remove(source)
      source.drop_reference(self)
    except ValueError as e:
      raise ParsingError(f"Attempted to remove source {source}, which was "
                         f"never added as a source") from e

  def replace_sources(self, new_sources: List[TGridItem]) -> None:
    for source in self._sources[:]:
      self.remove_source(source)
    for source in new_sources:
      self.add_source(source)

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
      wrapped_label, text_height = wrap_text(
        text=self._serializing_params.label,
        text_width=self._serializing_params.width - self.MAX_WIDTH_PENALTY,
        letter_width=LABEL_LETTER_WIDTH,
        line_height=LABEL_LINE_HEIGHT
      )
      self._serializing_params.label = wrapped_label
      if "description" in self._serializing_params.data:
        # add space to display description
        self._serializing_params.data["description"] = \
          self._serializing_params.data["description"].replace("\n", " ")
        wrapped_desc, desc_height = wrap_text(
          text=self._serializing_params.data["description"],
          text_width=self._serializing_params.width,
          letter_width=DESC_LETTER_WIDTH,
          line_height=DESC_LINE_HEIGHT,
          max_lines=MAX_DESC_LINES
        )
        if self.MAX_WIDTH_PENALTY > 0:
          text_height += DESC_LINE_HEIGHT
        self._serializing_params.data["truncated_description"] = wrapped_desc
        text_height += desc_height
      return text_height
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

  def add_data(self, key: str, value: Any) -> None:
    """Adds a key/value pair to the data dict of GridItem.

    Args:
      key: key of the pair.
      value: value of the pair.
    """
    if key in self._serializing_params.data:
      raise ParsingError(f"Cannot override {key} in GridItem's data dict.")
    else:
      self._serializing_params.data[key] = value
