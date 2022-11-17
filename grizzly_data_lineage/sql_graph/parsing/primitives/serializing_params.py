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

"""Serializing params class."""
from typing import List
from typing import Optional

from sql_graph.parsing.primitives import Coordinates
from sql_graph.typing import TConnection
from sql_graph.typing import TGridItem


class SerializingParams:
  """Serializing parameters that are attached GridItem object.

  Attributes:
    ready (bool): whether SP were calculated. Initially is false. Used by owner
      object to prevent access to these params if they weren't calculated yet.
    id (str): id of the owner object.
    python_type (str): python type name of the owner object.
    parent (Container): parent of the owner object.
    query (Query): query from where owner object was parsed.
    coordinates (Coordinates): coordinates used for visualization.
    width (int): visualization width of the parent object.
    height (int): visualization height of the parent object.
    label (str): text label used for visualization.
    data (JsonDict): any other object data. Will be used by ReactFlow.
    connections (List[Connection]): list of the owner object connections.
    has_inbound_connection (bool): whether the object has an inbound connection.
    has_outbound_connection (bool): whether the object has an outbound
      connection.
  """

  def __init__(self, owner: TGridItem):
    self.ready = False

    self.id = None
    self.python_type = owner.__class__.__name__
    self.parent = None
    self.query = None

    self.coordinates = Coordinates(initialized=False)
    self.width = 0
    self.height = 0
    self.label = None
    self.data = {}

    self.connections: List[TConnection] = []
    self.has_inbound_connection = False
    self.has_outbound_connection = False

  def get_parent_id(self) -> Optional[str]:
    """Retrieves parent ID string of the owner object.

    Returns:
      (str, None): parent ID or none if object has no parent.
    """
    if self.parent is not None:
      return self.parent.serializing_params.id
    else:
      return None
