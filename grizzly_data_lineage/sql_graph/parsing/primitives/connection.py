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
