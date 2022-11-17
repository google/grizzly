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

"""Reactflow serializer class"""
from sql_graph.serializing.serializer import Serializer
from sql_graph.typing import JsonDict


class ReactFlowSerializer(Serializer):
  """Implementation of the Serializer for ReactFlow."""

  def serialize(self) -> JsonDict:
    """Overwrite of the serialize method."""
    objects = [{
      "id": obj.serializing_params.id,
      "parentNode": obj.serializing_params.get_parent_id(),
      "data": {
        **obj.serializing_params.data,
        **{
          "label": obj.serializing_params.label,
          "pythonType": obj.serializing_params.python_type,
          "hasInboundConnection": obj.serializing_params.has_inbound_connection,
          "hasOutboundConnection":
            obj.serializing_params.has_outbound_connection,
          "domain": obj.serializing_params.query.domain,
          "target_table": obj.serializing_params.query.target_table,
        }
      },
      "position": {
        "x": obj.serializing_params.coordinates.x,
        "y": obj.serializing_params.coordinates.y,
      },
      "style": {
        "width": obj.serializing_params.width,
        "height": obj.serializing_params.height,
      }
    } for obj in self._get_object_list()]
    for obj in objects:
      if obj["parentNode"] is None:
        del obj["parentNode"]
    connections = [{
      "id": (conn.source.serializing_params.id + "-" +
             conn.target.serializing_params.id),
      "source": conn.source.serializing_params.id,
      "target": conn.target.serializing_params.id,
      "data": conn.data,
    } for conn in self._get_connection_list()]
    return {"objects": objects, "connections": connections}
