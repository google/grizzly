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
