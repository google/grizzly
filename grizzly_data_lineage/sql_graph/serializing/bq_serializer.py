"""BQ serializer class."""
import json

from sql_graph.serializing.serializer import Serializer
from sql_graph.typing import JsonDict


class BQSerializer(Serializer):
  """Implementation of the Serializer for BQ."""

  def serialize(self) -> JsonDict:
    """Overwrite of the serialize method."""

    objects = [{
      "object_id": obj.serializing_params.id,
      "parent_object_id": obj.serializing_params.get_parent_id(),
      "object_type": obj.serializing_params.python_type,
      "final_target_table_name": obj.serializing_params.query.target_table,
      "subject_area": obj.serializing_params.query.domain.upper(),
      "object_data": json.dumps({
        **obj.serializing_params.data,
        **{
          "coordinates": obj.serializing_params.coordinates.to_tuple(),
          "height": obj.serializing_params.height,
          "width": obj.serializing_params.width,
          "label": obj.serializing_params.label,
          "hasInboundConnection": obj.serializing_params.has_inbound_connection,
          "hasOutboundConnection":
            obj.serializing_params.has_outbound_connection,
        }
      }),
    } for obj in self._get_object_list()]
    connections = [{
      "source_object_id": connection.source.serializing_params.id,
      "target_object_id": connection.target.serializing_params.id,
      "subject_area": connection.target.serializing_params.query.domain.upper(),
      "connection_data": json.dumps(connection.data),
    } for connection in self._get_connection_list()]
    return {"objects": objects, "connections": connections}
