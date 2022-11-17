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

import functools
import json
import sys
import traceback

from flask import jsonify

import sql_graph
from backend.bq import BackendBQClient
from backend.validation import ValidationError


def _convert_bq_obj_to_rf(obj):
  data_dict = json.loads(obj["object_data"])
  converted_obj = {
    "id": obj["object_id"],
    "parentNode": obj["parent_object_id"],
    "data": {
      "pythonType": obj["object_type"],
      "domain": obj["subject_area"],
      "target_table": obj["target_table_name"],
    },
    "position": {
      "x": data_dict["coordinates"][0],
      "y": data_dict["coordinates"][1],
    },
    "style": {
      "width": data_dict["width"],
      "height": data_dict["height"],
    }
  }

  del data_dict["coordinates"]
  del data_dict["width"]
  del data_dict["height"]
  converted_obj["data"].update(data_dict)

  if obj["parent_object_id"] is None:
    del converted_obj["parentNode"]
  return converted_obj


def _convert_bq_conn_to_rf(conn):
  converted_conn = {
    "id": conn["source_object_id"] + "-" + conn["target_object_id"],
    "source": conn["source_object_id"],
    "target": conn["target_object_id"],
    "data": json.loads(conn["connection_data"]),
  }
  return converted_conn


def _sort_by_type(objects):
  def comp(t):
    if t.lower().endswith("table"):
      return 0
    elif t.lower().endswith("container"):
      return 1
    else:
      return 2

  objects.sort(key=lambda o: comp(o["data"]["pythonType"]))
  return objects


def bq_workflow(gcp_project, object_query_function, connection_query_function,
                query_args):
  client = BackendBQClient(gcp_project)
  objects = getattr(client, object_query_function)(**query_args)
  connections = getattr(client, connection_query_function)(**query_args)
  return {
    "objects": _sort_by_type([_convert_bq_obj_to_rf(obj) for obj in objects]),
    "connections": [_convert_bq_conn_to_rf(conn) for conn in connections],
  }


def on_demand_workflow(gcp_project, datetime, domains, physical):
  loader = sql_graph.GrizzlyLoader(gcp_project, datetime)
  graph = sql_graph.Graph(loader.filter_queries_by_domain_list(domains))
  serializer = sql_graph.ReactFlowSerializer(graph, physical)
  return serializer.serialize()


def response_code_handler(func):
  @functools.wraps(func)
  def wrapper():
    try:
      return jsonify(func()), 200
    except ValidationError as e:
      return str(e), 400
    except sql_graph.exceptions.ParsingError as e:
      tb = traceback.format_exc()
      print(tb, file=sys.stderr)
      return "An error has occurred during parsing: " + str(e), 500
    except Exception as e:
      tb = traceback.format_exc()
      print(tb, file=sys.stderr)
      return "Unexpected error has occurred: " + str(e), 500

  return wrapper
