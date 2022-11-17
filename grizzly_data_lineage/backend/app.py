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

import os
import pathlib

from flask import Flask
from flask import send_from_directory

from backend.bq import BackendBQClient
from backend.utils import bq_workflow
from backend.utils import response_code_handler
from backend.validation import format_datetime
from backend.validation import get_check_not_empty
from backend.validation import validate_parse_grizzly_domain_args
from backend.validation import validate_parse_grizzly_project_args
from backend.validation import validate_parse_grizzly_query_args

project_root = pathlib.Path(__file__).parent.parent.absolute()
app = Flask(__name__, static_folder=(project_root / "frontend/build"))


@app.route("/", defaults={"path": ""})
@app.route("/<path:path>")
def serve(path):
  if path == "":
    return send_from_directory(app.static_folder, "index.html")
  elif path != "" and os.path.exists(app.static_folder + "/" + path):
    return send_from_directory(app.static_folder, path)
  else:
    return "Page not found", 404


@app.route("/get_projects", methods=["GET"])
@response_code_handler
def get_projects():
  return os.environ.get("GRIZZLY_DATA_LINEAGE_PROJECTS").split(",")


@app.route("/get_build_datetimes", methods=["GET"])
@response_code_handler
def get_build_datetimes():
  gcp_project = get_check_not_empty("project")
  client = BackendBQClient(gcp_project)
  return [str(t) for t in client.get_build_datetimes()]


@app.route("/get_domains", methods=["GET"])
@response_code_handler
def get_domains():
  gcp_project = get_check_not_empty("project")
  datetime = format_datetime(get_check_not_empty("datetime"))
  client = BackendBQClient(gcp_project)
  return client.get_domains(datetime)


@app.route("/get_job_build_ids", methods=["GET"])
@response_code_handler
def get_job_build_ids():
  gcp_project = get_check_not_empty("project")
  datetime = format_datetime(get_check_not_empty("datetime"))
  domain = get_check_not_empty("domain")
  client = BackendBQClient(gcp_project)
  return client.get_job_build_ids(datetime, domain)


@app.route("/parse_grizzly_project", methods=["GET"])
@response_code_handler
def parse_grizzly_project():
  args = validate_parse_grizzly_project_args()
  return bq_workflow(**args)


@app.route("/parse_grizzly_domain", methods=["GET"])
@response_code_handler
def parse_grizzly_domain():
  args = validate_parse_grizzly_domain_args()
  return bq_workflow(**args)


@app.route("/parse_grizzly_query", methods=["GET"])
@response_code_handler
def parse_grizzly_query():
  args = validate_parse_grizzly_query_args()
  return bq_workflow(**args)


# URLS below available only in debug
if app.debug:
  @app.route("/parse_grizzly_on_demand", methods=["GET"])
  @response_code_handler
  def parse_grizzly_on_demand():
    from backend.validation import validate_parse_grizzly_on_demand_args
    args = validate_parse_grizzly_on_demand_args()
    from backend.utils import on_demand_workflow
    return on_demand_workflow(**args)


  # @app.route("/get_tests", methods=["GET"])
  # @response_code_handler
  # def get_tests():
  #   from backend.testing import TestRunner
  #   return TestRunner.get_tests()
  #
  #
  # @app.route("/parse_test", methods=["GET"])
  # @response_code_handler
  # def parse_test():
  #   from backend.validation import validate_parse_grizzly_test_args
  #   from backend.testing import TestRunner
  #   args = validate_parse_grizzly_test_args()
  #   return TestRunner(**args).run()


@app.route("/debug", methods=["GET"])
def get_debug_status():
  return str(app.debug)


if __name__ == '__main__':
  app.run(use_reloader=True, port=5000, threaded=True)
