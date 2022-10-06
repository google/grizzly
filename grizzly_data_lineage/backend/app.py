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


# @app.route("/parse_grizzly_on_demand", methods=["GET"])
# @response_code_handler
# def parse_grizzly_on_demand():
#   args = validate_parse_grizzly_on_demand_args()
#   return on_demand_workflow(**args)
#
#
# @app.route("/parse_test", methods=["GET"])
# @response_code_handler
# def parse_test():
#   return parse_test_queries()


if __name__ == '__main__':
  app.run(use_reloader=True, port=5000, threaded=True)
