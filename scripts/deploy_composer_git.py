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

"""Deploy ETL code to GCP Composer environment with git data.

Application generates deployment files in [/tmp/...] folder.
DAG py file is generated automatically on a base of ETL scope and sql files used
Once all referenced files are prepared in [/tmp/...] folder application performs
upload into correspondent [gs://.../DAG/] and [gs://.../data/ETL/<DOMAIN>/]
folder linked with GCP Composer environment.

Example:
  python3 ./deploy_composer_git.py -p <GCP_ENVIRONMENT>
    -l <GCP_COMPOSER_LOCATION>
    -c <GCP_COMPOSER_INSTANCE_NAME>
    -s <ETL_SOURCE_PATH>
    -bid <BUILD_ID>
    -x <COMMIT_SHA>
    [-l]
"""

import argparse
import pathlib
import sys

from deployment_utils import BQUtils
from google.cloud.bigquery import Table
from grizzly.composer_environment import ComposerEnvironment
from grizzly.scope import Scope
from grizzly_git.build_deployment import BuildDeploy
from grizzly_git.config import GIT_DATASET
from grizzly_git.config import JOB_BUILD_TABLE_NAME
from grizzly_git.config import JOB_BUILD_TABLE_SCHEMA
from grizzly_git.config import SQL_CREATE_VW_SUBJECT_AREA_BUILD_FILES
from grizzly_git.config import SQL_GET_BUILD_FILES
from grizzly_git.config import SQL_MERGE_TMP_TO_JOB_BUILD
from grizzly_git.config import STAGE_DATASET

from grizzly_git.config import SUBJECT_AREA_BUILD_TABLE_NAME
from grizzly_git.config import SUBJECT_AREA_BUILD_TABLE_SCHEMA

CURRENT_PATH = pathlib.Path(__file__).resolve().parent
TEMPLATE_PATH = CURRENT_PATH / "templates"


def crate_job_build_tmp_table(bq_utils: BQUtils) -> Table:
  """Create tmp table for data for job_build table."""
  table_name = f"{STAGE_DATASET}.{JOB_BUILD_TABLE_NAME}"
  ret_table: Table = bq_utils.create_temp_table(table_name,
                                                JOB_BUILD_TABLE_SCHEMA)
  return ret_table

def crate_subject_area_build_table(bq_utils: BQUtils) -> Table:
  """Create job_build table."""
  table_name = f"{GIT_DATASET}.{SUBJECT_AREA_BUILD_TABLE_NAME}"
  ret_table: Table = bq_utils.create_table(table_name, SUBJECT_AREA_BUILD_TABLE_SCHEMA)
  return ret_table

def crate_job_build_table(bq_utils: BQUtils) -> Table:
  """Create job_build table."""
  table_name = f"{GIT_DATASET}.{JOB_BUILD_TABLE_NAME}"
  ret_table: Table = bq_utils.create_table(table_name, JOB_BUILD_TABLE_SCHEMA)
  return ret_table


def merge_job_build_tmp_to_job_build(bq_utils: BQUtils,
    job_build_tmp_table_name: str) -> None:
  """Merge data between temporary and non-temporary job_build table."""

  sql = SQL_MERGE_TMP_TO_JOB_BUILD.format(GIT_DATASET=GIT_DATASET,
                                          tmp_table=job_build_tmp_table_name)
  bq_utils.bq_client.query(query=sql).result()


def get_build_files_rows(
  bq_client, 
  domain_name: str,
  commit_sha: str):
  """Return rows from the build_files table."""

  sql = SQL_GET_BUILD_FILES.format(
    GIT_DATASET=GIT_DATASET,
    subject_area=domain_name,
    commit_id=commit_sha)

  job = bq_client.query(sql)

  rows = []
  for res in job.result():
    rows.append({"file_path": res.file_path,
                 "last_commit_id": res.last_commit_id,
                 "file_cont": res.file_cont})
  print("get_build_files_rows")
  print(rows)
  return rows


def scope_to_job_build_rows(scope: Scope,
    commit_sha: str,
    build_files):
  """Return scope data to insert into job_build table."""

  ret_rows = []

  subject_area = scope.config.domain_name.lower()

  for _, task in scope.tasks.items():
    row = dict()
    row["job_build_id"] = task.task_id
    row["subject_area_build_id"] = commit_sha
    row["subject_area"] = subject_area.upper()

    row["files"] = list()

    for f in task.files:
      file = dict()
      file_path = "/".join(str(f).split("/")[2:])
      file["file_path"] = file_path

      for bf in build_files:
        if bf["file_path"] == file_path:
          file["file_commit_id"] = bf["last_commit_id"]
          file["file_value"] = bf["file_cont"]

      row["files"].append(file)

    ret_rows.append(row)

  return ret_rows


def main(args: argparse.Namespace):
  """Implement the command line interface described in the module doc string."""

  gcp_composer_environment = ComposerEnvironment(
      project_id=args.gcp_project_id,
      location=args.gcp_location,
      environment_name=args.gcp_composer_env_name)

  source_path = pathlib.Path(args.source_path)
  deployment_scope: Scope = Scope(source_path,
    project_gcp=args.gcp_project_id,
    metadata_project_gcp=args.gcp_project_metadata_id
  )
  deployment_scope.generate_staging_files()
  deployment_scope.generate_DAG_file(TEMPLATE_PATH / "dag.py.jinja2")

  gcp_composer_environment.publish_scope(deployment_scope)

  bq_utils = BQUtils(gcp_project_id=args.gcp_project_id)
  job_build_tmp_table = crate_job_build_tmp_table(bq_utils=bq_utils)
  crate_job_build_table(bq_utils=bq_utils)
  crate_subject_area_build_table(bq_utils=bq_utils)

  build_files = get_build_files_rows(
      bq_client=bq_utils.bq_client,
      domain_name=deployment_scope.config.domain_name,
      commit_sha=args.commit_sha)

  rows = scope_to_job_build_rows(scope=deployment_scope,
                                 commit_sha=args.commit_sha,
                                 build_files=build_files)

  job_build_tmp_table_name = "{project}.{dataset_id}.{table_id}".format(
      project=job_build_tmp_table.project,
      dataset_id=job_build_tmp_table.dataset_id,
      table_id=job_build_tmp_table.table_id)

  bq_utils.bq_client.insert_rows_json(job_build_tmp_table_name, rows)

  sql = SQL_CREATE_VW_SUBJECT_AREA_BUILD_FILES.format(GIT_DATASET=GIT_DATASET)
  bq_utils.bq_client.query(sql).result()

  merge_job_build_tmp_to_job_build(
      bq_utils=bq_utils,
      job_build_tmp_table_name=job_build_tmp_table_name)

  bd = BuildDeploy(project_id=args.gcp_project_id,
                   location=args.gcp_location,
                   environment_name=args.gcp_composer_env_name,
                   commit_id=args.commit_sha,
                   domain=deployment_scope.config.domain_name,
                   build_id=args.build_id)

  bd.create_build()

if __name__ == "__main__":

  try:
    # Construct the argument parser
    ap = argparse.ArgumentParser(
        description="Script used for "
                    "deploy ETL code on GCP Composer environment."
    )

    # Add the arguments to the parser
    ap.add_argument(
        "-p",
        "--project",
        dest="gcp_project_id",
        required=True,
        help="Target GCP project")
    ap.add_argument(
        "-mp",
        "--metadata_project",
        dest="gcp_project_metadata_id",
        required=True,
        help="Metadata GCP project")
    ap.add_argument(
        "-l",
        "--location",
        dest="gcp_location",
        required=True,
        help="GCP Composer environment location.")
    ap.add_argument(
        "-c",
        "--composer_environment",
        dest="gcp_composer_env_name",
        required=True,
        help="GCP Composer environment name")
    ap.add_argument(
        "-s",
        "--source_path",
        required=True,
        help="Directory with pipeline to be deployed")
    ap.add_argument(
        "-bid",
        "--build_id",
        required=True,
        help="Id of build"
    )
    ap.add_argument(
        "-x",
        "--commit_sha",
        required=True,
        dest="commit_sha",
        help="Commit SHA of the build"
    )
    ap.add_argument(
        "--local",
        required=False,
        dest="local",
        action="store_true",
        help="Generate DAG in temporary folder "
             "without deployment on environment."
    )

    arguments = ap.parse_args()
    main(args=arguments)
  except:
    print("Unexpected error:", sys.exc_info()[1])
    raise
