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

"""Build data lineage build.

Example: 
  python3 ./deploy_data_lineage.py -p <GCP_ENVIRONMENT>
"""

import argparse
import sys
import datetime

from deployment_utils import BQUtils
from load_data_lineage import LoadDomainLevelDataLineage

from load_data_lineage import LoadProjectLevelDataLineage
from load_data_lineage import LoadQueryLevelDataLineage
from load_data_lineage import LoadDataLineageBuild
from sql_graph import GrizzlyLoader


def main(args: argparse.Namespace):
  """Implement the command line interface described in the module doc string."""

  bq_utils = BQUtils(gcp_project_id=args.gcp_project_id)
  LoadDataLineageBuild.create_dl_build_log_table(bq_utils)
  results = bq_utils.bq_client.query(
    query="select * from etl_log.vw_build_data_lineage_queue").result()

  for row in results:
    print(f"Build_id={row.build_id}: build_datetime={row.build_datetime}")
    print(f"Started data loading at {datetime.datetime.now()}")
    loader = GrizzlyLoader(
      gcp_project=args.gcp_project_id,
      datetime=row.dt_build_datetime,
    )
    print(f"Finished data loading at {datetime.datetime.now()}")

    print("Calculating Query-Level Data Lineage")
    LoadQueryLevelDataLineage(
        bq_utils=bq_utils,
        loader=loader,
        build_id=row.build_id,
        build_datetime=row.dt_build_datetime,
    ).load_data()

    print("Calculating Domain-Level Data Lineage")
    LoadDomainLevelDataLineage(
        bq_utils=bq_utils,
        loader=loader,
        build_id=row.build_id,
        build_datetime=row.dt_build_datetime,
    ).load_data()

    print("Calculating Project-Level Data Lineage")
    LoadProjectLevelDataLineage(
        bq_utils=bq_utils,
        loader=loader,
        build_id=row.build_id,
        build_datetime=row.dt_build_datetime,
    ).load_data()

    print("Writing Data Lineage build info to BQ")
    LoadDataLineageBuild(
        bq_utils=bq_utils,
        build_id=row.build_id
    ).load_data()

    print("=" * 20 + "\n")


if __name__ == "__main__":

  try:
    # Construct the argument parser
    ap = argparse.ArgumentParser(
        description="Script used for "
                    "Deploy data lineage."
    )

    # Add the arguments to the parser
    ap.add_argument(
        "-p",
        "--project",
        dest="gcp_project_id",
        required=True,
        help="Target GCP project")

    arguments = ap.parse_args()
    main(args=arguments)
  except:
    print("Unexpected error:", sys.exc_info()[1])
    raise
