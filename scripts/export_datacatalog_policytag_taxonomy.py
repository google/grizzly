# Copyright 2021 Google LLC
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
"""Export DataCatalog Taxonomy templates.

  Script used by Cloud Build to export DataCatalog Taxonomy templates as proto
  files.

  Typical usage example:

  python3 ./export_datacatalog_policytag_taxonomy.py
    -p <GCP_PROJECT_ID>
    -l <GCP_COMPOSER_LOCATION>
    -s <EXPORT_FOLDER>
"""

import argparse
import sys

import grizzly.cloud_platform_auth as Auth
from grizzly.serializer.datacatalog_policytag_taxonomy import DataCatalogPolicyTagTaxonomy


def main(args: argparse.Namespace) -> None:
  """Export Data Catalog taxonomy templates.

  Exports Data Catalog taxonomy templates as protos to a repository.
  Script used for sync of Data Catalog taxonomy templates.

  Args:
    args (argparse.Namespace): Input arguments
  """
  Auth.auth()
  # Get a set of locations. As GCP composer and BQ could operates only with
  # DataCatalog taxonomies in a same location. We will get location from
  # GCP Composer environment plus multiregion locations.
  # For example: [us-central1, us]
  locations = {
      args.gcp_location,
      args.gcp_location.split('-')[0]
  }
  # Iterate Data Catalog Taxonomy locations
  for l in locations:
    taxonomies_on_location = DataCatalogPolicyTagTaxonomy(
        gcp_project_id=args.gcp_project_id,
        location=l)
    print('Exporting taxonomies for: ', taxonomies_on_location.parent_resource)
    taxonomies_on_location.export_taxonomy(template_path=args.source_path)
  return

if __name__ == '__main__':
  try:
    # Construct the argument parser
    ap = argparse.ArgumentParser(description=__doc__)
    # Add the arguments to the parser
    ap.add_argument(
        '-p', '--project',
        dest='gcp_project_id', required=True,
        help='Target GCP project')
    ap.add_argument(
        '-l', '--location',
        dest='gcp_location', required=True,
        help='GCP Composer environment location.')
    ap.add_argument(
        '-s', '--source_path',
        dest='source_path', required=True,
        help='Destination directory for templates.')
    args_values = ap.parse_args()
    main(args=args_values)
  except:
    print('Unexpected error:', sys.exc_info()[1])
    raise
