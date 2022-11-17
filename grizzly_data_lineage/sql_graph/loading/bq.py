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

"""Grizzly BQ client.

Example usage:
client = GrizzlyBQClient(project)
client.get_build_files(subject_area)
"""

from typing import Optional, List
from datetime import datetime as DateTime

import google.auth.transport.requests
from google.cloud import bigquery


class GrizzlyBQClient:
  """
  Grizzly BQ client. Can perform queries specific for SQL parser.

  Class Attributes:
    BUILD_DATETIMES_QUERY: query to get all datetimes at which builds
      have occurred.
    DOMAINS_QUERY: query to get all domains in a project at the specific
      datetime.
    JOB_BUILD_IDS_QUERY: query to get all job_build_ids in the domain at
      the specific datetime.
    ALL_BUILD_FILES_ON_DATETIME_QUERY: get all build files at the
      specific datetime.
    AUTH_SCOPES: default auth scopes.
  Attributes:
    project: gcp project name.
    _auth_scopes: auth scopes of the instance.
    client: BQ client.
  """

  BUILD_DATETIMES_QUERY = """
  SELECT DISTINCT from_build_datetime
  FROM etl_log.vw_subject_areas_build AS b
  ORDER BY b.from_build_datetime DESC
  """
  DOMAINS_QUERY = """
  SELECT DISTINCT b.subject_area
  FROM etl_log.fn_get_subject_area_build_sql_files(
    datetime ('{datetime}')) AS b
  ORDER BY b.subject_area ASC
  """
  JOB_BUILD_IDS_QUERY = """
  SELECT DISTINCT b.job_build_id
  FROM etl_log.fn_get_subject_area_build_sql_files(
    datetime ('{datetime}')) AS b
  WHERE b.subject_area = '{domain}'
  ORDER BY b.job_build_id ASC
  """
  BUILD_FILES_ON_DATETIME_QUERY = """
  SELECT b.job_build_id, b.subject_area, b.file_path, b.file_value
  FROM etl_log.fn_get_subject_area_build_sql_files(
    datetime ('{datetime}')) AS b
  """

  AUTH_SCOPES = ["https://www.googleapis.com/auth/cloud-platform"]

  def _auth(self) -> None:
    """Perform authentication using default credentials."""
    credentials, _ = google.auth.default(scopes=self._auth_scopes)
    auth_req = google.auth.transport.requests.Request()
    credentials.refresh(auth_req)

  def __init__(self, project, auth_scopes: Optional[List] = None) -> None:
    self.project = project
    self._auth_scopes = auth_scopes or self.AUTH_SCOPES
    self._auth()
    self.client = bigquery.Client(project=project)

  def get_build_datetimes(self) -> List[DateTime]:
    """Method that gets all build datetimes.

    Datetimes are sorted in descending order.

    Returns:
      List[DateTime]: list of build datetimes.
    """
    query = self.BUILD_DATETIMES_QUERY.format()
    result = self.client.query(query).result()
    return [r[0] for r in result]

  def get_domains(self, datetime: str) -> List[str]:
    """Method that gets all domain names at datetime.

    Args:
      datetime (str): datetime of the build to look at.
    Returns:
      List[str]: list of domain names.
    """
    query = self.DOMAINS_QUERY.format(datetime=datetime)
    result = self.client.query(query).result()
    return [r[0] for r in result]

  def get_job_build_ids(self, datetime: str, domain: str) -> List[str]:
    """Method that gets all job build ids from domain.

    Args:
      datetime (str): datetime string.
      domain (str): domain name.
    Returns:
      List[str]: list of job build ids.
    """
    query = self.JOB_BUILD_IDS_QUERY.format(datetime=datetime, domain=domain)
    result = self.client.query(query).result()
    return [r[0] for r in result]

  def get_build_files(self, datetime: str) -> List:
    """Method that gets all build files for a particular datetime.

    Args:
      datetime (str): datetime string.
    Returns:
      List[str]: list of build files and info about them.
    """
    query = self.BUILD_FILES_ON_DATETIME_QUERY.format(datetime=datetime)
    return list(self.client.query(query).result())
