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

from typing import Optional, List

import google.auth.transport.requests
from google.cloud import bigquery


class BackendBQClient:
  BUILD_DATETIMES_QUERY = """
    SELECT DISTINCT dlb.dt_build_datetime
    FROM `etl_log.vw_data_lineage_build` AS dlb
    ORDER BY dlb.dt_build_datetime DESC
    """
  DL_BUILD_ID_QUERY = """
    SELECT data_lineage_build_id
    FROM `etl_log.vw_data_lineage_build` AS dlb
    WHERE dlb.dt_build_datetime = DATETIME ('{datetime}')
    """

  DOMAINS_QUERY = """
    SELECT DISTINCT dlo.subject_area
    FROM `etl_log.vw_data_lineage_object` AS dlo
    WHERE dlo.data_lineage_build_id = '{build_id}'
    ORDER BY dlo.subject_area ASC
    """
  JOB_BUILD_IDS_QUERY = """
    SELECT DISTINCT target_table_name
    FROM `etl_log.vw_data_lineage_object` AS dlo
    WHERE dlo.data_lineage_build_id = '{build_id}'
    AND dlo.subject_area = '{domain}'
    ORDER BY dlo.target_table_name ASC
    """

  DLO_QUERY = """
    SELECT 
      dlo.subject_area, 
      dlo.object_id, 
      dlo.parent_object_id, 
      dlo.object_type, 
      dlo.target_table_name, 
      dlo.object_data	
    FROM `etl_log.vw_data_lineage_object` AS dlo
    WHERE dlo.data_lineage_build_id LIKE '{build_id}'
    AND dlo.subject_area LIKE '{domain}'
    AND dlo.target_table_name LIKE '{job_build_id}'
    AND dlo.data_lineage_type = '{dl_type}'
    """
  DLC_QUERY = """
    SELECT 
      dlc.subject_area, 
      dlc.source_object_id, 
      dlc.target_object_id, 
      dlc.connection_data
    FROM `etl_log.vw_data_lineage_objects_connection` AS dlc
    WHERE dlc.data_lineage_build_id LIKE '{build_id}'
    AND dlc.subject_area LIKE '{domain}'
    AND dlc.target_table_name LIKE '{job_build_id}'
    AND dlc.data_lineage_type = '{dl_type}'
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

  def get_build_datetimes(self) -> List[str]:
    query = self.BUILD_DATETIMES_QUERY.format()
    result = self.client.query(query).result()
    return [r[0] for r in result]

  def get_build_id_by_datetime(self, datetime: str) -> List[str]:
    query = self.DL_BUILD_ID_QUERY.format(datetime=datetime)
    result = self.client.query(query).result()
    return [r[0] for r in result][0]

  def get_domains(self, datetime: str) -> List[str]:
    build_id = self.get_build_id_by_datetime(datetime)
    query = self.DOMAINS_QUERY.format(build_id=build_id)
    result = self.client.query(query).result()
    return [r[0] for r in result]

  def get_job_build_ids(self, datetime: str, domain: str) -> List[str]:
    build_id = self.get_build_id_by_datetime(datetime)
    query = self.JOB_BUILD_IDS_QUERY.format(build_id=build_id,
                                            domain=domain)
    result = self.client.query(query).result()
    return [r[0] for r in result]

  def get_project_level_objects(self, datetime: str) -> List:
    build_id = self.get_build_id_by_datetime(datetime)
    query = self.DLO_QUERY.format(
      build_id=build_id,
      domain="%",
      job_build_id="%",
      dl_type="PROJECT_LEVEL",
    )
    return list(self.client.query(query).result())

  def get_project_level_connections(self, datetime: str) -> List:
    build_id = self.get_build_id_by_datetime(datetime)
    query = self.DLC_QUERY.format(
      build_id=build_id,
      domain="%",
      job_build_id="%",
      dl_type="PROJECT_LEVEL",
    )
    return list(self.client.query(query).result())

  def get_domain_level_objects(self, datetime: str, domain: str) -> List:
    build_id = self.get_build_id_by_datetime(datetime)
    query = self.DLO_QUERY.format(
      build_id=build_id,
      domain=domain,
      job_build_id="%",
      dl_type="DOMAIN_LEVEL",
    )
    return list(self.client.query(query).result())

  def get_domain_level_connections(self, datetime: str, domain: str) -> List:
    build_id = self.get_build_id_by_datetime(datetime)
    query = self.DLC_QUERY.format(
      build_id=build_id,
      domain=domain,
      job_build_id="%",
      dl_type="DOMAIN_LEVEL",
    )
    return list(self.client.query(query).result())

  def get_query_level_objects(self, datetime: str, domain: str,
                              job_build_id: str) -> List:
    build_id = self.get_build_id_by_datetime(datetime)
    query = self.DLO_QUERY.format(
      build_id=build_id,
      domain=domain,
      job_build_id=job_build_id,
      dl_type="QUERY_LEVEL",
    )
    return list(self.client.query(query).result())

  def get_query_level_connections(self, datetime: str, domain: str,
                                  job_build_id: str) -> List:
    build_id = self.get_build_id_by_datetime(datetime)
    query = self.DLC_QUERY.format(
      build_id=build_id,
      domain=domain,
      job_build_id=job_build_id,
      dl_type="QUERY_LEVEL",
    )
    return list(self.client.query(query).result())
