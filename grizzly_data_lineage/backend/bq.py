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

  PHYSICAL_DLO_QUERY = """
    SELECT dlo.subject_area, dlo.object_id, dlo.parent_object_id, dlo.object_type, dlo.target_table_name, dlo.object_data	
    FROM `etl_log.vw_data_lineage_object` AS dlo
    WHERE dlo.data_lineage_build_id = '{build_id}'
    AND dlo.data_lineage_type = 'PHYSICAL'
    """
  PHYSICAL_DLC_QUERY = """
    SELECT dlc.subject_area, dlc.source_object_id, dlc.target_object_id, dlc.connection_data
    FROM `etl_log.vw_data_lineage_objects_connection` AS dlc
    WHERE dlc.data_lineage_build_id = '{build_id}'
    AND dlc.data_lineage_type = 'PHYSICAL'
    """

  QUERY_LEVEL_DLO_QUERY = """
    SELECT dlo.subject_area, dlo.object_id, dlo.parent_object_id, dlo.object_type, dlo.target_table_name, dlo.object_data	
    FROM `etl_log.vw_data_lineage_object` AS dlo
    WHERE dlo.data_lineage_build_id = '{build_id}'
    AND dlo.subject_area = '{domain}'
    AND dlo.target_table_name = '{job_build_id}'
    AND dlo.data_lineage_type = 'QUERY_LEVEL'
    """
  QUERY_LEVEL_DLC_QUERY = """
    SELECT dlc.subject_area, dlc.source_object_id, dlc.target_object_id, dlc.connection_data
    FROM `etl_log.vw_data_lineage_objects_connection` AS dlc
    WHERE dlc.data_lineage_build_id = '{build_id}'
    AND dlc.subject_area = '{domain}'
    AND dlc.target_table_name = '{job_build_id}'
    AND dlc.data_lineage_type = 'QUERY_LEVEL'
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

  def get_physical_objects(self, datetime: str) -> List:
    build_id = self.get_build_id_by_datetime(datetime)
    query = self.PHYSICAL_DLO_QUERY.format(build_id=build_id)
    return list(self.client.query(query).result())

  def get_physical_connections(self, datetime: str) -> List:
    build_id = self.get_build_id_by_datetime(datetime)
    query = self.PHYSICAL_DLC_QUERY.format(build_id=build_id)
    return list(self.client.query(query).result())

  def get_query_level_objects(self, datetime: str, domain: str,
                              job_build_id: str) -> List:
    build_id = self.get_build_id_by_datetime(datetime)
    query = self.QUERY_LEVEL_DLO_QUERY.format(
      build_id=build_id,
      domain=domain,
      job_build_id=job_build_id
    )
    return list(self.client.query(query).result())

  def get_query_level_connections(self, datetime: str, domain: str,
                                  job_build_id: str) -> List:
    build_id = self.get_build_id_by_datetime(datetime)
    query = self.QUERY_LEVEL_DLC_QUERY.format(
      build_id=build_id,
      domain=domain,
      job_build_id=job_build_id
    )
    return list(self.client.query(query).result())
