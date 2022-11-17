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

import abc

from deployment_utils import BQUtils
from grizzly_metadata.config import DATA_LINEAGE_OBJECT_TABLE_NAME
from grizzly_metadata.config import DATA_LINEAGE_OBJECTS_CONNECTION_TABLE_NAME
from grizzly_metadata.config import DATA_LINEAGE_OBJECTS_CONNECTION_TABLE_SCHEMA
from grizzly_metadata.config import DATA_LINEAGE_OBJECT_TABLE_SCHEMA

from grizzly_metadata.config import DATA_LINEAGE_BUILD_TABLE_NAME
from grizzly_metadata.config import DATA_LINEAGE_BUILD_TABLE_SCHEMA

from grizzly_metadata.config import SQL_MERGE_TMP_TO_DATA_LINEAGE_BUILD
from grizzly_metadata.config import METADATA_DATASET
from grizzly_metadata.config import SQL_INSERT_TMP_TO_DATA_LINEAGE_OBJECT
from grizzly_metadata.config import \
  SQL_INSERT_TMP_TO_DATA_LINEAGE_OBJECTS_CONNECTION
from grizzly_metadata.config import STAGE_DATASET

from sql_graph import BQSerializer
from sql_graph import Graph
from sql_graph import GrizzlyLoader


class AbstractLoadDataLineage(abc.ABC):

  def _create_table_set(self,
                        table_name: str,
                        table_schema: str,
                        tmp_table_schema=None):
    """Create serialized_data_lineage table."""
    return self.bq_utils.create_tables(
      table_name=f"{METADATA_DATASET}.{table_name}",
      tmp_table_name=f"{STAGE_DATASET}.{table_name}",
      table_schema=table_schema,
      tmp_table_schema=tmp_table_schema
    )

  def _create_tables(self):
    self.objects_table, self.objects_temp_table = self._create_table_set(
      table_name=DATA_LINEAGE_OBJECT_TABLE_NAME,
      table_schema=DATA_LINEAGE_OBJECT_TABLE_SCHEMA,
      tmp_table_schema=DATA_LINEAGE_OBJECT_TABLE_SCHEMA
    )
    self.conns_table, self.conns_temp_table = self._create_table_set(
      table_name=DATA_LINEAGE_OBJECTS_CONNECTION_TABLE_NAME,
      table_schema=DATA_LINEAGE_OBJECTS_CONNECTION_TABLE_SCHEMA,
      tmp_table_schema=DATA_LINEAGE_OBJECTS_CONNECTION_TABLE_SCHEMA
    )

  def __init__(self,
               bq_utils: BQUtils,
               loader: GrizzlyLoader,
               build_id: str,
               build_datetime,
               data_lineage_type: str,
               ):
    self.bq_utils = bq_utils
    self.loader = loader

    self.data_lineage_build_id = build_id
    self.data_lineage_type = data_lineage_type
    self.build_datetime = build_datetime

    self.objects_temp_table = None
    self.objects_table = None
    self.conns_temp_table = None
    self.conns_table = None
    self._create_tables()

  def _format_object_rows(self, serialization_result):
    return [{
      "data_lineage_build_id": self.data_lineage_build_id,
      "data_lineage_type": self.data_lineage_type,
      "subject_area": r["subject_area"],
      "object_id": r["object_id"],
      "parent_object_id": r["parent_object_id"],
      "object_type": r["object_type"],
      "target_table_name": r["final_target_table_name"],
      "object_data": r["object_data"]
    } for r in serialization_result["objects"]]

  def _format_connection_rows(self, serialization_result):
    return [{
      "data_lineage_build_id": self.data_lineage_build_id,
      "data_lineage_type": self.data_lineage_type,
      "subject_area": r["subject_area"],
      "source_object_id": r["source_object_id"],
      "target_object_id": r["target_object_id"],
      "connection_data": r["connection_data"]
    } for r in serialization_result["connections"]]

  @abc.abstractmethod
  def _aggregate_result_rows(self):
    pass

  @staticmethod
  def _get_table_name(table):
    return f"{table.project}.{table.dataset_id}.{table.table_id}"

  def _load_and_merge_data(self,
                           rows,
                           table,
                           temp_table,
                           merge_sql):
    temp_table_name = self._get_table_name(temp_table)

    self.bq_utils.bq_client.insert_rows_json(temp_table_name, rows)

    merge_sql = merge_sql.format(
      table=self._get_table_name(table),
      temp_table=temp_table_name
    )

    self.bq_utils.bq_client.query(query=merge_sql).result()

  def load_data(self):
    object_rows, connection_rows = self._aggregate_result_rows()

    if object_rows:
      self._load_and_merge_data(
        rows=object_rows,
        table=self.objects_table,
        temp_table=self.objects_temp_table,
        merge_sql=SQL_INSERT_TMP_TO_DATA_LINEAGE_OBJECT
      )
    else:
      print(f"Skipping loading of object rows for {self.data_lineage_type}"
            f"at datetime {self.build_datetime}: no rows present.")

    if connection_rows:
      self._load_and_merge_data(
        rows=connection_rows,
        table=self.conns_table,
        temp_table=self.conns_temp_table,
        merge_sql=SQL_INSERT_TMP_TO_DATA_LINEAGE_OBJECTS_CONNECTION
      )
    else:
      print(f"Skipping loading of connection rows for {self.data_lineage_type} "
            f"at datetime {self.build_datetime}: no rows present.")


class LoadQueryLevelDataLineage(AbstractLoadDataLineage):

  def __init__(self,
               bq_utils: BQUtils,
               loader: GrizzlyLoader,
               build_id: str,
               build_datetime):
    super().__init__(
      bq_utils=bq_utils,
      loader=loader,
      build_id=build_id,
      build_datetime=build_datetime,
      data_lineage_type='QUERY_LEVEL'
    )

  def _load_parse_serialize_graph(self, domain, job_build_id):
    graph = Graph(self.loader.filter_queries_by_job_build_id(
      domain, job_build_id
    ))
    serializer = BQSerializer(graph, physical=False)
    return serializer.serialize()

  def _aggregate_result_rows(self):
    object_rows, connection_rows = [], []
    for domain in self.loader.get_domains():
      for job_build_id in self.loader.get_job_build_ids(domain):
        serialization_result = self._load_parse_serialize_graph(
          domain=domain,
          job_build_id=job_build_id
        )
        object_rows.extend(self._format_object_rows(serialization_result))
        connection_rows.extend(self._format_connection_rows(
          serialization_result))
    return object_rows, connection_rows


class LoadDomainLevelDataLineage(AbstractLoadDataLineage):

  def __init__(self,
               bq_utils: BQUtils,
               loader: GrizzlyLoader,
               build_id: str,
               build_datetime):
    super().__init__(
      bq_utils=bq_utils,
      loader=loader,
      build_id=build_id,
      build_datetime=build_datetime,
      data_lineage_type='DOMAIN_LEVEL'
    )

  def _load_parse_serialize_graph(self, domain):
    graph = Graph(self.loader.filter_queries_by_domain_list([domain]))
    serializer = BQSerializer(graph, physical=True)
    return serializer.serialize()

  def _aggregate_result_rows(self):
    object_rows, connection_rows = [], []
    for domain in self.loader.get_domains():
      serialization_result = self._load_parse_serialize_graph(domain=domain)
      object_rows.extend(self._format_object_rows(serialization_result))
      connection_rows.extend(self._format_connection_rows(serialization_result))
    return object_rows, connection_rows


class LoadProjectLevelDataLineage(AbstractLoadDataLineage):

  def __init__(self,
               bq_utils: BQUtils,
               loader: GrizzlyLoader,
               build_id: str,
               build_datetime):
    super().__init__(
      bq_utils=bq_utils,
      loader=loader,
      build_id=build_id,
      build_datetime=build_datetime,
      data_lineage_type='PROJECT_LEVEL'
    )

  def _load_parse_serialize_graph(self):
    graph = Graph(self.loader.get_queries())
    serializer = BQSerializer(graph, physical=True)
    return serializer.serialize()

  def _aggregate_result_rows(self):
    serialization_result = self._load_parse_serialize_graph()
    object_rows = self._format_object_rows(serialization_result)
    connection_rows = self._format_connection_rows(serialization_result)
    return object_rows, connection_rows


class LoadDataLineageBuild:

  def __init__(self,
               bq_utils: BQUtils,
               build_id: str):
    self.bq_utils = bq_utils
    self.build_id = build_id

    self.temp_table = None
    self.table = None

  @classmethod
  def create_dl_build_log_table(cls, bq_utils):
    """Create primary Data Lineage Build table.

    This method is used to create the table if it doesn't exist, so the
    build query check doesn't fail.
    """
    table_name = f"{METADATA_DATASET}.{DATA_LINEAGE_BUILD_TABLE_NAME}"
    bq_utils.create_table(
      table_name=table_name,
      table_schema=DATA_LINEAGE_BUILD_TABLE_SCHEMA
    )

  def _create_tables(self):
    """Create Data Lineage Build tables"""
    table_name = f"{METADATA_DATASET}.{DATA_LINEAGE_BUILD_TABLE_NAME}"
    table_tmp_name = f"{STAGE_DATASET}.{DATA_LINEAGE_BUILD_TABLE_NAME}"
    self.table, self.temp_table = self.bq_utils.create_tables(
      table_name=table_name,
      tmp_table_name=table_tmp_name,
      table_schema=DATA_LINEAGE_BUILD_TABLE_SCHEMA
    )

  def load_data(self):
    self._create_tables()
    self._merge_data()

  def _merge_data(self):
    sql = SQL_MERGE_TMP_TO_DATA_LINEAGE_BUILD
    sql = sql.format(build_id=self.build_id)

    self.bq_utils.bq_client.query(query=sql).result()
