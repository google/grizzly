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

"""Configuration of grizzly metadata api."""
import pathlib
import sys

from google.cloud import bigquery

METADATA_DATASET = 'etl_log'
STAGE_DATASET = 'etl_staging_composer'

SQL_GRAPH_MODULE_PATH = (pathlib.Path(__file__).absolute().parent.parent.parent
                         / "grizzly_data_lineage/")
sys.path.insert(0, str(SQL_GRAPH_MODULE_PATH))

DATA_LINEAGE_OBJECT_TABLE_NAME = "data_lineage_object"
DATA_LINEAGE_OBJECTS_CONNECTION_TABLE_NAME = "data_lineage_objects_connection"
DATA_LINEAGE_BUILD_TABLE_NAME = "data_lineage_build"

DATA_LINEAGE_BUILD_TABLE_SCHEMA = [
    bigquery.SchemaField('data_lineage_build_id', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('subject_area_build_id', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('build_datetime', 'STRING', mode='NULLABLE')
]

DATA_LINEAGE_OBJECT_TABLE_SCHEMA = [
    bigquery.SchemaField('data_lineage_build_id', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('data_lineage_type', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('subject_area', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('object_id', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('parent_object_id', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('object_type', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('target_table_name', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('object_data', 'STRING', mode='NULLABLE')
]

DATA_LINEAGE_OBJECTS_CONNECTION_TABLE_SCHEMA = [
    bigquery.SchemaField('data_lineage_build_id', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('data_lineage_type', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('subject_area', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('source_object_id', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('target_object_id', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('connection_data', 'STRING', mode='NULLABLE')
]

SQL_MERGE_TMP_TO_DATA_LINEAGE_BUILD = """

insert into etl_log.data_lineage_build (
  data_lineage_build_id, 
  subject_area_build_id,
  build_datetime
)
select 
  src.id as data_lineage_build_id,
  src.commit_sha as subject_area_build_id,
  substr(src.start_time,0,19) as build_datetime
from `etl_log.cb_trigger_execution` as src
where src.id = '{build_id}'
"""

SQL_INSERT_TMP_TO_DATA_LINEAGE_OBJECT = """
insert into `{table}` 
 (
    data_lineage_build_id, 
    data_lineage_type,
    subject_area,
    object_id, 
    parent_object_id,
    object_type, 
    target_table_name,
    object_data
 )
select 
    data_lineage_build_id, 
    data_lineage_type,
    subject_area,
    object_id, 
    parent_object_id,
    object_type, 
    target_table_name,
    object_data
from {temp_table}
"""

SQL_INSERT_TMP_TO_DATA_LINEAGE_OBJECTS_CONNECTION = """
insert into `{table}` 
 (
    data_lineage_build_id, 
    data_lineage_type,
    subject_area,
    source_object_id, 
    target_object_id,
    connection_data
 )
select 
    data_lineage_build_id, 
    data_lineage_type,
    subject_area,
    source_object_id, 
    target_object_id,
    connection_data
from {temp_table}
"""