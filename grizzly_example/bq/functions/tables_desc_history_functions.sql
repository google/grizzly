-- Copyright 2022 Google LLC
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--      http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

CREATE OR REPLACE TABLE FUNCTION `etl_log.fn_get_information_schema_column_field_paths`
 (v_datetime DATETIME, v_table_schema STRING, v_table_name STRING) AS (
  with max_datetime as (SELECT 
    max(metadata_datetime) as metadata_datetime,
    table_name,
    table_schema
  FROM `etl_log.grizzly_information_schema_column_field_paths` 
  where table_schema like v_table_schema
  and table_name like v_table_name
  and metadata_datetime <= v_datetime
  group by table_schema, table_name
  )
  select 
  src_data.*
  from 
  `etl_log.grizzly_information_schema_column_field_paths` src_data
  join max_datetime md on 
        src_data.metadata_datetime = md.metadata_datetime
        and src_data.table_schema = md.table_schema
        and src_data.table_name = md.table_name
);

CREATE OR REPLACE TABLE FUNCTION `etl_log.fn_get_information_schema_table_options` 
 (v_datetime DATETIME, v_table_schema STRING, v_table_name STRING) AS (
  with max_datetime as (SELECT 
    max(metadata_datetime) as metadata_datetime,
    table_schema,
    table_name
  FROM `etl_log.grizzly_information_schema_table_options` 
  where table_schema like v_table_schema
  and table_name like v_table_name
  and metadata_datetime <= v_datetime
  group by table_schema, table_name
  )
  select 
  src_data.*
  from 
  `etl_log.grizzly_information_schema_table_options` src_data
  join max_datetime md on 
        src_data.metadata_datetime = md.metadata_datetime
        and src_data.table_schema = md.table_schema
        and src_data.table_name = md.table_name
);
