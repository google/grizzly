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

declare sql string;
DECLARE i INT64 DEFAULT 0;
DECLARE sql_prefix string;

set sql = ( SELECT
 STRING_AGG(
   'SELECT * FROM `region-us.INFORMATION_SCHEMA.OBJECT_PRIVILEGES` WHERE object_name = "'|| schema_name ||'"'
   ,  " union all "
  ) as str_agg
FROM INFORMATION_SCHEMA.SCHEMATA );

EXECUTE IMMEDIATE '''
  CREATE TEMP TABLE tmp_dataset_OBJECT_PRIVILEGES
  AS
    '''|| sql ||''' ''';

set sql = (SELECT
string_agg(
'SELECT * FROM `region-us.INFORMATION_SCHEMA.OBJECT_PRIVILEGES` WHERE object_schema = "'|| table_schema || '" AND object_name = "'|| table_name ||'"'
, " union distinct " ) as s
FROM `region-us.INFORMATION_SCHEMA.TABLES` where table_schema not in ('etl_staging_composer','etl_log') );

EXECUTE IMMEDIATE '''
  CREATE TEMP TABLE tmp_table_OBJECT_PRIVILEGES
  AS
    '''|| sql ||''' ''';

set  sql_prefix  = '''
SELECT
table_name,
table_schema,
table_catalog,
sum(total_rows) as total_rows,
sum(total_logical_bytes) as total_logical_bytes,
sum(total_billable_bytes) as total_billable_bytes,
max(last_modified_time) as last_modified_time
FROM
''';

set sql = ( SELECT
 STRING_AGG(
   sql_prefix || schema_name || '.INFORMATION_SCHEMA.PARTITIONS group by table_schema, table_name, table_catalog'
   ,  " union all "
  ) as str_agg
FROM INFORMATION_SCHEMA.SCHEMATA where schema_name not in ('etl_staging_composer','etl_log') );

EXECUTE IMMEDIATE '''
  CREATE TEMP TABLE tmp_table_TABLE_METADATA
  AS
    '''|| sql ||''' ''';


set sql = ( SELECT
 STRING_AGG(
   'select * from ' || schema_name || '.INFORMATION_SCHEMA.TABLES where table_type = "VIEW"'
   ,  " union all "
  ) as str_agg
FROM INFORMATION_SCHEMA.SCHEMATA where schema_name not in ('etl_staging_composer') );

EXECUTE IMMEDIATE '''
  CREATE TEMP TABLE tmp_view_VIEW_METADATA
  AS
    '''|| sql ||''' ''';

with security_tables as
(
    SELECT
    SUBSTR(job_build_id, 0,STRPOS(job_build_id, '.')-1) AS dataset_id,
    SUBSTR(job_build_id, STRPOS(job_build_id, '.')+1) AS table_id,
    job_build_id,
    cont
    FROM(
    SELECT
            distinct
            files.file_path as file_path,
            jb.job_build_id
    FROM  etl_log.job_build jb
    CROSS JOIN UNNEST(jb.files) AS files
    where files.file_path LIKE "%security%"
    ) AS jb
    INNER JOIN `etl_log.git_file` gf on jb.file_path = gf.file_path
),
objects as (
  select table_name, table_schema, table_catalog,
         total_rows, total_logical_bytes,
         total_billable_bytes, last_modified_time,
         'table' as type
    from tmp_table_TABLE_METADATA
  union all
  select table_name, table_schema, table_catalog,
         null as total_rows, null as total_logical_bytes,
         null as total_billable_bytes, creation_time as last_modified_time,
         'view' as type
  from tmp_view_VIEW_METADATA
)
select
 tables.table_catalog as project_id,
 tables.table_schema as dataset_id,
 tables.table_name as table_id,
 tables.type as type,
 FORMAT_TIMESTAMP("%Y-%m", tables.last_modified_time) as last_modified_month,
 dataset_privileges.privilege_type as ds_privilege,
 dataset_privileges.grantee as ds_grantee,
 table_privileges.privilege_type as tbl_privilege,
 table_privileges.grantee as tbl_grantee,
 security_tables.cont as cont
from objects as tables
left join tmp_table_OBJECT_PRIVILEGES table_privileges
  on tables.table_schema = table_privileges.object_schema
 and tables.table_name = table_privileges.object_schema
left join tmp_dataset_OBJECT_PRIVILEGES dataset_privileges
  on tables.table_schema = dataset_privileges.object_name
left join security_tables
  on security_tables.dataset_id = tables.table_schema
 and security_tables.table_id = tables.table_name
 order by 1,2,3
