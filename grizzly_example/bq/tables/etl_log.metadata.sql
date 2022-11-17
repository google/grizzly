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

CREATE TABLE IF NOT EXISTS `etl_log.cb_trigger_execution`
(
  id	        STRING,			
  status	    STRING,			
  start_time	STRING,			
  finish_time	STRING,			
  trigger_name	STRING,			
  commit_sha	STRING,			
  short_sha	    STRING,			
  subject_area	STRING
);

CREATE TABLE IF NOT EXISTS `etl_log.data_lineage_build`
(
  data_lineage_build_id	STRING,			
  subject_area_build_id STRING,			
  build_datetime	    STRING
);

CREATE TABLE IF NOT EXISTS `etl_log.data_lineage_object`
(
  data_lineage_build_id	STRING,			
  data_lineage_type     STRING,			
  subject_area	        STRING,
  object_id             STRING,
  parent_object_id      STRING,
  object_type           STRING,
  target_table_name     STRING,
  object_data           STRING
);

CREATE TABLE IF NOT EXISTS `etl_log.data_lineage_objects_connection`
(
  data_lineage_build_id	STRING,			
  data_lineage_type     STRING,			
  subject_area	        STRING,
  source_object_id      STRING,
  target_object_id      STRING,
  connection_data       STRING
);


CREATE TABLE IF NOT EXISTS `etl_log.git_commit`
(
  id STRING,
  author STRING,
  datetime TIMESTAMP,
  message STRING,
  branch STRING,
  repo_url STRING,
  repo_git_url STRING
);

CREATE TABLE IF NOT EXISTS `etl_log.git_file`
(
  file_name STRING,
  file_path STRING,
  branch STRING,
  subject_area STRING,
  cont STRING
);

CREATE TABLE IF NOT EXISTS `etl_log.git_files_version`
(
  commit_id STRING,
  file_path STRING,
  cont STRING
);

CREATE TABLE IF NOT EXISTS `etl_log.job_build`
(
  job_build_id STRING,
  subject_area_build_id STRING,
  subject_area STRING,
  files ARRAY < STRUCT <file_path STRING, file_value STRING, file_commit_id STRING> >
);

CREATE TABLE IF NOT EXISTS `etl_log.project_table_access`
(
  project_id	        STRING,			
  dataset_id	        STRING,			
  table_id	            STRING,			
  type	                STRING,			
  last_modified_month	STRING,			
  ds_privilege	        STRING,			
  ds_grantee	        STRING,			
  tbl_privilege	        STRING,			
  tbl_grantee	        STRING,			
  cont	                STRING		
);

CREATE TABLE IF NOT EXISTS `etl_log.project_taxonomy`
(
  project_id	    STRING,			
  dataset_name	    STRING,		
  table_name	    STRING,			
  column_name	    STRING,			
  taxonomy_name	    STRING,			
  policy_tag_name	STRING
);

CREATE TABLE IF NOT EXISTS `etl_log.subject_area`
(
  subject_area STRING
);

CREATE TABLE IF NOT EXISTS `etl_log.subject_area_build`
(
  subject_area	        STRING,			
  subject_area_build_id	STRING,			
  status	            STRING,			
  build_id	            STRING,			
  build_datetime	    STRING	
);


CREATE TABLE IF NOT EXISTS `etl_log.subject_area_delete_build`
(
  subject_area	        STRING,			
  subject_area_build_id	STRING,			
  status	            STRING,			
  build_id	            STRING,			
  build_datetime	    STRING	
);

CREATE TABLE IF NOT EXISTS `etl_log.grizzly_information_schema_column_field_paths`
(
  table_catalog STRING,
  table_schema STRING,
  table_name STRING,
  column_name STRING,
  field_path STRING,
  data_type STRING,
  description STRING,
  collation_name STRING,
  metadata_datetime DATETIME,
  job_id INT64,
  job_name STRING
);

CREATE TABLE IF NOT EXISTS `etl_log.grizzly_information_schema_table_options`
(
  table_catalog STRING,
  table_schema STRING,
  table_name STRING,
  option_name STRING,
  option_type STRING,
  option_value STRING,
  metadata_datetime DATETIME,
  job_id INT64,
  job_name STRING
);
