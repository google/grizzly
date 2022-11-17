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

CREATE OR REPLACE TABLE FUNCTION `etl_log.fn_get_build_files`(v_subject_area STRING, commit_sha STRING) AS (
(
(
  with virtual_subject_area_build as
  (
    select upper(v_subject_area) as subject_area, commit_sha as subject_area_build_id
  )
  select
    distinct
      sab.subject_area_build_id as build_commit_id,
      gc.datetime as build_commit_timestamp,
      gf.file_path,
    first_value(gfv_commits.id) over (partition by gf.file_path order by gfv_commits.datetime desc) as last_commit_id,
    first_value(gfv_commits.author) over (partition by gf.file_path order by gfv_commits.datetime desc) as last_author,
    first_value(gfv_commits.message) over (partition by gf.file_path order by gfv_commits.datetime desc) as last_message,
    first_value(gfv.cont) over (partition by gf.file_path order by gfv_commits.datetime desc) as file_cont
  from virtual_subject_area_build sab
    join etl_log.git_commit gc on gc.id = sab.subject_area_build_id
    join etl_log.git_file gf on gf.subject_area = sab.subject_area
    --
    join etl_log.git_files_version gfv on gfv.file_path = gf.file_path
    join etl_log.git_commit gfv_commits on gfv_commits.id = gfv.commit_id and gfv_commits.datetime <= gc.datetime
  )
)
);

CREATE OR REPLACE TABLE FUNCTION `etl_log.fn_get_subject_area_build_sql_files`(v_build_datetime DATETIME) AS (
(
     (SELECT
       files.*
      FROM etl_log.vw_subject_areas_build as sab
      join etl_log.vw_subject_area_build_sql_file as files on
        files.start_time = sab.build_datetime and
        files.subject_area = sab.subject_area
      where v_build_datetime
       between
         sab.from_build_datetime and
         DATETIME_ADD(sab.to_build_datetime, interval -1 SECOND )
    )
  )
);

CREATE OR REPLACE TABLE FUNCTION `etl_log.fn_get_subject_area_builds`(v_build_datetime DATETIME) AS (
(
     (SELECT * FROM etl_log.vw_subject_areas_build t
       where v_build_datetime
       between
         t.from_build_datetime and
         DATETIME_ADD(t.to_build_datetime, interval -1 SECOND )
    )
  )
);

CREATE OR REPLACE TABLE FUNCTION `etl_log.get_subject_area_builds`(v_build_datetime DATETIME) AS (
(
     (SELECT * FROM etl_log.vw_subject_areas_build t where v_build_datetime between t.from_build_datetime and t.to_build_datetime )
    )
);
