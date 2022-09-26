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

create or replace view etl_log.vw_subject_area_build_files
as
select
distinct
sab.subject_area_build_id as build_commit_id,
  gc.datetime as build_commit_timestamp,
 gf.file_path,
 first_value(gfv_commits.id) over (partition by gf.file_path order by gfv_commits.datetime desc) as last_commit_id,
 first_value(gfv_commits.author) over (partition by gf.file_path order by gfv_commits.datetime desc) as last_author,
 first_value(gfv_commits.message) over (partition by gf.file_path order by gfv_commits.datetime desc) as last_message,
 first_value(gf.cont) over (partition by gf.file_path order by gfv_commits.datetime desc) as file_cont
from etl_log.subject_area_build sab
join etl_log.git_commit gc on gc.id = sab.subject_area_build_id
join etl_log.git_file gf on gf.subject_area = sab.subject_area
--
join etl_log.git_files_version gfv on gfv.file_path = gf.file_path
join etl_log.git_commit gfv_commits on gfv_commits.id = gfv.commit_id and gfv_commits.datetime <= gc.datetime
