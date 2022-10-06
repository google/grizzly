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

create or replace view etl_log.vw_subject_area_build_sql_file
as
with max_executions as (
select 
 max(start_time) over (partition by subject_area) as  max_time,
 commit_sha as subject_area_build_id,
 subject_area,
 start_time
from `etl_log.cb_trigger_execution`
where status <> 'FAILURE' )
, subject_areas_builds as (
select me.*, 
 dense_rank() over (partition by me.subject_area order by me.start_time) as build_number,
 case when me.start_time = me.max_time then 'Y' else 'N' end as last_build
from max_executions me )
select jb.* except(files),
   flatten_files.*,
   build_number,
   sabs.start_time,
   last_build,
   DATETIME (substr(sabs.start_time,0,19)) as build_datetime  
from subject_areas_builds sabs
join `etl_log.job_build` jb on 
  sabs.subject_area_build_id = jb.subject_area_build_id and 
  sabs.subject_area = jb.subject_area
cross join UNNEST(jb.files) AS flatten_files