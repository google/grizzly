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

create or replace view etl_log.vw_build_data_lineage_queue
as
with sa_builds as (
select build_id, build_datetime from etl_log.subject_area_build 
union all
select build_id, build_datetime from etl_log.subject_area_delete_build
)
select sab.*, cast( substr(sab.build_datetime,0,19) as datetime) as dt_build_datetime
 from sa_builds as  sab
where not exists (
  select 1 from `etl_log.data_lineage_build` dlb
  where dlb.data_lineage_build_id = sab.build_id
)
order by sab.build_datetime desc