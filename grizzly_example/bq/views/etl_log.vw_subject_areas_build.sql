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

create or replace view etl_log.vw_subject_areas_build
as
with max_executions as (
  select 
    max(build_datetime) over (partition by subject_area) as  max_time,
    subject_area_build_id,
    subject_area,
    build_datetime as start_time,
    op
  from (
      select *, 'DEPLOY' as op from `etl_log.subject_area_build`
      union all
      select *, 'DELETE' as op from `etl_log.subject_area_delete_build`
  )
 )
, subject_areas_ranked_ops as (
select me.*, 
 dense_rank() over (partition by me.subject_area order by me.start_time) as op_number,
 case when me.start_time = me.max_time then 'Y' else 'N' end as last_op
from max_executions me )
,  subject_areas_ops as (
  select 
    low.subject_area_build_id,
    low.subject_area,
    low.last_op,
    low.op_number,
    high.op_number as to_op_number,
    low.start_time as build_datetime,
    low.op,
    DATETIME (substr(low.start_time,0,19)) as from_op_datetime,
    datetime (ifnull(
         substr(high.start_time,0,19) , 
        '2099-01-01T00:00:00') ) as to_op_datetime
   from subject_areas_ranked_ops low 
  left join subject_areas_ranked_ops as high on 
    low.subject_area = high.subject_area and
    low.op_number + 1 = high.op_number
  where low.op = 'DEPLOY'
),
subject_areas_builds as 
(
  select 
    subject_area_build_id,
    subject_area,
    last_op as last_build,
    dense_rank() over (partition by subject_area order by op_number) as build_number,
    case when last_op = 'Y' then NULL
     else
      dense_rank() over (partition by subject_area order by to_op_number) 
      end as to_build_number,
    build_datetime,
    from_op_datetime as from_build_datetime,
    to_op_datetime as to_build_datetime
  from subject_areas_ops
)
select * from subject_areas_builds