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

create or replace view etl_log.vw_data_lineage_objects_connection
as
select c.*, 
  o_target.target_table_name
from etl_log.data_lineage_objects_connection c
join `etl_log.vw_data_lineage_object` o_target 
    on o_target.object_id = c.target_object_id
and o_target.data_lineage_build_id = c.data_lineage_build_id
and o_target.data_lineage_type = c.data_lineage_type
and o_target.subject_area = c.subject_area