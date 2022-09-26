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

create or replace view etl_log.vw_job_git_files
as
select
log.job_id,
log.job_name,
log.subject_area,
files.file_commit_id as file_commit_id,
substr(files.file_commit_id,-6) as short_file_commit_id,
files.file_path,
gc.author as commit_author,
gc.datetime as commit_datetime,
gc.message as commit_message,
gc.branch as git_branch,
gc.repo_git_url as git_url
from etl_log.composer_job_details log
join etl_log.job_build jb on jb.job_build_id = log.job_name
                         and jb.subject_area = log.subject_area
                         and log.subject_area_build_id = jb.subject_area_build_id
CROSS JOIN UNNEST(jb.files) AS files
join etl_log.git_commit gc on gc.id = files.file_commit_id
