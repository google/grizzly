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

create or replace view etl_log.vw_build_repository_difference
as
SELECT
 DISTINCT
  sa.last_commit_id = lg.commit_id AS has_last_commit,
  IF(sa.last_commit_id = lg.commit_id,1,0) AS num_check,
  sa.file_path,
  sa.last_commit_id AS build_git_commit,
  SUBSTR(sa.last_commit_id ,-6) AS short_build_git_commit,
  CONCAT(repo_url,'/+/',sa.last_commit_id) AS build_repo_git_url,
  lg.commit_id AS git_commit,
  SUBSTR(lg.commit_id,-6) as  short_git_commit,
  repo_url,
  repo_git_url
FROM `etl_log.vw_subject_area_build_files` AS sa
LEFT JOIN
`etl_log.vw_last_git_commit` AS lg
 ON sa.file_path = lg.file_path
