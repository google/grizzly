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

create or replace view etl_log.vw_last_git_commit
as
SELECT
    commit_id,
    author,
    datetime,
    message,
    repo_url,
    repo_git_url,
    file_path
FROM (

    SELECT
        ROW_NUMBER() OVER(PARTITION BY file_path ORDER BY datetime DESC) AS row_num,
        *
    FROM `etl_log.git_commit` AS gc
    LEFT JOIN
        (SELECT * FROM `etl_log.git_files_version`) AS gf
    ON gc.id = gf.commit_id
    WHERE gf.file_path IS NOT NULL
)
WHERE
    row_num = 1
