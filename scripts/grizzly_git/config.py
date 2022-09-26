# Copyright 2022 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Configuration of grizzly git api."""

from google.cloud import bigquery

GIT_REPO_PATH = r'/workspace/grizzly/'
GIT_DATASET = 'etl_log'
STAGE_DATASET = 'etl_staging_composer'
REPO_GIT_URL_TEMPLATE = '{repo_url}/+/{commit_sha}'

GIT_COMMITS_TABLE_NAME = 'git_commit'
GIT_COMMITS_TABLE_SCHEMA = [
    bigquery.SchemaField('id', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('author', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('datetime', 'TIMESTAMP', mode='NULLABLE'),
    bigquery.SchemaField('message', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('branch', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('repo_url', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('repo_git_url', 'STRING', mode='NULLABLE')
]

GIT_FILES_TABLE_NAME = 'git_file'
GIT_FILES_TABLE_SCHEMA = [
    bigquery.SchemaField('file_name', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('file_path', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('branch', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('subject_area', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('cont', 'STRING', mode='NULLABLE')
]

GIT_FILES_VERSION_TABLE_NAME = 'git_files_version'
GIT_FILES_VERSION_TABLE_SCHEMA = [
    bigquery.SchemaField('commit_id', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('file_path', 'STRING', mode='NULLABLE')
]

JOB_BUILD_TABLE_NAME = 'job_build'
JOB_BUILD_TABLE_SCHEMA = [
    bigquery.SchemaField('job_build_id', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('subject_area_build_id', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('subject_area', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField(
        'files',
        'RECORD',
        mode='REPEATED',
        fields=[
            bigquery.SchemaField('file_path', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('file_value', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('file_commit_id', 'STRING', mode='NULLABLE'),
        ],
    ),
]

CB_TRIGGER_EXECUTION_TABLE_NAME = 'cb_trigger_execution'
CB_TRIGGER_EXECUTION_TABLE_SCHEMA = [
    bigquery.SchemaField('id', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('status', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('start_time', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('finish_time', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('trigger_name', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('commit_sha', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('short_sha', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('subject_area', 'STRING', mode='NULLABLE')
]

SUBJECT_AREA_TABLE_NAME = 'subject_area'
SUBJECT_AREA_TABLE_SCHEMA = [
    bigquery.SchemaField('subject_area', 'STRING', mode='NULLABLE')
]

SUBJECT_AREA_BUILD_TABLE_NAME = 'subject_area_build'
SUBJECT_AREA_BUILD_TABLE_SCHEMA = [
    bigquery.SchemaField('subject_area', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('subject_area_build_id', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('status', 'STRING', mode='NULLABLE')
]

SQL_MERGE_TMP_INTO_GIT_COMMITS = """
    MERGE
        {GIT_DATASET}.git_commit T
    USING
        {tmp_git_commits_name} S
    ON T.id = S.id
    WHEN NOT MATCHED THEN
        INSERT(id, author, datetime, message, branch, repo_url, repo_git_url)
        VALUES(id, author, datetime, message, branch, repo_url, repo_git_url)
  """

SQL_MERGE_TMP_INTO_GIT_FILES_VERSION = """
    MERGE
        {GIT_DATASET}.git_files_version T
    USING
        {tmp_table} S
    ON
       T.commit_id = S.commit_id and
       t.file_path = s.file_path
    WHEN NOT MATCHED THEN
        INSERT(commit_id, file_path)
        VALUES(commit_id, file_path)
    """

SQL_MERGE_TMP_INTO_GIT_FILES = """
    MERGE
        {GIT_DATASET}.git_file T
    USING
        {tmp_table} S
    ON T.file_path = S.file_path
    WHEN NOT MATCHED THEN
        INSERT(file_name, file_path, branch, subject_area, cont)
        VALUES(file_name, file_path, branch, subject_area, cont)
    """

SQL_INSERT_INTO_SUBJECT_AREA = f"""
    insert into {GIT_DATASET}.subject_area (subject_area)
    select distinct subject_area
    from {GIT_DATASET}.git_file gf
    where
      not exists (select 1 from {GIT_DATASET}.subject_area sa where sa.subject_area = gf.subject_area)
      and gf.subject_area is not null
    """


SQL_GET_SUBJECT_AREA_BUILD_FILES = """
SELECT * FROM `{GIT_DATASET}.vw_subject_area_build_files`
where build_commit_id = "{commit_id}"
"""

SQL_MERGE_TMP_TO_JOB_BUILD = """
merge into `{GIT_DATASET}.job_build` as target
using {tmp_table} as source
on
  target.job_build_id = source.job_build_id and
  target.subject_area_build_id = source.subject_area_build_id and
  target.subject_area = source.subject_area
when not matched by target then
  insert (
    job_build_id, subject_area_build_id, subject_area, files
  )
  values (source.job_build_id, source.subject_area_build_id, source.subject_area, source.files)
when matched then
  update set files = source.files
"""

SQL_MERGE_TMP_TO_CB_TRIGGERS_EXECUTION = """
    MERGE
        {GIT_DATASET}.cb_trigger_execution T
    USING
        {tmp_table} S
    ON T.id = S.id
    WHEN NOT MATCHED THEN
        INSERT(id, status, start_time, finish_time, trigger_name, commit_sha, short_sha, subject_area)
        VALUES(id, status, start_time, finish_time, trigger_name, commit_sha, short_sha, subject_area)
    when MATCHED then
        update set
          status = s.status,
          finish_time = s.finish_time
  """

SQL_MERGE_TO_SUBJECT_AREA_BUILD = """
    MERGE
        {GIT_DATASET}.subject_area_build T
    USING
        (
          select * from (
            SELECT distinct
                max(source.start_time) over (partition by source.subject_area, source.commit_sha) as max_start_time,
                source.subject_area,
                source.commit_sha as subject_area_build_id,
                source.status,
                source.start_time
              FROM  {GIT_DATASET}.cb_trigger_execution source
              WHERE subject_area IS NOT NULL
              and trigger_name like '%deploy-composer%'
              and not exists (
                    select 1
                    from {GIT_DATASET}.subject_area_build target
                    where
                          source.commit_sha = target.subject_area_build_id
                    and source.status = target.status
                    and source.subject_area = target.subject_area
                  ) ) source where source.max_start_time = source.start_time
        ) S
    ON
        T.subject_area_build_id = S.subject_area_build_id
    and T.subject_area = S.subject_area
    WHEN NOT MATCHED THEN
        INSERT(subject_area, subject_area_build_id, status)
        VALUES(subject_area, subject_area_build_id, status)
    when matched then
       update set
          status = s.status
  """


SQL_CREATE_VW_SUBJECT_AREA_BUILD_FILES = """
create or replace view {GIT_DATASET}.vw_subject_area_build_files
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
from {GIT_DATASET}.subject_area_build sab
join {GIT_DATASET}.git_commit gc on gc.id = sab.subject_area_build_id
join {GIT_DATASET}.git_file gf on gf.subject_area = sab.subject_area
--
join {GIT_DATASET}.git_files_version gfv on gfv.file_path = gf.file_path
join {GIT_DATASET}.git_commit gfv_commits on gfv_commits.id = gfv.commit_id and gfv_commits.datetime <= gc.datetime
"""


SQL_CREATE_VW_JOB_GIT_FILES = """
create or replace view {GIT_DATASET}.vw_job_git_files
as
select
log.job_id,
log.job_name,
log.subject_area,
files.file_commit_id as file_commit_id,
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
"""
