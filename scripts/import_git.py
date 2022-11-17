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

"""Import git repository into set of tables.

Read git repository with files and fill in the tables.

Example:
  python3 ./deploy_composer.py -p <GCP_ENVIRONMENT>
    -e <ENVIRONMENT>
    -c <ENV_CONFIG_FILE>
    -f <ETL_SOURCE_PATH> [-l]
"""

import argparse
import json
import sys
from typing import Dict, List, Tuple

from deployment_utils import BQUtils
from deployment_utils import ImportToolConfig
from deployment_utils import Utils as DeploymentUtils
import git
from git.repo.base import Repo
from grizzly.forecolor import ForeColor
from grizzly_git.config import GIT_COMMITS_TABLE_NAME
from grizzly_git.config import GIT_COMMITS_TABLE_SCHEMA
from grizzly_git.config import GIT_DATASET
from grizzly_git.config import GIT_FILES_TABLE_NAME
from grizzly_git.config import GIT_FILES_TABLE_SCHEMA
from grizzly_git.config import GIT_FILES_VERSION_TABLE_NAME
from grizzly_git.config import GIT_FILES_VERSION_TABLE_SCHEMA
from grizzly_git.config import GIT_REPO_PATH
from grizzly_git.config import REPO_GIT_URL_TEMPLATE
from grizzly_git.config import SQL_INSERT_INTO_SUBJECT_AREA
from grizzly_git.config import SQL_MERGE_TMP_INTO_GIT_COMMITS
from grizzly_git.config import SQL_MERGE_TMP_INTO_GIT_FILES
from grizzly_git.config import SQL_MERGE_TMP_INTO_GIT_FILES_VERSION
from grizzly_git.config import STAGE_DATASET
from grizzly_git.config import SUBJECT_AREA_TABLE_NAME
from grizzly_git.config import SUBJECT_AREA_TABLE_SCHEMA


REPO: Repo = git.Repo(GIT_REPO_PATH)
GIT_COMMITS = list(REPO.iter_commits())
GIT_REPO_URL = REPO.remote().url

FILE_COMMITS = {}
SUBJECT_AREAS = []

GCP_PROJECT = None


def clear_file_path(file_path):
  return '/'.join(file_path.split('/')[3: ])


def get_files_commit():
  """Return files for each commit."""

  res = {}

  for commit in GIT_COMMITS:
    files = REPO.git.show('--pretty=', '--name-only', commit.hexsha)
    files = str(files).splitlines()

    for f in files:
      if f not in res:
        res[f] = [commit.hexsha]
      else:
        res[f].append(commit.hexsha)

  return res


def get_file_subject_area(file_commits: List[str]) -> List[str]:
  """Return files for each subject area."""

  res = dict()

  for file in file_commits:
    if 'SCOPE.yml' == file.split('/')[-1]:
      file_parts = file.split('/')
      if not file_parts:
        res['N/A'] = -1
      elif len(file_parts) == 1:
        res['root'] = 0
      elif len(file_parts) == 2:
        res[file_parts[0]] = 1
      else:
        res['/'.join(file_parts[0: -1])] = len(file_parts)-1

  res = [(k, v) for k, v in res.items()]
  res = [x[0] for x in sorted(res, key=lambda x: (x[1], len(x[0])), reverse=True)]

  return res


def chunks(lst: List[any], n: int) -> List[any]:
  """Yield successive n-sized chunks from lst."""
  for i in range(0, len(lst), n):
    yield lst[i: i + n]


def load_files(folder: str) -> Dict[str, str]:
  """Return files from git folder with specific extensions."""

  yml_files = DeploymentUtils.yaml_load(path=folder, file_filter='**/*.yml')
  yaml_files = DeploymentUtils.yaml_load(path=folder, file_filter='**/*.yaml')
  sql_files = DeploymentUtils.files_load(path=folder, file_filter='**/*.sql')

  files = {**yml_files, **yaml_files, **sql_files}
  files = {clear_file_path(f): c for f, c in files.items()}

  return files


def get_subject_area(filename: str) -> str:
  """Return subject area from file name."""
  for sa in SUBJECT_AREAS:
    if filename.startswith(sa):
      return sa


def git_commits_data_proc(bq_utils: BQUtils,
                          config: ImportToolConfig) -> None:
  """Process and insert commit rows."""

  tmp_table_name = f'{STAGE_DATASET}.{GIT_COMMITS_TABLE_NAME}'
  table_name = f'{GIT_DATASET}.{GIT_COMMITS_TABLE_NAME}'

  bq_utils.create_table(table_name=table_name,
                        table_schema=GIT_COMMITS_TABLE_SCHEMA)
  tmp_table = bq_utils.create_temp_table(table_name=tmp_table_name,
                                         table_schema=GIT_COMMITS_TABLE_SCHEMA)

  rows_to_insert = []

  for commit in GIT_COMMITS:
    commit_id = commit.hexsha
    author = commit.author.name
    date_time = commit.committed_date
    message = commit.message.strip()
    repo_git_url = REPO_GIT_URL_TEMPLATE.format(repo_url=GIT_REPO_URL,
                                                commit_sha=commit_id)
    rows_to_insert.append({'id': commit_id,
                           'author': author,
                           'datetime': date_time,
                           'message': message,
                           'branch': config.env,
                           'repo_url': GIT_REPO_URL,
                           'repo_git_url': repo_git_url})

  git_commits_tmp_table_name = '{}.{}.{}'.format(
      tmp_table.project,
      tmp_table.dataset_id,
      tmp_table.table_id)

  bq_utils.bq_client.insert_rows_json(git_commits_tmp_table_name,
                                      rows_to_insert)

  sql_merge = SQL_MERGE_TMP_INTO_GIT_COMMITS.format(
      GIT_DATASET=GIT_DATASET,
      tmp_git_commits_name=git_commits_tmp_table_name)

  bq_utils.bq_client.query(sql_merge).result()


def get_git_files_tables_rows(
    config: ImportToolConfig,
    files: Dict[str, str]) -> Tuple[List[str], List[str]]:
  """Return git files rows for tables."""

  git_files_rows = []
  git_files_version_rows = []

  for filename, cont in files.items():

    cont = json.dumps(cont)
    cont = cont.replace('|', '\\u007C')
    file_commits = FILE_COMMITS[filename]

    rows_to_insert_add = [
        {'commit_id': commit, 'file_path': filename, 'cont': cont}
        for commit in file_commits]

    git_files_version_rows += rows_to_insert_add

    subject_area = get_subject_area(filename)

    if subject_area:
      subject_area = subject_area.split('/')[-1].upper()

    file_name_wo_path = filename.split('/')[-1]

    row = {'file_name': file_name_wo_path,
           'file_path': filename,
           'branch': config.env,
           'subject_area': subject_area}

    git_files_rows.append(row)

  return git_files_rows, git_files_version_rows


def git_files_version_data_proc(
    bq_utils: BQUtils,
    config: ImportToolConfig,
    files: List[str]) -> None:
  """Loading data into git_files & git_files_version tables."""

  print('Loading data into git_files & git_files_version tables')

  # git_files_version processing

  # create tmp and target table: git_files_version
  tmp_table_name = f'{STAGE_DATASET}.{GIT_FILES_VERSION_TABLE_NAME}'
  table_name = f'{GIT_DATASET}.{GIT_FILES_VERSION_TABLE_NAME}'

  bq_utils.create_table(table_name=table_name,
                        table_schema=GIT_FILES_VERSION_TABLE_SCHEMA)
  git_files_version_tmp_table = bq_utils.create_temp_table(
      table_name=tmp_table_name,
      table_schema=GIT_FILES_VERSION_TABLE_SCHEMA)

  git_files_rows, git_files_version_rows = get_git_files_tables_rows(
      config=config,
      files=files)

  # git_files_version inserting rows
  git_files_version_tmp_table_name = '{}.{}.{}'.format(
      git_files_version_tmp_table.project,
      git_files_version_tmp_table.dataset_id,
      git_files_version_tmp_table.table_id,
  )

  for chunk_git_files_version_rows in chunks(git_files_version_rows, 500):
    bq_utils.bq_client.insert_rows_json(git_files_version_tmp_table_name,
                                        chunk_git_files_version_rows)

  # git_files_version merging with target
  sql_merge = SQL_MERGE_TMP_INTO_GIT_FILES_VERSION.format(
      GIT_DATASET=GIT_DATASET,
      tmp_table=git_files_version_tmp_table_name)

  print(sql_merge)

  bq_utils.bq_client.query(sql_merge).result()

  # git_files processing

  # create tmp and target table: git_files
  tmp_table_name = f'{STAGE_DATASET}.{GIT_FILES_TABLE_NAME}'
  table_name = f'{GIT_DATASET}.{GIT_FILES_TABLE_NAME}'

  bq_utils.create_table(table_name=table_name,
                        table_schema=GIT_FILES_TABLE_SCHEMA)
  git_files_tmp_table = bq_utils.create_temp_table(
      table_name=tmp_table_name,
      table_schema=GIT_FILES_TABLE_SCHEMA)

  # git_files inserting rows
  tmp_val = '{}.{}.{}'.format(
      git_files_tmp_table.project,
      git_files_tmp_table.dataset_id,
      git_files_tmp_table.table_id)

  git_files_tmp_table_name = tmp_val
  for chunk_git_files_rows in chunks(git_files_rows, 500):
    bq_utils.bq_client.insert_rows_json(git_files_tmp_table_name,
                                        chunk_git_files_rows)

  # git_files merging with target
  sql_merge = SQL_MERGE_TMP_INTO_GIT_FILES.format(
      GIT_DATASET=GIT_DATASET,
      tmp_table=git_files_tmp_table_name)

  bq_utils.bq_client.query(sql_merge).result()


def subjec_area_data_proc(bq_utils: BQUtils) -> None:
  """Loading data into subject_area table by merge."""

  print('Loading data into subject_area table by insert')

  table_name = f'{GIT_DATASET}.{SUBJECT_AREA_TABLE_NAME}'
  bq_utils.create_table(table_name=table_name,
                        table_schema=SUBJECT_AREA_TABLE_SCHEMA)

  sql_merge = SQL_INSERT_INTO_SUBJECT_AREA.format(GIT_DATASET=GIT_DATASET)
  bq_utils.bq_client.query(sql_merge).result()


def main(args: argparse.Namespace) -> None:
  """Implement the command line interface described in the module doc string."""

  env = args.env.lower()
  env_config_file = args.env_config_file

  folder = args.folder

  config = ImportToolConfig(env=env, env_config_file=env_config_file)

  bq_utils = BQUtils(gcp_project_id=config.gcp_environment_target)

  files = load_files(folder=folder)

  git_commits_data_proc(bq_utils=bq_utils, config=config)

  git_files_version_data_proc(bq_utils=bq_utils, config=config, files=files)

  subjec_area_data_proc(bq_utils=bq_utils)

if __name__ == '__main__':

  FILE_COMMITS = get_files_commit()
  SUBJECT_AREAS = get_file_subject_area(file_commits=FILE_COMMITS)

  try:
    # Construct the argument parser
    ap = argparse.ArgumentParser(
        description='Script used for import git data into the tables.')
    # Add the arguments to the parser
    ap.add_argument(
        '-e',
        '--env',
        dest='env',
        required=True,
        help='Env of GCP project.')
    ap.add_argument(
        '-c',
        '--env_config_file',
        dest='env_config_file',
        required=True,
        help='Environment file')
    ap.add_argument(
        '-f',
        '--folder',
        dest='folder',
        required=True,
        help='File folder')

    input_args = ap.parse_args()
    main(args=input_args)
  except:
    print(f'{ForeColor.RED}Unexpected error: {ForeColor.RESET}',
          sys.exc_info()[1])
    raise


