# Copyright 2021 Google LLC
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

"""Grizzly typing module.

Typical usage example:
from grizzly.grizzly_typing import TGrizzlyOperator

bqtab = BQTableSecurity(execution_context: TGrizzlyOperator,
                        raw_access_scripts: str)

"""
from __future__ import annotations

from typing import TYPE_CHECKING, Dict
from typing import TypeVar

if TYPE_CHECKING:
  # pylint: disable=g-import-not-at-top
  from google.cloud.bigquery.job import CopyJob
  from google.cloud.bigquery.job import QueryJob
  from operators.grizzly_operator import GrizzlyOperator
  from grizzly.task_instance import TaskInstance
  from grizzly.execution_log import ExecutionLog
  from operators.grizzly_dlp_operator import GrizzlyDLPOperator

TGrizzlyOperator = TypeVar('TGrizzlyOperator', bound='GrizzlyOperator')
TQueryJob = TypeVar('TQueryJob', bound='QueryJob')
TCopyJob = TypeVar('TCopyJob', bound='CopyJob')
TGrizzlyTaskConfig = TypeVar('TGrizzlyTaskConfig', bound='TaskInstance')
TExecutionLog = TypeVar('TExecutionLog', bound='ExecutionLog')
TGrizzlyTableParsed = Dict[str, str]

TGrizzlyDLPOperator = TypeVar('TGrizzlyDLPOperator', bound='GrizzlyDLPOperator')
