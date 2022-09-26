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

"""Big Query tables security.

Deployment Big Query tables security by using file of configurations.
Perform actions with BQ tables security. Table and Row-level security
supported.

Typical usage example:

bqtab = BQTableSecurity(execution_context, raw_access_scripts)
bqtab.run_bq_access_scripts()
"""

import json
import pathlib
from typing import Any, Dict, List

from airflow.exceptions import AirflowException
import google.auth.transport.requests
from google.auth.transport.urllib3 import AuthorizedHttp
import grizzly.etl_action
from grizzly.execution_log import etl_step
from grizzly.execution_log import ExecutionLog
from grizzly.grizzly_typing import TGrizzlyOperator, TQueryJob

import jinja2


class BQTableSecurity():
  """Perform actions with BQ tables and row-level security.

  Implementation of BQTableSecurity uses BQ RestApi for work with Row Level
  security information.

  Attributes:
    execution_context (GrizzlyOperator): Instance of GrizzlyOperator executed.
    raw_access_scripts (list[string]): List of access scripts to be executed.
      attribute [access_scripts] from task YML file.
    authed_http (google.auth.transport.urllib3.AuthorizedHttp): Authorized http
      connection for work with BigQuery RestApi
  """
  SCOPES = ['https://www.googleapis.com/auth/cloud-platform']
  base_bq_api_url = 'https://bigquery.googleapis.com/bigquery/v2/{api_call}'

  def __init__(self,
               execution_context: TGrizzlyOperator,
               raw_access_scripts: List[str]) -> None:
    """Init BQTableSecurity.

    Args:
      execution_context (TGrizzlyOperator): Instance of GrizzlyOperator
        executed.
      raw_access_scripts (list[string]): List of access scripts to be executed.

    Raises:
        Exception: Exception raisen in case if BQ REstApi authorization failed.
    """
    self.execution_context = execution_context
    self.raw_access_scripts = raw_access_scripts
    # pylint: disable=unused-variable
    credentials, project = google.auth.default(scopes=self.SCOPES)
    auth_req = google.auth.transport.requests.Request()
    credentials.refresh(auth_req)
    self.authed_http = AuthorizedHttp(credentials)

  def get_row_access_policies(self, target_table: str) -> List[Dict[str, Any]]:
    """Return a list of ROW ACCESS POLICIES configured for a table.

    Args:
      target_table (string): Target table name.

    Raises:
      AirflowException: Exception if RestApi call for access table row level
        security configuration failed.

    Returns:
      list(dict)): List of all row access policies on the specified table.
        More details available here
        https://cloud.google.com/bigquery/docs/reference/rest/v2/rowAccessPolicies/list
    """
    if isinstance(target_table, str):
      target_table = grizzly.etl_action.parse_table(target_table)
    api_call = 'projects/{project_Id}/datasets/{dataset_id}/tables/{table_id}/rowAccessPolicies'.format(
        project_Id=target_table['project_id'],
        dataset_id=target_table['dataset_id'],
        table_id=target_table['table_id'])
    session_url = self.base_bq_api_url.format(api_call=api_call)
    r = self.authed_http.urlopen(method='get', url=session_url)
    if r.status == 200:
      response = json.loads(r.data)
    else:
      raise AirflowException(
          (f'Could not receive a List of table`s row-level access policies '
           f'for [{target_table}]. {vars(r)}')
      )
    return response.get('rowAccessPolicies', [])

  @etl_step
  def run_bq_access_scripts(
      self,
      target_table: str,
      # pylint: disable=unused-argument
      etl_log: ExecutionLog,
      # pylint: disable=unused-argument
      job_step_name: str = 'run_access_scripts'
  ) -> TQueryJob:
    """Run BQ scripts for configuration of row and table security rules.

    Method parses all scripts defined in raw_access_scripts and generates one
    security script on a base of JINJA2 template
    [templates/apply_security.sql.jinja2]
    Final scripts applied security rules one by one and grants access to
    GCP Composer service account.
    Once all security scripts applied method performs clean up of all row level
    security rules that were not defined inside task YML.

    Args:
      target_table (string or dict): Target table name.
      etl_log (grizzly.execution_log.ExecutionLog): ExecutionLog object used by
        etl_step decorator.
      job_step_name (string): Value to be stored in job_step.job_step_name
        attribute of etl log table. Used by etl_log decorator.

    Returns:
      (TQueryJob): BQ job with statistic of execution of security script.
    """
    if isinstance(target_table, str):
      target_table_str = target_table
      target_table = grizzly.etl_action.parse_table(target_table)
    else:
      target_table_str = '{dataset_id}.{table_id}'.format(
          dataset_id=target_table['dataset_id'],
          table_id=target_table['table_id']
      )
    # Get a security scripts configuration and render them with Jinja2
    security_scripts = self.execution_context.task_config.get_value_from_list(
        file_list=self.raw_access_scripts,
        file_format='sql',
        table_name=target_table_str)
    # get existing row access rules before scripts apply
    row_policy_before = self.get_row_access_policies(target_table)
    # apply security scripts
    template_folder = pathlib.Path('/home/airflow/gcs/plugins/templates')
    view_template = template_folder / 'apply_security.sql.jinja2'
    access_query = jinja2.Template(view_template.read_text()).render(
        table_name=target_table_str,
        security_scripts=security_scripts,
        task_config=self.execution_context.task_config)
    job_stat = grizzly.etl_action.run_bq_query(
        execution_context=self.execution_context, sql=access_query)
    # get row access rules after scripts apply
    row_policy_after = self.get_row_access_policies(target_table)
    before_row_policy_dict = {
        item['rowAccessPolicyReference']['policyId']: item['lastModifiedTime']
        for item in row_policy_before
    }
    after_row_policy_dict = {
        item['rowAccessPolicyReference']['policyId']: item['lastModifiedTime']
        for item in row_policy_after
    }
    # If after apply of security scripts we have only 1 [all_access] row access
    # rule for serviceaccount. It means that no rowaccess scripts were defined.
    # Clean up rowaccess rule for service account if it's only 1 one assigned.
    if (len(after_row_policy_dict) == 1 and
        'all_access' in after_row_policy_dict):
      grizzly.etl_action.run_bq_query(
          execution_context=self.execution_context,
          sql=f'DROP ALL ROW ACCESS POLICIES ON `{target_table_str}`;')
    else:
      # remove all rules that were not changed after access scripts apply.
      # If they where not changed then they are not in security scope anymore
      rules_to_be_removed = {
          p for p, t in after_row_policy_dict.items()
          if before_row_policy_dict.get(p, None) == t
      }
      if rules_to_be_removed:
        self.execution_context.log.info(
            (f'Next policies will be removed on table [{target_table_str}]: '
             f'{rules_to_be_removed}')
        )
        # Cleanup orphaned row level security
        housekeeping_template = """{% for policyId in rules_to_be_removed %}
          DROP ROW ACCESS POLICY IF EXISTS {{ policyId }} ON `{{ table_name }}`;
          {% endfor %}
          """
        housekeeping_query = jinja2.Template(housekeeping_template).render(
            rules_to_be_removed=rules_to_be_removed,
            table_name=target_table_str)
        grizzly.etl_action.run_bq_query(
            execution_context=self.execution_context, sql=housekeeping_query)
    return job_stat
