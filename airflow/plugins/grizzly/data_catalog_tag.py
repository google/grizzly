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

"""Setup a DataCatalog tags and Column Level security.

Deployment and configuration DataCatalog entities.
Read more about Data Catalog and column level security here:
https://cloud.google.com/data-catalog
https://cloud.google.com/bigquery/docs/column-level-security-intro

Typical usage example:

dt_tag = DataCatalogTag(execution_context,
    column_policy_tags,
    datacatalog_tags)
"""

import json
import re
from typing import Any, Dict, List, Text

from airflow.exceptions import AirflowException
from airflow.providers.google.cloud.hooks.datacatalog import CloudDataCatalogHook
import google.auth.transport.requests
from google.auth.transport.urllib3 import AuthorizedHttp
from grizzly.config import Config
from grizzly.etl_action import parse_table
from grizzly.grizzly_typing import TGrizzlyOperator

_TPolicyTags = Dict[str, str]


class DataCatalogTag:
  """Perform actions with DataCatalog.

  Should be used for Data Catalog Table and column tags.
  Assign Column level security throw DataCatalog Taxonomy.

  Attributes:
    execution_context (GrizzlyOperator): Instance of GrizzlyOperator executed.
    column_policy_tags (list[dict]): List of Column level policy (security) tags
      to be applied in format
      { 'column_name: 'column_policy_tag_id'}
    datacatalog_tags (list[dict]): Content of JSON file defined in
      [data_catalog_tags] attribute of task YML file. Content is rendered as
      JINJA2 template and loaded as list of dictionaries with definition of
      table and column tags to be applied.
    authed_http (google.auth.transport.urllib3.AuthorizedHttp): Autorized http
      connection for work with Data Catalog Rest API.
    base_api_url (string): Base URL for work with DataCatalog Rest API.
    dc_hook (CloudDataCatalogHook):
      Airflow predefined hooks for work with GCP Data Catalog.
  """

  def __init__(self,
               execution_context: TGrizzlyOperator,
               column_policy_tags: List[_TPolicyTags],
               datacatalog_tags: List[Text]) -> None:
    """Set up DataCatalogTag instance.

    If [column_policy_tags] or [datacatalog_tags] was defined set up
    correspondent class properties.

    Args:
      execution_context (TGrizzlyOperator): Instance of GrizzlyOperator
        executed.
      column_policy_tags (list): List of Column level policy (security)
        tags to be applied in format
        {'column_name: 'taxonomy|tag_hierarchy'}
        Contains column level security configuration.
      datacatalog_tags (list): Content of JSON file defined in
        [data_catalog_tags] attribute of task YML file. Content is rendered as
        JINJA2 template and loaded as list of dictionaries with definition of
        table and column tags to be applied. Contains Table and column tags.
    """
    self.execution_context = execution_context
    if column_policy_tags or datacatalog_tags:
      self.__setup_datacatalog_connection()

    if column_policy_tags:
      # Get list of DataCatalog security policy tag mapping
      self.column_policy_tags = self.__get_column_policy_tags_mapping(
          column_policy_tags)
    else:
      self.column_policy_tags = None
    if datacatalog_tags:
      self.datacatalog_tags = datacatalog_tags
    else:
      self.datacatalog_tags = None

  def __get_table_entry_id(self, target_table: Dict[str, str]) -> Any:
    """Get an DataCatalog EntryId by table name."""
    target_table = parse_table(target_table)
    resource_name = (f'//bigquery.googleapis.com/'
                     f'projects/{target_table["project_id"]}/'
                     f'datasets/{target_table["dataset_id"]}/'
                     f'tables/{target_table["table_id"]}')
    table_entry = self.dc_hook.lookup_entry(linked_resource=resource_name)
    return table_entry

  def __setup_datacatalog_connection(self) -> None:
    """Setup connection credentials for access Data Catalog API."""
    scopes = ['https://www.googleapis.com/auth/cloud-platform']
    # pylint: disable=unused-variable
    credentials, project = google.auth.default(scopes=scopes)

    auth_req = google.auth.transport.requests.Request()
    credentials.refresh(auth_req)
    self.authed_http = AuthorizedHttp(credentials)
    access_token = credentials.token
    self.base_api_url = (
        'https://datacatalog.googleapis.com/v1/{api_call}?access_token='
        + access_token)
    # setup datacatalog hooks
    self.dc_hook = CloudDataCatalogHook()

  def __get_column_policy_tags_mapping(
      self,
      column_policy_tags: List[_TPolicyTags]
  ) -> _TPolicyTags:
    """Return a list of all applicable taxonomies for job/table.

    Parse user defined format from task YML file and transform it into format
    consumable by DataCatalog Rest API.
    Method gets all taxonomy list on environment. Then select taxonomy defined
    by user and parses taxonomy tag hierarchy to find [column_policy_tag_id]
    that matches with taxonomy tag hierarchy defined by user in task YML file
    attribute [column_policy_tags].

    Args:
      column_policy_tags (list[dict]): List of column policy tag definition to
        be parsed in format: {'column_name: 'taxonomy|tag_hierarchy'}

    Raises:
      AirflowException: Raise error in case if Column policy taxonomy as not
        defined on target GCP project or if user defined reference to policy tag
        that does not exist.

    Returns:
      (dict): List of column policy tag definition in format
        {'column_name: 'column_policy_tag_id'}
    """
    column_policy_tags_mapping = {}
    # get a set of all applicable taxonomies
    # accordingly to job YML configuration [column_policy_tags]
    requested_taxonomies = set()
    for c in column_policy_tags:
      for v in c.values():
        # Add taxonomy name to set
        requested_taxonomies.add(v.split('|')[0])

    # Get list of DataCatalog taxonomies
    api_call = Config.DEFAULT_DATACATALOG_TAXONOMY_LOCATION
    session_url = self.base_api_url.format(api_call=api_call)
    r = self.authed_http.urlopen(method='get', url=session_url)
    taxonomy_mapping = {
    }
    # looks like {'taxonomy_name': 'projects/prj_id/locations/us/taxonomies/64'}
    if r.status == 200:
      response = json.loads(r.data)
      # work only with taxonomies that were requested in YML
      taxonomy_mapping = {
          i['displayName']: i['name']
          for i in response['taxonomies']
          if i['displayName'] in requested_taxonomies
      }
      # extract raw list of tags for each taxonomy
      for k, v in taxonomy_mapping.items():
        taxonomy_tag_list_raw = self.__get_taxonomy_policy_tags_raw(v)
        for t in taxonomy_tag_list_raw:
          column_policy_tags_mapping.update(
              self.__get_tag_hierarchy(
                  taxonomy_name=k, raw_data=taxonomy_tag_list_raw, tag=t))
    else:
      raise AirflowException(
          ('Could not receive a list of taxonomies for '
           f'project {Config.GCP_PROJECT_ID}. Check security configuration '
           'for service account.')
      )

    # iterate requested tags.
    # raise Exception if taxonomy does not exist in project
    for ct in column_policy_tags:
      for column, tag in ct.items():
        if tag not in column_policy_tags_mapping:
          raise AirflowException(
              (f'Check your YML configuration. Column [{column}] : Tag [{tag}] '
               'does not exist in GCP Data Catalog.')
          )
    # transform array column policy mapping into dictionary with correct tag Ids
    column_policy_tags_resultset = dict()
    for c in column_policy_tags:
      for key in c:
        column_policy_tags_resultset[key] = column_policy_tags_mapping[c[key]]
    return column_policy_tags_resultset

  def __get_tag_hierarchy(self,
                          taxonomy_name: str,
                          raw_data: Any,
                          tag: Dict[str, Any],
                          tag_display_name: str = '',
                          tag_id: str = '') -> Dict[str, Any]:
    """Get Data Catalog Taxonomy tag hierarchy mapping.

    Method performs recursive scan of taxonomy tags hierarchy and creates
    mapping between DataCatalog policy tag id and human readable
    representation of this tag in format similar to 'taxonomy|tag_hierarchy'

    Args:
      taxonomy_name (string): Human readable taxonoy name from
        [column_policy_tags] attribute defined in task YML raw_data.
      raw_data: Raw json response from DataCatalog Rest API.
      tag (dict): Rest API definition of policy tag. More details about format
        of dictionary yoou can find here:
        https://cloud.google.com/data-catalog/docs/reference/rest/v1/projects.locations.taxonomies.policyTags#PolicyTag
      tag_display_name (string): Tag name in human readable format
        'parent_tag_1|parent_tag_1.1|tag'
      tag_id (string): Tag id in formnat supported by Data Catalog Rest API.
        projects/{project}/locations/{location}/taxonomies/{taxonomies}/policyTags/{policytag}

    Returns:
      (dict): List of column policy tag definition in format
        {'taxonomy_name|tag_display_name': 'tag_id'}
        For example:
        {
          'proto_column_access_policy|PII|high':
            'projects/prj/locations/us/taxonomies/11/policyTags/22'
        }
    """
    # parse raw taxonomy data and return tag hierarchy
    parent_id = tag.get('parentPolicyTag', None)
    tag_id = tag_id if tag_id else tag['name']
    tag_display_name = '|'.join([tag['displayName'], tag_display_name
                                ]) if tag_display_name else tag['displayName']
    # if tag not in a root of hierarchy
    if parent_id:
      # get parent tag details
      parent_tag = list(filter(lambda x: x['name'] == parent_id, raw_data))[0]
      return self.__get_tag_hierarchy(
          taxonomy_name=taxonomy_name,
          raw_data=raw_data,
          tag=parent_tag,
          tag_display_name=tag['displayName'],
          tag_id=tag_id)
    else:
      return {taxonomy_name + '|' + tag_display_name: tag_id}

  def __get_taxonomy_policy_tags_raw(self,
                                     taxonomy_id: str) -> List[Dict[str, Any]]:
    """Get a list of all policy tags inside Data Catalog Policy Tags taxonomy.

    Next Rest API call is used
    https://cloud.google.com/data-catalog/docs/reference/rest/v1/projects.locations.taxonomies.policyTags/list


    Args:
      taxonomy_id (string): Taxonomy id in format acceptable by Rest API
        projects/{project}/locations/{location}/taxonomies/{taxonomies}

    Raises:
      AirflowException: Raise exception in case if Data Catalog Rest API not
        able to retrieve list of tags inside taxonomy.

    Returns:
      (list(dict)): List of policy tags in format
        https://cloud.google.com/data-catalog/docs/reference/rest/v1/projects.locations.taxonomies.policyTags#PolicyTag
    """
    api_call = f'{taxonomy_id}/policyTags'
    session_url = self.base_api_url.format(api_call=api_call)
    r = self.authed_http.urlopen(method='GET', url=session_url)

    if r.status == 200:
      response = json.loads(r.data)
    else:
      raise AirflowException(
          f'Could not receive a tag list for taxonomy {taxonomy_id}.')
    return response['policyTags']

  def set_column_policy_tags(self, target_table: str) -> None:
    """Update column policy tags on target table.

    Assign Column policy tags from [self.column_policy_tags] to table columns on
    a base of column level security defined in attribute [column_policy_tags] of
    task YML file.

    Args:
      target_table (string): Name of a table on which you want to setup column
        level security.
    """
    if self.column_policy_tags:
      target_table = parse_table(target_table)
      table_schema_definition = self.execution_context.bq_cursor.get_schema(
          dataset_id=target_table['dataset_id'],
          table_id=target_table['table_id'])['fields']
      tagged_column_list = [*self.column_policy_tags
                           ]  # get list of tagged columns from dictionaryy
      # filter only columns that tagged
      # iterate schema and set policy tags
      for i in range(len(table_schema_definition)):
        cn = table_schema_definition[i]['name']
        if cn in tagged_column_list:
          table_schema_definition[i]['policyTags'] = {
              'names': [self.column_policy_tags[cn]]
          }
      # patch target table with updated fields
      self.execution_context.bq_cursor.patch_table(
          dataset_id=target_table['dataset_id'],
          table_id=target_table['table_id'],
          schema=table_schema_definition)
    return

  def set_table_tags(self, target_table: str) -> None:
    """Set DataCatalog tags on a table and table columns.

    Apply tags from self.datacatalog_tags. All tags that were not defined in
    JSON tag configuration file will be removed.

    Args:
      target_table (string): Target table  for which data catalog tags should
        be assigned.

    Raises:
      Exception: Exception raised in case if Rest API does not rreturn Data
        Catalog EntityId for requested table.
      AirflowException: Also exception raised in case if application is not
        able to delete or create tags due some security restriction or other
        issues.
    """
    if self.datacatalog_tags:
      # get entry_id for target_table
      entry_id = self.__get_table_entry_id(target_table)
      # parse entry_id
      entry_id_parsed = re.match(
          (r'^projects/(?P<project_id>.+)/locations/(?P<location>.+)/'
           r'entryGroups/(?P<entry_group>.+)/entries/(?P<entry_id>.+)$'),
          entry_id.name)
      if not entry_id_parsed:
        raise AirflowException(
            f'Could not extract entity_id for [{target_table}].')
      # get a list of tags already assigned to table
      existing_table_tags = self.dc_hook.list_tags(
          location=entry_id_parsed['location'],
          entry_group=entry_id_parsed['entry_group'],
          entry=entry_id_parsed['entry_id'],
          project_id=entry_id_parsed['project_id'],
          page_size=500)
      # construct a list of (template, column) for requested tags
      requested_tags = [
          (t['template'], t.get('column', '')) for t in self.datacatalog_tags
      ]
      # drop existing tags in case of importance
      for et in existing_table_tags:
        tag_name = et.name
        tag_template = et.template
        tag_column = getattr(et, 'column', '')

        if (tag_template, tag_column) in requested_tags:
          # drop existing tag first for avoid ERROR 409
          api_call = f'{tag_name}'
          session_url = self.base_api_url.format(api_call=api_call)
          r = self.authed_http.urlopen(method='DELETE', url=session_url)
          if r.status != 200:
            raise AirflowException(
                (f'Could not delete tag from table table.\n'
                 f'ERROR:  {r.status} - {r.data}')
            )

      for tag in self.datacatalog_tags:
        api_call = f'{entry_id.name}/tags'
        session_url = self.base_api_url.format(api_call=api_call)
        session_body = json.dumps(tag)
        r = self.authed_http.urlopen(
            method='POST', url=session_url, body=session_body)
        if r.status != 200:
          raise AirflowException(
              (f'Could not create new tag on target table. {tag} \n'
               f'ERROR:  {r.status} - {r.data}')
          )
    return
