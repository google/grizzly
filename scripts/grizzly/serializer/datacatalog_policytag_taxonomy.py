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
"""Definition of DataCatalog PolicyTag taxonomy serializer class.

Could be used for import/export policy tag taxonomies from proto files.

Typical usage example:
  t = DataCatalogPolicyTagTaxonomy(
        gcp_project_id='MY_GCP_PROJECT_ID,
        location='us-central1')
  t.export_taxonomy(template_path='/tmp/data-catalog-policytag-taxonomy-templates')
  t.import_taxonomy(template_path='/tmp/data-catalog-policytag-taxonomy-templates')
"""

import pathlib
import re

from typing import List, Optional, Union
from deployment_utils import Utils as DeploymentUtils
from google.cloud.datacatalog_v1.services.policy_tag_manager import PolicyTagManagerClient
from google.cloud.datacatalog_v1.services.policy_tag_manager_serialization import PolicyTagManagerSerializationClient
from google.cloud.datacatalog_v1.types import ExportTaxonomiesRequest
from google.cloud.datacatalog_v1.types import ImportTaxonomiesRequest
from google.cloud.datacatalog_v1.types import InlineSource


class DataCatalogPolicyTagTaxonomy:
  """DataCatalog taxonomies in a project in a particular location.

  Attributes:
    _taxonomy_manager_client (PolicyTagManagerClient): Used for receive a list
      of taxonomies on gcp project location.
    _serialization_client (PolicyTagManagerSerializationClient): Used for
      receive of Policy Tag taxonomy definitions.
    FILE_EXTENSION (str): File name extension for proto files.
    gcp_project_id (str): GCP project Id.
    location (str): GCP Composer environment location.
    parent_resource (str): Resource name of the project.
        It takes the form [projects/{project}/locations/{location}].
  """

  _taxonomy_manager_client = PolicyTagManagerClient()
  _serialization_client = PolicyTagManagerSerializationClient()
  FILE_EXTENSION = '.gcp_proto'

  def __init__(self,
               gcp_project_id: str,
               location: str,
              ) -> None:
    self.gcp_project_id = gcp_project_id
    self.location = location
    self.parent_resource = f'projects/{gcp_project_id}/locations/{location}'

  def get_taxonomy_list(self,
                        parent_resource: Optional[str] = None) -> List[str]:
    """Return a list of taxonomies.

    Args:
      parent_resource (str): Resource name of the project to list the
        taxonomies of.
        It takes the form [projects/{project}/locations/{location}].

    Returns:
      List[str]: List of taxonomies.
    """
    if not parent_resource:
      parent_resource = self.parent_resource
    taxonomies = self._taxonomy_manager_client.list_taxonomies(
        parent=parent_resource)
    return taxonomies

  def export_taxonomy(self,
                      template_path: Union[str, pathlib.Path]) -> None:
    """Export DataCatalog Policy Tag taxonomies to proto files.

    Args:
      template_path (Union[str, pathlib.Path]): Destination folder where
        DataCatalog Policy Tag taxonomies will be exported.
    """
    if isinstance(template_path, str):
      template_path = pathlib.Path(template_path)
    # prepare directory for taxonomy template files
    template_path.mkdir(parents=True, exist_ok=True)
    # Get list of taxonomies to be exported
    taxonomy_list = [
        t.name
        for t in self.get_taxonomy_list(self.parent_resource)
    ]
    request = ExportTaxonomiesRequest(
        parent=self.parent_resource,
        taxonomies=taxonomy_list,
        serialized_taxonomies=True
    )
    taxonomy_response = self._serialization_client.export_taxonomies(
        request=request)
    for i in range(len(taxonomy_response.taxonomies)):
      # Replace gcp_project_id in taxonomy display_name
      display_name = re.sub('^%s-' % self.gcp_project_id,
                            '{{ GCP_PROJECT_ID }}-',
                            taxonomy_response.taxonomies[i].display_name)
      taxonomy_response.taxonomies[i].display_name = display_name

    proto_file = pathlib.Path(template_path,
                              self.location).with_suffix(self.FILE_EXTENSION)
    # If gcp_project_id location has DataCatalog taxonomies defined create file
    if taxonomy_response.taxonomies:
      file_name = DeploymentUtils.proto_save(
          obj=taxonomy_response,
          class_message=type(taxonomy_response),
          file_name=proto_file.name,
          path=template_path
      )
      print('Taxonomy template file was generated:', file_name)
    else:
      # Remove template file from folder if taxonomy was not defined
      # for gcp_project_id/location
      if proto_file.exists():
        proto_file.unlink()
        print('Taxonomy template file was removed:', proto_file)
      else:
        print(f'Skipping [{self.location}].')
    return

  def clean_taxonomy(self, scope: List[str]) -> None:
    """Remove DataCatalog Policy Tags taxonomies on a project/location.

    Args:
      scope (List[str]): List of display names of taxonomies to be imported
        from template file.
    """
    taxonomy_list = self.get_taxonomy_list(self.parent_resource)
    for t in taxonomy_list:
      # MERGE mode. Remove only taxonomies defined in template file.
      if t.display_name in scope:
        self._taxonomy_manager_client.delete_taxonomy(name=t.name)
    return

  def import_taxonomy(self,
                      template_path: Union[str, pathlib.Path]) -> None:
    """Import DataCatalog Policy Tag taxonomies to proto files.

    Copy template files to temp folder. Replace placeholder {{ GCP_PROJECT_ID }}
    with gcp_project_id. Then import template file into
    GCP DataCatalog Policy Tags

    Args:
      template_path (Union[str, pathlib.Path]): Destination folder where
        DataCatalog Policy Tag taxonomies will be exported.
    """
    if isinstance(template_path, str):
      template_path = pathlib.Path(template_path)
    # Read template file
    template_file = template_path / f'{self.location}{self.FILE_EXTENSION}'
    if template_file.exists():
      # Proceed only in case if template exists for requested location.
      print(f'Importing file [{template_file}]')
      taxonomy_template = DeploymentUtils.proto_load(
          path=template_path,
          file_filter=f'{self.location}{self.FILE_EXTENSION}',
          proto_class=InlineSource)
      # in case of taxonomies we are working only with one file per location
      taxonomy_template = taxonomy_template[str(template_file)]
      # Update display_name. Replace {{ GCP_PROJECT_ID }} with gcp_project_id
      for i in range(len(taxonomy_template.taxonomies)):
        display_name = taxonomy_template.taxonomies[i].display_name.replace(
            '{{ GCP_PROJECT_ID }}',
            self.gcp_project_id
        )
        taxonomy_template.taxonomies[i].display_name = display_name
      taxonomy_display_names = [
          t.display_name
          for t in taxonomy_template.taxonomies
      ]
      self.clean_taxonomy(scope=taxonomy_display_names)
      request = ImportTaxonomiesRequest(
          parent=self.parent_resource,
          inline_source=taxonomy_template
      )
      self._serialization_client.import_taxonomies(
          request=request)
    else:
      print(f'No template file for location: {self.location}')
    return
