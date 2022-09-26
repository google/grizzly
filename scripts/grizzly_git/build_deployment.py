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

"""Deploy Build File.

Application generates BUILD file in [/tmp/...] folder.
upload into correspondent [gs://.../DAG/] and [gs://.../data/ETL/<DOMAIN>/]
folder linked with GCP Composer environment.
"""

import pathlib
import tempfile
from grizzly.composer_environment import ComposerEnvironment
import yaml

CURRENT_PATH = pathlib.Path(__file__).resolve().parent
TEMPLATE_PATH = CURRENT_PATH / "templates"


class BuildDeploy:
  """Creating file of domain with information of build domain_BUILD.yml.

  Attributes:
    gcp_project_id (string): GCP project Id.
    gcp_location (string): Compute Engine region in which composer
        environment was created.
    gcp_composer_env_name(string): GCP Composer environment name.
    release (string): Coomit SHA of the current build (latest commit).
    domain (string): Name of domain.
    commit_id (string): Unique name of release
    build_id (string): Unique name of build.
    gcp_composer_environment:
       (grizzly.composer_environment.ComposerEnvironment)  Interract
       with GCP composer and retrieve Environment details.

  """

  def __init__(self,
               project_id: str,
               location: str,
               environment_name: str,
               commit_id: str,
               domain: str,
               release: str = None,
               build_id: str = None) -> None:
    """Init of BuildDeploy.

    Args:
      project_id (string): GCP project Id.
      location (string): Compute Engine region in which composer environment was
          created.
      environment_name(string): GCP Composer environment name.
      commit_id (string): Coomit SHA of the current build (latest commit).
      domain (string): Name of domain.
      release (string): Unique name of release
      build_id (string): Unique name of build.
    """

    self.gcp_project_id = project_id
    self.gcp_location = location
    self.gcp_composer_env_name = environment_name

    self.release = release
    self.build_id = build_id

    self.commit_id = commit_id
    self.domain = domain

    self.gcp_composer_environment = ComposerEnvironment(
        project_id=self.gcp_project_id,
        location=self.gcp_location,
        environment_name=self.gcp_composer_env_name)

    pass

  def create_build(self):
    """Creates file with information about build.
    """

    r = self.release if self.release else self.build_id
    data = {"release": r, "commit_id": self.commit_id}

    domain_name = self.domain.split("/")[-1].upper()

    temp_path = pathlib.Path(
        tempfile.mkdtemp(prefix="AIRFLOW.")) / self.domain

    temp_path.mkdir(parents=True, exist_ok=True)

    file_name = str((temp_path / f"{domain_name}_BUILD.yml"))

    (temp_path / f"{domain_name}_BUILD.yml").write_text(yaml.dump(data))

    self.gcp_composer_environment.publish_file(
        domain=domain_name,
        file=file_name)
