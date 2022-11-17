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

import os
import subprocess

class Datalineage:

  def __init__(self):
    pass

  @classmethod
  def create_default_md(self,
    metadata_project_gcp: str,
    project_gcp: str,
    domain: str,
    file_path: str):

    cmd_template = "gcloud app browse --project={project_gcp} --service={service}"
    cmd = cmd_template.format(
        project_gcp=metadata_project_gcp,
        service="data-lineage",
        file_path=file_path
    )

    print(cmd)
    result = subprocess.run(cmd, capture_output=True, text=False, shell=True, check=False)
    print(result)
    datalineage_url=str(result.stdout.decode("utf-8")).replace("\n","")

    url_template = "{datalineage_url}/?type=DOMAIN+LEVEL&project={project}&datetime=latest&domain={domain}"
    url = url_template.format(
        datalineage_url=datalineage_url,
        project=project_gcp,
        domain=domain
    )

    default_md_template = "[Domain level data lineage for {domain}]({url})"
    default_md_text = default_md_template.format(
        domain=domain,
        url=url
    )
    default_md = open(file_path,"w+")
    default_md.write(default_md_text)
    default_md.close()