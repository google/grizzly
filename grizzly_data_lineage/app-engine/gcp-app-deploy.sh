#!/bin/bash
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

cat <<EOF > app.yaml
runtime: custom
env: flex

service: $4

env_variables:
  GRIZZLY_DATA_LINEAGE_PROJECTS: "$1"
EOF

cat <<EOF > Dockerfile
from $2
EOF

gcloud app deploy --project $3