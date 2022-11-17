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

set -e


while getopts ":b" option; do
   case $option in
      b) # rebuild docker
         export BUILD_IMAGE=1;;
     \?) # Invalid option
         echo "Error: Invalid option"
         exit;;
   esac
done

export IMAGE_NAME="grizzly_sql_graph_flask"

if [ ${BUILD_IMAGE} == 1 ]; then
  sudo docker build . -t ${IMAGE_NAME}
fi

sudo docker run \
--rm \
-p 8080:8080 \
-e GOOGLE_APPLICATION_CREDENTIALS=/tmp/keys/key.json \
-e GRIZZLY_DATA_LINEAGE_PROJECTS="grizzly-dev,grizzly-uat,grizzly-prod" \
-v ~/.config/gcloud/application_default_credentials.json:/tmp/keys/key.json:ro \
${IMAGE_NAME}
