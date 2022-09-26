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


ENVIRONMENT=${ENVIRONMENT:-NA}
GCP_PROJECT_METADATA=${GCP_PROJECT_METADATA:-NA}
# Get the options
# assign input parameters to variables
while [[ $# -gt 0 ]]; do

   if [[ $1 == *"--"* ]]; then
        param="${1/--/}"
        declare "$param"="$2"
   fi

  shift
done

# check that all parameters where provided
if [[ "$ENVIRONMENT" == "" ]] || [[ "$ENVIRONMENT" == "NA" ]]; then
    echo 'Incorrect ENVIRONMENT was provided'  1>&2
    exit 64
fi
if [[ "$GCP_PROJECT_METADATA" == "" ]] || [[ "$GCP_PROJECT_METADATA" == "NA" ]]; then
    echo 'Incorrect GCP_PROJECT_METADATA was provided'  1>&2
    exit 64
fi

set -e
set -x

cd ~/grizzly/"$ENVIRONMENT"/grizzly_framework/terraform
git pull

terraform init && terraform apply

# copy sample data
cd ./grizzly_example/
terraform init -var-file="../main.auto.tfvars" && terraform apply -var-file="../main.auto.tfvars"

# install Python dependencies
pip3 install -r ~/grizzly/"$ENVIRONMENT"/grizzly_framework/requirements.txt
# run cb triggers
cd ~/grizzly/"$ENVIRONMENT"/grizzly_framework/scripts
python3 ./run_build_triggers.py "$GCP_PROJECT_METADATA" "$ENVIRONMENT"
