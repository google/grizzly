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


# Before deployment execute
### In case of importance configure GIT
# git config --global user.email "email@mail.com"
# git config --global user.name "Your Name"
# gh auth login
# sudo rm -rf ~/grizzly_repo && mkdir ~/grizzly_repo
# cd ~/grizzly_repo
# gh repo clone google/grizzly


Help()
{
    # Display Help
    echo "Init grizzly GCP projects."
    echo
    echo "Define folder structure for further deploymnet by terraform."
    echo "Create all required repositories."
    echo
    echo "Syntax: init_grizzly_environment_from_scratch.sh [-m|d|u|p|l|c|h]"
    echo "options:"
    echo "  -m     GCP METADATA project."
    echo "  -d     GCP DEV project."
    echo "  -u     GCP UAT project."
    echo "  -p     GCP PROD project."
    echo "  -l     GCP Composer Airflow instance location."
    echo "  -c     GCP Composer image."
    echo "  -s     Defauls security user or group. In format:"
    echo "          user:joe@example.com"
    echo "          group:v2-demo@google.com"
    echo "  -h     Print this Help."
    echo
}

############################################################
############################################################
# Main program                                             #
############################################################
############################################################
GCP_PROJECT_METADATA=${GCP_PROJECT_METADATA:-NA}
GCP_PROJECT_DEV=${GCP_PROJECT_DEV:-NA}
GCP_PROJECT_UAT=${GCP_PROJECT_UAT:-NA}
GCP_PROJECT_PROD=${GCP_PROJECT_PROD:-NA}
AIRFLOW_LOCATION=${AIRFLOW_LOCATION:-NA}
COMPOSER_IMAGE=${COMPOSER_IMAGE:-NA}
SECURITY_USER=${SECURITY_USER:-NA}
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
if [[ "$GCP_PROJECT_METADATA" == "" ]] || [[ "$GCP_PROJECT_METADATA" == "NA" ]]; then
    echo 'Incorrect GCP_PROJECT_METADATA was provided'  1>&2
    exit 64
fi
if [[ "$GCP_PROJECT_DEV" == "" ]] || [[ "$GCP_PROJECT_DEV" == "NA" ]]; then
    echo 'Incorrect GCP_PROJECT_DEV was provided'  1>&2
    exit 64
fi
if [[ "$GCP_PROJECT_UAT" == "" ]] || [[ "$GCP_PROJECT_UAT" == "NA" ]]; then
    echo 'Incorrect GCP_PROJECT_UAT was provided'  1>&2
    exit 64
fi
if [[ "$GCP_PROJECT_PROD" == "" ]] || [[ "$GCP_PROJECT_PROD" == "NA" ]]; then
    echo 'Incorrect GCP_PROJECT_PROD was provided'  1>&2
    exit 64
fi
if [[ "$AIRFLOW_LOCATION" == "" ]] || [[ "$AIRFLOW_LOCATION" == "NA" ]]; then
    echo 'Incorrect AIRFLOW_LOCATION was provided'  1>&2
    exit 64
fi
if [[ "$COMPOSER_IMAGE" == "" ]] || [[ "$COMPOSER_IMAGE" == "NA" ]]; then
    echo 'Incorrect COMPOSER_IMAGE was provided'  1>&2
    exit 64
fi
if [[ "$SECURITY_USER" == "" ]] || [[ "$SECURITY_USER" == "NA" ]]; then
    SECURITY_USER="user:$(git config user.email)"
    echo "$SECURITY_USER account will be used as default in BQ security scripts"
fi
set -e
set -x

sudo rm -rf ~/grizzly && mkdir ~/grizzly
mkdir ~/grizzly/metadata
mkdir ~/grizzly/dev
mkdir ~/grizzly/uat
mkdir ~/grizzly/prod

# copy framework code into environment directories
cp -rf ~/grizzly_repo/grizzly ~/grizzly/metadata/grizzly_framework/
cp -rf ~/grizzly_repo/grizzly ~/grizzly/dev/grizzly_framework/
cp -rf ~/grizzly_repo/grizzly ~/grizzly/uat/grizzly_framework/
cp -rf ~/grizzly_repo/grizzly ~/grizzly/prod/grizzly_framework/

pip3 install -r ~/grizzly/metadata/grizzly_framework/requirements.txt

# prepare terraform variable files
cat > ~/grizzly/metadata/grizzly_framework/terraform/metadata/main.auto.tfvars <<EOF
gcp_project_id = "$GCP_PROJECT_METADATA"
managed_gcp_project_list = ["$GCP_PROJECT_DEV", "$GCP_PROJECT_UAT", "$GCP_PROJECT_PROD"]
repository_name = "grizzly"
EOF
cat > ~/grizzly/dev/grizzly_framework/terraform/main.auto.tfvars <<EOF
composer_environment_name = "dev"
composer_image_version = "$COMPOSER_IMAGE"
gcp_project_id = "$GCP_PROJECT_DEV"
composer_location = "$AIRFLOW_LOCATION"
composer_node_zone = "$AIRFLOW_LOCATION-c"
default_datacatalog_taxonomy_location = "us"
EOF
cat > ~/grizzly/uat/grizzly_framework/terraform/main.auto.tfvars <<EOF
composer_environment_name = "uat"
composer_image_version = "$COMPOSER_IMAGE"
gcp_project_id = "$GCP_PROJECT_UAT"
composer_location = "$AIRFLOW_LOCATION"
composer_node_zone = "$AIRFLOW_LOCATION-c"
default_datacatalog_taxonomy_location = "us"
EOF
cat > ~/grizzly/prod/grizzly_framework/terraform/main.auto.tfvars <<EOF
composer_environment_name = "prod"
composer_image_version = "$COMPOSER_IMAGE"
gcp_project_id = "$GCP_PROJECT_PROD"
composer_location = "$AIRFLOW_LOCATION"
composer_node_zone = "$AIRFLOW_LOCATION-c"
default_datacatalog_taxonomy_location = "us"
EOF

# Create GIT repo in case of importance
repo_name=""
repo_name=$(gcloud source repos list --project=$GCP_PROJECT_METADATA --filter=name=projects/$GCP_PROJECT_METADATA/repos/grizzly --format="value(name)")
# if grizzly repository does not exists on metadata gcp project
if [ -z "$repo_name" ]
then
    # Init GIT and copy example code.
    sudo rm -rf /tmp/grizzly_example && mkdir /tmp/grizzly_example
    cd /tmp/grizzly_example
    git init
    git checkout -b dev
    cp ~/grizzly/metadata/grizzly_framework/.gitignore /tmp/grizzly_example/.gitignore
    cp -a ~/grizzly/metadata/grizzly_framework/grizzly_example/. /tmp/grizzly_example/
    cat > /tmp/grizzly_example/ENVIRONMENT_CONFIGURATIONS.yml <<EOF
# DEV environment configuration
dev:
  GCP_ENVIRONMENT: $GCP_PROJECT_DEV
  AIRFLOW_ENVIRONMENT: dev
  AIRFLOW_LOCATION: $AIRFLOW_LOCATION
# UAT environment configuration
uat:
  GCP_ENVIRONMENT: $GCP_PROJECT_UAT
  AIRFLOW_ENVIRONMENT: uat
  AIRFLOW_LOCATION: $AIRFLOW_LOCATION
# Prod environment configuration
prod:
  GCP_ENVIRONMENT: $GCP_PROJECT_PROD
  AIRFLOW_ENVIRONMENT: prod
  AIRFLOW_LOCATION: $AIRFLOW_LOCATION
EOF
    # substitute default user in BQ security scripts
    find . -name "*.sql" -exec sed -i -e "s/{{{DEFAULT_USER}}}/$SECURITY_USER/g" {} \;
    # init branches
    git add .
    git commit -m "Init Grizzly repo"
    git checkout -b uat
    git checkout -b prod
    git checkout dev
    # Create remote repository on target grizzly METADATA GCP project.
    gcloud source repos create grizzly --project=$GCP_PROJECT_METADATA
    # # Push local changes to remote repository.
    git config --global credential.https://source.developers.google.com.helper gcloud.sh
    git remote add google https://source.developers.google.com/p/$GCP_PROJECT_METADATA/r/grizzly
    git push --all google
else
      echo "GIT $repo_name already exists."
fi

# init grizzly_framework GIT repo on metadata GCP project.
framework_repo_name=""
framework_repo_name=$(gcloud source repos list --project=$GCP_PROJECT_METADATA --filter=name=projects/$GCP_PROJECT_METADATA/repos/grizzly_framework --format="value(name)")
if [ -z "$framework_repo_name" ]
then
    # cleanup framework from grizzly examples.
    cd ~/grizzly/metadata/grizzly_framework/
    git filter-branch --index-filter "git rm -rf --cached --ignore-unmatch ./grizzly_example/" HEAD
    # Init GIT and copy example code.
    # Create remote repository on target grizzly METADATA GCP project.
    gcloud source repos create grizzly_framework --project=$GCP_PROJECT_METADATA
    # # Push local changes to remote repository.
    git config --global credential.https://source.developers.google.com.helper gcloud.sh
    git remote add google https://source.developers.google.com/p/$GCP_PROJECT_METADATA/r/grizzly_framework
    git push --all -f google
else
    echo "GIT $framework_repo_name already exists."
fi
