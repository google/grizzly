#!/bin/bash
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
-e GRIZZLY_DATA_LINEAGE_PROJECTS="gt10-dev,gt10-uat,gt10-prod" \
-v ~/.config/gcloud/application_default_credentials.json:/tmp/keys/key.json:ro \
${IMAGE_NAME}
