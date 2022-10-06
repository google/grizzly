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