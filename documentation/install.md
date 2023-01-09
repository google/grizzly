# How to install Grizzly

## Creating Google Cloud Projects

For this platform, you need four Google Cloud
[projects](https://cloud.google.com/resource-manager/docs/cloud-platform-resource-hierarchy#projects)
with the Project IDs as [stem_name]-metadata, [stem_name]-dev, [stem_name]-uat,
and [stem_name]-prod. For example, grizzly-metadata, grizzly-dev, grizzly-uat,
and grizzly-prod. Please note that while it is possible to set a Project's name
and ID as different values, it is important that the name and ID should match
exactly.

Create new projects for this tutorial.

1.  [Create](https://console.cloud.google.com/projectselector2/home/dashboard) the four Google Cloud projects and ensure that you have IAM [roles/owner access](https://cloud.google.com/iam/docs/understanding-roles) for each project.
1.  Make sure that
    [billing is enabled](https://support.google.com/cloud/answer/6293499#enable-billing)
    for your Google Cloud project.

## Costs

This tutorial uses billable components of Google Cloud, including the following:

*   [BigQuery](https://cloud.google.com/bigquery/pricing)
*   [Cloud Build](https://cloud.google.com/build/pricing)
*   [Cloud Composer](https://cloud.google.com/composer/pricing)
*   [Cloud Data Loss Prevention](https://cloud.google.com/dlp/pricing)
*   [Cloud Pub/Sub](https://cloud.google.com/pubsub/pricing)
*   [Cloud Source Repositories](https://cloud.google.com/source-repositories/pricing)
*   [Cloud Storage](https://cloud.google.com/storage/pricing)
*   [Data Catalog](https://cloud.google.com/data-catalog/pricing)
*   [Dataflow](https://cloud.google.com/dataflow/pricing)

Use the [pricing calculator](https://cloud.google.com/products/calculator) to
generate a cost estimate based on your projected usage.

## Enabling Cloud Shell

[Open Cloud Shell](https://console.cloud.google.com/?cloudshell=true).

At the bottom of the Cloud Console, a
[Cloud Shell](https://cloud.google.com/shell/docs/features) session opens and
displays a command-line prompt. Cloud Shell is a shell environment with the
Cloud SDK already installed, including the
[gcloud](https://cloud.google.com/sdk/gcloud/) command-line tool, and with
values already set for your current project. It can take a few seconds for the
session to initialize.

## Setting up your environment

1.  In Cloud Shell, set your Git credentials, clone the source repository, and
    go to the directory for this tutorial.

    ```
    git config --global user.email "abc@example.com"
    ```

    ```
    git config --global user.name "Your Name"
    ```

    ```
    sudo rm -rf ~/grizzly_repo && mkdir ~/grizzly_repo
    ```

    ```
    cd ~/grizzly_repo
    ```

    ```
    gh auth login
    ```

    ```
    gh repo clone google/grizzly ./grizzly
    ```

1.  In Cloud Shell, set the required environment variables.

    *   The [stem_name] value should match the Project IDs you created earlier.
    *   The [composer_location] value should be from a Region Name from
        the list of available
        [Cloud Storage locations](https://cloud.google.com/storage/docs/locations#available-locations).
        Please note that the Grizzly platform does not currently support
        Dual-regions or Multi-regions.
    *   The [gcp_resource_location] value should be "us" or "eu".  Only if the location is outside of the United States or the European Union, the value should match the [composer_location] value.
    *    The [app_engine_location] value should be from a Region Name from
        the list of available [App Engine locations](https://cloud.google.com/appengine/docs/standard/locations).
    *   The [security_user] value is a user or Google group that will be configured for
        Grizzly's demo examples.  Supported formats of the parameter are
        user:abc@example.com or group:abc_group@example.com.

        ```
        cd ~/grizzly_repo/grizzly
        ./tools/init_grizzly_environment_from_scratch.sh \
        --GCP_PROJECT_METADATA "[stem_name]-metadata" \
        --GCP_PROJECT_DEV "[stem_name]-dev" \
        --GCP_PROJECT_UAT "[stem_name]-uat" \
        --GCP_PROJECT_PROD "[stem_name]-prod" \
        --GCP_RESOURCE_LOCATION "[gcp_resource_location]" \
        --AIRFLOW_LOCATION "[composer_location]" \
        --APP_ENGINE_LOCATION "[app_engine_location]" \
        --COMPOSER_IMAGE "composer-2.1.1-airflow-2.3.4" \
        --SECURITY_USER "[security_user]" 
        ```

        For example,

        ```
        cd ~/grizzly_repo/grizzly
        ./tools/init_grizzly_environment_from_scratch.sh \
        --GCP_PROJECT_METADATA "grizzly-metadata" \
        --GCP_PROJECT_DEV "grizzly-dev" \
        --GCP_PROJECT_UAT "grizzly-uat" \
        --GCP_PROJECT_PROD "grizzly-prod" \
        --GCP_RESOURCE_LOCATION "us" \
        --AIRFLOW_LOCATION "us-central1" \
        --APP_ENGINE_LOCATION "us-central" \
        --COMPOSER_IMAGE "composer-2.1.1-airflow-2.3.4" \
        --SECURITY_USER "user:abc@example.com" 
        ```

        Once the script completes, it will create all required terraform
        variable files and initialize the Git repository on the
        [stem_name]-metadata project.

1.  In Cloud Shell, build the [stem_name]-metadata GCP project.

    ```
    cd ~/grizzly/metadata/grizzly_framework/terraform/metadata
    ```

    ```
    terraform init && terraform apply
    ```

    The terraform script takes about 30 minutes to an hour to complete. If you
    see an error, rerun the terraform init && terraform apply command.

1.  In Cloud Shell, build the [stem_name]-dev GCP project.

    ```
    cd ~/grizzly_repo/grizzly/tools
    ./apply_grizzy_terraform.sh \
    --GCP_PROJECT_METADATA "[stem_name]-metadata" \
    --GCP_RESOURCE_LOCATION "[gcp_resource_location]" \
    --ENVIRONMENT dev
    ```
    
    For example,
    
    ```
    cd ~/grizzly_repo/grizzly/tools
    ./apply_grizzy_terraform.sh \
    --GCP_PROJECT_METADATA "grizzly-metadata" \
    --GCP_RESOURCE_LOCATION "us" \
    --ENVIRONMENT dev
    ```

1.  In Cloud Shell, build the [stem_name]-uat GCP project.

    ```
    cd ~/grizzly_repo/grizzly/tools
    ./apply_grizzy_terraform.sh \
    --GCP_PROJECT_METADATA "[stem_name]-metadata" \
    --GCP_RESOURCE_LOCATION "[gcp_resource_location]" \
    --ENVIRONMENT uat
    ```
    
    For example,
    
    ```
    cd ~/grizzly_repo/grizzly/tools
    ./apply_grizzy_terraform.sh \
    --GCP_PROJECT_METADATA "grizzly-metadata" \
    --GCP_RESOURCE_LOCATION "us" \
    --ENVIRONMENT uat
    ```

1.  In Cloud Shell, build the [stem_name]-prod GCP project.

    ```
    cd ~/grizzly_repo/grizzly/tools
    ./apply_grizzy_terraform.sh \
    --GCP_PROJECT_METADATA "[stem_name]-metadata" \
    --GCP_RESOURCE_LOCATION "[gcp_resource_location]" \
    --ENVIRONMENT prod
    ```
    
    For example,
    
    ```
    cd ~/grizzly_repo/grizzly/tools
    ./apply_grizzy_terraform.sh \
    --GCP_PROJECT_METADATA "grizzly-metadata" \
    --GCP_RESOURCE_LOCATION "us" \
    --ENVIRONMENT prod
    ```    

1.  In the [stem_name]-dev project, monitor the first run of each DAG by navigating to
    Composer and then opening the
    Airflow instance.
    <img src="./images/composer.png" width="400" height="400">
    ![](./images/airflow.png)

1.  Once the Airflow DAGs finish, navigate to
    Cloud Build
    in the [stem_name]-metadata project.
    <img src="./images/cloud_build.png" width="400" height="400">

    *   Deploy the demo machine learning model by running the deploy-bigquery trigger
        after setting its SCOPE_FILE value
        to SCOPE_ML.yml.
        ![](./images/deploy_bigquery_trigger1.png)
        ![](./images/deploy_bigquery_trigger2.png)

    *   Import the initial Git metadata into Grizzly's log tables by running the
        import-git-rep trigger
        after setting its Branch value
        to dev.
        ![](./images/import_git_rep_trigger1.png)
        ![](./images/import_git_rep_trigger2.png)

1.  In the [stem_name]-metadata project, enable the column-level data flow visualization. 

    *   Configure the Identity-Aware Proxy.  Add the audience of users (individual people or Google Groups) that should be able to see the visualization.
        ![](./images/data_lineage_installation1.png)
        ![](./images/data_lineage_installation2.png)
        
    *   Configure OAuth.  
        ![](./images/data_lineage_installation3.png)
        ![](./images/data_lineage_installation4.png)
        ![](./images/data_lineage_installation5.png)
        ![](./images/data_lineage_installation6.png)
        ![](./images/data_lineage_installation7.png)
        ![](./images/data_lineage_installation8.png)
        ![](./images/data_lineage_installation9.png)
        
    *   On your desktop, open the column-level data flow visualization in a browser window by entering https://data-lineage-dot-[stem-name]-metadata.uc.r.appspot.com/.  For example, [https://data-lineage-dot-grizzly-metadata.uc.r.appspot.com/](https://data-lineage-dot-grizzly-metadata.uc.r.appspot.com/).
    
        ![](./images/data_lineage_installation10.png)
    
1.  Install and configure the
    [Superset application](https://superset.apache.org/) to see the demo
    dashboards.  This step is optional and can be performed later.

    *   On your Mac, Windows, or Linux desktop,
        [install](https://docs.docker.com/get-docker/) and then open the Docker
        application.
    *   From the command line of your Mac, Windows, or Linux desktop, copy and
        configure the Superset application.

        ```
        # pull stable version 1.5.2
        docker pull apache/superset:1.5.2
        ```

        ```
        # tag stable version 1.5.2 to grizzly_superset_152
        docker tag apache/superset:1.5.2 grizzly_superset_152
        ```

        ```
        # execute grizzly_superset_152 as grizzly_superset_152 container
        docker run -d -p 8080:8088 --name grizzly_superset_152 grizzly_superset_152
        ```

        ```
        # init admin user
        docker exec -it grizzly_superset_152 superset fab create-admin \
        --username admin \
        --firstname superset \
        --lastname admin \
        --email superset@example.com \
        --password admin
        ```

        ```
        # init db upgrade
        docker exec -it grizzly_superset_152 superset db upgrade
        ```

        ```
        # load examples
        docker exec -it grizzly_superset_152 superset load_examples
        ```

        ```
        # load pybigquery lib
        docker exec -it grizzly_superset_152 pip3 install pybigquery
        ```
        
        ```
        # init superset
        docker exec -it grizzly_superset_152 superset init
        ```
        
        ```
        # restart grizzly_superset_152 to pickup bigquery
        docker restart grizzly_superset_152
        ```

    *   On your desktop, open Superset in a browser window by entering
        [http://localhost:8080](http://localhost:8080). Username and password
        are admin.

    *   In the [stem_name]-dev project,
        create a Service Account Key with the BigQuery Data Viewer, BigQuery Job User,
        and BigQuery Read Session User permissions. Download the key as a json
        file to your desktop.
        ![](./images/service_account1.png)
        ![](./images/service_account2.png)
        ![](./images/service_account3.png)
        ![](./images/service_account4.png)
        ![](./images/service_account5.png)
        ![](./images/service_account6.png)

    *   In Superset,
        [create](https://superset.apache.org/docs/databases/bigquery/#connecting-to-bigquery)
        the BigQuery default database connection using the json Service Account Key
        you downloaded in the previous step. The Display Name value should be
        bq_connection instead of the default "Google
        BigQuery".

    *   Using your browser,
        [download](https://github.com/google/grizzly/blob/main/grizzly_example/dashboard_examples.zip)
        the eight example dashboard json files to your desktop.  Using Superset, import
        each dashboard (Settings -> Import Dashboards).
        ![](./images/superset.png)
