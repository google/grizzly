# Grizzly

The Grizzly DataOps platform is a deployable blueprint to create and operate an analyst-first data warehouse with minimal engineering support.  It provides a simple Git .yml and .sql file interface as the single pane of glass to configure and run the following components in concert:
- [BigQuery](https://cloud.google.com/bigquery) including its database, [function](https://cloud.google.com/bigquery/docs/reference/standard-sql/functions-and-operators), [assert](https://cloud.google.com/bigquery/docs/reference/standard-sql/debugging-statements), [table-level security](https://cloud.google.com/bigquery/docs/table-access-controls), [column-level security](https://cloud.google.com/bigquery/docs/column-level-security-intro), [row-level security](https://cloud.google.com/bigquery/docs/row-level-security-intro), and [machine learning](https://cloud.google.com/bigquery-ml/docs/introduction) functionality.
- [Cloud Composer](https://cloud.google.com/composer) as a complete end-to-end data movement/integration tool for BigQuery, Cloud Spanner, CloudSQL, Cloud Storage, and Google Sheets.
- [Cloud Data Loss Prevention](https://cloud.google.com/dlp) to detect and mask or remove sensitive data automatically.
- [Cloud Pub/Sub](https://cloud.google.com/pubsub) to provide outbound event-driven data and streaming analytics.
- [Cloud Source Repositories](https://cloud.google.com/source-repositories) to control code.
- [Cloud Storage](https://cloud.google.com/storage) to provide object and file storage.
- [Data Catalog](https://cloud.google.com/data-catalog) to provide data taxonomy and data discovery.
- [Dataflow](https://cloud.google.com/dataflow) to provide inbound streaming data.


Please see the [Documentation](./documentation/) to get started.

![](./documentation/images/grizzly_architecture.png)

## License
    Copyright 2022 Google LLC

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
