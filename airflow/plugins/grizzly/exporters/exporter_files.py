# Copyright 2021 Google LLC
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

"""Implementation of file export functionality.

  This class is used by ETLFactory.
"""

from typing import Any, Optional

from airflow.configuration import conf
from airflow.providers.google.cloud.hooks.pubsub import PubSubHook
import grizzly.etl_action
from grizzly.exporters.base_exporter import BaseExporter
from grizzly.grizzly_typing import TGrizzlyOperator
from grizzly.grizzly_typing import TGrizzlyTaskConfig


GS_BUCKET = conf.get('logging',
                     'remote_base_log_folder').replace('/logs',
                                                       '').replace('gs://', '')


class ExporterFiles(BaseExporter):
  """Implementation of ETL for export BQ data to csv files on GCS."""

  def __init__(self,
               execution_context: Optional[TGrizzlyOperator] = None,
               task_config: Optional[TGrizzlyTaskConfig] = None,
               write_disposition: Optional[str] = None) -> None:
    super().__init__(
        execution_context=execution_context,
        task_config=task_config,
        write_disposition=write_disposition)

  def load(self, data: Any) -> None:
    """Load result of BQ query into csv file stored in GCS.

    Run query defined in [stage_loading_query] attribute of task YML file.
    Export script result into CSV file.
    Once file generated method publish notification into PubSub topic defined in
    [notification_pubsub] attribute of task YML file.

    Args:
      data (dict): Empty placeholder from base extract and transformation
        methods.
    """
    # Run query
    self.job_stat = grizzly.etl_action.run_bq_query(
        execution_context=self.execution_context,
        sql=self.task_config.stage_loading_query)

    table = '{project_id}.{dataset_id}.{table_id}'.format(
        project_id=self.job_stat.destination.project,
        dataset_id=self.job_stat.destination.dataset_id,
        table_id=self.job_stat.destination.table_id
    )

    delimiter = ','
    print(self.task_config.export_config)
    if self.task_config.export_config:
      if 'delimiter' in self.task_config.export_config:
        delimiter = self.task_config.export_config['delimiter']

    file = self.task_config.get_context_value('task').task_id
    domain = self.execution_context.dag_id

    export_file = f'gs://{GS_BUCKET}/data/EXPORT/{domain}/{file}.csv'

    self.execution_context.bq_hook.run_extract(
        source_project_dataset_table=table,
        destination_cloud_storage_uris=[export_file],
        field_delimiter=delimiter)

    print(f'File exported: {export_file}')

    file_name_bytes = export_file.encode()
    message = {'data': file_name_bytes}

    pubsub: PubSubHook = PubSubHook()
    pubsub.publish(
        topic=self.task_config.notification_pubsub,
        messages=[message],
        project_id=self.task_config.gcp_project_id)

    print(f'Message is published: {message}')

    print(self.job_stat)
