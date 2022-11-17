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

"""Descriptions class update descriptions on BQ objects.

  Typical usage example:

  Descriptions.get_sql_statement(
    execution_context= self.execution_context,
    task_config=self.task_config)
"""

import pathlib
from typing import Optional
from google.cloud import bigquery
from grizzly.grizzly_typing import TGrizzlyOperator
from grizzly.grizzly_typing import TGrizzlyTaskConfig
from grizzly.etl_action import parse_table

DESCRIPTION_FAIL_IF_NOT_EXISTS = True

class Descriptions:
  """Descriptions class update descriptions on BQ objects.

  If yaml file of object contains tag
    descriptions:
      table: free-text
      columns:
        col1: free-text
        col2: free-text

  """

  supported_write_mode = ["WRITE_APPEND", 
                          "WRITE_EMPTY", 
                          "WRITE_TRUNCATE", 
                          "CREATE_VIEW", 
                          "UPDATE", 
                          "DELETE"]

  @classmethod
  def update_description(cls,
                        execution_context: TGrizzlyOperator,
                        task_config: TGrizzlyTaskConfig
                        ) -> None:
    """Update descriptions in BQ object.

    This method used to update/delete/add descriptions to BQ object. 

    Args:
      execution_context (TGrizzlyOperator): execution context of task
      task_config (TGrizzlyTaskConfig): task configuration

    Returns:
      None
    """
    target_table = parse_table(task_config.target_table_name)
    target_table = ".".join(target_table.values())

    bq_table = execution_context.bq_client.get_table(target_table)
    original_schema = bq_table.schema[:]

    table_desirable_desc = task_config.descriptions.get("table", None)

    fail_if_not_exists = DESCRIPTION_FAIL_IF_NOT_EXISTS

    if table_desirable_desc != bq_table.description:
      bq_table.description = table_desirable_desc
      bq_table = execution_context.bq_client.update_table(bq_table, ["description"])
    
    if task_config.descriptions and "columns" in task_config.descriptions:
      desirable_cols_desc = {k.lower():v for k,v in task_config.descriptions["columns"].items() }

      if fail_if_not_exists:
        orig_columns = [col.name.lower() for col in original_schema]
        notfound_columns = [k for k,v in desirable_cols_desc.items() if k.lower() not in orig_columns]

        if notfound_columns:
          notfound_columns_st = ", ".join(notfound_columns)
          msg = f"The [{notfound_columns_st}] columns in descriptions.columns section are not found."
          execution_context.log.info(msg)
          execution_context.log.info("The ETL step will be stopped. But the data is already processed. ")
          raise Exception(f"[{notfound_columns_st}] should be presented in the {target_table} table.")

      new_schema = []
      for col in original_schema:
        column_data = col.to_api_repr()
        column_name = column_data["name"]

        if column_name.lower() in desirable_cols_desc:
          column_data["description"] = desirable_cols_desc[column_name.lower()]
        elif column_name.lower() not in desirable_cols_desc and column_data.get("description", None) is not None:
          column_data["description"] = None

        col = bigquery.SchemaField.from_api_repr(column_data)
        new_schema.append(col)

      bq_table.schema = new_schema
      execution_context.bq_client.update_table(bq_table,["schema"])