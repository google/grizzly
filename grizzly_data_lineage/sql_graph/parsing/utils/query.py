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

"""Query data structure that is used during graph init."""
from typing import Dict
from typing import Optional


class Query:
  """Query data structure that contains basic information.

  Attributes:
    raw_query (str): raw SQL query string.
    target_table (str): name of the target table of a query.
    domain (str): name of the domain of the query.
    job_write_mode (str): job write mode of the query.
    descriptions (Dict): dictionary with table and column descriptions.
  """

  def __init__(self, query: str, target_table: str, domain: str,
               job_write_mode: str, descriptions: Optional[Dict]) -> None:
    self.raw_query = query
    self.target_table = target_table
    self.domain = domain
    self.job_write_mode = job_write_mode
    self.descriptions = descriptions

  def __repr__(self) -> str:
    """Representation of the class in print for debug purposes."""
    return f"<Query for table {self.target_table} in domain {self.domain}>"
