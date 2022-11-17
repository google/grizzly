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

"""SQL Graph Tables.

Classes that represent various types of tables.
There are two major categories of tables:
  physical: tables that are physically stored or defined somewhere.
    Those tables will be stored in graph namespace. This also includes views.
  virtual (not physical): tables that are intermediate byproducts of a query.
    Those tables will be stored in another table's namespace.
"""
from sql_graph.parsing.tables.table import Table
from sql_graph.parsing.tables.select_table import SelectTable
from sql_graph.parsing.tables.external_table import ExternalTable
from sql_graph.parsing.tables.unnest_table import UnnestTable
from sql_graph.parsing.tables.cycle_breaker_table import CycleBreakerTable
from sql_graph.parsing.tables.source_lookup import lookup_source
