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

"""Cycle breaker table."""
from typing import Set

from sql_graph.parsing.tables import Table
from sql_graph.typing import TTable
from sql_graph.typing import TableLocation


class CycleBreakerTable(Table):
  """Special table class used to break cycles in the graph.

  Copies all info from the source column and links itself to the target table.

  Attributes:
      source_table (Table): table that will be copied.
      target_table (Table): table that CycleBreaker will link to.
  """

  def _relink_references(self) -> None:
    """Relinks references of target table to the CycleBreaker instance."""
    self.target_table.remove_source(self.source_table)
    self.target_table.add_source(self)
    for reference in self.source_table.get_all_references():
      from sql_graph.parsing.columns import Column
      if isinstance(reference, Column) and reference.table == self.target_table:
        reference.replace_sources_from_table(old_table=self.source_table,
                                             new_table=self)

  def _parse(self) -> None:
    """Override of the parsing method to set up the CycleBreaker table.

    Copies all columns from the source table and relinks references of the
    target table.
    """
    for column in self.source_table.columns:
      column_copy = column.copy(new_table=self, skip_parsing=True)
      for source in column_copy.get_sources():
        column_copy.remove_source(source)
      self.columns.add_column(column_copy)
    self._relink_references()

  def __init__(self, source: TTable, target: TTable,
               location: TableLocation) -> None:
    self.source_table = source
    self.target_table = target
    super(CycleBreakerTable, self).__init__(
      name=f"{self.source_table.name}__copy_for__{self.target_table.name}_",
      table_info=None,
      location=location,
      query=self.target_table.query
    )

  def __repr__(self) -> str:
    """Representation of the class in print for debug purposes."""
    return f"<Cycle Breaker Table source={self.source_table} " \
           f"target={self.target_table}>"

  def get_all_sources(self) -> Set[TTable]:
    """Override of get_all_sources to return an empty set.

    CycleBreaker by design has no sources.
    """
    return set()

  def _get_label(self) -> str:
    """Override of the _get_label method to indicate that this is a copy."""
    return self.source_table.name + " (Copy)"
