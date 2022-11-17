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

from sql_graph.parsing.columns import Column
from sql_graph.typing import TColumn
from sql_graph.typing import TTable
from sql_graph.typing import TokenizedJson


class TableColumn(Column):
  """Representation of regular column located in a table.

  Contains additional parsing logic in init method, as well as copy
    functionality. Also has a value attribute.

  Attributes:
    value (TokenizedJson): value of the column.
  """

  def _parse(self) -> None:
    """Override of _parse method."""
    self._parse_value_json(self.value)
    if self.value is None:
      self._parse_value_json(self.name)
      self.name = self.name.split(".")[-1]

  def __init__(self, name: str, value: TokenizedJson, table: TTable,
               skip_parsing: bool = False) -> None:
    super(TableColumn, self).__init__(name, table)
    self.value = value
    if not skip_parsing:
      self._parse()

  def _get_serializing_id(self) -> str:
    """Overwrite of _get_serializing_id method."""
    return f"{self._serializing_params.get_parent_id()}.{self.name}"

  def copy(self, new_table: TTable, skip_parsing: bool = False) -> TColumn:
    """Copy column for another table.

    Replaces the table attribute and adds self as the source. This method is
    used in case of * syntax.

    Args:
      new_table (Table): new table that will contain new column object.
      skip_parsing (bool): whether the parsing should be skipped.

    Returns:
      Column: new column.
    """
    if not skip_parsing:
      # new value will be parsed by new column, which will add this column
      # as a source
      new_value = f"{self.table.name}.{self.name}"
      c = TableColumn(name=self.name, value=new_value, table=new_table)
    else:
      c = TableColumn(name=self.name, value=None, table=new_table,
                      skip_parsing=TColumn)
    return c
