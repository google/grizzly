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

"""Select table class."""
from typing import Optional
from typing import Tuple

from sql_graph.parsing.tables import Table
from sql_graph.typing import JsonDict
from sql_graph.typing import TokenizedJson


class SelectTable(Table):
  """A table that represents a SELECT statement.

  It is used both for regular SELECT and for special cases, like UNION.
  """

  @staticmethod
  def _extract_alias(source_info: TokenizedJson) -> Tuple[
    Optional[str], TokenizedJson]:
    """Attempts to extract the alias name from source info.

    If alias is present, returns it along with properly formatted source info.
    Otherwise, returns None for alias.

    Args:
      source_info (TokenizedJson): source info JSON.
    Returns:
      Tuple[Optional[str], str]: tuple of alias name and correctly formatted
        source_info JSON.
    """
    if isinstance(source_info, dict) and ("name" in source_info
                                          and "value" in source_info):
      return source_info["name"], source_info["value"]
    else:
      return None, source_info

  @staticmethod
  def _check_for_virtual_table(source_info: TokenizedJson) -> bool:
    """Checks if a virtual table is present in a tokenized JSON"""
    virtual_table_tags = ["select", "select_distinct", "union", "union_all",
                          "unnest"]
    if isinstance(source_info, dict):
      return any([t in virtual_table_tags for t in source_info])
    else:
      return False

  @staticmethod
  def _check_for_join(source_info: TokenizedJson) -> bool:
    """Checks if JOIN is present in a tokenized JSON."""
    if isinstance(source_info, dict):
      return any(["join" in k for k in source_info.keys()])
    else:
      return False

  def _parse_virtual_table(self, name: Optional[str],
                           table_info: JsonDict) -> None:
    """Creates a virtual table of the correct type.

    Args:
      name (str or None): name of the table. If it is None, table will be
        anonymous.
      table_info (JsonDict): JSON with the information about the virtual table.
    """
    if "unnest" in table_info:
      virtual_table = self.namespace.create_unnest_table(unnest_name=name,
                                                         table_info=table_info)
    else:
      virtual_table = self.namespace.create_select_table(name, table_info)
    self.add_source(virtual_table)

  def _parse_join(self, join_info: JsonDict) -> None:
    """Parses the join from a JSON."""
    for key in join_info:
      if "join" in key:
        self._parse_source_info(join_info[key])
        self.join_info.add_join(join_info)
        break

  def _parse_source_info(self, source_info: TokenizedJson):
    """Parses JSON with information about a source."""
    alias_name, source_info = self._extract_alias(source_info)
    if isinstance(source_info, str):
      self._add_source_by_name(source_info)
      if alias_name is not None:
        self.namespace.register_alias(alias_name=alias_name,
                                      table_name=source_info)
    elif self._check_for_virtual_table(source_info):
      self._parse_virtual_table(alias_name, table_info=source_info)
    elif self._check_for_join(source_info):
      self._parse_join(source_info)
    else:
      print(f"Unknown source info in {self}: {source_info}")

  def _parse_from(self, from_info: TokenizedJson) -> None:
    """Parses a FROM statement"""
    if not isinstance(from_info, list):
      from_info = [from_info]
    for source_info in from_info:
      self._parse_source_info(source_info)

  def _parse_with(self, with_info: TokenizedJson) -> None:
    """Parses a WITH statement"""
    if isinstance(with_info, dict):
      with_info = [with_info]
    for table in with_info:
      self.namespace.create_select_table(
        name=table["name"],
        table_info=table["value"]
      )

  def _parse(self) -> None:
    """Table generic parsing method."""
    if "with" in self.table_info:
      self._parse_with(self.table_info["with"])
    if "from" in self.table_info:
      self._parse_from(self.table_info["from"])
      self.join_info.parse_joins()

    if "select" in self.table_info:
      self.columns.parse_select(select_type="Select",
                                select_info=self.table_info["select"])
    elif "select_distinct" in self.table_info:
      self.columns.parse_select(select_type="Select Distinct",
                                select_info=self.table_info["select_distinct"])
    elif "union" in self.table_info:
      self.columns.parse_union(union_type="Union",
                               union_info=self.table_info["union"])
    elif "union_all" in self.table_info:
      self.columns.parse_union(union_type="Union All",
                               union_info=self.table_info["union_all"])

    if "where" in self.table_info:
      self.where_info.add_where(self.table_info["where"])

  def __repr__(self):
    """Representation of the class in print for debug purposes."""
    return f"<Select Table '{self.name}'>"
