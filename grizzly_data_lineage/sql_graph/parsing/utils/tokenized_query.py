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

"""Tokenizable Query Module."""
from typing import List

import mo_parsing
import mo_sql_parsing
from sql_formatter.core import format_sql

from sql_graph.typing import JsonDict
from sql_graph.typing import TGraph
from sql_graph.typing import TQuery


class TokenizedQuery:
  """Class representing a singe query.

  Query supports multistep syntax and has tokenizing logic.

  Attributes:
    raw_query(str): raw query text.
    target_table(str): name of the query's last step target table.
    domain (str): name of the query's domain.
    _tokenized_step_info_list (List[TokenizedStepInfo]): list of tokenized steps
      with table name and tokenized query.
  """

  class TokenizedStepInfo:
    """Data structure representing a tokenized step."""

    def __init__(self, name: str,
                 query: JsonDict,
                 temporary: bool = False) -> None:
      self.table_name = name
      self.temporary = temporary
      self.query = query

  @staticmethod
  def _remove_comments_and_split_steps(sql: str) -> List[str]:
    """Prepares query for tokenizing.

    Removes comments and splits query into a list of steps.
    Accounts for stings and escape characters inside query.

    Args:
      sql (str): string text of the query.
    Returns:
      (List[str]): list of query steps.
    """
    sql = sql.lower()

    query_steps = []
    current_step = ""

    inside_single_line_comment = False
    inside_multi_line_comment = False
    inside_string = False
    string_opening_char = ""
    prev_char = ""
    current_char_escaped = False

    current_char_index = 0
    while current_char_index < len(sql):
      # iterating through all chars individually
      # each iteration considers current and previous chars
      current_char = sql[current_char_index]
      if inside_single_line_comment:
        # until newline is reached, ignore all chars
        if current_char == "\n":
          # exit single-line comment mode
          inside_single_line_comment = False
      elif inside_multi_line_comment:
        # until */ is reached, ignore all chars
        if prev_char == "*" and current_char == "/":
          # exit multi-line comment mode
          inside_multi_line_comment = False
      elif inside_string:
        if not current_char_escaped:
          # if current char was not escaped, treat it like a regular char
          if current_char == "\\":
            # if current char is \ escape the next char
            current_char_escaped = True
          elif current_char == string_opening_char:
            # if matching closing quote encountered, exit string mode
            inside_string = False
            string_opening_char = ""
        else:
          # reset escaped char flag
          current_char_escaped = False
        current_step += current_char  # chars inside strings are added to sql
      else:
        # if outside comments or strings
        if current_char == prev_char == "-" or current_char == "#":
          # enter single-line comment mode
          inside_single_line_comment = True
          current_step = current_step[:-1]
        elif current_char == "*" and prev_char == "/":
          # enter multi-line comment mode
          inside_multi_line_comment = True
          current_step = current_step[:-1]
        elif current_char == "'" or current_char == "\"":
          # enter string mode and record opening quote symbol
          inside_string = True
          string_opening_char = current_char
          current_step += current_char  # quotes are added to sql
        elif current_char == ";":
          # if ; is encountered, finish current step and start new step
          query_steps.append(current_step)
          current_step = ""
        elif current_char == "\n" and (prev_char == "\n" or prev_char == ";"):
          # ignore newlines after ; and on empty lines
          pass
        else:
          current_step += current_char  # all other chars are added to sql
      prev_char = current_char
      current_char_index += 1

    if current_step != "":
      # finish recording last step if it wasn't done
      query_steps.append(current_step)
    return query_steps

  def _tokenize_query(self, query_steps: List[str]) -> List[JsonDict]:
    """Splits query into single steps and tries to tokenize each of them.

    Args:
      query_steps ():
    Returns:
      List[JsonDict]: list of tokenized steps.
    """

    def _get_step_preview(s) -> str:
      """Truncates step for debug."""
      step_preview = s.replace("\n", " ")
      step_preview = step_preview[:min(len(step_preview), 50)]
      return step_preview

    def _trace_tokenizer_error(s, e):
      """Logs the error of tokenizer."""
      step_preview = _get_step_preview(s)
      print(f"Could not parse step '{step_preview}...' of {str(self)}"
            f"\n\tdue to tokenizer error: {e}"
            f"\n\tSkipping step")

    def _trace_formatter_error(s, e_t, e_f):
      """Logs errors of both tokenizer and formatter."""
      step_preview = _get_step_preview(s)
      print(f"Could not parse step '{step_preview}...' of {str(self)}"
            f"\n\tdue to tokenizer error: {e_t}"
            f"\n\tand formatter error {e_f}"
            f"\n\tSkipping step")

    tokenized_steps = []
    for step in query_steps:
      try:
        if step:
          # first, attempt to parse step as is
          tokenized_step = mo_sql_parsing.parse_bigquery(step)
          tokenized_steps.append(tokenized_step)
      except mo_parsing.ParseException as tokenizer_error1:
        try:
          # if parsing failed, try to fix the step by formatting it
          formatted_step = format_sql(step).lower()
          try:
            # if formatting was successful, retry parsing
            tokenized_step = mo_sql_parsing.parse_bigquery(formatted_step)
            tokenized_steps.append(tokenized_step)
          except mo_parsing.ParseException as tokenizer_error2:
            # if parsing fails again, skip the step and log the error
            _trace_tokenizer_error(formatted_step, tokenizer_error2)
        except Exception as formatter_error:
          # if formatting fails, skip the step and log the error
          _trace_formatter_error(step, tokenizer_error1, formatter_error)
    return tokenized_steps

  def _process_other_step(self, step: JsonDict) -> None:
    """Processes step other than final step and adds it to the list.

    Currently, only supports steps with create table syntax.

    Args:
      self (JsonDict): tokenized step.
    """
    if len(step) == 1 and "create table" in step:
      raw_step_info = step["create table"]
      step_info = self.TokenizedStepInfo(
        name=raw_step_info["name"],
        query=raw_step_info["query"],
        temporary=raw_step_info.get("temporary", False)
      )
      self._tokenized_step_info_list.append(step_info)
    else:
      print(f"Could not process step with keys {list(step.keys())} "
            f"of {str(self)}: unknown command")

  def _process_steps(self, tokenized_steps: List[JsonDict]) -> None:
    """Processes a list of tokenized steps.

    Args:
      tokenized_steps (List[JsonDict]): list of tokenized steps.
    """
    if tokenized_steps:
      *other_steps, final_step = tokenized_steps
      for step in other_steps:
        self._process_other_step(step)
      final_step_info = self.TokenizedStepInfo(name=self.target_table,
                                               query=final_step,
                                               temporary=False)
      self._tokenized_step_info_list.append(final_step_info)

  def __init__(self, query: TQuery, graph: TGraph) -> None:
    self.raw_query = query.raw_query
    self.target_table = query.target_table
    self.domain = query.domain
    self.graph = graph
    self._tokenized_step_info_list = []

    query_steps = self._remove_comments_and_split_steps(self.raw_query)
    tokenized_steps = self._tokenize_query(query_steps)
    self._process_steps(tokenized_steps)

  def __repr__(self) -> str:
    """Representation of the class in print for debug purposes."""
    return f"<Tokenized query for table {self.target_table} in" \
           f" domain {self.domain}>"

  def get_tokenized_steps(self) -> List:
    """Returns the list of tokenized steps."""
    return self._tokenized_step_info_list[:]
