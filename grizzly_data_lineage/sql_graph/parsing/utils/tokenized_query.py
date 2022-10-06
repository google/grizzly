"""Tokenizable Query Module."""
import re
from typing import List

import mo_parsing
import mo_sql_parsing

from sql_graph.typing import JsonDict
from sql_graph.typing import TGraph
from sql_graph.typing import TQuery


class TokenizedQuery:
  """Class representing a singe query.

  Query supports multistep syntax and has tokenizing logic.

  Class Attributes:
    MULTISTEP_LINE_SEPARATOR (str): characters used to separate multistep steps.
  Attributes:
    raw_query(str): raw query text.
    target_table(str): name of the query's last step target table.
    domain (str): name of the query's domain.
    _tokenized_step_info_list (List[TokenizedStepInfo]): list of tokenized steps
      with table name and tokenized query.
  """

  class TokenizedStepInfo:
    """Data structure representing a tokenized step."""

    def __init__(self, name: str, query: JsonDict) -> None:
      self.table_name = name
      self.query = query

  MULTISTEP_SEPARATOR = ";\n"

  def _get_tokenizer_friendly_query(self) -> str:
    """Method that will format raw sql to be tokenizer friendly.

    Removes comments and does a number of other substitutions.

    Returns:
      str: tokenizer friendly sql.
    """
    sql = self.raw_query.lower()
    # remove comments
    sql = "\n".join([t for t in sql.split("\n") if not t.startswith("--")])
    # remove multiline comments
    sql = re.sub(r"/\*.*?\*/", "", sql, flags=re.DOTALL)
    # remove empty lines
    sql = "\n".join([t for t in sql.split("\n") if t])
    # TODO: statements below are temporary fixes in order for parser to work
    sql = sql.replace("temp table", "table")
    return sql

  def _tokenize_query(self, tokenizer_friendly_query) -> List[JsonDict]:
    """Splits query to single steps and tries to tokenize each of them.

    Args:
      tokenizer_friendly_query (str): raw tokenizer friendly query.
    Returns:
      List[JsonDict]: list of tokenized steps.
    """
    query_steps = tokenizer_friendly_query.split(self.MULTISTEP_SEPARATOR)
    tokenized_steps = []
    for step in query_steps:
      try:
        if step:
          tokenized_steps.append(mo_sql_parsing.parse_bigquery(step))
      except mo_parsing.ParseException as e:
        step_preview = step.replace("\n", "")
        step_preview = step_preview[:min(len(step_preview), 20)]
        print(f"Could not parse step '{step_preview}...' of {str(self)}"
              f" due to error: {e}")
    return tokenized_steps

  def _process_other_step(self, step: JsonDict) -> None:
    """Processes step other than final step and adds it to the list.

    Currently, only supports steps with create table syntax.

    Args:
      self (JsonDict): tokenized step.
    """
    if len(step) == 1 and "create table" in step:
      step_info = self.TokenizedStepInfo(**step["create table"])
      self._tokenized_step_info_list.append(step_info)
    else:
      print(f"Could not process step {step}\n of {str(self)}: unknown command")

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
                                               query=final_step)
      self._tokenized_step_info_list.append(final_step_info)

  def __init__(self, query: TQuery, graph: TGraph) -> None:
    self.raw_query = query.raw_query
    self.target_table = query.target_table
    self.domain = query.domain
    self.graph = graph
    self._tokenized_step_info_list = []

    tokenizer_friendly_query = self._get_tokenizer_friendly_query()
    tokenized_steps = self._tokenize_query(tokenizer_friendly_query)
    self._process_steps(tokenized_steps)

  def __repr__(self) -> str:
    """Representation of the class in print for debug purposes."""
    return f"<Tokenized query for table {self.target_table} in" \
           f" domain {self.domain}>"

  def get_tokenized_steps(self) -> List:
    """Returns the list of tokenized steps."""
    return self._tokenized_step_info_list[:]
