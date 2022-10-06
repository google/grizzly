"""Query data structure that is used during graph init."""


class Query:
  """Query data structure that contains basic information.

  Attributes:
    raw_query (str): raw SQL query string.
    target_table (str): name of the target table of a query.
    domain (str): name of the domain of the query.
  """

  def __init__(self, query: str, target_table: str, domain: str) -> None:
    self.raw_query = query
    self.target_table = target_table
    self.domain = domain

  def __repr__(self) -> str:
    """Representation of the class in print for debug purposes."""
    return f"<Query for table {self.target_table} in domain {self.domain}>"
