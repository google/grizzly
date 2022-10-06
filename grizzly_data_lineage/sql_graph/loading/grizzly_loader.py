from datetime import datetime as DateTime
from typing import Dict, List

from sql_graph.loading.domain import Domain
from sql_graph.loading.bq import GrizzlyBQClient
from sql_graph.typing import TQuery


class GrizzlyLoader:
  """Loader class specific for Grizzly.

  Will load SQL files from Grizzly ETL log tables.
  Includes Grizzly specific attributes and methods.
  Can be reused multiple times.

  Attributes:
    gcp_project (str): name of the GCP project.
    datetime (DateTime): datetime of the build, which can be used
      to retrieve SQL files from past deployments.
    _client (GrizzlyBQClient): GrizzlyBQClient instance for SQL queries.
    _domains (Dict[str, Domain]): dictionary of domains with domain names as
      keys and domain objects as values.
  """

  def _group_build_files_rows_by_domain(self) -> Dict:
    """Groups rows into a dictionary with domain names as the key.

    Returns:
      Dict: rows grouped by domain.
    """
    rows_grouped_by_domain = {}
    for item in self._build_files_rows:
      rows_grouped_by_domain.setdefault(item["subject_area"], []).append(item)
    return rows_grouped_by_domain

  def _build_domains(self) -> None:
    """Builds Domain objects from build files grouped by domain name."""
    rows_grouped_by_domain = self._group_build_files_rows_by_domain()
    for domain_name in rows_grouped_by_domain.keys():
      try:
        self._domains[domain_name] = Domain(
          name=domain_name,
          queried_files_rows=rows_grouped_by_domain[domain_name]
        )
      except Exception as e:
        print(f"Failed to get data for {domain_name} with error: {e}")

  def __init__(self, gcp_project: str, datetime: DateTime) -> None:
    self.gcp_project = gcp_project
    self._client = GrizzlyBQClient(self.gcp_project)
    self.datetime = datetime
    self._domains: Dict[str, Domain] = {}

    self._build_files_rows = self._client.get_build_files(str(self.datetime))
    self._build_domains()

  def get_domains(self) -> List[str]:
    """Returns a list of all domain names that are loaded.

    Returns:
      List[str]: list of domain names for selected datetime.
    """
    return list(self._domains.keys())

  def get_job_build_ids(self, domain_name: str) -> List[str]:
    """Returns a list of all job build IDs for a domain.

    Args:
      domain_name (str): name of the domain.
    Returns:
      List[str]: list of job build IDs.
    """
    return list(self._domains[domain_name].tasks.keys())

  def get_queries(self) -> List[TQuery]:
    """Returns query objects from all domains ordered by domain name.

    Returns:
      List[TQuery]: queries and info about them.
    """
    queries = []
    for domain_name in sorted(self._domains.keys()):
      queries.extend(self._domains[domain_name].get_queries())
    return queries

  def filter_queries_by_sa_list(self, domain_list: List[str]) -> List[TQuery]:
    """Returns query objects from only selected domains.

    Args:
      domain_list (List[str]): list of domain names to include.
        If a name on the list was not loaded it will be ignored.
    Returns:
      List[TQuery]: queries and info about them.
    """
    queries = []
    do_filtering = len(domain_list) > 0

    for domain_name in sorted(self._domains.keys()):
      if (do_filtering and domain_name in domain_list) or not do_filtering:
        queries.extend(self._domains[domain_name].get_queries())
    return queries

  def filter_queries_by_job_build_id(self, domain_name: str,
                                     job_build_id: str) -> List[TQuery]:
    """Returns query objects from a single job build ID.

    Args:
      domain_name (str): name of the domain.
      job_build_id (str): name of the job build ID.
    Returns:
      List[TQuery]: queries and info about them.
    """
    return self._domains[domain_name].filter_queries_by_task_id(job_build_id)
