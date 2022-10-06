"""Domain class."""

from typing import List, Dict

from sql_graph.loading.task import Task
from sql_graph.parsing import Query
from sql_graph.typing import TQuery


class Domain:
  """Class representing a Grizzly domain (also called subject area).

  Attributes:
    name (str): domain name.
    tasks (dict): dictionary of tasks.
  """

  @staticmethod
  def _group_rows_by_task(queried_files_rows: List) -> Dict:
    """Groups rows into a dictionary with task ID as the key.

    Args:
      queried_files_rows (List): list of rows with file descriptions.
    Returns:
      Dict: rows grouped by task.
    """
    rows_grouped_by_task = {}
    for item in queried_files_rows:
      rows_grouped_by_task.setdefault(item["job_build_id"], []).append(item)
    return rows_grouped_by_task

  def __init__(self, name: str, queried_files_rows: List) -> None:
    self.name = name
    self.tasks = {}

    rows_grouped_by_task = self._group_rows_by_task(queried_files_rows)
    for task_id, task_rows in rows_grouped_by_task.items():
      task_files = {r["file_path"]: r["file_value"] for r in task_rows}
      task_instance = Task(task_id=task_id, files=task_files)
      self.tasks[task_instance.task_id] = task_instance

  def get_queries(self) -> List[TQuery]:
    """Builds and returns query objects in alphabetical order by task id.

    Returns:
      List[TQuery]: queries.
    """
    queries = []
    for task_id in sorted(self.tasks.keys()):
      task = self.tasks[task_id]
      if task.target_table_name is not None and task.query:
        queries.append(
          Query(
            query=task.query,
            target_table=task.target_table_name,
            domain=self.name
          )
        )
    return queries

  def filter_queries_by_task_id(self, task_id: str):
    """Builds and returns query objects for a specific task id.

    Args:
      task_id (str): task ID.
    Returns:
      List[TQuery]: queries.
    """
    task = self.tasks[task_id]
    if task.target_table_name is not None and task.query:
      return [Query(
        query=task.query,
        target_table=task.target_table_name,
        domain=self.name
      )]
    else:
      return []
