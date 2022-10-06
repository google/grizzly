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
