import sql_graph


def parse_test_queries():
  query1 = sql_graph.Query(
    query="""
    SELECT
      col1,
      col2
    FROM
      `some.public.table`
    """,
    target_table="table1",
    domain="1"
  )
  query2 = sql_graph.Query(
    query="""
    SELECT *
    FROM
      table1
    JOIN table3 ON
      table1.col1 = table3.col1
    """,
    target_table="table2",
    domain="1"
  )
  query3 = sql_graph.Query(
    query="""
    SELECT 
      table2.col1,
      1 AS calculated_column
    FROM
      table2
    """,
    target_table="table3",
    domain="2"
  )
  query4 = sql_graph.Query(
    query="""select t.* from etl_log.fn_get_build_files('xxx','sha_x123') as t
join etl_log.fn_get_build_files2('xxx','sha_x123') as t2 on t.a = t2.b""",
    target_table="target.table",
    domain="domain"
  )
  graph = sql_graph.Graph(queries=[query3, query1, query2])
  serializer = sql_graph.ReactFlowSerializer(graph=graph, physical=False)
  return serializer.serialize()
