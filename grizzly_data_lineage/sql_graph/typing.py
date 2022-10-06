from typing import TypeVar, TYPE_CHECKING, Dict, Any, Union, Optional

"""Column Level Visualization typing module"""


if TYPE_CHECKING:
  from sql_graph.parsing.primitives import Coordinates
  from sql_graph.parsing.primitives import Connection
  from sql_graph.parsing.primitives import SerializingParams
  from sql_graph.parsing.primitives import GridItem
  from sql_graph.parsing.primitives import Container
  from sql_graph.parsing.primitives import TextInfoPanel
  from sql_graph.parsing.columns import Column
  from sql_graph.parsing.columns import TableColumn
  from sql_graph.parsing.columns import InfoColumn
  from sql_graph.parsing.columns import JoinInfo
  from sql_graph.parsing.columns import WhereInfo
  from sql_graph.parsing.columns import ColumnContainer
  from sql_graph.parsing.tables import Table
  from sql_graph.parsing.tables import SelectTable
  from sql_graph.parsing.tables import ExternalTable
  from sql_graph.parsing.tables import UnnestTable
  from sql_graph.parsing.utils import Query
  from sql_graph.parsing.utils import TokenizedQuery
  from sql_graph.parsing.graph import Graph

JsonDict = Dict[str, Optional[Any]]
TokenizedJson = Union[JsonDict, str, int, None]

TCoordinates = TypeVar("TCoordinates", bound="Coordinates")
TConnection = TypeVar("TConnection", bound="Connection")
TSerializingParams = TypeVar("TSerializingParams", bound="SerializingParams")
TGridItem = TypeVar("TGridItem", bound="GridItem")
TContainer = TypeVar("TContainer", bound="Container")
TTextInfoPanel = TypeVar("TTextInfoPanel", bound="TextInfoPanel")

TColumn = TypeVar("TColumn", bound="Column")
TTableColumn = TypeVar("TTableColumn", bound="TableColumn")
TStarColumn = TypeVar("TStarColumn", bound="StarColumn")
TInfoColumn = TypeVar("TInfoColumn", bound="InfoColumn")
TJoinInfo = TypeVar("TJoinInfo", bound="JoinInfo")
TWhereInfo = TypeVar("TWhereInfo", bound="WhereInfo")
TColumnContainer = TypeVar("TColumnContainer", bound="ColumnContainer")

TTable = TypeVar("TTable", bound="Table")
TSelectTable = TypeVar("TSelectTable", bound="SelectTable")
TExternalTable = TypeVar("TExternalTable", bound="ExternalTable")
TUnnestTable = TypeVar("TUnnestTable", bound="UnnestTable")

TQuery = TypeVar("TQuery", bound="Query")
TTokenizedQuery = TypeVar("TTokenizedQuery", bound="TokenizedQuery")

TGraph = TypeVar("TGraph", bound="Graph")

TableLocation = Union[TGraph, TQuery, TTable]
