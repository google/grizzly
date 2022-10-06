"""Module with info panels that don't have any direct SQL meaning."""
from sql_graph.parsing.primitives import GridItem
from sql_graph.parsing.primitives.settings import COLUMN_VERTICAL_MARGIN
from sql_graph.parsing.primitives.settings import COLUMN_WIDTH


class TextInfoPanel(GridItem):
  """Text only info panel.

  Attributes:
    name (str): name of the info panel. will be used in ID calculation.
    text (str): text label that will be displayed.
  """

  WIDTH = COLUMN_WIDTH
  VERTICAL_MARGIN = COLUMN_VERTICAL_MARGIN

  def __init__(self, name: str, text: str):
    super(TextInfoPanel, self).__init__()
    self.name = name
    self.text = text

  def _get_serializing_id(self) -> str:
    """Overwrite of _get_serializing_id method."""
    return f"{self._serializing_params.get_parent_id()}__{self.name}"

  def _get_label(self) -> str:
    return self.text

  def _calculate_serializing_params(self) -> None:
    """Override of _calculate_serializing_params method."""
    super(TextInfoPanel, self)._calculate_serializing_params()
    self._serializing_params.data["panelName"] = self.name
