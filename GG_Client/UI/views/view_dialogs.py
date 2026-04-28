from PyQt5.QtWidgets import QDialog
from PyQt5.QtCore import Qt
from PyQt5 import uic

from UI.components.models import PandasModel
from UI.resource_resolver import resource_path
from config.program_info import 프로그램정보
from pandas import DataFrame

logger = None  # 초기값


def set_logger(external_logger):
    global logger
    logger = external_logger


try:
    FORM_CLASS_VER, _ = uic.loadUiType(resource_path("UI/버전.ui"))
except Exception as e:
    logger.error(f"Failed to load Dialog UIs: {e}")
    raise

class View_버전(QDialog, FORM_CLASS_VER):
    def __init__(self, parent=None):
        super(View_버전, self).__init__(parent)
        self.setAttribute(Qt.WA_DeleteOnClose)
        self.setupUi(self)
        self.setWindowTitle("버전")
        self.parent = parent

        self.model = PandasModel()
        self.tableView.setModel(self.model)

        df = DataFrame(data=프로그램정보, columns=["A", "B"])

        self.model.update(df)
        for i in range(len(df.columns)):
            self.tableView.resizeColumnToContents(i)
