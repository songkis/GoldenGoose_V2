import datetime
import os
import sys
from PyQt5.QtWidgets import QDialog, QFileDialog
from PyQt5.QtCore import Qt
from PyQt5 import uic

from UI.components.models import PandasModel
from UI.resource_resolver import resource_path
from xing.XAQuaries import t1833, t1857

logger = None  # 초기값


def set_logger(external_logger):
    global logger
    logger = external_logger


try:
    FORM_CLASS_SEARCH, _ = uic.loadUiType(resource_path("UI/종목검색.ui"))
    FORM_CLASS_E_SEARCH, _ = uic.loadUiType(resource_path("UI/e종목검색.ui"))
except Exception as e:
    logger.error(f"Failed to load Analysis UIs: {e}")
    raise

class View_종목검색(QDialog, FORM_CLASS_SEARCH):
    def __init__(self, parent=None):
        super(View_종목검색, self).__init__(parent)
        self.setAttribute(Qt.WA_DeleteOnClose)
        self.setupUi(self)

        self.model = PandasModel()
        self.tableView.setModel(self.model)

        self.parent = parent

    def OnReceiveMessage(self, systemError, messageCode, message):
        일자 = "{:%Y-%m-%d %H:%M:%S.%f}".format(datetime.datetime.now())
        클래스이름 = self.__class__.__name__
        logger.info(
            "일자 : %s, 클래스이름 : %s, systemError : %s, messageCode : %s, message : %s"
            % (일자, 클래스이름, systemError, messageCode, message)
        )

    def OnReceiveData(self, szTrCode, result):
        if szTrCode == "t1833":
            종목검색수, df = result
            self.model.update(df)
            for i in range(len(df.columns)):
                self.tableView.resizeColumnToContents(i)

    def fileselect(self):
        # sys.argv[0] usage might be fragile in some contexts, but keeping original logic
        pathname = os.path.dirname(sys.argv[0])
        RESDIR = "%s\\ADF\\" % os.path.abspath(pathname)

        fname = QFileDialog.getOpenFileName(
            self, "Open file", RESDIR, "조검검색(*.adf)"
        )
        if fname[0]:
            self.lineEdit.setText(fname[0])

    def inquiry(self):
        filename = self.lineEdit.text()
        XQ = t1833(parent=self)
        XQ.Query(종목검색파일=filename)


class View_e종목검색(QDialog, FORM_CLASS_E_SEARCH):
    def __init__(self, parent=None):
        super(View_e종목검색, self).__init__(parent)
        self.setAttribute(Qt.WA_DeleteOnClose)
        self.setupUi(self)

        self.model = PandasModel()
        self.tableView.setModel(self.model)

        self.parent = parent

    def OnReceiveMessage(self, systemError, messageCode, message):
        일자 = "{:%Y-%m-%d %H:%M:%S.%f}".format(datetime.datetime.now())
        클래스이름 = self.__class__.__name__
        logger.info(
            "일자 : %s, 클래스이름 : %s, systemError : %s, messageCode : %s, message : %s"
            % (일자, 클래스이름, systemError, messageCode, message)
        )

    def OnReceiveData(self, szTrCode, result):
        if szTrCode == "t1857":
            식별자, 검색종목수, 포착시간, 실시간키, df = result
            self.model.update(df)
            for i in range(len(df.columns)):
                self.tableView.resizeColumnToContents(i)

    def OnReceiveSearchRealData(self, szTrCode, result):
        if szTrCode == "t1857":
            logger.info(result)

    def fileselect(self):
        pathname = os.path.dirname(sys.argv[0])
        RESDIR = "%s\\acf\\" % os.path.abspath(pathname)

        fname = QFileDialog.getOpenFileName(
            self, "Open file", RESDIR, "조검검색(*.acf)"
        )
        if fname[0]:
            self.lineEdit.setText(fname[0])

    def inquiry(self):
        filename = self.lineEdit.text()
        XQ = t1857(parent=self)
        XQ.Query(실시간구분="0", 종목검색구분="F", 종목검색입력값=filename)
