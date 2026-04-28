import datetime
from PyQt5.QtWidgets import QDialog
from PyQt5.QtCore import Qt, QTimer
from PyQt5 import uic

from UI.components.models import PandasModel
from UI.resource_resolver import resource_path
from config.comm_settings import 주문지연
from xing.XAQuaries import t8436, t1302, t1305

logger = None  # 초기값


def set_logger(external_logger):
    global logger
    logger = external_logger


try:
    FORM_CLASS_MINUTE_PRICE, _ = uic.loadUiType(resource_path("UI/분별주가조회.ui"))
    FORM_CLASS_DAILY_PRICE, _ = uic.loadUiType(resource_path("UI/일자별주가조회.ui"))
except Exception as e:
    logger.error(f"Failed to load Price UIs: {e}")
    raise


class View_분별주가(QDialog, FORM_CLASS_MINUTE_PRICE):
    def __init__(self, parent=None):
        super(View_분별주가, self).__init__(parent)
        self.setAttribute(Qt.WA_DeleteOnClose)
        self.setupUi(self)
        self.setWindowTitle("분별 주가 조회")
        self.parent = parent

        self.model = PandasModel()
        self.tableView.setModel(self.model)

        self.columns = []

        self.result = []

        XQ = t8436(parent=self)
        XQ.Query(구분="0")

        self.XQ_t1302 = t1302(parent=self)

    def OnReceiveMessage(self, systemError, messageCode, message):
        pass

    def OnReceiveData(self, szTrCode, result):
        if szTrCode == "t8436":
            self.종목코드테이블 = result[0][result[0]["ETF구분"] == 0]
            # Ensure you have a copy of the DataFrame
            self.종목코드테이블 = self.종목코드테이블.copy()
            self.종목코드테이블["컬럼"] = (
                ">> "
                + self.종목코드테이블["종목코드"]
                + " : "
                + self.종목코드테이블["종목명"]
            )
            self.종목코드테이블 = self.종목코드테이블.sort_values(
                ["종목코드", "종목명"], ascending=[True, True]
            )
            self.comboBox.addItems(self.종목코드테이블["컬럼"].values)

        if szTrCode == "t1302":
            시간CTS, df = result
            self.model.update(df)
            for i in range(len(df.columns)):
                self.tableView.resizeColumnToContents(i)

    def inquiry(self):
        종목코드 = self.comboBox.currentText().strip()[3:9]
        조회건수 = self.lineEdit_cnt.text().strip().replace("-", "")

        self.XQ_t1302.Query(
            종목코드=종목코드, 작업구분="1", 시간="", 건수=조회건수, 연속조회=False
        )


class View_일별주가(QDialog, FORM_CLASS_DAILY_PRICE):
    def __init__(self, parent=None):
        super(View_일별주가, self).__init__(parent)
        self.setAttribute(Qt.WA_DeleteOnClose)
        self.setupUi(self)

        self.setWindowTitle("일자별 주가 조회")

        self.parent = parent

        self.model = PandasModel()
        self.tableView.setModel(self.model)

        self.columns = [
            "날짜",
            "시가",
            "고가",
            "저가",
            "종가",
            "전일대비구분",
            "전일대비",
            "등락율",
            "누적거래량",
            "거래증가율",
            "체결강도",
            "소진율",
            "회전율",
            "외인순매수",
            "기관순매수",
            "종목코드",
            "누적거래대금",
            "개인순매수",
            "시가대비구분",
            "시가대비",
            "시가기준등락율",
            "고가대비구분",
            "고가대비",
            "고가기준등락율",
            "저가대비구분",
            "저가대비",
            "저가기준등락율",
            "시가총액",
        ]

        self.result = []

        d = datetime.date.today()

        XQ = t8436(parent=self)
        XQ.Query(구분="0")

    def OnReceiveMessage(self, systemError, messageCode, message):
        pass

    def OnReceiveData(self, szTrCode, result):
        if szTrCode == "t8436":
            self.종목코드테이블 = result[0][result[0]["ETF구분"] == 0]
            # Ensure you have a copy of the DataFrame
            self.종목코드테이블 = self.종목코드테이블.copy()
            self.종목코드테이블["컬럼"] = (
                ">> "
                + self.종목코드테이블["종목코드"]
                + " : "
                + self.종목코드테이블["종목명"]
            )
            self.종목코드테이블 = self.종목코드테이블.sort_values(
                ["종목코드", "종목명"], ascending=[True, True]
            )
            self.comboBox.addItems(self.종목코드테이블["컬럼"].values)

        if szTrCode == "t1305":
            CNT, 날짜, IDX, df = result

            self.model.update(df)
            for i in range(len(df.columns)):
                self.tableView.resizeColumnToContents(i)

            if int(CNT) == int(self.조회건수):
                QTimer.singleShot(
                    주문지연, lambda: self.inquiry_repeatly(result=result)
                )
            else:
                pass

    def inquiry_repeatly(self, result):
        CNT, 날짜, IDX, df = result
        # print('CNT %s, 날짜 %s, IDX %s, df %s'% (CNT, 날짜, IDX, df))
        self.XQ.Query(
            종목코드=self.종목코드,
            일주월구분="1",
            날짜=날짜,
            IDX=IDX,
            건수=self.조회건수,
            연속조회=True,
        )

    def inquiry(self):
        self.종목코드 = self.comboBox.currentText()[3:9]
        self.조회건수 = self.lineEdit_date.text().strip().replace("-", "")

        self.XQ = t1305(parent=self)
        self.XQ.Query(
            종목코드=self.종목코드,
            일주월구분="1",
            날짜="",
            IDX="",
            건수=self.조회건수,
            연속조회=False,
        )
