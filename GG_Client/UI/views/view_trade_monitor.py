import datetime
import os
import sys
import pandas as pd
from PyQt5.QtWidgets import QDialog, QFileDialog
from PyQt5.QtCore import Qt
from PyQt5 import uic

from UI.components.models import PandasModel
from UI.resource_resolver import resource_path
from xing.XAQuaries import CSPAT00600
from xing.XAReals import SC1, S3_
from util.CommUtils import get_db_connection, get_linenumber, to_numeric_safe
from util.FileWatcher import FileWatcher
import inspect

logger = None  # 초기값


def set_logger(external_logger):
    global logger
    logger = external_logger


try:
    FORM_CLASS_ORDER, _ = uic.loadUiType(resource_path("UI/주문테스트.ui"))
    FORM_CLASS_EXT_SIG, _ = uic.loadUiType(resource_path("UI/외부신호2LS.ui"))
    FORM_CLASS_TRADE_RES, _ = uic.loadUiType(resource_path("UI/거래결과.ui"))
    FORM_CLASS_REAL_INFO, _ = uic.loadUiType(resource_path("UI/실시간정보.ui"))
except Exception as e:
    logger.error(f"Failed to load TradeMonitor UIs: {e}")
    raise


class View_주문테스트(QDialog, FORM_CLASS_ORDER):
    def __init__(self, parent=None):
        super(View_주문테스트, self).__init__(parent)
        self.setAttribute(Qt.WA_DeleteOnClose)
        self.setupUi(self)
        self.parent = parent

        self.connection = self.parent.connection

        # 계좌정보 불러오기
        if self.connection and hasattr(self.connection, "ActiveX"):
            nCount = self.connection.ActiveX.GetAccountListCount()
            for i in range(nCount):
                self.comboBox.addItem(self.connection.ActiveX.GetAccountList(i))

        self.QA_CSPAT00600 = CSPAT00600(parent=self)

        self.setup()

    def setup(self):
        self.XR_SC1 = SC1(parent=self)
        self.XR_SC1.AdviseRealData()
        self.주문번호리스트 = []
        self.매수Lock = dict()

    def OnReceiveMessage(self, systemError, messageCode, message):
        self.textEdit.insertPlainText(
            "systemError:[%s] messageCode:[%s] message:[%s]\r"
            % (systemError, messageCode, message)
        )

    def OnReceiveData(self, szTrCode, result):
        if szTrCode == "CSPAT00600":
            df, df1 = result
            # print("df, df1 : %s, %s" % (df, df1))
            주문번호 = df1["주문번호"].values[0]
            self.textEdit.insertPlainText("주문번호 : %s\r" % 주문번호)
            if 주문번호 != "0":
                # 주문번호처리
                self.주문번호리스트.append(str(주문번호))

    def OnReceiveRealData(self, szTrCode, result):
        try:
            self.textEdit.insertPlainText(szTrCode + "\r")
            self.textEdit.insertPlainText(str(result) + "\r")
        except Exception as e:
            클래스이름 = self.__class__.__name__
            함수이름 = inspect.currentframe().f_code.co_name
            logger.error("%s-%s %s: %s" % (클래스이름, 함수이름, get_linenumber(), e))
            pass

        if szTrCode == "SC1":
            체결시각 = result["체결시각"]
            종목코드 = result["종목코드"].strip().replace("A", "")
            종목명 = result["종목명"]
            매매구분 = result["매매구분"]
            주문번호 = result["주문번호"]
            체결번호 = result["체결번호"]
            주문수량 = result["주문수량"]
            주문가격 = result["주문가격"]
            체결수량 = result["체결수량"]
            체결가격 = result["체결가격"]
            주문평균체결가격 = result["주문평균체결가격"]
            주문계좌번호 = result["주문계좌번호"]

            # 내가 주문한 것이 맞을 경우 처리
            if 주문번호 in self.주문번호리스트:
                s = "[%s] %s %s %s %s %s %s %s %s %s %s %s" % (
                    szTrCode,
                    체결시각,
                    종목코드,
                    매매구분,
                    주문번호,
                    체결번호,
                    주문수량,
                    주문가격,
                    체결수량,
                    체결가격,
                    주문평균체결가격,
                    주문계좌번호,
                )
                try:
                    self.textEdit.insertPlainText(s + "\r")
                except Exception as e:
                    pass

                일자 = "{:%Y-%m-%d}".format(datetime.datetime.now())
                with (
                    get_db_connection() as conn
                ):  # with 문을 사용하여 자동으로 conn.close()
                    query = "insert into 거래결과(GG_NM, UUID, 일자, 체결시각, 종목코드, 종목명, 매매구분, 주문번호, 체결번호, 주문수량, 주문가격, 체결수량, 체결가격, 주문평균체결가격) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
                    data = [
                        "주문테스트",
                        "주문테스트-UUID",
                        일자,
                        체결시각,
                        종목코드,
                        종목명,
                        매매구분,
                        주문번호,
                        체결번호,
                        주문수량,
                        주문가격,
                        체결수량,
                        체결가격,
                        주문평균체결가격,
                    ]
                    cursor = conn.cursor()
                    cursor.execute(query, data)
                    cursor.close()
            # print("3 self.매수Lock: %s" % self.매수Lock.keys())
            self.매수Lock.pop(종목코드, None)  # pop with default to avoid KeyError

    def Order(self):
        계좌번호 = self.comboBox.currentText().strip()
        비밀번호 = self.lineEdit_pwd.text().strip()
        종목코드 = self.lineEdit_code.text().strip()
        주문가 = self.lineEdit_price.text().strip()
        주문수량 = self.lineEdit_amt.text().strip()
        매매구분 = self.lineEdit_bs.text().strip()
        호가유형 = self.lineEdit_hoga.text().strip()
        신용거래 = self.lineEdit_sin.text().strip()
        주문조건 = self.lineEdit_jogun.text().strip()

        # print("1 self.매수Lock: %s" % self.매수Lock.keys())
        for i in range(0, 3):
            매수락 = self.매수Lock.get(종목코드, None)
            # print("i : %s 매수락 %s" % (i, 매수락))
            if 매수락:
                self.매수Lock[종목코드] = ""
                # print("2 self.매수Lock: %s" % self.매수Lock.keys())
                self.QA_CSPAT00600.Query(
                    계좌번호=계좌번호,
                    입력비밀번호=비밀번호,
                    종목번호=종목코드,
                    주문수량=주문수량,
                    주문가=주문가,
                    매매구분=매매구분,
                    호가유형코드=호가유형,
                    신용거래코드=신용거래,
                    주문조건구분=주문조건,
                )
                break  # Added break to stop loop after ordering


class View_외부신호2LS(QDialog, FORM_CLASS_EXT_SIG):
    def __init__(self, parent=None):
        super(View_외부신호2LS, self).__init__(parent)
        self.setAttribute(Qt.WA_DeleteOnClose)
        self.setupUi(self)
        self.parent = parent

        self.pathname = os.path.dirname(sys.argv[0])
        self.file = "%s\\" % os.path.abspath(self.pathname)

        self.매도 = 1
        self.매수 = 2
        self.매수방법 = "00"
        self.매도방법 = "00"
        self.조건없음 = 0
        self.조건IOC = 1
        self.조건FOK = 2

        self.신용거래코드 = "000"

        self.주문번호리스트 = []
        self.QA_CSPAT00600 = CSPAT00600(parent=self)
        self.XR_SC1 = SC1(parent=self)
        self.XR_SC1.AdviseRealData()

        self.connection = self.parent.connection

        # 계좌정보 불러오기
        if self.connection and hasattr(self.connection, "ActiveX"):
            nCount = self.connection.ActiveX.GetAccountListCount()
            for i in range(nCount):
                self.comboBox.addItem(self.connection.ActiveX.GetAccountList(i))

    def OnReceiveMessage(self, systemError, messageCode, message):
        s = "\r%s %s %s\r" % (systemError, messageCode, message)
        try:
            self.plainTextEdit.insertPlainText(s)
        except Exception as e:
            클래스이름 = self.__class__.__name__
            함수이름 = inspect.currentframe().f_code.co_name
            logger.error("%s-%s %s: %s" % (클래스이름, 함수이름, get_linenumber(), e))
            pass

    def OnReceiveData(self, szTrCode, result):
        if szTrCode == "CSPAT00600":
            df, df1 = result
            주문번호 = df1["주문번호"].values[0]
            if 주문번호 != "0":
                self.주문번호리스트.append(str(주문번호))
                s = "주문번호 : %s\r" % 주문번호
                try:
                    self.plainTextEdit.insertPlainText(s)
                except Exception as e:
                    클래스이름 = self.__class__.__name__
                    함수이름 = inspect.currentframe().f_code.co_name
                    logger.error(
                        "%s-%s %s: %s" % (클래스이름, 함수이름, get_linenumber(), e)
                    )
                    pass

    def OnReceiveRealData(self, szTrCode, result):
        if szTrCode == "SC1":
            체결시각 = result["체결시각"]
            종목코드 = result["종목코드"].strip().replace("A", "")
            종목명 = result["종목명"]
            매매구분 = result["매매구분"]
            주문번호 = result["주문번호"]
            체결번호 = result["체결번호"]
            주문수량 = result["주문수량"]
            주문가격 = result["주문가격"]
            체결수량 = result["체결수량"]
            체결가격 = result["체결가격"]
            주문평균체결가격 = result["주문평균체결가격"]
            주문계좌번호 = result["주문계좌번호"]

            # 내가 주문한 것이 체결된 경우 처리
            if 주문번호 in self.주문번호리스트:
                s = "\r주문체결[%s] : %s %s %s %s %s %s %s %s %s %s %s\r" % (
                    szTrCode,
                    체결시각,
                    종목코드,
                    매매구분,
                    주문번호,
                    체결번호,
                    주문수량,
                    주문가격,
                    체결수량,
                    체결가격,
                    주문평균체결가격,
                    주문계좌번호,
                )
                try:
                    self.plainTextEdit.insertPlainText(s)
                except Exception as e:
                    클래스이름 = self.__class__.__name__
                    함수이름 = inspect.currentframe().f_code.co_name
                    logger.error(
                        "%s-%s %s: %s" % (클래스이름, 함수이름, get_linenumber(), e)
                    )
                    pass

                일자 = "{:%Y-%m-%d}".format(datetime.datetime.now())
                with (
                    get_db_connection() as conn
                ):  # with 문을 사용하여 자동으로 conn.close()
                    query = "insert into 거래결과(GG_NM, UUID, 일자, 체결시각, 종목코드, 종목명, 매매구분, 주문번호, 체결번호, 주문수량, 주문가격, 체결수량, 체결가격, 주문평균체결가격) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
                    data = [
                        "툴박스2LS",
                        "툴박스2LS-UUID",
                        일자,
                        체결시각,
                        종목코드,
                        종목명,
                        매매구분,
                        주문번호,
                        체결번호,
                        주문수량,
                        주문가격,
                        체결수량,
                        체결가격,
                        주문평균체결가격,
                    ]
                    cursor = conn.cursor()
                    cursor.execute(query, data)
                    cursor.close()

    def OnReadFile(self, line):
        try:
            self.plainTextEdit.insertPlainText("\r>> " + line.strip() + "\r")
        except Exception as e:
            클래스이름 = self.__class__.__name__
            함수이름 = inspect.currentframe().f_code.co_name
            logger.error("%s-%s %s: %s" % (클래스이름, 함수이름, get_linenumber(), e))
            pass

        lst = line.strip().split(",")

        try:
            시각, 종류, 종목코드, 가격, 수량 = lst
            가격 = int(가격)
            수량 = int(수량)

            if 종류 == "매수":
                self.QA_CSPAT00600.Query(
                    계좌번호=self.계좌번호,
                    입력비밀번호=self.비밀번호,
                    종목번호=종목코드,
                    주문수량=수량,
                    주문가=가격,
                    매매구분=self.매수,
                    호가유형코드=self.매수방법,
                    신용거래코드=self.신용거래코드,
                    주문조건구분=self.조건없음,
                )
            if 종류 == "매도":
                self.QA_CSPAT00600.Query(
                    계좌번호=self.계좌번호,
                    입력비밀번호=self.비밀번호,
                    종목번호=종목코드,
                    주문수량=수량,
                    주문가=가격,
                    매매구분=self.매도,
                    호가유형코드=self.매도방법,
                    신용거래코드=self.신용거래코드,
                    주문조건구분=self.조건없음,
                )
        except Exception as e:
            클래스이름 = self.__class__.__name__
            함수이름 = inspect.currentframe().f_code.co_name
            logger.error("%s-%s %s: %s" % (클래스이름, 함수이름, get_linenumber(), e))
            pass

    def fileselect(self):
        ret = QFileDialog.getOpenFileName(
            self, "Open file", self.file, "CSV,TXT(*.csv;*.txt)"
        )
        self.file = ret[0]
        self.lineEdit.setText(self.file)

    def StartWatcher(self):
        self.계좌번호 = self.comboBox.currentText().strip()
        self.비밀번호 = self.lineEdit_pwd.text().strip()

        self.fw = FileWatcher(
            filename=self.file, callback=self.OnReadFile, encoding="utf-8"
        )
        self.fw.start()


class View_거래결과(QDialog, FORM_CLASS_TRADE_RES):
    def __init__(self, parent=None):
        super(View_거래결과, self).__init__(parent)
        self.setAttribute(Qt.WA_DeleteOnClose)
        self.setupUi(self)
        self.setWindowTitle("거래결과 조회")
        self.parent = parent

        self.model = PandasModel()
        self.tableView.setModel(self.model)

        self.columns = []

        with get_db_connection() as conn:  # with 문을 사용하여 자동으로 conn.close()
            query = "select distinct GG_NM from 거래결과 order by GG_NM"
            df = pd.read_sql_query(query, con=conn)  # changed pdsql to pd
        for name in df["GG_NM"].values.tolist():
            self.comboBox.addItem(name)

    def inquiry(self):
        GG_NM = self.comboBox.currentText().strip()
        with get_db_connection() as conn:  # with 문을 사용하여 자동으로 conn.close()
            query = (
                "select GG_NM, UUID, 일자, 체결시각, 종목코드, 종목명, 매매구분, 주문번호, 체결번호, 주문수량, 주문가격, 체결수량, 체결가격, 주문평균체결가격 from 거래결과  where  GG_NM='%s' order by 일자, 체결시각"
                % GG_NM
            )
            df = pd.read_sql_query(query, con=conn)  # changed pdsql to pd

        self.model.update(df)
        for i in range(len(df.columns)):
            self.tableView.resizeColumnToContents(i)


class View_실시간정보(QDialog, FORM_CLASS_REAL_INFO):
    def __init__(self, parent=None):
        super(View_실시간정보, self).__init__(parent)
        self.setAttribute(Qt.WA_DeleteOnClose)
        self.setupUi(self)
        self.parent = parent
        self.틱데이터캐시 = {}  #  캐시 저장소 추가
        self.kospi_real = S3_(parent=self)

    def OnReceiveRealData(self, szTrCode, result):
        try:
            종목코드 = result.get("종목코드", "").strip().replace("A", "")
            현재가 = to_numeric_safe(result.get("현재가", 0))
            if 종목코드:
                self.틱데이터캐시[종목코드] = 현재가

            onReceiveRealData = "{}:{} - {}--{}\r".format(
                result.get("체결시간", ""),
                종목코드,
                현재가,
                result.get("체결량", ""),
            )
            self.textEdit.insertPlainText(onReceiveRealData)
        except Exception as e:
            클래스이름 = self.__class__.__name__
            함수이름 = inspect.currentframe().f_code.co_name
            logger.error("%s-%s %s: %s" % (클래스이름, 함수이름, get_linenumber(), e))
            pass

    def AddCode(self):
        종목코드 = self.comboBox.currentText().strip()
        self.comboBox.addItems([종목코드])
        self.kospi_real.AdviseRealData(종목코드=종목코드)

    def RemoveCode(self):
        종목코드 = self.comboBox.currentText().strip()
        self.kospi_real.UnadviseRealDataWithKey(종목코드=종목코드)
