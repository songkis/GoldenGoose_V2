import datetime
import inspect
import time
import pandas as pd
from time import sleep

from PyQt5.QtWidgets import QDialog, QMessageBox
from PyQt5.QtCore import Qt, QTimer
from PyQt5 import uic
from pandas import DataFrame

from UI.components.models import PandasModel
from UI.resource_resolver import resource_path
from util.CommUtils import (
    get_db_connection,
    isOverBackupTime,
    get_linenumber,
    isOverCloseTime,
)
from config.telegram_setting import ToTelegram
from config.comm_settings import 주문지연

from xing.XAQuaries import (
    t8436,
    t1702,
    t8424,
    t1511,
    t1514,
    t1516,
    t8425,
    t1537,
    t1717,
    ChartIndex,
    t1102,
)
from xing.XAReals import H1_, HA_, NWS
from config.ai_settings import NWS_RSLT_BUY_YN
from SQL.sql import GET_STOCK_LIST

logger = None  # 초기값


def set_logger(external_logger):
    global logger
    logger = external_logger


# Load UI Views
try:
    FORM_CLASS_CODE, _ = uic.loadUiType(resource_path("UI/종목코드조회.ui"))
    FORM_CLASS_INV, _ = uic.loadUiType(resource_path("UI/종목별투자자정보백업.ui"))
    FORM_CLASS_SEC_BACK, _ = uic.loadUiType(resource_path("UI/일별업종정보백업.ui"))
    FORM_CLASS_SEC_INFO, _ = uic.loadUiType(resource_path("UI/업종정보조회.ui"))
    FORM_CLASS_SEC_STK, _ = uic.loadUiType(resource_path("UI/업종별종목정보.ui"))
    FORM_CLASS_THEME, _ = uic.loadUiType(resource_path("UI/테마정보조회.ui"))
    FORM_CLASS_INV_STK, _ = uic.loadUiType(
        resource_path("UI/종목별투자자조회.ui")
    )  # Reused for both View_종목별투자자 and View_종목별투자자2
    FORM_CLASS_CHART_IDX, _ = uic.loadUiType(resource_path("UI/차트인덱스.ui"))
    FORM_CLASS_HOGA, _ = uic.loadUiType(resource_path("UI/호가창정보.ui"))
    FORM_CLASS_NEWS, _ = uic.loadUiType(resource_path("UI/뉴스.ui"))
except Exception as e:
    logger.error(f"Failed to load MarketInfo UIs: {e}")
    raise


class View_종목코드조회(QDialog, FORM_CLASS_CODE):
    def __init__(self, parent=None):
        super(View_종목코드조회, self).__init__(parent)
        self.setAttribute(Qt.WA_DeleteOnClose)
        self.setupUi(self)

        self.parent = parent

        self.model = PandasModel()
        self.tableView.setModel(self.model)

        self.df = DataFrame()
        self.XQ_t8436 = t8436(parent=self)
        self.inquiry()

    def inquiry(self):
        self.XQ_t8436.Query(구분="0")
        print(f"종목코드 조회")

    def OnReceiveMessage(self, systemError, messageCode, message):
        pass

    def OnReceiveData(self, szTrCode, result):
        print(f"종목코드 수신 {szTrCode}, {result}")
        if szTrCode == "t8436":
            self.df = result[0]
            print(f"self.df : {self.df}")
            self.model.update(self.df)
            for i in range(len(self.df.columns)):
                self.tableView.resizeColumnToContents(i)
            self.SaveCode()

    def SaveCode(self):
        if self.df.empty:
            return
        # 1. 종목코드가 숫자 6자리인 행만 필터링
        self.df = self.df[self.df["종목코드"].astype(str).str.match(r"^\d{6}$")]

        with get_db_connection() as conn:  # with 문을 사용하여 자동으로 conn.close()
            cursor = conn.cursor()
            query = "DELETE FROM 종목코드"
            cursor.execute(query)
            query = "insert or replace into 종목코드(종목명,종목코드,확장코드,ETF구분,상한가,하한가,전일가,주문수량단위,기준가,구분,증권그룹,기업인수목적회사여부) values(?,?,?,?,?,?,?,?,?,?,?,?)"
            cursor.executemany(query, self.df.values.tolist())
            cursor.close()
        # window.statusbar.showMessage(" %s 건의 종목코드를 저장함!" % (len(self.df)))
        logger.info(
            f"==---->>> GoldenGoose {len(self.df)} 건의 종목코드를 저장함!  <<<-------=="
        )

        if not isOverBackupTime():
            ToTelegram(f"GoldenGoose {len(self.df)} 건의 종목코드를 저장함!")
            today = datetime.datetime.today()
            logger.info(
                f"==---->>> GoldenGoose {today.strftime('%Y.%m.%d')} 일봉 가격정보 백업이 시작됨  <<<-------=="
            )
            ToTelegram(
                f"GoldenGoose {today.strftime('%Y.%m.%d')} 일봉 가격정보 백업이 시작됨!"
            )
            # Safe lazy loading to avoid circular imports / uninitialized state
            # Assuming View_일별가격정보백업 is imported inside method or we rely on parent's dialog controller

            # Using absolute import or parent's logic
            # Here we assume parent handles the dialog creation if we just trigger it, but code creates instance.
            # We need to import View_일별가격정보백업 here to avoid top-level circular import
            from UI.views.view_backup import View_일별가격정보백업

            if hasattr(self.parent, "dialog"):
                if self.parent.dialog.get("일별가격정보백업") is None:
                    self.parent.dialog["일별가격정보백업"] = View_일별가격정보백업(
                        parent=self
                    )
                try:
                    self.parent.dialog["일별가격정보백업"].Backup_All()
                except Exception as e:
                    self.parent.dialog["일별가격정보백업"] = View_일별가격정보백업(
                        parent=self
                    )
                    self.parent.dialog["일별가격정보백업"].Backup_All()
        else:
            QMessageBox.about(
                self, "종목코드 생성", " %s 항목의 종목코드를 저장함!" % (len(self.df))
            )


class View_종목별투자자정보백업(QDialog, FORM_CLASS_INV):
    def __init__(self, parent=None):
        super(View_종목별투자자정보백업, self).__init__(parent)
        self.setAttribute(Qt.WA_DeleteOnClose)
        self.setupUi(self)
        self.setWindowTitle("종목별 투자자 정보 백업")
        self.parent = parent

        self.columns = [
            "일자",
            "현재가",
            "전일대비",
            "누적거래대금",
            "개인투자자",
            "외국인투자자",
            "기관계",
            "금융투자",
            "보험",
            "투신",
            "기타금융",
            "은행",
            "연기금등",
            "국가",
            "내외국인",
            "사모펀드",
            "기타법인",
        ]

        d = datetime.date.today()
        self.lineEdit_date.setText(str(d))

        self.XQ_t8436 = t8436(parent=self)
        self.XQ_t8436.Query(구분="0")

        self.조회건수 = 10
        self.XQ_t1702 = t1702(parent=self)

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

        if szTrCode == "t1702":
            CTSIDX, CTSDATE, df = result
            df["종목코드"] = self.종목코드[0]
            with (
                get_db_connection() as conn
            ):  # with 문을 사용하여 자동으로 conn.close()
                cursor = conn.cursor()
                query = "insert or replace into 종목별투자자(일자, 종가, 전일대비구분, 전일대비, 등락율, 누적거래량, 사모펀드, 증권, 보험, 투신, 은행, 종금, 기금, 기타법인, 개인, 등록외국인, 미등록외국인, 국가외, 기관, 외인계, 기타계, 종목코드) values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
                cursor.executemany(query, df.values.tolist())
                cursor.close()
            try:
                # Check if UI object is still valid
                if not hasattr(self, "radioButton_all") or self.radioButton_all is None:
                    return

                if len(df) == int(self.조회건수) and self.radioButton_all.isChecked():
                    QTimer.singleShot(주문지연, lambda: self.Request(result=result))
                else:
                    self.백업한종목수 += 1
                    if len(self.백업할종목코드) > 0:
                        self.종목코드 = self.백업할종목코드.pop(0)
                        self.result = []

                        self.progressBar.setValue(
                            int(
                                self.백업한종목수
                                / (
                                    len(self.종목코드테이블.index)
                                    - self.comboBox.currentIndex()
                                )
                                * 100
                            )
                        )
                        S = "%s %s" % (self.종목코드[0], self.종목코드[1])
                        self.label_codename.setText(S)

                        QTimer.singleShot(주문지연, lambda: self.Request([]))
                    else:
                        QMessageBox.about(self, "백업완료", "백업을 완료함!")
            except Exception as e:
                클래스이름 = self.__class__.__name__
                함수이름 = inspect.currentframe().f_code.co_name
                logger.error(
                    "%s-%s %s: %s" % (클래스이름, 함수이름, get_linenumber(), e)
                )
                pass

    def Request(self, result=[]):
        if len(result) > 0:
            CTSIDX, CTSDATE, df = result
            self.XQ_t1702.Query(
                종목코드=self.종목코드[0],
                종료일자="",
                금액수량구분="0",
                매수매도구분="0",
                누적구분="0",
                CTSDATE=CTSDATE,
                CTSIDX=CTSIDX,
            )
        else:
            self.XQ_t1702.Query(
                종목코드=self.종목코드[0],
                종료일자="",
                금액수량구분="0",
                매수매도구분="0",
                누적구분="0",
                CTSDATE="",
                CTSIDX="",
            )

    def Backup_One(self):
        idx = self.comboBox.currentIndex()

        self.백업한종목수 = 1
        self.백업할종목코드 = []
        self.종목코드 = self.종목코드테이블[idx : idx + 1][
            ["종목코드", "종목명"]
        ].values[0]
        self.기준일자 = self.lineEdit_date.text().strip().replace("-", "")
        self.result = []
        self.Request(result=[])

    def Backup_All(self):
        idx = self.comboBox.currentIndex()
        self.백업한종목수 = 1
        self.백업할종목코드 = list(
            self.종목코드테이블[idx:][["종목코드", "종목명"]].values
        )
        self.종목코드 = self.백업할종목코드.pop(0)
        self.기준일자 = self.lineEdit_date.text().strip().replace("-", "")

        self.progressBar.setValue(
            int(
                self.백업한종목수
                / (len(self.종목코드테이블.index) - self.comboBox.currentIndex())
                * 100
            )
        )
        S = "%s %s" % (self.종목코드[0], self.종목코드[1])
        self.label_codename.setText(S)

        self.result = []
        self.Request(result=[])


class View_일별업종정보백업(QDialog, FORM_CLASS_SEC_BACK):
    def __init__(self, parent=None):
        super(View_일별업종정보백업, self).__init__(parent)
        self.setAttribute(Qt.WA_DeleteOnClose)
        self.setupUi(self)
        self.setWindowTitle("업종 정보 백업")
        self.parent = parent

        self.columns = [
            "현재가",
            "거래량",
            "일자",
            "시가",
            "고가",
            "저가",
            "거래대금",
            "대업종구분",
            "소업종구분",
            "종목정보",
            "종목정보",
            "수정주가이벤트",
            "전일종가",
        ]

        self.result = []

        d = datetime.date.today()
        self.lineEdit_date.setText(str(d))

        XQ = t8424(parent=self)
        XQ.Query()

        self.조회건수 = 10
        self.XQ_t1514 = t1514(parent=self)

    def OnReceiveMessage(self, systemError, messageCode, message):
        pass

    def OnReceiveData(self, szTrCode, result):
        if szTrCode == "t8424":
            df = result[0]
            with (
                get_db_connection() as conn
            ):  # with 문을 사용하여 자동으로 conn.close()
                cursor = conn.cursor()
                query = "insert or replace into 업종코드(업종명, 업종코드) values(?, ?)"
                cursor.executemany(query, df.values.tolist())
                cursor.close()

            self.업종코드테이블 = result[0]
            self.업종코드테이블["컬럼"] = (
                ">> "
                + self.업종코드테이블["업종코드"]
                + " : "
                + self.업종코드테이블["업종명"]
            )
            self.업종코드테이블 = self.업종코드테이블.sort_values(
                ["업종코드", "업종명"], ascending=[True, True]
            )
            self.comboBox.addItems(self.업종코드테이블["컬럼"].values)

        if szTrCode == "t1514":
            try:
                CTS일자, df = result
            except (TypeError, ValueError) as e:
                logger.error(f"t1514 result error: {e}, result: {result}")
                return

            with (
                get_db_connection() as conn
            ):  # with 문을 사용하여 자동으로 conn.close()
                cursor = conn.cursor()
                query = "insert or replace into 업종정보(일자, 지수, 전일대비구분, 전일대비, 등락율, 거래량, 거래증가율, 거래대금1, 상승, 보합, 하락, 상승종목비율, 외인순매수, 시가, 고가, 저가, 거래대금2, 상한, 하한, 종목수, 기관순매수, 업종코드, 거래비중, 업종배당수익률) values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
                cursor.executemany(query, df.values.tolist())
                cursor.close()
            try:
                # Check if UI object is still valid
                if not hasattr(self, "radioButton_all") or self.radioButton_all is None:
                    return

                if len(df) == int(self.조회건수) and self.radioButton_all.isChecked():
                    QTimer.singleShot(주문지연, lambda: self.Request(result=result))
                else:
                    self.백업한종목수 += 1
                    if len(self.백업할업종코드) > 0:
                        self.업종코드 = self.백업할업종코드.pop(0)
                        self.result = []

                        self.progressBar.setValue(
                            int(
                                self.백업한종목수
                                / (
                                    len(self.업종코드테이블.index)
                                    - self.comboBox.currentIndex()
                                )
                                * 100
                            )
                        )
                        S = "%s %s" % (self.업종코드[0], self.업종코드[1])
                        self.label_codename.setText(S)

                        QTimer.singleShot(주문지연, lambda: self.Request([]))
                    else:
                        QMessageBox.about(self, "백업완료", "백업을 완료함!")
            except Exception as e:
                클래스이름 = self.__class__.__name__
                함수이름 = inspect.currentframe().f_code.co_name
                logger.error(
                    "%s-%s %s: %s" % (클래스이름, 함수이름, get_linenumber(), e)
                )
                pass

    def Request(self, result=[]):
        if len(result) > 0:
            CTS일자, df = result
            self.XQ_t1514.Query(
                업종코드=self.업종코드[0],
                구분1="",
                구분2="1",
                CTS일자=CTS일자,
                조회건수=self.조회건수,
                비중구분="",
                연속조회=True,
            )
        else:
            self.XQ_t1514.Query(
                업종코드=self.업종코드[0],
                구분1="",
                구분2="1",
                CTS일자="",
                조회건수=self.조회건수,
                비중구분="",
                연속조회=False,
            )

    def Backup_One(self):
        idx = self.comboBox.currentIndex()

        self.백업한종목수 = 1
        self.백업할업종코드 = []
        self.업종코드 = self.업종코드테이블[idx : idx + 1][
            ["업종코드", "업종명"]
        ].values[0]
        self.기준일자 = self.lineEdit_date.text().strip().replace("-", "")
        self.result = []
        self.Request(result=[])

    def Backup_All(self):
        idx = self.comboBox.currentIndex()
        self.백업한종목수 = 1
        self.백업할업종코드 = list(
            self.업종코드테이블[idx:][["업종코드", "업종명"]].values
        )
        self.업종코드 = self.백업할업종코드.pop(0)
        self.기준일자 = self.lineEdit_date.text().strip().replace("-", "")

        self.progressBar.setValue(
            int(
                self.백업한종목수
                / (len(self.업종코드테이블.index) - self.comboBox.currentIndex())
                * 100
            )
        )
        S = "%s %s" % (self.업종코드[0], self.업종코드[1])
        self.label_codename.setText(S)

        self.result = []
        self.Request(result=[])


class View_업종정보(QDialog, FORM_CLASS_SEC_INFO):
    def __init__(self, parent=None):
        super(View_업종정보, self).__init__(parent)
        self.setAttribute(Qt.WA_DeleteOnClose)
        self.setupUi(self)

        self.setWindowTitle("업종정보 조회")

        self.parent = parent

        self.model = PandasModel()
        self.tableView.setModel(self.model)

        self.columns = [
            "일자",
            "지수",
            "전일대비구분",
            "전일대비",
            "등락율",
            "거래량",
            "거래증가율",
            "거래대금1",
            "상승",
            "보합",
            "하락",
            "상승종목비율",
            "외인순매수",
            "시가",
            "고가",
            "저가",
            "거래대금2",
            "상한",
            "하한",
            "종목수",
            "기관순매수",
            "업종코드",
            "거래비중",
            "업종배당수익률",
        ]

        self.result = []

        d = datetime.date.today()

        XQ = t8424(parent=self)
        XQ.Query()

    def OnReceiveMessage(self, systemError, messageCode, message):
        pass

    def OnReceiveData(self, szTrCode, result):
        if szTrCode == "t8424":
            df = result[0]
            df["컬럼"] = df["업종코드"] + " : " + df["업종명"]
            df = df.sort_values(["업종코드", "업종명"], ascending=[True, True])
            self.comboBox.addItems(df["컬럼"].values)

        if szTrCode == "t1511":
            logger.info(f"업종정보 result : {result} ")
            # CTS일자, df = result
            # 1. 리스트 형태의 데이터를 DataFrame으로 변환
            df = pd.DataFrame(result)
            self.model.update(df)
            for i in range(len(df.columns)):
                self.tableView.resizeColumnToContents(i)

    def inquiry(self):
        업종코드 = self.comboBox.currentText()[:3]
        조회건수 = self.lineEdit_date.text().strip().replace("-", "")

        XQ = t1511(parent=self)
        # XQ.Query(업종코드=업종코드,구분1='',구분2='1',CTS일자='',조회건수=조회건수,비중구분='', 연속조회=False)
        XQ.Query(업종코드=업종코드)


class View_업종별종목정보(QDialog, FORM_CLASS_SEC_STK):
    def __init__(self, parent=None):
        super(View_업종별종목정보, self).__init__(parent)
        self.setAttribute(Qt.WA_DeleteOnClose)
        self.setupUi(self)

        self.setWindowTitle("업종별 종목정보 조회")

        self.parent = parent

        self.model = PandasModel()
        self.tableView.setModel(self.model)

        self.columns = [
            "업종코드",
            "종목명",
            "현재가",
            "전일대비구분",
            "전일대비",
            "등락율",
            "누적거래량",
            "시가",
            "고가",
            "저가",
            "소진율",
            "베타계수",
            "PER",
            "외인순매수",
            "기관순매수",
            "거래증가율",
            "종목코드",
            "시가총액",
            "거래대금",
        ]

        self.result = []

        d = datetime.date.today()

        XQ = t8424(parent=self)
        XQ.Query()

        self.df = None

    def OnReceiveMessage(self, systemError, messageCode, message):
        pass

    def OnReceiveData(self, szTrCode, result):
        # [t8424: 업종코드 리스트]
        if szTrCode == "t8424":
            if not result:
                return
            df = result[0]
            df["컬럼"] = df["업종코드"] + " : " + df["업종명"]
            df = df.sort_values(["업종코드", "업종명"], ascending=[True, True])
            self.comboBox.clear()  # 중복 방지
            self.comboBox.addItems(df["컬럼"].values)

        # [t1516: 업종별 종목정보]
        if szTrCode == "t1516":
            # LS증권 t1516은 보통 (지수, 전일대비구분, 전일대비, 등락율, 데이터프레임) 5개를 반환
            if len(result) < 5:
                logger.error("t1516 응답 데이터 형식이 올바르지 않습니다.")
                return

            지수, 전일대비구분, 전일대비, 등락율, df = result

            # 데이터가 없을 경우 방어
            if df is None or len(df) == 0:
                return

            df["업종코드"] = self.업종코드
            df = df[self.columns]  # 설정된 컬럼 순서 유지

            # 1. 데이터 누적
            if self.df is None:
                self.df = df
            else:
                self.df = pd.concat([self.df, df], ignore_index=True)

            logger.info(
                f"📈 [t1516] {self.업종코드} 수신: {len(df)}건 (누적: {len(self.df)}건)"
            )

            # 2. 연속 조회 판단 (데이터가 딱 40건이면 다음 페이지가 있을 가능성이 높음)
            if len(df) == 40:
                # -21 에러(과부하) 방지를 위한 미세 지연
                time.sleep(0.5)

                # 마지막 종목코드를 Key로 다음 데이터 요청
                last_stock_code = df["종목코드"].values[-1]
                success = self.XQ.Query(
                    업종코드=self.업종코드, 종목코드=last_stock_code, 연속조회=True
                )

                if not success:
                    logger.error(f"❌ 연속 조회 요청 실패 (코드: {self.업종코드})")

            else:
                # 3. 모든 데이터 수신 완료 시 (DB 저장 및 UI 업데이트)
                self.finalize_t1516_data()

    def finalize_t1516_data(self):
        """모든 조회가 끝난 후 처리 로직"""
        self.model.update(self.df)
        for i in range(len(self.df.columns)):
            self.tableView.resizeColumnToContents(i)

        # DB 저장 ( Transaction)
        try:
            with get_db_connection() as conn:
                cursor = conn.cursor()
                # 쿼리 문 내 컬럼 개수와 df 컬럼 개수 일치 확인 필수 (19개)
                query = f"INSERT OR REPLACE INTO 업종별종목({','.join(self.columns)}) VALUES({','.join(['?'] * len(self.columns))})"
                cursor.executemany(query, self.df.values.tolist())
                conn.commit()
                logger.info(f"✅ DB 저장 완료: {len(self.df)}건")
        except Exception as e:
            logger.error(f"❌ DB 저장 오류: {e}")
        finally:
            # 다음 업종 조회를 위해 데이터 초기화 준비 (필요시)
            self.df = None

    def inquiry(self):
        # 001, 301 코스피, 코스탁 업종코드별 종목만 기본 조회 등록
        import pythoncom

        for i in range(self.comboBox.count()):
            self.업종코드 = self.comboBox.itemText(i)[:3]
            if self.업종코드 in ("001", "301"):
                print("업종코드", self.업종코드)
                self.df = None
                self.XQ = t1516(parent=self)
                self.XQ.Query(업종코드=self.업종코드)
                try:
                    pythoncom.PumpWaitingMessages()
                except Exception:
                    pass
                sleep(1)


class View_테마정보(QDialog, FORM_CLASS_THEME):
    def __init__(self, parent=None):
        super(View_테마정보, self).__init__(parent)
        self.setAttribute(Qt.WA_DeleteOnClose)
        self.setupUi(self)

        self.setWindowTitle("테마정보 조회")

        self.parent = parent

        self.model = PandasModel()
        self.tableView.setModel(self.model)

        self.columns = [
            "일자",
            "지수",
            "전일대비구분",
            "전일대비",
            "등락율",
            "거래량",
            "거래증가율",
            "거래대금1",
            "상승",
            "보합",
            "하락",
            "상승종목비율",
            "외인순매수",
            "시가",
            "고가",
            "저가",
            "거래대금2",
            "상한",
            "하한",
            "종목수",
            "기관순매수",
            "업종코드",
            "거래비중",
            "업종배당수익률",
        ]

        self.result = []

        d = datetime.date.today()

        XQ = t8425(parent=self)
        XQ.Query()

    def OnReceiveMessage(self, systemError, messageCode, message):
        pass

    def OnReceiveData(self, szTrCode, result):
        if szTrCode == "t8425":
            df = result[0]
            df["컬럼"] = df["테마코드"] + " : " + df["테마명"]
            df = df.sort_values(["테마코드", "테마명"], ascending=[True, True])
            self.comboBox.addItems(df["컬럼"].values)

        if szTrCode == "t1537":
            df0, df = result
            self.model.update(df)
            for i in range(len(df.columns)):
                self.tableView.resizeColumnToContents(i)

    def inquiry(self):
        테마코드 = self.comboBox.currentText()[:4]

        XQ = t1537(parent=self)
        XQ.Query(테마코드=테마코드, 연속조회=False)


class View_종목별투자자(QDialog, FORM_CLASS_INV_STK):
    def __init__(self, parent=None):
        super(View_종목별투자자, self).__init__(parent)
        self.setAttribute(Qt.WA_DeleteOnClose)
        self.setupUi(self)
        self.setWindowTitle("종목별 투자자 조회")
        self.parent = parent

        self.model = PandasModel()
        self.tableView.setModel(self.model)

        self.columns = [
            "일자",
            "종가",
            "전일대비구분",
            "전일대비",
            "등락율",
            "누적거래량",
            "사모펀드",
            "증권",
            "보험",
            "투신",
            "은행",
            "종금",
            "기금",
            "기타법인",
            "개인",
            "등록외국인",
            "미등록외국인",
            "국가외",
            "기관",
            "외인계",
            "기타계",
        ]

        self.result = []

        d = datetime.date.today()
        self.lineEdit_date.setText(str(d))

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

        if szTrCode == "t1702":
            CTSIDX, CTSDATE, df = result
            self.model.update(df)
            for i in range(len(df.columns)):
                self.tableView.resizeColumnToContents(i)

    def Request(self, _repeat=0):
        종목코드 = self.lineEdit_code.text().strip()
        기준일자 = self.lineEdit_date.text().strip().replace("-", "")

    def inquiry(self):
        종목코드 = self.comboBox.currentText()[3:9]
        조회건수 = self.lineEdit_date.text().strip().replace("-", "")

        XQ = t1702(parent=self)
        XQ.Query(
            종목코드=종목코드,
            종료일자="",
            금액수량구분="0",
            매수매도구분="0",
            누적구분="0",
            CTSDATE="",
            CTSIDX="",
        )


class View_종목별투자자2(QDialog, FORM_CLASS_INV_STK):
    def __init__(self, parent=None):
        super(View_종목별투자자2, self).__init__(parent)
        self.setAttribute(Qt.WA_DeleteOnClose)
        self.setupUi(self)
        self.setWindowTitle("종목별 투자자 조회")
        self.parent = parent

        self.model = PandasModel()
        self.tableView.setModel(self.model)

        self.columns = []

        self.result = []

        d = datetime.date.today()
        self.lineEdit_date.setText(str(d))

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

        if szTrCode == "t1717":
            df = result[0]
            self.model.update(df)
            for i in range(len(df.columns)):
                self.tableView.resizeColumnToContents(i)

    def Request(self, _repeat=0):
        종목코드 = self.lineEdit_code.text().strip()
        기준일자 = self.lineEdit_date.text().strip().replace("-", "")

    def inquiry(self):
        종목코드 = self.comboBox.currentText()[3:9]
        조회건수 = self.lineEdit_date.text().strip().replace("-", "")

        XQ = t1717(parent=self)
        XQ.Query(종목코드=종목코드, 구분="0", 시작일자="20170101", 종료일자="20172131")


class View_차트인덱스(QDialog, FORM_CLASS_CHART_IDX):
    def __init__(self, parent=None):
        super(View_차트인덱스, self).__init__(parent)
        self.setAttribute(Qt.WA_DeleteOnClose)
        self.setupUi(self)

        self.model = PandasModel()
        self.tableView.setModel(self.model)

        self.parent = parent

        self.columns = [
            "일자",
            "시간",
            "시가",
            "고가",
            "저가",
            "종가",
            "거래량",
            "지표값1",
            "지표값2",
            "지표값3",
            "지표값4",
            "지표값5",
            "위치",
        ]

        self.XQ_ChartIndex = ChartIndex(parent=self)
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

        if szTrCode == "CHARTINDEX":
            식별자, 지표ID, 레코드갯수, 유효데이터컬럼갯수, self.df = result

            self.model.update(self.df)
            for i in range(len(self.df.columns)):
                self.tableView.resizeColumnToContents(i)

    def OnReceiveChartRealData(self, szTrCode, lst):
        if szTrCode == "CHARTINDEX":
            식별자, result = lst
            지표ID, 레코드갯수, 유효데이터컬럼갯수, d = result
            lst = [
                [
                    d["일자"],
                    d["시간"],
                    d["시가"],
                    d["고가"],
                    d["저가"],
                    d["종가"],
                    d["거래량"],
                    d["지표값1"],
                    d["지표값2"],
                    d["지표값3"],
                    d["지표값4"],
                    d["지표값5"],
                    d["위치"],
                ]
            ]
            self.df = self.df.append(
                pd.DataFrame(lst, columns=self.columns), ignore_index=True
            )

            try:
                self.model.update(self.df)
                for i in range(len(self.df.columns)):
                    self.tableView.resizeColumnToContents(i)
            except Exception as e:
                클래스이름 = self.__class__.__name__
                함수이름 = inspect.currentframe().f_code.co_name
                logger.error(
                    "%s-%s %s: %s" % (클래스이름, 함수이름, get_linenumber(), e)
                )
                pass

    def inquiry(self):
        지표명 = self.lineEdit_name.text()
        종목코드 = self.comboBox.currentText()[3:9]
        요청건수 = self.lineEdit_cnt.text()
        실시간 = "1" if self.checkBox.isChecked() == True else "0"

        self.XQ_ChartIndex.Query(
            지표ID="",
            지표명=지표명,
            지표조건설정="",
            시장구분="1",
            주기구분="0",
            종목코드=종목코드,
            요청건수=요청건수,
            단위="3",
            시작일자="",
            종료일자="",
            수정주가반영여부="1",
            갭보정여부="1",
            실시간데이터수신자동등록여부=실시간,
        )


# Use FORM_CLASS_HOGA which is not defined yet, need to load it.
# Re-checking loaded forms. added FORM_CLASS_HOGA in startline 37 replacement.


class View_호가창정보(QDialog, FORM_CLASS_HOGA):
    def __init__(self, parent=None):
        super(View_호가창정보, self).__init__(parent)
        self.setAttribute(Qt.WA_DeleteOnClose)
        self.setupUi(self)
        self.parent = parent

        self.t1102_df = None

        self.매도호가컨트롤 = [
            self.label_offerho1,
            self.label_offerho2,
            self.label_offerho3,
            self.label_offerho4,
            self.label_offerho5,
            self.label_offerho6,
            self.label_offerho7,
            self.label_offerho8,
            self.label_offerho9,
            self.label_offerho10,
        ]

        self.매수호가컨트롤 = [
            self.label_bidho1,
            self.label_bidho2,
            self.label_bidho3,
            self.label_bidho4,
            self.label_bidho5,
            self.label_bidho6,
            self.label_bidho7,
            self.label_bidho8,
            self.label_bidho9,
            self.label_bidho10,
        ]

        self.매도호가잔량컨트롤 = [
            self.label_offerrem1,
            self.label_offerrem2,
            self.label_offerrem3,
            self.label_offerrem4,
            self.label_offerrem5,
            self.label_offerrem6,
            self.label_offerrem7,
            self.label_offerrem8,
            self.label_offerrem9,
            self.label_offerrem10,
        ]

        self.매수호가잔량컨트롤 = [
            self.label_bidrem1,
            self.label_bidrem2,
            self.label_bidrem3,
            self.label_bidrem4,
            self.label_bidrem5,
            self.label_bidrem6,
            self.label_bidrem7,
            self.label_bidrem8,
            self.label_bidrem9,
            self.label_bidrem10,
        ]

        with get_db_connection() as conn:  # with 문을 사용하여 자동으로 conn.close()
            query = GET_STOCK_LIST
            df = pd.read_sql_query(query, con=conn)  # Changed from pdsql to pd

        self.kospi_codes = df.query("구분=='1'")["종목코드"].values.tolist()
        self.kosdaq_codes = df.query("구분=='2'")["종목코드"].values.tolist()

        # XQ = t8436(parent=self)
        # XQ.Query(구분="0")
        XQ = t1102(parent=self)
        XQ.Query(종목코드="234340")

        self.kospi_askbid = H1_(parent=self)
        self.kosdaq_askbid = HA_(parent=self)

    def inquiry(self, 종목코드):
        XQ = t1102(parent=self)
        XQ.Query(종목코드=종목코드)

    def OnReceiveMessage(self, systemError, messageCode, message):
        pass

    def OnReceiveData(self, szTrCode, result):

        if szTrCode == "t1102":
            self.t1102_df = pd.DataFrame(result)
            logger.info(
                f"호가창정보 szTrCode, self.t1102_df : {szTrCode}, {self.t1102_df}"
            )
        if szTrCode == "t8436":
            self.종목코드테이블 = result[0][result[0]["ETF구분"] == 0]
            # Ensure you have a copy of the DataFrame
            self.종목코드테이블 = self.종목코드테이블.copy()
            self.종목코드테이블["컬럼"] = (
                self.종목코드테이블["종목코드"] + " : " + self.종목코드테이블["종목명"]
            )
            self.종목코드테이블 = self.종목코드테이블.sort_values(
                ["종목코드", "종목명"], ascending=[True, True]
            )
            self.comboBox.addItems(self.종목코드테이블["컬럼"].values)

    def OnReceiveRealData(self, szTrCode, result):
        try:
            # logger.info(f" 호가창정보 szTrCode, result : {szTrCode}, {result}")
            s = "%s:%s:%s" % (
                result["호가시간"][0:2],
                result["호가시간"][2:4],
                result["호가시간"][4:6],
            )
            self.label_hotime.setText(s)

            for i in range(0, 10):
                self.매도호가컨트롤[i].setText(result["매도호가"][i])
                self.매수호가컨트롤[i].setText(result["매수호가"][i])
                self.매도호가잔량컨트롤[i].setText(result["매도호가잔량"][i])
                self.매수호가잔량컨트롤[i].setText(result["매수호가잔량"][i])

            self.label_offerremALL.setText(result["총매도호가잔량"])
            self.label_bidremALL.setText(result["총매수호가잔량"])
            self.label_donsigubun.setText(result["동시호가구분"])
            self.label_alloc_gubun.setText(result["배분적용구분"])
        except Exception as e:
            클래스이름 = self.__class__.__name__
            함수이름 = inspect.currentframe().f_code.co_name
            logger.error("%s-%s %s: %s" % (클래스이름, 함수이름, get_linenumber(), e))
            pass

    def AddCode(self):
        종목코드 = self.comboBox.currentText().strip()[0:6]

        self.kospi_askbid.UnadviseRealData()
        self.kosdaq_askbid.UnadviseRealData()

        if 종목코드 in self.kospi_codes:
            self.kospi_askbid.AdviseRealData(종목코드=종목코드)
        if 종목코드 in self.kosdaq_codes:
            self.kosdaq_askbid.AdviseRealData(종목코드=종목코드)


class View_뉴스(QDialog, FORM_CLASS_NEWS):
    def __init__(self, parent=None):
        super(View_뉴스, self).__init__(parent)
        self.setAttribute(Qt.WA_DeleteOnClose)
        self.setupUi(self)

        self.parent = parent

        self.news = NWS(parent=self)

    def OnReceiveRealData(self, szTrCode, result):
        onReceiveRealData = "{}:{} - {}-{}-{}-{}\r".format(
            result["날짜"],
            result["시간"],
            result["뉴스구분자"],
            result["키값"],
            result["종목코드"],
            result["제목"],
        )
        try:
            if NWS_RSLT_BUY_YN == "Y":
                self.뉴스등록(result)
        except Exception as e:
            클래스이름 = self.__class__.__name__
            함수이름 = inspect.currentframe().f_code.co_name
            logger.error("%s-%s %s: %s" % (클래스이름, 함수이름, get_linenumber(), e))
            pass

    def AddCode(self):
        self.news.AdviseRealData()

    def RemoveCode(self):
        self.news.UnadviseRealData()

    def 뉴스등록(self, result):
        if len(result["종목코드"]) > 0 and not isOverCloseTime():
            data = [
                result["날짜"],
                result["시간"],
                result["뉴스구분자"],
                result["키값"],
                result["종목코드"],
                result["제목"],
                result["BODY길이"],
            ]
            self.parent.gooses[1].뉴스등록(data=data)
            # print(f"뉴스 등록 result : {result}")
            for keyWord in self.parent.positiveSignal:
                if result["제목"].find(keyWord) >= 0:  # 제목에서 키워드 있으면
                    for keyWord in self.parent.negativeSignal:
                        if (
                            result["제목"].find(keyWord) >= 0
                        ):  # 긍정단어포함이지만 부정단어 포함시
                            logger.info(
                                f"[{self.__class__.__name__}] negative keyword {keyWord}"
                            )
                            return
                    regNewsStockCodes = self.parent.gooses[1].뉴스종목등록(data=data)
                    # DB에 등록된 뉴스종목코드 리스트에도 등록
                    if regNewsStockCodes is not None:
                        for newsStockCode in regNewsStockCodes:
                            # 주의: self.parent.newsStockCodes가 GoldenGoose MainWindow에 있어야 함.
                            # check if initialized in MainWindow
                            if hasattr(self.parent, "newsStockCodes"):
                                if (
                                    newsStockCode not in self.parent.newsStockCodes
                                ):  # 중복입력방지
                                    self.parent.newsStockCodes.append(newsStockCode)
