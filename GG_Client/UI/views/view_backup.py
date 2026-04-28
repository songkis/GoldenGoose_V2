import datetime
import inspect
import threading
from concurrent.futures import ThreadPoolExecutor
import pandas.io.sql as pdsql

from PyQt5.QtWidgets import QDialog, QMessageBox
from PyQt5.QtCore import Qt, QTimer, QDateTime, QTime
from PyQt5 import uic

from UI.resource_resolver import resource_path
from util.CommUtils import (
    get_db_connection,
    isOverCloseTime,
    isOverBackupTime,
    get_linenumber,
)
from config.ai_settings import (
    MIN_TERM,
    MIN_TERM_CD,
    MIN_TERM_GAIN_STK_CNT,
    SYS_ID,
    BATCH_PRGS,
)
from config.comm_settings import 주문지연

from xing.XAQuaries import t8436, t1305, t1302
from SQL.sql import (
    GET_DAILY_BACKUP_TARGET,
    GET_GAIN_LAST_60_STK_LIST,
    GET_MIN_TERM_GAIN_STK_LIST,
    GET_STOCK_LIST,
)

from config.telegram_setting import ToTelegram

logger = None  # 초기값


def set_logger(external_logger):
    global logger
    logger = external_logger


# Load UI Views
try:
    FORM_CLASS_DAILY, _ = uic.loadUiType(resource_path("UI/일별가격정보백업.ui"))
    FORM_CLASS_MINUTE, _ = uic.loadUiType(resource_path("UI/분별가격정보백업.ui"))
except Exception as e:
    logger.error(f"Failed to load Backup UIs: {e}")
    raise


class View_일별가격정보백업(QDialog, FORM_CLASS_DAILY):
    def __init__(self, parent=None):
        super(View_일별가격정보백업, self).__init__(parent)
        self.setAttribute(Qt.WA_DeleteOnClose)
        self.setupUi(self)
        self.setWindowTitle("가격 정보 백업")
        self.parent = parent
        self.result = []
        self.주문지연 = 3 * 1000
        d = datetime.date.today()
        # self.d='2023-05-09'
        self.lineEdit_date.setText(str(d))

        self.XQ_t8436 = t8436(parent=self)
        self.XQ_t8436.Query(구분="0")

        # self.조회건수 = 1300 #5년치
        # self.조회건수 = 780 #3년치
        self.조회건수 = 380  # 제공되는 최대치
        self.다운로드할종목수 = 1
        self.XQ_t1305 = t1305(parent=self)

    def OnReceiveMessage(self, systemError, messageCode, message):
        pass

    def OnReceiveData(self, szTrCode, result):
        if szTrCode == "t8436":
            self.종목코드테이블 = result[0][result[0]["ETF구분"] == 0]
            # self.종목코드테이블 = result[0] # 전체를 가져와서 학습데이터 많이 백업.
            # Ensure you have a copy of the DataFrame
            self.종목코드테이블 = self.종목코드테이블.copy()
            self.종목코드테이블["컬럼"] = (
                ">> "
                + self.종목코드테이블["종목코드"]
                + " : "
                + self.종목코드테이블["종목명"]
            )
            # Assuming self.종목코드테이블 is your DataFrame
            # self.종목코드테이블.loc[:, '컬럼'] = ">> " + self.종목코드테이블['종목코드'] + " : " + self.종목코드테이블['종목명']
            self.종목코드테이블 = self.종목코드테이블.sort_values(
                ["종목코드", "종목명"], ascending=[True, True]
            )
            self.comboBox.addItems(self.종목코드테이블["컬럼"].values)

        if szTrCode == "t1305":
            CNT, 날짜, IDX, df = result
            # print('OnReceiveData CNT %s, 날짜 %s, IDX %s, df %s '%(CNT, 날짜, IDX, df) )
            with (
                get_db_connection() as conn
            ):  # with 문을 사용하여 자동으로 conn.close()
                cursor = conn.cursor()
                query = "insert or replace into 일별주가( 날짜, 시가, 고가, 저가, 종가, 전일대비구분, 전일대비, 등락율, 누적거래량, 거래증가율, 체결강도, 소진율, 회전율, 외인순매수, 기관순매수, 종목코드, 누적거래대금, 개인순매수, 시가대비구분, 시가대비, 시가기준등락율, 고가대비구분, 고가대비, 고가기준등락율, 저가대비구분, 저가대비, 저가기준등락율, 시가총액) values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
                cursor.executemany(query, df.values.tolist())
                cursor.close()
            try:
                if (
                    int(CNT) == int(self.조회건수)
                    and self.radioButton_all.isChecked() == True
                ):
                    QTimer.singleShot(
                        self.주문지연, lambda: self.Request(result=result)
                    )
                else:
                    self.백업한종목수 += 1
                    if len(self.백업할종목코드) > 0:
                        self.result = []
                        self.종목코드 = self.백업할종목코드.pop(0)
                        self.기준일자 = (
                            self.lineEdit_date.text().strip().replace("-", "")
                        )
                        # self.조회건수 = int(self.종목코드[2])

                        if self.다운로드할종목수 and self.다운로드할종목수 > 0:
                            self.progressBar.setValue(
                                int(self.백업한종목수 / self.다운로드할종목수 * 100)
                            )
                        else:
                            total_remaining = (
                                len(self.종목코드테이블.index)
                                - self.comboBox.currentIndex()
                            )
                            if total_remaining > 0:
                                self.progressBar.setValue(
                                    int(self.백업한종목수 / total_remaining * 100)
                                )
                            else:
                                self.progressBar.setValue(100)

                        S = "%s %s %d건" % (
                            self.종목코드[0],
                            self.종목코드[1],
                            self.조회건수,
                        )
                        self.label_codename.setText(S)

                        QTimer.singleShot(self.주문지연, lambda: self.Request([]))
                    else:
                        self.complete_backup()

            except Exception as e:
                클래스이름 = self.__class__.__name__
                함수이름 = inspect.currentframe().f_code.co_name
                logger.error(
                    "%s-%s %s: %s" % (클래스이름, 함수이름, get_linenumber(), e)
                )
                pass

    def Request(self, result=[]):
        if len(result) > 0:
            CNT, 날짜, IDX, df = result
            # print('Request CNT %s, 날짜 %s, IDX %s, df %s '%(CNT, 날짜, IDX, df) )
            self.XQ_t1305.Query(
                종목코드=self.종목코드[0],
                일주월구분="1",
                날짜=날짜,
                IDX=IDX,
                건수=self.조회건수,
                연속조회=True,
            )
        else:
            try:
                self.XQ_t1305.Query(
                    종목코드=self.종목코드[0],
                    일주월구분="1",
                    날짜="",
                    IDX="",
                    건수=self.조회건수,
                    연속조회=False,
                )
            except Exception as e:
                클래스이름 = self.__class__.__name__
                함수이름 = inspect.currentframe().f_code.co_name
                logger.error(
                    "%s-%s %s: %s" % (클래스이름, 함수이름, get_linenumber(), e)
                )
                pass

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

        # 백업완료 테스트
        self.complete_backup()
        # self.parent.request_close()

    def Backup_All(self):
        idx = self.comboBox.currentIndex()
        self.백업한종목수 = 1

        # db에 없는 부분만 가져온다.
        with get_db_connection() as conn:  # with 문을 사용하여 자동으로 conn.close()
            query = GET_DAILY_BACKUP_TARGET
            df = pdsql.read_sql_query(query, con=conn)
        # 백업할 건이 없는 종목은 제외처리.
        df = df[df["days_difference"] != 0].copy()
        logger.info(
            f"[{self.__class__.__name__}] Backup_All df: {df}, {df.empty}, {len(df)}"
        )
        if df.empty or len(df) == 0:
            self.complete_backup()
            return
        self.다운로드할종목수 = len(df)
        # print( 'df : %s'% df)
        # Convert the '종목코드' column to a list
        self.백업할종목코드 = df[
            ["종목코드", "종목명", "days_difference"]
        ].values.tolist()
        # self.백업할종목코드 = list(self.종목코드테이블[idx:][['종목코드','종목명']].values)
        # print( 'self.백업할종목코드 : %s'% self.백업할종목코드)

        self.종목코드 = self.백업할종목코드.pop(0)
        self.기준일자 = self.lineEdit_date.text().strip().replace("-", "")
        if self.종목코드[2] is not None:
            self.조회건수 = int(self.종목코드[2])

        if self.다운로드할종목수 and self.다운로드할종목수 > 0:
            self.progressBar.setValue(
                int(self.백업한종목수 / self.다운로드할종목수 * 100)
            )
        else:
            total_remaining = (
                len(self.종목코드테이블.index) - self.comboBox.currentIndex()
            )
            if total_remaining > 0:
                self.progressBar.setValue(
                    int(self.백업한종목수 / total_remaining * 100)
                )
            else:
                self.progressBar.setValue(100)

        S = "%s %s %d건" % (self.종목코드[0], self.종목코드[1], self.조회건수)
        self.label_codename.setText(S)

        self.result = []
        self.Request(result=[])

    def complete_backup(self):

        # Check parent attributes safely
        if not hasattr(self.parent, "일별가격정보백업완료"):
            logger.warning("Parent missing '일별가격정보백업완료' attribute")

        if not isOverBackupTime():
            today = datetime.datetime.today()
            logger.info(
                f"[{self.__class__.__name__}] GoldenGoose {today.strftime('%Y.%m.%d')} 일봉 가격정보 백업이 완료됨!"
            )
            ToTelegram(
                f"GoldenGoose {today.strftime('%Y.%m.%d')} 일봉 가격정보 백업이 완료됨!"
            )
            # === 1) 종료 준비 단계 (시간 약간 걸릴 수 있음) ===
            try:
                # view_market_info 등에서 parent가 다른 View 다이얼로그로 지정될 수 있으므로,
                # run_file 메서드를 가지고 있는 상위 parent(MainWindow)를 찾는다.
                main_win = self.parent
                while main_win and not hasattr(main_win, "run_file"):
                    if hasattr(main_win, "parent") and not callable(main_win.parent):
                        main_win = main_win.parent
                    elif hasattr(main_win, "parent") and callable(main_win.parent):
                        main_win = main_win.parent()
                    else:
                        break

                if main_win and hasattr(main_win, "run_file"):
                    main_win.run_file(BATCH_PRGS[0])
                else:
                    logger.warning(
                        "run_file 속성을 가진 상위 윈도우를 찾을 수 없습니다."
                    )
            except Exception as e:
                logger.error(f"request_close 오류:{e}")

        else:
            try:
                if hasattr(self.parent, "일별가격정보백업완료"):
                    self.parent.일별가격정보백업완료 = True

                if (
                    isOverCloseTime()
                    and hasattr(self.parent, "dialog")
                    and "분별가격정보백업" in self.parent.dialog
                ):
                    self.parent.dialog["분별가격정보백업"].start_timer()
            except Exception as e:
                logger.error(f"request_close 오류:{e}")

            QMessageBox.about(
                self, "백업완료", "백업을 완료함! 일별주가로 점수계산 시작함!"
            )


from PyQt5.QtCore import Qt, QTimer, QDateTime, QTime, pyqtSignal


class View_분별가격정보백업(QDialog, FORM_CLASS_MINUTE):
    query_finished = pyqtSignal(object)

    def __init__(self, parent=None):
        super(View_분별가격정보백업, self).__init__(parent)
        self.setAttribute(Qt.WA_DeleteOnClose)
        self.setupUi(self)
        self.setWindowTitle("가격 정보 백업")
        self.parent = parent

        # ThreadPoolExecutor 생성
        self.minExecutor = ThreadPoolExecutor(
            max_workers=1
        )  # 필요에 따라 max_workers 조정

        self.query_finished.connect(self._on_backup_all_query_finished)

        self.columns = ["체결시간", "현재가", "시가", "고가", "저가", "거래량"]

        with get_db_connection() as conn:  # with 문을 사용하여 자동으로 conn.close()
            query = GET_STOCK_LIST  # AIGoose,2 Run()에서 실제사용은 ETF제외.
            self.종목코드테이블 = pdsql.read_sql_query(query, con=conn)

        self.result = []

        self.XQ_t8436 = t8436(parent=self)
        self.XQ_t8436.Query(구분="0")

        self.조회건수 = 30  # int(round((TREND_MINUTE_WIN_CNT *3) / 4., 2))

        self.XQ_t1302 = t1302(parent=self)

        # [ZMQ] Publisher 초기화
        try:
            from util.zmq_manager import get_shared_publisher, TOPIC_EVENT

            self.zmq_pub = get_shared_publisher()
            self.TOPIC_EVENT = TOPIC_EVENT
            if self.zmq_pub:
                logger.info(
                    f"[View_분별가격정보백업] ZMQ Publisher initialized successfully."
                )
            else:
                logger.warning(
                    f"[View_분별가격정보백업] ZMQ Publisher is None after initialization."
                )
        except Exception as e:
            logger.error(f"[View_분별가격정보백업] ZMQ Init Error: {e}")
            self.zmq_pub = None

    def closeEvent(self, event):
        # Shared Publisher - Do not close individually
        # if self.zmq_pub:
        #     self.zmq_pub.close()
        event.accept()

    def schedule_next_run(self):
        now = QDateTime.currentDateTime()
        target_time = QDateTime(
            now.date(), QTime(9, MIN_TERM, 1)
        )  # 장시작후 MIN_TERM 지난후 백업.
        # print( f"schedule_next_run isOverCloseTime() :{isOverCloseTime()}")
        if now > target_time and not isOverCloseTime():
            # target_time = target_time.addDays(1)
            target_time = now.addSecs(3)
        elif now > target_time:
            target_time = target_time.addDays(1)
        delay = now.msecsTo(target_time)
        print(f"분별가격정보 백업 delay: {delay}")
        QTimer.singleShot(delay, self.start_timer)

    def start_timer(self):
        env = "Unknown"
        if hasattr(self.parent, "account_dict"):
            env = self.parent.account_dict.get("거래환경", "Unknown")

        logger.info(
            f"[{self.__class__.__name__}] ==---->>> {env} GoldenGoose OnLogin 포트폴리오, 분봉백업대상 분봉 가격정보백업 query {MIN_TERM}분주기 <<<-------=="
        )
        thrd = threading.Thread(
            target=ToTelegram,
            args=(
                f"GoldenGoose 포트폴리오, 분봉백업대상의 {MIN_TERM}분봉 가격정보 백업이 시작됨!",
            ),
            daemon=True,
        )
        thrd.start()

        # os.system("shutdown /s /t 300")
        self.Backup_All()
        self.timer = QTimer(self)
        self.timer.timeout.connect(self.runBackup_All_and_check_time)
        self.timer.start(MIN_TERM * 60 * 1000)  # MIN_TERM 단위 실행

    def runBackup_All_and_check_time(self):
        self.Backup_All()
        # running 중에만
        # if isOverCloseTime():
        #    self.timer.stop()
        #    logger.info(
        #        f"[{self.__class__.__name__}] ===> 15:30 도달! 분별가격정보백업 타이머 종료됨."
        #    )

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
            df["종목코드"] = self.종목코드[0]

            reg_dt = datetime.datetime.now().strftime("%Y-%m-%d")
            df["등록일자"] = reg_dt

            try:
                import traceback

                # self.minExecutor.submit(self._handle_save_min_data, self.종목코드, df)
                # ThreadPoolExecutor is safe, but let's log the attempt
                logger.debug(
                    f"[{self.__class__.__name__}] Submitting save task for {self.종목코드[0]} ({len(df)} bars)"
                )
                self.minExecutor.submit(self._handle_save_min_data, self.종목코드, df)
            except Exception as e:
                logger.error(
                    f"[{self.__class__.__name__}] Failed to submit save task: {e}"
                )

            try:
                if len(df) == int(self.조회건수) and self.radioButton_all.isChecked():
                    QTimer.singleShot(주문지연, lambda: self.Request(result=result))
                else:
                    self.백업한종목수 += 1
                    if len(self.백업할종목코드) > 0:
                        self.prepare_request()
                        QTimer.singleShot(주문지연, lambda: self.Request([]))
                    else:
                        if hasattr(self.parent, "statusbar"):
                            self.parent.statusbar.showMessage("백업을 완료함!")

                        logger.info(
                            f"[{self.__class__.__name__}] REACHED: All stocks in batch processed. Pending ZMQ check (pub={self.zmq_pub is not None})."
                        )

                        # [ZMQ] 수집 완료 (Batch Complete) 이벤트 전송
                        if self.zmq_pub:
                            try:
                                event_data = {
                                    "event": "COLLECTION_ALL_COMPLETE",
                                    "ticker": "ALL",
                                    "timestamp": datetime.datetime.now().strftime(
                                        "%Y-%m-%d %H:%M:%S"
                                    ),
                                }
                                self.zmq_pub.publish_data(self.TOPIC_EVENT, event_data)
                                logger.info(
                                    f"[ZMQ] Sent COLLECTION_ALL_COMPLETE (Batch) event"
                                )
                            except Exception as e:
                                logger.error(f"[ZMQ] Failed to publish event: {e}")
            except Exception as e:
                logger.error(
                    f"[{self.__class__.__name__}] OnReceiveData(t1302) Error for {self.종목코드[0] if hasattr(self, '종목코드') else 'Unknown'}: {e}"
                )
                import traceback

                logger.debug(traceback.format_exc())
                # 에러가 나더라도 다음 종목으로 넘어가도록 시도
                if hasattr(self, "백업할종목코드") and len(self.백업할종목코드) > 0:
                    self.백업한종목수 += 1
                    try:
                        self.prepare_request()
                        QTimer.singleShot(주문지연, lambda: self.Request([]))
                    except Exception:
                        pass

    def _handle_save_min_data(self, 종목코드, df):
        try:
            if df is None or len(df) == 0:
                logger.warning(
                    f"[{self.__class__.__name__}] No data to save for {종목코드[1]}({종목코드[0]})"
                )
                return

            with (
                get_db_connection() as conn
            ):  # with 문을 사용하여 자동으로 conn.close()
                cursor = conn.cursor()
                query = """insert or replace into 분별주가(시간, 종가, 전일대비구분, 전일대비, 등락율, 체결강도, 매도체결수량, 매수체결수량, 순매수체결량, 매도체결건수, 매수체결건수, 순체결건수, 거래량, 시가, 고가, 저가, 체결량, 매도체결건수시간, 매수체결건수시간, 매도잔량, 매수잔량, 시간별매도체결량, 시간별매수체결량,종목코드, 등록일자) 
                values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""
                cursor.executemany(query, df.values.tolist())
                # Autocommit 모드가 아닐 경우를 대비해 commit 호출 검토 (현재는 None이므로 자동)
                # conn.commit()
                cursor.close()
            logger.info(
                f"[{self.__class__.__name__}] SUCCESS: Saved {len(df)} bars for {종목코드[1]}({종목코드[0]})"
            )

            #  Send COLLECTION_ONE_COMPLETE even if nothing to backup
            # This ensures IntradayTrading is triggered to check for signals.
            if self.zmq_pub:
                try:
                    event_data = {
                        "event": "COLLECTION_ONE_COMPLETE",
                        "ticker": 종목코드,
                        "timestamp": datetime.datetime.now().strftime(
                            "%Y-%m-%d %H:%M:%S"
                        ),
                    }
                    self.zmq_pub.publish_data(self.TOPIC_EVENT, event_data)
                    logger.info(
                        f"[ZMQ] Sent COLLECTION_ONE_COMPLETE (Empty Batch) event"
                    )
                except Exception as e:
                    logger.error(f"[ZMQ] Failed to publish event: {e}")

        except Exception as e:
            logger.error(
                f"[{self.__class__.__name__}] _handle_save_min_data Error for {종목코드[1]}({종목코드[0]}): {e}"
            )
            import traceback

            logger.debug(traceback.format_exc())

    def Request(self, result=[]):
        if len(result) > 0:
            시간CTS, df = result
            self.XQ_t1302.Query(
                종목코드=self.종목코드[0],
                작업구분=self.틱범위,
                시간=시간CTS,
                건수=self.조회건수,
                연속조회=True,
            )
        else:
            self.XQ_t1302.Query(
                종목코드=self.종목코드[0],
                작업구분=self.틱범위,
                시간="",
                건수=self.조회건수,
                연속조회=False,
            )

    def Backup_One(self):
        idx = self.comboBox.currentIndex()

        self.백업한종목수 = 1
        self.백업할종목코드 = []
        self.종목코드 = self.종목코드테이블[idx : idx + 1][
            ["종목코드", "종목명"]
        ].values[0]
        self.틱범위 = self.comboBox_min.currentText()[0:1].strip()
        if self.틱범위[0] == "0":
            self.틱범위 = self.틱범위[1:]
        self.result = []
        self.Request(result=[])

    def Backup_All(self):
        # current_sys_id 결정 (global 보단 parent의 것을 우선)
        effective_sys_id = SYS_ID
        if (
            hasattr(self.parent, "account_dict")
            and "SYS_ID" in self.parent.account_dict
        ):
            effective_sys_id = int(self.parent.account_dict["SYS_ID"])

        logger.info(
            f"[{self.__class__.__name__}] Backup_All triggered. effective_sys_id: {effective_sys_id}, isOverCloseTime: {isOverCloseTime()}"
        )

        # logger.info(f"[{self.__class__.__name__}] Backup_All !!!")
        idx = self.comboBox.currentIndex()
        self.백업한종목수 = 1

        if isOverCloseTime():  # or  장 시작전 장 종료 후.
            query = GET_GAIN_LAST_60_STK_LIST
            params = (
                MIN_TERM_CD,
                MIN_TERM,
                MIN_TERM,
                MIN_TERM_GAIN_STK_CNT,
            )
        else:  # 장중
            query = GET_MIN_TERM_GAIN_STK_LIST
            params = (
                MIN_TERM_CD,  # 1
                MIN_TERM,  # 2
                MIN_TERM,  # 3
                effective_sys_id,  # 4
                MIN_TERM_CD,  # 5
                MIN_TERM,  # 6
                MIN_TERM,  # 7
                effective_sys_id,  # 8
                MIN_TERM_GAIN_STK_CNT,  # 9
            )

        def _do_query():
            try:
                msg = (
                    f"[{self.__class__.__name__}] _do_query: Starting database read..."
                )
                logger.info(msg)
                # ToTelegram(f"DEBUG: {msg}") # Too noisy if repeated, but good for one-off

                with get_db_connection() as conn:
                    df = pdsql.read_sql_query(query, params=params, con=conn)

                msg = f"[{self.__class__.__name__}] _do_query: Read {len(df) if df is not None else 0} rows. Emitting signal."
                logger.info(msg)

                # pyqtSignal을 사용하여 메인스레드 복귀 (스레드 안전)
                self.query_finished.emit(df)
            except Exception as e:
                logger.exception(f"Backup_All_Async_Query_Failed: {e}")
                ToTelegram(f"ERROR: Backup_All_Async_Query_Failed: {e}")

        # self.minExecutor.submit(_do_query)
        t = threading.Thread(target=_do_query, daemon=True)
        t.start()

    def _on_backup_all_query_finished(self, df):
        try:
            logger.info(
                f"[{self.__class__.__name__}] Callback: _on_backup_all_query_finished triggered."
            )
            is_backup_complete = False
            if hasattr(self.parent, "일별가격정보백업완료"):
                is_backup_complete = self.parent.일별가격정보백업완료

            logger.info(
                f"분별가격 정보: {datetime.datetime.now()} {len(df) if df is not None else 0} 일별가격정보백업완료 {is_backup_complete} "
            )

            if isOverCloseTime() and is_backup_complete:
                if df is None or len(df) == 0:
                    main_win = self.parent
                    while main_win and not hasattr(main_win, "run_file"):
                        if hasattr(main_win, "parent") and not callable(
                            main_win.parent
                        ):
                            main_win = main_win.parent
                        elif hasattr(main_win, "parent") and callable(main_win.parent):
                            main_win = main_win.parent()
                        else:
                            break

                    if main_win and hasattr(main_win, "run_file"):
                        main_win.run_file(BATCH_PRGS[1])
                    else:
                        logger.warning(
                            "run_file 속성을 가진 상위 윈도우를 찾을 수 없습니다."
                        )

            if df is not None and len(df) > 0:
                self.백업할종목코드 = df[
                    ["종목코드", "종목명", "tick", "missing_min_candles"]
                ].values.tolist()
                logger.info(
                    f"[{self.__class__.__name__}] 백업 할 종목코드: {len(self.백업할종목코드)}"
                )
                self.prepare_request()
                self.Request(result=[])
            else:
                #  Send COLLECTION_ALL_COMPLETE even if nothing to backup
                # This ensures IntradayTrading is triggered to check for signals.
                if self.zmq_pub:
                    try:
                        event_data = {
                            "event": "COLLECTION_ALL_COMPLETE",
                            "ticker": "ALL",
                            "timestamp": datetime.datetime.now().strftime(
                                "%Y-%m-%d %H:%M:%S"
                            ),
                        }
                        self.zmq_pub.publish_data(self.TOPIC_EVENT, event_data)
                        logger.info(
                            f"[ZMQ] Sent COLLECTION_ALL_COMPLETE (Empty Batch) event"
                        )
                    except Exception as e:
                        logger.error(f"[ZMQ] Failed to publish event: {e}")
        except Exception as e:
            logger.exception(f"_on_backup_all_query_finished 예외: {e}")

    def prepare_request(self):

        self.종목코드 = self.백업할종목코드.pop(0)

        self.틱범위 = self.comboBox_min.currentText()[0:1].strip()

        if self.종목코드[2] is not None:
            self.틱범위 = self.종목코드[2]
        elif self.틱범위[0] == "0":
            self.틱범위 = self.틱범위[1:]

        self.조회건수 = self.종목코드[3]

        if hasattr(self.progressBar, "setValue"):
            #  Safe checking for ZeroDivision
            try:
                total_remaining = (
                    len(self.종목코드테이블.index) - self.comboBox.currentIndex()
                )
                if total_remaining > 0:
                    self.progressBar.setValue(
                        int(self.백업한종목수 / total_remaining * 100)
                    )
                else:
                    self.progressBar.setValue(100)
            except Exception as e:
                logger.debug(f"Progress bar update skip: {e}")
                pass

        S = "%s %s %s %s" % (
            self.종목코드[0],
            self.종목코드[1],
            self.틱범위,
            self.조회건수,
        )
        self.label_codename.setText(S)
        self.result = []
