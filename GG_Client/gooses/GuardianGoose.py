import datetime as dt
import queue
import threading
from threading import Thread

import pandas as pd
import pandas.io.sql as pdsql

import inspect
import time
from PyQt5 import QtCore
from PyQt5.QtCore import Qt
from PyQt5.QtGui import QPalette, QColor
from PyQt5.QtWidgets import QDialog, QFileDialog

from config.ai_settings import (
    SRCH_RSLT_BUY_YN,
)
from config.comm_settings import GDG_ID, GDG_NM, acf_DIR
from gooses.BaseGoose import BaseGoose
from gooses.lock_controllers import LockController
from SQL.sql import GET_STOCK_LIST
from util.CommUtils import (
    updateSearchStock,
    get_linenumber,
    to_numeric_safe,
    get_db_stats,
    get_db_connection,
    isOverCloseTime,
    isBeforeOpenTime,
)
from config.telegram_setting import ToTelegram
from xing.XAQuaries import t1857, CSPAT00600
from xing.XAReals import SC1

logger = None  # 초기값


def set_logger(external_logger):
    global logger
    logger = external_logger


# __PATHNAME__ = os.path.dirname(sys.argv[0])
# __PLUGINDIR__ = os.path.abspath(__PATHNAME__)

GG_NM = GDG_NM
# exe_env = False
# if getattr(sys, 'frozen', False):
#    # PyInstaller로 빌드된 경우
#    base_path = sys._MEIPASS
#    exe_env = True
# else:
#    # 개발 환경
#    base_path = os.path.dirname(__file__)


class RecvDataQ:
    def __init__(self, goose, szTrCode, result):
        self.goose = goose
        self.szTrCode = szTrCode
        self.result = result


# if exe_env:
#    # 변환된 UI 모듈 import
#    Ui_GuardianGoose = globals()["UI.GuardianGoose_ui"].Ui_Dialog
# else:
#    #Ui_GuardianGoose, QtBaseClass_GuardianGoose = uic.loadUiType(os.path.join(base_path, UI_DIR_NM, "GuardianGoose.ui"))
#    Ui_GuardianGoose, QtBaseClass_GuardianGoose = uic.loadUiType(f"{__PLUGINDIR__}\\UI\\GuardianGoose.ui")

import UI.GuardianGoose_ui

Ui_GuardianGoose = UI.GuardianGoose_ui.Ui_Dialog  # 직접 참조


class CUIGuardianGoose(QDialog, Ui_GuardianGoose):
    def __init__(self, parent=None):
        super(__class__, self).__init__(parent)
        # self.setAttribute(Qt.WA_DeleteOnClose)
        Ui_GuardianGoose.__init__(self)
        self.setupUi(self)
        self.parent = parent

        pal = QPalette()
        pal.setColor(QPalette.Background, QColor(85, 93, 80))
        self.setAutoFillBackground(True)
        self.setPalette(pal)

    def SearchFile(self):
        RESDIR = acf_DIR

        fname = QFileDialog.getOpenFileName(
            self, "Open file", str(RESDIR), "조검검색(*.acf)"
        )
        self.lineEdit_filename.setText(fname[0])


class GuardianGoose(BaseGoose):
    def instance(self):
        # UUID = uuid.uuid4().hex
        UUID = GDG_ID
        return GuardianGoose(Name=GG_NM, UUID=UUID)

    def __init__(self, Name, UUID):
        super(__class__, self).__init__(Name, UUID)
        self.parent = None

        self.단위투자금 = 100 * 10000
        self.매수방법 = "03"
        self.매도방법 = "03"
        self.매도유형 = "01"
        self.포트폴리오수 = 6
        self.profitstop = 3.0
        self.losscut = 8.0  # 10.0 -> 8.0으로 조정 (더 빠른 손절)
        self.trailstop = 1.2  # 1.5 -> 1.2로 조정 (더 빠른 트레일링 스탑)
        self.acf파일 = ""
        self.일괄매도시각 = "15:19:10"
        self.일괄매도수익율 = "-3.0~5.0"
        # self.매수거래시간STR = '09:00:00-11:00:00,\n12:00:00-13:00:00,\n14:00:00-15:15:00'
        self.매수거래시간STR = "09:04:25-15:20:00"
        self.매도거래시간STR = "09:01:25-15:20:00"  # 매도로 먼저 털어내고

        self.clock = None
        self.전량매도 = False
        # ✅ 매수거래시간, 매도거래시간 속성을 초기화
        self.매수거래시간 = self.parse_time_range(self.매수거래시간STR)
        self.매도거래시간 = self.parse_time_range(self.매도거래시간STR)

        self.running = False
        self.portfolio = dict()
        self.매수거래중 = False
        self.매도거래중 = False
        self.금일매도종목 = []
        self.주문번호리스트 = set()

        # 추가 필터링 설정
        self.min_score_for_buy = 5.0  # 최소 SCORE 점수
        self.strong_score_threshold = 8.0  # 강한 SCORE 임계값
        self.volume_filter_threshold = 0.3  # 거래량 필터 임계값
        self.price_change_filter = 0.05  # 가격 변동 필터 (5%)

    def modal(self, parent):
        ui = CUIGuardianGoose(parent=parent)
        ui.setModal(True)

        ui.lineEdit_name.setText(self.Name)
        ui.lineEdit_name.setDisabled(True)
        # ui.lineEdit_unit.setText(str(self.단위투자금 // 10000))
        ui.lineEdit_unit.setText(str(self.단위투자금))
        ui.lineEdit_unit.setDisabled(True)
        ui.lineEdit_profitstop.setText(str(self.profitstop))
        ui.lineEdit_losscut.setText(str(self.losscut))
        ui.lineEdit_trailstop.setText(str(self.trailstop))
        ui.lineEdit_portsize.setText(str(self.포트폴리오수))
        ui.comboBox_buy_sHogaGb.setCurrentIndex(
            ui.comboBox_buy_sHogaGb.findText(self.매수방법, flags=Qt.MatchContains)
        )
        ui.comboBox_sell_sHogaGb.setCurrentIndex(
            ui.comboBox_sell_sHogaGb.findText(self.매도방법, flags=Qt.MatchContains)
        )
        ui.comboBox_sell_type.setCurrentIndex(
            ui.comboBox_sell_type.findText(self.매도유형, flags=Qt.MatchContains)
        )
        ui.lineEdit_filename.setText(self.acf파일)
        ui.lineEdit_filename.setDisabled(True)
        ui.plainTextEdit_buytime.setPlainText(self.매수거래시간STR)
        ui.plainTextEdit_selltime.setPlainText(self.매도거래시간STR)
        ui.lineEdit_sellall.setText(self.일괄매도시각)
        ui.lineEdit_sellallprofitstop.setText(str(self.일괄매도수익율))

        r = ui.exec_()
        if r == 1:
            self.Name = ui.lineEdit_name.text().strip()
            # self.단위투자금 = int(ui.lineEdit_unit.text().strip()) * 10000
            self.매수방법 = ui.comboBox_buy_sHogaGb.currentText().strip()[0:2]
            self.매도방법 = ui.comboBox_sell_sHogaGb.currentText().strip()[0:2]
            self.매도유형 = ui.comboBox_sell_type.currentText().strip()[0:2]
            self.포트폴리오수 = int(ui.lineEdit_portsize.text().strip())
            self.acf파일 = ui.lineEdit_filename.text().strip()
            self.profitstop = float(ui.lineEdit_profitstop.text().strip())
            self.losscut = float(ui.lineEdit_losscut.text().strip())
            self.trailstop = float(ui.lineEdit_trailstop.text().strip())
            self.매수거래시간STR = ui.plainTextEdit_buytime.toPlainText().strip()
            self.매도거래시간STR = ui.plainTextEdit_selltime.toPlainText().strip()

            매수거래시간1 = self.매수거래시간STR
            매수거래시간2 = [x.strip() for x in 매수거래시간1.split(",")]

            result = []
            for temp in 매수거래시간2:
                result.append([x.strip() for x in temp.split("-")])

            self.매수거래시간 = result

            매도거래시간1 = self.매도거래시간STR
            매도거래시간2 = [x.strip() for x in 매도거래시간1.split(",")]

            result = []
            for temp in 매도거래시간2:
                result.append([x.strip() for x in temp.split("-")])

            self.매도거래시간 = result

            self.일괄매도시각 = ui.lineEdit_sellall.text().strip()
            self.일괄매도수익율 = str(ui.lineEdit_sellallprofitstop.text().strip())

            print("Name : %s" % (ui.lineEdit_name.text().strip()))
            # print( '단위투자금 : %s' % (int(ui.lineEdit_unit.text().strip()) * 10000))
            print("단위투자금 : %s" % self.단위투자금)
            print(
                "매수방법 : %s" % (ui.comboBox_buy_sHogaGb.currentText().strip()[0:2])
            )
            print(
                "매도방법 : %s" % (ui.comboBox_sell_sHogaGb.currentText().strip()[0:2])
            )
            print("매도유형 : %s" % (ui.comboBox_sell_type.currentText().strip()[0:2]))
            print("포트폴리오수 : %s" % (int(ui.lineEdit_portsize.text().strip())))
            print("acf파일 : %s" % (ui.lineEdit_filename.text().strip()))
            print("profitstop : %s" % (float(ui.lineEdit_profitstop.text().strip())))
            print("losscut : %s" % (float(ui.lineEdit_losscut.text().strip())))
            print("trailstop : %s" % (float(ui.lineEdit_trailstop.text().strip())))
            print(
                "매수거래시간STR : %s"
                % (ui.plainTextEdit_buytime.toPlainText().strip())
            )
            print(
                "매도거래시간STR : %s"
                % (ui.plainTextEdit_selltime.toPlainText().strip())
            )
            print("일괄매도시각 : %s" % (ui.lineEdit_sellall.text().strip()))
            print(
                "일괄매도수익율 : %s"
                % (str(ui.lineEdit_sellallprofitstop.text().strip()))
            )

        return r

    def OnReceiveMessage(self, systemError, messageCode, message):
        클래스이름 = self.__class__.__name__
        try:
            msg_text = message.strip()
            logger.info(
                f"[{클래스이름}] <<<-------== OnReceiveMessageCode: {messageCode}, Msg: {msg_text} (SystemError: {systemError})"
            )

            #  에러 코드 분류 및 64-bit 피드백 전파 (01478: 매도가능수량 부족 등 추가)
            critical_errors = ["01410", "01425", "01156", "03563", "03575", "03588", "01478", "03551", "03444"]
            msg_code_stripped = str(messageCode).strip()

            if (
                msg_code_stripped in critical_errors or systemError != 0
            ) and msg_code_stripped != "-16":
                ticker = "ALL"
                if hasattr(self, "smart_executor") and self.smart_executor.active_tasks:
                    active_tickers = list(self.smart_executor.active_tasks.keys())
                    if active_tickers:
                        ticker = active_tickers[0]

                logger.error(
                    f"❌ [API Reject-Guardian] {ticker} | Code: {msg_code_stripped} | Msg: {msg_text}"
                )

                # [Smart-Exec Control] 치명적 거부 발생 시 즉시 아이스버그 중단 (Spam 방지)
                if ticker != "ALL" and hasattr(self, "smart_executor"):
                    self.smart_executor.cancel_task(ticker, f"REJECTED_{msg_code_stripped}")

                # 64-bit 엔진에 즉시 실패 보고
                # [Zero-Defect] "매수" 문자열 파싱 대신 active_tasks의 실제 side 정보를 참조하여 정합성을 보장합니다.
                inferred_side = "buy"  # Default fallback
                if ticker != "ALL" and hasattr(self, "smart_executor") and ticker in self.smart_executor.active_tasks:
                    task = self.smart_executor.active_tasks[ticker]
                    inferred_side = str(task.get("side", "buy")).lower()
                elif ticker == "ALL" and hasattr(self, "smart_executor"):
                    # 전역 에러인 경우 활성 태스크 중 하나라도 BUY면 buy로 보고하여 락 해제 유도
                    for t_code, t_data in self.smart_executor.active_tasks.items():
                        if str(t_data.get("side", "")).upper() == "BUY":
                            inferred_side = "buy"
                            break
                else:
                    # 최종 Fallback: 여전히 문자열 파이팅 (기존 로직 유지하되 후순위)
                    inferred_side = "buy" if "매수" in msg_text or msg_code_stripped == "01425" else "sell"

                self._publish_order_result(
                    ticker,
                    "FAIL",
                    f"[{msg_code_stripped}] {msg_text}",
                    side=inferred_side,
                )

                if msg_code_stripped == "01425" or msg_code_stripped == "03551":
                    if self.parent:
                        self.parent.주문가능금액부족 = True
            elif msg_code_stripped == "-16":
                logger.warning(
                    f"⚠️ [API Comm-Warning] Code: {msg_code_stripped} | Msg: {msg_text} (BaseGoose retry logic should handle this)"
                )

            elif messageCode == "00039" or messageCode == "00040":  # 매도, 매수완료
                self.parent.주문가능금액부족 = False
                logger.info(f"✅ [Guardian Order Confirmed] {msg_text}")

        except Exception as e:
            logger.exception("[OnReceiveMessage] 예외 발생: %s", str(e))
            ToTelegram(f"[OnReceiveMessage] 예외 발생: {str(e)}")

    # @jit
    def OnReceiveData(self, szTrCode, result):
        try:
            # logger.info(f"[{GG_NM}] <<<-------== OnReceiveData: {szTrCode}\n{result}")

            if szTrCode == "t1857" and self.running:
                식별자, 검색종목수, 포착시간, 실시간키, df = result
                logger.info(
                    f"[{GG_NM}] OnReceiveData [t1857][result 디버그] 식별자: {식별자}, 종목수: {검색종목수}, df:\n{df}"
                )
                if 식별자 == self.XQ_t1857.식별자:
                    # [Performance] iterrows() -> itertuples() 10x faster
                    for row in df[["종목코드", "종목상태"]].itertuples(index=False):
                        code, flag = row
                        # 시작하자마 응답되는 검색겱과는 회피
                        # self.executeAdviseRealData(code, flag)
                        # self.executeUnadviseRealData(code, flag)#flag 'O'인경우만 작동
                        # logger.info(f"OnReceiveData executeAdviseRealData 회피{code, flag} ")
                        # 검색종목테이블에 대기등록.
                        if SRCH_RSLT_BUY_YN == "Y" and not self.parent.주문가능금액부족:
                            self.saveSearchStock(
                                code, 1 if self.Name == "AIGoose" else 2, 0
                            )

            elif szTrCode == "CSPAT00600":
                df, df1 = result
                if not df1.empty:
                    주문번호 = df1["주문번호"].values[0]
                    if 주문번호 != "0":
                        self.주문번호리스트.add(str(주문번호))  # set으로 관리
        except Exception as e:
            logger.exception("[OnReceiveData] 예외 발생: %s", str(e))
            ToTelegram("[OnReceiveData] 예외 발생: %s", str(e))
            # raise

    def OnReceiveSearchRealData(self, szTrCode, lst):
        # self.executeUnadviseRealDatas()
        if SRCH_RSLT_BUY_YN == "Y" and not self.parent.주문가능금액부족:
            self.executeSearchRealData(lst)

    # NOTE: OnReceiveRealData, recv_realdata_worker, process_recv_realdata,
    #       start_advs_realdata_queue_handler, setup_qtimer_for_advs_realdata
    #       moved to BaseGoose (공통함수 통합)

    # ✅ 비동기 구조나 PyQt 외부에서 실행해야 할 경우 유용

    # NOTE: advs_realdata_worker, process_buy_orders, process_sell_orders
    #       moved to BaseGoose (inherited)

    def readyBatchSell(self, dfAccInfo):
        """모든 포트폴리오 종목을 매도 큐에 적재 (일괄 매도/과부하 정리)"""
        logger.info(
            f"[{self.Name}] readyBatchSell 시작 (전량매도 여부: {self.전량매도})"
        )
        if dfAccInfo is None or dfAccInfo.empty:
            logger.warning(f"[{self.Name}] readyBatchSell 중단: 계좌 정보가 비어있음")
            return

        for code, stock_obj in self.portfolio.items():
            try:
                # 1. 이미 매도 큐에 있거나 매도 잠금 상태이면 스킵
                if code in self.금일매도종목 or code in self.lock_controller.get_all(
                    "sell"
                ):
                    continue

                # 2. 계좌 정보에서 현재가 동기화
                col = "종목번호" if "종목번호" in dfAccInfo.columns else "종목코드"
                condition = dfAccInfo[col] == code
                df = dfAccInfo[condition]
                if not df.empty:
                    stock_obj.현재가 = int(df.iloc[0]["현재가"])

                # 3. 매도 큐 적재
                self.batSellQ.put(stock_obj)
                logger.info(
                    f"[{self.Name}] readyBatchSell → batSellQ 추가: {code} ({stock_obj.종목명})"
                )
            except Exception as e:
                logger.error(f"[{self.Name}] readyBatchSell 오류 ({code}): {e}")

    def OnClockTick(self):
        current = dt.datetime.now()
        current_str = current.strftime("%H:%M:%S")

        # print(f"self.매수거래시간 : {self.매수거래시간}, self.매도거래시간 : {self.매도거래시간}")
        거래중 = False
        for t in self.매수거래시간:
            if t[0] <= current_str and current_str <= t[1]:
                거래중 = True
        self.매수거래중 = 거래중
        # print(f"self.매수거래중 :  {self.매수거래중}")
        거래중 = False
        for t in self.매도거래시간:
            if t[0] <= current_str and current_str <= t[1]:
                거래중 = True
        self.매도거래중 = 거래중
        # print(f"self.매도거래중 :  {self.매도거래중}")
        #  dfAccInfo를 루프 밖에서 미리 선언하여 NameError 방지
        try:
            dfAccInfo = self.parent.dialog["계좌정보조회"].dfAccStockInfo
        except Exception as e:
            logger.warning(f"계좌정보 조회 실패(OnClockTick): {e}")
            dfAccInfo = pd.DataFrame()

        if self.일괄매도시각.strip() != "":
            # [Pythonic] PEP 8 준수: == False 대신 not 연산자 사용
            if self.일괄매도시각 <= current_str and not self.전량매도:
                self.전량매도 = True
                self.readyBatchSell(dfAccInfo)

        if not isOverCloseTime() and current.minute % 5 == 0 and current.second == 0:
            #  DB I/O가 포함된 saveMarketInfo를 백그라운드 스레드로 분리하여 GUI 프리징 방지
            threading.Thread(target=self.saveMarketInfo, daemon=True).start()

        #  일괄매도 수익율 파싱을 루프 밖에서 1회만 수행하여 효율성 극대화
        try:
            min_profit_str, max_profit_str = self.일괄매도수익율.split("~", 1)
            min_profit = float(min_profit_str.strip())
            max_profit = float(max_profit_str.strip())
        except Exception as e:
            if current.second % 10 == 0:  # 로그 폭사 방지 (10초에 한 번만 출력)
                logger.warning(
                    f"일괄매도수익율 파싱 오류('{self.일괄매도수익율}'): {e}"
                )
            min_profit = -3.0
            max_profit = 5.0

        #  빈 DataFrame에 .종목번호 접근 시 AttributeError 방지
        if dfAccInfo.empty or (
            "종목번호" not in dfAccInfo.columns and "종목코드" not in dfAccInfo.columns
        ):
            if current.second % 10 == 0:
                #  통신 에러 시 노이즈 절감을 위해 debug 레벨로 하향
                logger.debug(
                    f"[{self.Name}] dfAccInfo가 비어있거나 종목번호/종목코드 컬럼이 없어 수익율 조회 스킵"
                )
        else:
            acc_col = "종목번호" if "종목번호" in dfAccInfo.columns else "종목코드"
            port = self.portfolio
            for k, v in port.items():
                수익율, 현재가 = 0, 0
                try:
                    #  이미 매도 처리 중이거나 락이 걸린 종목은 즉시 스킵 (Queue Flooding 방지)
                    if (
                        v.종목코드 in self.금일매도종목
                        or v.종목코드 in self.lock_controller.get_all("sell")
                    ):
                        continue

                    condition = dfAccInfo[acc_col] == v.종목코드
                    df = dfAccInfo[condition]
                    if not df.empty:
                        수익율 = df.iloc[0]["수익율"]
                        현재가 = df.iloc[0]["현재가"]
                    else:
                        logger.warning(f"{v.종목코드} 종목 정보 없음")
                        continue
                except Exception as e:
                    logger.warning(f"{v.종목코드} 수익율 조회 오류: {e}")
                    continue

                수익율_val = to_numeric_safe(수익율)
                if 수익율_val <= min_profit or 수익율_val >= max_profit:
                    v.현재가 = int(to_numeric_safe(현재가))
                    self.batSellQ.put(v)
                    logger.info(
                        f"🚀 [Profit Threshold Exit] {v.종목명}({v.종목코드}) 수익율 {수익율_val}% 가 범위({min_profit}~{max_profit})를 벗어나 일괄매도 큐에 추가함."
                    )

        if self.batSellQ.qsize() > 0:
            thrd = threading.Thread(target=self.batchSell, daemon=True)
            thrd.start()

    def batchSell(self):
        print(
            "batchSell >>> self %s,  %s, qsize() : %s"
            % (self, self.batSellQ, self.batSellQ.qsize())
        )
        try:
            while self.batSellQ.qsize() > 0:
                item = self.batSellQ.get()
                종목코드 = item.종목코드
                수량 = item.수량
                종목명 = item.종목명
                매수가 = item.매수가
                # 이후 사용되지 않으니 의미에 맞지 않으나 매수후 고가를 현재가를 전달하는 인자로 사용
                # 일괄매도 후 매수후 고가에 byte값이 들어감 int()해야함 의미가 혼돈되어 현재가 생성
                현재가 = item.현재가
                매수일 = item.매수일
                매도가 = "0"
                호가유형코드 = "03"
                self.lock_controller.lock_sell(종목코드)
                if (
                    종목코드 not in self.금일매도종목
                    and 종목코드 not in self.parent.gooses[1].금일매도종목
                ):
                    self.금일매도종목.append(종목코드)

                sell_type = "일괄매도"
                accInfo = self.parent.dialog[
                    "계좌정보조회"
                ]  # 다른 쓰레드에서 쓰고있을 때 lock방지용
                market_info = self.get_market_info(
                    accInfo, "S3_" if 종목코드 in self.kospi_codes else "K3_"
                )
                수익율 = int((현재가 - 매수가) * 수량)
                평가손익 = int((현재가 - 매수가) * 수량)
                주문가 = 0
                logger.info(
                    f"accInfo.dfAccStockInfo : {type(accInfo.dfAccStockInfo)}, {type(종목코드)}"
                )
                _acc_col = (
                    "종목번호"
                    if "종목번호" in accInfo.dfAccStockInfo.columns
                    else "종목코드"
                )
                logger.info(
                    f"accInfo.dfAccStockInfo[{_acc_col}] : {type(accInfo.dfAccStockInfo[_acc_col])}, {type(종목코드)}"
                )
                condition = accInfo.dfAccStockInfo[_acc_col] == 종목코드
                df = accInfo.dfAccStockInfo[condition]
                if not df.empty:
                    수익율 = to_numeric_safe(df.iloc[0]["수익율"])
                    현재가 = to_numeric_safe(df.iloc[0]["현재가"])
                    평가손익 = to_numeric_safe(df.iloc[0]["평가손익"])
                    매수가 = to_numeric_safe(df.iloc[0]["평균단가"])
                    수량 = to_numeric_safe(df.iloc[0]["잔고수량"])
                    주문가 = 현재가

                # [Pardon-Audit] 주문 데이터 패키징 후 query_sell로 전달 (전달 후 기록 보존용)
                _szTrCode = "S3_" if 종목코드 in self.kospi_codes else "K3_"
                order_log_data = {
                    "market_code": _szTrCode,
                    "market_info": market_info,
                    "종목코드": 종목코드,
                    "종목명": 종목명,
                    "거래유형": sell_type,
                    "매수가": 매수가,
                    "매도가": 현재가,
                    "매도수량": 수량,
                    "매수일": str(매수일),
                    "평가손익": 평가손익,
                    "result": None,
                }

                self.query_sell(종목코드, 수량, 매도가, order_log_data=order_log_data)
                self.batSellQ.task_done()
                self.executeUnadviseRealData(종목코드, "O")
                updateSearchStock(종목코드, 11)  # 배치매도
                logger.info(
                    "%s 일괄매도 :  %s, 평가손익: %s" % (GG_NM, 종목명, 평가손익)
                )

        except Exception as e:
            클래스이름 = self.__class__.__name__
            함수이름 = inspect.currentframe().f_code.co_name
            logger.error("%s-%s %s: %s" % (클래스이름, 함수이름, get_linenumber(), e))

    def saveMarketInfo(self):
        buy_type = "시장정보"

        #  계좌정보조회 다이얼로그 안전하게 가져오기 (초기 구동 시 KeyError 방지)
        acc_dialog = (
            self.parent.dialog.get("계좌정보조회")
            if hasattr(self.parent, "dialog")
            else None
        )

        # 계좌 다이얼로그가 준비되었을 때만 DB에 계좌 잔고를 바탕으로 한 기록 저장
        if (
            acc_dialog
            and hasattr(acc_dialog, "dfAccSumInfo")
            and not acc_dialog.dfAccSumInfo.empty
        ):
            market_info = self.get_market_info(acc_dialog, "S3_")
            self.saveOrderHist(
                market_code="S3_",
                market_info=market_info,
                종목코드="",
                종목명="",
                거래유형=buy_type,
                매수가=0,
                매도가=0,
                매도수량=0,
                매수일="",
                평가손익=0,
                result=None,
            )

            market_info = self.get_market_info(acc_dialog, "K3_")
            self.saveOrderHist(
                market_code="K3_",
                market_info=market_info,
                종목코드="",
                종목명="",
                거래유형=buy_type,
                매수가=0,
                매도가=0,
                매도수량=0,
                매수일="",
                평가손익=0,
                result=None,
            )
        else:
            # 초기 구동 시점이라 계좌 정보가 아직 없다면 에러를 내지 않고 DB 저장만 조용히 건너뜀
            if time.time() - self._start_time < 30:
                logger.info(
                    f"[{self.__class__.__name__}] 초기 구동 중: 계좌 데이터 준비 대기 (시장 잔고 DB 기록 건너뜜)"
                )
            else:
                logger.warning(
                    f"[{self.__class__.__name__}] 계좌정보조회 다이얼로그가 준비되지 않았습니다."
                )

        # [Dumb Router] 32bit는 더이상 시장 상황(BUY/SELL/HOLD)을 직접 분석하지 않음.
        # 모든 전략적 판단은 64bit 브레인이 전담하며, 32bit는 원시 데이터 로깅만 수행.
        pass

    def setup_event_handlers(self):
        ##요청이 들어올 때 마다 인스턴스 생성하여 이벤트연결함
        ## 실시간 체결
        ## KOSPI체결
        # self.XR_S3_ = S3_(parent=self)
        # logger.info(f'[{GG_NM}] ==------->>> self.XR_S3_ = S3_(parent=self)')
        ## KOSDAQ체결
        # self.XR_K3_ = K3_(parent=self)
        # logger.info(f'[{GG_NM}] ==------->>> self.XR_K3_ = K3_(parent=self)')
        ## 주식주문체결
        self.XR_SC1 = SC1(parent=self)
        logger.info(f"[{GG_NM}] ==------->>> self.XR_SC1 = SC1(parent=self)")

        # 실시간 주식주문체결 정보수신
        self.XR_SC1.AdviseRealData()
        logger.info(f"[{GG_NM}] ==------->>> self.XR_SC1.AdviseRealData()")

        # self.XR_SC0 = SC0(parent=self)
        # self.XR_SC2 = SC2(parent=self)
        # self.XR_SC3 = SC3(parent=self)
        # self.XR_SC4 = SC4(parent=self)
        # self.XR_SC0.AdviseRealData()
        # self.XR_SC2.AdviseRealData()
        # self.XR_SC3.AdviseRealData()
        # self.XR_SC4.AdviseRealData()

        # 종목검색 AIGoose의 기능은 QTimer를 사용하여 반복작업을 해야함으로 GoldenGoose.py에서 작업.
        self.XQ_t1857 = t1857(parent=self, 식별자=GDG_ID)  # uuid.uuid4().hex)
        self.XQ_t1857.Query(
            실시간구분="1", 종목검색구분="F", 종목검색입력값=self.acf파일
        )

        self.QA_CSPAT00600 = CSPAT00600(parent=self)

    def prepareRun(self):
        # self.cleanup_timer = QTimer()
        # self.cleanup_timer.timeout.connect(self.cleanup_old_entries)
        # self.cleanup_timer.start(300_000)  # 5분
        self.setup_event_handlers()
        # 일괄매도 종목담기용
        self.batSellQ = queue.Queue()

        self.clock = QtCore.QTimer()
        self.clock.timeout.connect(self.OnClockTick)
        self.clock.start(1000)
        self.전량매도 = False
        self.isBatchSellAll = True

        self.계좌번호, self.비밀번호 = self.parent.Account(구분="종합매매")
        # print(  self.계좌번호, self.비밀번호)

        self.lock_controller = LockController(default_timeout_seconds=120)
        self.kospi_codes = []
        self.kosdaq_codes = []

        # [Concurrency & State] Already initialized in BaseGoose.__init__ / objectInit
        # We only need to set tr_handlers here
        #  부모 클래스에서 설정된 H1_ 핸들러 등을 유지하기 위해 update 사용
        self.tr_handlers.update(
            {
                "SC1": self.executeSC1,
                "K3_": self.executeK3_S3_,
                "S3_": self.executeK3_S3_,
            }
        )
        # 등록 가능 판단 기준

        with get_db_connection() as conn:  # with 문을 사용하여 자동으로 conn.close()
            query = (
                GET_STOCK_LIST  # +" AND A.COMBINED_SCORE = 1"#COMBINED_SCORE 1: 상승
            )
            # query = 'select 종목코드,종목명,ETF구분,구분 from 종목코드 WHERE COMBINED_SCORE = 1'# 상승
            self.종목코드테이블 = pdsql.read_sql_query(query, con=conn)
        self.kospi_codes = self.종목코드테이블.query("구분==1")[
            "종목코드"
        ].values.tolist()
        self.kosdaq_codes = self.종목코드테이블.query("구분==2")[
            "종목코드"
        ].values.tolist()

    def Lay(self, flag=True, parent=None):

        logger.info(f"[{self.Name}] Goose.Lay flag: {flag} / parent: {parent}")
        if isBeforeOpenTime() or isOverCloseTime():
            self.금일매도종목 = []
        if self.running == flag:
            return

        self.parent = parent
        self.running = flag

        if flag:
            self.prepareRun()

            일시 = "[{:%Y-%m-%d %H:%M:%S.%f}] =->".format(dt.datetime.now())
            ToTelegram("%s을 시작함!" % (__class__.__name__))

            if (
                getattr(self, "recv_realdata_worker_thread", None)
                and self.recv_realdata_worker_thread.is_alive()
            ):
                logger.info(
                    "recv_realdata_worker_thread already running, skipping start."
                )
            else:
                self.recv_realdata_worker_thread = Thread(
                    target=self.recv_realdata_worker, daemon=True
                )
                self.recv_realdata_worker_thread.start()

            self.start_advs_realdata_queue_handler(use_gui_timer=True)

            # 포트폴리오 실시간 가격정보 요청

            # self.test_OnReceiveRealData()
            thrd = Thread(target=self.executeAdviseRealDataPortFolio, daemon=True)
            thrd.start()

            #  메인 스레드 블로킹 제거: advs_realdata_mng_queue 소비는 백그라운드 워커가 담당.
            # while+time.sleep(0.5)이 메인 스레드(Qt 이벤트 루프)를 차단하면,
            # Qt 이벤트가 필요한 advs_realdata_worker가 큐를 처리 못해 GuardianGoose 시작이 무한 차단됨.
            if len(self.advs_realdata_mng_queue) > 0:
                logger.info(
                    f"[{self.Name}] advs_realdata_mng_queue 잔여: {len(self.advs_realdata_mng_queue)}건 (백그라운드 워커가 처리)"
                )

            # 포트정리 후 거래시작
            # print(f"self.lock_controller.get_all('buy'): {self.lock_controller.get_all('buy')}")
            lst = set(self.portfolio.keys()).union(self.lock_controller.get_all("buy"))
            # 포트폴리오 수 설정 이내에 등록되어 있지 않으면 매수 프로세스 시작
            # 매도요청 후 체결결과 executeSC1에서 실시간가격정보 요청함.
            if len(lst) > self.포트폴리오수:
                try:
                    # parent.dialog[...] 접근은 오직 Run() 안에서만 하도록 유지
                    dfAccInfo = self.parent.dialog["계좌정보조회"].dfAccStockInfo
                except Exception as e:
                    logger.error(f"계좌정보 조회 실패: {e}")
                    dfAccInfo = pd.DataFrame()

                self.readyBatchSell(dfAccInfo)  # ✅ 그냥 호출하면 됨!
            # 시장정보 저정.
            self.saveMarketInfo()
            logger.info(f"self.매수거래시간 : {self.매수거래시간}")
            logger.info(f"self.매수거래중 : {self.매수거래중}")
            logger.info(
                f"self.parent.주문가능금액부족 : {self.parent.주문가능금액부족}"
            )
            logger.info(f"self.금일매도종목 : {self.금일매도종목}")
            logger.info(
                f"self.parent.gooses[0].portfolio.keys() : {self.parent.gooses[0].portfolio.keys()}"
            )
            logger.info(
                f"[{self.__class__.__name__}] get_db_stats() : {get_db_stats()}"
            )
            lst = set(self.portfolio.keys()).union(self.lock_controller.get_all("buy"))
            logger.info(f"lst:{lst} , self.포트폴리오수: {self.포트폴리오수}")

        else:
            self.running = False


def goose_loader():
    # UUID = uuid.uuid4().hex
    UUID = GDG_ID
    goose = GuardianGoose(Name=GG_NM, UUID=UUID)
    return goose
