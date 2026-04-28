# -*- coding: utf-8 -*-

# BaseGoose 사용
import sys
import os
from pathlib import Path


import datetime
import inspect
import io
import subprocess
import threading
import webbrowser

import dill as pickle  # threading 직렬화
import numpy as np
import pandas as pd


from pandas import DataFrame
from PyQt5.QtCore import (
    Qt,
    QTimer,
    pyqtSlot,
    QMutexLocker,
    QTime,
)
from PyQt5.QtGui import (
    QStandardItem,
)
from PyQt5.QtWidgets import (
    QApplication,
    QMessageBox,
    QWidget,
    QLayout,
)

from config.ai_settings import (
    BATCH_PRGS,
    PC_POWER_STAT,
)
from config.comm_settings import DATABASE, ZMQ_PULL_PORT
from config.log_settings import setup_logger
from gooses import AIGoose, BaseGoose, GuardianGoose

from GG_Server.strategy.core.TradingComm import (
    tradingComm_set_logger,
)
from GG_Server.strategy.indicators.market_analysis import analyze_market_conditions
from util.CommUtils import (
    get_db_connection,
    set_commutils_logger,
    isOverCloseTime,
    get_linenumber,
)
from config.telegram_setting import ToTelegram

from util.Utils32 import set_utils32_logger


import logging

# Standardize Logging for extracted modules ---
from managers import plugin_manager, thread_manager, batch_manager
from UI.components import models as ui_models
from UI.components import delegates as ui_delegates
from UI.components import password_dialog as ui_password_dialog
from UI.views import (
    view_account,
    view_analysis,
    view_backup,
    view_configuration,
    view_dialogs,
    view_market_info,
    view_price,
    view_trade_monitor,
)


from xing.XAQuaries import t8430
from xing.XASessions import XASession

# Imports for extracted modules
from managers.plugin_manager import CPluginManager


# 변환된 UI 모듈 import
from UI.views.view_configuration import View_Configuration, View_버전
from UI.views.view_account import View_계좌정보조회
from UI.views.view_backup import View_일별가격정보백업, View_분별가격정보백업
from UI.views.view_market_info import (
    View_업종정보,
    View_업종별종목정보,
    View_테마정보,
    View_종목별투자자,
    View_종목별투자자2,
    View_차트인덱스,
    View_호가창정보,
    View_뉴스,
    View_종목코드조회,
    View_일별업종정보백업,
    View_종목별투자자정보백업,
)
from UI.views.view_price import View_분별주가, View_일별주가
from UI.views.view_analysis import View_종목검색, View_e종목검색
from UI.views.view_trade_monitor import (
    View_주문테스트,
    View_외부신호2LS,
    View_거래결과,
    View_실시간정보,
)

t = threading.Thread()
logger = logging.getLogger(__name__)  # 기본 로거 초기화


def set_main_window_logger(external_logger):
    global logger
    logger = external_logger


set_commutils_logger(logger)
set_utils32_logger(logger)
BaseGoose.set_logger(logger)
AIGoose.set_logger(logger)
GuardianGoose.set_logger(logger)
tradingComm_set_logger(logger)

# Managers
plugin_manager.set_logger(logger)
thread_manager.set_logger(logger)
batch_manager.set_logger(logger)

# UI Components
ui_models.set_logger(logger)
ui_delegates.set_logger(logger)
ui_password_dialog.set_logger(logger)

# UI Views
view_account.set_logger(logger)
view_analysis.set_logger(logger)
view_backup.set_logger(logger)
view_configuration.set_logger(logger)
view_dialogs.set_logger(logger)
view_market_info.set_logger(logger)
view_price.set_logger(logger)
view_trade_monitor.set_logger

# Add project root to sys.path to ensure absolute imports work correctly
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

logger.debug(f"{Path(__file__).name}사용된 모듈: {sys.modules.keys()}")
print("pywin32 라이브러리 정상")

# Redundant classes removed. They are imported from managers.thread_manager
print(f"DEBUG: CPluginManager imported from: {CPluginManager}")


# 메인
# 변환된 UI 모듈 import
# Ui_MainWindow is already defined above via uic.loadUiType


future = []


class MainWindowLogic:
    """
    MainWindow의 비즈니스 로직 및 이벤트 핸들러를 관리하는 Mixin 클래스입니다.
    """

    # Logic methods below

    def start_goose_thread(self, goose):
        self.goose_thread_manager.start(goose)

    @pyqtSlot(object)
    def create_activeX_objects(self, goose):
        with QMutexLocker(self.goose_creation_mutex):
            try:
                logger.info(
                    f"[{self.__class__.__name__}] [create_activeX_objects] 시작 - {goose.Name}"
                )
                goose.Lay(flag=True, parent=self)
                logger.info(
                    f"[{self.__class__.__name__}] [create_activeX_objects] goose.Run() 호출 완료 - {goose.Name}"
                )
            except Exception as e:
                logger.exception(
                    f"[MainWindow.create_activeX_objects] 오류 - {goose.Name}, error: {e}"
                )
                # QMessageBox.critical(self, "오류", f"[MainWindow.create_activeX_objects] 처리 중 오류 발생:\n{str(e)}")
                ToTelegram(
                    f"[MainWindow.create_activeX_objects] 처리 중 오류 발생:\n{str(e)}"
                )

    @pyqtSlot(object)
    def run_in_main_thread(self, func):
        try:
            func()
        except Exception as e:
            logger.exception("run_in_main_thread 실행 중 예외: %s", e)
            # QMessageBox.critical(self, "오류", f"run_in_main_thread 실행 중 오류 발생:\n{str(e)}")
            ToTelegram(f"run_in_main_thread 실행 중 오류 발생:\n{str(e)}")

    def resizeLayout(self):
        # centralwidget이 정상적으로 존재하는지 확인
        self.centralwidget = self.findChild(QWidget, "centralwidget")

        if self.centralwidget:
            logger.debug("centralwidget이 정상적으로 존재합니다.")
            # QGridLayout 찾기
            grid_layout = self.centralwidget.findChild(QLayout, "gridLayout")
            logger.debug("gridLayout 정상적으로 존재합니다.")
            if grid_layout:
                grid_layout.setRowMinimumHeight(0, 100)  # 최소 높이
                grid_layout.setRowStretch(0, 1)  # 0번째 행을 비율
                grid_layout.setRowMinimumHeight(1, 300)  # 최소 높이
                grid_layout.setRowStretch(1, 5)  # 1번째 행 비율
                grid_layout.setRowMinimumHeight(2, 200)  # 최소 높이
                grid_layout.setRowStretch(2, 4)  # 1번째 행 비율
            else:
                logger.debug("centralwidget에 QGridLayout이 없습니다.")
        else:
            logger.debug("centralwidget을 찾을 수 없습니다.")

    def OnQApplicationStarted(self):
        print("DEBUG: [OnQApplicationStarted] Execution began.")
        self.clock = QTimer()
        self.clock.timeout.connect(self.OnClockTick)
        self.clock.start(1000)

        self.LoadGoosesFromDb()
        # self.LoadGoosesFromFile()#gooses_loader에서 생성한 GoldenGoose.gg 원복
        self.GooseView()

        # TODO:자동로그인
        self.MyLogin()

    def OnClockTick(self):
        current = datetime.datetime.now()
        current_str = current.strftime("%H:%M:%S")
        self.statusbar.showMessage(current.strftime("%Y-%m-%d %H:%M:%S"))

        # 09:01:00에 작동되지 않는 경우가 있어 5초동안 작동여부 체크 :기동에 12~3초 정도 소요
        # if current_str[:5] in ['09:01'] and current_str in ['09:01:00','09:01:01','09:01:02','09:01:03','09:01:04']:
        if (
            not self.batchGooses4Timer.hasStartedToday
            and current_str >= "09:01:00"
            and not isOverCloseTime()
        ):
            logger.info(
                f"[{self.__class__.__name__}] Goose작동여부 {current_str, self.batchGooses4Timer.hasStartedToday}"
            )
            logger.info("Goose 자동 START af: %s" % current_str)
            self.GooseRun()
        # 전체Goose 시간차 실행
        elif self.batchGooses4Timer.isStartGooses:
            for goose in self.gooses:
                if not goose.running:
                    strtTime_str = self.batchGooses4Timer.dictGooseRunTime.get(
                        goose.UUID
                    )
                    if strtTime_str and current_str >= strtTime_str:
                        # 이미 스레드가 생성되어 시작 중이거나 실행 중인지 이중 확인
                        if goose.UUID not in self.goose_thread_manager.threads:
                            logger.info(
                                f"[{self.__class__.__name__}] current_str : {current_str}, dict : {self.batchGooses4Timer.dictGooseRunTime} "
                            )
                            try:
                                self.start_goose_thread(goose)
                            except Exception as e:
                                logger.error(
                                    f"start_goose_thread 실행 중 오류 발생: {e}",
                                    exc_info=True,
                                )
                        # self.batchGooses4Timer.gooseRunningCnt +=1
                        # self.batchGooses4Timer.dictGooseRunTime[r.UUID] =''
            self.GooseView()

            # if( len(self.gooses) == int(self.batchGooses4Timer.gooseRunningCnt)):
            #    self.GooseView()
            #    #실행됐으니 초기화
            #    self.batchGooses4Timer.isStartGooses = False
            #    self.batchGooses4Timer.gooseRunTime = dict()
            #    self.batchGooses4Timer.gooseRunningCnt = 0
            #    self.batchGooses4Timer.hasStartedToday = True

        if current.second == 0:  # 매 0초
            try:
                if self.connection is not None:
                    msg = "오프라인"
                    if self.connection.IsConnected():
                        msg = "온라인"

                        # 현재시간 및 미체결 잔고(Reconciliation) 조회
                        self.XQ_t0167.Query()

                        #  Pending Amount Reconciliation
                        # 매 분 0초마다 실제 증권사 미체결 데이터를 가져와 내부 상태 교정
                        if hasattr(self, "XQ_t0425"):
                            acc_no = self.account_dict.get("계좌번호", "")
                            acc_pwd = self.account_dict.get("비밀번호", "")
                            if acc_no and acc_pwd:
                                self.XQ_t0425.Query(acc_no, acc_pwd)
                    else:
                        msg = "오프라인"
                    self.statusbar.showMessage(msg)
            except Exception as e:
                클래스이름 = self.__class__.__name__
                함수이름 = inspect.currentframe().f_code.co_name
                logger.error(
                    "%s-%s %s: %s" % (클래스이름, 함수이름, get_linenumber(), e)
                )
                pass
            if current_str == "00:00:00":
                logger.info("자정이 되어 hasStartedToday 플래그를 초기화합니다.")
                self.batchGooses4Timer.hasStartedToday = False

            # if current_str in ['07:00:00']:
            #    logger.info( "종료전 저장")
            #    self.SaveGoosesToDb()
            #    self.조용히종료 = True
            #    self.close()

            # TODO: 자동로그인
            # if current_str in ['08:50:00']:
            #     self.MyLogin()
            # logger.info( "OnClockTick : %s" % current_str)

            # if current_str in ['15:30:10']:
            #    logger.info( "장종료후 저장 START")
            #    self.SaveGoosesToDb()
            #    logger.info( "장종료후 저장 END")
            #    self.GooseView()
            #    #self.close()

        # 장오픈 시간대
        _TIME = ["%02d" % i for i in range(8, 17)]

        if (
            current_str[:2] in _TIME
            and current_str[3:] in ["30:00"]
            and not isOverCloseTime()
        ):
            #  GUI 블로킹 방지를 위해 메시지 생성 및 전송 로직 전체를 스레드로 분리
            thrd = threading.Thread(
                target=self.SendIntradayTelegramUpdate, args=(current_str,), daemon=True
            )
            thrd.start()

        if current.minute % 10 == 0:  # 매 10 분
            pass

    def SendIntradayTelegramUpdate(self, current_str):
        """매 30분 시장 상황 및 포트폴리오 정보를 생성하여 Telegram으로 전송 (비동기)"""
        try:
            sendMsg = ""

            def get_shared_market_status(market_tp):
                """엔진(IT)이 기록한 시장 상태를 DB에서 읽어옴 (SSOT)"""
                m_tp_str = "KOSPI" if market_tp == 1 else "KOSDAQ"
                try:
                    with get_db_connection() as conn:
                        df = pd.read_sql_query(
                            "SELECT REGIME, INDEX_CHANGE, ADR_RATIO, LAST_UPDATED FROM TB_MARKET_STATUS WHERE MARKET_TYPE = ?",
                            conn,
                            params=(m_tp_str,),
                        )
                        if not df.empty:
                            row = df.iloc[0]
                            last_updated = row["LAST_UPDATED"]

                            #  데이터 신선도 체크 (30분 이상 지연 시 경고 부착)
                            stale_prefix = ""
                            try:
                                dt_updated = datetime.datetime.strptime(
                                    last_updated, "%Y-%m-%d %H:%M:%S"
                                )
                                if (
                                    datetime.datetime.now() - dt_updated
                                ).total_seconds() > 1800:
                                    stale_prefix = "⚠️[DATA_STALE] "
                            except Exception:
                                pass

                            return {
                                "market_regime": row["REGIME"],
                                "current_index_change": row["INDEX_CHANGE"],
                                "adr_ratio": row["ADR_RATIO"],
                                "stale_prefix": stale_prefix,
                            }
                except Exception:
                    pass
                # DB 조회 실패 시 직접 분석 (Fallback)
                return analyze_market_conditions(market_tp)

            # 1. KOSPI 분석
            m_kospi = get_shared_market_status(1)
            regime_kospi = m_kospi.get("market_regime", "NEUTRAL")
            change_kospi = m_kospi.get("current_index_change", 0.0)
            adr_kospi = m_kospi.get("adr_ratio", 1.0)
            prefix_kospi = m_kospi.get("stale_prefix", "")

            sendMsg += f"\n📊 {prefix_kospi}KOSPI [{regime_kospi}] ({change_kospi:+.2f}%, ADR:{adr_kospi:.2f}) : "
            #  buy_condition/sell_condition 판단 로직 동기화
            if regime_kospi in ["BULL", "NEUTRAL"]:
                sendMsg += "BUY"
            elif regime_kospi in ["BEAR", "CRASH"]:
                sendMsg += "SELL"
            else:
                sendMsg += "HOLD"

            # 2. KOSDAQ 분석
            m_kosdaq = get_shared_market_status(2)
            regime_kosdaq = m_kosdaq.get("market_regime", "NEUTRAL")
            change_kosdaq = m_kosdaq.get("current_index_change", 0.0)
            adr_kosdaq = m_kosdaq.get("adr_ratio", 1.0)
            prefix_kosdaq = m_kosdaq.get("stale_prefix", "")

            sendMsg += f"\n📊 {prefix_kosdaq}KOSDAQ [{regime_kosdaq}] ({change_kosdaq:+.2f}%, ADR:{adr_kosdaq:.2f}) : "
            if regime_kosdaq in ["BULL", "NEUTRAL"]:
                sendMsg += "BUY"
            elif regime_kosdaq in ["BEAR", "CRASH"]:
                sendMsg += "SELL"
            else:
                sendMsg += "HOLD"

            # 3. 포트폴리오 종목 정보 수집
            for r in self.gooses:
                if len(r.portfolio.items()) > 0:
                    result = []
                    for p, v in r.portfolio.items():
                        # GUI 객체(dialog) 접근 시 주의 필요하지만 read-only DataFrame이므로 유지
                        condition = (
                            self.dialog["계좌정보조회"].dfAccStockInfo.종목번호
                            == v.종목코드
                        )
                        df = self.dialog["계좌정보조회"].dfAccStockInfo[condition]
                        try:
                            현재가 = df.iloc[0]["현재가"]
                            수익율 = df.iloc[0]["수익율"]
                        except Exception:
                            현재가, 수익율 = 0, 0

                        result.append(
                            (
                                v.종목명.strip(),
                                self.format_number(v.매수가),
                                self.format_number(v.수량),
                                self.format_number(현재가),
                                self.format_float(수익율),
                            )
                        )

                    accDf = DataFrame(
                        data=result,
                        columns=["종목명", "매수가", "수량", "현재가", "수익율"],
                    )
                    sendMsg += (
                        f"\n\n{current_str} : [LS API {r.Name}]\n{accDf.to_string()}"
                    )

            # 4. 계좌 합계 정보 수집
            if hasattr(self, "dialog") and "계좌정보조회" in self.dialog:
                acc_sum_df = self.dialog["계좌정보조회"].dfAccSumInfo
                if not acc_sum_df.empty:
                    stockList = []
                    for data in acc_sum_df.iloc[0]:
                        if isinstance(data, (np.int64, int)):
                            data = self.format_number(data)
                        stockList.append(data)
                    stockDf = DataFrame(data=[stockList], columns=self.account_columns)
                    sendMsg += "\n\n" + stockDf.to_string()

            # 5. 전송
            if sendMsg:
                ToTelegram(sendMsg)
                logger.info(
                    f"[{self.__class__.__name__}] 30분 단위 Telegram 업데이트 전송 완료"
                )

        except Exception as e:
            logger.error(f"SendIntradayTelegramUpdate Error: {e}", exc_info=True)

    def closeEvent(self, event):
        """
        GoldenGoose 완전 종료 루틴 (재귀 없음, 안정형)
        """

        # 조용히 종료 옵션
        if not self.조용히종료:
            result = QMessageBox.Yes
            if result != QMessageBox.Yes:
                event.ignore()
                return

        # 1. Save data (Non-blocking failure)
        try:
            self.SaveGoosesToDb()
        except Exception as e:
            logger.error(f"SaveGoosesToDb failed: {e}")
            # Do not return here, continue to stop gooses
        # 2. Stop Gooses (Critical cleanup)
        try:
            if self.gooses:
                for g in self.gooses:
                    try:
                        self.stop_goose(g)
                    except Exception as e:
                        logger.error(f"Failed to stop some goose: {e}")
        except Exception as e:
            logger.error(f"Error iterating gooses: {e}")

        # ActiveX 정리
        try:
            self.connection.logout()
            self.connection.disconnect()
        except Exception:
            pass
        # === 2) 서버 및 ZMQ 종료 ===
        try:
            if hasattr(self, "zmq_receiver") and self.zmq_receiver:
                self.zmq_receiver.stop()
                logger.info("[MainWindow] ZMQ Receiver thread stopped.")
        except Exception as e:
            logger.error(f"Error stopping ZMQ receiver thread: {e}")

        try:
            if hasattr(self, "zmq_pull") and self.zmq_pull:
                self.zmq_pull.close()
                logger.info("[MainWindow] Legacy ZMQ Pull listener closed.")
        except Exception as e:
            logger.error(f"Error closing ZMQ pull listener: {e}")

        try:
            from util.zmq_manager import cleanup_all_zmq

            cleanup_all_zmq()
        except Exception as e:
            logger.error(f"Error calling cleanup_all_zmq: {e}")

        try:
            self.stop_server()
        except Exception:
            pass

        try:
            self.clock.stop()
        except Exception:
            pass

        # === 3) Qt 이벤트 루프 종료 ===
        try:
            QApplication.processEvents()
            QApplication.instance().quit()
        except Exception:
            pass

        # === 4) OS 종료 명령 ===
        if self.code_close_request and PC_POWER_STAT == 0:
            subprocess.Popen(["shutdown", "/s", "/f", "/t", "0"])

        logger.info("GoldenGoose 완전 종료됨.")
        event.accept()

        # if self.조용히종료 == True:
        #    event.accept()
        # else:
        #    # result = QMessageBox.question(self,"프로그램 종료","정말 종료하시겠습니까 ?", QMessageBox.Yes| QMessageBox.No)
        #    # 안내팝업창 없이 종료
        #    result = QMessageBox.Yes

    #
    #    if result == QMessageBox.Yes:
    #        event.accept()
    #        self.clock.stop()
    #    else:
    #        event.ignore()

    def beforeCloseEvent(self):

        try:
            logger.info(
                f"[{self.__class__.__name__}] ==---->>> closeEvent 뉴스삭제 <<<-------=="
            )
            self.gooses[0].뉴스삭제()
            logger.info(
                f"[{self.__class__.__name__}] ==---->>> closeEvent 거래주문내역삭제 <<<-------=="
            )
            self.gooses[0].거래주문내역삭제()
            logger.info(
                f"[{self.__class__.__name__}] ==---->>> closeEvent 분별작업정보삭제 <<<-------=="
            )
            self.gooses[0].분별작업정보삭제()
            logger.info(
                f"[{self.__class__.__name__}] ==---->>> closeEvent 환율가격정보삭제 <<<-------=="
            )
            self.gooses[0].환율가격정보삭제()
            # logger.info(
            #     f"[{self.__class__.__name__}] ==---->>> closeEvent AIGoose 포트폴리오 반 남기고 GuardianGoose로 이동 <<<-------=="
            # )
            # self.gooses[0].moveHalfOverAIToGuardian(self.gooses[1])
            logger.info(
                f"[{self.__class__.__name__}] ==---->>> closeEvent Goose DB정보 3개만 남기고 삭제 <<<-------=="
            )
            self.gooses[0].deleteGooseSaveHist()
        except Exception as e:
            print("Goose 종료 정리 오류:", e)

        # Logger 핸들러 종료
        try:
            logger.info("logger close")
            for handler in logger.handlers[:]:
                logger.removeHandler(handler)
                handler.close()
        except Exception:
            pass

        try:
            ToTelegram("GoldenGoose가 종료됨!")
        except Exception:
            pass
        # 종료 준비 완료 시그널 emit
        self.beforeCloseFinished.emit()
        # 🔥 여기서 절대 self.shutdown(), self.close() 호출 금지

    def run_file(self, file_path: str):

        # CREATE_NEW_CONSOLE (0x00000010): 새 창 생성 및 포커스
        CREATE_NEW_CONSOLE = 0x00000010
        CREATE_NO_WINDOW = 0x08000000

        try:
            logger.info(f"🌑 [백그라운드 배치] {file_path} 실행 중...")
            working_dir = os.path.dirname(file_path)
            path_lower = file_path.lower()
            # 1. 경로 존재 확인
            if not os.path.exists(file_path):
                logger.error(f"파일을 찾을 수 없습니다: {file_path}")
                return

            # 2. 실행 명령 구성 (확장자에 따른 분기)
            path_lower = file_path.lower()

            # 백그라운드 실행 시에는 cmd /c 를 통하지 않고 직접 실행하는 것이 더 안정적입니다.
            if path_lower.endswith(".py"):
                cmd_list = [sys.executable, file_path]
            elif path_lower.endswith(".exe"):
                cmd_list = [file_path]
            else:
                logger.warning(f"지원하지 않는 확장자입니다: {file_path}")
                return

            # 3. 동기식 실행
            # 파일이 위치한 디렉토리 추출
            with subprocess.Popen(
                cmd_list,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                cwd=working_dir,
                creationflags=subprocess.CREATE_NEW_CONSOLE,
            ) as proc:
                # 실시간 로그 스트리밍
                for line in proc.stdout:
                    # 필터링 로직: 필요한 정보만 로그에 남김
                    if any(
                        keyword in line
                        for keyword in [" E ", "Final Grade", "CRITICAL"]
                    ):
                        logger.info(f"[Batch] {line.strip()}")

                    # (선택 사항) 진행 상황 시각화 - 100단위로 점 찍기 등
                    # if "processing" in line: counter += 1 ...

                # 프로세스가 끝날 때까지 대기 (필요시 timeout 설정 가능)
                proc.wait(timeout=700)

            # 4. 결과 처리 (with 블록 밖에서 진행)
            if proc.returncode == 0:
                logger.info(f"[{file_path}] 프로그램이 정상 종료되었습니다.")

                if file_path == BATCH_PRGS[0]:  # StockPicking
                    self.일별가격정보백업완료 = True
                elif file_path == BATCH_PRGS[1]:  # PreIntradasyTrrading
                    self.request_close()

                logger.info(
                    f"내일 장을 준비하는 분별가격정보 정보 백업이 시작 되었습니다."
                )
            else:
                logger.warning(
                    f"[{file_path}] 프로그램이 오류 코드({proc.returncode})와 함께 종료되었습니다."
                )

        except subprocess.CalledProcessError as e:
            # 에러 발생 시 자식이 남긴 마지막 메시지 확인 가능
            logger.error(f"❌ 에러 발생 (코드 {e.returncode}): {e.output}")
        except subprocess.TimeoutExpired:
            logger.error(f"[{file_path}] 실행 시간 초과(약 11.6분)")
        except Exception as e:
            logger.error(f"[{file_path}] 실행 중 예외 발생: {e}")

    def request_close(self):
        # beforeCloseEvent 실행 후 완료되면 close() 호출
        self.beforeCloseEvent()  # 동기 호출

    def _do_close(self):
        self.code_close_request = True
        self.close()  # 이제 완전 종료

    def shutdown(self):
        pass
        # try:
        #    self.stop_server()
        # except Exception:
        #    pass

    def SaveGoosesToFile(self):
        for r in self.gooses:
            r.포트폴리오쓰기()
            # r.Lay(flag=False, parent=None)

        with open("GoldenGoose.gg", "wb") as handle:
            pickle.dump(self.gooses, handle, protocol=pickle.HIGHEST_PROTOCOL)
        # try:
        #     with open('GoldenGoose.gg', 'wb') as handle:
        #         pickle.dump(self.gooses, handle, protocol=pickle.HIGHEST_PROTOCOL)
        # except Exception as e:
        #     logger.info(  e)
        # finally:
        #     for r in self.gooses:
        #         r.Lay(flag=False, parent=self)

        # for r in self.gooses:
        #     r.Lay(flag=False, parent=self)

    def LoadGoosesFromFile(self):
        with open("GoldenGoose.gg", "rb") as handle:
            try:
                self.gooses = pickle.load(handle)
            except Exception as e:
                클래스이름 = self.__class__.__name__
                함수이름 = inspect.currentframe().f_code.co_name
                logger.error(
                    "%s-%s %s: %s" % (클래스이름, 함수이름, get_linenumber(), e)
                )
            finally:
                pass

        for r in self.gooses:
            r.포트폴리오읽기()
            r.Lay(flag=False, parent=None)

    def create_table(self):
        """DB 테이블 생성"""
        with get_db_connection() as conn:  # with 문을 사용하여 자동으로 conn.close()
            cursor = conn.cursor()
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS TB_GOOSE (
                    ID INTEGER PRIMARY KEY AUTOINCREMENT,
                    DATA BLOB NOT NULL
                )
            """
            )
            cursor.close()

    def stop_goose(self, goose):

        goose.Lay(flag=False, parent=None)
        if hasattr(goose, "advs_realdata_objs"):
            goose.unregister_all()
        self.goose_thread_manager.stop(goose.UUID)
        goose.queryInit()

        #  데이터 숙취 방지 (Data Hangover Flush)
        # 스레드가 완전히 멈춘 후, 다음 실행을 위해 큐에 남은 과거 쓰레기 데이터를 모두 비워줍니다.

        if hasattr(goose, "recv_realdata_queue"):
            goose.recv_realdata_queue.clear()

        if hasattr(goose, "advs_realdata_mng_queue"):
            goose.advs_realdata_mng_queue.clear()

        if hasattr(goose, "advs_realdata_mng_set"):
            goose.advs_realdata_mng_set["register"].clear()
            goose.advs_realdata_mng_set["unregister"].clear()

        goose.objectInit()
        self.batchGooses4Timer.hasStartedToday = True  # 수동 종료 시 자동 재시작 방지
        self.batchGooses4Timer.isStartGooses = False
        self.batchGooses4Timer.dictGooseRunTime = dict()
        self.batchGooses4Timer.gooseRunningCnt = 0

    def SaveGoosesToDb(self):
        """객체 리스트를 SQLite 테이블에 JSON 형태로 저장"""
        # self.create_table()
        goose_data = []
        logger.info(
            f"[{self.__class__.__name__}] SaveGoosesToDb self.gooses : {self.gooses}"
        )
        if self.gooses is None or len(self.gooses) == 0:
            return

        for r in self.gooses:
            r.포트폴리오쓰기()
            # r.Lay(flag=False, parent=None)

        # self.gooses 객체를 Pickle로 변환하여 BLOB 컬럼에 저장
        blob_data = pickle.dumps(self.gooses, protocol=pickle.HIGHEST_PROTOCOL)

        with get_db_connection() as conn:  # with 문을 사용하여 자동으로 conn.close()
            cursor = conn.cursor()
            cursor.execute("INSERT INTO TB_GOOSE (DATA) VALUES (?)", (blob_data,))
            cursor.close()

        # for r in self.gooses:
        #     r.Lay(flag=False, parent=self)

    class CustomUnpickler(pickle.Unpickler):
        def find_class(self, module, name):
            # AIGoose 모듈과 GuardianGoose 모듈이 EXE 환경에서 제대로 로드되도록 처리
            if module == "AIGoose":
                import gooses.AIGoose as AIGoose

                return getattr(AIGoose, name)
            elif module == "GuardianGoose":
                import gooses.GuardianGoose as GuardianGoose

                return getattr(GuardianGoose, name)
            return super().find_class(module, name)

    def LoadGoosesFromDb(self):
        """SQLite에서 JSON 데이터를 읽어와 객체 리스트로 변환"""
        with get_db_connection() as conn:  # with 문을 사용하여 자동으로 conn.close()
            cursor = conn.cursor()

            try:
                cursor.execute("SELECT DATA FROM TB_GOOSE ORDER BY ID DESC LIMIT 1")
                row = cursor.fetchone()
                # print(f"row[0] ; {row[0]}")
                if row:
                    logger.info(
                        f"[{self.__class__.__name__}] LoadGoosesFromDb pickle.loads(row[0]) : {pickle.loads(row[0])}"
                    )
                    # self.gooses = pickle.loads(row[0])  # BLOB 데이터를 다시 객체 리스트로 변환
                    # CustomUnpickler를 사용하여 pickle 데이터를 로드
                    self.gooses = self.CustomUnpickler(io.BytesIO(row[0])).load()
            except Exception as e:
                클래스이름 = self.__class__.__name__
                함수이름 = inspect.currentframe().f_code.co_name
                logger.error(
                    "%s-%s %s: %s" % (클래스이름, 함수이름, get_linenumber(), e)
                )
            finally:
                cursor.close()

        for r in self.gooses:
            r.포트폴리오읽기()
            r.Lay(flag=False, parent=None)

    def GetGoosesFromDb(self):
        """SQLite에서 JSON 데이터를 읽어와 객체 리스트로 변환"""
        with get_db_connection() as conn:  # with 문을 사용하여 자동으로 conn.close()
            cursor = conn.cursor()

            try:
                cursor.execute("SELECT DATA FROM TB_GOOSE ORDER BY ID DESC LIMIT 1")
                row = cursor.fetchone()
                if row:
                    # logger.info(f"[{self.__class__.__name__}]  GetGoosesFromDb row[0] : {row[0]}")
                    # gooses = pickle.loads(row[0])  # BLOB 데이터를 다시 객체 리스트로 변환
                    # CustomUnpickler를 사용하여 pickle 데이터를 로드
                    gooses = self.CustomUnpickler(io.BytesIO(row[0])).load()
                    logger.info(
                        f"[{self.__class__.__name__}] GetGoosesFromDb gooses : {gooses}"
                    )
                    return gooses
            except Exception as e:
                클래스이름 = self.__class__.__name__
                함수이름 = inspect.currentframe().f_code.co_name
                logger.error(
                    "%s-%s %s: %s" % (클래스이름, 함수이름, get_linenumber(), e)
                )
            finally:
                cursor.close()

    def goose_selected(self, QModelIndex):
        Goose타입 = self.goose_model._data[QModelIndex.row() : QModelIndex.row() + 1][
            "Goose타입"
        ].values[0]

        uuid = self.goose_model._data[QModelIndex.row() : QModelIndex.row() + 1][
            "GooseID"
        ].values[0]
        portfolio = None
        for r in self.gooses:
            if r.UUID == uuid:
                self.viewPorttfolio(r)
                break

    def goose_double_clicked(self, QModelIndex):
        self.GooseEdit(QModelIndex)
        self.GooseView()

    def portfolio1_selected(self, QModelIndex):
        pass

    def portfolio1_double_clicked(self, QModelIndex):
        return
        # 첫 번째 행 선택
        # 첫 번째 행(0번 행)의 첫 번째 셀(0, 0) 선택 효과 적용
        index = self.goose_model.index(0, 0)  # 첫 번째 행, 첫 번째 열의 QModelIndex
        self.tableView_goose.setCurrentIndex(index)  # 현재 선택된 셀 설정
        self.tableView_goose.selectRow(0)
        self.GooseCurrentIndex(index)
        GooseUUID = self.goose_model._data[
            self.goose_modeltableView_goose_current_index.row() : self.goose_modeltableView_goose_current_index.row()
            + 1
        ]["GooseID"].values[0]
        # stock_cd = self.portfolio_model1._data[self.tableView_portfolio1_current_index.row():self.tableView_portfolio1_current_index.row() + 1]['TAG'].values[0]

        for r in self.gooses:
            if r.UUID == GooseUUID:
                index = self.tableView_portfolio1_current_index.row()
                stock_cd = self.portfolio_model1.item(
                    index, 0
                ).text()  # 0은 '종목코드' 열의 인덱스입니다.

                portfolio_keys = list(r.portfolio.keys())
                for k in portfolio_keys:
                    if k == stock_cd:
                        v = r.portfolio[k]
                        result = QMessageBox.question(
                            self,
                            "포트폴리오 종목 삭제",
                            "[%s-%s] 을/를 삭제 하시겠습니까 ?"
                            % (v.종목코드, v.종목명),
                            QMessageBox.Yes | QMessageBox.No,
                        )
                        if result == QMessageBox.Yes:
                            r.금일매도종목.append(v.종목코드)
                            r.portfolio.pop(stock_cd)

                        self.PortfolioView()
                        # self.GooseView()

    def portfolio2_selected(self, QModelIndex):
        pass

    def portfolio2_double_clicked(self, QModelIndex):
        return
        # 첫 번째 행 선택
        # 첫 번째 행(0번 행)의 첫 번째 셀(0, 0) 선택 효과 적용
        index = self.goose_model.index(1, 0)  # 첫 번째 행, 첫 번째 열의 QModelIndex
        self.tableView_goose.setCurrentIndex(index)  # 현재 선택된 셀 설정
        self.tableView_goose.selectRow(0)
        self.GooseCurrentIndex(index)

        GooseUUID = self.goose_model._data[
            self.goose_modeltableView_goose_current_index.row() : self.goose_modeltableView_goose_current_index.row()
            + 1
        ]["GooseID"].values[0]
        # stock_cd = self.portfolio_model1._data[self.tableView_portfolio1_current_index.row():self.tableView_portfolio1_current_index.row() + 1]['TAG'].values[0]

        for r in self.gooses:
            if r.UUID == GooseUUID:
                index = self.tableView_portfolio2_current_index.row()
                stock_cd = self.portfolio_model2.item(
                    index, 0
                ).text()  # 0은 '종목코드' 열의 인덱스입니다.

                portfolio_keys = list(r.portfolio.keys())
                for k in portfolio_keys:
                    if k == stock_cd:
                        v = r.portfolio[k]
                        result = QMessageBox.question(
                            self,
                            "포트폴리오 종목 삭제",
                            "[%s-%s] 을/를 삭제 하시겠습니까 ?"
                            % (v.종목코드, v.종목명),
                            QMessageBox.Yes | QMessageBox.No,
                        )
                        if result == QMessageBox.Yes:
                            r.금일매도종목.append(v.종목코드)
                            r.portfolio.pop(stock_cd)

                        self.PortfolioView()
                        # self.GooseView()

    def GooseCurrentIndex(self, index):
        self.goose_modeltableView_goose_current_index = index

    def GooseRun(self):

        # 검색결과 수신을 위한 시간차를 두고 실행
        # 3초후에 AIGoose시작 그 후 10초텸. (0.00166..)
        aigooseTerm = 1 / 60 * 7
        guardianGooseTerm = 1 / 60 * 5
        current = datetime.datetime.now() + datetime.timedelta(
            minutes=float(aigooseTerm)
        )
        current_str = current.strftime("%H:%M:%S")

        for r in self.gooses:
            if r.running:
                continue
            r.Lay(flag=False, parent=self)
            logger.info(
                f"[{self.__class__.__name__}] Goose %s 자동 START time : %s"
                % (r.Name, current_str)
            )
            self.batchGooses4Timer.dictGooseRunTime[r.UUID] = current_str

            next = datetime.datetime.now() + datetime.timedelta(
                minutes=float(aigooseTerm) + float(guardianGooseTerm)
            )
            current_str = next.strftime("%H:%M:%S")

        self.batchGooses4Timer.isStartGooses = True
        self.batchGooses4Timer.hasStartedToday = True

    def GooseView(self):
        result = []
        for r in self.gooses:
            result.append(r.getstatus())

        self.goose_model.update(DataFrame(data=result, columns=self.goose_columns))

        # GooseID 숨김
        self.tableView_goose.setColumnHidden(2, True)
        # Goose타이1 숨김
        self.tableView_goose.setColumnHidden(0, True)

        for i in range(len(self.goose_columns)):
            self.tableView_goose.resizeColumnToContents(i)

    def GooseEdit(self, QModelIndex):
        Goose타입 = self.goose_model._data[QModelIndex.row() : QModelIndex.row() + 1][
            "Goose타입"
        ].values[0]
        GooseUUID = self.goose_model._data[QModelIndex.row() : QModelIndex.row() + 1][
            "GooseID"
        ].values[0]
        print("GooseEdit Goose타입 %s" % Goose타입)
        print("GooseEdit GooseUUID %s" % GooseUUID)
        for r in self.gooses:
            print("GooseEdit r %s r.UUID %s" % (r, r.UUID))
            if r.UUID == GooseUUID:
                result = r.modal(parent=self)
                if result:
                    isRunning = r.running
                    print(f"modal result {result}")
                    self.SaveGoosesToDb()
                    if isRunning:
                        # self.gooseExecutor.shutdown(wait=True)  # 현재 작업이 모두 끝날 때까지 기다림
                        # self.gooseExecutor = ThreadPoolExecutor(max_workers=2)  # 새 풀 생성
                        for r in self.gooses:
                            # 저장하면 전체 종료되어 재실행 필요.
                            # r.Lay(flag=True, parent=self)
                            # self.gooseExecutor.submit(r.Lay(flag=True, parent=self))
                            # Run을 직접 실행하면 안 되고, 함수 참조와 인자 전달을 분리해야 함!
                            # self.gooseExecutor.submit(r.Run, flag=True, parent=self)
                            self.start_goose_thread(r)
                        self.GooseView()

    # 숫자를 3자리마다 쉼표로 포맷하고 오른쪽 정렬하는 함수
    def format_number(self, value):
        """숫자를 3자리마다 쉼표로 포맷하고 문자열로 반환합니다."""
        return f"{value:,.0f}"

    def AccountView(self):
        try:
            formatted_data = []  # 포맷팅된 데이터
            for data in self.dialog["계좌정보조회"].dfAccSumInfo.iloc[0]:
                if isinstance(data, (int, float, np.integer, np.floating)):
                    try:
                        data = self.format_number(
                            data
                        )  # 소수점 없이 천 단위로 쉼표 추가
                    except Exception:
                        data = str(data)
                else:
                    data = str(data)
                formatted_data.append(data)

            self.account_model.setHorizontalHeaderLabels(self.account_columns)
            ## 모델에 데이터 추가
            for column in range(len(formatted_data)):
                item = QStandardItem(formatted_data[column])
                item.setEditable(False)  # 개별 항목을 읽기 전용으로 설정
                item.setTextAlignment(Qt.AlignRight)  # 오른쪽 정렬
                self.account_model.setItem(0, column, item)

            self.tableView_account.setColumnHidden(5, True)

            for i in range(len(self.account_columns)):
                self.tableView_account.resizeColumnToContents(i)

        except Exception as e:
            if logger:
                logger.error(f"Error occurred in AccountView: {e}")
            else:
                print(f"Error occurred in AccountView: {e}")

    def PortfolioView(self):
        try:
            for r in self.gooses:
                self.viewPorttfolio(r)

        except Exception as e:
            print(f"Error occurred: {e}")

    def format_float(self, value):
        return "{:,.02f}".format(value)

    def viewPorttfolio(self, r):
        try:
            portfolio = r.portfolio

            if r.UUID.strip() == self.gooses[0].UUID.strip():
                pfModel = self.portfolio_model1
                tvPortfolio = self.tableView_portfolio1
            elif r.UUID.strip() == self.gooses[1].UUID.strip():
                pfModel = self.portfolio_model2
                tvPortfolio = self.tableView_portfolio2

            # 기존 데이터 모두 삭제
            pfModel.clear()
            pfModel.setHorizontalHeaderLabels(self.portfolio_columns)

            for idx, (p, v) in enumerate(portfolio.items()):
                매수일 = "%s" % v.매수일
                result = (
                    v.종목코드,
                    v.종목명.strip(),
                    self.format_number(v.매수가),
                    self.format_number(v.수량),
                    self.format_number(v.매입금액),
                    self.format_number(v.현재가),
                    self.format_number(v.평가금액),
                    self.format_number(v.평가손익),
                    self.format_float(v.수익율),
                    매수일[:19],
                )

                for column, value in enumerate(result):
                    item = QStandardItem(str(value))  # 각 값을 문자열로 변환
                    item.setEditable(False)  # 개별 항목을 읽기 전용으로 설정
                    if column not in (0, 1, 9):
                        item.setTextAlignment(Qt.AlignRight)  # 오른쪽 정렬
                    pfModel.setItem(idx, column, item)

            for i in range(len(self.portfolio_columns)):
                tvPortfolio.resizeColumnToContents(i)

        except Exception as e:
            print(f"Error occurred: {e}")

    def Portfolio1CurrentIndex(self, index):
        self.tableView_portfolio1_current_index = index

    def Portfolio2CurrentIndex(self, index):
        self.tableView_portfolio2_current_index = index

    def Account(self, 구분):
        if self.account_dict is not None:
            계좌번호 = self.account_dict["계좌번호"]
            거래비밀번호 = self.account_dict["거래비밀번호"]

        return (계좌번호, 거래비밀번호)

    def MyLogin(self):
        if self.account_dict and "계좌번호" in self.account_dict:
            if self.connection is None:
                self.connection = XASession(parent=self)

            self.계좌번호 = self.account_dict.get("계좌번호", "").strip()
            self.id = self.account_dict.get("사용자ID", "").strip()
            self.pwd = self.account_dict.get("사용자비밀번호", "").strip()
            self.cert = self.account_dict.get("인증서비밀번호", "").strip()
            self.거래비밀번호 = self.account_dict.get("거래비밀번호", "").strip()
            self.url = self.account_dict.get("접속URL", "").strip()
            self.connection.login(
                url=self.url, id=self.id, pwd=self.pwd, cert=self.cert
            )
        else:
            logger.info(
                f"[{self.__class__.__name__}] DB에 계좌정보가 존재하는지 확인해 주세요."
            )
            QMessageBox.information(
                self, "환경설정", "DB에 계좌정보가 존재하는지 확인해 주세요!"
            )
            if self.dialog.get("환경설정") is not None:
                try:
                    self.dialog["환경설정"].show()
                except Exception as e:
                    self.dialog["환경설정"] = View_Configuration(parent=self)
                    self.dialog["환경설정"].show()
            else:
                self.dialog["환경설정"] = View_Configuration(parent=self)
                self.dialog["환경설정"].show()

    def OnLogin(self, code, msg):
        클래스이름 = self.__class__.__name__
        함수이름 = inspect.currentframe().f_code.co_name
        logger.info("%s-%s code %s msg %s " % (클래스이름, 함수이름, code, msg))

        if code == "0000":
            self.statusbar.showMessage("==---->>> 로그인 됨! <<<-------==")

            # 포트폴리오 통기화 시작
            if self.dialog.get("계좌정보조회") is None:
                self.dialog["계좌정보조회"] = View_계좌정보조회(parent=self)
            try:
                self.dialog["계좌정보조회"].schedule_next_run()
                # self.dialog["계좌정보조회"].start_timer()
                self.dialog["계좌정보조회"].inquiry()  # 시작하면 화면을 그리기 위해.
            except Exception as e:
                self.dialog["계좌정보조회"] = View_계좌정보조회(parent=self)
                self.dialog["계좌정보조회"].schedule_next_run()

            if self.dialog.get("분별가격정보백업") is None:
                self.dialog["분별가격정보백업"] = View_분별가격정보백업(parent=self)
            try:
                self.dialog["분별가격정보백업"].schedule_next_run()
                self.dialog["분별가격정보백업"].start_timer()
            except Exception as e:
                self.dialog["분별가격정보백업"] = View_분별가격정보백업(parent=self)
                self.dialog["분별가격정보백업"].schedule_next_run()
                # self.dialog["분별가격정보백업"].start_timer()

            # 누락된 종목을 등록하지 못하는 현상 발생
            # 뉴스조회가 부하를 주는 듯..1초에 4건씩 쓰기도
            logger.info(
                f"[{self.__class__.__name__}] ==---->>> OnLogin 뉴스조회 query <<<-------=="
            )
            if self.dialog.get("뉴스") is None:
                self.dialog["뉴스"] = View_뉴스(parent=self)
            try:
                self.dialog["뉴스"].AddCode()
            except Exception as e:
                self.dialog["뉴스"] = View_뉴스(parent=self)
                self.dialog["뉴스"].AddCode()

        else:
            self.statusbar.showMessage("%s %s" % (code, msg))
            logger.info(f"[{self.__class__.__name__}] {code, msg}")
            QMessageBox.information(self, "환경설정", "%s %s" % (code, msg))
            if self.dialog.get("환경설정") is not None:
                try:
                    self.dialog["환경설정"].show()
                except Exception as e:
                    self.dialog["환경설정"] = View_Configuration(parent=self)
                    self.dialog["환경설정"].show()
            else:
                self.dialog["환경설정"] = View_Configuration(parent=self)
                self.dialog["환경설정"].show()

    def OnLogout(self):
        # 클래스이름 = self.__class__.__name__
        # 함수이름 = inspect.currentframe().f_code.co_name
        # logger.info(  "%s-%s " % (클래스이름, 함수이름))
        self.statusbar.showMessage("==---->>> 로그아웃 됨! <<<-------==")

    def OnDisconnect(self):
        # 클래스이름 = self.__class__.__name__
        # 함수이름 = inspect.currentframe().f_code.co_name
        # logger.info(  "%s-%s " % (클래스이름, 함수이름))

        # Goose 상태 저장
        self.SaveGoosesToDb()

        self.statusbar.showMessage("==---->>> 연결이 끊겼음! <<<-------==")
        self.connection.login(url=self.url, id=self.id, pwd=self.pwd, cert=self.cert)

    def OnReceiveMessage(self, systemError, messageCode, message):
        # 클래스이름 = self.__class__.__name__
        # 함수이름 = inspect.currentframe().f_code.co_name
        # logger.info(  "%s-%s " % (클래스이름, 함수이름), systemError, messageCode, message)
        pass

    def OnReceiveData(self, szTrCode, result):
        if szTrCode == "t0167":
            # logger.info( "Server Time : {} {}".format(result[0], result[1]))
            self.statusbar.showMessage("{} {}".format(result[0], result[1]))

        elif szTrCode == "t0425":
            #  Pending Order Amount Reconciliation
            total_unfilled_amt = result.get("total_unfilled_amt", 0.0)
            BaseGoose.coordinator.reconcile(total_unfilled_amt)
            logger.debug(
                f"⚖️ [Reconciler] t0425 Sync Completed: {total_unfilled_amt:,.0f}원"
            )

    def OnReceiveRealData(self, szTrCode, result):
        # logger.info(  szTrCode, result)
        pass

    def MENU_Action(self, qaction):
        logger.debug("Action Slot %s %s " % (qaction.objectName(), qaction.text()))
        _action = qaction.objectName()
        if _action == "actionExit":
            # self.connection.disconnect()
            self.close()

        if _action == "actionLogin":
            self.MyLogin()

        if _action == "actionLogout":
            self.connection.logout()

        # 일별가격정보 백업
        if _action == "actionPriceBackupDay":
            if self.dialog.get("일별가격정보백업") is not None:
                try:
                    self.dialog["일별가격정보백업"].show()
                except Exception as e:
                    self.dialog["일별가격정보백업"] = View_일별가격정보백업(parent=self)
                    self.dialog["일별가격정보백업"].show()
            else:
                self.dialog["일별가격정보백업"] = View_일별가격정보백업(parent=self)
                self.dialog["일별가격정보백업"].show()

        # 분별가격정보 백업
        if _action == "actionPriceBackupMin":
            if self.dialog.get("분별가격정보백업") is not None:
                try:
                    self.dialog["분별가격정보백업"].show()
                except Exception as e:
                    self.dialog["분별가격정보백업"] = View_분별가격정보백업(parent=self)
                    self.dialog["분별가격정보백업"].show()
            else:
                self.dialog["분별가격정보백업"] = View_분별가격정보백업(parent=self)
                self.dialog["분별가격정보백업"].show()

        # 일별업종정보 백업
        if _action == "actionSectorBackupDay":
            if self.dialog.get("일별업종정보백업") is not None:
                try:
                    self.dialog["일별업종정보백업"].show()
                except Exception as e:
                    self.dialog["일별업종정보백업"] = View_일별업종정보백업(parent=self)
                    self.dialog["일별업종정보백업"].show()
            else:
                self.dialog["일별업종정보백업"] = View_일별업종정보백업(parent=self)
                self.dialog["일별업종정보백업"].show()

        # 종목별 투자자정보 백업
        if _action == "actionInvestorBackup":
            if self.dialog.get("종목별투자자정보백업") is not None:
                try:
                    self.dialog["종목별투자자정보백업"].show()
                except Exception as e:
                    self.dialog["종목별투자자정보백업"] = View_종목별투자자정보백업(
                        parent=self
                    )
                    self.dialog["종목별투자자정보백업"].show()
            else:
                self.dialog["종목별투자자정보백업"] = View_종목별투자자정보백업(
                    parent=self
                )
                self.dialog["종목별투자자정보백업"].show()

        # 종목코드 조회/저장
        if _action == "actionStockcode":
            if self.dialog.get("종목코드조회") is not None:
                try:
                    self.dialog["종목코드조회"].show()
                except Exception as e:
                    self.dialog["종목코드조회"] = View_종목코드조회(parent=self)
                    self.dialog["종목코드조회"].show()
            else:
                self.dialog["종목코드조회"] = View_종목코드조회(parent=self)
                self.dialog["종목코드조회"].show()

        # 거래결과
        if _action == "actionTool2ls":
            if self.dialog.get("외부신호2LS") is not None:
                try:
                    self.dialog["외부신호2LS"].show()
                except Exception as e:
                    self.dialog["외부신호2LS"] = View_외부신호2LS(parent=self)
                    self.dialog["외부신호2LS"].show()
            else:
                self.dialog["외부신호2LS"] = View_외부신호2LS(parent=self)
                self.dialog["외부신호2LS"].show()

        if _action == "actionTradeResult":
            if self.dialog.get("거래결과") is not None:
                try:
                    self.dialog["거래결과"].show()
                except Exception as e:
                    self.dialog["거래결과"] = View_거래결과(parent=self)
                    self.dialog["거래결과"].show()
            else:
                self.dialog["거래결과"] = View_거래결과(parent=self)
                self.dialog["거래결과"].show()

        # 일자별 주가
        if _action == "actionDailyPrice":
            if self.dialog.get("일자별주가") is not None:
                try:
                    self.dialog["일자별주가"].show()
                except Exception as e:
                    self.dialog["일자별주가"] = View_일별주가(parent=self)
                    self.dialog["일자별주가"].show()
            else:
                self.dialog["일자별주가"] = View_일별주가(parent=self)
                self.dialog["일자별주가"].show()

        # 분별 주가
        if _action == "actionMinuitePrice":
            if self.dialog.get("분별주가") is not None:
                try:
                    self.dialog["분별주가"].show()
                except Exception as e:
                    self.dialog["분별주가"] = View_분별주가(parent=self)
                    self.dialog["분별주가"].show()
            else:
                self.dialog["분별주가"] = View_분별주가(parent=self)
                self.dialog["분별주가"].show()

        # 업종정보
        if _action == "actionSectorView":
            if self.dialog.get("업종정보조회") is not None:
                try:
                    self.dialog["업종정보조회"].show()
                except Exception as e:
                    self.dialog["업종정보조회"] = View_업종정보(parent=self)
                    self.dialog["업종정보조회"].show()
            else:
                self.dialog["업종정보조회"] = View_업종정보(parent=self)
                self.dialog["업종정보조회"].show()

        # 업종별종목
        if _action == "actionStocksInIndex":
            if self.dialog.get("업종별종목정보") is not None:
                try:
                    self.dialog["업종별종목정보"].show()
                except Exception as e:
                    self.dialog["업종별종목정보"] = View_업종별종목정보(parent=self)
                    self.dialog["업종별종목정보"].show()
            else:
                self.dialog["업종별종목정보"] = View_업종별종목정보(parent=self)
                self.dialog["업종별종목정보"].show()

        # 테마정보
        if _action == "actionTheme":
            if self.dialog.get("테마정보조회") is not None:
                try:
                    self.dialog["테마정보조회"].show()
                except Exception as e:
                    self.dialog["테마정보조회"] = View_테마정보(parent=self)
                    self.dialog["테마정보조회"].show()
            else:
                self.dialog["테마정보조회"] = View_테마정보(parent=self)
                self.dialog["테마정보조회"].show()

        # 종목별 투자자
        if _action == "actionInvestors":
            if self.dialog.get("종목별투자자") is not None:
                try:
                    self.dialog["종목별투자자"].show()
                except Exception as e:
                    self.dialog["종목별투자자"] = View_종목별투자자(parent=self)
                    self.dialog["종목별투자자"].show()
            else:
                self.dialog["종목별투자자"] = View_종목별투자자(parent=self)
                self.dialog["종목별투자자"].show()

        # 종목별 투자자2
        if _action == "actionInvestors2":
            if self.dialog.get("종목별투자자2") is not None:
                try:
                    self.dialog["종목별투자자2"].show()
                except Exception as e:
                    self.dialog["종목별투자자2"] = View_종목별투자자2(parent=self)
                    self.dialog["종목별투자자2"].show()
            else:
                self.dialog["종목별투자자2"] = View_종목별투자자2(parent=self)
                self.dialog["종목별투자자2"].show()

        # 호가창정보
        if _action == "actionAskBid":
            if self.dialog.get("호가창정보") is not None:
                try:
                    self.dialog["호가창정보"].show()
                except Exception as e:
                    self.dialog["호가창정보"] = View_호가창정보(parent=self)
                    self.dialog["호가창정보"].show()
            else:
                self.dialog["호가창정보"] = View_호가창정보(parent=self)
                self.dialog["호가창정보"].show()

        # 실시간정보
        if _action == "actionRealDataDialog":
            if self.dialog.get("실시간정보") is not None:
                try:
                    self.dialog["실시간정보"].show()
                except Exception as e:
                    self.dialog["실시간정보"] = View_실시간정보(parent=self)
                    self.dialog["실시간정보"].show()
            else:
                self.dialog["실시간정보"] = View_실시간정보(parent=self)
                self.dialog["실시간정보"].show()

        # 뉴스
        if _action == "actionNews":
            if self.dialog.get("뉴스") is not None:
                try:
                    self.dialog["뉴스"].show()
                except Exception as e:
                    self.dialog["뉴스"] = View_뉴스(parent=self)
                    self.dialog["뉴스"].show()
            else:
                self.dialog["뉴스"] = View_뉴스(parent=self)
                self.dialog["뉴스"].show()

        # 환경설정
        if _action == "actionConfigurationDialog":
            if self.dialog.get("환경설정") is not None:
                try:
                    self.dialog["환경설정"].show()
                except Exception as e:
                    self.dialog["환경설정"] = View_Configuration(parent=self)
                    self.dialog["환경설정"].show()
            else:
                self.dialog["환경설정"] = View_Configuration(parent=self)
                self.dialog["환경설정"].show()

        # 계좌정보 조회
        if _action == "actionAccountDialog":
            if self.dialog.get("계좌정보조회") is not None:
                try:
                    self.dialog["계좌정보조회"].show()
                except Exception as e:
                    self.dialog["계좌정보조회"] = View_계좌정보조회(parent=self)
                    self.dialog["계좌정보조회"].show()
            else:
                self.dialog["계좌정보조회"] = View_계좌정보조회(parent=self)
                self.dialog["계좌정보조회"].show()

        # 차트인덱스
        if _action == "actionChartIndex":
            if self.dialog.get("차트인덱스") is not None:
                try:
                    self.dialog["차트인덱스"].show()
                except Exception as e:
                    self.dialog["차트인덱스"] = View_차트인덱스(parent=self)
                    self.dialog["차트인덱스"].show()
            else:
                self.dialog["차트인덱스"] = View_차트인덱스(parent=self)
                self.dialog["차트인덱스"].show()

        # 종목검색
        if _action == "actionSearchItems":
            if self.dialog.get("종목검색") is not None:
                try:
                    self.dialog["종목검색"].show()
                except Exception as e:
                    self.dialog["종목검색"] = View_종목검색(parent=self)
                    self.dialog["종목검색"].show()
            else:
                self.dialog["종목검색"] = View_종목검색(parent=self)
                self.dialog["종목검색"].show()

        # e종목검색
        if _action == "actionESearchItems":
            if self.dialog.get("e종목검색") is not None:
                try:
                    self.dialog["e종목검색"].show()
                except Exception as e:
                    self.dialog["e종목검색"] = View_e종목검색(parent=self)
                    self.dialog["e종목검색"].show()
            else:
                self.dialog["e종목검색"] = View_e종목검색(parent=self)
                self.dialog["e종목검색"].show()

        if _action == "actionOpenScreen":
            XQ = t8430(parent=self)
            XQ.Query(구분="0")

            res = XQ.RequestLinkToHTS("&STOCK_CODE", "069500", "")

        # 주문테스트
        if _action == "actionOrder":
            if self.dialog.get("주문테스트") is not None:
                try:
                    self.dialog["주문테스트"].show()
                except Exception as e:
                    self.dialog["주문테스트"] = View_주문테스트(parent=self)
                    self.dialog["주문테스트"].show()
            else:
                self.dialog["주문테스트"] = View_주문테스트(parent=self)
                self.dialog["주문테스트"].show()

        # 사용법
        if _action == "actionMustRead":
            webbrowser.open("https://thinkpoolost.wixsite.com/moneybot")

        if _action == "actionUsage":
            webbrowser.open(
                "https://docs.google.com/document/d/1BGENxWqJyZdihQFuWcmTNy3_4J0kHolCc-qcW3RULzs/edit"
            )

        if _action == "actionVersion":
            if self.dialog.get("Version") is not None:
                try:
                    self.dialog["Version"].show()
                except Exception as e:
                    self.dialog["Version"] = View_버전(parent=self)
                    self.dialog["Version"].show()
            else:
                self.dialog["Version"] = View_버전(parent=self)
                self.dialog["Version"].show()

        if _action == "actionGooseLoad":
            reply = QMessageBox.question(
                self,
                "Goose 탑제",
                "저장된 Goose를 읽어올까요?",
                QMessageBox.Yes | QMessageBox.Cancel,
            )
            if reply == QMessageBox.Cancel:
                pass
            elif reply == QMessageBox.Yes:
                self.LoadGoosesFromDb()

                self.GooseView()

        elif _action == "actionGooseSave":
            reply = QMessageBox.question(
                self,
                "Goose 저장",
                "현재 Goose를 저장할까요?",
                QMessageBox.Yes | QMessageBox.No | QMessageBox.Cancel,
            )
            if reply == QMessageBox.Cancel:
                pass
            elif reply == QMessageBox.No:
                pass
            elif reply == QMessageBox.Yes:
                self.SaveGoosesToDb()

            # self.GooseView()

        elif _action == "actionGooseOneRun":
            try:
                GooseUUID = self.goose_model._data[
                    self.goose_modeltableView_goose_current_index.row() : self.goose_modeltableView_goose_current_index.row()
                    + 1
                ]["GooseID"].values[0]
            except Exception as e:
                클래스이름 = self.__class__.__name__
                함수이름 = inspect.currentframe().f_code.co_name
                logger.error(
                    "%s-%s %s: %s" % (클래스이름, 함수이름, get_linenumber(), e)
                )
                GooseUUID = ""

            goose_found = None
            for r in self.gooses:
                if r.UUID == GooseUUID:
                    goose_found = r
                    break

            if goose_found == None:
                return

            goose_found.Lay(flag=True, parent=self)

            self.GooseView()

        elif _action == "actionGooseOneStop":
            try:
                GooseUUID = self.goose_model._data[
                    self.goose_modeltableView_goose_current_index.row() : self.goose_modeltableView_goose_current_index.row()
                    + 1
                ]["GooseID"].values[0]
            except Exception as e:
                클래스이름 = self.__class__.__name__
                함수이름 = inspect.currentframe().f_code.co_name
                logger.error(
                    "%s-%s %s: %s" % (클래스이름, 함수이름, get_linenumber(), e)
                )
                GooseUUID = ""

            goose_found = None
            for r in self.gooses:
                if r.UUID == GooseUUID:
                    goose_found = r
                    break

            if goose_found == None:
                return

            reply = QMessageBox.question(
                self,
                "Goose 실행 종료",
                "Goose 실행을 종료할까요?\n%s" % goose_found.getstatus(),
                QMessageBox.Yes | QMessageBox.No | QMessageBox.Cancel,
            )
            if reply == QMessageBox.Cancel:
                pass
            elif reply == QMessageBox.No:
                pass
            elif reply == QMessageBox.Yes:
                self.stop_goose(goose_found)
                self.GooseView()

        elif _action == "actionGooseRun":
            # 전체Goose실행 배뉴 선택시 재연결 - 검색종목등록 수 초과 에러 방지
            # 재연결이 되지 않음..수동으로 로그아웃 하는 걸로
            # if self.connection.IsConnected():
            #     self.connection.logout()
            #     self.MyLogin()
            self.GooseRun()
            # 시간차 기동 후로 이동
            # self.GooseView()

        elif _action == "actionGooseStop":
            for r in self.gooses:
                if not r.running:
                    return
            reply = QMessageBox.question(
                self,
                "알낳기 종료",
                "알낳기를 종료할까요?",
                QMessageBox.Yes | QMessageBox.No | QMessageBox.Cancel,
            )
            if reply == QMessageBox.Cancel:
                pass
            elif reply == QMessageBox.No:
                pass
            elif reply == QMessageBox.Yes:
                for g in self.gooses:
                    self.stop_goose(g)
                self.GooseView()

        elif _action == "actionGooseRemove":
            try:
                GooseUUID = self.goose_model._data[
                    self.goose_modeltableView_goose_current_index.row() : self.goose_modeltableView_goose_current_index.row()
                    + 1
                ]["GooseID"].values[0]

                goose_found = None
                for r in self.gooses:
                    if r.UUID == GooseUUID:
                        goose_found = r
                        break

                if goose_found == None:
                    return

                reply = QMessageBox.question(
                    self,
                    "Goose 삭제",
                    "Goose를 삭제할까요?\n%s" % goose_found.getstatus()[0:4],
                    QMessageBox.Yes | QMessageBox.No | QMessageBox.Cancel,
                )
                if reply == QMessageBox.Cancel:
                    pass
                elif reply == QMessageBox.No:
                    pass
                elif reply == QMessageBox.Yes:
                    self.gooses.remove(goose_found)

                # self.GooseView()
            except Exception as e:
                클래스이름 = self.__class__.__name__
                함수이름 = inspect.currentframe().f_code.co_name
                logger.error(
                    "%s-%s %s: %s" % (클래스이름, 함수이름, get_linenumber(), e)
                )
                pass

        elif _action == "actionGooseClear":
            reply = QMessageBox.question(
                self,
                "Goose 전체 삭제",
                "Goose 전체를 삭제할까요?",
                QMessageBox.Yes | QMessageBox.No | QMessageBox.Cancel,
            )
            if reply == QMessageBox.Cancel:
                pass
            elif reply == QMessageBox.No:
                pass
            elif reply == QMessageBox.Yes:
                self.gooses = []

            # self.GooseView()

        elif _action == "actionGooseView":
            self.GooseView()
            for r in self.gooses:
                logger.debug(
                    "%s %s %s %s" % (r.Name, r.UUID, len(r.portfolio), r.getstatus())
                )

        # print(f"_action : {_action}, self.plugins : {self.plugins}")
        if _action in self.plugins.keys():
            goose = self.plugins[_action].instance()
            # print("111 >>>>>>>>> _action %s goose %s %s" %(_action, goose, goose.UUID))
            goose.set_database(database=DATABASE)
            # goose.set_secret(계좌번호=self.계좌번호, 비밀번호=self.거래비밀번호)
            ret = goose.modal(parent=self)
            if ret == 1:
                self.gooses.append(goose)
            self.GooseView()

    def schedule_backup(self):
        # QTimer.singleShot(5000, self.start_backup)
        now = QTime.currentTime()  # QTime 사용
        backup_time = QTime(15, 30, 10)  # 15시 31분 설정
        if now > backup_time:
            return  # 현재 시간이 종료 시간 이후라면 종료 예약을 하지 않음
        delay = now.msecsTo(backup_time)
        print(f"schedule_backup delay: {delay}")
        QTimer.singleShot(delay, self.start_backup)

    def start_backup(self):
        self.SaveGoosesToDb()
        # self.GooseView()
        if self.dialog.get("종목코드조회") is None:
            self.dialog["종목코드조회"] = View_종목코드조회(parent=self)
        try:
            # self.dialog['종목코드조회'].inquiry()
            pass
        except Exception as e:
            self.dialog["종목코드조회"] = View_종목코드조회(parent=self)
            # self.dialog['종목코드조회'].inquiry()

    def schedule_shutdown(self):
        now = QTime.currentTime()  # QTime 사용
        shutdown_time = QTime(15, 30, 10)  # 15시 31분 설정
        if now > shutdown_time:
            return  # 현재 시간이 종료 시간 이후라면 종료 예약을 하지 않음
        delay = now.msecsTo(shutdown_time)
        print(f"schedule_shutdown delay: {delay}")
        QTimer.singleShot(delay, self.close)

    def getGooseStatus(self):
        result = False
        if self.gooses is not None and len(self.gooses) > 0:
            for r in self.gooses:
                if r.running:
                    result = True
        else:
            result = False
        print(f"getGooseStatus : {result}")
        return result

    def init_zmq(self):
        """Called by MainWindow.__init__ to start ZMQ Thread-safe Listener"""
        try:
            from managers.EventLoopManager import ZMQReceiverThread

            self.zmq_receiver = ZMQReceiverThread(self)

            # [Step 3.2] Connect signal to central router slot
            self.zmq_receiver.signal_received.connect(self._on_zmq_command_received)
            self.zmq_receiver.start()

            logger.info(
                "✅ [GoldenGoose] ZMQ Receiver Thread (QThread) Started on Port 5558"
            )
        except Exception as e:
            logger.error(f"❌ [GoldenGoose] ZMQ Receiver Init Failed: {e}")

    @pyqtSlot(dict)
    def _on_zmq_command_received(self, data):
        """
        Callback for ZMQ messages (Runs in Main Thread via QThread Signal)
        Dispatches commands to Gooses.
        """
        try:
            from util.Utils32 import safe_invoke

            # [Support] Both single dict and list of dicts
            raw_commands = data if isinstance(data, list) else [data]

            for cmd_data in raw_commands:
                if not isinstance(cmd_data, dict):
                    continue

                # 1. ZMQ Payload 파싱 (안전한 추출)
                command_type = cmd_data.get("command", "")
                if not command_type:
                    command_type = cmd_data.get("order_type", "")

                if (
                    not command_type
                    and "command" in cmd_data
                    and isinstance(cmd_data["command"], dict)
                ):
                    command_type = cmd_data["command"].get("type", "")

                ticker = cmd_data.get("ticker", "")
                if (
                    not ticker
                    and "command" in cmd_data
                    and isinstance(cmd_data["command"], dict)
                ):
                    ticker = cmd_data["command"].get("stock_cd", "")

                command_type = str(command_type).upper()

                # 🛡️  ZMQ 통신 주파수 일치 및 스레드 세이프(Thread-Safe) 응답
                if command_type == "REQ_ACCOUNT_INFO":
                    # 1. [Institutional-Grade] Robust Account Data Sync
                    #  64-bit Brain이 정확히 기대하는 Key와 구조체(포트폴리오)로 강제 초기화
                    acc_data = {
                        "D+2추정예수금": 0.0,
                        "총평가금액": 0.0,
                        "추정순자산": 0.0,  # Legacy UI 호환용 유지
                        "미체결주문금액": BaseGoose.coordinator.get_pending_amount(),  # [Phase 1] Pending Sync
                        "포트폴리오": {},  # [핵심] Brain의 Iteration KeyError 원천 차단
                    }

                    # Merge with existing account_dict if it exists
                    stored_dict = getattr(self, "account_dict", None)
                    if stored_dict is not None:
                        acc_data.update(stored_dict)

                    # 2. Extract live data from the Account Info Dialog safely
                    dialogs = getattr(self, "dialog", None)
                    if dialogs is not None and "계좌정보조회" in dialogs:
                        acc_info = dialogs["계좌정보조회"]
                        df = getattr(acc_info, "dfAccSumInfo", None)

                        if df is not None and not df.empty:
                            try:
                                #  콤마(,) 포함 문자열 및 None에 대한 안전한 형변환
                                def safe_cast(val):
                                    if isinstance(val, str):
                                        val = val.replace(",", "")
                                    return float(val) if val else 0.0

                                # 64-bit 엔진의 명세에 맞춰 정확하게 데이터 매핑
                                if "추정순자산" in df.columns:
                                    val = safe_cast(df.at[0, "추정순자산"])
                                    acc_data["추정순자산"] = val
                                    acc_data["총평가금액"] = (
                                        val  # Brain 전용 Key 동기화
                                    )

                                if "추정D2예수금" in df.columns:
                                    acc_data["D+2추정예수금"] = safe_cast(
                                        df.at[0, "추정D2예수금"]
                                    )

                            except Exception as e:
                                logger.error(
                                    f"⚠️ [Router] Account DataFrame Parse Error: {e}"
                                )

                    # 3. [Thread-Safe] Main Thread Callback for ZMQ Publisher
                    def safe_publish_account_status():
                        try:
                            from util.zmq_manager import (
                                get_shared_publisher,
                                TOPIC_EVENT,
                            )

                            pub = get_shared_publisher()
                            pub.publish_data(
                                TOPIC_EVENT,
                                {"event": "ACCOUNT_STATUS", "account_info": acc_data},
                            )
                            # 콤마 포맷팅 적용하여 로그 가독성 확보
                            val = acc_data.get("총평가금액", 0)
                            logger.info(
                                f"📡 [Router] Multi-Thread Sync: Account data published ({val:,.0f}원)"
                            )
                        except Exception as e:
                            logger.error(f"❌ [Router] ZMQ Async Publish Failed: {e}")

                    # 4. Delegate to Main GUI Thread (Thread-Safety Guardian)
                    safe_invoke(None, None, safe_publish_account_status)
                    continue  # Exit current loop iteration immediately

                if command_type not in ["BUY", "SELL", "CANCEL"]:
                    continue

                # 🛡️  Dumb Router Pattern (32bit 검열 로직 완전 폐기)
                # 64bit Brain이 보낸 명령은 슬롯/보유 여부와 무관하게 100% 무조건 집행합니다.
                goose_instance = None

                # AIGoose나 GuardianGoose 중 살아있는 첫 번째 객체에 무조건 명령을 하달합니다.
                ai_goose = next(
                    (
                        g
                        for g in self.gooses
                        if getattr(g, "Name", "").lower() == "aigoose"
                    ),
                    None,
                )
                guardian_goose = next(
                    (
                        g
                        for g in self.gooses
                        if getattr(g, "Name", "").lower() == "guardiangoose"
                    ),
                    None,
                )

                goose_instance = ai_goose or guardian_goose

                if not goose_instance:
                    logger.error(
                        f"❌ [Router] CRITICAL: 활성화된 Goose 인스턴스가 없습니다. CMD={command_type}, TICKER={ticker}"
                    )
                    continue

                #  Pre-emptive Pending Amount Allocation (Margin Guard)
                if command_type == "BUY" and goose_instance:
                    qty = int(cmd_data.get("qty", cmd_data.get("quantity", 0)))
                    price = float(cmd_data.get("price", 0.0))
                    brain_qty = int(
                        cmd_data.get("brain_qty", qty)
                    )  # 💡 전체 목표 수량 추출

                    if (
                        not qty
                        and "command" in cmd_data
                        and isinstance(cmd_data["command"], dict)
                    ):
                        qty = int(cmd_data["command"].get("qty", 0))
                        price = float(cmd_data["command"].get("price", 0.0))
                        brain_qty = int(cmd_data["command"].get("brain_qty", qty))

                    if hasattr(goose_instance, "coordinator"):
                        with goose_instance.coordinator._lock:
                            # 1. Iceberg 락(Lock)을 추적할 상태 딕셔너리가 없으면 초기화
                            if not hasattr(
                                goose_instance.coordinator, "iceberg_locked_qty"
                            ):
                                goose_instance.coordinator.iceberg_locked_qty = {}

                            est_amt = 0

                            # 2. 분할 전송(Iceberg) 주문 판단: 목표 전체 수량(brain_qty)이 현재 수신된 조각(qty)보다 클 때
                            if brain_qty > qty:
                                if (
                                    ticker
                                    not in goose_instance.coordinator.iceberg_locked_qty
                                ):
                                    # [첫 번째 조각 도착] 빙산 전체(brain_qty) 금액을 한 번에 락(Lock)
                                    est_amt = brain_qty * price
                                    goose_instance.coordinator.iceberg_locked_qty[
                                        ticker
                                    ] = brain_qty - qty
                                    logger.info(
                                        f"💳 [Margin Guard-Iceberg] {ticker} 전체수량({brain_qty}주) 선차감 락 적용: +{est_amt:,.0f}원"
                                    )
                                else:
                                    # [두 번째 이후 조각 도착] 이미 첫 조각에서 전체 돈을 뺐으므로 차감액 0원
                                    est_amt = 0
                                    goose_instance.coordinator.iceberg_locked_qty[
                                        ticker
                                    ] -= qty
                                    logger.info(
                                        f"💳 [Margin Guard-Iceberg] {ticker} 후속조각({qty}주) 통과 (이미 선차감됨)"
                                    )

                                    # 모든 조각이 도착했으면 메모리 누수 방지를 위해 락(Lock) 해제
                                    if (
                                        goose_instance.coordinator.iceberg_locked_qty[
                                            ticker
                                        ]
                                        <= 0
                                    ):
                                        del goose_instance.coordinator.iceberg_locked_qty[
                                            ticker
                                        ]
                            else:
                                # 3. 일반 단일 주문인 경우 (기존 로직과 동일)
                                est_amt = qty * price
                                logger.info(
                                    f"💳 [Margin Guard] Pre-empted Pending: {ticker} | +{est_amt:,.0f}원"
                                )

                            # 4. 가상 잔고에 차감액 최종 반영 (후속 조각은 est_amt가 0이므로 안전함)
                            if est_amt > 0:
                                goose_instance.coordinator.register_order_sent(est_amt)

                            if (
                                est_amt > 0 or brain_qty > qty
                            ):  # 불필요한 로그 도배 방지
                                logger.info(
                                    f"💰 [Margin Status] {ticker} 처리 후 Total Pending: {goose_instance.coordinator._pending_order_amt:,.0f}원"
                                )

                # [Step 3.2] 최종 확정된 Goose로 명령 하달 (New Injection Point: receive_external_signal)
                logger.info(
                    f"🚀 [{goose_instance.__class__.__name__}] Dispatching ZMQ CMD: {command_type} for {ticker}"
                )

                # QThread 시그널을 통해 이미 MainThread에 진입했으므로 직접 호출 가능
                # 하지만 로직의 일관성을 위해 slot 함수 호출
                goose_instance.receive_external_signal(cmd_data)

        except Exception as e:
            logger.error(
                f"[MainWindowLogic._on_zmq_command_received] Fatal Error: {e}",
                exc_info=True,
            )
