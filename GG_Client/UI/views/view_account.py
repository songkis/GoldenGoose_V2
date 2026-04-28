import threading
import inspect
import datetime
from collections import Counter

from PyQt5.QtWidgets import QDialog
from PyQt5.QtCore import Qt, QTimer, QDateTime, QTime

from PyQt5 import uic

from UI.components.models import PandasModel
from UI.resource_resolver import resource_path
from util.CommUtils import setUnitInvestment, isOverCloseTime, get_linenumber
from config.ai_settings import ACC_TERM
from dto.CPortStock import CPortStock

from xing.XAQuaries import t0424, t1511
from util.CommUtils import updateSearchStock
from util.zmq_manager import get_shared_publisher, TOPIC_EVENT

from config.telegram_setting import ToTelegram

logger = None  # 초기값


def set_logger(external_logger):
    global logger
    logger = external_logger


try:
    FORM_CLASS, _ = uic.loadUiType(resource_path("UI/계좌정보조회.ui"))
except Exception as e:
    logger.error(f"Failed to load UI 계좌정보조회.ui: {e}")
    raise


class View_계좌정보조회(QDialog, FORM_CLASS):
    def __init__(self, parent=None):
        super(View_계좌정보조회, self).__init__(parent)
        self.setAttribute(Qt.WA_DeleteOnClose)
        self.setupUi(self)
        self.parent = parent
        self.model1 = PandasModel()
        self.tableView_1.setModel(self.model1)
        self.model2 = PandasModel()
        self.tableView_2.setModel(self.model2)

        self.result = []
        if hasattr(self.parent, "connection"):
            self.connection = self.parent.connection
        else:
            logger.error("Parent has no connection attribute")
            self.connection = None

        # 계좌정보 불러오기
        if self.connection and hasattr(self.connection, "ActiveX"):
            nCount = self.connection.ActiveX.GetAccountListCount()
            for i in range(nCount):
                self.comboBox.addItem(self.connection.ActiveX.GetAccountList(i))

        # secret파일에 등록된 계좌번호 항목 selected
        if hasattr(self.parent, "계좌번호"):
            self.comboBox.setCurrentText(self.parent.계좌번호)
        if hasattr(self.parent, "거래비밀번호"):
            self.lineEdit.setText(self.parent.거래비밀번호)

        self.XQ_t0424 = t0424(parent=self)

        # 업종정보 코스피 코스닥 지수 get
        self.XQ_t1511 = t1511(parent=self)
        self.kospiDic = None
        self.kosdaqDic = None
        self.last_trigger_minute = -1
        self.portfolio_lock = threading.Lock()  # 🔐 Lock 객체 생성

    def schedule_next_run(self):
        now = QDateTime.currentDateTime()
        target_time = QDateTime(now.date(), QTime(9, 0, 0))

        if now > target_time and not isOverCloseTime():
            target_time = now.addSecs(1)
        elif now > target_time:
            target_time = target_time.addDays(1)
        delay = now.msecsTo(target_time)
        print(f"계좌정보조회 delay: {delay}")
        QTimer.singleShot(delay, self.start_timer)

    def start_timer(self):
        # window.account_dict might be self.parent.account_dict
        env = "Unknown"
        if hasattr(self.parent, "account_dict"):
            env = self.parent.account_dict.get("거래환경", "Unknown")

        logger.info(
            f"[{self.__class__.__name__}] ==---->>> {env} GoldenGoose OnLogin 계좌정보조회 query {ACC_TERM}초주기 <<<-------=="
        )
        thrd = threading.Thread(
            target=ToTelegram(f"GoldenGoose {ACC_TERM}초 단위 계좌정보 조회가 시작됨!"),
            daemon=True,
        )
        thrd.start()
        self.inquiry()
        self.timer = QTimer(self)
        self.timer.timeout.connect(self.runInquiry_and_check_time)
        self.timer.start(ACC_TERM * 1000)  # MIN_TERM 단위 실행

    def runInquiry_and_check_time(self):
        self.inquiry()
        # running 중에만
        # 계좌정보 수신은 계속 동기화.
        if isOverCloseTime():
            self.timer.stop()
            logger.info(
                f"[{self.__class__.__name__}] ===> 15:30 도달! 계좌정보조회 타이머 종료됨."
            )

    def OnReceiveMessage(self, systemError, messageCode, message):
        # if logger:
        #     logger.info(
        #         f"[{self.__class__.__name__}] OnReceiveMessage: sysErr={systemError}, msgCode={messageCode}, msg={message}"
        #     )
        pass

    def OnReceiveData(self, szTrCode, result):
        # if logger:
        #     logger.info(
        #         f"[{self.__class__.__name__}] OnReceiveData triggered for: {szTrCode}"
        #     )
        if szTrCode == "t0424":
            self.dfAccSumInfo, self.dfAccStockInfo = result

            # if logger:
            #     logger.info(
            #         f"[{self.__class__.__name__}] t0424 data length - SumInfo: {len(self.dfAccSumInfo)}, StockInfo: {len(self.dfAccStockInfo)}"
            #     )

            # 계좌정보가 없으면
            if len(self.dfAccSumInfo) == 0:
                if logger:
                    logger.warning(
                        f"[{self.__class__.__name__}] dfAccSumInfo is empty. Returning early..."
                    )
                return

            self.model1.update(self.dfAccSumInfo)
            if hasattr(self.parent, "AccountView"):
                self.parent.AccountView()

            self.model2.update(self.dfAccStockInfo)
            if hasattr(self.parent, "PortfolioView"):
                self.parent.PortfolioView()

            try:
                CTS_종목번호 = self.dfAccSumInfo["CTS_종목번호"].values[0].strip()
                if CTS_종목번호 != "":
                    self.XQ_t0424.Query(
                        계좌번호=self.계좌번호,
                        비밀번호=self.비밀번호,
                        단가구분="1",
                        체결구분="0",
                        단일가구분="0",
                        제비용포함여부="1",
                        CTS_종목번호=CTS_종목번호,
                    )
                if result:
                    self.syncAccInfo()

            except KeyError as ke:
                if logger:
                    logger.error(
                        f"[{self.__class__.__name__}] KeyError during Data Parse/Sync: {ke}"
                    )
                pass
            except IndexError as ie:
                if logger:
                    logger.error(
                        f"[{self.__class__.__name__}] IndexError during Data Parse/Sync: {ie}"
                    )
                pass
            except Exception as e:
                if logger:
                    logger.error(
                        f"[{self.__class__.__name__}] Unexpected Error during Data Parse/Sync: {e}"
                    )
                pass

        if szTrCode == "t1511":
            t1511Dic = result[0]
            sectorCd = t1511Dic["첫번째지수코드"]
            if sectorCd == "001":
                self.kospiDic = t1511Dic
            elif sectorCd == "301":
                self.kosdaqDic = t1511Dic

    def inquiry(self):
        self.계좌번호 = self.comboBox.currentText().strip()
        self.비밀번호 = self.lineEdit.text().strip()

        self.XQ_t0424.Query(
            계좌번호=self.계좌번호,
            비밀번호=self.비밀번호,
            단가구분="1",
            체결구분="0",
            단일가구분="0",
            제비용포함여부="1",
            CTS_종목번호="",
        )

        import time

        # [Python 3.11 COM Fix] Pump messages to ensure t0424 Request(0) event isn't blocked by subsequent queries
        import pythoncom

        try:
            pythoncom.PumpWaitingMessages()
        except Exception as e:
            if logger:
                logger.error(f"inquiry pump error: {e}")

        # Adding delay to prevent Xing API TR request limit (Error -21)
        time.sleep(1.0)

        # 코스피, 코스탁 교차로 실행 t1514호출시
        self.XQ_t1511.Query(업종코드="001")
        import pythoncom

        try:
            pythoncom.PumpWaitingMessages()
        except Exception:
            pass
        time.sleep(1.0)
        self.XQ_t1511.Query(업종코드="301")

    def syncAccInfo(self):
        # if logger:
        #     logger.info(
        #         f"[{self.__class__.__name__}] syncAccInfo() started. Parsing dfAccSumInfo..."
        #     )
        try:
            # 1. 계좌 동기화
            추정순자산 = int(self.dfAccSumInfo["추정순자산"].values[0])
            매입금액 = int(self.dfAccSumInfo["매입금액"].values[0])
            추정D2예수금 = int(self.dfAccSumInfo["추정D2예수금"].values[0])
            # if logger:
            #     logger.info(
            #         f"[{self.__class__.__name__}] Parsed account cash: 순자산={추정순자산}, 매입={매입금액}, D2예수금={추정D2예수금}"
            #     )

            account_info = {
                "추정순자산": 추정순자산,
                "추정D2예수금": 추정D2예수금,
                "매입금액": 매입금액,
            }
            investment_info = setUnitInvestment(account_info, self.parent)
            # logger.info(f"[Account] Investment Info: {investment_info}")
            account_info.update(investment_info)
            # logger.info(f"[Account] Account Info: {account_info}")
            self.parent.account_info = account_info

            # if logger:
            #     logger.info(
            #         f"[{self.__class__.__name__}] Account Info appended: {account_info}"
            #     )

            # [Safety] 만약 예수금이 일정 수준 이상(예: 10만 원) 있다면 '주문가능금액부족' 플래그를 자동 해제
            if 추정D2예수금 > 100000:
                if getattr(self.parent, "주문가능금액부족", False):
                    # logger.info(
                    #    f"💰 [Account] Clearing '주문가능금액부족' flag. Available Cash: {추정D2예수금:,} KRW"
                    # )
                    self.parent.주문가능금액부족 = False

            isDel = False
            isAdd = False
            all_keys = []

            if not hasattr(self.parent, "gooses"):
                return

            # 2. 포트폴리오 오류점검 & 해결.
            for r in self.parent.gooses:
                all_keys.extend(r.portfolio.keys())

            # 각 key가 몇 번 나오는지 센다
            key_counts = Counter(all_keys)
            # 2번 이상 등장하는 key만 추출
            duplicates = [k for k, v in key_counts.items() if v > 1]
            for r in self.parent.gooses:
                for k in duplicates:
                    dup_portfolil = r.portfolio.get(k, None)
                    if dup_portfolil:
                        r.portfolio.pop(k)
                        r.포트폴리오종목삭제(k)
                        break

            condition = "잔고수량 > 0"  # 조건식 작성
            dfAccStockInfo = self.dfAccStockInfo.query(condition)

            if len(dfAccStockInfo) == 0:
                # DB에 저장되어 계속 load되는 쓰레기 제거.
                for r in self.parent.gooses:
                    for k, v in list(r.portfolio.items()):
                        if v.현재가 == 0 and v.매입금액 == 0 and v.평가금액 == 0:
                            r.portfolio.pop(k)
                            r.포트폴리오종목삭제(k)
                            r.executeUnadviseRealData(str(k), "O")
                            updateSearchStock(str(k), -3)
                # DO NOT RETURN EARLY HERE !!
                # We still need to process delList, AccountView updates, and ZMQ publisher!
                # logger.warning(
                #     f"[{self.__class__.__name__}] dfAccStockInfo is empty (no stocks held with 잔고 > 0). Proceeding with ZMQ sync..."
                # )

            # 3.  Goose 포트폴리오에서 실제 계좌정보에 없는 종목(Ghost/Zombie) 식별 및 삭제
            # Ticker Normalization: 'A005930' vs '005930' mismatch 방지를 위해 모두 numeric 코드로 통일
            normalized_held_codes = set(
                str(c).replace("A", "").strip()
                for c in dfAccStockInfo.종목번호.tolist()
            )

            # all_keys is already populated from r.portfolio.keys() at L293
            normalized_all_keys = [str(k).replace("A", "").strip() for k in all_keys]

            # 차집합 계산: 포트폴리오에는 있는데 실계좌에는 없는 종목들
            delList = set(normalized_all_keys).difference(normalized_held_codes)

            if delList and delList != {""} and len(delList) > 0:
                for 삭제종목코드 in delList:
                    for r in self.parent.gooses:
                        # Normalize keys in portfolio before removal
                        port_keys = list(r.portfolio.keys())
                        for p_key in port_keys:
                            if str(p_key).replace("A", "").strip() == 삭제종목코드:
                                delPortfolioItem = r.portfolio.pop(p_key)
                                r.포트폴리오종목삭제(p_key)  # DB 삭제
                                logger.info(
                                    "🧹 [%s] 포트폴리오 좀비 종목 자동 삭제 완료: %s (%s)"
                                    % (
                                        r.Name,
                                        p_key,
                                        getattr(delPortfolioItem, "종목명", "Unknown"),
                                    )
                                )
                                isDel = True
                                r.executeUnadviseRealData(str(p_key), "O")
                                updateSearchStock(
                                    str(p_key), -3
                                )  # Zombie Cleanup status

                                if r.lock_controller is not None:
                                    # 매도/매수 락 모두 해제 (좀비 종목이 락을 점유하고 있을 가능성 차단)
                                    if p_key in r.lock_controller.get_all("sell"):
                                        r.lock_controller.unlock_sell(p_key)
                                    if p_key in r.lock_controller.get_all("buy"):
                                        r.lock_controller.unlock_buy(p_key)
            # 4. 계좌정보에서 Goose 포트폴리오종목의 차집합 결과를 추가대상으로 선정
            addList = set(list(dfAccStockInfo.종목번호)) - set(all_keys)

            if addList and len(addList) > 0:
                for 추가종목코드 in addList:
                    condition_add = "종목번호=='%s'" % 추가종목코드
                    try:
                        잔고수량 = int(
                            dfAccStockInfo.query(condition_add).iloc[0]["잔고수량"]
                        )
                        평균단가 = int(
                            dfAccStockInfo.query(condition_add).iloc[0]["평균단가"]
                        )
                        현재가 = int(
                            dfAccStockInfo.query(condition_add).iloc[0]["현재가"]
                        )
                        매수후고가 = int(평균단가 if 평균단가 > 현재가 else 현재가)
                    except IndexError:
                        continue

                    if int(잔고수량) > 0:
                        dfRecentOrder = None
                        dfStkHighPrce = None
                        for r in self.parent.gooses:
                            transaction_env = self.parent.account_dict.get(
                                "거래환경", "Unknown"
                            )
                            dfRecentOrder = r.최근거래주문내역(
                                transaction_env,
                                "매수",
                                추가종목코드,
                            )
                            if dfRecentOrder is None or dfRecentOrder.empty:
                                주문시각 = datetime.datetime.now().strftime(
                                    "%Y-%m-%d %H:%M:%S"
                                )
                                매수일 = 주문시각
                                종목명 = dfAccStockInfo.query(condition_add).iloc[0][
                                    "종목명"
                                ]
                                dfStkHighPrce = r.최근거래주문고가(추가종목코드)
                                if (
                                    dfStkHighPrce is not None
                                    and not dfStkHighPrce.empty
                                ):
                                    val_high = dfStkHighPrce.iloc[0]["고가"]
                                    if val_high is not None and str(
                                        val_high
                                    ).lower() not in ("nan", "<na>"):
                                        매수후고가 = val_high
                            elif (
                                dfRecentOrder is not None
                                and not dfRecentOrder.empty
                                and int(dfRecentOrder.iloc[0]["수량"]) > 0
                            ):
                                주문시각 = datetime.datetime.strptime(
                                    dfRecentOrder.iloc[0]["주문시각"],
                                    "%Y-%m-%d %H:%M:%S",
                                )
                                매수일 = 주문시각
                                종목명 = dfRecentOrder.iloc[0]["종목명"]
                                val_recent = dfRecentOrder.iloc[0]["고가"]
                                if val_recent is not None and str(
                                    val_recent
                                ).lower() not in ("nan", "<na>"):
                                    매수후고가 = (
                                        val_recent
                                        if val_recent > 매수후고가
                                        else 매수후고가
                                    )

                        # 어떤 Goose이든 없는 경우만 입력
                        if 추가종목코드 not in all_keys:
                            # Defaulting to gooses[1] (AIGoose?) as per original code
                            target_goose = self.parent.gooses[1]
                            target_goose.portfolio[추가종목코드] = CPortStock(
                                매수일=매수일,
                                종목코드=추가종목코드,
                                종목명=종목명,
                                매수가=평균단가,
                                수량=잔고수량,
                                매수후고가=매수후고가,
                            )
                        # 신규추가
                        target_goose.포트폴리오종목갱신(
                            추가종목코드, target_goose.portfolio[추가종목코드]
                        )
                        isAdd = True
                        if target_goose.running:
                            target_goose.executeAdviseRealData(str(추가종목코드), "N")
                            isAdd = True
                    else:
                        for r in self.parent.gooses:
                            r.portfolio.pop(추가종목코드)
                            r.포트폴리오종목삭제(추가종목코드)
                            r.executeUnadviseRealData(str(추가종목코드), "O")
                            updateSearchStock(str(추가종목코드), -3)
                        isAdd = False

            # 5. 종목의 정보 동기화
            for r in self.parent.gooses:
                if len(r.portfolio.keys()) > 0:
                    with self.portfolio_lock:
                        for k, v in list(r.portfolio.items()):
                            condition_sync = "종목번호=='%s'" % v.종목코드
                            try:
                                잔고수량 = int(
                                    dfAccStockInfo.query(condition_sync).iloc[0][
                                        "잔고수량"
                                    ]
                                )
                                평균단가 = int(
                                    dfAccStockInfo.query(condition_sync).iloc[0][
                                        "평균단가"
                                    ]
                                )
                            except IndexError:
                                continue

                            if v.수량 != 잔고수량 or v.매수가 != 평균단가:
                                v.수량 = 잔고수량
                                v.매수가 = 평균단가
                                r.포트폴리오종목동기화(v)
                                isAdd = True

            self.joinAccInfoToPortfolio(dfAccStockInfo)
            self.parent.PortfolioView()

            # [ZMQ] Publish Account Status per sync cycle
            try:
                publisher = get_shared_publisher()
                total_loss = int(self.dfAccSumInfo["평가손익"].values[0])
                is_emergency = False
                if hasattr(self.parent, "account_guard") and self.parent.account_guard:
                    is_emergency = self.parent.account_guard.is_emergency_mode

                pending_order_amt = 0
                if hasattr(self.parent, "gooses") and len(self.parent.gooses) > 0:
                    goose = self.parent.gooses[0]
                    if hasattr(goose, "coordinator") and goose.coordinator:
                        pending_order_amt = goose.coordinator._pending_order_amt

                account_info.update(
                    {
                        "is_emergency": is_emergency,
                        "total_loss": total_loss,
                        "주문가능금액부족": self.parent.주문가능금액부족,
                        "pending_order_amt": pending_order_amt,
                        "total_slot_cnt": investment_info.get("total_slot_cnt", 5),
                        "aigoose_slots": investment_info.get("aigoose_slots", 0),
                        "guardian_slots": investment_info.get("guardian_slots", 0),
                    }
                )
                # logger.info(f"[Account] Account Info: {account_info}")
                publisher.publish_data(
                    TOPIC_EVENT,
                    {
                        "event": "ACCOUNT_STATUS",
                        "account_info": account_info,
                        "timestamp": datetime.datetime.now().strftime(
                            "%Y-%m-%d %H:%M:%S"
                        ),
                    },
                )
            except Exception as ze:
                logger.error(f"ZMQ Account Status Publish failed: {ze}")

            current = datetime.datetime.now()
            if current.minute % 5 == 0 and current.minute != self.last_trigger_minute:
                for r in self.parent.gooses:
                    if r.running:
                        r.cleanup_old_entries()
                        r.executeUnadviseRealDatas()
                        r.check_goose_worker_status()
                        self.last_trigger_minute = current.minute

        except Exception as e:
            클래스이름 = self.__class__.__name__
            함수이름 = inspect.currentframe().f_code.co_name
            logger.error("%s-%s %s: %s" % (클래스이름, 함수이름, get_linenumber(), e))
            logger.exception(f"syncAccInfo 실행 중 예외 발생: {e}")
            ToTelegram(f"syncInfo 실행 중 오류 발생:\n{str(e)}")

    def joinAccInfoToPortfolio(self, dfAccStockInfo):
        for i in range(0, len(dfAccStockInfo)):
            종목번호 = dfAccStockInfo.iloc[i]["종목번호"]
            잔고수량 = int(dfAccStockInfo.iloc[i]["잔고수량"])
            매입금액 = int(dfAccStockInfo.iloc[i]["매입금액"])
            현재가 = int(dfAccStockInfo.iloc[i]["현재가"])
            평가금액 = float(dfAccStockInfo.iloc[i]["평가금액"])
            평가손익 = int(dfAccStockInfo.iloc[i]["평가손익"])
            수익율 = float(dfAccStockInfo.iloc[i]["수익율"])
            if hasattr(self.parent, "gooses"):
                for r in self.parent.gooses:
                    for k, v in r.portfolio.items():
                        if k == 종목번호:
                            v.잔고수량 = 잔고수량
                            v.매입금액 = 매입금액
                            v.현재가 = 현재가
                            v.평가금액 = 평가금액
                            v.평가손익 = 평가손익
                            v.수익율 = 수익율
