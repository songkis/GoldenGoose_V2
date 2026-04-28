# -*- coding: utf-8 -*-

import inspect
import logging
import os
import sys

import win32com.client

from util.CommUtils import safe_float


class XARealEvents(object):
    def __init__(self):
        self._owner = None
        self._real_callback = None
        self._logger = logging.getLogger(self.__class__.__name__)
        self._ignore_tr_codes = {"NWS", "SC1"}  # 콜백이 필요없는 TR 코드 목록

    def set_real_callback(self, callback):
        """실시간 데이터 수신 콜백 함수를 설정합니다."""
        if not callable(callback):
            self._logger.error("콜백 함수가 callable이 아닙니다.")
            return
        self._real_callback = callback

    def OnReceiveRealData(self, szTrCode):
        # [DEBUG] Verify ALL Low-Level Event Reception unconditionally
        self._logger.debug(f"⚡ [XARealEvents] OnReceiveRealData: {szTrCode}")

        try:
            if self._owner is None:
                self._logger.error(
                    f"OnReceiveRealData: _owner가 설정되지 않았습니다. (szTrCode: {szTrCode})"
                )
                return

            if self._real_callback:
                result = self._owner.parse()
                if result is None:
                    self._logger.error(f"데이터 파싱 실패 (szTrCode: {szTrCode})")
                    return
                self._real_callback(szTrCode, result)
            else:
                # self._logger.warning(f"콜백 함수가 설정되지 않았습니다. (szTrCode: {szTrCode})")

                # if szTrCode in self._ignore_tr_codes:
                # SC1, NWS는 자체적으로 처리
                self._owner.OnReceiveRealData(szTrCode)
                # else:
                #    pass

        except Exception as e:
            self._logger.error(f"OnReceiveRealData 처리 중 오류 발생: {str(e)}")

    def OnReceiveMessage(self, systemError, messageCode, message):
        self._logger.info(
            f"⚡ [XARealEvents] OnReceiveMessage: ErrCode={systemError}, MsgCode={messageCode}, Msg={message}"
        )

    def OnReceiveData(self, szTrCode):
        self._logger.info(f"⚡ [XARealEvents] OnReceiveData (Unexpected): {szTrCode}")


class XAReal(object):
    def __init__(self, parent=None, 식별자=""):
        self.MYNAME = self.__class__.__name__
        self._logger = logging.getLogger(self.MYNAME)
        self.parent = parent
        self.식별자 = 식별자
        try:
            self.event_handler_class = type(f"{self.MYNAME}Events", (XARealEvents,), {})

            self.ActiveX_disp = win32com.client.Dispatch("XA_DataSet.XAReal")

            # Keep Sink separate from COM Object
            self.ActiveX_sink = win32com.client.WithEvents(
                self.ActiveX_disp, self.event_handler_class
            )
            self.ActiveX_sink._owner = self

            self.ActiveX = self.ActiveX_disp

            self._logger.info(
                f"XAReal 초기화 완료: {self.MYNAME} (Early Binding Attempted)"
            )
            try:
                self._logger.info(f"COM Class: {self.ActiveX_disp.__class__}")
            except Exception:
                pass
            try:
                self._logger.info(f"COM Module: {self.ActiveX_disp.__module__}")
            except Exception:
                pass
        except Exception as e:
            self._logger.exception(f"XAReal 초기화 실패: {e}")
            raise

        self.onadvise = dict()
        self.INBLOCK = "InBlock"
        self.OUTBLOCK = "OutBlock"

        base_path = (
            sys._MEIPASS
            if hasattr(sys, "_MEIPASS")
            else os.path.dirname(os.path.abspath(__file__))
        )
        self.RESFILE = os.path.join(base_path, "res", f"{self.MYNAME}.res")

        if not os.path.exists(self.RESFILE):
            raise FileNotFoundError(f"리소스 파일 없음: {self.RESFILE}")

        ret = self.ActiveX.LoadFromResFile(self.RESFILE)
        self._logger.info(f"XAReal 리소스 로드 결과: {ret} ({self.RESFILE})")

    def __repr__(self):
        return f"<{self.MYNAME} 식별자={self.식별자}>"

    def AdviseRealData(self, 종목코드):
        if 종목코드 not in self.onadvise:
            self.onadvise[종목코드] = ""
            self.ActiveX.SetFieldData(self.INBLOCK, "shcode", 종목코드)
            self.ActiveX.AdviseRealData()

    def UnadviseRealDataWithKey(self, 종목코드):
        if 종목코드 in self.onadvise:
            self.onadvise.pop(종목코드)
        try:
            self.ActiveX.UnadviseRealDataWithKey(종목코드)
        except Exception as e:
            print(f"[{self.MYNAME}] {종목코드} 해제 중 예외 발생: {e}")

    def UnadviseRealData(self):
        self.onadvise.clear()
        self.ActiveX.UnadviseRealData()

    def parse(self):
        # raise NotImplementedError("자식 클래스에서 반드시 구현해야 합니다.")
        raise NotImplementedError(
            f"{self.__class__.__name__}.parse() must be implemented."
        )

    def AdviseLinkFromHTS(self):
        self.ActiveX.AdviseLinkFromHTS()

    def UnAdviseLinkFromHTS(self):
        self.ActiveX.UnAdviseLinkFromHTS()

    def OnReceiveLinkData(self, szLinkName, szData, szFiller):
        print(
            f"OnReceiveLinkData szLinkName, szData, szFiller: {szLinkName, szData, szFiller}"
        )

    def connectRealSignal(self, callback):
        """실시간 데이터 수신 콜백 함수를 연결합니다."""
        # Check against the Sink, not the COM object
        if not hasattr(self.ActiveX_sink, "_owner"):
            raise AttributeError(
                f"[{self.MYNAME}] 이벤트 핸들러가 초기화되지 않았습니다."
            )
        self.ActiveX_sink._owner = self
        self.ActiveX_sink.set_real_callback(callback)


# KOSDAQ체결
class K3_(XAReal):
    def __init__(self, parent=None, 식별자="식별자"):
        super().__init__(parent=parent, 식별자=식별자)

    def parse(self):
        result = {
            "체결시간": self.ActiveX.GetFieldData(self.OUTBLOCK, "chetime"),
            "전일대비구분": self.ActiveX.GetFieldData(self.OUTBLOCK, "sign"),
            "전일대비": int(self.ActiveX.GetFieldData(self.OUTBLOCK, "change")),
            "등락율": safe_float(self.ActiveX.GetFieldData(self.OUTBLOCK, "drate")),
            "현재가": int(self.ActiveX.GetFieldData(self.OUTBLOCK, "price")),
            "시가시간": self.ActiveX.GetFieldData(self.OUTBLOCK, "opentime"),
            "시가": int(self.ActiveX.GetFieldData(self.OUTBLOCK, "open")),
            "고가시간": self.ActiveX.GetFieldData(self.OUTBLOCK, "hightime"),
            "고가": int(self.ActiveX.GetFieldData(self.OUTBLOCK, "high")),
            "저가시간": self.ActiveX.GetFieldData(self.OUTBLOCK, "lowtime"),
            "저가": int(self.ActiveX.GetFieldData(self.OUTBLOCK, "low")),
            "체결구분": self.ActiveX.GetFieldData(self.OUTBLOCK, "cgubun"),
            "체결량": int(self.ActiveX.GetFieldData(self.OUTBLOCK, "cvolume")),
            "누적거래량": int(self.ActiveX.GetFieldData(self.OUTBLOCK, "volume")),
            "누적거래대금": int(self.ActiveX.GetFieldData(self.OUTBLOCK, "value")),
            "매도누적체결량": int(self.ActiveX.GetFieldData(self.OUTBLOCK, "mdvolume")),
            "매도누적체결건수": int(
                self.ActiveX.GetFieldData(self.OUTBLOCK, "mdchecnt")
            ),
            "매수누적체결량": int(self.ActiveX.GetFieldData(self.OUTBLOCK, "msvolume")),
            "매수누적체결건수": int(
                self.ActiveX.GetFieldData(self.OUTBLOCK, "mschecnt")
            ),
            "체결강도": safe_float(self.ActiveX.GetFieldData(self.OUTBLOCK, "cpower")),
            "가중평균가": int(self.ActiveX.GetFieldData(self.OUTBLOCK, "w_avrg")),
            "매도호가": int(self.ActiveX.GetFieldData(self.OUTBLOCK, "offerho")),
            "매수호가": int(self.ActiveX.GetFieldData(self.OUTBLOCK, "bidho")),
            "장정보": self.ActiveX.GetFieldData(self.OUTBLOCK, "status"),
            "전일동시간대거래량": int(
                self.ActiveX.GetFieldData(self.OUTBLOCK, "jnilvolume")
            ),
            "종목코드": self.ActiveX.GetFieldData(self.OUTBLOCK, "shcode"),
        }
        return result


# KOSPI체결
class S3_(XAReal):
    def __init__(self, parent=None, 식별자="식별자"):
        super().__init__(parent=parent, 식별자=식별자)

    def parse(self):
        result = {
            "체결시간": self.ActiveX.GetFieldData(self.OUTBLOCK, "chetime"),
            "전일대비구분": self.ActiveX.GetFieldData(self.OUTBLOCK, "sign"),
            "전일대비": int(self.ActiveX.GetFieldData(self.OUTBLOCK, "change")),
            "등락율": safe_float(self.ActiveX.GetFieldData(self.OUTBLOCK, "drate")),
            "현재가": int(self.ActiveX.GetFieldData(self.OUTBLOCK, "price")),
            "시가시간": self.ActiveX.GetFieldData(self.OUTBLOCK, "opentime"),
            "시가": int(self.ActiveX.GetFieldData(self.OUTBLOCK, "open")),
            "고가시간": self.ActiveX.GetFieldData(self.OUTBLOCK, "hightime"),
            "고가": int(self.ActiveX.GetFieldData(self.OUTBLOCK, "high")),
            "저가시간": self.ActiveX.GetFieldData(self.OUTBLOCK, "lowtime"),
            "저가": int(self.ActiveX.GetFieldData(self.OUTBLOCK, "low")),
            "체결구분": self.ActiveX.GetFieldData(self.OUTBLOCK, "cgubun"),
            "체결량": int(self.ActiveX.GetFieldData(self.OUTBLOCK, "cvolume")),
            "누적거래량": int(self.ActiveX.GetFieldData(self.OUTBLOCK, "volume")),
            "누적거래대금": int(self.ActiveX.GetFieldData(self.OUTBLOCK, "value")),
            "매도누적체결량": int(self.ActiveX.GetFieldData(self.OUTBLOCK, "mdvolume")),
            "매도누적체결건수": int(
                self.ActiveX.GetFieldData(self.OUTBLOCK, "mdchecnt")
            ),
            "매수누적체결량": int(self.ActiveX.GetFieldData(self.OUTBLOCK, "msvolume")),
            "매수누적체결건수": int(
                self.ActiveX.GetFieldData(self.OUTBLOCK, "mschecnt")
            ),
            "체결강도": safe_float(self.ActiveX.GetFieldData(self.OUTBLOCK, "cpower")),
            "가중평균가": int(self.ActiveX.GetFieldData(self.OUTBLOCK, "w_avrg")),
            "매도호가": int(self.ActiveX.GetFieldData(self.OUTBLOCK, "offerho")),
            "매수호가": int(self.ActiveX.GetFieldData(self.OUTBLOCK, "bidho")),
            "장정보": self.ActiveX.GetFieldData(self.OUTBLOCK, "status"),
            "전일동시간대거래량": int(
                self.ActiveX.GetFieldData(self.OUTBLOCK, "jnilvolume")
            ),
            "종목코드": self.ActiveX.GetFieldData(self.OUTBLOCK, "shcode"),
        }
        return result


# 주식주문체결
class SC1(XAReal):
    def __init__(self, parent=None, 식별자="식별자"):
        super().__init__(parent=parent, 식별자=식별자)
        # self.ActiveX.LoadFromResFile(self.RESFILE)
        # print('주식 주문 체결 SC1 self : %s, parent : %s ' % (self, self.parent))

    def AdviseRealData(self):
        self.ActiveX.AdviseRealData()

    def UnadviseRealData(self):
        self.ActiveX.UnadviseRealData()

    def OnReceiveRealData(self, szTrCode):
        result = self.parse()
        if self.parent is not None:
            self.parent.OnReceiveRealData(szTrCode, result)

    def parse(self):
        result = dict()

        result["종목명"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "Isunm")
        result["주문번호"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "ordno")
        result["체결번호"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "execno")
        result["주문수량"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "ordqty")
        result["주문가격"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "ordprc")
        result["체결수량"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "execqty")
        result["체결가격"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "execprc")
        result["종목코드"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "shtnIsuno")
        result["주문평균체결가격"] = self.ActiveX.GetFieldData(
            self.OUTBLOCK, "ordavrexecprc"
        )
        result["매매구분"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "bnstp")
        result["주문계좌번호"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "ordacntno")
        result["체결시각"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "exectime")
        return result


# 실시간 뉴스 제목 패킷(NWS)
class NWS(XAReal):
    def __init__(self, parent=None, 식별자="식별자"):
        super().__init__(parent=parent, 식별자=식별자)
        # self.ActiveX.LoadFromResFile(self.RESFILE)

    def AdviseRealData(self, 뉴스코드="NWS001"):
        self.ActiveX.SetFieldData(self.INBLOCK, "nwcode", 뉴스코드)
        self.ActiveX.AdviseRealData()

    def UnadviseRealData(self):
        self.ActiveX.UnadviseRealData()

    def OnReceiveRealData(self, szTrCode):
        result = self.parse()
        if self.parent is not None:
            self.parent.OnReceiveRealData(szTrCode, result)

    def parse(self):
        result = dict()
        result["날짜"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "date")
        result["시간"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "time")
        result["뉴스구분자"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "id")
        result["키값"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "realkey")
        result["제목"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "title")
        result["종목코드"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "code")
        result["BODY길이"] = int(self.ActiveX.GetFieldData(self.OUTBLOCK, "bodysize"))
        return result


# 선물주문체결, C01
class C01(XAReal):
    def __init__(self, parent=None, 식별자="식별자"):
        super().__init__(parent=parent, 식별자=식별자)
        # self.ActiveX.LoadFromResFile(self.RESFILE)

    def AdviseRealData(self):
        self.ActiveX.AdviseRealData()

    def UnadviseRealData(self):
        self.ActiveX.UnadviseRealData()

    def OnReceiveRealData(self, szTrCode):
        result = dict()
        result["라인일련번호"] = int(
            self.ActiveX.GetFieldData(self.OUTBLOCK, "lineseq")
        )
        result["계좌번호"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "accno")
        result["조작자ID"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "user")
        result["일련번호"] = int(self.ActiveX.GetFieldData(self.OUTBLOCK, "seq"))
        result["trcode"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "trcode")
        result["매칭그룹번호"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "megrpno")
        result["보드ID"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "boardid")
        result["회원번호"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "memberno")
        result["지점번호"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "bpno")
        result["주문번호"] = int(self.ActiveX.GetFieldData(self.OUTBLOCK, "ordno"))
        result["원주문번호"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "ordordno")
        result["종목코드"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "expcode")
        result["약정번호"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "yakseq")
        result["체결가격"] = float(self.ActiveX.GetFieldData(self.OUTBLOCK, "cheprice"))
        result["체결수량"] = int(self.ActiveX.GetFieldData(self.OUTBLOCK, "chevol"))
        result["세션ID"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "sessionid")
        result["체결일자"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "chedate")
        result["체결시각"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "chetime")
        result["최근월체결가격"] = float(
            self.ActiveX.GetFieldData(self.OUTBLOCK, "spdprc1")
        )
        result["차근월체결가격"] = float(
            self.ActiveX.GetFieldData(self.OUTBLOCK, "spdprc2")
        )
        result["매도수구분"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "dosugb")
        result["계좌번호1"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "accno1")
        result["시장조성호가구분"] = self.ActiveX.GetFieldData(
            self.OUTBLOCK, "sihogagb"
        )
        result["위탁사번호"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "jakino")
        result["대용주권계좌번호"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "daeyong")
        result["mem_filler"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "mem_filler")
        result["mem_accno"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "mem_accno")
        result["mem_filler1"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "mem_filler")

        if self.parent is not None:
            self.parent.OnReceiveRealData(szTrCode, result)


# KOSPI200선물체결(C0), FC0
class FC0(XAReal):
    def __init__(self, parent=None, 식별자="식별자"):
        super().__init__(parent=parent, 식별자=식별자)
        # self.ActiveX.LoadFromResFile(self.RESFILE)
        # self.onadvise = dict()

    def AdviseRealData(self, 종목코드):
        # 클래스이름 = self.__class__.__name__
        # 함수이름 = inspect.currentframe().f_code.co_name
        # print("ENTER : %s --> %s" %(클래스이름, 함수이름))

        if 종목코드 not in list(self.onadvise.keys()):
            self.onadvise[종목코드] = ""
            self.ActiveX.SetFieldData(self.INBLOCK, "futcode", 종목코드)
            self.ActiveX.AdviseRealData()

        # print(" EXIT : %s --> %s" % (클래스이름, 함수이름))

    def UnadviseRealDataWithKey(self, 종목코드):
        # 클래스이름 = self.__class__.__name__
        # 함수이름 = inspect.currentframe().f_code.co_name
        # print("ENTER : %s --> %s" %(클래스이름, 함수이름))

        self.onadvise.pop(종목코드, None)
        self.ActiveX.UnadviseRealDataWithKey(종목코드)

        # print(" EXIT : %s --> %s" % (클래스이름, 함수이름))

    def UnadviseRealData(self):
        # 클래스이름 = self.__class__.__name__
        # 함수이름 = inspect.currentframe().f_code.co_name
        # print("ENTER : %s --> %s" %(클래스이름, 함수이름))

        # self.onadvise = dict()
        self.ActiveX.UnadviseRealData()

        # print(" EXIT : %s --> %s" % (클래스이름, 함수이름))

    def OnReceiveRealData(self, szTrCode):
        # 클래스이름 = self.__class__.__name__
        # 함수이름 = inspect.currentframe().f_code.co_name
        # print("ENTER : %s --> %s" %(클래스이름, 함수이름))

        try:
            result = dict()
            result["체결시간"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "chetime")
            result["전일대비구분"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "sign")
            result["전일대비"] = float(
                self.ActiveX.GetFieldData(self.OUTBLOCK, "change")
            )
            result["등락율"] = float(self.ActiveX.GetFieldData(self.OUTBLOCK, "drate"))
            result["현재가"] = float(self.ActiveX.GetFieldData(self.OUTBLOCK, "price"))
            result["시가"] = float(self.ActiveX.GetFieldData(self.OUTBLOCK, "open"))
            result["고가"] = float(self.ActiveX.GetFieldData(self.OUTBLOCK, "high"))
            result["저가"] = float(self.ActiveX.GetFieldData(self.OUTBLOCK, "low"))
            result["체결구분"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "cgubun")
            result["체결량"] = int(self.ActiveX.GetFieldData(self.OUTBLOCK, "cvolume"))
            result["누적거래량"] = int(
                self.ActiveX.GetFieldData(self.OUTBLOCK, "volume")
            )
            result["누적거래대금"] = int(
                self.ActiveX.GetFieldData(self.OUTBLOCK, "value")
            )
            result["매도누적체결량"] = int(
                self.ActiveX.GetFieldData(self.OUTBLOCK, "mdvolume")
            )
            result["매도누적체결건수"] = int(
                self.ActiveX.GetFieldData(self.OUTBLOCK, "mdchecnt")
            )
            result["매수누적체결량"] = int(
                self.ActiveX.GetFieldData(self.OUTBLOCK, "msvolume")
            )
            result["매수누적체결건수"] = int(
                self.ActiveX.GetFieldData(self.OUTBLOCK, "mschecnt")
            )
            result["체결강도"] = float(
                self.ActiveX.GetFieldData(self.OUTBLOCK, "cpower")
            )
            result["매도호가1"] = float(
                self.ActiveX.GetFieldData(self.OUTBLOCK, "offerho1")
            )
            result["매수호가1"] = float(
                self.ActiveX.GetFieldData(self.OUTBLOCK, "bidho1")
            )
            result["미결제약정수량"] = int(
                self.ActiveX.GetFieldData(self.OUTBLOCK, "openyak")
            )
            result["KOSPI200지수"] = float(
                self.ActiveX.GetFieldData(self.OUTBLOCK, "k200jisu")
            )
            result["이론가"] = float(
                self.ActiveX.GetFieldData(self.OUTBLOCK, "theoryprice")
            )
            result["괴리율"] = float(self.ActiveX.GetFieldData(self.OUTBLOCK, "kasis"))
            result["시장BASIS"] = float(
                self.ActiveX.GetFieldData(self.OUTBLOCK, "sbasis")
            )
            result["이론BASIS"] = float(
                self.ActiveX.GetFieldData(self.OUTBLOCK, "ibasis")
            )
            result["미결제약정증감"] = int(
                self.ActiveX.GetFieldData(self.OUTBLOCK, "openyakcha")
            )
            result["장운영정보"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "jgubun")
            result["전일동시간대거래량"] = int(
                self.ActiveX.GetFieldData(self.OUTBLOCK, "jnilvolume")
            )
            result["종목코드"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "futcode")

            if self.parent is not None:
                self.parent.OnReceiveRealData(szTrCode, result)
        except Exception as e:
            클래스이름 = self.__class__.__name__
            함수이름 = inspect.currentframe().f_code.co_name
            print("%s-%s " % (클래스이름, 함수이름), e)

        # print(" EXIT : %s --> %s" % (클래스이름, 함수이름))


# KOSPI200옵션체결(C0), OC0
class H1_(XAReal):
    def __init__(self, parent=None, 식별자="식별자"):
        super().__init__(parent=parent, 식별자=식별자)
        # self.ActiveX.LoadFromResFile(self.RESFILE)

    def AdviseRealData(self, 종목코드):
        self.ActiveX.SetFieldData(self.INBLOCK, "shcode", 종목코드)
        self.ActiveX.AdviseRealData()

    def UnadviseRealDataWithKey(self, 종목코드):
        self.ActiveX.UnadviseRealDataWithKey(종목코드)

    def UnadviseRealData(self):
        self.ActiveX.UnadviseRealData()

    def parse(self):
        result = dict()
        result["호가시간"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "hotime")
        매도호가 = []
        매수호가 = []
        매도호가잔량 = []
        매수호가잔량 = []
        for i in range(1, 11):
            매도호가.append(self.ActiveX.GetFieldData(self.OUTBLOCK, "offerho%s" % i))
            매수호가.append(self.ActiveX.GetFieldData(self.OUTBLOCK, "bidho%s" % i))
            매도호가잔량.append(
                self.ActiveX.GetFieldData(self.OUTBLOCK, "offerrem%s" % i)
            )
            매수호가잔량.append(
                self.ActiveX.GetFieldData(self.OUTBLOCK, "bidrem%s" % i)
            )

        result["매도호가"] = 매도호가
        result["매수호가"] = 매수호가
        result["매도호가잔량"] = 매도호가잔량
        result["매수호가잔량"] = 매수호가잔량

        result["총매도호가잔량"] = self.ActiveX.GetFieldData(
            self.OUTBLOCK, "totofferrem"
        )
        result["총매수호가잔량"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "totbidrem")
        result["동시호가구분"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "donsigubun")
        result["종목코드"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "shcode")
        result["배분적용구분"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "alloc_gubun")
        return result


# KOSDAQ호가잔랑
class HA_(XAReal):
    def __init__(self, parent=None, 식별자="식별자"):
        super().__init__(parent=parent, 식별자=식별자)
        # self.ActiveX.LoadFromResFile(self.RESFILE)

    def AdviseRealData(self, 종목코드):
        self.ActiveX.SetFieldData(self.INBLOCK, "shcode", 종목코드)
        self.ActiveX.AdviseRealData()

    def UnadviseRealDataWithKey(self, 종목코드):
        self.ActiveX.UnadviseRealDataWithKey(종목코드)

    def UnadviseRealData(self):
        self.ActiveX.UnadviseRealData()

    def parse(self):
        result = dict()
        result["호가시간"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "hotime")
        매도호가 = []
        매수호가 = []
        매도호가잔량 = []
        매수호가잔량 = []
        for i in range(1, 11):
            매도호가.append(self.ActiveX.GetFieldData(self.OUTBLOCK, "offerho%s" % i))
            매수호가.append(self.ActiveX.GetFieldData(self.OUTBLOCK, "bidho%s" % i))
            매도호가잔량.append(
                self.ActiveX.GetFieldData(self.OUTBLOCK, "offerrem%s" % i)
            )
            매수호가잔량.append(
                self.ActiveX.GetFieldData(self.OUTBLOCK, "bidrem%s" % i)
            )

        result["매도호가"] = 매도호가
        result["매수호가"] = 매수호가
        result["매도호가잔량"] = 매도호가잔량
        result["매수호가잔량"] = 매수호가잔량

        result["총매도호가잔량"] = self.ActiveX.GetFieldData(
            self.OUTBLOCK, "totofferrem"
        )
        result["총매수호가잔량"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "totbidrem")
        result["동시호가구분"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "donsigubun")
        result["종목코드"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "shcode")
        result["배분적용구분"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "alloc_gubun")
        return result


# 주식주문접수
class SC0(XAReal):
    def __init__(self, parent=None, 식별자="식별자"):
        super().__init__(parent=parent, 식별자=식별자)
        # self.ActiveX.LoadFromResFile(self.RESFILE)

    def AdviseRealData(self):
        self.ActiveX.AdviseRealData()

    def UnadviseRealData(self):
        self.ActiveX.UnadviseRealData()

    def OnReceiveRealData(self, szTrCode):
        result = dict()

        if self.parent is not None:
            self.parent.OnReceiveRealData(szTrCode, result)


# 주식주문정정
class SC2(XAReal):
    def __init__(self, parent=None, 식별자="식별자"):
        super().__init__(parent=parent, 식별자=식별자)
        # self.ActiveX.LoadFromResFile(self.RESFILE)

    def AdviseRealData(self):
        self.ActiveX.AdviseRealData()

    def UnadviseRealData(self):
        self.ActiveX.UnadviseRealData()

    def OnReceiveRealData(self, szTrCode):
        result = dict()

        result["종목코드"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "shtnIsuno")
        result["주문평균체결가격"] = self.ActiveX.GetFieldData(
            self.OUTBLOCK, "ordavrexecprc"
        )
        result["매매구분"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "bnstp")
        result["주문계좌번호"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "ordacntno")
        result["체결시각"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "exectime")
        return result


# 주식주문취소
class SC3(XAReal):
    def __init__(self, parent=None, 식별자="식별자"):
        super().__init__(parent=parent, 식별자=식별자)
        # self.ActiveX.LoadFromResFile(self.RESFILE)

    def AdviseRealData(self):
        self.ActiveX.AdviseRealData()

    def UnadviseRealData(self):
        self.ActiveX.UnadviseRealData()

    def OnReceiveRealData(self, szTrCode):
        result = dict()
        if self.parent is not None:
            self.parent.OnReceiveRealData(szTrCode, result)


# 주식주문거부
class SC4(XAReal):
    def __init__(self, parent=None, 식별자="식별자"):
        super().__init__(parent=parent, 식별자=식별자)
        # self.ActiveX.LoadFromResFile(self.RESFILE)

    def AdviseRealData(self):
        self.ActiveX.AdviseRealData()

    def UnadviseRealData(self):
        self.ActiveX.UnadviseRealData()

    def OnReceiveRealData(self, szTrCode):
        result = dict()

        if self.parent is not None:
            self.parent.OnReceiveRealData(szTrCode, result)


# 해외선물


# 해외선물 현재가체결(OVC)
class OVC(XAReal):
    def __init__(self, parent=None, 식별자="식별자"):
        super().__init__(parent=parent, 식별자=식별자)
        # self.ActiveX.LoadFromResFile(self.RESFILE)

    def AdviseRealData(self, 종목코드):
        self.ActiveX.SetFieldData(self.INBLOCK, "symbol", 종목코드)
        self.ActiveX.AdviseRealData()

    def UnadviseRealDataWithKey(self, 종목코드):
        self.ActiveX.UnadviseRealDataWithKey(종목코드)

    def UnadviseRealData(self):
        self.ActiveX.UnadviseRealData()

    def OnReceiveRealData(self, szTrCode):
        result = dict()
        result["종목코드"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "symbol")
        result["체결일자_현지"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "ovsdate")
        result["체결시간_현지"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "trdtm")
        result["체결가격"] = float(self.ActiveX.GetFieldData(self.OUTBLOCK, "curpr"))
        result["시가"] = float(self.ActiveX.GetFieldData(self.OUTBLOCK, "open"))
        result["고가"] = float(self.ActiveX.GetFieldData(self.OUTBLOCK, "high"))
        result["저가"] = float(self.ActiveX.GetFieldData(self.OUTBLOCK, "low"))
        result["건별체결수량"] = int(self.ActiveX.GetFieldData(self.OUTBLOCK, "trdq"))

        if self.parent is not None:
            self.parent.OnReceiveRealData(szTrCode, result)


# 해외선물 호가(OVH)
class TC2(XAReal):
    def __init__(self, parent=None, 식별자="식별자"):
        super().__init__(parent=parent, 식별자=식별자)
        # self.ActiveX.LoadFromResFile(self.RESFILE)

    def AdviseRealData(self):
        self.ActiveX.AdviseRealData()

    def UnadviseRealData(self):
        self.ActiveX.UnadviseRealData()

    def OnReceiveRealData(self, szTrCode):
        result = dict()
        result["라인일련번호"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "lineseq")
        result["KEY"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "key")
        result["조작자ID"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "user")
        result["서비스ID"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "svc_id")
        result["주문일자"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "ordr_dt")
        result["지점번호"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "brn_cd")
        result["주문번호"] = int(self.ActiveX.GetFieldData(self.OUTBLOCK, "ordr_no"))
        result["원주문번호"] = int(
            self.ActiveX.GetFieldData(self.OUTBLOCK, "orgn_ordr_no")
        )
        result["모주문번호"] = int(
            self.ActiveX.GetFieldData(self.OUTBLOCK, "mthr_ordr_no")
        )
        result["계좌번호"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "ac_no")
        result["종목코드"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "is_cd")
        result["매도매수유형"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "s_b_ccd")
        result["정정취소유형"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "ordr_ccd")
        result["주문유형코드"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "ordr_typ_cd")
        result["주문기간코드"] = self.ActiveX.GetFieldData(
            self.OUTBLOCK, "ordr_typ_prd_ccd"
        )
        result["주문적용시작일자"] = self.ActiveX.GetFieldData(
            self.OUTBLOCK, "ordr_aplc_strt_dt"
        )
        result["주문적용종료일자"] = self.ActiveX.GetFieldData(
            self.OUTBLOCK, "ordr_aplc_end_dt"
        )
        result["주문가격"] = float(self.ActiveX.GetFieldData(self.OUTBLOCK, "ordr_prc"))
        result["주문조건가격"] = float(
            self.ActiveX.GetFieldData(self.OUTBLOCK, "cndt_ordr_prc")
        )
        result["주문수량"] = int(self.ActiveX.GetFieldData(self.OUTBLOCK, "ordr_q"))
        result["주문시간"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "ordr_tm")
        result["호가확인수량"] = int(self.ActiveX.GetFieldData(self.OUTBLOCK, "cnfr_q"))
        result["호가거부사유코드"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "rfsl_cd")
        result["호가거부사유코드명"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "text")

        if self.parent is not None:
            self.parent.OnReceiveRealData(szTrCode, result)


# 해외선물체결
class WOC(XAReal):
    def __init__(self, parent=None, 식별자="식별자"):
        super().__init__(parent=parent, 식별자=식별자)
        # self.ActiveX.LoadFromResFile(self.RESFILE)

    def AdviseRealData(self, 종목코드):
        self.ActiveX.SetFieldData(self.INBLOCK, "symbol", 종목코드)
        self.ActiveX.AdviseRealData()

    def UnadviseRealDataWithKey(self, 종목코드):
        self.ActiveX.UnadviseRealDataWithKey(종목코드)

    def UnadviseRealData(self):
        self.ActiveX.UnadviseRealData()

    def OnReceiveRealData(self, szTrCode):
        result = dict()
        result["종목코드"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "symbol")
        result["체결일자_현지"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "ovsdate")
        result["체결일자_한국"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "kordate")
        result["체결시간_현지"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "trdtm")
        result["체결시간_한국"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "kortm")
        result["체결가격"] = float(self.ActiveX.GetFieldData(self.OUTBLOCK, "curpr"))
        result["전일대비"] = float(self.ActiveX.GetFieldData(self.OUTBLOCK, "ydiffpr"))
        result["전일대비기호"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "ydiffSign")
        result["시가"] = float(self.ActiveX.GetFieldData(self.OUTBLOCK, "open"))
        result["고가"] = float(self.ActiveX.GetFieldData(self.OUTBLOCK, "high"))
        result["저가"] = float(self.ActiveX.GetFieldData(self.OUTBLOCK, "low"))
        result["등락율"] = float(self.ActiveX.GetFieldData(self.OUTBLOCK, "chgrate"))
        result["건별체결수량"] = int(self.ActiveX.GetFieldData(self.OUTBLOCK, "trdq"))
        result["누적체결수량"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "totq")
        result["체결구분"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "cgubun")
        result["매도누적체결수량"] = self.ActiveX.GetFieldData(
            self.OUTBLOCK, "mdvolume"
        )
        result["매수누적체결수량"] = self.ActiveX.GetFieldData(
            self.OUTBLOCK, "msvolume"
        )
        result["장마감일"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "ovsmkend")

        if self.parent is not None:
            self.parent.OnReceiveRealData(szTrCode, result)


# 해외옵션 호가(WOH)
