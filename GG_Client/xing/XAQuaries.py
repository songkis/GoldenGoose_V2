# -*- coding: utf-8 -*-

import datetime as dt
import os
import sys
import time

import win32com.client
from pandas import DataFrame

from config.comm_settings import RES_DIR_NM
from util.CommUtils import get_linenumber, get_funcname


class XAQueryEvents(object):
    def __init__(self):
        self.parent = None

    def set_parent(self, parent):
        self.parent = parent

    def OnReceiveMessage(self, systemError, messageCode, message):
        if self.parent is not None:
            self.parent.OnReceiveMessage(systemError, messageCode, message)

    def OnReceiveData(self, szTrCode):
        # import logging

        # logger = logging.getLogger("XAQueryEvents")
        # logger.info(f"[XAQueryEvents] OnReceiveData triggered for: {szTrCode}")
        if self.parent is not None:
            self.parent.OnReceiveData(szTrCode)

    def OnReceiveChartRealData(self, szTrCode):
        if self.parent is not None:
            self.parent.OnReceiveChartRealData(szTrCode)

    def OnReceiveSearchRealData(self, szTrCode):
        if self.parent is not None:
            self.parent.OnReceiveSearchRealData(szTrCode)


class XAQuery(object):
    TIMER = {}
    # TIMER_RESTRICT = {}

    def __init__(self, parent=None, 식별자="식별자"):
        self.parent = parent
        self.식별자 = 식별자

        self.ActiveX = win32com.client.DispatchWithEvents(
            "XA_DataSet.XAQuery", XAQueryEvents
        )
        self.ActiveX.set_parent(parent=self)

        # pathname = os.path.dirname(sys.argv[0])
        # self.RESDIR = os.path.abspath(pathname)

        self.MYNAME = self.__class__.__name__
        self.INBLOCK = "%sInBlock" % self.MYNAME
        self.INBLOCK1 = "%sInBlock1" % self.MYNAME
        self.OUTBLOCK = "%sOutBlock" % self.MYNAME
        self.OUTBLOCK1 = "%sOutBlock1" % self.MYNAME
        self.OUTBLOCK2 = "%sOutBlock2" % self.MYNAME
        self.OUTBLOCK3 = "%sOutBlock3" % self.MYNAME
        # self.RESFILE = "%s\\res\\%s.res" % (self.RESDIR, self.MYNAME)
        # 기본 실행 경로 설정
        if hasattr(sys, "_MEIPASS"):
            # PyInstaller EXE 실행 환경
            base_path = sys._MEIPASS
        else:
            # 개발 환경
            base_path = os.path.dirname(os.path.abspath(__file__))

        # Res 디렉토리 및 .res 파일 경로 설정
        RES_DIR = os.path.join(base_path, RES_DIR_NM)
        self.RESFILE = os.path.join(RES_DIR, f"{self.MYNAME}.res")

    def QueryWaiting(self, howlong=100.0, limit_count=None, limit_seconds=600):
        NOW = dt.datetime.now()
        DELTA = dt.timedelta(milliseconds=howlong)
        result = XAQuery.TIMER.get(self.MYNAME, None)

        if result is None:
            XAQuery.TIMER[self.MYNAME] = (
                NOW + DELTA
            )  # 이 시간이 지나야 다음 쿼리가 가능

        else:
            if NOW > result:
                XAQuery.TIMER[self.MYNAME] = NOW + DELTA

            else:
                diff = (result - NOW).total_seconds()
                time.sleep(diff)

                NOW = dt.datetime.now()
                XAQuery.TIMER[self.MYNAME] = NOW + DELTA

    def toint(self, s):
        temp = s.strip()
        result = 0

        if temp not in ["-"]:
            result = int(temp)
        else:
            result = 0

        return result

    def tofloat(self, s):
        temp = s.strip()
        result = 0

        if temp not in ["-"]:
            result = float(temp)
        else:
            result = 0.0

        return result

    def OnReceiveMessage(self, systemError, messageCode, message):
        if self.parent is not None:
            self.parent.OnReceiveMessage(systemError, messageCode, message)

    def OnReceiveData(self, szTrCode):
        pass

    def OnReceiveChartRealData(self, szTrCode):
        pass

    def RequestLinkToHTS(self, szLinkName, szData, szFiller):
        return self.ActiveX.RequestLinkToHTS(szLinkName, szData, szFiller)


# 주식


# 주식현재가(호가)조회
class t1101(XAQuery):
    def Query(self, 종목코드):
        self.QueryWaiting(howlong=20.0, limit_count=None, limit_seconds=600)
        self.ActiveX.LoadFromResFile(self.RESFILE)
        self.ActiveX.SetFieldData(self.INBLOCK, "shcode", 0, 종목코드)
        self.ActiveX.Request(0)

    def OnReceiveData(self, szTrCode):
        result = dict()

        # 매도/매수 1호가 및 잔량 추출
        result["offerho1"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "offerho1", 0)
        result["bidho1"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "bidho1", 0)
        result["offerrem1"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "offerrem1", 0)
        result["bidrem1"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "bidrem1", 0)

        if self.parent is not None:
            self.parent.OnReceiveData(self.MYNAME, result)


class t1102(XAQuery):
    def Query(self, 종목코드):
        self.QueryWaiting(howlong=20.0, limit_count=None, limit_seconds=600)
        self.ActiveX.LoadFromResFile(self.RESFILE)
        self.ActiveX.SetFieldData(self.INBLOCK, "shcode", 0, 종목코드)
        self.ActiveX.Request(0)

    def OnReceiveData(self, szTrCode):
        result = dict()

        result["한글명"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "hname", 0)
        result["현재가"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "price", 0)
        result["전일대비구분"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "sign", 0)
        result["전일대비"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "change", 0)
        result["등락율"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "diff", 0)
        result["누적거래량"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "volume", 0)
        result["기준가_평가가격"] = self.ActiveX.GetFieldData(
            self.OUTBLOCK, "recprice", 0
        )
        result["가중평균"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "avg", 0)
        result["상한가_최고호가가격"] = self.ActiveX.GetFieldData(
            self.OUTBLOCK, "uplmtprice", 0
        )
        result["하한가_최저호가가격"] = self.ActiveX.GetFieldData(
            self.OUTBLOCK, "dnlmtprice", 0
        )
        result["전일거래량"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "jnilvolume", 0)
        result["거래량차"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "volumediff", 0)
        result["시가"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "open", 0)
        result["시가시간"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "opentime", 0)
        result["고가"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "high", 0)
        result["고가시간"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "hightime", 0)
        result["저가"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "low", 0)
        result["저가시간"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "lowtime", 0)
        result["최고가_52"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "high52w", 0)
        result["최고가일_52"] = self.ActiveX.GetFieldData(
            self.OUTBLOCK, "high52wdate", 0
        )
        result["최저가_52"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "low52w", 0)
        result["최저가일_52"] = self.ActiveX.GetFieldData(
            self.OUTBLOCK, "low52wdate", 0
        )
        result["소진율"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "exhratio", 0)
        result["PER"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "per", 0)
        result["PBRX"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "pbrx", 0)
        result["상장주식수_천"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "listing", 0)
        result["증거금율"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "jkrate", 0)
        result["수량단위"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "memedan", 0)
        result["매도증권사코드1"] = self.ActiveX.GetFieldData(
            self.OUTBLOCK, "offernocd1", 0
        )
        result["매수증권사코드1"] = self.ActiveX.GetFieldData(
            self.OUTBLOCK, "bidnocd1", 0
        )
        result["매도증권사명1"] = self.ActiveX.GetFieldData(
            self.OUTBLOCK, "offerno1", 0
        )
        result["매수증권사명1"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "bidno1", 0)
        result["총매도수량1"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "dvol1", 0)
        result["총매수수량1"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "svol1", 0)
        result["매도증감1"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "dcha1", 0)
        result["매수증감1"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "scha1", 0)
        result["매도비율1"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "ddiff1", 0)
        result["매수비율1"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "sdiff1", 0)
        result["매도증권사코드2"] = self.ActiveX.GetFieldData(
            self.OUTBLOCK, "offernocd2", 0
        )
        result["매수증권사코드2"] = self.ActiveX.GetFieldData(
            self.OUTBLOCK, "bidnocd2", 0
        )
        result["매도증권사명2"] = self.ActiveX.GetFieldData(
            self.OUTBLOCK, "offerno2", 0
        )
        result["매수증권사명2"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "bidno2", 0)
        result["총매도수량2"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "dvol2", 0)
        result["총매수수량2"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "svol2", 0)
        result["매도증감2"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "dcha2", 0)
        result["매수증감2"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "scha2", 0)
        result["매도비율2"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "ddiff2", 0)
        result["매수비율2"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "sdiff2", 0)
        result["매도증권사코드3"] = self.ActiveX.GetFieldData(
            self.OUTBLOCK, "offernocd3", 0
        )
        result["매수증권사코드3"] = self.ActiveX.GetFieldData(
            self.OUTBLOCK, "bidnocd3", 0
        )
        result["매도증권사명3"] = self.ActiveX.GetFieldData(
            self.OUTBLOCK, "offerno3", 0
        )
        result["매수증권사명3"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "bidno3", 0)
        result["총매도수량3"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "dvol3", 0)
        result["총매수수량3"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "svol3", 0)
        result["매도증감3"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "dcha3", 0)
        result["매수증감3"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "scha3", 0)
        result["매도비율3"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "ddiff3", 0)
        result["매수비율3"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "sdiff3", 0)
        result["매도증권사코드4"] = self.ActiveX.GetFieldData(
            self.OUTBLOCK, "offernocd4", 0
        )
        result["매수증권사코드4"] = self.ActiveX.GetFieldData(
            self.OUTBLOCK, "bidnocd4", 0
        )
        result["매도증권사명4"] = self.ActiveX.GetFieldData(
            self.OUTBLOCK, "offerno4", 0
        )
        result["매수증권사명4"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "bidno4", 0)
        result["총매도수량4"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "dvol4", 0)
        result["총매수수량4"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "svol4", 0)
        result["매도증감4"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "dcha4", 0)
        result["매수증감4"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "scha4", 0)
        result["매도비율4"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "ddiff4", 0)
        result["매수비율4"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "sdiff4", 0)
        result["매도증권사코드5"] = self.ActiveX.GetFieldData(
            self.OUTBLOCK, "offernocd5", 0
        )
        result["매수증권사코드5"] = self.ActiveX.GetFieldData(
            self.OUTBLOCK, "bidnocd5", 0
        )
        result["매도증권사명5"] = self.ActiveX.GetFieldData(
            self.OUTBLOCK, "offerno5", 0
        )
        result["매수증권사명5"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "bidno5", 0)
        result["총매도수량5"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "dvol5", 0)
        result["총매수수량5"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "svol5", 0)
        result["매도증감5"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "dcha5", 0)
        result["매수증감5"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "scha5", 0)
        result["매도비율5"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "ddiff5", 0)
        result["매수비율5"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "sdiff5", 0)
        result["외국계매도합계수량"] = self.ActiveX.GetFieldData(
            self.OUTBLOCK, "fwdvl", 0
        )
        result["외국계매도직전대비"] = self.ActiveX.GetFieldData(
            self.OUTBLOCK, "ftradmdcha", 0
        )
        result["외국계매도비율"] = self.ActiveX.GetFieldData(
            self.OUTBLOCK, "ftradmddiff", 0
        )
        result["외국계매수합계수량"] = self.ActiveX.GetFieldData(
            self.OUTBLOCK, "fwsvl", 0
        )
        result["외국계매수직전대비"] = self.ActiveX.GetFieldData(
            self.OUTBLOCK, "ftradmscha", 0
        )
        result["외국계매수비율"] = self.ActiveX.GetFieldData(
            self.OUTBLOCK, "ftradmsdiff", 0
        )
        result["회전율"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "vol", 0)
        result["종목코드"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "shcode", 0)
        result["누적거래대금"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "value", 0)
        result["전일동시간거래량"] = self.ActiveX.GetFieldData(
            self.OUTBLOCK, "jvolume", 0
        )
        result["연중최고가"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "highyear", 0)
        result["연중최고일자"] = self.ActiveX.GetFieldData(
            self.OUTBLOCK, "highyeardate", 0
        )
        result["연중최저가"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "lowyear", 0)
        result["연중최저일자"] = self.ActiveX.GetFieldData(
            self.OUTBLOCK, "lowyeardate", 0
        )
        result["목표가"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "target", 0)
        result["자본금"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "capital", 0)
        result["유동주식수"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "abscnt", 0)
        result["액면가"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "parprice", 0)
        result["결산월"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "gsmm", 0)
        result["대용가"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "subprice", 0)
        result["시가총액"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "total", 0)
        result["상장일"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "listdate", 0)
        result["전분기명"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "name", 0)
        result["전분기매출액"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "bfsales", 0)
        result["전분기영업이익"] = self.ActiveX.GetFieldData(
            self.OUTBLOCK, "bfoperatingincome", 0
        )
        result["전분기경상이익"] = self.ActiveX.GetFieldData(
            self.OUTBLOCK, "bfordinaryincome", 0
        )
        result["전분기순이익"] = self.ActiveX.GetFieldData(
            self.OUTBLOCK, "bfnetincome", 0
        )
        result["전분기EPS"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "bfeps", 0)
        result["전전분기명"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "name2", 0)
        result["전전분기매출액"] = self.ActiveX.GetFieldData(
            self.OUTBLOCK, "bfsales2", 0
        )
        result["전전분기영업이익"] = self.ActiveX.GetFieldData(
            self.OUTBLOCK, "bfoperatingincome2", 0
        )
        result["전전분기경상이익"] = self.ActiveX.GetFieldData(
            self.OUTBLOCK, "bfordinaryincome2", 0
        )
        result["전전분기순이익"] = self.ActiveX.GetFieldData(
            self.OUTBLOCK, "bfnetincome2", 0
        )
        result["전전분기EPS"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "bfeps2", 0)
        result["전년대비매출액"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "salert", 0)
        result["전년대비영업이익"] = self.ActiveX.GetFieldData(
            self.OUTBLOCK, "opert", 0
        )
        result["전년대비경상이익"] = self.ActiveX.GetFieldData(
            self.OUTBLOCK, "ordrt", 0
        )
        result["전년대비순이익"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "netrt", 0)
        result["전년대비EPS"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "epsrt", 0)
        result["락구분"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "info1", 0)
        result["관리_급등구분"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "info2", 0)
        result["정지_연장구분"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "info3", 0)
        result["투자_불성실구분"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "info4", 0)
        result["장구분"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "janginfo", 0)
        result["TPER"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "t_per", 0)
        result["통화ISO코드"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "tonghwa", 0)
        result["총매도대금1"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "dval1", 0)
        result["총매수대금1"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "sval1", 0)
        result["총매도대금2"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "dval2", 0)
        result["총매수대금2"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "sval2", 0)
        result["총매도대금3"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "dval3", 0)
        result["총매수대금3"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "sval3", 0)
        result["총매도대금4"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "dval4", 0)
        result["총매수대금4"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "sval4", 0)
        result["총매도대금5"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "dval5", 0)
        result["총매수대금5"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "sval5", 0)
        result["총매도평단가1"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "davg1", 0)
        result["총매수평단가1"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "savg1", 0)
        result["총매도평단가2"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "davg2", 0)
        result["총매수평단가2"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "savg2", 0)
        result["총매도평단가3"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "davg3", 0)
        result["총매수평단가3"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "savg3", 0)
        result["총매도평단가4"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "davg4", 0)
        result["총매수평단가4"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "savg4", 0)
        result["총매도평단가5"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "davg5", 0)
        result["총매수평단가5"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "savg5", 0)
        result["외국계매도대금"] = self.ActiveX.GetFieldData(
            self.OUTBLOCK, "ftradmdval", 0
        )
        result["외국계매수대금"] = self.ActiveX.GetFieldData(
            self.OUTBLOCK, "ftradmsval", 0
        )
        result["외국계매도평단가"] = self.ActiveX.GetFieldData(
            self.OUTBLOCK, "ftradmdvag", 0
        )
        result["외국계매수평단가"] = self.ActiveX.GetFieldData(
            self.OUTBLOCK, "ftradmsvag", 0
        )
        result["투자주의환기"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "info5", 0)
        result["기업인수목적회사여부"] = self.ActiveX.GetFieldData(
            self.OUTBLOCK, "spac_gubun", 0
        )
        result["발행가격"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "issueprice", 0)
        result["배분적용구분코드"] = self.ActiveX.GetFieldData(
            self.OUTBLOCK, "alloc_gubun", 0
        )
        result["배분적용구분"] = self.ActiveX.GetFieldData(
            self.OUTBLOCK, "alloc_text", 0
        )
        result["단기과열_VI발동"] = self.ActiveX.GetFieldData(
            self.OUTBLOCK, "shterm_text", 0
        )
        result["정적VI상한가"] = self.ActiveX.GetFieldData(
            self.OUTBLOCK, "svi_uplmtprice", 0
        )
        result["정적VI하한가"] = self.ActiveX.GetFieldData(
            self.OUTBLOCK, "svi_dnlmtprice", 0
        )
        result["저유동성종목여부"] = self.ActiveX.GetFieldData(
            self.OUTBLOCK, "low_lqdt_gu", 0
        )
        result["이상급등종목여부"] = self.ActiveX.GetFieldData(
            self.OUTBLOCK, "abnormal_rise_gu", 0
        )
        result["대차불가표시"] = self.ActiveX.GetFieldData(
            self.OUTBLOCK, "lend_text", 0
        )

        if self.parent is not None:
            self.parent.OnReceiveData(szTrCode, [result])


# 현물정상주문
class CSPAT00600(XAQuery):
    def Query(
        self,
        계좌번호="",
        입력비밀번호="",
        종목번호="",
        주문수량="",
        주문가="",
        매매구분="2",
        호가유형코드="00",
        신용거래코드="000",
        대출일="",
        주문조건구분="0",
    ):
        print("CSPAT00600 Query")
        self.QueryWaiting(howlong=4.0, limit_count=None, limit_seconds=600)

        self.주문결과코드 = ""
        self.주문결과메세지 = ""

        if 호가유형코드 == "03":
            주문가 = ""

        self.ActiveX.LoadFromResFile(self.RESFILE)
        self.ActiveX.SetFieldData(self.INBLOCK1, "AcntNo", 0, 계좌번호)
        self.ActiveX.SetFieldData(self.INBLOCK1, "InptPwd", 0, 입력비밀번호)
        self.ActiveX.SetFieldData(self.INBLOCK1, "IsuNo", 0, 종목번호)
        self.ActiveX.SetFieldData(self.INBLOCK1, "OrdQty", 0, 주문수량)
        self.ActiveX.SetFieldData(self.INBLOCK1, "OrdPrc", 0, 주문가)
        self.ActiveX.SetFieldData(self.INBLOCK1, "BnsTpCode", 0, 매매구분)
        self.ActiveX.SetFieldData(self.INBLOCK1, "OrdprcPtnCode", 0, 호가유형코드)
        self.ActiveX.SetFieldData(self.INBLOCK1, "MgntrnCode", 0, 신용거래코드)
        self.ActiveX.SetFieldData(self.INBLOCK1, "LoanDt", 0, 대출일)
        self.ActiveX.SetFieldData(self.INBLOCK1, "OrdCndiTpCode", 0, 주문조건구분)
        return self.ActiveX.Request(0)

    def OnReceiveData(self, szTrCode):
        result = []
        nCount = self.ActiveX.GetBlockCount(self.OUTBLOCK1)
        for i in range(nCount):
            레코드갯수 = int(
                self.ActiveX.GetFieldData(self.OUTBLOCK1, "RecCnt", i).strip()
            )
            계좌번호 = self.ActiveX.GetFieldData(self.OUTBLOCK1, "AcntNo", i).strip()
            입력비밀번호 = self.ActiveX.GetFieldData(
                self.OUTBLOCK1, "InptPwd", i
            ).strip()
            종목번호 = self.ActiveX.GetFieldData(self.OUTBLOCK1, "IsuNo", i).strip()
            주문수량 = int(
                self.ActiveX.GetFieldData(self.OUTBLOCK1, "OrdQty", i).strip()
            )
            주문가 = self.ActiveX.GetFieldData(self.OUTBLOCK1, "OrdPrc", i).strip()
            매매구분 = self.ActiveX.GetFieldData(self.OUTBLOCK1, "BnsTpCode", i).strip()
            호가유형코드 = self.ActiveX.GetFieldData(
                self.OUTBLOCK1, "OrdprcPtnCode", i
            ).strip()
            프로그램호가유형코드 = self.ActiveX.GetFieldData(
                self.OUTBLOCK1, "PrgmOrdprcPtnCode", i
            ).strip()
            공매도가능여부 = self.ActiveX.GetFieldData(
                self.OUTBLOCK1, "StslAbleYn", i
            ).strip()
            공매도호가구분 = self.ActiveX.GetFieldData(
                self.OUTBLOCK1, "StslOrdprcTpCode", i
            ).strip()
            통신매체코드 = self.ActiveX.GetFieldData(
                self.OUTBLOCK1, "CommdaCode", i
            ).strip()
            신용거래코드 = self.ActiveX.GetFieldData(
                self.OUTBLOCK1, "MgntrnCode", i
            ).strip()
            대출일 = self.ActiveX.GetFieldData(self.OUTBLOCK1, "LoanDt", i).strip()
            회원번호 = self.ActiveX.GetFieldData(self.OUTBLOCK1, "MbrNo", i).strip()
            주문조건구분 = self.ActiveX.GetFieldData(
                self.OUTBLOCK1, "OrdCndiTpCode", i
            ).strip()
            전략코드 = self.ActiveX.GetFieldData(self.OUTBLOCK1, "StrtgCode", i).strip()
            그룹ID = self.ActiveX.GetFieldData(self.OUTBLOCK1, "GrpId", i).strip()
            주문회차 = int(
                self.ActiveX.GetFieldData(self.OUTBLOCK1, "OrdSeqNo", i).strip()
            )
            포트폴리오번호 = int(
                self.ActiveX.GetFieldData(self.OUTBLOCK1, "PtflNo", i).strip()
            )
            바스켓번호 = int(
                self.ActiveX.GetFieldData(self.OUTBLOCK1, "BskNo", i).strip()
            )
            트렌치번호 = int(
                self.ActiveX.GetFieldData(self.OUTBLOCK1, "TrchNo", i).strip()
            )
            아이템번호 = int(
                self.ActiveX.GetFieldData(self.OUTBLOCK1, "ItemNo", i).strip()
            )
            운용지시번호 = self.ActiveX.GetFieldData(
                self.OUTBLOCK1, "OpDrtnNo", i
            ).strip()
            유동성공급자여부 = self.ActiveX.GetFieldData(
                self.OUTBLOCK1, "LpYn", i
            ).strip()
            반대매매구분 = self.ActiveX.GetFieldData(
                self.OUTBLOCK1, "CvrgTpCode", i
            ).strip()

            lst = [
                레코드갯수,
                계좌번호,
                입력비밀번호,
                종목번호,
                주문수량,
                주문가,
                매매구분,
                호가유형코드,
                프로그램호가유형코드,
                공매도가능여부,
                공매도호가구분,
                통신매체코드,
                신용거래코드,
                대출일,
                회원번호,
                주문조건구분,
                전략코드,
                그룹ID,
                주문회차,
                포트폴리오번호,
                바스켓번호,
                트렌치번호,
                아이템번호,
                운용지시번호,
                유동성공급자여부,
                반대매매구분,
            ]
            result.append(lst)

        columns = [
            "레코드갯수",
            "계좌번호",
            "입력비밀번호",
            "종목번호",
            "주문수량",
            "주문가",
            "매매구분",
            "호가유형코드",
            "프로그램호가유형코드",
            "공매도가능여부",
            "공매도호가구분",
            "통신매체코드",
            "신용거래코드",
            "대출일",
            "회원번호",
            "주문조건구분",
            "전략코드",
            "그룹ID",
            "주문회차",
            "포트폴리오번호",
            "바스켓번호",
            "트렌치번호",
            "아이템번호",
            "운용지시번호",
            "유동성공급자여부",
            "반대매매구분",
        ]
        df = DataFrame(data=result, columns=columns)

        result = []
        nCount = self.ActiveX.GetBlockCount(self.OUTBLOCK2)
        for i in range(nCount):
            레코드갯수 = int(
                self.ActiveX.GetFieldData(self.OUTBLOCK2, "RecCnt", i).strip()
            )
            주문번호 = int(
                self.ActiveX.GetFieldData(self.OUTBLOCK2, "OrdNo", i).strip()
            )
            주문시각 = self.ActiveX.GetFieldData(self.OUTBLOCK2, "OrdTime", i).strip()
            주문시장코드 = self.ActiveX.GetFieldData(
                self.OUTBLOCK2, "OrdMktCode", i
            ).strip()
            주문유형코드 = self.ActiveX.GetFieldData(
                self.OUTBLOCK2, "OrdPtnCode", i
            ).strip()
            종목코드 = self.ActiveX.GetFieldData(self.OUTBLOCK2, "ShtnIsuNo", i).strip()
            관리사원번호 = self.ActiveX.GetFieldData(
                self.OUTBLOCK2, "MgempNo", i
            ).strip()
            주문금액 = int(
                self.ActiveX.GetFieldData(self.OUTBLOCK2, "OrdAmt", i).strip()
            )
            예비주문번호 = int(
                self.ActiveX.GetFieldData(self.OUTBLOCK2, "SpareOrdNo", i).strip()
            )
            반대매매일련번호 = int(
                self.ActiveX.GetFieldData(self.OUTBLOCK2, "CvrgSeqno", i).strip()
            )
            예약주문번호 = int(
                self.ActiveX.GetFieldData(self.OUTBLOCK2, "RsvOrdNo", i).strip()
            )
            실물주문수량 = int(
                self.ActiveX.GetFieldData(self.OUTBLOCK2, "SpotOrdQty", i).strip()
            )
            재사용주문수량 = int(
                self.ActiveX.GetFieldData(self.OUTBLOCK2, "RuseOrdQty", i).strip()
            )
            현금주문금액 = int(
                self.ActiveX.GetFieldData(self.OUTBLOCK2, "MnyOrdAmt", i).strip()
            )
            대용주문금액 = int(
                self.ActiveX.GetFieldData(self.OUTBLOCK2, "SubstOrdAmt", i).strip()
            )
            재사용주문금액 = int(
                self.ActiveX.GetFieldData(self.OUTBLOCK2, "RuseOrdAmt", i).strip()
            )
            계좌명 = self.ActiveX.GetFieldData(self.OUTBLOCK2, "AcntNm", i).strip()
            종목명 = self.ActiveX.GetFieldData(self.OUTBLOCK2, "IsuNm", i).strip()

            lst = [
                레코드갯수,
                주문번호,
                주문시각,
                주문시장코드,
                주문유형코드,
                종목코드,
                관리사원번호,
                주문금액,
                예비주문번호,
                반대매매일련번호,
                예약주문번호,
                실물주문수량,
                재사용주문수량,
                현금주문금액,
                대용주문금액,
                재사용주문금액,
                계좌명,
                종목명,
            ]
            result.append(lst)

        columns = [
            "레코드갯수",
            "주문번호",
            "주문시각",
            "주문시장코드",
            "주문유형코드",
            "종목코드",
            "관리사원번호",
            "주문금액",
            "예비주문번호",
            "반대매매일련번호",
            "예약주문번호",
            "실물주문수량",
            "재사용주문수량",
            "현금주문금액",
            "대용주문금액",
            "재사용주문금액",
            "계좌명",
            "종목명",
        ]
        df1 = DataFrame(data=result, columns=columns)

        if self.parent is not None:
            self.parent.OnReceiveData(szTrCode, [df, df1])


# 현물정정주문
class t0425(XAQuery):
    """
    [Phase 9] 주식체결미체결상세조회 (t0425)
    - 계좌의 실시간 미체결 내역 및 총 미체결 금액을 조회함.
    - _pending_order_amt의 'State Drift' 방지를 위한 Reconciliation 용도.
    """

    def Query(self, account_num, password, chegb="2"):
        """
        Query Request
        :param account_num: 계좌번호
        :param password: 비밀번호
        :param chegb: 체결구분 (0:전체, 1:체결, 2:미체결)
        """
        self.QueryWaiting(howlong=50.0)
        self.ActiveX.LoadFromResFile(self.RESFILE)
        self.ActiveX.SetFieldData(self.INBLOCK, "accno", 0, account_num)
        self.ActiveX.SetFieldData(self.INBLOCK, "passwd", 0, password)
        self.ActiveX.SetFieldData(
            self.INBLOCK, "chegb", 0, chegb
        )  # '2' = 미체결만 조회
        self.ActiveX.SetFieldData(self.INBLOCK, "sortgb", 0, "1")  # 주문번호순
        self.ActiveX.Request(0)

    def OnReceiveData(self, szTrCode):
        # OutBlock1: Summary-like info (usually doesn't have total amt, but we can compute)
        unfilled_total_amt = 0.0

        # OutBlock2: Occurs List (Individual orders)
        unfilled_list = []

        nCount = self.ActiveX.GetBlockCount(self.OUTBLOCK2)
        for i in range(nCount):
            uncheqty = self.toint(
                self.ActiveX.GetFieldData(self.OUTBLOCK2, "uncheqty", i)
            )
            ordprice = self.tofloat(
                self.ActiveX.GetFieldData(self.OUTBLOCK2, "ordprice", i)
            )
            ticker = self.ActiveX.GetFieldData(self.OUTBLOCK2, "expcode", i).strip()

            if uncheqty > 0:
                unfilled_total_amt += uncheqty * ordprice
                unfilled_list.append(
                    {"ticker": ticker, "uncheqty": uncheqty, "ordprice": ordprice}
                )

        if self.parent:
            self.parent.OnReceiveData(
                szTrCode,
                {"total_unfilled_amt": unfilled_total_amt, "list": unfilled_list},
            )


# 현물취소주문
class t0424(XAQuery):
    def Query(
        self,
        계좌번호="",
        비밀번호="",
        단가구분="1",
        체결구분="0",
        단일가구분="0",
        제비용포함여부="1",
        CTS_종목번호="",
    ):
        self.QueryWaiting(howlong=100.0, limit_count=None, limit_seconds=600)

        self.ActiveX.LoadFromResFile(self.RESFILE)
        self.ActiveX.SetFieldData(self.INBLOCK, "accno", 0, 계좌번호)
        self.ActiveX.SetFieldData(self.INBLOCK, "passwd", 0, 비밀번호)
        self.ActiveX.SetFieldData(self.INBLOCK, "prcgb", 0, 단가구분)
        self.ActiveX.SetFieldData(self.INBLOCK, "chegb", 0, 체결구분)
        self.ActiveX.SetFieldData(self.INBLOCK, "dangb", 0, 단일가구분)
        self.ActiveX.SetFieldData(self.INBLOCK, "charge", 0, 제비용포함여부)
        self.ActiveX.SetFieldData(self.INBLOCK, "cts_expcode", 0, CTS_종목번호)
        ret = self.ActiveX.Request(0)
        # import logging

        # logger = logging.getLogger("XAQuaries")
        # logger.info(f"[t0424] Query Request(0) returned: {ret}")

    def OnReceiveData(self, szTrCode):
        # import logging

        # logger = logging.getLogger("t0424")
        # logger.info(
        #     f"[t0424] OnReceiveData triggered for {szTrCode}. Starting to parse BlockCount..."
        # )
        result = []
        nCount = self.ActiveX.GetBlockCount(self.OUTBLOCK)
        for i in range(nCount):
            추정순자산 = int(
                self.ActiveX.GetFieldData(self.OUTBLOCK, "sunamt", i).strip()
            )
            실현손익 = int(
                self.ActiveX.GetFieldData(self.OUTBLOCK, "dtsunik", i).strip()
            )
            매입금액 = int(self.ActiveX.GetFieldData(self.OUTBLOCK, "mamt", i).strip())
            추정D2예수금 = int(
                self.ActiveX.GetFieldData(self.OUTBLOCK, "sunamt1", i).strip()
            )
            CTS_종목번호 = self.ActiveX.GetFieldData(
                self.OUTBLOCK, "cts_expcode", i
            ).strip()
            평가금액 = int(
                self.ActiveX.GetFieldData(self.OUTBLOCK, "tappamt", i).strip()
            )
            평가손익 = int(
                self.ActiveX.GetFieldData(self.OUTBLOCK, "tdtsunik", i).strip()
            )

            lst = [
                추정순자산,
                실현손익,
                매입금액,
                추정D2예수금,
                CTS_종목번호,
                평가금액,
                평가손익,
            ]
            result.append(lst)

        columns = [
            "추정순자산",
            "실현손익",
            "매입금액",
            "추정D2예수금",
            "CTS_종목번호",
            "평가금액",
            "평가손익",
        ]
        df = DataFrame(data=result, columns=columns)

        result = []
        nCount = self.ActiveX.GetBlockCount(self.OUTBLOCK1)
        for i in range(nCount):
            종목번호 = self.ActiveX.GetFieldData(self.OUTBLOCK1, "expcode", i).strip()
            잔고구분 = self.ActiveX.GetFieldData(self.OUTBLOCK1, "jangb", i).strip()
            잔고수량 = int(
                self.ActiveX.GetFieldData(self.OUTBLOCK1, "janqty", i).strip()
            )
            매도가능수량 = int(
                self.ActiveX.GetFieldData(self.OUTBLOCK1, "mdposqt", i).strip()
            )
            평균단가 = int(self.ActiveX.GetFieldData(self.OUTBLOCK1, "pamt", i).strip())
            매입금액 = int(self.ActiveX.GetFieldData(self.OUTBLOCK1, "mamt", i).strip())
            대출금액 = int(
                self.ActiveX.GetFieldData(self.OUTBLOCK1, "sinamt", i).strip()
            )
            만기일자 = self.ActiveX.GetFieldData(self.OUTBLOCK1, "lastdt", i).strip()
            당일매수금액 = int(
                self.ActiveX.GetFieldData(self.OUTBLOCK1, "msat", i).strip()
            )
            당일매수단가 = int(
                self.ActiveX.GetFieldData(self.OUTBLOCK1, "mpms", i).strip()
            )
            당일매도금액 = int(
                self.ActiveX.GetFieldData(self.OUTBLOCK1, "mdat", i).strip()
            )
            당일매도단가 = int(
                self.ActiveX.GetFieldData(self.OUTBLOCK1, "mpmd", i).strip()
            )
            전일매수금액 = int(
                self.ActiveX.GetFieldData(self.OUTBLOCK1, "jsat", i).strip()
            )
            전일매수단가 = int(
                self.ActiveX.GetFieldData(self.OUTBLOCK1, "jpms", i).strip()
            )
            전일매도금액 = int(
                self.ActiveX.GetFieldData(self.OUTBLOCK1, "jdat", i).strip()
            )
            전일매도단가 = int(
                self.ActiveX.GetFieldData(self.OUTBLOCK1, "jpmd", i).strip()
            )
            처리순번 = int(
                self.ActiveX.GetFieldData(self.OUTBLOCK1, "sysprocseq", i).strip()
            )
            대출일자 = self.ActiveX.GetFieldData(self.OUTBLOCK1, "loandt", i).strip()
            종목명 = self.ActiveX.GetFieldData(self.OUTBLOCK1, "hname", i).strip()
            시장구분 = self.ActiveX.GetFieldData(self.OUTBLOCK1, "marketgb", i).strip()
            종목구분 = self.ActiveX.GetFieldData(self.OUTBLOCK1, "jonggb", i).strip()
            보유비중 = float(
                self.ActiveX.GetFieldData(self.OUTBLOCK1, "janrt", i).strip()
            )
            현재가 = int(self.ActiveX.GetFieldData(self.OUTBLOCK1, "price", i).strip())
            평가금액 = int(
                self.ActiveX.GetFieldData(self.OUTBLOCK1, "appamt", i).strip()
            )
            평가손익 = int(
                self.ActiveX.GetFieldData(self.OUTBLOCK1, "dtsunik", i).strip()
            )
            수익율 = float(
                self.ActiveX.GetFieldData(self.OUTBLOCK1, "sunikrt", i).strip()
            )
            수수료 = int(self.ActiveX.GetFieldData(self.OUTBLOCK1, "fee", i).strip())
            제세금 = int(self.ActiveX.GetFieldData(self.OUTBLOCK1, "tax", i).strip())
            신용이자 = int(
                self.ActiveX.GetFieldData(self.OUTBLOCK1, "sininter", i).strip()
            )

            lst = [
                종목번호,
                잔고구분,
                잔고수량,
                매도가능수량,
                평균단가,
                매입금액,
                대출금액,
                만기일자,
                당일매수금액,
                당일매수단가,
                당일매도금액,
                당일매도단가,
                전일매수금액,
                전일매수단가,
                전일매도금액,
                전일매도단가,
                처리순번,
                대출일자,
                종목명,
                시장구분,
                종목구분,
                보유비중,
                현재가,
                평가금액,
                평가손익,
                수익율,
                수수료,
                제세금,
                신용이자,
            ]
            result.append(lst)

        columns = [
            "종목번호",
            "잔고구분",
            "잔고수량",
            "매도가능수량",
            "평균단가",
            "매입금액",
            "대출금액",
            "만기일자",
            "당일매수금액",
            " 당일매수단가",
            "당일매도금액",
            "당일매도단가",
            "전일매수금액",
            "전일매수단가",
            "전일매도금액",
            "전일매도단가",
            " 처리순번",
            "대출일자",
            "종목명",
            "시장구분",
            "종목구분",
            "보유비중",
            "현재가",
            "평가금액",
            "평가손익",
            "수익율",
            "수수료",
            "제세금",
            "신용이자",
        ]
        df1 = DataFrame(data=result, columns=columns)

        if self.parent is not None:
            self.parent.OnReceiveData(szTrCode, [df, df1])


# 주식분별주가조회
class t1302(XAQuery):
    def Query(self, 종목코드="", 작업구분="1", 시간="", 건수="900", 연속조회=False):
        self.QueryWaiting(howlong=100.0, limit_count=200, limit_seconds=600)

        if not 연속조회:
            self.ActiveX.LoadFromResFile(self.RESFILE)
            self.ActiveX.SetFieldData(self.INBLOCK, "shcode", 0, 종목코드)
            self.ActiveX.SetFieldData(self.INBLOCK, "gubun", 0, 작업구분)
            self.ActiveX.SetFieldData(self.INBLOCK, "time", 0, 시간)
            self.ActiveX.SetFieldData(self.INBLOCK, "cnt", 0, 건수)
            self.ActiveX.Request(0)
        else:
            self.ActiveX.SetFieldData(self.INBLOCK, "cts_time", 0, 시간)

            err_code = self.ActiveX.Request(True)  # 연속조회인경우만 True
            if err_code < 0:
                클래스이름 = self.__class__.__name__
                함수이름 = get_funcname()
                print(
                    "%s-%s " % (클래스이름, 함수이름), "error... {0}".format(err_code)
                )

    def OnReceiveData(self, szTrCode):
        # [1] 변수 초기화 (UnboundLocalError 방지)
        시간CTS = ""
        result = []
        nCount = self.ActiveX.GetBlockCount(self.OUTBLOCK)
        for i in range(nCount):
            시간CTS = self.ActiveX.GetFieldData(self.OUTBLOCK, "cts_time", i).strip()

        result = []
        nCount = self.ActiveX.GetBlockCount(self.OUTBLOCK1)
        for i in range(nCount):
            시간 = self.ActiveX.GetFieldData(self.OUTBLOCK1, "chetime", i).strip()
            종가 = int(self.ActiveX.GetFieldData(self.OUTBLOCK1, "close", i).strip())
            전일대비구분 = self.ActiveX.GetFieldData(self.OUTBLOCK1, "sign", i).strip()
            전일대비 = int(
                self.ActiveX.GetFieldData(self.OUTBLOCK1, "change", i).strip()
            )
            등락율 = float(self.ActiveX.GetFieldData(self.OUTBLOCK1, "diff", i).strip())
            체결강도 = float(
                self.ActiveX.GetFieldData(self.OUTBLOCK1, "chdegree", i).strip()
            )
            매도체결수량 = int(
                self.ActiveX.GetFieldData(self.OUTBLOCK1, "mdvolume", i).strip()
            )
            매수체결수량 = int(
                self.ActiveX.GetFieldData(self.OUTBLOCK1, "msvolume", i).strip()
            )
            순매수체결량 = int(
                self.ActiveX.GetFieldData(self.OUTBLOCK1, "revolume", i).strip()
            )
            매도체결건수 = int(
                self.ActiveX.GetFieldData(self.OUTBLOCK1, "mdchecnt", i).strip()
            )
            매수체결건수 = int(
                self.ActiveX.GetFieldData(self.OUTBLOCK1, "mschecnt", i).strip()
            )
            순체결건수 = int(
                self.ActiveX.GetFieldData(self.OUTBLOCK1, "rechecnt", i).strip()
            )
            거래량 = int(self.ActiveX.GetFieldData(self.OUTBLOCK1, "volume", i).strip())
            시가 = int(self.ActiveX.GetFieldData(self.OUTBLOCK1, "open", i).strip())
            고가 = int(self.ActiveX.GetFieldData(self.OUTBLOCK1, "high", i).strip())
            저가 = int(self.ActiveX.GetFieldData(self.OUTBLOCK1, "low", i).strip())
            체결량 = int(
                self.ActiveX.GetFieldData(self.OUTBLOCK1, "cvolume", i).strip()
            )
            매도체결건수시간 = int(
                self.ActiveX.GetFieldData(self.OUTBLOCK1, "mdchecnttm", i).strip()
            )
            매수체결건수시간 = int(
                self.ActiveX.GetFieldData(self.OUTBLOCK1, "mschecnttm", i).strip()
            )
            매도잔량 = int(
                self.ActiveX.GetFieldData(self.OUTBLOCK1, "totofferrem", i).strip()
            )
            매수잔량 = int(
                self.ActiveX.GetFieldData(self.OUTBLOCK1, "totbidrem", i).strip()
            )
            시간별매도체결량 = int(
                self.ActiveX.GetFieldData(self.OUTBLOCK1, "mdvolumetm", i).strip()
            )
            시간별매수체결량 = int(
                self.ActiveX.GetFieldData(self.OUTBLOCK1, "msvolumetm", i).strip()
            )

            lst = [
                시간,
                종가,
                전일대비구분,
                전일대비,
                등락율,
                체결강도,
                매도체결수량,
                매수체결수량,
                순매수체결량,
                매도체결건수,
                매수체결건수,
                순체결건수,
                거래량,
                시가,
                고가,
                저가,
                체결량,
                매도체결건수시간,
                매수체결건수시간,
                매도잔량,
                매수잔량,
                시간별매도체결량,
                시간별매수체결량,
            ]
            result.append(lst)

        columns = [
            "시간",
            "종가",
            "전일대비구분",
            "전일대비",
            "등락율",
            "체결강도",
            "매도체결수량",
            "매수체결수량",
            "순매수체결량",
            "매도체결건수",
            "매수체결건수",
            "순체결건수",
            "거래량",
            "시가",
            "고가",
            "저가",
            "체결량",
            "매도체결건수시간",
            "매수체결건수시간",
            "매도잔량",
            "매수잔량",
            "시간별매도체결량",
            "시간별매수체결량",
        ]
        df = DataFrame(data=result, columns=columns)

        if self.parent is not None:
            self.parent.OnReceiveData(szTrCode, [시간CTS, df])


# 기간별주가
class t1305(XAQuery):
    def Query(
        self, 종목코드="", 일주월구분="1", 날짜="", IDX="", 건수="900", 연속조회=False
    ):
        self.QueryWaiting(howlong=100.0, limit_count=200, limit_seconds=600)

        if not 연속조회:
            self.ActiveX.LoadFromResFile(self.RESFILE)
            self.ActiveX.SetFieldData(self.INBLOCK, "shcode", 0, 종목코드)
            self.ActiveX.SetFieldData(self.INBLOCK, "dwmcode", 0, 일주월구분)
            self.ActiveX.SetFieldData(self.INBLOCK, "date", 0, 날짜)
            self.ActiveX.SetFieldData(self.INBLOCK, "idx", 0, IDX)
            self.ActiveX.SetFieldData(self.INBLOCK, "cnt", 0, 건수)
            self.ActiveX.Request(0)
        else:
            self.ActiveX.SetFieldData(self.INBLOCK, "date", 0, 날짜)
            self.ActiveX.SetFieldData(self.INBLOCK, "idx", 0, IDX)
            self.ActiveX.SetFieldData(self.INBLOCK, "cnt", 0, 건수)

            err_code = self.ActiveX.Request(True)  # 연속조회인경우만 True
            if err_code < 0:
                클래스이름 = self.__class__.__name__
                함수이름 = get_funcname()
                print(
                    "%s-%s " % (클래스이름, 함수이름), "error... {0}".format(err_code)
                )

    def OnReceiveData(self, szTrCode):
        result = []
        CNT = None
        날짜 = None
        IDX = None
        nCount = self.ActiveX.GetBlockCount(self.OUTBLOCK)
        CNT = nCount
        for i in range(nCount):
            CNT = int(self.ActiveX.GetFieldData(self.OUTBLOCK, "cnt", i).strip())
            날짜 = self.ActiveX.GetFieldData(self.OUTBLOCK, "date", i).strip()
            IDX = int(self.ActiveX.GetFieldData(self.OUTBLOCK, "idx", i).strip())

        result = []
        nCount = self.ActiveX.GetBlockCount(self.OUTBLOCK1)
        CNT = nCount
        for i in range(nCount):
            날짜 = self.ActiveX.GetFieldData(self.OUTBLOCK1, "date", i).strip()
            시가 = int(self.ActiveX.GetFieldData(self.OUTBLOCK1, "open", i).strip())
            고가 = int(self.ActiveX.GetFieldData(self.OUTBLOCK1, "high", i).strip())
            저가 = int(self.ActiveX.GetFieldData(self.OUTBLOCK1, "low", i).strip())
            종가 = int(self.ActiveX.GetFieldData(self.OUTBLOCK1, "close", i).strip())
            전일대비구분 = self.ActiveX.GetFieldData(self.OUTBLOCK1, "sign", i).strip()
            전일대비 = int(
                self.ActiveX.GetFieldData(self.OUTBLOCK1, "change", i).strip()
            )
            등락율 = float(self.ActiveX.GetFieldData(self.OUTBLOCK1, "diff", i).strip())
            누적거래량 = int(
                self.ActiveX.GetFieldData(self.OUTBLOCK1, "volume", i).strip()
            )
            거래증가율 = float(
                self.ActiveX.GetFieldData(self.OUTBLOCK1, "diff_vol", i).strip()
            )
            체결강도 = float(
                self.ActiveX.GetFieldData(self.OUTBLOCK1, "chdegree", i).strip()
            )
            소진율 = float(
                self.ActiveX.GetFieldData(self.OUTBLOCK1, "sojinrate", i).strip()
            )
            회전율 = float(
                self.ActiveX.GetFieldData(self.OUTBLOCK1, "changerate", i).strip()
            )
            외인순매수 = int(
                self.ActiveX.GetFieldData(self.OUTBLOCK1, "fpvolume", i).strip()
            )
            기관순매수 = int(
                self.ActiveX.GetFieldData(self.OUTBLOCK1, "covolume", i).strip()
            )
            종목코드 = self.ActiveX.GetFieldData(self.OUTBLOCK1, "shcode", i).strip()
            누적거래대금 = int(
                self.ActiveX.GetFieldData(self.OUTBLOCK1, "value", i).strip()
            )
            개인순매수 = int(
                self.ActiveX.GetFieldData(self.OUTBLOCK1, "ppvolume", i).strip()
            )
            시가대비구분 = self.ActiveX.GetFieldData(
                self.OUTBLOCK1, "o_sign", i
            ).strip()
            시가대비 = int(
                self.ActiveX.GetFieldData(self.OUTBLOCK1, "o_change", i).strip()
            )
            시가기준등락율 = float(
                self.ActiveX.GetFieldData(self.OUTBLOCK1, "o_diff", i).strip()
            )
            고가대비구분 = self.ActiveX.GetFieldData(
                self.OUTBLOCK1, "h_sign", i
            ).strip()
            고가대비 = int(
                self.ActiveX.GetFieldData(self.OUTBLOCK1, "h_change", i).strip()
            )
            고가기준등락율 = float(
                self.ActiveX.GetFieldData(self.OUTBLOCK1, "h_diff", i).strip()
            )
            저가대비구분 = self.ActiveX.GetFieldData(
                self.OUTBLOCK1, "l_sign", i
            ).strip()
            저가대비 = int(
                self.ActiveX.GetFieldData(self.OUTBLOCK1, "l_change", i).strip()
            )
            저가기준등락율 = float(
                self.ActiveX.GetFieldData(self.OUTBLOCK1, "l_diff", i).strip()
            )
            시가총액 = int(
                self.ActiveX.GetFieldData(self.OUTBLOCK1, "marketcap", i).strip()
            )

            lst = [
                날짜,
                시가,
                고가,
                저가,
                종가,
                전일대비구분,
                전일대비,
                등락율,
                누적거래량,
                거래증가율,
                체결강도,
                소진율,
                회전율,
                외인순매수,
                기관순매수,
                종목코드,
                누적거래대금,
                개인순매수,
                시가대비구분,
                시가대비,
                시가기준등락율,
                고가대비구분,
                고가대비,
                고가기준등락율,
                저가대비구분,
                저가대비,
                저가기준등락율,
                시가총액,
            ]
            result.append(lst)

        columns = [
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
        df = DataFrame(data=result, columns=columns)

        if self.parent is not None:
            self.parent.OnReceiveData(szTrCode, [CNT, 날짜, IDX, df])


# 거래량상위
class t1463(XAQuery):
    def Query(
        self,
        구분="0",
        전일구분="",
        대상제외="",
        시작가격="",
        종료가격="",
        거래량="",
        IDX="",
        대상제외2="",
        연속조회=False,
    ):
        self.QueryWaiting(howlong=100.0, limit_count=None, limit_seconds=600)

        if not 연속조회:
            self.ActiveX.LoadFromResFile(self.RESFILE)
            self.ActiveX.SetFieldData(self.INBLOCK, "gubun", 0, 구분)
            self.ActiveX.SetFieldData(self.INBLOCK, "jnilgubun", 0, 전일구분)
            self.ActiveX.SetFieldData(self.INBLOCK, "jc_num", 0, 대상제외)
            self.ActiveX.SetFieldData(self.INBLOCK, "sprice", 0, 시작가격)
            self.ActiveX.SetFieldData(self.INBLOCK, "eprice", 0, 종료가격)
            self.ActiveX.SetFieldData(self.INBLOCK, "volume", 0, 거래량)
            self.ActiveX.SetFieldData(self.INBLOCK, "idx", 0, IDX)
            self.ActiveX.SetFieldData(self.INBLOCK, "jc_num2", 0, 대상제외2)
            self.ActiveX.Request(0)
        else:
            self.ActiveX.SetFieldData(self.INBLOCK, "idx", 0, IDX)

            err_code = self.ActiveX.Request(True)  # 연속조회인경우만 True
            if err_code < 0:
                클래스이름 = self.__class__.__name__
                함수이름 = get_funcname()
                print(
                    "%s-%s " % (클래스이름, 함수이름), "error... {0}".format(err_code)
                )

    def OnReceiveData(self, szTrCode):
        result = []
        nCount = self.ActiveX.GetBlockCount(self.OUTBLOCK)
        for i in range(nCount):
            IDX = int(self.ActiveX.GetFieldData(self.OUTBLOCK, "idx", i).strip())

        result = []
        nCount = self.ActiveX.GetBlockCount(self.OUTBLOCK1)
        for i in range(nCount):
            한글명 = self.ActiveX.GetFieldData(self.OUTBLOCK1, "hname", i).strip()
            현재가 = int(self.ActiveX.GetFieldData(self.OUTBLOCK1, "price", i).strip())
            전일대비구분 = self.ActiveX.GetFieldData(self.OUTBLOCK1, "sign", i).strip()
            전일대비 = int(
                self.ActiveX.GetFieldData(self.OUTBLOCK1, "change", i).strip()
            )
            등락율 = float(self.ActiveX.GetFieldData(self.OUTBLOCK1, "diff", i).strip())
            누적거래량 = int(
                self.ActiveX.GetFieldData(self.OUTBLOCK1, "volume", i).strip()
            )
            거래대금 = int(
                self.ActiveX.GetFieldData(self.OUTBLOCK1, "value", i).strip()
            )
            전일거래대금 = int(
                self.ActiveX.GetFieldData(self.OUTBLOCK1, "jnilvalue", i).strip()
            )
            전일비 = float(
                self.ActiveX.GetFieldData(self.OUTBLOCK1, "bef_diff", i).strip()
            )
            종목코드 = self.ActiveX.GetFieldData(self.OUTBLOCK1, "shcode", i).strip()
            filler = self.ActiveX.GetFieldData(self.OUTBLOCK1, "filler", i).strip()
            전일거래량 = int(
                self.ActiveX.GetFieldData(self.OUTBLOCK1, "jnilvolume", i).strip()
            )

            lst = [
                한글명,
                현재가,
                전일대비구분,
                전일대비,
                등락율,
                누적거래량,
                거래대금,
                전일거래대금,
                전일비,
                종목코드,
                filler,
                전일거래량,
            ]
            result.append(lst)

        columns = [
            "한글명",
            "현재가",
            "전일대비구분",
            "전일대비",
            "등락율",
            "누적거래량",
            "거래대금",
            "전일거래대금",
            "전일비",
            "종목코드",
            "filler",
            "전일거래량",
        ]
        df = DataFrame(data=result, columns=columns)

        if self.parent is not None:
            self.parent.OnReceiveData(szTrCode, [IDX, df])


# 업종현재가
class t1511(XAQuery):
    def Query(self, 업종코드="001"):
        self.QueryWaiting(howlong=100.0, limit_count=None, limit_seconds=600)
        self.ActiveX.LoadFromResFile(self.RESFILE)
        self.ActiveX.SetFieldData(self.INBLOCK, "upcode", 0, 업종코드)
        self.ActiveX.Request(0)

    def OnReceiveData(self, szTrCode):
        result = dict()

        result["현재지수"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "pricejisu", 0)
        result["전일지수"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "jniljisu", 0)
        result["전일대비구분"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "sign", 0)
        result["지수등락율"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "diffjisu", 0)
        result["거래량전일대비"] = self.ActiveX.GetFieldData(
            self.OUTBLOCK, "volumechange", 0
        )
        result["거래대금전일대비"] = self.ActiveX.GetFieldData(
            self.OUTBLOCK, "valuechange", 0
        )
        result["시가지수"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "openjisu", 0)
        result["시가등락율"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "opendiff", 0)
        result["시가시간"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "opentime", 0)
        result["고가지수"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "highjisu", 0)
        result["고가등락율"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "highdiff", 0)
        result["고가시간"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "hightime", 0)
        result["저가지수"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "lowjisu", 0)
        result["저가등락율"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "lowdiff", 0)
        result["저가시간"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "lowtime", 0)
        result["첫번째지수코드"] = self.ActiveX.GetFieldData(
            self.OUTBLOCK, "firstjcode", 0
        )
        result["첫번째등락율"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "firdiff", 0)
        result["두번째등락율"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "secdiff", 0)
        result["세번째등락율"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "thrdiff", 0)
        result["네번째등락율"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "fordiff", 0)
        result["상승종목수"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "highjo", 0)
        result["보합종목수"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "unchgjo", 0)
        result["하락종목수"] = self.ActiveX.GetFieldData(self.OUTBLOCK, "lowjo", 0)

        if self.parent is not None:
            self.parent.OnReceiveData(szTrCode, [result])


# 업종기간별추이
class t1514(XAQuery):
    def Query(
        self,
        업종코드="001",
        구분1="",
        구분2="1",
        CTS일자="",
        조회건수="100",
        비중구분="",
        연속조회=False,
    ):
        self.QueryWaiting(howlong=100.0, limit_count=None, limit_seconds=600)

        if not 연속조회:
            self.ActiveX.LoadFromResFile(self.RESFILE)
            self.ActiveX.SetFieldData(self.INBLOCK, "upcode", 0, 업종코드)
            self.ActiveX.SetFieldData(self.INBLOCK, "gubun1", 0, 구분1)
            self.ActiveX.SetFieldData(self.INBLOCK, "gubun2", 0, 구분2)
            self.ActiveX.SetFieldData(self.INBLOCK, "cts_date", 0, CTS일자)
            self.ActiveX.SetFieldData(self.INBLOCK, "cnt", 0, 조회건수)
            self.ActiveX.SetFieldData(self.INBLOCK, "rate_gbn", 0, 비중구분)
            self.ActiveX.Request(0)
        else:
            self.ActiveX.SetFieldData(self.INBLOCK, "cts_date", 0, CTS일자)

            err_code = self.ActiveX.Request(True)  # 연속조회인경우만 True
            if err_code < 0:
                클래스이름 = self.__class__.__name__
                함수이름 = get_funcname()
                print(
                    "%s-%s " % (클래스이름, 함수이름), "error... {0}".format(err_code)
                )

    def OnReceiveData(self, szTrCode):
        CTS일자 = ""
        result = []
        nCount = self.ActiveX.GetBlockCount(self.OUTBLOCK)
        for i in range(nCount):
            CTS일자 = self.ActiveX.GetFieldData(self.OUTBLOCK, "cts_date", i).strip()

        result = []
        nCount = self.ActiveX.GetBlockCount(self.OUTBLOCK1)
        for i in range(nCount):
            일자 = self.ActiveX.GetFieldData(self.OUTBLOCK1, "date", i).strip()
            지수 = self.tofloat(self.ActiveX.GetFieldData(self.OUTBLOCK1, "jisu", i))
            전일대비구분 = self.ActiveX.GetFieldData(self.OUTBLOCK1, "sign", i).strip()
            전일대비 = self.tofloat(
                self.ActiveX.GetFieldData(self.OUTBLOCK1, "change", i)
            )
            등락율 = self.tofloat(self.ActiveX.GetFieldData(self.OUTBLOCK1, "diff", i))
            거래량 = self.toint(self.ActiveX.GetFieldData(self.OUTBLOCK1, "volume", i))
            거래증가율 = self.tofloat(
                self.ActiveX.GetFieldData(self.OUTBLOCK1, "diff_vol", i)
            )
            거래대금1 = self.toint(
                self.ActiveX.GetFieldData(self.OUTBLOCK1, "value1", i)
            )
            상승 = self.toint(self.ActiveX.GetFieldData(self.OUTBLOCK1, "high", i))
            보합 = self.toint(self.ActiveX.GetFieldData(self.OUTBLOCK1, "unchg", i))
            하락 = self.toint(self.ActiveX.GetFieldData(self.OUTBLOCK1, "low", i))
            상승종목비율 = self.tofloat(
                self.ActiveX.GetFieldData(self.OUTBLOCK1, "uprate", i)
            )
            외인순매수 = self.toint(
                self.ActiveX.GetFieldData(self.OUTBLOCK1, "frgsvolume", i)
            )
            시가 = self.tofloat(
                self.ActiveX.GetFieldData(self.OUTBLOCK1, "openjisu", i)
            )
            고가 = self.tofloat(
                self.ActiveX.GetFieldData(self.OUTBLOCK1, "highjisu", i)
            )
            저가 = self.tofloat(self.ActiveX.GetFieldData(self.OUTBLOCK1, "lowjisu", i))
            거래대금2 = self.toint(
                self.ActiveX.GetFieldData(self.OUTBLOCK1, "value2", i)
            )
            상한 = self.toint(self.ActiveX.GetFieldData(self.OUTBLOCK1, "up", i))
            하한 = self.toint(self.ActiveX.GetFieldData(self.OUTBLOCK1, "down", i))
            종목수 = self.toint(self.ActiveX.GetFieldData(self.OUTBLOCK1, "totjo", i))
            기관순매수 = self.toint(
                self.ActiveX.GetFieldData(self.OUTBLOCK1, "orgsvolume", i)
            )
            업종코드 = self.ActiveX.GetFieldData(self.OUTBLOCK1, "upcode", i).strip()
            거래비중 = self.tofloat(
                self.ActiveX.GetFieldData(self.OUTBLOCK1, "rate", i)
            )
            업종배당수익률 = self.tofloat(
                self.ActiveX.GetFieldData(self.OUTBLOCK1, "divrate", i)
            )

            lst = [
                일자,
                지수,
                전일대비구분,
                전일대비,
                등락율,
                거래량,
                거래증가율,
                거래대금1,
                상승,
                보합,
                하락,
                상승종목비율,
                외인순매수,
                시가,
                고가,
                저가,
                거래대금2,
                상한,
                하한,
                종목수,
                기관순매수,
                업종코드,
                거래비중,
                업종배당수익률,
            ]

            result.append(lst)

        columns = [
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
        df = DataFrame(data=result, columns=columns)

        if self.parent is not None:
            self.parent.OnReceiveData(szTrCode, [CTS일자, df])


# 업종별 종목시세
# 업종별종목 리스트
class t1516(XAQuery):
    def Query(self, 업종코드="001", 구분="", 종목코드="", 연속조회=False):
        self.QueryWaiting(howlong=100.0, limit_count=200, limit_seconds=600)

        if not 연속조회:
            self.ActiveX.LoadFromResFile(self.RESFILE)
            self.ActiveX.SetFieldData(self.INBLOCK, "upcode", 0, 업종코드)
            self.ActiveX.SetFieldData(self.INBLOCK, "gubun", 0, 구분)
            self.ActiveX.SetFieldData(self.INBLOCK, "shcode", 0, 종목코드)
            self.ActiveX.Request(0)
        else:
            self.ActiveX.SetFieldData(self.INBLOCK, "shcode", 0, 종목코드)

            err_code = self.ActiveX.Request(True)  # 연속조회인경우만 True
            if err_code < 0:
                클래스이름 = self.__class__.__name__
                함수이름 = get_funcname()
                print(
                    "%s-%s " % (클래스이름, 함수이름), "error... {0}".format(err_code)
                )

    def OnReceiveData(self, szTrCode):
        result = []
        nCount = self.ActiveX.GetBlockCount(self.OUTBLOCK)
        for i in range(nCount):
            종목코드 = self.ActiveX.GetFieldData(self.OUTBLOCK, "shcode", i).strip()
            지수 = self.tofloat(
                self.ActiveX.GetFieldData(self.OUTBLOCK, "pricejisu", i).strip()
            )
            지수_전일대비구분 = self.ActiveX.GetFieldData(
                self.OUTBLOCK, "sign", i
            ).strip()
            지수_전일대비 = self.tofloat(
                self.ActiveX.GetFieldData(self.OUTBLOCK, "change", i).strip()
            )
            지수_등락율 = self.tofloat(
                self.ActiveX.GetFieldData(self.OUTBLOCK, "jdiff", i).strip()
            )

        result = []
        nCount = self.ActiveX.GetBlockCount(self.OUTBLOCK1)
        for i in range(nCount):
            종목명 = self.ActiveX.GetFieldData(self.OUTBLOCK1, "hname", i).strip()
            현재가 = self.toint(self.ActiveX.GetFieldData(self.OUTBLOCK1, "price", i))
            전일대비구분 = self.ActiveX.GetFieldData(self.OUTBLOCK1, "sign", i).strip()
            전일대비 = self.toint(
                self.ActiveX.GetFieldData(self.OUTBLOCK1, "change", i)
            )
            등락율 = self.tofloat(self.ActiveX.GetFieldData(self.OUTBLOCK1, "diff", i))
            누적거래량 = self.toint(
                self.ActiveX.GetFieldData(self.OUTBLOCK1, "volume", i)
            )
            시가 = self.toint(self.ActiveX.GetFieldData(self.OUTBLOCK1, "open", i))
            고가 = self.toint(self.ActiveX.GetFieldData(self.OUTBLOCK1, "high", i))
            저가 = self.toint(self.ActiveX.GetFieldData(self.OUTBLOCK1, "low", i))
            소진율 = self.tofloat(
                self.ActiveX.GetFieldData(self.OUTBLOCK1, "sojinrate", i)
            )
            베타계수 = self.tofloat(
                self.ActiveX.GetFieldData(self.OUTBLOCK1, "beta", i)
            )
            PER = self.tofloat(self.ActiveX.GetFieldData(self.OUTBLOCK1, "perx", i))
            외인순매수 = self.toint(
                self.ActiveX.GetFieldData(self.OUTBLOCK1, "frgsvolume", i)
            )
            기관순매수 = self.toint(
                self.ActiveX.GetFieldData(self.OUTBLOCK1, "orgsvolume", i)
            )
            거래증가율 = self.tofloat(
                self.ActiveX.GetFieldData(self.OUTBLOCK1, "diff_vol", i)
            )
            종목코드 = self.ActiveX.GetFieldData(self.OUTBLOCK1, "shcode", i).strip()
            시가총액 = self.toint(self.ActiveX.GetFieldData(self.OUTBLOCK1, "total", i))
            거래대금 = self.toint(self.ActiveX.GetFieldData(self.OUTBLOCK1, "value", i))

            lst = [
                종목명,
                현재가,
                전일대비구분,
                전일대비,
                등락율,
                누적거래량,
                시가,
                고가,
                저가,
                소진율,
                베타계수,
                PER,
                외인순매수,
                기관순매수,
                거래증가율,
                종목코드,
                시가총액,
                거래대금,
            ]

            result.append(lst)

        columns = [
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
        df = DataFrame(data=result, columns=columns)

        if self.parent is not None:
            self.parent.OnReceiveData(
                szTrCode, [지수, 지수_전일대비구분, 지수_전일대비, 지수_등락율, df]
            )


# 테마종목별 시세조회
# 연속조회를 False로 넘김
class t1537(XAQuery):
    def Query(self, 테마코드="0001", CTS일자="", 연속조회=False):
        self.QueryWaiting(howlong=100.0, limit_count=200, limit_seconds=600)

        if not 연속조회:
            self.ActiveX.LoadFromResFile(self.RESFILE)
            self.ActiveX.SetFieldData(self.INBLOCK, "tmcode", 0, 테마코드)
            self.ActiveX.Request(0)
        else:
            self.ActiveX.SetFieldData(self.INBLOCK, "cts_date", 0, CTS일자)

            err_code = self.ActiveX.Request(True)  # 연속조회인경우만 True
            if err_code < 0:
                클래스이름 = self.__class__.__name__
                함수이름 = get_funcname()
                print(
                    "%s-%s " % (클래스이름, 함수이름), "error... {0}".format(err_code)
                )

    def OnReceiveData(self, szTrCode):
        result = []
        nCount = self.ActiveX.GetBlockCount(self.OUTBLOCK)
        for i in range(nCount):
            상승종목수 = int(
                self.ActiveX.GetFieldData(self.OUTBLOCK, "upcnt", i).strip()
            )
            테마종목수 = int(
                self.ActiveX.GetFieldData(self.OUTBLOCK, "tmcnt", i).strip()
            )
            상승종목비율 = int(
                self.ActiveX.GetFieldData(self.OUTBLOCK, "uprate", i).strip()
            )
            테마명 = self.ActiveX.GetFieldData(self.OUTBLOCK, "tmname", i).strip()

            lst = [상승종목수, 테마종목수, 상승종목비율, 테마명]
            result.append(lst)

        columns = ["상승종목수", "테마종목수", "상승종목비율", "테마명"]
        df = DataFrame(data=result, columns=columns)

        result = []
        nCount = self.ActiveX.GetBlockCount(self.OUTBLOCK1)
        for i in range(nCount):
            종목명 = self.ActiveX.GetFieldData(self.OUTBLOCK1, "hname", i).strip()
            현재가 = int(self.ActiveX.GetFieldData(self.OUTBLOCK1, "price", i).strip())
            전일대비구분 = self.ActiveX.GetFieldData(self.OUTBLOCK1, "sign", i).strip()
            전일대비 = int(
                self.ActiveX.GetFieldData(self.OUTBLOCK1, "change", i).strip()
            )
            등락율 = float(self.ActiveX.GetFieldData(self.OUTBLOCK1, "diff", i).strip())
            누적거래량 = int(
                self.ActiveX.GetFieldData(self.OUTBLOCK1, "volume", i).strip()
            )
            전일동시간 = float(
                self.ActiveX.GetFieldData(self.OUTBLOCK1, "jniltime", i).strip()
            )
            종목코드 = self.ActiveX.GetFieldData(self.OUTBLOCK1, "shcode", i).strip()
            예상체결가 = int(
                self.ActiveX.GetFieldData(self.OUTBLOCK1, "yeprice", i).strip()
            )
            시가 = int(self.ActiveX.GetFieldData(self.OUTBLOCK1, "open", i).strip())
            고가 = int(self.ActiveX.GetFieldData(self.OUTBLOCK1, "high", i).strip())
            저가 = int(self.ActiveX.GetFieldData(self.OUTBLOCK1, "low", i).strip())
            누적거래대금 = int(
                self.ActiveX.GetFieldData(self.OUTBLOCK1, "value", i).strip()
            )
            시가총액 = int(
                self.ActiveX.GetFieldData(self.OUTBLOCK1, "marketcap", i).strip()
            )

            lst = [
                종목명,
                현재가,
                전일대비구분,
                전일대비,
                등락율,
                누적거래량,
                전일동시간,
                종목코드,
                예상체결가,
                시가,
                고가,
                저가,
                누적거래대금,
                시가총액,
            ]
            result.append(lst)

        columns = [
            "종목명",
            "현재가",
            "전일대비구분",
            "전일대비",
            "등락율",
            "누적거래량",
            "전일동시간",
            "종목코드",
            "예상체결가",
            "시가",
            "고가",
            "저가",
            "누적거래대금",
            "시가총액",
        ]
        df1 = DataFrame(data=result, columns=columns)

        if self.parent is not None:
            self.parent.OnReceiveData(szTrCode, [df, df1])


# 시간대별투자자매매추이(t1602)
class t1702(XAQuery):
    def Query(
        self,
        종목코드="069500",
        종료일자="",
        금액수량구분="0",
        매수매도구분="0",
        누적구분="0",
        CTSDATE="",
        CTSIDX="",
    ):
        self.QueryWaiting(howlong=100.0, limit_count=200, limit_seconds=600)

        self.ActiveX.LoadFromResFile(self.RESFILE)
        self.ActiveX.SetFieldData(self.INBLOCK, "shcode", 0, 종목코드)
        self.ActiveX.SetFieldData(self.INBLOCK, "todt", 0, 종료일자)
        self.ActiveX.SetFieldData(self.INBLOCK, "volvalgb", 0, 금액수량구분)
        self.ActiveX.SetFieldData(self.INBLOCK, "msmdgb", 0, 매수매도구분)
        self.ActiveX.SetFieldData(self.INBLOCK, "cumulgb", 0, 누적구분)
        self.ActiveX.SetFieldData(self.INBLOCK, "cts_date", 0, CTSDATE)
        self.ActiveX.SetFieldData(self.INBLOCK, "cts_idx", 0, CTSIDX)
        self.ActiveX.Request(0)

    def OnReceiveData(self, szTrCode):
        CTSIDX = 0
        CTSDATE = ""
        result = []
        nCount = self.ActiveX.GetBlockCount(self.OUTBLOCK)
        for i in range(nCount):
            CTSIDX = int(self.ActiveX.GetFieldData(self.OUTBLOCK, "cts_idx", i).strip())
            CTSDATE = self.ActiveX.GetFieldData(self.OUTBLOCK, "cts_date", i).strip()

        result = []
        nCount = self.ActiveX.GetBlockCount(self.OUTBLOCK1)
        for i in range(nCount):
            try:
                일자 = self.ActiveX.GetFieldData(self.OUTBLOCK1, "date", i).strip()
            except Exception:
                일자 = ""
            try:
                종가 = int(
                    self.ActiveX.GetFieldData(self.OUTBLOCK1, "close", i).strip()
                )
            except Exception:
                종가 = 0
            try:
                전일대비구분 = self.ActiveX.GetFieldData(
                    self.OUTBLOCK1, "sign", i
                ).strip()
            except Exception:
                전일대비구분 = 0
            try:
                전일대비 = int(
                    self.ActiveX.GetFieldData(self.OUTBLOCK1, "change", i).strip()
                )
            except Exception:
                전일대비 = 0
            try:
                등락율 = float(
                    self.ActiveX.GetFieldData(self.OUTBLOCK1, "diff", i).strip()
                )
            except Exception:
                등락율 = 0
            try:
                누적거래량 = int(
                    self.ActiveX.GetFieldData(self.OUTBLOCK1, "volume", i).strip()
                )
            except Exception:
                누적거래량 = 0
            try:
                사모펀드 = int(
                    self.ActiveX.GetFieldData(self.OUTBLOCK1, "amt0000", i).strip()
                )
            except Exception:
                사모펀드 = 0
            try:
                증권 = int(
                    self.ActiveX.GetFieldData(self.OUTBLOCK1, "amt0001", i).strip()
                )
            except Exception:
                증권 = 0
            try:
                보험 = int(
                    self.ActiveX.GetFieldData(self.OUTBLOCK1, "amt0002", i).strip()
                )
            except Exception:
                보험 = 0
            try:
                투신 = int(
                    self.ActiveX.GetFieldData(self.OUTBLOCK1, "amt0003", i).strip()
                )
            except Exception:
                투신 = 0
            try:
                은행 = int(
                    self.ActiveX.GetFieldData(self.OUTBLOCK1, "amt0004", i).strip()
                )
            except Exception:
                은행 = 0
            try:
                종금 = int(
                    self.ActiveX.GetFieldData(self.OUTBLOCK1, "amt0005", i).strip()
                )
            except Exception:
                종금 = 0
            try:
                기금 = int(
                    self.ActiveX.GetFieldData(self.OUTBLOCK1, "amt0006", i).strip()
                )
            except Exception:
                기금 = 0
            try:
                기타법인 = int(
                    self.ActiveX.GetFieldData(self.OUTBLOCK1, "amt0007", i).strip()
                )
            except Exception:
                기타법인 = 0
            try:
                개인 = int(
                    self.ActiveX.GetFieldData(self.OUTBLOCK1, "amt0008", i).strip()
                )
            except Exception:
                개인 = 0
            try:
                등록외국인 = int(
                    self.ActiveX.GetFieldData(self.OUTBLOCK1, "amt0009", i).strip()
                )
            except Exception:
                등록외국인 = 0
            try:
                미등록외국인 = int(
                    self.ActiveX.GetFieldData(self.OUTBLOCK1, "amt0010", i).strip()
                )
            except Exception:
                미등록외국인 = 0
            try:
                국가외 = int(
                    self.ActiveX.GetFieldData(self.OUTBLOCK1, "amt0011", i).strip()
                )
            except Exception:
                국가외 = 0
            try:
                기관 = int(
                    self.ActiveX.GetFieldData(self.OUTBLOCK1, "amt0018", i).strip()
                )
            except Exception:
                기관 = 0
            try:
                외인계 = int(
                    self.ActiveX.GetFieldData(self.OUTBLOCK1, "amt0088", i).strip()
                )
            except Exception:
                외인계 = 0
            try:
                기타계 = int(
                    self.ActiveX.GetFieldData(self.OUTBLOCK1, "amt0099", i).strip()
                )
            except Exception:
                기타계 = 0

            lst = [
                일자,
                종가,
                전일대비구분,
                전일대비,
                등락율,
                누적거래량,
                사모펀드,
                증권,
                보험,
                투신,
                은행,
                종금,
                기금,
                기타법인,
                개인,
                등록외국인,
                미등록외국인,
                국가외,
                기관,
                외인계,
                기타계,
            ]

            result.append(lst)

        columns = [
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
        df = DataFrame(data=result, columns=columns)

        if self.parent is not None:
            self.parent.OnReceiveData(szTrCode, [CTSIDX, CTSDATE, df])


# 외인기관종목별동향
class t1717(XAQuery):
    def Query(
        self, 종목코드="069500", 구분="0", 시작일자="20170101", 종료일자="20172131"
    ):
        self.QueryWaiting(howlong=100.0, limit_count=200, limit_seconds=600)

        self.ActiveX.LoadFromResFile(self.RESFILE)
        self.ActiveX.SetFieldData(self.INBLOCK, "shcode", 0, 종목코드)
        self.ActiveX.SetFieldData(self.INBLOCK, "gubun", 0, 구분)
        self.ActiveX.SetFieldData(self.INBLOCK, "fromdt", 0, 시작일자)
        self.ActiveX.SetFieldData(self.INBLOCK, "todt", 0, 종료일자)
        self.ActiveX.Request(0)

    def OnReceiveData(self, szTrCode):
        result = []
        nCount = self.ActiveX.GetBlockCount(self.OUTBLOCK)
        for i in range(nCount):
            try:
                일자 = self.ActiveX.GetFieldData(self.OUTBLOCK, "date", i).strip()
            except Exception:
                일자 = ""
            try:
                종가 = int(self.ActiveX.GetFieldData(self.OUTBLOCK, "close", i).strip())
            except Exception:
                종가 = 0
            try:
                전일대비구분 = self.ActiveX.GetFieldData(
                    self.OUTBLOCK, "sign", i
                ).strip()
            except Exception:
                전일대비구분 = 0
            try:
                전일대비 = int(
                    self.ActiveX.GetFieldData(self.OUTBLOCK, "change", i).strip()
                )
            except Exception:
                전일대비 = 0
            try:
                등락율 = float(
                    self.ActiveX.GetFieldData(self.OUTBLOCK, "diff", i).strip()
                )
            except Exception:
                등락율 = 0
            try:
                누적거래량 = int(
                    self.ActiveX.GetFieldData(self.OUTBLOCK, "volume", i).strip()
                )
            except Exception:
                누적거래량 = 0
            try:
                사모펀드_순매수 = int(
                    self.ActiveX.GetFieldData(self.OUTBLOCK, "tjj0000_vol", i).strip()
                )
            except Exception:
                사모펀드_순매수 = 0
            try:
                증권_순매수 = int(
                    self.ActiveX.GetFieldData(self.OUTBLOCK, "tjj0001_vol", i).strip()
                )
            except Exception:
                증권_순매수 = 0
            try:
                보험_순매수 = int(
                    self.ActiveX.GetFieldData(self.OUTBLOCK, "tjj0002_vol", i).strip()
                )
            except Exception:
                보험_순매수 = 0
            try:
                투신_순매수 = int(
                    self.ActiveX.GetFieldData(self.OUTBLOCK, "tjj0003_vol", i).strip()
                )
            except Exception:
                투신_순매수 = 0
            try:
                은행_순매수 = int(
                    self.ActiveX.GetFieldData(self.OUTBLOCK, "tjj0004_vol", i).strip()
                )
            except Exception:
                은행_순매수 = 0
            try:
                종금_순매수 = int(
                    self.ActiveX.GetFieldData(self.OUTBLOCK, "tjj0005_vol", i).strip()
                )
            except Exception:
                종금_순매수 = 0
            try:
                기금_순매수 = int(
                    self.ActiveX.GetFieldData(self.OUTBLOCK, "tjj0006_vol", i).strip()
                )
            except Exception:
                기금_순매수 = 0
            try:
                기타법인_순매수 = int(
                    self.ActiveX.GetFieldData(self.OUTBLOCK, "tjj0007_vol", i).strip()
                )
            except Exception:
                기타법인_순매수 = 0
            try:
                개인_순매수 = int(
                    self.ActiveX.GetFieldData(self.OUTBLOCK, "tjj0008_vol", i).strip()
                )
            except Exception:
                개인_순매수 = 0
            try:
                등록외국인_순매수 = int(
                    self.ActiveX.GetFieldData(self.OUTBLOCK, "tjj0009_vol", i).strip()
                )
            except Exception:
                등록외국인_순매수 = 0
            try:
                미등록외국인_순매수 = int(
                    self.ActiveX.GetFieldData(self.OUTBLOCK, "tjj0010_vol", i).strip()
                )
            except Exception:
                미등록외국인_순매수 = 0
            try:
                국가외_순매수 = int(
                    self.ActiveX.GetFieldData(self.OUTBLOCK, "tjj0011_vol", i).strip()
                )
            except Exception:
                국가외_순매수 = 0
            try:
                기관_순매수 = int(
                    self.ActiveX.GetFieldData(self.OUTBLOCK, "tjj0018_vol", i).strip()
                )
            except Exception:
                기관_순매수 = 0
            try:
                외인계_순매수 = int(
                    self.ActiveX.GetFieldData(self.OUTBLOCK, "tjj0016_vol", i).strip()
                )
            except Exception:
                외인계_순매수 = 0
            try:
                기타계_순매수 = int(
                    self.ActiveX.GetFieldData(self.OUTBLOCK, "tjj0017_vol", i).strip()
                )
            except Exception:
                기타계_순매수 = 0
            try:
                사모펀드_단가 = int(
                    self.ActiveX.GetFieldData(self.OUTBLOCK, "tjj0000_dan", i).strip()
                )
            except Exception:
                사모펀드_단가 = 0
            try:
                증권_단가 = int(
                    self.ActiveX.GetFieldData(self.OUTBLOCK, "tjj0001_dan", i).strip()
                )
            except Exception:
                증권_단가 = 0
            try:
                보험_단가 = int(
                    self.ActiveX.GetFieldData(self.OUTBLOCK, "tjj0002_dan", i).strip()
                )
            except Exception:
                보험_단가 = 0
            try:
                투신_단가 = int(
                    self.ActiveX.GetFieldData(self.OUTBLOCK, "tjj0003_dan", i).strip()
                )
            except Exception:
                투신_단가 = 0
            try:
                은행_단가 = int(
                    self.ActiveX.GetFieldData(self.OUTBLOCK, "tjj0004_dan", i).strip()
                )
            except Exception:
                은행_단가 = 0
            try:
                종금_단가 = int(
                    self.ActiveX.GetFieldData(self.OUTBLOCK, "tjj0005_dan", i).strip()
                )
            except Exception:
                종금_단가 = 0
            try:
                기금_단가 = int(
                    self.ActiveX.GetFieldData(self.OUTBLOCK, "tjj0006_dan", i).strip()
                )
            except Exception:
                기금_단가 = 0
            try:
                기타법인_단가 = int(
                    self.ActiveX.GetFieldData(self.OUTBLOCK, "tjj0007_dan", i).strip()
                )
            except Exception:
                기타법인_단가 = 0
            try:
                개인_단가 = int(
                    self.ActiveX.GetFieldData(self.OUTBLOCK, "tjj0008_dan", i).strip()
                )
            except Exception:
                개인_단가 = 0
            try:
                등록외국인_단가 = int(
                    self.ActiveX.GetFieldData(self.OUTBLOCK, "tjj0009_dan", i).strip()
                )
            except Exception:
                등록외국인_단가 = 0
            try:
                미등록외국인_단가 = int(
                    self.ActiveX.GetFieldData(self.OUTBLOCK, "tjj0010_dan", i).strip()
                )
            except Exception:
                미등록외국인_단가 = 0
            try:
                국가외_단가 = int(
                    self.ActiveX.GetFieldData(self.OUTBLOCK, "tjj0011_dan", i).strip()
                )
            except Exception:
                국가외_단가 = 0
            try:
                기관_단가 = int(
                    self.ActiveX.GetFieldData(self.OUTBLOCK, "tjj0018_dan", i).strip()
                )
            except Exception:
                기관_단가 = 0
            try:
                외인계_단가 = int(
                    self.ActiveX.GetFieldData(self.OUTBLOCK, "tjj0016_dan", i).strip()
                )
            except Exception:
                외인계_단가 = 0
            try:
                기타계_단가 = int(
                    self.ActiveX.GetFieldData(self.OUTBLOCK, "tjj0017_dan", i).strip()
                )
            except Exception:
                기타계_단가 = 0

            lst = [
                일자,
                종가,
                전일대비구분,
                전일대비,
                등락율,
                누적거래량,
                사모펀드_순매수,
                증권_순매수,
                보험_순매수,
                투신_순매수,
                은행_순매수,
                종금_순매수,
                기금_순매수,
                기타법인_순매수,
                개인_순매수,
                등록외국인_순매수,
                미등록외국인_순매수,
                국가외_순매수,
                기관_순매수,
                외인계_순매수,
                기타계_순매수,
                사모펀드_단가,
                증권_단가,
                보험_단가,
                투신_단가,
                은행_단가,
                종금_단가,
                기금_단가,
                기타법인_단가,
                개인_단가,
                등록외국인_단가,
                미등록외국인_단가,
                국가외_단가,
                기관_단가,
                외인계_단가,
                기타계_단가,
            ]

            result.append(lst)

        columns = [
            "일자",
            "종가",
            "전일대비구분",
            "전일대비",
            "등락율",
            "누적거래량",
            "사모펀드_순매수",
            "증권_순매수",
            "보험_순매수",
            "투신_순매수",
            "은행_순매수",
            "종금_순매수",
            "기금_순매수",
            "기타법인_순매수",
            "개인_순매수",
            "등록외국인_순매수",
            "미등록외국인_순매수",
            "국가외_순매수",
            "기관_순매수",
            "외인계_순매수",
            "기타계_순매수",
            "사모펀드_단가",
            "증권_단가",
            "보험_단가",
            "투신_단가",
            "은행_단가",
            "종금_단가",
            "기금_단가",
            "기타법인_단가",
            "개인_단가",
            "등록외국인_단가",
            "미등록외국인_단가",
            "국가외_단가",
            "기관_단가",
            "외인계_단가",
            "기타계_단가",
        ]
        df = DataFrame(data=result, columns=columns)

        if self.parent is not None:
            self.parent.OnReceiveData(szTrCode, [df])


# 주식챠트(틱/n틱)
class t8424(XAQuery):
    def Query(self, 구분=""):
        self.QueryWaiting(howlong=100.0, limit_count=None, limit_seconds=600)

        self.ActiveX.LoadFromResFile(self.RESFILE)
        self.ActiveX.SetFieldData(self.INBLOCK, "gubun1", 0, 구분)
        self.ActiveX.Request(0)

    def OnReceiveData(self, szTrCode):
        result = []
        nCount = self.ActiveX.GetBlockCount(self.OUTBLOCK)
        for i in range(nCount):
            업종명 = self.ActiveX.GetFieldData(self.OUTBLOCK, "hname", i).strip()
            업종코드 = self.ActiveX.GetFieldData(self.OUTBLOCK, "upcode", i).strip()

            lst = [업종명, 업종코드]
            result.append(lst)

        columns = ["업종명", "업종코드"]
        df = DataFrame(data=result, columns=columns)

        if self.parent is not None:
            self.parent.OnReceiveData(szTrCode, [df])


# 전체테마
class t8425(XAQuery):
    def Query(self):
        self.QueryWaiting(howlong=100.0, limit_count=None, limit_seconds=600)

        self.ActiveX.LoadFromResFile(self.RESFILE)
        self.ActiveX.SetFieldData(self.INBLOCK, "dummy", 0, "0")
        self.ActiveX.Request(0)

    def OnReceiveData(self, szTrCode):
        result = []
        nCount = self.ActiveX.GetBlockCount(self.OUTBLOCK)
        for i in range(nCount):
            테마명 = self.ActiveX.GetFieldData(self.OUTBLOCK, "tmname", i).strip()
            테마코드 = self.ActiveX.GetFieldData(self.OUTBLOCK, "tmcode", i).strip()

            lst = [테마명, 테마코드]
            result.append(lst)

        columns = ["테마명", "테마코드"]
        df = DataFrame(data=result, columns=columns)

        if self.parent is not None:
            self.parent.OnReceiveData(szTrCode, [df])


# 주식종목코드조회
class t8430(XAQuery):
    def Query(self, 구분="0"):
        self.QueryWaiting(howlong=50.0, limit_count=None, limit_seconds=600)

        self.ActiveX.LoadFromResFile(self.RESFILE)
        self.ActiveX.SetFieldData(self.INBLOCK, "gubun", 0, 구분)
        self.ActiveX.Request(0)

    def OnReceiveData(self, szTrCode):
        result = []
        nCount = self.ActiveX.GetBlockCount(self.OUTBLOCK)
        for i in range(nCount):
            종목명 = self.ActiveX.GetFieldData(self.OUTBLOCK, "hname", i).strip()
            종목코드 = self.ActiveX.GetFieldData(self.OUTBLOCK, "shcode", i).strip()
            확장코드 = self.ActiveX.GetFieldData(self.OUTBLOCK, "expcode", i).strip()
            ETF구분 = int(
                self.ActiveX.GetFieldData(self.OUTBLOCK, "etfgubun", i).strip()
            )
            상한가 = int(
                self.ActiveX.GetFieldData(self.OUTBLOCK, "uplmtprice", i).strip()
            )
            하한가 = int(
                self.ActiveX.GetFieldData(self.OUTBLOCK, "dnlmtprice", i).strip()
            )
            전일가 = int(
                self.ActiveX.GetFieldData(self.OUTBLOCK, "jnilclose", i).strip()
            )
            주문수량단위 = int(
                self.ActiveX.GetFieldData(self.OUTBLOCK, "memedan", i).strip()
            )
            기준가 = int(
                self.ActiveX.GetFieldData(self.OUTBLOCK, "recprice", i).strip()
            )
            구분 = int(self.ActiveX.GetFieldData(self.OUTBLOCK, "gubun", i).strip())

            lst = [
                종목명,
                종목코드,
                확장코드,
                ETF구분,
                상한가,
                하한가,
                전일가,
                주문수량단위,
                기준가,
                구분,
            ]
            result.append(lst)

        columns = [
            "종목명",
            "종목코드",
            "확장코드",
            "ETF구분",
            "상한가",
            "하한가",
            "전일가",
            "주문수량단위",
            "기준가",
            "구분",
        ]
        df = DataFrame(data=result, columns=columns)

        if self.parent is not None:
            self.parent.OnReceiveData(szTrCode, [df])


# 주식종목코드조회(API용)
class t8436(XAQuery):
    def Query(self, 구분="0"):
        self.QueryWaiting(howlong=50.0, limit_count=None, limit_seconds=600)

        self.ActiveX.LoadFromResFile(self.RESFILE)
        self.ActiveX.SetFieldData(self.INBLOCK, "gubun", 0, 구분)
        self.ActiveX.Request(0)

    def OnReceiveData(self, szTrCode):
        result = []
        nCount = self.ActiveX.GetBlockCount(self.OUTBLOCK)
        for i in range(nCount):
            종목명 = self.ActiveX.GetFieldData(self.OUTBLOCK, "hname", i).strip()
            종목코드 = self.ActiveX.GetFieldData(self.OUTBLOCK, "shcode", i).strip()
            확장코드 = self.ActiveX.GetFieldData(self.OUTBLOCK, "expcode", i).strip()
            ETF구분 = int(
                self.ActiveX.GetFieldData(self.OUTBLOCK, "etfgubun", i).strip()
            )
            상한가 = int(
                self.ActiveX.GetFieldData(self.OUTBLOCK, "uplmtprice", i).strip()
            )
            하한가 = int(
                self.ActiveX.GetFieldData(self.OUTBLOCK, "dnlmtprice", i).strip()
            )
            전일가 = int(
                self.ActiveX.GetFieldData(self.OUTBLOCK, "jnilclose", i).strip()
            )
            주문수량단위 = int(
                self.ActiveX.GetFieldData(self.OUTBLOCK, "memedan", i).strip()
            )
            기준가 = int(
                self.ActiveX.GetFieldData(self.OUTBLOCK, "recprice", i).strip()
            )
            구분 = int(self.ActiveX.GetFieldData(self.OUTBLOCK, "gubun", i).strip())
            증권그룹 = self.ActiveX.GetFieldData(self.OUTBLOCK, "bu12gubun", i).strip()
            기업인수목적회사여부 = self.ActiveX.GetFieldData(
                self.OUTBLOCK, "spac_gubun", i
            ).strip()

            lst = [
                종목명,
                종목코드,
                확장코드,
                ETF구분,
                상한가,
                하한가,
                전일가,
                주문수량단위,
                기준가,
                구분,
                증권그룹,
                기업인수목적회사여부,
            ]
            result.append(lst)

        columns = [
            "종목명",
            "종목코드",
            "확장코드",
            "ETF구분",
            "상한가",
            "하한가",
            "전일가",
            "주문수량단위",
            "기준가",
            "구분",
            "증권그룹",
            "기업인수목적회사여부",
        ]
        df = DataFrame(data=result, columns=columns)

        if self.parent is not None:
            self.parent.OnReceiveData(szTrCode, [df])


# 종목검색
class t1833(XAQuery):
    def Query(self, 종목검색파일=""):
        self.QueryWaiting(howlong=100.0, limit_count=None, limit_seconds=600)

        self.ActiveX.LoadFromResFile(self.RESFILE)
        self.ActiveX.SetFieldData(self.INBLOCK, "dummy", 0, "")
        self.ActiveX.RequestService(self.MYNAME, 종목검색파일)

    def OnReceiveData(self, szTrCode):
        nCount = self.ActiveX.GetBlockCount(self.OUTBLOCK)
        for i in range(nCount):
            검색종목수 = int(
                self.ActiveX.GetFieldData(self.OUTBLOCK, "JongCnt", i).strip()
            )

        result = []
        nCount = self.ActiveX.GetBlockCount(self.OUTBLOCK1)
        for i in range(nCount):
            종목코드 = self.ActiveX.GetFieldData(self.OUTBLOCK1, "shcode", i).strip()
            종목명 = self.ActiveX.GetFieldData(self.OUTBLOCK1, "hname", i).strip()
            전일대비구분 = self.ActiveX.GetFieldData(self.OUTBLOCK1, "sign", i).strip()
            연속봉수 = int(
                self.ActiveX.GetFieldData(self.OUTBLOCK1, "signcnt", i).strip()
            )
            현재가 = int(self.ActiveX.GetFieldData(self.OUTBLOCK1, "close", i).strip())
            전일대비 = int(
                self.ActiveX.GetFieldData(self.OUTBLOCK1, "change", i).strip()
            )
            등락율 = float(self.ActiveX.GetFieldData(self.OUTBLOCK1, "diff", i).strip())
            거래량 = int(self.ActiveX.GetFieldData(self.OUTBLOCK1, "volume", i).strip())

            lst = [
                종목코드,
                종목명,
                전일대비구분,
                연속봉수,
                현재가,
                전일대비,
                등락율,
                거래량,
            ]
            result.append(lst)

        columns = [
            "종목코드",
            "종목명",
            "전일대비구분",
            "연속봉수",
            "현재가",
            "전일대비",
            "등락율",
            "거래량",
        ]
        df = DataFrame(data=result, columns=columns)

        if self.parent is not None:
            self.parent.OnReceiveData(szTrCode, [검색종목수, df])


# e종목검색
class t1857(XAQuery):
    def Query(self, 실시간구분, 종목검색구분, 종목검색입력값):
        print(
            "t1857(e종목검색) Query ==------->>>",
            self,
            실시간구분,
            종목검색구분,
            종목검색입력값,
        )
        self.QueryWaiting(howlong=100.0, limit_count=200, limit_seconds=600)

        self.실시간키 = ""

        # self.종목검색파일 = "%s\\ADF\\%s.ADF" % (self.RESDIR, 종목검색파일)
        self.ActiveX.LoadFromResFile(self.RESFILE)
        self.ActiveX.ClearBlockdata(self.OUTBLOCK)
        self.ActiveX.ClearBlockdata(self.OUTBLOCK1)
        self.ActiveX.SetFieldData(self.INBLOCK, "sRealFlag", 0, 실시간구분)
        self.ActiveX.SetFieldData(self.INBLOCK, "sSearchFlag", 0, 종목검색구분)
        self.ActiveX.SetFieldData(self.INBLOCK, "query_index", 0, 종목검색입력값)
        self.ActiveX.RequestService(self.MYNAME, "")

    def OnReceiveData(self, szTrCode):

        try:
            try:
                검색종목수 = int(
                    self.ActiveX.GetFieldData(self.OUTBLOCK, "result_count", 0).strip()
                )
                print(
                    "t1857(e종목검색) <<<-------== OnReceiveData: %s %s 겸색종목수: %d"
                    % (self, szTrCode, 검색종목수)
                )
            except Exception as e:
                print(repr(e))
                검색종목수 = 0

            포착시간 = self.ActiveX.GetFieldData(
                self.OUTBLOCK, "result_time", 0
            ).strip()
            실시간키 = self.ActiveX.GetFieldData(self.OUTBLOCK, "AlertNum", 0).strip()
            self.실시간키 = 실시간키

            result = []
            for i in range(검색종목수):
                종목코드 = self.ActiveX.GetFieldData(
                    self.OUTBLOCK1, "shcode", i
                ).strip()
                종목명 = self.ActiveX.GetFieldData(self.OUTBLOCK1, "hname", i).strip()
                현재가 = int(
                    self.ActiveX.GetFieldData(self.OUTBLOCK1, "price", i).strip()
                )
                전일대비구분 = self.ActiveX.GetFieldData(
                    self.OUTBLOCK1, "sign", i
                ).strip()
                전일대비 = int(
                    self.ActiveX.GetFieldData(self.OUTBLOCK1, "change", i).strip()
                )
                등락율 = float(
                    self.ActiveX.GetFieldData(self.OUTBLOCK1, "diff", i).strip()
                )
                거래량 = int(
                    self.ActiveX.GetFieldData(self.OUTBLOCK1, "volume", i).strip()
                )
                종목상태 = self.ActiveX.GetFieldData(
                    self.OUTBLOCK1, "JobFlag", i
                ).strip()

                lst = [
                    종목코드,
                    종목명,
                    현재가,
                    전일대비구분,
                    전일대비,
                    등락율,
                    거래량,
                    종목상태,
                ]
                result.append(lst)

            columns = [
                "종목코드",
                "종목명",
                "현재가",
                "전일대비구분",
                "전일대비",
                "등락율",
                "거래량",
                "종목상태",
            ]
            df = DataFrame(data=result, columns=columns)

            if self.parent is not None:
                self.parent.OnReceiveData(
                    szTrCode, [self.식별자, 검색종목수, 포착시간, 실시간키, df]
                )
        except Exception as e:
            print(
                "%s-%s %s: %s"
                % (self.__class__.__name__, get_funcname(), get_linenumber(), e)
            )

    def OnReceiveSearchRealData(self, szTrCode):
        result = dict()
        # print(f"t1857 OnReceiveSearchRealData:{szTrCode}, {result}")
        result["종목코드"] = self.ActiveX.GetFieldSearchRealData(
            self.OUTBLOCK1, "shcode"
        ).strip()
        result["종목명"] = self.ActiveX.GetFieldSearchRealData(
            self.OUTBLOCK1, "hname"
        ).strip()
        result["현재가"] = int(
            self.ActiveX.GetFieldSearchRealData(self.OUTBLOCK1, "price").strip()
        )
        result["전일대비구분"] = self.ActiveX.GetFieldSearchRealData(
            self.OUTBLOCK1, "sign"
        ).strip()
        result["전일대비"] = int(
            self.ActiveX.GetFieldSearchRealData(self.OUTBLOCK1, "change").strip()
        )
        result["등락율"] = float(
            self.ActiveX.GetFieldSearchRealData(self.OUTBLOCK1, "diff").strip()
        )
        result["거래량"] = int(
            self.ActiveX.GetFieldSearchRealData(self.OUTBLOCK1, "volume").strip()
        )
        result["종목상태"] = self.ActiveX.GetFieldSearchRealData(
            self.OUTBLOCK1, "JobFlag"
        ).strip()

        if self.parent is not None:
            self.parent.OnReceiveSearchRealData(szTrCode, [self.식별자, result])

    def RemoveService(self):
        if self.실시간키 != "":
            result = self.ActiveX.RemoveService(self.MYNAME, self.실시간키)


# 주식종목코드조회(API용)
# 사용안함
class ChartIndex(XAQuery):
    def Query(
        self,
        지표ID="",
        지표명="",
        지표조건설정="",
        시장구분="",
        주기구분="",
        종목코드="",
        요청건수="500",
        단위="",
        시작일자="",
        종료일자="",
        수정주가반영여부="",
        갭보정여부="",
        실시간데이터수신자동등록여부="0",
    ):
        self.QueryWaiting(howlong=100.0, limit_count=200, limit_seconds=600)

        self.ActiveX.LoadFromResFile(self.RESFILE)
        self.ActiveX.SetFieldData(self.INBLOCK, "indexid", 0, 지표ID)
        self.ActiveX.SetFieldData(self.INBLOCK, "indexname", 0, 지표명)
        self.ActiveX.SetFieldData(self.INBLOCK, "indexparam", 0, 지표조건설정)
        self.ActiveX.SetFieldData(self.INBLOCK, "market", 0, 시장구분)
        self.ActiveX.SetFieldData(self.INBLOCK, "period", 0, 주기구분)
        self.ActiveX.SetFieldData(self.INBLOCK, "shcode", 0, 종목코드)
        self.ActiveX.SetFieldData(self.INBLOCK, "qrycnt", 0, 요청건수)
        self.ActiveX.SetFieldData(self.INBLOCK, "ncnt", 0, 단위)
        self.ActiveX.SetFieldData(self.INBLOCK, "sdate", 0, 시작일자)
        self.ActiveX.SetFieldData(self.INBLOCK, "edate", 0, 종료일자)
        self.ActiveX.SetFieldData(self.INBLOCK, "Isamend", 0, 수정주가반영여부)
        self.ActiveX.SetFieldData(self.INBLOCK, "Isgab", 0, 갭보정여부)
        self.ActiveX.SetFieldData(
            self.INBLOCK, "IsReal", 0, 실시간데이터수신자동등록여부
        )
        self.ActiveX.RequestService("ChartIndex", "")

    def RemoveService(self):
        try:
            지표ID = self.ActiveX.GetFieldData(self.OUTBLOCK, "indexid", 0).strip()
            self.ActiveX.RemoveService("ChartIndex", 지표ID)
        except Exception:
            pass

    def OnReceiveData(self, szTrCode):
        nCount = self.ActiveX.GetBlockCount(self.OUTBLOCK)
        for i in range(nCount):
            지표ID = int(self.ActiveX.GetFieldData(self.OUTBLOCK, "indexid", i).strip())
            레코드갯수 = int(
                self.ActiveX.GetFieldData(self.OUTBLOCK, "rec_cnt", i).strip()
            )
            유효데이터컬럼갯수 = int(
                self.ActiveX.GetFieldData(self.OUTBLOCK, "validdata_cnt", i).strip()
            )

        result = []
        nCount = self.ActiveX.GetBlockCount(self.OUTBLOCK1)
        for i in range(nCount):
            일자 = self.ActiveX.GetFieldData(self.OUTBLOCK1, "date", i).strip()
            시간 = self.ActiveX.GetFieldData(self.OUTBLOCK1, "time", i).strip()
            시가 = float(self.ActiveX.GetFieldData(self.OUTBLOCK1, "open", i).strip())
            고가 = float(self.ActiveX.GetFieldData(self.OUTBLOCK1, "high", i).strip())
            저가 = float(self.ActiveX.GetFieldData(self.OUTBLOCK1, "low", i).strip())
            종가 = float(self.ActiveX.GetFieldData(self.OUTBLOCK1, "close", i).strip())
            거래량 = float(
                self.ActiveX.GetFieldData(self.OUTBLOCK1, "volume", i).strip()
            )
            지표값1 = float(
                self.ActiveX.GetFieldData(self.OUTBLOCK1, "value1", i).strip()
            )
            지표값2 = float(
                self.ActiveX.GetFieldData(self.OUTBLOCK1, "value2", i).strip()
            )
            지표값3 = float(
                self.ActiveX.GetFieldData(self.OUTBLOCK1, "value3", i).strip()
            )
            지표값4 = float(
                self.ActiveX.GetFieldData(self.OUTBLOCK1, "value4", i).strip()
            )
            지표값5 = float(
                self.ActiveX.GetFieldData(self.OUTBLOCK1, "value5", i).strip()
            )
            위치 = int(self.ActiveX.GetFieldData(self.OUTBLOCK1, "pos", i).strip())

            lst = [
                일자,
                시간,
                시가,
                고가,
                저가,
                종가,
                거래량,
                지표값1,
                지표값2,
                지표값3,
                지표값4,
                지표값5,
                위치,
            ]
            result.append(lst)

        columns = [
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
        df = DataFrame(data=result, columns=columns)

        if self.parent is not None:
            self.parent.OnReceiveData(
                szTrCode, [self.식별자, 지표ID, 레코드갯수, 유효데이터컬럼갯수, df]
            )

    def OnReceiveChartRealData(self, szTrCode):
        지표ID = self.ActiveX.GetFieldChartRealData(self.OUTBLOCK, "indexid").strip()
        레코드갯수 = self.ActiveX.GetFieldChartRealData(
            self.OUTBLOCK, "rec_cnt"
        ).strip()
        유효데이터컬럼갯수 = self.ActiveX.GetFieldChartRealData(
            self.OUTBLOCK, "validdata_cnt"
        ).strip()

        result = dict()
        result["일자"] = self.ActiveX.GetFieldChartRealData(
            self.OUTBLOCK1, "date"
        ).strip()
        result["시간"] = self.ActiveX.GetFieldChartRealData(
            self.OUTBLOCK1, "time"
        ).strip()
        result["시가"] = float(
            self.ActiveX.GetFieldChartRealData(self.OUTBLOCK1, "open").strip()
        )
        result["고가"] = float(
            self.ActiveX.GetFieldChartRealData(self.OUTBLOCK1, "high").strip()
        )
        result["저가"] = float(
            self.ActiveX.GetFieldChartRealData(self.OUTBLOCK1, "low").strip()
        )
        result["종가"] = float(
            self.ActiveX.GetFieldChartRealData(self.OUTBLOCK1, "close").strip()
        )
        result["거래량"] = float(
            self.ActiveX.GetFieldChartRealData(self.OUTBLOCK1, "volume").strip()
        )
        result["지표값1"] = float(
            self.ActiveX.GetFieldChartRealData(self.OUTBLOCK1, "value1").strip()
        )
        result["지표값2"] = float(
            self.ActiveX.GetFieldChartRealData(self.OUTBLOCK1, "value2").strip()
        )
        result["지표값3"] = float(
            self.ActiveX.GetFieldChartRealData(self.OUTBLOCK1, "value3").strip()
        )
        result["지표값4"] = float(
            self.ActiveX.GetFieldChartRealData(self.OUTBLOCK1, "value4").strip()
        )
        result["지표값5"] = float(
            self.ActiveX.GetFieldChartRealData(self.OUTBLOCK1, "value5").strip()
        )
        result["위치"] = int(
            self.ActiveX.GetFieldChartRealData(self.OUTBLOCK1, "pos").strip()
        )

        if self.parent is not None:
            self.parent.OnReceiveChartRealData(
                szTrCode, [self.식별자, 지표ID, 레코드갯수, 유효데이터컬럼갯수, result]
            )


# 선물옵션 정상주문,CFOAT00100
class t0167(XAQuery):
    def Query(self, id=""):
        self.QueryWaiting(howlong=20.0, limit_count=None, limit_seconds=600)

        self.ActiveX.LoadFromResFile(self.RESFILE)
        self.ActiveX.ClearBlockdata(self.OUTBLOCK)
        self.ActiveX.SetFieldData(self.INBLOCK, "id", 0, id)
        self.ActiveX.Request(0)

    def OnReceiveData(self, szTrCode):
        dt = self.ActiveX.GetFieldData(self.OUTBLOCK, "dt", 0).strip()
        time = self.ActiveX.GetFieldData(self.OUTBLOCK, "time", 0).strip()

        if self.parent is not None:
            self.parent.OnReceiveData(szTrCode, [dt, time])
