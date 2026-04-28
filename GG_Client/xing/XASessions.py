# -*- coding: utf-8 -*-


import inspect
import win32com.client


class XASessionEvents(object):
    def __init__(self):
        self.parent = None

    def set_parent(self, parent):
        self.parent = parent

    def OnLogin(self, code, msg):
        클래스이름 = self.__class__.__name__
        함수이름 = inspect.currentframe().f_code.co_name
        print("%s-%s " % (클래스이름, 함수이름))
        if self.parent is not None:
            self.parent.OnLogin(code, msg)

    def OnLogout(self):
        클래스이름 = self.__class__.__name__
        함수이름 = inspect.currentframe().f_code.co_name
        # print("%s-%s " % (클래스이름, 함수이름))
        if self.parent is not None:
            self.parent.OnLogout()

    def OnDisconnect(self):
        클래스이름 = self.__class__.__name__
        함수이름 = inspect.currentframe().f_code.co_name
        # print("%s-%s " % (클래스이름, 함수이름))
        if self.parent is not None:
            self.parent.OnDisconnect()


class XASession:
    def __init__(self, parent=None):
        try:
            # pythoncom.CoInitialize()  # COM 초기화 추가
            self.ActiveX = win32com.client.DispatchWithEvents(
                "XA_Session.XASession", XASessionEvents
            )
            self.ActiveX.SetMode("_XINGAPI7_", "TRUE")
            if parent == None:
                self.ActiveX.set_parent(parent=self)
            else:
                self.ActiveX.set_parent(parent=parent)
            print("XASession ActiveX 실행 성공!")
        except Exception as e:
            print(f"XASession ActiveX 실행 실패: {e}")

    def login(
        self,
        url="demo.ls-sec.co.kr",
        port=200001,
        svrtype=0,
        id="userid",
        pwd="password",
        cert="공인인증 비밀번호",
    ):
        result = self.ActiveX.ConnectServer(url, port)
        if not result:
            nErrCode = self.ActiveX.GetLastError()
            strErrMsg = self.ActiveX.GetErrorMessage(nErrCode)
            print("nErrCode : ", nErrCode)
            print("strErrMsg : ", strErrMsg)
            return (False, nErrCode, strErrMsg)

        self.ActiveX.Login(id, pwd, cert, svrtype, 0)

        return (True, 0, "OK")

    def logout(self):
        self.ActiveX.Logout()

    def disconnect(self):
        self.ActiveX.DisconnectServer()

    def IsConnected(self):
        return self.ActiveX.IsConnected()

    #
    # def OnLogin(self, code, msg):
    #     클래스이름 = self.__class__.__name__
    #     함수이름 = inspect.currentframe().f_code.co_name
    #     print("%s-%s " % (클래스이름, 함수이름), code, msg)
    #
    # def OnLogout(self):
    #     클래스이름 = self.__class__.__name__
    #     함수이름 = inspect.currentframe().f_code.co_name
    #     print("%s-%s " % (클래스이름, 함수이름))
    #
    # def OnDisconnect(self):
    #     클래스이름 = self.__class__.__name__
    #     함수이름 = inspect.currentframe().f_code.co_name
    #     print("%s-%s " % (클래스이름, 함수이름))
