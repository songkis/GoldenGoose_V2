class CPortStock(object):
    def __init__(self, 매수일, 종목코드, 종목명, 매수가, 수량, 매수후고가=0, STATUS=''
                , 잔고수량=0, 평균단가=0, 매입금액=0, 현재가=0, 평가금액=0, 평가손익=0, 수익율=0.0):
        self.매수일 = 매수일
        self.종목코드 = 종목코드
        self.종목명 = 종목명
        self.매수가 = 매수가
        self.현재가 = 0
        self.수량 = 수량
        self.STATUS = STATUS
        self.매수후고가 = int(매수후고가 if 매수후고가 > 0 else 매수가)
        self.잔고수량 = 잔고수량
        self.평균단가 = 평균단가
        self.매입금액 = 매입금액
        self.평가금액 = 평가금액
        self.평가손익 = 평가손익
        self.수익율 = 수익율