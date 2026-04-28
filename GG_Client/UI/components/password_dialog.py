from PyQt5.QtWidgets import QDialog, QVBoxLayout, QLabel, QLineEdit, QPushButton

logger = None  # 초기값


def set_logger(external_logger):
    global logger
    logger = external_logger


# 비밀번호 입력을 받는 대화창
class PasswordDialog(QDialog):
    def __init__(self, parent):
        super().__init__()
        self.parent = parent
        self.setWindowTitle(
            self.parent.parent.account_dict["거래환경"] + "사용자비밀번호 입력"
        )
        self.layout = QVBoxLayout()
        self.label = QLabel(
            self.parent.parent.account_dict["거래환경"] + "사용자비밀번호:", self
        )
        self.layout.addWidget(self.label)
        self.password_input = QLineEdit(self)
        self.password_input.setEchoMode(QLineEdit.Password)
        self.layout.addWidget(self.password_input)
        self.submit_button = QPushButton("확인", self)
        self.submit_button.clicked.connect(self.on_submit)
        self.layout.addWidget(self.submit_button)
        self.setLayout(self.layout)

    def on_submit(self):
        entered_password = self.password_input.text()
        if self.check_password(entered_password):
            self.accept()  # 비밀번호가 맞으면 대화창을 닫고, 메인 화면 표시
        else:
            self.reject()  # 비밀번호가 틀리면 대화창 닫기

    def check_password(self, input_password):
        trxPwd = self.parent.parent.account_dict["사용자비밀번호"]
        if len(trxPwd) > 0 and input_password == trxPwd:
            return True
        elif len(trxPwd) == 0:
            return True  # 등록되지 않은경우 등록을 위해 환경설정 창 오픈.
        else:
            return False
