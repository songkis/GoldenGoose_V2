import pandas as pd
from PyQt5.QtWidgets import QDialog, QMessageBox
from PyQt5.QtCore import Qt
from PyQt5 import uic
from UI.components.password_dialog import PasswordDialog
from UI.components.models import PandasModel
from UI.components.delegates import ReadOnlyItemDelegate, ComboBoxDelegate
from UI.resource_resolver import resource_path
from util.CommUtils import get_db_connection, decrypt_data, is_encrypted, encrypt_data
from SQL.sql import REPLACE_TB_AI_CONF

logger = None  # 초기값


def set_logger(external_logger):
    global logger
    logger = external_logger


# UI 파일 로드
try:
    FORM_CLASS, _ = uic.loadUiType(resource_path("UI/Configuration.ui"))
except Exception as e:
    logger.error(f"Failed to load UI: {e}")
    raise


class View_Configuration(QDialog, FORM_CLASS):
    def __init__(self, parent=None):
        super(View_Configuration, self).__init__(parent)
        self.setAttribute(Qt.WA_DeleteOnClose)
        self.setupUi(self)
        self.parent = parent
        # 모델 초기화
        self.modelAcc = PandasModel()
        self.tableView_Acc.setModel(self.modelAcc)
        self.modelAI = PandasModel()
        self.tableView_AI.setModel(self.modelAI)

        # Save 버튼 이벤트 연결
        self.buttonBox.accepted.connect(self.on_save_clicked)
        # Cancel 버튼 이벤트 연결 (선택 사항)
        self.buttonBox.rejected.connect(self.reject)

        self.open_password_dialog()

    def open_password_dialog(self):
        dialog = PasswordDialog(self)
        # 비밀번호가 맞으면 메인 화면을 표시
        if dialog.exec_() == QDialog.Accepted:
            # 데이터 불러오기
            self.load_data()
        else:
            QMessageBox.information(
                self, "비밀번호 불일치", "사용자비밀번호가 일치하지 않습니다."
            )
            self.reject()
            return

    def load_data(self):
        """SQLite에서 데이터를 불러와 테이블에 표시"""
        with get_db_connection() as conn:  # with 문을 사용하여 자동으로 conn.close()
            # TB_ACC_INFO 불러오기
            queryAcc = "SELECT * FROM TB_ACC_INFO"
            dfAcc = pd.read_sql(queryAcc, conn, index_col=None)

        # 컬럼명을 한글로 매핑
        acc_column_mapping = {
            "SYS_ID": "접속시스템ID",
            "SYS_NM": "접속시스템명",
            "URL": "접속URL",
            "ACC_NO": "계좌번호",
            "ACC_NM": "계좌명",
            "USER_ID": "사용자 ID",
            "PWD": "비밀번호",
            "CERT_PWD": "공인인증 비밀번호",
            "TRX_PWD": "거래 비밀번호",
        }
        acc_ediatble_column_mapping = {
            "ACC_NO": "계좌번호",
            "USER_ID": "사용자 ID",
            "PWD": "비밀번호",
            "CERT_PWD": "공인인증 비밀번호",
            "TRX_PWD": "거래 비밀번호",
        }
        dfAcc.rename(columns=acc_column_mapping, inplace=True)

        # 데이터 복호화 처리
        for column in acc_column_mapping.values():  # 한글 컬럼명 기준
            dfAcc[column] = dfAcc[column].apply(
                lambda x: decrypt_data(x) if is_encrypted(x) else x
            )

        # 테이블에 delegate를 설정하는 부분
        acc_editable_columns = list(acc_ediatble_column_mapping.values())
        acc_delegate = ReadOnlyItemDelegate(editable_columns=acc_editable_columns)
        self.tableView_Acc.setItemDelegate(acc_delegate)

        # PandasModel을 테이블에 설정
        self.modelAcc = PandasModel(dfAcc, editable_columns=acc_editable_columns)
        self.tableView_Acc.setModel(self.modelAcc)

        # QTableView의 인덱스 숨기기
        self.tableView_Acc.verticalHeader().setVisible(False)
        # 🔹 첫 번째 컬럼 숨기기
        # self.tableView_Acc.setColumnHidden(0, True)

        header_style = """
                QHeaderView::section {
                    font-weight: bold;
                    font-size: 12pt;
                    text-align: center;
                    background-color: #E0E0E0;  /* 옅은 회색 */
                    border: 1px solid #B0B0B0;  /* 테두리 색상 */
                    padding: 5px;
                }
            """
        # 컬럼 타이틀 스타일 설정
        header = self.tableView_Acc.horizontalHeader()
        header.setStyleSheet(header_style)

        # 컬럼 크기 조정
        for i in range(dfAcc.shape[1]):
            self.tableView_Acc.resizeColumnToContents(i)

        with get_db_connection() as conn:  # with 문을 사용하여 자동으로 conn.close()
            # TB_AI_CONF 불러오기
            queryAI = "SELECT CONF_ID, CONF_KEY, CONF_VALUE, CONF_CMNT FROM TB_AI_CONF ORDER BY CONF_ID "
            dfAI = pd.read_sql(queryAI, conn, index_col=None)

        # 컬럼명을 한글로 매핑
        conf_column_mapping = {
            "CONF_ID": "설정ID",
            "CONF_KEY": "설정항목",
            "CONF_VALUE": "설정값",
            "CONF_CMNT": "설명",
        }
        conf_ediatble_column_mapping = {"CONF_VALUE": "설정값"}
        dfAI.rename(columns=conf_column_mapping, inplace=True)

        # 테이블에 delegate를 설정하는 부분
        conf_editable_columns = list(conf_ediatble_column_mapping.values())
        conf_delegate = ReadOnlyItemDelegate(editable_columns=conf_editable_columns)
        self.tableView_AI.setItemDelegate(conf_delegate)

        # PandasModel을 테이블에 설정
        self.modelAI = PandasModel(dfAI, editable_columns=conf_editable_columns)
        self.tableView_AI.setModel(self.modelAI)

        # QTableView의 인덱스 숨기기
        self.tableView_AI.verticalHeader().setVisible(False)
        # 🔹 첫 번째 컬럼 숨기기
        # self.tableView_AI.setColumnHidden(0, True)

        # 컬럼 타이틀 스타일 설정
        header = self.tableView_AI.horizontalHeader()
        header.setStyleSheet(header_style)

        self.tableView_AI.setColumnHidden(0, True)  # 첫 번째 컬럼 숨기기
        for i in range(dfAI.shape[1]):
            self.tableView_AI.resizeColumnToContents(i)

        # 콤보박스 옵션 지정
        combo_options = {
            (0, 2): [("모의", "1"), ("실전", "2")],  # 1행의 '설정값' 컬럼
            (5, 2): [
                ("1분", "1"),
                ("3분", "3"),
                ("5분", "5"),
                ("10분", "10"),
            ],  # 6행의 '설정값' 컬럼
            (12, 2): [("종료", "0"), ("유지", "1")],  # 13행의 '설정값' 컬럼
            (13, 2): [("매수", "Y"), ("미매수", "N")],  # 14행의 '설정값' 컬럼
            (14, 2): [("매수", "Y"), ("미매수", "N")],  # 15행의 '설정값' 컬럼
            (15, 2): [("매수", "Y"), ("미매수", "N")],  # 16행의 '설정값' 컬럼
        }
        combo_delegate = ComboBoxDelegate(options_dict=combo_options)
        # 기존의 ReadOnlyDelegate 설정은 유지하고, 콤보박스 우선 적용
        self.tableView_AI.setItemDelegate(combo_delegate)

    def on_save_clicked(self):
        """Save 버튼 클릭 시 실행할 함수"""
        with get_db_connection() as conn:  # with 문을 사용하여 자동으로 conn.close()
            cursor = conn.cursor()
            # self.modelAcc와 self.modelAI에서 데이터를 추출
            dfAcc_updated = self.modelAcc._data  # TB_ACC_INFO의 데이터
            dfAI_updated = self.modelAI._data  # TB_AI_CONF의 데이터
            # TB_ACC_INFO에 데이터 저장
            for _, row in dfAcc_updated.iterrows():
                encrypted_data = (
                    encrypt_data(row["계좌번호"]),
                    encrypt_data(row["사용자 ID"]),
                    encrypt_data(row["비밀번호"]),
                    encrypt_data(row["공인인증 비밀번호"]),
                    encrypt_data(row["거래 비밀번호"]),
                )
                query = """
                    REPLACE INTO TB_ACC_INFO (SYS_ID, SYS_NM, URL, ACC_NM, ACC_NO, USER_ID, PWD, CERT_PWD, TRX_PWD)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """
                cursor.execute(
                    query,
                    tuple(
                        row[
                            ["접속시스템ID", "접속시스템명", "접속URL", "계좌명"]
                        ].values
                    )
                    + encrypted_data,
                )

            # TB_AI_CONF에 데이터 저장
            for _, row in dfAI_updated.iterrows():
                cursor.execute(REPLACE_TB_AI_CONF, tuple(row))
            cursor.close()
            cursor.close()
        QMessageBox.information(
            self, "저장", "설정이 저장되었습니다!\nGoldenGoose를 재시작해 주세요!"
        )


try:
    FORM_CLASS_VERSION, _ = uic.loadUiType(resource_path("UI/버전.ui"))
except Exception as e:
    logger.error(f"Failed to load Version UI: {e}")

    # dummy class to prevent crash if UI missing, though it will fail on instantiation
    class FORM_CLASS_VERSION:
        pass


from config.program_info import 프로그램정보
from pandas import DataFrame


class View_버전(QDialog, FORM_CLASS_VERSION):
    def __init__(self, parent=None):
        super(View_버전, self).__init__(parent)
        self.setAttribute(Qt.WA_DeleteOnClose)
        self.setupUi(self)
        self.setWindowTitle("버전")
        self.parent = parent

        self.model = PandasModel()
        self.tableView.setModel(self.model)

        df = DataFrame(data=프로그램정보, columns=["A", "B"])

        self.model.update(df)
        for i in range(len(df.columns)):
            self.tableView.resizeColumnToContents(i)
