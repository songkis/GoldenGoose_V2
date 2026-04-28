# -*- coding: utf-8 -*-
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'GG_Shared')))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'GG_Server')))
from pathlib import Path

import pythoncom

try:
    pythoncom.CoInitialize()
except Exception:
    pass

import datetime as dt
from PyQt5 import uic
from PyQt5.QtCore import pyqtSignal, Qt, QTimer, QMutex
from PyQt5.QtGui import QIcon, QPalette, QBrush, QColor, QStandardItemModel
from PyQt5.QtWidgets import QMainWindow, QApplication

# UI/Logic Imports (May contain sub-module loggers)
from UI.main_window_logic import MainWindowLogic
from UI.resource_resolver import resource_path

from gooses import AIGoose, BaseGoose, GuardianGoose
from config.log_settings import setup_logger
from GG_Shared.util.Utils32 import set_utils32_logger
from GG_Shared.util.CommUtils import set_commutils_logger, AccountGuard
from UI.main_window_logic import set_main_window_logger

# Standardize Logging (Absolute Top priority to avoid submodule hijacking)
logger = setup_logger(Path(__file__).stem)

set_main_window_logger(logger)
set_commutils_logger(logger)
set_utils32_logger(logger)
BaseGoose.set_logger(logger)
AIGoose.set_logger(logger)
GuardianGoose.set_logger(logger)

# Robust sys.path Injection
PROJECT_ROOT = Path(__file__).resolve().parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

if hasattr(os, "add_dll_directory"):
    try:
        os.add_dll_directory(r"C:\LS_SEC\xingAPI")
    except Exception:
        pass

# Load MainWindow UI
try:
    Ui_MainWindow, _ = uic.loadUiType(resource_path("UI/GoldenGoose.ui"))
except Exception as e:
    logger.error(f"Failed to load GoldenGoose UI: {e}")
    raise


class MainWindow(QMainWindow, Ui_MainWindow, MainWindowLogic):
    beforeCloseFinished = pyqtSignal()

    def __init__(self, *args, **kwargs):
        # [Safety Guard] XING API는 반드시 MainThread에서만 생성되어야 함
        import threading

        if threading.current_thread() is not threading.main_thread():
            raise RuntimeError("Critical Error: GUI/ActiveX must run on MainThread")

        super(MainWindow, self).__init__(*args, **kwargs)
        self.logger = logger
        self.setupUi(self)
        self.setWindowIcon(QIcon("./ico/golden_goose.ico"))

        # Initialize Logic Component Attributes (Normally handled in MainWindowLogic or manually here)
        self.resizeLayout()

        from managers.plugin_manager import CPluginManager
        from managers.batch_manager import BatchGooses4Timer
        from managers.thread_manager import (
            LimitedThreadPoolExecutor,
            GooseThreadManager,
        )

        plugin_manager = CPluginManager(self)
        self.plugins = plugin_manager.plugin_loader()
        self.시작시각 = dt.datetime.now()
        self.gooses = []
        self.threads = []
        self.batchGooses4Timer = BatchGooses4Timer(False, dict(), 0, False)
        self.dialog = dict()
        self.account_dict = dict()
        self.ai_conf_dict = dict()

        self.account_columns = [
            "추정순자산",
            "실현손익",
            "매입금액",
            "추정D2예수금",
            "CTS_종목번호",
            "평가금액",
            "평가손익",
        ]
        self.goose_columns = ["Goose타입", "Goose", "GooseID", "실행상태", "종목수"]
        self.portfolio_columns = [
            "종목코드",
            "종목명",
            "매수가",
            "수량",
            "매입금액",
            "현재가",
            "평가금액",
            "평가손익",
            "수익율",
            "매수일",
        ]

        self.newsStockCodes = []
        self.positiveSignal = [
            "상승 전망",
            "강세 출발",
            "상승 출발",
            "연속 순매수",
            "발동",
            "신고가",
            "연속 상승",
            "상승폭 확대",
            "상승세",
            "강세 토픽",
            "상승전환",
            "상한가 진입",
            "반등",
            "강세",
            "정배열",
            "기대감",
            "급등",
            "매력종목",
            "상승 기대",
            "매수우위",
            "매수강세",
            "상승률 상위",
            "급증",
            "연일 강세",
            "매수 유입",
            "수혜",
            "기회",
            "매수체결 상위",
            "매수",
            "폭등",
            "+0",
            "+1",
            "+2",
            "+3",
            "+4",
            "+5",
            "+6",
            "+7",
            "+8",
            "+9",
            "↑",
            "▲",
            '"종목" 大 공개',
        ]
        self.negativeSignal = [
            "↓",
            "-",
            "▼",
            "순매수·순매도",
            "공매도",
            "하락",
            "매도우위",
            "매도강세",
            "순매수,도 상위",
        ]

        self.init_models()
        
        # [Critical] Initalize Invoker on Main Thread BEFORE ZMQ starts
        from GG_Shared.util.Utils32 import get_invoker
        get_invoker()
        
        self.init_zmq()

        self.주문제한 = 0
        self.조회제한 = 0
        self.금일백업작업중 = False
        self.종목선정작업중 = False
        self.조용히종료 = False

        self.connection = None
        from xing.XAQuaries import t0167, t0425

        self.XQ_t0167 = t0167(parent=self)
        self.XQ_t0425 = t0425(parent=self)
        self.gooseTradeExecutor = LimitedThreadPoolExecutor(
            max_workers=64, queue_size=2000
        )
        self.timers = {}
        self.주문가능금액부족 = False
        self.일별가격정보백업완료 = False
        self.schedule_backup()

        self.goose_thread_manager = GooseThreadManager(self)
        self.server_socket = None
        self.tsg_svr_running = False
        self.code_close_request = False
        self.beforeCloseFinished.connect(self._do_close)
        self.account_info = None
        # AccountGuard is now imported at the top level
        self.account_guard = AccountGuard(0)
        self.goose_creation_mutex = QMutex()

    def init_models(self):
        from UI.components.models import PandasModel

        self.account_model = QStandardItemModel()
        self.tableView_account.setModel(self.account_model)
        self.goose_model = PandasModel()
        self.tableView_goose.setModel(self.goose_model)
        self.tableView_goose.pressed.connect(self.GooseCurrentIndex)

        self.portfolio_model1 = QStandardItemModel()
        self.tableView_portfolio1.setModel(self.portfolio_model1)
        self.tableView_portfolio1.pressed.connect(self.Portfolio1CurrentIndex)

        self.portfolio_model2 = QStandardItemModel()
        self.tableView_portfolio2.setModel(self.portfolio_model2)
        self.tableView_portfolio2.pressed.connect(self.Portfolio2CurrentIndex)


if __name__ == "__main__":
    try:
        app = QApplication(sys.argv)
        app.setQuitOnLastWindowClosed(True)

        # Global Theme: Blue Goose (Color Swap Refresh)
        app.setStyle("Fusion")

        # 1. Palette (Base Colors)
        palette = QPalette()

        # Background: Sampled Blue (62, 83, 197)
        c_blue_bg = QColor(62, 83, 197)
        c_gold_bright = QColor(255, 212, 0)  # Content Gold
        c_white_dense = QColor(240, 240, 240)  # Title White

        brush_bg = QBrush(c_blue_bg)
        brush_bg.setStyle(Qt.SolidPattern)

        palette.setBrush(QPalette.Active, QPalette.Window, brush_bg)
        palette.setBrush(QPalette.Inactive, QPalette.Window, brush_bg)
        palette.setBrush(QPalette.Disabled, QPalette.Window, brush_bg)
        palette.setBrush(QPalette.Active, QPalette.Base, brush_bg)
        palette.setBrush(QPalette.Inactive, QPalette.Base, brush_bg)
        palette.setBrush(QPalette.Disabled, QPalette.Base, brush_bg)

        # Default Text (Content): Bright Gold
        brush_text = QBrush(c_gold_bright)
        brush_text.setStyle(Qt.SolidPattern)

        palette.setBrush(QPalette.Active, QPalette.WindowText, brush_text)
        palette.setBrush(QPalette.Inactive, QPalette.WindowText, brush_text)
        palette.setBrush(QPalette.Active, QPalette.Text, brush_text)
        palette.setBrush(QPalette.Inactive, QPalette.Text, brush_text)
        palette.setBrush(QPalette.Active, QPalette.ButtonText, brush_text)
        palette.setBrush(QPalette.Inactive, QPalette.ButtonText, brush_text)

        # Placeholder
        brush_ph = QBrush(QColor(255, 255, 255, 128))
        palette.setBrush(QPalette.Active, QPalette.PlaceholderText, brush_ph)
        palette.setBrush(QPalette.Inactive, QPalette.PlaceholderText, brush_ph)

        # Button: Transparent-ish
        palette.setBrush(QPalette.Active, QPalette.Button, QBrush(Qt.transparent))
        palette.setBrush(QPalette.Inactive, QPalette.Button, QBrush(Qt.transparent))

        app.setPalette(palette)

        # 2. Global Stylesheet (Color Swap & Typography)
        # background_path = "./ico/golden_goose.png"
        background_path = "./PNG/blue_goose_final_faint.png"

        qss = f"""
            /* Global Typography: 16px Bold Gold Content */
            QWidget {{
                font-family: "Malgun Gothic", "Segoe UI", sans-serif;
                font-size: 13px;
                font-weight: bold;
                color: rgb(255, 212, 0); /* Gold for content */
            }}

            /* Main Window Full Background */
            QMainWindow {{
                background-image: url({background_path});
                background-repeat: no-repeat;
                background-position: center;
                background-size: cover;
                background-attachment: fixed;
                background-color: rgb(62, 83, 197);
            }}
            
            /* Menus: Dark Background + White Text */
            QMenuBar {{
                background-color: rgba(0, 0, 0, 120);
                padding: 5px;
                color: white;
            }}
            QMenuBar::item {{
                background-color: transparent;
                padding: 4px 12px;
                color: white;
            }}
            QMenuBar::item:selected {{
                background-color: rgba(255, 255, 255, 40);
                border-radius: 4px;
            }}
            QMenu {{
                background-color: rgb(40, 40, 40);
                border: 2px solid white;
                padding: 5px;
            }}
            QMenu::item {{
                padding: 6px 30px 6px 20px;
                color: white;
            }}
            QMenu::item:selected {{
                background-color: white;
                color: black;
            }}
            
            /* Header Views (Tables): Dark Background + White Text */
            QHeaderView::section {{
                background-color: rgba(0, 0, 0, 150);
                border: 1px solid white;
                padding: 1px;
                color: white; /* White headers */
                font-size: 14px; /* Reduced by 1pt */
            }}
            
            /* PushButtons: Transparent Background + Gold Text */
            QPushButton {{
                background-color: transparent;
                border: 2px solid rgb(255, 212, 0);
                border-radius: 6px;
                padding: 1px 1px;
                min-width: 90px;
                color: rgb(255, 212, 0); /* Gold buttons */
            }}
            QPushButton:hover {{
                background-color: rgba(255, 212, 0, 30);
            }}
            QPushButton:pressed {{
                background-color: rgb(255, 212, 0);
                color: rgb(62, 83, 197);
            }}
            
            /* GroupBox: Dark Titles + White Text */
            QGroupBox {{
                border: 2px solid rgba(255, 255, 255, 80);
                border-radius: 8px;
                margin-top: 20px;
                padding: 13px;
                background-color: transparent;
            }}
            QGroupBox::title {{
                subcontrol-origin: margin;
                left: 15px;
                padding: 0 8px;
                background-color: rgba(0, 0, 0, 120);
                color: white; /* White GroupBox titles */
                border-radius: 4px;
                font-size: 14px; /* Reduced by 1pt */
            }}
            
            /* Tables: Gold Content */
            QTableView {{
                background-color: transparent;
                gridline-color: rgba(255, 212, 0, 60);
                selection-background-color: rgb(255, 212, 0);
                selection-color: rgb(62, 83, 197);
                border: none;
                color: rgb(255, 212, 0); /* Gold table content */
            }}

            /* Input Fields */
            QLineEdit, QComboBox, QSpinBox {{
                background-color: rgba(0, 0, 0, 60);
                border: 1px solid rgb(255, 212, 0);
                border-radius: 4px;
                padding: 4px;
                color: rgb(255, 212, 0);
            }}
            
            /* Specific fix for Labels to ensure Gold */
            QLabel {{
                color: rgb(255, 212, 0);
            }}
        """
        app.setStyleSheet(qss)

        print("DEBUG: Starting GoldenGoose...")
        global window
        print("DEBUG: Initializing MainWindow...")
        window = MainWindow()
        print("DEBUG: MainWindow instance created.")

        from util.CommUtils import set_window_context

        print("DEBUG: Calling set_window_context to load DB settings...")
        set_window_context(window, logger)
        print("DEBUG: setConfig completed.")

        title = window.account_dict.get("거래환경", "") + " GoldenGoose"
        window.setWindowTitle(title)

        print(f"DEBUG: Calling window.show() (Title: {title})...")
        window.show()
        window.raise_()
        window.activateWindow()

        print("DEBUG: Scheduling OnQApplicationStarted...")
        QTimer().singleShot(100, window.OnQApplicationStarted)

        # [Safety] 앱 종료 시 소켓 정리를 확실히 하기 위해 연결
        app.aboutToQuit.connect(window.close)

        print(f"DEBUG: window.isVisible() state: {window.isVisible()}")
        print("DEBUG: Entering QApplication event loop...")
        exit_code = app.exec_()
        print(f"DEBUG: QApplication event loop exited with code: {exit_code}")
        sys.exit(exit_code)
    except Exception as e:
        import traceback

        with open("fatal_error.log", "w") as f:
            traceback.print_exc(file=f)
