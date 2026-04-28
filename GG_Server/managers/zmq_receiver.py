import logging
from PyQt5.QtCore import QThread, pyqtSignal
from GG_Shared.util.zmq_manager import ZMQPushPull
from config.comm_settings import ZMQ_PULL_PORT

logger = logging.getLogger(__name__)

class ZMQReceiverThread(QThread):
    """
    [ZMQReceiverThread] 64-bit 서버로부터 시그널을 수신하는 전용 스레드 (32-bit Client용)
    - QThread를 사용하여 UI Thread(MainThread)와의 시그널 연동 시 Thread-Safety 보장
    """
    signal_received = pyqtSignal(dict)

    def __init__(self, parent=None):
        super().__init__(parent)
        self.running = True
        self.zmq_pull = None

    def run(self):
        try:
            self.zmq_pull = ZMQPushPull(mode="PULL", port=ZMQ_PULL_PORT)
            logger.info(f"✅ [ZMQReceiverThread] Listening on PULL port {ZMQ_PULL_PORT}")
            
            def on_data(data):
                if data:
                    self.signal_received.emit(data)
            
            self.zmq_pull.start_pull_listener(on_data)
            
            while self.running:
                self.msleep(500)
        except Exception as e:
            logger.error(f"❌ [ZMQReceiverThread] Critical Run Error: {e}")
        finally:
            self.stop()

    def stop(self):
        self.running = False
        if self.zmq_pull:
            try:
                self.zmq_pull.close()
                logger.info("[ZMQReceiverThread] ZMQ Socket closed.")
            except Exception: pass
        self.quit()
