import os
from pathlib import Path
# TF Log Level (3: ERROR)
TF_CPP_MIN_LOG_LEVEL = (
    "3"
)
os.environ["TF_CPP_MIN_LOG_LEVEL"] = (
    TF_CPP_MIN_LOG_LEVEL
)

PROJECT_ROOT = Path("C:\\GoldenGoose")
DB_DIR = os.path.join(PROJECT_ROOT, "db")
acf_DIR = os.path.join(PROJECT_ROOT, "acf")
LOG_DIR = os.path.join(PROJECT_ROOT, "logs")
LOG_DIR = Path(LOG_DIR)
LOG_DIR.mkdir(parents=True, exist_ok=True)

DATABASE = os.path.join(DB_DIR, "GoldenGoose.sqlite")

주문지연 = 3000
UI_DIR_NM = "UI"
RES_DIR_NM = "res"

AIG_NM = "AIGoose"
AIG_ID = "3464b25eda87400e9b9c4d8033bf4dc1"
GDG_NM = "GuardianGoose"
GDG_ID = "a491e22f61854133a9349fa9f371fb79"


ZMQ_PULL_PORT = 5558
ZMQ_PUB_PORT = 5559
ZMQ_STATUS_PORT = 5560

TOPIC_EVENT = b"EVT"
TOPIC_ORDER = b"ORD"
TOPIC_TICK = b"TICK"
TOPIC_ORDER_RESULT = b"ORD_RES"



ZMQ_HWM_SIZE = 1000
ZMQ_LINGER_MS = 0