import zmq
import msgpack
import threading
import logging
import numpy as np
import math
from typing import Any, Dict, Callable

from config.comm_settings import (
    ZMQ_PUB_PORT,
    ZMQ_PULL_PORT,
    ZMQ_HWM_SIZE,
    ZMQ_LINGER_MS,
    TOPIC_EVENT,
    TOPIC_ORDER,
    TOPIC_TICK,
    TOPIC_ORDER_RESULT,
    ZMQ_STATUS_PORT,
)

logger = logging.getLogger(__name__)


def make_json_safe(obj: Any) -> Any:
    """
    [Absolute Defense] Recursive cleaner to ensure msgpack compatibility.
    Handles numpy/pandas objects by string-based type detection to avoid module identity issues.
    """
    if obj is None:
        return 0.0

    type_str = str(type(obj))

    # 1. Handle Dicts
    if isinstance(obj, dict):
        return {str(k): make_json_safe(v) for k, v in obj.items()}

    # 2. Handle Lists/Tuples/Arrays (excluding Pandas)
    if isinstance(obj, (list, tuple, np.ndarray)) and "pandas" not in type_str:
        if hasattr(obj, "tolist"):
            return [make_json_safe(v) for v in obj.tolist()]
        return [make_json_safe(v) for v in obj]

    # 3. Handle Pandas specifically (Series/DataFrame/Index)
    if "pandas" in type_str or "Series" in type_str or "DataFrame" in type_str:
        if ("DataFrame" in type_str) and hasattr(obj, "to_dict"):
            return [make_json_safe(v) for v in obj.to_dict(orient="records")]
        elif hasattr(obj, "tolist"):
            res = obj.tolist()
            if isinstance(res, list) and len(res) == 1:
                return make_json_safe(res[0])
            return [make_json_safe(v) for v in res]
        elif hasattr(obj, "iloc") and hasattr(obj, "__len__") and len(obj) == 1:
            return make_json_safe(obj.iloc[0])

    # 4. Handle Timestamps
    if "Timestamp" in type_str or "datetime" in type_str:
        return str(obj)

    # 5. Handle Scalars (Numpy primitives)
    if "int" in type_str.lower():
        return int(obj)
    if "float" in type_str.lower():
        val = float(obj)
        if math.isnan(val) or math.isinf(val):
            return 0.0
        return val
    if "bool" in type_str.lower():
        return bool(obj)

    # 6. Handle dict_keys/dict_values (JSON/MsgPack serializable list)
    if "dict_keys" in type_str or "dict_values" in type_str:
        return [make_json_safe(v) for v in list(obj)]

    # 7. Handle Enum and Datetime (Critical Execution Boundary)
    from enum import Enum
    from datetime import datetime
    if isinstance(obj, Enum):
        return obj.name
    if isinstance(obj, datetime):
        return obj.isoformat()

    # Final Catch-all
    if isinstance(obj, (int, float, str, bool)):
        return obj
    return str(obj)


#  Global ZMQ Shared Resources Lock
_zmq_global_lock = threading.RLock()
_shared_pub = None


def get_shared_publisher(port: int = ZMQ_PUB_PORT):
    """
    Returns a shared ZMQPublisher instance (Singleton per process) with thread-safe lock.
    Prevents "Address in use" errors and provides resilient socket state.
    """
    global _shared_pub
    with _zmq_global_lock:
        if _shared_pub is None:
            _shared_pub = ZMQPublisher(port=port)
        return _shared_pub


def send_feedback(payload: Dict[str, Any]):
    """
    [Step 4.1] 32-bit 클라이언트 -> 64-bit 서버 상태 피드백 발송 (PUSH)
    - 체결(FILL), 잔고 변화 등을 서버의 Shadow State로 즉시 전송
    """
    try:
        # 피드백 채널은 PUSH-PULL (Port 5560) 고정 사용
        feedback_push = ZMQPushPull(mode="PUSH", port=ZMQ_STATUS_PORT)
        feedback_push.push_data(payload)
        feedback_push.close()
    except Exception as e:
        logger.error(f"[ZMQManager] send_feedback failed: {e}")


def cleanup_all_zmq():
    """
    [Critical] Forces cleanup of all ZMQ resources in the current process.
    Should be called during application shutdown.
    """
    global _shared_pub
    with _zmq_global_lock:
        if _shared_pub is not None:
            try:
                _shared_pub.close()
                _shared_pub = None
                logger.info("[ZMQManager] Shared publisher cleaned up.")
            except Exception as e:
                logger.error(f"[ZMQManager] Error cleaning up shared publisher: {e}")


class ZMQPublisher:
    """
     Phoenix-Socket Publisher
    Handles PUB/SUB pattern for event notifications with automatic recovery.
    Uses threading.RLock to prevent race conditions during socket re-initialization.
    """

    def __init__(self, port: int = ZMQ_PUB_PORT):
        self.port = port
        self.context = zmq.Context()
        self.socket = None
        self._lock = threading.RLock()
        self._is_closed = False

        # Initial boot
        self._init_socket()

    def _init_socket(self):
        """Internal worker to (re)bind the socket with industrial-grade options."""
        with self._lock:
            # 1. Cleanup old socket if it exists
            if self.socket:
                try:
                    self.socket.setsockopt(zmq.LINGER, 0)
                    self.socket.close()
                except Exception:
                    pass

            # 2. Re-create and Bind
            self.socket = self.context.socket(zmq.PUB)
            self.socket.setsockopt(zmq.SNDHWM, ZMQ_HWM_SIZE)
            self.socket.setsockopt(zmq.LINGER, ZMQ_LINGER_MS)
            try:
                self.socket.setsockopt(zmq.REUSEADDR, 1)
            except Exception:
                pass

            try:
                self.socket.bind(f"tcp://*:{self.port}")
                logger.info(f"[ZMQPublisher] Phoenix-Socket bound to port {self.port}")
            except zmq.ZMQError as e:
                logger.error(f"[ZMQPublisher] Failed to bind port {self.port}: {e}")
                # Don't raise here, allow later recovery attempts
                self.socket = None

    def publish_data(self, topic: bytes, data: Dict[str, Any]):
        """
        Publishes data with topic prefix. Auto-recovers if socket is lost.
        """
        if self._is_closed:
            logger.warning("[ZMQPublisher] Attempted to publish on a CLOSED instance.")
            return

        with self._lock:
            # [Phoenix-Socket] Auto-Recovery Guard
            if self.socket is None:
                logger.warning(
                    "[ZMQPublisher] Socket is None. Attempting on-the-fly recovery..."
                )
                self._init_socket()

            if self.socket is None:
                logger.error("[ZMQPublisher] Auto-recovery failed. Data dropped.")
                return

            try:
                safe_data = make_json_safe(data)
                packed_data = msgpack.packb(safe_data)

                # [NoneType Defense] Double check before transmission
                if self.socket:
                    self.socket.send_multipart([topic, packed_data])
                    logger.debug(f"[ZMQPublisher] Published: {topic}")
                else:
                    logger.error(
                        "[ZMQPublisher] Socket disappeared during serialization."
                    )
            except Exception as e:
                logger.error(f"[ZMQPublisher] Serialization/Send Error: {e}")

    def __del__(self):
        self.close()

    def close(self):
        with self._lock:
            if self._is_closed:
                return
            self._is_closed = True
            try:
                if self.socket:
                    self.socket.setsockopt(zmq.LINGER, 0)
                    self.socket.close()
                    self.socket = None
                if self.context:
                    self.context.term()
                    self.context = None
                logger.info("[ZMQPublisher] Closed gracefully")
            except Exception as e:
                logger.error(f"[ZMQPublisher] Close Error: {e}")


class ZMQSubscriber:
    """
    Subscribes to PUB messages.
    Usually connects to port 5555.
    Run loop() in a separate thread.
    """

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = ZMQ_PUB_PORT,
        topics: list = [TOPIC_EVENT],
    ):
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.SUB)

        # Architect Addendum: HWM & Linger
        self.socket.setsockopt(zmq.RCVHWM, ZMQ_HWM_SIZE)
        self.socket.setsockopt(zmq.LINGER, ZMQ_LINGER_MS)

        self.topics = topics  # Store for resilient splitting
        try:
            self.socket.connect(f"tcp://{host}:{port}")
            for topic in self.topics:
                self.socket.setsockopt(zmq.SUBSCRIBE, topic)
            logger.info(
                f"[ZMQSubscriber] Connected to {host}:{port}, Topics: {self.topics}"
            )
        except zmq.ZMQError as e:
            logger.error(f"[ZMQSubscriber] Failed to connect: {e}")
            raise

        self.running = False

    def start_listener(self, callback: Callable[[bytes, Dict[str, Any]], None]):
        """
        Starts the listener loop in a daemon thread.
        callback(topic, data)
        """
        if self.running and hasattr(self, "thread") and self.thread.is_alive():
            logger.warning(
                f"[ZMQSubscriber] Listener already running in thread {self.thread.name}. Ignoring start request."
            )
            return

        self.running = True
        self.thread = threading.Thread(target=self._loop, args=(callback,), daemon=True)
        self.thread.name = f"ZMQSub-{hex(id(self))[-4:]}"
        self.thread.start()
        logger.info(f"[ZMQSubscriber] Listener started: {self.thread.name}")

    def _loop(self, callback):
        poller = zmq.Poller()
        poller.register(self.socket, zmq.POLLIN)

        while self.running:
            try:
                # Poll with timeout to allow checking self.running
                socks = dict(poller.poll(1000))

                if self.socket in socks and socks[self.socket] == zmq.POLLIN:
                    parts = self.socket.recv_multipart()

                    # [Debug] Track which thread is processing this message
                    t_id = threading.get_ident()

                    topic = None
                    packed_data = None

                    if len(parts) >= 2:
                        topic = parts[0]
                        packed_data = parts[-1]
                    elif len(parts) == 1:
                        # [Resilient Mode] Fused Topic+Data Frame handling
                        raw_frame = parts[0]
                        for t in getattr(self, "topics", []):
                            if raw_frame.startswith(t):
                                topic = t
                                packed_data = raw_frame[len(t) :]
                                logger.warning(
                                    f"[ZMQSubscriber] Fused frame detected for topic: {topic}"
                                )
                                break

                    if topic and packed_data:
                        try:
                            # 1. Standard Unpack
                            data = msgpack.unpackb(packed_data)
                            callback(topic, data)
                        except msgpack.ExtraData:
                            # 2. Resilient Unpack (Handling extra junk/frame concat)
                            logger.error(
                                f"[ZMQSubscriber][T{t_id}] ExtraData detected. Len: {len(packed_data)}. Parsing first object only."
                            )
                            try:
                                # Extract only the first available msgpack object
                                data = (
                                    msgpack.Unpacker(raw=False)
                                    .feed(packed_data)
                                    .unpack()
                                )
                                callback(topic, data)
                                # Log the first 50 bytes of trailing junk for diagnosis
                                remaining_hex = (
                                    packed_data[len(packed_data) - 100 :].hex()
                                    if len(packed_data) >= 100
                                    else packed_data.hex()
                                )
                                logger.warning(
                                    f"[ZMQSubscriber][T{t_id}] Trailing bytes hex: {remaining_hex}"
                                )
                            except Exception as sub_e:
                                logger.error(
                                    f"[ZMQSubscriber][T{t_id}] Failed resilient unpack: {sub_e}"
                                )
                        except Exception as e:
                            logger.error(f"[ZMQSubscriber][T{t_id}] Unpack Error: {e}")
                    else:
                        logger.warning(
                            f"[ZMQSubscriber][T{t_id}] Received malformed multipart message with {len(parts)} parts. Frame: {parts[0].hex() if parts else 'Empty'}"
                        )

            except zmq.ZMQError as e:
                logger.error(f"[ZMQSubscriber] ZMQ Error: {e}")
                if not self.running:
                    break
            except Exception as e:
                logger.error(f"[ZMQSubscriber] Error in loop: {e}")

    def __del__(self):
        self.stop()

    def stop(self):
        self.running = False
        try:
            if hasattr(self, "socket") and self.socket:
                self.socket.setsockopt(zmq.LINGER, 0)
                self.socket.close()
                self.socket = None
            if hasattr(self, "context") and self.context:
                self.context.term()
                self.context = None
            logger.info("[ZMQSubscriber] Stopped gracefully")
        except Exception as e:
            logger.error(f"[ZMQSubscriber] Stop Error: {e}")

    def stop_listener(self):
        self.stop()


class ZMQPushPull:
    """
    Handles PUSH/PULL pattern for Command/Order distribution.
    Push (Sender) -> Pull (Receiver/Worker).
    Usually port 5556.
    """

    def __init__(self, mode: str, host: str = "127.0.0.1", port: int = ZMQ_PULL_PORT):
        """
        mode: 'PUSH' or 'PULL'
        """
        self.context = zmq.Context()
        self.mode = mode.upper()

        if self.mode == "PUSH":
            self.socket = self.context.socket(zmq.PUSH)
            self.socket.setsockopt(zmq.SNDHWM, ZMQ_HWM_SIZE)
            try:
                # PUSH typically binds (if it's the stable server) or connects.
                self.socket.connect(f"tcp://{host}:{port}")
                logger.info(f"[ZMQPush] Connected to {host}:{port}")
            except Exception as e:
                logger.error(f"[ZMQPush] Connect Error: {e}")
                raise

        elif self.mode == "PULL":
            self.socket = self.context.socket(zmq.PULL)
            self.socket.setsockopt(zmq.RCVHWM, ZMQ_HWM_SIZE)
            try:
                self.socket.setsockopt(zmq.REUSEADDR, 1)
            except Exception:
                pass
            try:
                self.socket.bind(f"tcp://*:{port}")
                logger.info(f"[ZMQPull] Bound to port {port}")
            except Exception as e:
                logger.error(f"[ZMQPull] Bind Error: {e}")
                raise

        self.socket.setsockopt(zmq.LINGER, ZMQ_LINGER_MS)
        self.running = False

    def push_data(self, data: Dict[str, Any]):
        """
        Sends data (Only for PUSH mode).
        """
        if self.mode != "PUSH":
            raise ValueError("Only PUSH socket can send data")

        try:
            safe_data = make_json_safe(data)
            packed_data = msgpack.packb(safe_data)
            self.socket.send(packed_data)
            logger.info(f"[ZMQPush] Sent order/command")
        except Exception as e:
            logger.error(f"[ZMQPush] Send Error: {e}")

    def start_pull_listener(self, callback: Callable[[Dict[str, Any]], None]):
        """
        Starts PULL listener loop (Only for PULL mode).
        """
        if self.mode != "PULL":
            raise ValueError("Only PULL socket can listen")

        if self.running and hasattr(self, "thread") and self.thread.is_alive():
            logger.warning(
                f"[ZMQPull] Listener already running in thread {self.thread.name}. Ignoring start request."
            )
            return

        self.running = True
        self.thread = threading.Thread(target=self._loop, args=(callback,), daemon=True)
        self.thread.name = f"ZMQPull-{hex(id(self))[-4:]}"
        self.thread.start()
        logger.info(f"[ZMQPull] Listener started: {self.thread.name}")

    def _loop(self, callback):
        poller = zmq.Poller()
        poller.register(self.socket, zmq.POLLIN)

        while self.running:
            try:
                socks = dict(poller.poll(1000))
                if self.socket in socks and socks[self.socket] == zmq.POLLIN:
                    packed_data = self.socket.recv()
                    t_id = threading.get_ident()

                    try:
                        data = msgpack.unpackb(packed_data)
                        callback(data)
                    except msgpack.ExtraData:
                        # Resilient unpack for PULL socket too
                        logger.error(
                            f"[ZMQPull][T{t_id}] ExtraData Error. Len: {len(packed_data)}. Parsing first object only."
                        )
                        try:
                            data = (
                                msgpack.Unpacker(raw=False).feed(packed_data).unpack()
                            )
                            callback(data)
                        except Exception as sub_e:
                            logger.error(
                                f"[ZMQPull][T{t_id}] Failed resilient unpack: {sub_e}"
                            )
                    except Exception as e:
                        logger.error(
                            f"[ZMQPull][T{t_id}] Unpack Error: {e}. Len: {len(packed_data)}"
                        )
                if not self.running:
                    break
            except zmq.ZMQError as e:
                #  'not a socket' 에러 대응: 소켓이 이미 닫혔거나 파괴된 경우
                if "not a socket" in str(e).lower() or not self.running:
                    logger.warning(
                        f"[ZMQPull] Socket invalidated or closed: {e}. Exiting loop."
                    )
                    break
                logger.error(f"[ZMQPull] ZMQ Error in loop: {e}")
            except Exception as e:
                logger.error(f"[ZMQPull] Loop Error: {e}")
                if not self.running:
                    break

    def __del__(self):
        self.close()

    def close(self):
        self.running = False
        mode_str = getattr(self, "mode", "UNKNOWN")
        try:
            if hasattr(self, "socket") and self.socket:
                self.socket.setsockopt(zmq.LINGER, 0)
                self.socket.close()
                self.socket = None
            if hasattr(self, "context") and self.context:
                self.context.term()
                self.context = None
            logger.info(f"[ZMQ{mode_str}] Closed gracefully")
        except Exception as e:
            logger.error(f"[ZMQ{mode_str}] Close Error: {e}")

    def stop_listener(self):
        self.close()
