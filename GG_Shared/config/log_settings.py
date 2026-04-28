import logging
import re
from logging.handlers import TimedRotatingFileHandler

LOG_FORMAT = "%(asctime)s [%(thread)d] %(levelname).1s %(short_name)s: %(message)s"
LOG_LEVEL = logging.INFO
LOG_BACKUP_COUNT = 7

class SafeTimedRotatingFileHandler(TimedRotatingFileHandler):
    """파일 잠금 대응 안전 핸들러"""
    def doRollover(self):
        try:
            super().doRollover()
        except PermissionError as e:
            import sys
            print(f"\n[SafeLog] Rotation Failed (Locked): {e}", file=sys.stderr)
        except Exception as e:
            import sys
            print(f"\n[SafeLog] Rotation Failed: {e}", file=sys.stderr)

class InitialsFilter(logging.Filter):
    """클래스명 이니셜 추출 필터"""
    def filter(self, record):
        name = record.name
        initials = "".join(re.findall(r"[A-Z]", name))
        if not initials: initials = name[:2].upper()
        record.short_name = initials
        return True

def setup_logger(logger_name: str, log_level=LOG_LEVEL, log_format=LOG_FORMAT, backup_count=LOG_BACKUP_COUNT):
    """로거 설정 및 핸들러 연결"""
    from config.comm_settings import LOG_DIR
    
    # [Directive 1] 로그 폴더 자동 생성 (Urgent)
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    
    log_file = LOG_DIR / f"{logger_name}.log"

    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)

    if not any(isinstance(h, logging.FileHandler) for h in root_logger.handlers):
        file_handler = SafeTimedRotatingFileHandler(
            filename=str(log_file), when="midnight", encoding="utf-8", backupCount=backup_count
        )
        file_handler.setFormatter(logging.Formatter(log_format))
        file_handler.addFilter(InitialsFilter())

        if not root_logger.handlers:
            console_handler = logging.StreamHandler()
            console_handler.setFormatter(logging.Formatter(log_format))
            console_handler.addFilter(InitialsFilter())
            root_logger.addHandler(console_handler)

        root_logger.addHandler(file_handler)

    logger = logging.getLogger(logger_name)
    logger.setLevel(log_level)
    return logger
