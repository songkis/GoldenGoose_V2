
# AI 동적 설정 관리자 (JSON 기반 조회/업데이트)

import json
import os
from typing import Any, Dict

CONFIG_DIR = "c:/GoldenGoose/config"
PARAMS_FILE = os.path.join(CONFIG_DIR, "ai_parameters.json")

class AIConfigManager:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(AIConfigManager, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if self._initialized:
            return

        self.params: Dict[str, Any] = {}
        self._load_config()
        self._initialized = True

    def _load_config(self):
        """JSON 설정 로드"""
        if os.path.exists(PARAMS_FILE):
            try:
                with open(PARAMS_FILE, "r", encoding="utf-8") as f:
                    self.params = json.load(f)
            except Exception as e:
                print(f"[AIConfigManager] 설정 로드 실패: {e}")
        else:
            print(f"[AIConfigManager] 설정 파일이 없습니다: {PARAMS_FILE}")

    def _save_config(self):
        """JSON 설정 저장"""
        try:
            with open(PARAMS_FILE, "w", encoding="utf-8") as f:
                json.dump(self.params, f, indent=4, ensure_ascii=False)
        except Exception as e:
            print(f"[AIConfigManager] 설정 저장 실패: {e}")

    def get(self, key: str, default: Any = None) -> Any:
        """설정 값 조회"""
        return self.params.get(key, default)

config_manager = AIConfigManager()
