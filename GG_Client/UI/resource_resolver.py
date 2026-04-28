import os
import sys

def resource_path(relative_path):
    """ Get absolute path to resource, works for dev and for PyInstaller """
    if hasattr(sys, '_MEIPASS'):
        base_path = sys._MEIPASS
    else:
        # 프로젝트 루트를 기준으로 경로 설정 (현재 파일 위치 기준 2단계 상위가 루트라고 가정 시)
        # UI/resource_resolver.py -> UI/ -> Root
        base_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    return os.path.join(base_path, relative_path)
