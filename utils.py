from datetime import date
from sqlite3 import Date

import requests



class Utils:
    @staticmethod
    def safe_int(val) -> int:
        try:
            return int(str(val).replace(",", "").strip() or 0)
        except (ValueError, TypeError):
            return 0
    @staticmethod
    def safe_float(val) -> float:
        try:
            return float(str(val).replace(",", "").strip() or 0)
        except (ValueError, TypeError):
            return 0.0

    @staticmethod
    def is_running_on_gcp() -> bool:
        try:
            response = requests.get(
                "http://metadata.google.internal",
                headers={"Metadata-Flavor": "Google"},
                timeout=1
            )
            return response.status_code == 200
        except Exception:
            return False

