from datetime import date
from sqlite3 import Date



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

