from datetime import date, timedelta

from logger.gcp_logger import GCPLogger, LogLevel
from services.bigquery_service import BigQueryService



class TransferService:
    def __init__(self, session_id: str,ad_name: str = ""):
        self.session_id  = session_id
        self.ads_service      = None
        self.bigquery         = BigQueryService(session_id)
        self._set_ads_service(ad_name)

    def _set_ads_service(self,ad_name: str = ""):
        match ad_name:
            case "bing":
                from services.bing_service import BingService
                self.ads_service = BingService(session_id=self.session_id)
            case _:
                raise ValueError(f"Unsupported ad platform: {ad_name}")

    def run(self, from_x_days: int, to_x_days: int) -> dict:
        start_date = date.today() - timedelta(days=from_x_days)
        end_date = date.today() - timedelta(days=to_x_days)


        try:
            GCPLogger.log(LogLevel.INFO, "bingads-transfer-data", {
                "session_id": self.session_id,
                "message": f"Transfer started for {start_date} -> {end_date}",
            })
            records = self.ads_service.get_all_accounts_ad_performance(
                start_date=start_date,
                end_date=end_date,
            )

        except Exception as e:
            GCPLogger.log(LogLevel.ERROR, "bingads-transfer-data", {
                "session_id": self.session_id,
                "message": f"Error occurred: {str(e)}",
            })


        return records