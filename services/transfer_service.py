from datetime import date, datetime, timedelta

from logger.gcp_logger import GCPLogger, LogLevel
from services.bigquery_service import BigQueryService



class TransferService:
    def __init__(self, session_id: str,ad_name: str = ""):
        if session_id is None:
            session_id = str(int(datetime.utcnow().timestamp()))
        self.session_id  = session_id
        self.ads_service      = None
        self.bigquery         = BigQueryService(session_id,"BingAds",True)
        self._set_ads_service(ad_name)

    def _set_ads_service(self,ad_name: str = ""):
        match ad_name:
            case "bing":
                from services.bing_service import BingService
                self.ads_service = BingService(session_id=self.session_id)
            case _:
                raise ValueError(f"Unsupported ad platform: {ad_name}")


    def upload_ad_data_to_gcp(self, from_x_days: int, to_x_days: int) -> dict:
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

        # DELETE existing rows for this date range to prevent duplicates
            delete_query = f"""
                DELETE FROM `unidb-442214.GoogleAds.ad_data`
                WHERE data_date BETWEEN '{start_date}' AND '{end_date}'
            """
            self.bigquery.execute_query(delete_query)

            # Build INSERT query
            rows = ",\n".join([
                f"('{r.data_date}', {self._q(r.account_id)}, {self._q(r.account_name)}, "
                f"{self._q(r.campaign_id)}, {self._q(r.campaign_name)}, {self._q(r.campaign_type)}, "
                f"{self._q(r.ad_group_id)}, {self._q(r.ad_group_name)}, {self._q(r.ad_id)}, {self._q(r.ad_name)}, "
                f"{self._q(r.device_type)}, {self._q(r.final_url)}, "
                f"{r.impressions}, {r.clicks}, {r.spend}, {r.conversions})"
                for r in records
            ])

            insert_query = f"""
                INSERT INTO `unidb-442214.GoogleAds.ad_data`
                (data_date, account_id, account_name, campaign_id, campaign_name,
                campaign_type, ad_group_id, ad_group_name, ad_id, ad_name,
                device_type, final_url, impressions, clicks, spend, conversions)
                VALUES {rows}
            """
            self.bigquery.execute_query(insert_query)

            GCPLogger.log(LogLevel.INFO, "bingads-transfer-data", {
                "session_id": self.session_id,
                "message": f"Uploaded {len(records)} rows for {start_date} -> {end_date}",
            })
            return {"success": True, "rows_uploaded": len(records)}
        except Exception as e:
            GCPLogger.log(LogLevel.ERROR, "bingads-transfer-data", {
                "session_id": self.session_id,
                "message": f"Error occurred: {str(e)}",
            })
            raise e

    def _q(self, val) -> str:
        """Safely quote a string value for SQL, returns NULL for None."""
        if val is None:
            return ""
        escaped = str(val).replace("'", "\\'")
        return f"'{escaped}'"