from datetime import date, datetime, timedelta
from typing import Optional

from logger.gcp_logger import GCPLogger, LogLevel
from services.bigquery_service import BigQueryService
from schemas.bigquery_bing import AdTableRecord





class TransferService:
    def __init__(self, session_id: str, ad_name: str = ""):
        if session_id is None:
            session_id = str(int(datetime.utcnow().timestamp()))
        self.session_id  = session_id
        self.ads_service = None
        self.bigquery = None
        self._set_ads_service(ad_name)
        self._set_bigquery_service(ad_name)

    # ── Ad service factory ────────────────────────────────────────────────────

    def _set_ads_service(self, ad_name: str = ""):
        match ad_name:
            case "bing":
                from services.bing_service import BingService
                self.ads_service = BingService(session_id=self.session_id)
            case _:
                raise ValueError(f"Unsupported ad platform: {ad_name}")
    def _set_bigquery_service(self,ad_name):
        match ad_name:
            case "bing":
                self.table_ad_data="ad_data"
                self.bigquery = BigQueryService(self.session_id, "BingAds", True)
            case _:
                raise ValueError(f"Unsupported ad platform: {ad_name}")


    # ── Public upload methods ─────────────────────────────────────────────────

    def upload_all_accounts(self, from_x_days: int, to_x_days: int) -> dict:
        """Upload ad data for all accounts."""
        start_date, end_date = self._get_date_range(from_x_days, to_x_days)
        records = self.ads_service.get_all_accounts_ad_performance(
            start_date=start_date,
            end_date=end_date,
        )
        return self._upload_records(records, start_date, end_date)

    def upload_account(self, account_id: str, from_x_days: int, to_x_days: int) -> dict:
        """Upload ad data for a single account."""
        start_date, end_date = self._get_date_range(from_x_days, to_x_days)
        records = self.ads_service.fetch_ad_performance(
            account_id=account_id,
            start_date=start_date,
            end_date=end_date,
        )
        return self._upload_records(records, start_date, end_date)

    # ── Core upload logic ─────────────────────────────────────────────────────

    def _upload_records(self, records: list[AdTableRecord], start_date: date, end_date: date) -> dict:
        try:
            GCPLogger.log(LogLevel.INFO, "bingads-transfer-data", {
                "session_id": self.session_id,
                "message": f"Transfer started for {start_date} -> {end_date}",
            })

            if not records:
                GCPLogger.log(LogLevel.INFO, "bingads-transfer-data", {
                    "session_id": self.session_id,
                    "message": "No records to upload",
                })
                return {"success": True, "rows_uploaded": 0, "accounts_processed": 0}

            #create set of account_ids in the records to optimize deletion
            account_ids = set(r.account_id for r in records if r.account_id)
            self._delete_date_range(start_date, end_date, account_ids)
            self._insert_records(records)

            GCPLogger.log(LogLevel.INFO, "bingads-transfer-data", {
                "session_id": self.session_id,
                "message": f"Uploaded {len(records)} rows for {start_date} -> {end_date}",
                "accounts_processed": len(account_ids),
            })
            return {"success": True, "rows_uploaded": len(records), "accounts_processed": len(account_ids)}

        except Exception as e:
            GCPLogger.log(LogLevel.ERROR, "bingads-transfer-data", {
                "session_id": self.session_id,
                "message": f"Error occurred: {str(e)}",
            })
            raise

    # ── BigQuery operations ───────────────────────────────────────────────────


    def _delete_date_range(
        self,
        start_date: date,
        end_date: date,
        account_ids: Optional[set[str]] = None,
    ) -> None:
        """Delete existing rows for the date range to prevent duplicates."""
        table = self._full_table_name(self.table_ad_data)
        query = f"""
            DELETE FROM `{table}`
            WHERE data_date BETWEEN '{start_date}' AND '{end_date}'
        """
        parameters = None

        if account_ids:
            query += " AND account_id IN UNNEST(@account_ids)"
            parameters = {"account_ids": list(account_ids)}

        self.bigquery.execute_query(query, parameters)


    def _insert_records(self, records: list[AdTableRecord], chunk_size: int = 300) -> None:
        """Insert ad records into BigQuery in chunks to avoid query size limits."""
        table = self._full_table_name(self.table_ad_data)

        for i in range(0, len(records), chunk_size):
            chunk = records[i:i + chunk_size]
            rows  = ",\n".join([
                f"('{r.data_date}', {self._q(r.account_id)}, {self._q(r.account_name)}, "
                f"{self._q(r.campaign_id)}, {self._q(r.campaign_name)}, {self._q(r.campaign_type)}, "
                f"{self._q(r.ad_group_id)}, {self._q(r.ad_group_name)}, {self._q(r.ad_id)}, {self._q(r.ad_name)}, "
                f"{self._q(r.device_type)}, {self._q(r.final_url)}, "
                f"{r.impressions}, {r.clicks}, {r.spend}, {r.conversions})"
                for r in chunk
            ])
            query = f"""
                INSERT INTO `{table}`
                (data_date, account_id, account_name, campaign_id, campaign_name,
                campaign_type, ad_group_id, ad_group_name, ad_id, ad_name,
                device_type, final_url, impressions, clicks, spend, conversions)
                VALUES {rows}
            """
            self.bigquery.execute_query(query)

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _full_table_name(self, table_name: str) -> str:
        return f"{self.bigquery._project_id}.{self.bigquery._database_name}.{table_name}"

    def _get_date_range(self, from_x_days: int, to_x_days: int) -> tuple[date, date]:
        return (
            date.today() - timedelta(days=from_x_days),
            date.today() - timedelta(days=to_x_days),
        )

    def _q(self, val) -> str:
        """Safely quote a string value for SQL, returns NULL for None."""
        if val is None:
            return "NULL"
        escaped = str(val).replace("'", "\\'")
        return f"'{escaped}'"