from datetime import date, datetime, timedelta
from typing import Optional

from logger.gcp_logger import GCPLogger, LogLevel
from services.background_task_log import BackgroundTaskLog
from services.bigquery_service import BigQueryService
from schemas.bigquery_bing import BingAdTableRecord


class TransferService:
    def __init__(self, session_id: str, ad_name: str = "", background_task_log: BackgroundTaskLog | None = None) -> None:
        if session_id is None:
            session_id = str(int(datetime.utcnow().timestamp()))
        self.session_id = session_id
        self.ads_service = None
        self.bigquery = None
        self.background_task_log = background_task_log
        self._set_ads_service(ad_name)
        self._set_bigquery_service(ad_name)

    # ── Ad service factory ────────────────────────────────────────────────────

    def _set_ads_service(self, ad_name: str = ""):
        match ad_name:
            case "bing":
                from services.bing_service import BingService
                self.ads_service = BingService(session_id=self.session_id, background_task_log=self.background_task_log)
            case _:
                raise ValueError(f"Unsupported ad platform: {ad_name}")

    def _set_bigquery_service(self, ad_name):
        match ad_name:
            case "bing":
                self.table_ad_data = "ad_data"
                self.bigquery = BigQueryService(self.session_id, "BingAds", True)
            case _:
                raise ValueError(f"Unsupported ad platform: {ad_name}")

    # ── Public upload methods ─────────────────────────────────────────────────

    def upload_all_accounts(self, from_x_days: int, to_x_days: int, use_delete: bool = False) -> dict:
        """Upload ad data for all accounts."""
        start_date, end_date = self._get_date_range(from_x_days, to_x_days)
        records = self.ads_service.get_all_accounts_ad_performance(
            start_date=start_date,
            end_date=end_date,
        )
        return self._upload_records(records, start_date, end_date, use_delete=use_delete)

    def upload_account(self, account_id: str, from_x_days: int, to_x_days: int, use_delete: bool = False) -> dict:
        """Upload ad data for a single account."""
        start_date, end_date = self._get_date_range(from_x_days, to_x_days)
        self._update_task_step(f"Fetching data for specific account: {account_id}")
        records = self.ads_service.fetch_ad_performance(
            account_id=account_id,
            start_date=start_date,
            end_date=end_date,
        )
        return self._upload_records(records, start_date, end_date, use_delete=use_delete)

    # ── Core upload logic ─────────────────────────────────────────────────────

    def _upload_records(
        self,
        records: list[BingAdTableRecord],
        start_date: date,
        end_date: date,
        use_delete: bool = False,
    ) -> dict:
        try:
            GCPLogger.log(LogLevel.INFO, "transfer_data", {
                "session_id": self.session_id,
                "message": f"Transfer started for {start_date} -> {end_date} (use_delete={use_delete})",
            })

            if not records:
                GCPLogger.log(LogLevel.INFO, "transfer_data", {
                    "session_id": self.session_id,
                    "message": "No records to upload",
                })
                return {"success": True, "rows_uploaded": 0, "accounts_processed": 0}

            # create set of account_ids in the records to scope BQ queries
            account_ids = set(r.account_id for r in records if r.account_id)

            #use delete is True cause assets data like impressions and clicks are updated retroactively in the source system,
            #so we need to delete the existing data for the date range to avoid duplicates.
            #If False, we filter out existing rows and only insert new ones,
            # which is faster but may result in duplicates if source data was updated after initial transfer.
            if use_delete:
                self._update_task_step(f"Deleting BigQuery data for {len(account_ids)} accounts")
                self._delete_date_range(start_date, end_date, account_ids)
            else:
                self._update_task_step(f"Filtering already-existing rows for {len(account_ids)} accounts")
                records = self._filter_new_records(records, start_date, end_date, account_ids)

                if not records:
                    GCPLogger.log(LogLevel.INFO, "transfer_data", {
                        "session_id": self.session_id,
                        "message": "All records already exist in BigQuery, nothing to insert",
                    })
                    res = {"success": True, "rows_uploaded": 0, "accounts_processed": len(account_ids)}
                    self._update_task_step("Task completed — no new rows")
                    if self.background_task_log:
                        self.background_task_log.end_task(result=res)
                    return res

            self._insert_records(records)

            GCPLogger.log(LogLevel.INFO, "transfer_data", {
                "session_id": self.session_id,
                "message": f"Uploaded {len(records)} rows for {start_date} -> {end_date}",
                "accounts_processed": len(account_ids),
            })
            res = {"success": True, "rows_uploaded": len(records), "accounts_processed": len(account_ids)}
            self._update_task_step(f"Task completed for {start_date} -> {end_date}")
            if self.background_task_log:
                self.background_task_log.end_task(result=res)
            return res

        except Exception as e:
            GCPLogger.log(LogLevel.ERROR, "transfer_data_error", {
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
        GCPLogger.log(LogLevel.INFO, "transfer_data", {
            "session_id": self.session_id,
            "message": f"Deleted existing rows for {start_date} -> {end_date}",
            "account_ids": list(account_ids) if account_ids else "all",
        })

    def _filter_new_records(
        self,
        records: list[BingAdTableRecord],
        start_date: date,
        end_date: date,
        account_ids: Optional[set[str]] = None,
        ignore_columns: list[str] = None,
    ) -> list[BingAdTableRecord]:
        """Query BigQuery for existing rows and return only records not already present.

        Compares all columns except those in ignore_columns as the natural key.
        The query window is expanded by one day on each side to catch edge cases
        where a record's data_date falls just outside the nominal range.
        """
        if ignore_columns is None:
            ignore_columns = ["pulled_at","impressions","clicks","spend","conversions"]

        buffered_start = start_date - timedelta(days=1)
        buffered_end = end_date + timedelta(days=1)

        table = self._full_table_name(self.table_ad_data)
        query = f"""
            SELECT *
            FROM `{table}`
            WHERE data_date BETWEEN '{buffered_start}' AND '{buffered_end}'
        """
        parameters = None

        if account_ids:
            query += " AND account_id IN UNNEST(@account_ids)"
            parameters = {"account_ids": list(account_ids)}

        rows = self.bigquery.execute_query(query, parameters)
        rows =  rows.data if rows else []

        def _make_row_key(row: dict) -> tuple:
            return tuple(
                str(v) for k, v in row.items()
                if k not in ignore_columns
            )

        def _make_record_key(record: BingAdTableRecord) -> tuple:
            return tuple(
                str(v) for k, v in vars(record).items()
                if k not in ignore_columns
            )

        existing_keys = {_make_row_key(row) for row in rows}

        GCPLogger.log(LogLevel.INFO, "transfer_data", {
            "session_id": self.session_id,
            "message": (
                f"Found {len(existing_keys)} existing rows in BigQuery "
                f"for buffered range {buffered_start} -> {buffered_end}"
            ),
        })

        filtered = [
            r for r in records
            if _make_record_key(r) not in existing_keys
        ]

        GCPLogger.log(LogLevel.INFO, "transfer_data", {
            "session_id": self.session_id,
            "message": (
                f"{len(records) - len(filtered)} duplicate rows removed, "
                f"{len(filtered)} new rows to insert"
            ),
        })

        return filtered

    def _insert_records(self, records: list[BingAdTableRecord], chunk_size: int = 300) -> None:
        """Insert ad records into BigQuery in chunks to avoid query size limits."""
        table = self._full_table_name(self.table_ad_data)
        total_chunks = -(-len(records) // chunk_size)
        pulled_at = int(datetime.utcnow().timestamp())

        GCPLogger.log(LogLevel.INFO, "transfer_data", {
            "session_id": self.session_id,
            "message": f"Starting to insert {len(records)} records in {total_chunks} chunks",
            "pulled_at": pulled_at,
        })
        for i in range(0, len(records), chunk_size):
            chunk_num = (i // chunk_size) + 1
            chunk = records[i:i + chunk_size]
            rows = ",\n".join([
                f"('{r.data_date}', {self._q(r.account_id)}, {self._q(r.account_name)}, "
                f"{self._q(r.campaign_id)}, {self._q(r.campaign_name)}, {self._q(r.campaign_type)}, "
                f"{self._q(r.ad_group_id)}, {self._q(r.ad_group_name)}, {self._q(r.ad_id)}, {self._q(r.ad_name)}, "
                f"{self._q(r.device_type)}, {self._q(r.final_url)}, "
                f"{r.impressions}, {r.clicks}, {r.spend}, {r.conversions}, {pulled_at})"
                for r in chunk
            ])
            query = f"""
                INSERT INTO `{table}`
                (data_date, account_id, account_name, campaign_id, campaign_name,
                campaign_type, ad_group_id, ad_group_name, ad_id, ad_name,
                device_type, final_url, impressions, clicks, spend, conversions, pulled_at)
                VALUES {rows}
            """
            self._update_task_step(f"Inserting Chunk {chunk_num} of {total_chunks}")
            self.bigquery.execute_query(query)
            GCPLogger.log(LogLevel.INFO, "transfer_data", {
                "session_id": self.session_id,
                "message": f"Inserted Chunk {chunk_num} of {total_chunks}",
            })

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

    def _update_task_step(self, step: str) -> None:
        if self.background_task_log:
            self.background_task_log.update_task(step=step)