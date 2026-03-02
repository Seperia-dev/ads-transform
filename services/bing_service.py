import os
import copy
import tempfile
from datetime import date
from typing import Optional

from bingads.authorization import OAuthWebAuthCodeGrant, AuthorizationData
from bingads.service_client import ServiceClient
from bingads.v13.reporting import ReportingDownloadParameters
from bingads.v13.reporting.reporting_service_manager import ReportingServiceManager

from logger.gcp_logger import GCPLogger, LogLevel
from schemas.bigquery_bing import BingAdTableRecord


class BingService:
    ENVIRONMENT      = "production"
    POLL_INTERVAL    = 10   # seconds
    MAX_POLL_TIMEOUT = 600  # seconds

    def __init__(self, session_id: str):
        self.session_id        = session_id
        self._authorization_data: Optional[AuthorizationData] = None
        self._environment      = self.ENVIRONMENT
        self._initialize_client()

    # ── Auth ──────────────────────────────────────────────────────────────────

    def _initialize_client(self) -> None:
        try:
            customer_id     = os.environ.get("MICROSOFT_ADS_CUSTOMER_ID")
            developer_token = os.environ.get("MICROSOFT_ADS_DEVELOPER_TOKEN")
            client_id       = os.environ.get("MICROSOFT_ADS_CLIENT_ID")
            client_secret   = os.environ.get("MICROSOFT_ADS_CLIENT_SECRET")
            refresh_token   = os.environ.get("MICROSOFT_ADS_REFRESH_TOKEN")

            authentication = OAuthWebAuthCodeGrant(
                client_id=client_id,
                client_secret=client_secret,
                redirection_uri="https://unidb.seperia.com/ms/auth",
                env=self._environment,
            )
            authentication.request_oauth_tokens_by_refresh_token(refresh_token)

            self._authorization_data = AuthorizationData(
                account_id=None,
                customer_id=customer_id,
                developer_token=developer_token,
                authentication=authentication,
            )

            GCPLogger.log(LogLevel.INFO, "bingads-transfer-data", {
                "session_id": self.session_id,
                "message": "Microsoft Ads authentication successful",
            })
        except Exception as e:
            GCPLogger.log(LogLevel.ERROR, "bingads-transfer-data", {
                "session_id": self.session_id,
                "message": f"Authentication error: {str(e)}",
            })
            raise

    def _create_service_client(self, account_id: str, service_name: str) -> ServiceClient:
        """Create a fresh service client with a deep-copied auth per account."""
        auth = copy.deepcopy(self._authorization_data)
        auth.account_id = int(account_id)
        return ServiceClient(
            service            = service_name,
            authorization_data = auth,
            version            = 13,
            environment        = self._environment,
        )

    # ── Account discovery ─────────────────────────────────────────────────────

    def get_account_ids(self) -> list[str]:
        try:
            svc      = self._create_service_client("0", "CustomerManagementService")
            response = svc.GetAccountsInfo()
            ids      = [str(a.Id) for a in response.AccountInfo]
            GCPLogger.log(LogLevel.INFO, "bingads-transfer-data", {
                "session_id": self.session_id,
                "message": f"Found {len(ids)} accounts",
                "ids": ids,
            })
            return ids
        except Exception as e:
            GCPLogger.log(LogLevel.ERROR, "bingads-transfer-data", {
                "session_id": self.session_id,
                "message": f"Error occurred: {str(e)}",
            })
            raise

    # ── Ad performance ────────────────────────────────────────────────────────

    def get_all_accounts_ad_performance(
        self,
        start_date: date,
        end_date: date,
    ) -> list[dict]:
        all_records = []
        account_ids = self.get_account_ids()
        for account_id in account_ids:
            try:
                records = self.fetch_ad_performance(
                    account_id=account_id,
                    start_date=start_date,
                    end_date=end_date,
                )
                if records:
                    all_records.extend(records)
            except Exception as e:
                GCPLogger.log(LogLevel.ERROR, "bingads-transfer-data", {
                    "session_id": self.session_id,
                    "message": f"Error fetching performance for account {account_id}: {str(e)}",
                })

        return all_records

    def fetch_ad_performance(self, account_id: str, start_date: date, end_date: date):
        # ONE shared auth object for both svc and mgr — avoids suds WSDL context mismatch
        auth            = copy.deepcopy(self._authorization_data)
        auth.account_id = int(account_id)

        svc = ServiceClient(
            service            = "ReportingService",
            authorization_data = auth,
            version            = 13,
            environment        = self._environment,
        )
        mgr = ReportingServiceManager(
            authorization_data            = auth,
            poll_interval_in_milliseconds = self.POLL_INTERVAL * 1000,
        )

        req = svc.factory.create("AdPerformanceReportRequest")
        req.Format                 = "Csv"
        req.ReportName             = f"Ad report {start_date}"
        req.Aggregation            = "Daily"
        req.ReturnOnlyCompleteData = False

        cols = svc.factory.create("ArrayOfAdPerformanceReportColumn")
        cols.AdPerformanceReportColumn.extend([
            "TimePeriod", "AccountId", "AccountName",
            "CampaignId", "CampaignName", "CampaignType",
            "AdGroupId", "AdGroupName", "AdId", "AdTitle",
            "DeviceType", "FinalUrl",
            "Impressions", "Clicks", "Spend", "Conversions",
        ])
        req.Columns = cols

        scope = svc.factory.create("AccountThroughAdGroupReportScope")
        scope.AccountIds = {"long": [int(account_id)]}
        scope.AdGroups   = None
        req.Scope        = scope

        report_time = svc.factory.create("ReportTime")
        report_time.CustomDateRangeStart = svc.factory.create("Date")
        report_time.CustomDateRangeStart.Day   = start_date.day
        report_time.CustomDateRangeStart.Month = start_date.month
        report_time.CustomDateRangeStart.Year  = start_date.year
        report_time.CustomDateRangeEnd = svc.factory.create("Date")
        report_time.CustomDateRangeEnd.Day   = end_date.day
        report_time.CustomDateRangeEnd.Month = end_date.month
        report_time.CustomDateRangeEnd.Year  = end_date.year
        req.Time = report_time

        ymd      = start_date.strftime("%Y-%m-%d")
        csv_name = f"ad_report_{account_id}_{ymd}.csv"

        dl = ReportingDownloadParameters(
            report_request        = req,
            result_file_directory = tempfile.gettempdir(),
            result_file_name      = csv_name,
            overwrite_result_file = True,
        )
        report_container = mgr.download_report(dl)
        return self._parse_ad_report(report_container)

    def _parse_ad_report(self, report_container) -> list[BingAdTableRecord]:
        records = []

        if report_container is None:
            return records

        column_names = report_container.report_columns

        for record in report_container.report_records:
            def col(name):
                try:
                    return record.value(name)
                except Exception:
                    return None

            records.append(BingAdTableRecord(
                data_date   = date.fromisoformat(col("TimePeriod")),
                account_id    = col("AccountId")    or "",
                account_name  = col("AccountName")  or "",
                campaign_id   = col("CampaignId")   or "",
                campaign_name = col("CampaignName") or "",
                campaign_type = col("CampaignType"),
                ad_group_id   = col("AdGroupId")    or "",
                ad_group_name = col("AdGroupName")  or "",
                ad_id         = col("AdId")         or "",
                ad_name       = col("AdTitle")      or "",
                device_type   = col("DeviceType"),
                final_url     = col("FinalUrl"),
                impressions   = int(col("Impressions")  or 0),
                clicks        = int(col("Clicks")       or 0),
                spend         = float(col("Spend")      or 0.0),
                conversions   = float(col("Conversions") or 0.0),
            ))

        return records