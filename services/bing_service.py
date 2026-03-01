import os
from datetime import date
from bingads.service_client import ServiceClient
from bingads.v13.reporting import ReportingDownloadParameters
from bingads.v13.reporting.reporting_service_manager import ReportingServiceManager
from datetime import date
from typing import Optional

from bingads.authorization import OAuthWebAuthCodeGrant, AuthorizationData
from bingads.service_client import ServiceClient
from logger.gcp_logger import GCPLogger, LogLevel
from schemas.bigquery_bing import AdTableRecord


class BingService:
    ENVIRONMENT      = "production"
    POLL_INTERVAL    = 10   # seconds
    MAX_POLL_TIMEOUT = 600  # seconds

    _MS_DEV_TOKEN     = os.environ.get("MICROSOFT_ADS_DEVELOPER_TOKEN", "")
    _MS_CLIENT_ID     = os.environ.get("MICROSOFT_ADS_CLIENT_ID", "")
    _MS_CLIENT_SECRET = os.environ.get("MICROSOFT_ADS_CLIENT_SECRET", "")
    _MS_REFRESH_TOKEN = os.environ.get("MICROSOFT_ADS_REFRESH_TOKEN", "")
    _MS_CUSTOMER_ID   = os.environ.get("MICROSOFT_ADS_CUSTOMER_ID", "")


    def __init__(self, session_id: str):
        self.session_id       = session_id
        self._auth_data: Optional[AuthorizationData] = None
        self._reporting_mgr: Optional[ReportingServiceManager] = None
        self._reporting_svc   = None
        self._authenticate()

    # ── Auth ──────────────────────────────────────────────────────────────────

    def _authenticate(self) -> None:
        try:
            oauth = OAuthWebAuthCodeGrant(
                client_id=self._MS_CLIENT_ID,
                client_secret=self._MS_CLIENT_SECRET,
                redirection_uri="https://login.microsoftonline.com/common/oauth2/nativeclient",
                env=self.ENVIRONMENT,
            )
            oauth.request_oauth_tokens_by_refresh_token(self._MS_REFRESH_TOKEN)

            self._auth_data = AuthorizationData(
                account_id=None,
                customer_id=self._MS_CUSTOMER_ID,
                developer_token=self._MS_DEV_TOKEN,
                authentication=oauth,
            )

            self._reporting_mgr = ReportingServiceManager(
                authorization_data=self._auth_data,
                poll_interval_in_milliseconds=self.POLL_INTERVAL * 1000,
            )

            self._reporting_svc = ServiceClient(
                service="ReportingService",
                version=13,
                authorization_data=self._auth_data,
                environment=self.ENVIRONMENT,
            )

            GCPLogger.log(LogLevel.INFO, "bingads-transfer-data", {
                "session_id": self.session_id,
                "message": "Microsoft Ads authentication successful",
            })
        except Exception as e:
            GCPLogger.log(LogLevel.ERROR, "bingads-transfer-data", {
                "session_id": self.session_id,
                "message": f"Error occurred: {str(e)}",
            })
            raise

    # ── Account discovery ─────────────────────────────────────────────────────

    def get_account_ids(self) -> list[str]:
        try:
            svc = ServiceClient(
                service="CustomerManagementService",
                version=13,
                authorization_data=self._auth_data,
                environment=self.ENVIRONMENT,
            )
            response = svc.GetAccountsInfo()
            ids = [str(a.Id) for a in response.AccountInfo]
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
                all_records.extend(records)
            except Exception as e:
                GCPLogger.log(LogLevel.ERROR, "bingads-transfer-data", {
                    "session_id": self.session_id,
                    "message": f"Error fetching performance for account {account_id}: {str(e)}",
                })
        return all_records

    def fetch_ad_performance(self, account_id, start_date, end_date):

        self._auth_data.account_id = int(account_id)

        # Fresh ServiceClient per account — same pattern as the working example
        svc = ServiceClient(
            service            = "ReportingService",
            version            = 13,
            authorization_data = self._auth_data,
            environment        = self.ENVIRONMENT,
        )
        mgr = ReportingServiceManager(
            authorization_data            = self._auth_data,
            poll_interval_in_milliseconds = self.POLL_INTERVAL * 1000,
        )

        report_request = svc.factory.create(
            "AdPerformanceReportRequest"
        )

        report_request.Aggregation = "Daily"
        report_request.Format = "Csv"
        report_request.ReportName = "Ad Performance Report"
        report_request.ReturnOnlyCompleteData = False

        # Time
        report_time = svc.factory.create("ReportTime")
        report_time.PredefinedTime = None
        report_time.CustomDateRangeStart = {
            "Year": start_date.year,
            "Month": start_date.month,
            "Day": start_date.day,
        }
        report_time.CustomDateRangeEnd = {
            "Year": end_date.year,
            "Month": end_date.month,
            "Day": end_date.day,
        }
        report_request.Time = report_time

        scope = svc.factory.create(
            "AccountThroughAdGroupReportScope"
        )
        scope.AccountIds = {"long": [int(account_id)]}
        scope.AdGroups = None
        report_request.Scope = scope

        # Columns
        cols = svc.factory.create("ArrayOfAdPerformanceReportColumn")
        cols.AdPerformanceReportColumn.extend([
            "TimePeriod",
            "AccountId",
            "AccountName",
            "CampaignId",
            "CampaignName",
            "CampaignType",
            "AdGroupId",
            "AdGroupName",
            "AdId",
            "AdTitle",
            "DeviceType",
            "FinalUrl",
            "Impressions",
            "Clicks",
            "Spend",
            "Conversions",
        ])
        report_request.Columns = cols

        download_parameters = ReportingDownloadParameters(
            report_request        = report_request,
            result_file_directory = None,
            result_file_name      = None,
            overwrite_result_file = True,
            timeout_in_milliseconds = self.MAX_POLL_TIMEOUT * 1000,
        )

        report_container = mgr.download_report(download_parameters)

        return report_container