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
from services.backgroundTaskLog import BackgroundTaskLog


class BingService:
    ENVIRONMENT      = "production"
    POLL_INTERVAL    = 10   # seconds
    MAX_POLL_TIMEOUT = 600  # seconds

    def __init__(self, session_id: str,background_task_log: BackgroundTaskLog | None = None) -> None:
        self.session_id        = session_id
        self.background_task_log          = background_task_log
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

            GCPLogger.log(LogLevel.INFO, "Bing", {
                "session_id": self.session_id,
                "message": "Microsoft Ads authentication successful",
            })
        except Exception as e:
            GCPLogger.log(LogLevel.ERROR, "Bing", {
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
            GCPLogger.log(LogLevel.INFO, "Bing", {
                "session_id": self.session_id,
                "message": f"Found {len(ids)} accounts",
                "ids": ids,
            })
            return ids
        except Exception as e:
            GCPLogger.log(LogLevel.ERROR, "Bing", {
                "session_id": self.session_id,
                "message": f"Error occurred: {str(e)}",
            })
            raise

    # ── Ad performance ────────────────────────────────────────────────────────

    def get_all_accounts_ad_performance(
        self,
        start_date: date,
        end_date: date,
    ) -> list[BingAdTableRecord]:
        all_records = []
        self._update_task_log(step="Fetching Bing Accounts")
        account_ids = self.get_account_ids()
        total_accounts = len(account_ids)
        for idx, account_id in enumerate(account_ids):
            try:
                self._update_task_log(step=f"Fetching Account Performance ({idx+1} of {total_accounts})")
                records = self.fetch_ad_performance(
                    account_id=account_id,
                    start_date=start_date,
                    end_date=end_date,
                )
                if records:
                    all_records.extend(records)
            except Exception as e:
                GCPLogger.log(LogLevel.ERROR, "Bing", {
                    "session_id": self.session_id,
                    "message": f"Error fetching performance for account {account_id}: {str(e)}",
                })

        return all_records

    def fetch_ad_performance(self, account_id: str, start_date: date, end_date: date) -> list[BingAdTableRecord]:
        regular = self._fetch_ad_report(account_id, start_date, end_date)
        pmax    = self._fetch_pmax_report(account_id, start_date, end_date)
        return regular + pmax

    def _fetch_ad_report(self, account_id: str, start_date: date, end_date: date) -> list[BingAdTableRecord]:
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

        req.Time = self._build_report_time(svc, start_date, end_date)

        dl = ReportingDownloadParameters(
            report_request        = req,
            result_file_directory = tempfile.gettempdir(),
            result_file_name      = f"ad_report_{account_id}_{start_date.strftime('%Y-%m-%d')}.csv",
            overwrite_result_file = True,
        )
        return self._parse_ad_report(mgr.download_report(dl))

    def _fetch_pmax_report(self, account_id: str, start_date: date, end_date: date) -> list[BingAdTableRecord]:
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

        req = svc.factory.create("AssetGroupPerformanceReportRequest")
        req.Format                 = "Csv"
        req.ReportName             = f"PMax report {start_date}"
        req.Aggregation            = "Daily"
        req.ReturnOnlyCompleteData = False

        cols = svc.factory.create("ArrayOfAssetGroupPerformanceReportColumn")
        cols.AssetGroupPerformanceReportColumn.extend([
            "TimePeriod", "AccountId", "AccountName",
            "CampaignId", "CampaignName",
            "AssetGroupId", "AssetGroupName",
            "Impressions", "Clicks", "Spend", "Conversions",
        ])
        req.Columns = cols

        scope = svc.factory.create("AccountThroughAssetGroupReportScope")
        scope.AccountIds  = {"long": [int(account_id)]}
        scope.AssetGroups = None
        req.Scope         = scope

        req.Time = self._build_report_time(svc, start_date, end_date)

        dl = ReportingDownloadParameters(
            report_request        = req,
            result_file_directory = tempfile.gettempdir(),
            result_file_name      = f"pmax_report_{account_id}_{start_date.strftime('%Y-%m-%d')}.csv",
            overwrite_result_file = True,
        )
        return self._parse_pmax_report(mgr.download_report(dl))

    # ── Parsers ───────────────────────────────────────────────────────────────

    def _parse_ad_report(self, report_container) -> list[BingAdTableRecord]:
        records = []
        if report_container is None:
            return records

        for record in report_container.report_records:
            def col(name):
                try:
                    return record.value(name)
                except Exception:
                    return None

            records.append(BingAdTableRecord(
                data_date     = date.fromisoformat(col("TimePeriod")),
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
                impressions   = int(col("Impressions")   or 0),
                clicks        = int(col("Clicks")        or 0),
                spend         = float(col("Spend")       or 0.0),
                conversions   = float(col("Conversions") or 0.0),
            ))

        return records

    def _parse_pmax_report(self, report_container) -> list[BingAdTableRecord]:
        records = []
        if report_container is None:
            return records

        for record in report_container.report_records:
            def col(name):
                try:
                    return record.value(name)
                except Exception:
                    return None

            records.append(BingAdTableRecord(
                data_date     = date.fromisoformat(col("TimePeriod")),
                account_id    = col("AccountId")      or "",
                account_name  = col("AccountName")    or "",
                campaign_id   = col("CampaignId")     or "",
                campaign_name = col("CampaignName")   or "",
                campaign_type = "PerformanceMax",
                ad_group_id   = col("AssetGroupId")   or "",  # asset group mapped to ad_group
                ad_group_name = col("AssetGroupName") or "",
                ad_id         = "",                            # no ad-level for PMax
                ad_name       = "",
                device_type   = None,                          # not available for PMax reports
                final_url     = None,                          # not available at asset group level
                impressions   = int(col("Impressions")   or 0),
                clicks        = int(col("Clicks")        or 0),
                spend         = float(col("Spend")       or 0.0),
                conversions   = float(col("Conversions") or 0.0),
            ))

        return records

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _build_report_time(self, svc: ServiceClient, start_date: date, end_date: date):
        """Build a ReportTime object with a custom date range."""
        report_time = svc.factory.create("ReportTime")
        report_time.CustomDateRangeStart = svc.factory.create("Date")
        report_time.CustomDateRangeStart.Day   = start_date.day
        report_time.CustomDateRangeStart.Month = start_date.month
        report_time.CustomDateRangeStart.Year  = start_date.year
        report_time.CustomDateRangeEnd = svc.factory.create("Date")
        report_time.CustomDateRangeEnd.Day   = end_date.day
        report_time.CustomDateRangeEnd.Month = end_date.month
        report_time.CustomDateRangeEnd.Year  = end_date.year
        return report_time

    def find_account_by_campaign_id(self, campaign_id: str) -> Optional[str]:
        """Loop through all accounts and return the account_id that contains the given campaign_id."""
        account_ids = self.get_account_ids()

        for account_id in account_ids:
            try:
                svc = self._create_service_client(account_id, "CampaignManagementService")
                response = svc.GetCampaignsByAccountId(
                    AccountId=int(account_id),
                    CampaignType="Search Shopping DynamicSearchAds Audience PerformanceMax",
                )
                for campaign in response.Campaign or []:
                    if str(campaign.Id) == str(campaign_id):
                        GCPLogger.log(LogLevel.INFO, "Bing", {
                            "session_id": self.session_id,
                            "message": f"Found campaign {campaign_id} in account {account_id}",
                            "campaign_name": campaign.Name,
                        })
                        return account_id
            except Exception as e:
                GCPLogger.log(LogLevel.ERROR, "Bing", {
                    "session_id": self.session_id,
                    "message": f"Error checking account {account_id}: {str(e)}",
                })
                continue

        return None

    def get_campaign_types(self, account_id: str) -> list[str]:
        svc = self._create_service_client(account_id, "CampaignManagementService")
        response = svc.GetCampaignsByAccountId(
            AccountId=int(account_id),
            CampaignType="Search Shopping DynamicSearchAds Audience PerformanceMax",
        )
        return list({c.CampaignType for c in response.Campaign or []})

    def _update_task_log(self,  step:   str | None = None,status: str | None = None) -> None:
        if self.background_task_log:
            self.background_task_log.update_task(step=step, status=status)