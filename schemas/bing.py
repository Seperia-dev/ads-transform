from dataclasses import dataclass
from datetime import date
from typing import Optional


# ── Dataclasses (internal DTOs) ───────────────────────────────────────────────

@dataclass
class CampaignRecord:
    time_period:   date
    account_id:    str
    account_name:  str
    campaign_id:   str
    campaign_name: str
    impressions:   int
    clicks:        int
    spend:         float
    conversions:   float


@dataclass
class AdGroupRecord:
    time_period:   date
    account_id:    str
    account_name:  str
    campaign_id:   str
    campaign_name: str
    ad_group_id:   str
    ad_group_name: str
    impressions:   int
    clicks:        int
    spend:         float
    conversions:   float


@dataclass
class AdRecord:
    time_period:   date
    account_id:    str
    account_name:  str
    campaign_id:   str
    campaign_name: str
    ad_group_id:   str
    ad_group_name: str
    ad_id:         str
    device_type:   Optional[str]
    final_url:     Optional[str]
    impressions:   int
    clicks:        int
    spend:         float
    conversions:   float



# ── BigQuery table schemas ─────────────────────────────────────────────────────

CAMPAIGN_SCHEMA = [
    {"name": "time_period",   "type": "DATE"},
    {"name": "account_id",    "type": "STRING"},
    {"name": "account_name",  "type": "STRING"},
    {"name": "campaign_id",   "type": "STRING"},
    {"name": "campaign_name", "type": "STRING"},
    {"name": "impressions",   "type": "INTEGER"},
    {"name": "clicks",        "type": "INTEGER"},
    {"name": "spend",         "type": "FLOAT"},
    {"name": "conversions",   "type": "FLOAT"},
]

AD_GROUP_SCHEMA = [
    {"name": "time_period",    "type": "DATE"},
    {"name": "account_id",     "type": "STRING"},
    {"name": "account_name",   "type": "STRING"},
    {"name": "campaign_id",    "type": "STRING"},
    {"name": "campaign_name",  "type": "STRING"},
    {"name": "ad_group_id",    "type": "STRING"},
    {"name": "ad_group_name",  "type": "STRING"},
    {"name": "impressions",    "type": "INTEGER"},
    {"name": "clicks",         "type": "INTEGER"},
    {"name": "spend",          "type": "FLOAT"},
    {"name": "conversions",    "type": "FLOAT"},
]

AD_SCHEMA = [
    {"name": "time_period",    "type": "DATE"},
    {"name": "account_id",     "type": "STRING"},
    {"name": "account_name",   "type": "STRING"},
    {"name": "campaign_id",    "type": "STRING"},
    {"name": "campaign_name",  "type": "STRING"},
    {"name": "ad_group_id",    "type": "STRING"},
    {"name": "ad_group_name",  "type": "STRING"},
    {"name": "ad_id",          "type": "STRING"},
    {"name": "device_type",    "type": "STRING"},
    {"name": "final_url",      "type": "STRING"},
    {"name": "impressions",    "type": "INTEGER"},
    {"name": "clicks",         "type": "INTEGER"},
    {"name": "spend",          "type": "FLOAT"},
    {"name": "conversions",    "type": "FLOAT"},
]