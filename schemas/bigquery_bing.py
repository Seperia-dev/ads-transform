from datetime import date
from typing import Optional
from dataclasses import dataclass



@dataclass
class AdTableRecord:
    data_date:   date
    account_id:    str
    account_name:  str
    campaign_id:   str
    campaign_name: str
    campaign_type: Optional[str]
    ad_group_id:   str
    ad_group_name: str
    ad_id:         str
    ad_name:       str
    device_type:   Optional[str]
    final_url:     Optional[str]
    impressions:   int
    clicks:        int
    spend:         float
    conversions:   float