from datetime import date
from typing import Optional
from pydantic import BaseModel

class TransferRequest(BaseModel):
    start_date: Optional[date] = None  
    end_date: Optional[date]   = None


class TransferResponse(BaseModel):
    session_id: str
    start_date: str
    end_date: str
    accounts_processed: int
    campaign_rows: int
    ad_group_rows: int
    ad_rows: int
    errors: list[str]
    status: str

