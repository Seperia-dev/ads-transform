from datetime import date
from typing import Optional
from pydantic import BaseModel

class TransferRequest(BaseModel):
    from_x_days: int
    to_x_days: int
    ad_name: str


class TransferResponse(BaseModel):
    session_id: str
    accounts_processed: int
    rows_uploaded: int
    success: bool

class AccountTransferRequest(TransferRequest):
    account_id: str

