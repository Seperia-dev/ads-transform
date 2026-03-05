from datetime import date
from typing import Optional
from pydantic import BaseModel

class TransferRequest(BaseModel):
    ad_name:      str
    from_x_days:  int | None = None
    to_x_days:    int | None = None
    background:   bool = True

class TransferResponse(BaseModel):
    session_id:         str
    task_id:            str | None = None
    success:            bool
    rows_uploaded:      int
    accounts_processed: int

class AccountTransferRequest(TransferRequest):
    account_id: str

