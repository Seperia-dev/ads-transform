import uuid
from datetime import date, timedelta

from fastapi import APIRouter, HTTPException

from logger.gcp_logger import GCPLogger, LogLevel
from schemas.bing import TransferRequest, TransferResponse

router = APIRouter()


@router.post("/transfer", response_model=TransferResponse)
def trigger_transfer(request: TransferRequest) -> TransferResponse:
    session_id = str(uuid.uuid4())
    yesterday  = date.today() - timedelta(days=1)
    start      = request.start_date or yesterday
    end        = request.end_date   or yesterday

    # if start > end:
    #     raise HTTPException(status_code=400, detail="start_date must be <= end_date")

    # GCPLogger.log(LogLevel.INFO, "bingads-transfer-data", {
    #     "session_id": session_id,
    #     "message": f"Transfer request received: {start} -> {end}",
    # })

    # # summary = TransferService(session_id=session_id).run(start_date=start, end_date=end)
    # status  = "completed_with_errors" if summary["errors"] else "completed"
    # return TransferResponse(**summary, status=status)