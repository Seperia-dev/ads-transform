import uuid
from datetime import datetime
from fastapi import APIRouter, HTTPException
from logger.gcp_logger import GCPLogger, LogLevel
from schemas.transfer import AccountTransferRequest, TransferRequest, TransferResponse
from services.transfer_service import TransferService

router = APIRouter(prefix="/transfer", tags=["Transfer"])


def _make_session_id() -> str:
    return str(int(datetime.utcnow().timestamp()))


def _make_service(session_id: str, ad_name: str) -> TransferService:
    return TransferService(session_id=session_id, ad_name=ad_name)


@router.post("/all", response_model=TransferResponse)
def transfer_all_accounts(req: TransferRequest):
    """Upload ad data for all accounts of the given platform."""
    session_id = _make_session_id()
    try:
        svc    = _make_service(session_id, req.ad_name)
        result = svc.upload_all_accounts(
            from_x_days=req.from_x_days,
            to_x_days=req.to_x_days,
        )
        return TransferResponse(session_id=session_id, **result)
    except Exception as e:
        GCPLogger.log(LogLevel.ERROR, "transfer-router", {
            "session_id": session_id,
            "message": str(e),
        })
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/account", response_model=TransferResponse)
def transfer_single_account(req: AccountTransferRequest):
    """Upload ad data for a single account of the given platform."""
    session_id = _make_session_id()
    try:
        svc    = _make_service(session_id, req.ad_name)
        result = svc.upload_account(
            account_id=req.account_id,
            from_x_days=req.from_x_days,
            to_x_days=req.to_x_days,
        )
        return TransferResponse(session_id=session_id, **result)
    except Exception as e:
        GCPLogger.log(LogLevel.ERROR, "transfer-router", {
            "session_id": session_id,
            "message": str(e),
        })
        raise HTTPException(status_code=500, detail=str(e))