from datetime import datetime

from fastapi import APIRouter, BackgroundTasks, HTTPException

from logger.gcp_logger import GCPLogger, LogLevel
from schemas.transfer import AccountTransferRequest, TransferRequest, TransferResponse
from services.background_task_log import BackgroundTaskLog
from services.transfer_service import TransferService

router = APIRouter(prefix="/transfer", tags=["Transfer"])


# ── /all ─────────────────────────────────────────────────────────────────────
#this is transfer the performance (like click,spend,conversions,impressions) data as for all accounts
@router.post("/all", response_model=TransferResponse)
def transfer_all_accounts(req: TransferRequest, background_tasks: BackgroundTasks):
    session_id = _make_session_id()
    background_task_log   = None
    try:
        GCPLogger.log(LogLevel.INFO, "transfer_all_accounts", {
            "session_id": session_id,
            "ad_name": req.ad_name,
            "from_x_days": req.from_x_days,
            "to_x_days": req.to_x_days,
            "background": req.background,
            "use_delete_rows":req.use_delete_rows,
        })

        if not req.ad_name:
            raise ValueError("ad_name is required")
        if req.from_x_days is None or req.to_x_days is None:
            raise ValueError("from_x_days and to_x_days are required")
        if req.from_x_days < req.to_x_days:
            raise ValueError("from_x_days cannot be less than to_x_days")

        background_task_log = _make_background_task_log(
                name=f"transfer_all_{req.ad_name}",
                req_args=req.model_dump(exclude={"background"}),
                session_id=session_id,
            )

        if req.background:
            transfer_service = _make_transfer_service(session_id, req.ad_name, background_task_log)
            background_tasks.add_task(
                transfer_service.upload_all_accounts,
                from_x_days=req.from_x_days,
                to_x_days=req.to_x_days,
                use_delete=req.use_delete_rows
            )
            return TransferResponse(
                session_id=session_id,
                task_id=background_task_log.task_id,
                success=True,
                rows_uploaded=0,
                accounts_processed=0,
            )

        transfer_service = _make_transfer_service(session_id, req.ad_name,background_task_log)
        result = transfer_service.upload_all_accounts(
            from_x_days=req.from_x_days,
            to_x_days=req.to_x_days,
            use_delete=req.use_delete_rows,
        )
        GCPLogger.log(LogLevel.INFO, "transfer_all_accounts_result", {
            "session_id": session_id,
            "result": result,
        })
        return TransferResponse(session_id=session_id, **result)

    except Exception as e:
        _handle_error(e, session_id, background_task_log)


# ── /account ─────────────────────────────────────────────────────────────────
#this is transfer the performance (like click,spend,conversions,impressions) data as for single account
@router.post("/account", response_model=TransferResponse)
def transfer_single_account(req: AccountTransferRequest, background_tasks: BackgroundTasks):
    session_id = _make_session_id()
    background_task_log   = None
    try:
        GCPLogger.log(LogLevel.INFO, "transfer_single_account", {
            "session_id": session_id,
            "account_id": req.account_id,
            "ad_name": req.ad_name,
            "from_x_days": req.from_x_days,
            "to_x_days": req.to_x_days,
            "background": req.background,
        })
        if not req.account_id:
            raise ValueError("account_id is required")
        if not req.ad_name:
            raise ValueError("ad_name is required")
        if req.from_x_days is None or req.to_x_days is None:
            raise ValueError("from_x_days and to_x_days are required")
        if req.from_x_days < req.to_x_days:
            raise ValueError("from_x_days cannot be less than to_x_days")

        background_task_log = _make_background_task_log(
                name=f"transfer_account_{req.ad_name}",
                req_args=req.model_dump(exclude={"background"}),
                session_id=session_id,
            )

        if req.background:
            transfer_service = _make_transfer_service(session_id, req.ad_name, background_task_log)
            background_tasks.add_task(
                transfer_service.upload_account,
                account_id=req.account_id,
                from_x_days=req.from_x_days,
                to_x_days=req.to_x_days,
                use_delete=req.use_delete_rows
            )
            return TransferResponse(
                session_id=session_id,
                task_id=background_task_log.task_id,
                success=True,
                rows_uploaded=0,
                accounts_processed=0,
            )

        transfer_service = _make_transfer_service(session_id, req.ad_name, background_task_log)
        result = transfer_service.upload_account(
            account_id=req.account_id,
            from_x_days=req.from_x_days,
            to_x_days=req.to_x_days,
            use_delete=req.use_delete_rows,
        )
        GCPLogger.log(LogLevel.INFO, "transfer_single_account_result", {
            "session_id": session_id,
            "result": result,
        })
        return TransferResponse(session_id=session_id, **result)

    except Exception as e:
        _handle_error(e, session_id, background_task_log)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _handle_error(e: Exception, session_id: str, background_task_log: BackgroundTaskLog | None) -> None:
    error_msg = str(e)
    GCPLogger.log(LogLevel.ERROR, "transfer_error", {
        "session_id": session_id,
        "message": error_msg,
    })
    if background_task_log:
        background_task_log.fail_task(error=error_msg)
    raise HTTPException(status_code=500, detail=error_msg)


def _make_session_id() -> str:
    return str(int(datetime.utcnow().timestamp()))


def _make_transfer_service(
    session_id:          str,
    ad_name:             str,
    background_task_log: BackgroundTaskLog | None = None,
) -> TransferService:
    return TransferService(
        session_id=session_id,
        ad_name=ad_name,
        background_task_log=background_task_log,
    )


def _make_background_task_log(name: str, req_args: dict, session_id: str | None = None) -> BackgroundTaskLog:
    return BackgroundTaskLog(name=name, req_args=req_args, session_id=session_id)