import json
import uuid
from datetime import datetime, timezone
from typing import Any

from schemas.background_task import TaskReqArgs, TaskResult
from services.bigquery_service import BigQueryService


class BackgroundTaskLog:

    TABLE_ID    = "`unidb-442214.UniDB.background_tasks_log`"
    DATABASE    = "UniDB"

    STATUS_IN_PROGRESS = "in_progress"
    STATUS_DONE        = "done"
    STATUS_FAILED      = "failed"

    def __init__(
        self,
        name:     str,
        req_args: TaskReqArgs | dict[str, Any] | None = None,
    ) -> None:
        self.task_id:     str                        = str(uuid.uuid4())
        self.name:        str                        = name
        self.status:      str                        = self.STATUS_IN_PROGRESS
        self.step:        str                        = "start"
        self.req_args:    dict[str, Any] | None      = self._resolve_req_args(req_args)
        self.result:      dict[str, Any] | None      = None
        self.error:       str | None                 = None
        self.created_at:  datetime                   = datetime.now(timezone.utc)
        self.finished_at: datetime | None            = None

        self._bq = BigQueryService(
            session_id=self.task_id,
            database_name=self.DATABASE,
        )
        self.create_new_task()

    # ------------------------------------------------------------------
    # Public methods
    # ------------------------------------------------------------------

    def create_new_task(self) -> None:
        """Insert the initial task row into BigQuery."""
        query = f"""
            INSERT INTO {self.TABLE_ID}
                (task_id, name, status, step, req_args, result, error, created_at, finished_at)
            VALUES
                (@task_id, @name, @status, @step, PARSE_JSON(@req_args), NULL, NULL, @created_at, NULL)
        """
        params = {
            "task_id":    self.task_id,
            "name":       self.name,
            "status":     self.status,
            "step":       self.step,
            "req_args":   self._to_json_str(self.req_args),
            "created_at": self.created_at.isoformat(),
        }
        self._execute(query, params)

    def update_task(
        self,
        status: str | None = None,
        step:   str | None = None,
    ) -> None:
        """Update status and/or step. Only provided fields are written."""
        clauses: list[str] = []
        params:  dict[str, Any] = {"task_id": self.task_id}

        if status is not None:
            self.status = status
            clauses.append("status = @status")
            params["status"] = status

        if step is not None:
            self.step = step
            clauses.append("step = @step")
            params["step"] = step

        if not clauses:
            return

        self._run_update(clauses, params)

    def end_task(
        self,
        result: TaskResult | dict[str, Any] | None = None,
        step:   str = "end",
    ) -> None:
        """Mark task as done, persist result and finished_at."""
        self.status      = self.STATUS_DONE
        self.step        = step
        self.finished_at = datetime.now(timezone.utc)
        self.result      = result.to_dict() if isinstance(result, TaskResult) else result

        clauses = [
            "status      = @status",
            "step        = @step",
            "finished_at = @finished_at",
            "result      = PARSE_JSON(@result)",
        ]
        params = {
            "task_id":     self.task_id,
            "status":      self.STATUS_DONE,
            "step":        step,
            "finished_at": self.finished_at.isoformat(),
            "result":      self._to_json_str(self.result),
        }
        self._run_update(clauses, params)

    def fail_task(
        self,
        error: str,
    ) -> None:
        """Mark task as failed with an error message."""
        self.status      = self.STATUS_FAILED
        self.error       = error
        self.finished_at = datetime.now(timezone.utc)

        clauses = [
            "status      = @status",
            "error       = @error",
            "finished_at = @finished_at",
        ]
        params: dict[str, Any] = {
            "task_id":     self.task_id,
            "status":      self.STATUS_FAILED,
            "error":       error,
            "finished_at": self.finished_at.isoformat(),
        }
        if self.step:
            clauses.append("step = @step")
            params["step"] = self.step

        self._run_update(clauses, params)

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _run_update(self, clauses: list[str], params: dict[str, Any]) -> None:
        set_clause = ", \n               ".join(clauses)
        query = f"""
            UPDATE {self.TABLE_ID}
            SET    {set_clause}
            WHERE  task_id = @task_id
        """
        self._execute(query, params)

    def _execute(self, query: str, params: dict[str, Any]) -> None:
        result = self._bq.execute_query(query, parameters=params)
        if not result.success:
            raise RuntimeError(
                f"[BackgroundTaskLog] BQ query failed — task_id={self.task_id} | {result.error_message}"
            )

    @staticmethod
    def _resolve_req_args(
        req_args: TaskReqArgs | dict[str, Any] | None,
    ) -> dict[str, Any] | None:
        if isinstance(req_args, TaskReqArgs):
            return req_args.to_dict()
        return req_args

    @staticmethod
    def _to_json_str(value: dict[str, Any] | None) -> str | None:
        if value is None:
            return None
        return json.dumps(value)