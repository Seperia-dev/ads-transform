from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any


@dataclass
class TaskReqArgs:
    """Typed wrapper for task request arguments passed to BackgroundTaskLog."""
    data: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return self.data


@dataclass
class TaskResult:
    """Typed wrapper for task result payload passed to end_task()."""
    success: bool
    data: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {"success": self.success, "data": self.data}


@dataclass
class TaskRow:
    """Represents a full background_tasks_log row as returned from BigQuery."""
    task_id:     str
    name:        str
    status:      str
    step:        str | None
    req_args:    dict[str, Any] | None
    result:      dict[str, Any] | None
    error:       str | None
    created_at:  datetime | None
    finished_at: datetime | None