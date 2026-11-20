"""HTTP request/response models (Pydantic v2)."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

STATUS_LITERAL = Literal[
    "queued", "pending", "running", "completed", "expired"
]

# Job and node identifiers are opaque strings; restricting the alphabet keeps
# URLs and logs readable without forbidding useful separators.
IDENTIFIER_PATTERN = r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$"

MAX_DURATION_SECONDS = 365 * 24 * 60 * 60
MAX_PREP_TIMEOUT_SECONDS = 24 * 60 * 60


class NodeConfigRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    node_id: str = Field(pattern=IDENTIFIER_PATTERN)
    capacity: int = Field(ge=1, le=1_000_000)


class NodeView(BaseModel):
    node_id: str
    capacity: int
    used_slots: int
    free_slots: int
    running_jobs: int
    pending_jobs: int


class JobSubmitRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    job_id: str = Field(pattern=IDENTIFIER_PATTERN)
    required_slots: int = Field(ge=1, le=1_000_000)
    max_duration_seconds: float = Field(gt=0, le=MAX_DURATION_SECONDS)
    # When set, dispatch reserves the slots but the job stays ``pending``
    # until the node confirms within this many seconds.  When omitted, a
    # dispatched job starts running immediately.
    prep_timeout_seconds: float | None = Field(
        default=None, gt=0, le=MAX_PREP_TIMEOUT_SECONDS
    )


class JobView(BaseModel):
    job_id: str
    required_slots: int
    max_duration_seconds: float
    prep_timeout_seconds: float | None
    status: STATUS_LITERAL
    queue_position: int | None
    submitted_at: str
    assigned_node: str | None
    # Dispatch time: when the slots were reserved.
    started_at: str | None
    # Pending jobs only: by when the node must confirm the start.
    prep_deadline: str | None
    # Running jobs only: lease end (actual after confirmation).
    lease_expires_at: str | None
    confirmed_at: str | None
    ended_at: str | None
    # Pending: seconds left to confirm.  Running: seconds left on the lease.
    remaining_seconds: float | None


class HeadEstimate(BaseModel):
    head: JobView | None
    earliest_start_at: str | None
    earliest_start_in_seconds: float | None
    blocked_reason: Literal[
        "waiting_for_free_slots", "insufficient_capacity", "no_nodes_configured"
    ] | None


class QueueView(HeadEstimate):
    jobs: list[JobView]


class SchedulerEvent(BaseModel):
    type: Literal["dispatched", "confirmed", "expired", "released"]
    job_id: str
    node_id: str | None
    slots: int
    at: str | None
    lease_expires_at: str | None


class TickResponse(BaseModel):
    events: list[SchedulerEvent]


class ErrorResponse(BaseModel):
    detail: str
