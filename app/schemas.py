"""Pydantic request/response models."""
from __future__ import annotations

import math
import re
from datetime import datetime, timezone
from typing import Literal, Optional

from pydantic import BaseModel, Field, field_validator, model_validator


# Epsilon caps may never exceed this registered ceiling; it also bounds the
# destination dataset's cap after an incoming budget transfer.
MAX_EPSILON_CAP = 1_000.0

# Mirrors the {dataset_id} path constraint in main.py.
_DATASET_ID_PATTERN = re.compile(r"^[A-Za-z0-9._\-]+$")


def _finite(name: str):
    @field_validator(name)
    @classmethod
    def _check(cls, v: float) -> float:
        if not math.isfinite(v):
            raise ValueError(f"{name} must be a finite number")
        return v

    return _check


class DatasetRegister(BaseModel):
    epsilon_cap: float = Field(
        ...,
        gt=0.0,
        le=MAX_EPSILON_CAP,
        examples=[1.0],
        description="Total epsilon budget allowed for this dataset.",
    )
    delta_cap: float = Field(
        ...,
        ge=1e-20,
        lt=1.0,
        examples=[1e-6],
        description="Total delta budget; also the target delta used when the "
        "composed RDP curve is converted back to (epsilon, delta)-DP.",
    )
    description: Optional[str] = Field(default=None, max_length=2000)

    _eps_finite = _finite("epsilon_cap")
    _delta_finite = _finite("delta_cap")


class AnalysisRequest(BaseModel):
    request_id: str = Field(
        ...,
        min_length=1,
        max_length=200,
        examples=["req-20260923-0001"],
        description="Client-generated idempotency key; unique per dataset.",
    )
    query_type: str = Field(
        ...,
        min_length=1,
        max_length=100,
        examples=["gaussian_mean"],
        description="Free-form label of the analysis, e.g. "
        "'gaussian_mean', 'histogram', 'sql_count'.",
    )
    epsilon: float = Field(..., gt=0.0, le=1_000.0, examples=[0.1])
    delta: float = Field(
        ...,
        ge=0.0,
        lt=1.0,
        examples=[1e-7],
        description="Per-request delta; use 0 for pure epsilon-DP.",
    )

    _eps_finite = _finite("epsilon")
    _delta_finite = _finite("delta")


# An atomic batch carries between 2 and 20 analysis items.
MIN_BATCH_ITEMS = 2
MAX_BATCH_ITEMS = 20


class BatchAnalysisItem(BaseModel):
    """One analysis item inside an atomic batch submission."""

    request_id: str = Field(
        ...,
        min_length=1,
        max_length=200,
        examples=["req-20260924-0001"],
        description="Client-generated idempotency key; unique per dataset and "
        "unique within the batch.",
    )
    query_type: str = Field(
        ...,
        min_length=1,
        max_length=100,
        examples=["gaussian_mean"],
        description="Free-form label of the analysis.",
    )
    epsilon: float = Field(..., gt=0.0, le=1_000.0, examples=[0.1])
    delta: float = Field(
        ...,
        ge=0.0,
        lt=1.0,
        examples=[1e-7],
        description="Per-item delta; use 0 for pure epsilon-DP.",
    )

    _eps_finite = _finite("epsilon")
    _delta_finite = _finite("delta")


class BatchAnalysisRequest(BaseModel):
    """An all-or-nothing batch of 2-20 analysis items under one batch_id."""

    batch_id: str = Field(
        ...,
        min_length=1,
        max_length=200,
        examples=["batch-20260924-0001"],
        description="Client-generated idempotency key; unique per dataset.",
    )
    items: list[BatchAnalysisItem] = Field(
        ...,
        min_length=MIN_BATCH_ITEMS,
        max_length=MAX_BATCH_ITEMS,
    )

    @field_validator("batch_id")
    @classmethod
    def _valid_batch_id(cls, v: str) -> str:
        if not _DATASET_ID_PATTERN.fullmatch(v):
            raise ValueError(
                "may contain only letters, digits, '.', '_' and '-'"
            )
        return v


# Longest accepted reservation time-to-live: 90 days.  Prevents a typo from
# locking budget essentially forever; a confirmed reservation consumes the
# budget permanently regardless of the TTL.
MAX_TTL_SECONDS = 90 * 24 * 3600


class ReservationCreate(BaseModel):
    reservation_id: str = Field(
        ...,
        min_length=1,
        max_length=200,
        examples=["res-20260923-0001"],
        description="Client-generated idempotency key; unique per dataset.",
    )
    epsilon: float = Field(..., gt=0.0, le=1_000.0, examples=[0.1])
    delta: float = Field(
        ...,
        ge=0.0,
        lt=1.0,
        examples=[1e-7],
        description="Reserved per-analysis delta; use 0 for pure epsilon-DP.",
    )
    ttl_seconds: int = Field(
        ...,
        ge=1,
        le=MAX_TTL_SECONDS,
        examples=[600],
        description="Reservation lifetime in seconds.  An unconfirmed "
        "reservation releases its budget automatically once it has expired; "
        "replays return the stored TTL.",
    )

    _eps_finite = _finite("epsilon")
    _delta_finite = _finite("delta")


class ReservationConfirm(BaseModel):
    request_id: str = Field(
        ...,
        min_length=1,
        max_length=200,
        examples=["req-20260923-0001"],
        description="Idempotency key of the analysis request the reservation "
        "is turned into; unique per dataset.",
    )
    query_type: str = Field(
        ...,
        min_length=1,
        max_length=100,
        examples=["gaussian_mean"],
        description="Free-form analysis label recorded on the analysis "
        "request.",
    )


class ReservationRenewalCreate(BaseModel):
    """Extend a granted, non-expired reservation's expiry instant.

    The reserved ``epsilon`` / ``delta`` and the original ``ttl_seconds`` stay
    unchanged and no extra budget is occupied; only ``expires_at`` moves
    forward by ``extend_seconds``.
    """

    renewal_id: str = Field(
        ...,
        min_length=1,
        max_length=200,
        examples=["ren-20260925-0001"],
        description="Client-generated idempotency key; unique per dataset.",
    )
    extend_seconds: int = Field(
        ...,
        ge=1,
        le=MAX_TTL_SECONDS,
        examples=[600],
        description="Positive number of seconds appended to the current "
        "expiry instant.  The resulting expiry may never be more than 90 "
        "days after the reservation's created_at.",
    )

    @field_validator("renewal_id")
    @classmethod
    def _valid_id(cls, v: str) -> str:
        if not _DATASET_ID_PATTERN.fullmatch(v):
            raise ValueError(
                "may contain only letters, digits, '.', '_' and '-'"
            )
        return v


class BudgetView(BaseModel):
    epsilon_cap: float
    delta_cap: float
    # Consumed by confirmed / directly granted analysis requests.
    epsilon_consumed: float
    # Additional epsilon projected to be locked by active (non-expired)
    # reservations, counted on the same composed RDP curve.
    epsilon_reserved: float
    # cap - (consumed RDP-projected together with active reservations).
    epsilon_remaining: float
    rdp_best_order: float
    granted_requests: int
    active_reservations: int
    # Composed RDP curve of granted requests *and* active reservations.
    rdp_curve: dict[str, float]


class DatasetInfo(BaseModel):
    dataset_id: str
    description: Optional[str]
    created_at: datetime
    budget: BudgetView


class Verdict(BaseModel):
    """Result of an analysis submission.

    A refusal is returned with HTTP 200 and ``allowed = false``: the request
    was processed correctly, it simply does not fit in the remaining budget.
    """

    dataset_id: str
    request_id: str
    query_type: str
    epsilon: float
    delta: float
    allowed: bool
    status: Literal["granted", "rejected"]
    reason: Optional[str] = None
    epsilon_before: float
    epsilon_after: float
    epsilon_remaining: float
    rdp_best_order: float
    idempotent_replay: bool = False
    created_at: datetime


class RequestRecord(BaseModel):
    dataset_id: str
    request_id: str
    query_type: str
    epsilon: float
    delta: float
    status: Literal["granted", "rejected"]
    reason: Optional[str]
    epsilon_before: float
    epsilon_after: float
    created_at: datetime
    # Set when this granted request has been reversed; the original verdict
    # fields above are retained as the historical admission record.
    reversed: bool = False
    reversal_id: Optional[str] = None
    reversal_reason: Optional[str] = None
    reversed_at: Optional[datetime] = None


class RequestList(BaseModel):
    dataset_id: str
    count: int
    requests: list[RequestRecord]


class BatchItemVerdict(BaseModel):
    """One item of an atomic batch verdict.

    On a granted batch every item is an ordinary granted analysis request and
    the epsilon snapshots are the cumulative composed values up to that item
    (in submission order).  On a rejected batch no item occupies anything: the
    snapshots are ``None`` and each item mirrors the batch-level reason.
    """

    request_id: str
    query_type: str
    epsilon: float
    delta: float
    allowed: bool
    status: Literal["granted", "rejected"]
    reason: Optional[str] = None
    epsilon_before: Optional[float]
    epsilon_after: Optional[float]
    epsilon_remaining: Optional[float]
    rdp_best_order: Optional[float]
    created_at: datetime


class BatchVerdict(BaseModel):
    """Result of an all-or-nothing atomic analysis batch.

    Either every item was admitted (``allowed = true``, one granted request
    row per item) or none was (``allowed = false``, no request rows and no
    budget consumed).  A processed verdict is returned with HTTP 201; replaying
    the same ``(dataset_id, batch_id)`` with the identical item payload returns
    HTTP 200 with ``idempotent_replay = true`` and the stored verdict.
    """

    dataset_id: str
    batch_id: str
    allowed: bool
    status: Literal["granted", "rejected"]
    reason: Optional[str] = None
    item_count: int
    epsilon_cap: float
    epsilon_before: float
    epsilon_after: float
    epsilon_remaining: float
    rdp_best_order: float
    idempotent_replay: bool = False
    created_at: datetime
    items: list[BatchItemVerdict]


# -------------------------------------------------------------- reversals --


class ReversalCreate(BaseModel):
    """Reverse a previously granted analysis request.

    The original request record is retained unchanged; a separate immutable
    reversal record moves the request out of the live RDP composition, freeing
    its budget.
    """

    reversal_id: str = Field(
        ...,
        min_length=1,
        max_length=200,
        examples=["rev-20260924-0001"],
        description="Client-generated idempotency key; globally unique.",
    )
    reason: str = Field(
        ...,
        min_length=1,
        max_length=2000,
        examples=["mistakenly submitted analysis"],
        description="Why the granted request is being reversed; stored "
        "immutably on the reversal record.",
    )

    @field_validator("reversal_id")
    @classmethod
    def _valid_id(cls, v: str) -> str:
        if not _DATASET_ID_PATTERN.fullmatch(v):
            raise ValueError(
                "may contain only letters, digits, '.', '_' and '-'"
            )
        return v


class ReversalView(BaseModel):
    """Immutable record of one granted-request reversal.

    ``epsilon_before`` / ``epsilon_after`` are the composed epsilon snapshots
    at reversal time (target request still included vs. removed); the original
    request's own verdict fields are never rewritten.  Replaying the same
    ``reversal_id`` returns this stored record with
    ``idempotent_replay = true`` without reversing again.
    """

    reversal_id: str
    dataset_id: str
    request_id: str
    reason: str
    epsilon: float
    delta: float
    epsilon_cap: float
    epsilon_before: float
    epsilon_after: float
    epsilon_remaining: float
    rdp_best_order: float
    idempotent_replay: bool = False
    created_at: datetime


class ReversalList(BaseModel):
    dataset_id: str
    count: int
    reversals: list[ReversalView]


ReservationStatus = Literal[
    "granted", "rejected", "confirmed", "released", "expired"
]


class ReservationView(BaseModel):
    """One reservation with its current lifecycle state.

    ``allowed`` is the immutable admission decision taken at creation;
    ``status`` is the live state (``granted`` reservations turn into
    ``confirmed``, ``released`` or ``expired`` over time).
    """

    dataset_id: str
    reservation_id: str
    epsilon: float
    delta: float
    ttl_seconds: int
    allowed: bool
    status: ReservationStatus
    reason: Optional[str] = None
    epsilon_before: float
    epsilon_after: float
    epsilon_remaining: float
    rdp_best_order: float
    idempotent_replay: bool = False
    created_at: datetime
    expires_at: Optional[datetime] = None
    expired_at: Optional[datetime] = None
    confirmed_at: Optional[datetime] = None
    confirmed_request_id: Optional[str] = None
    released_at: Optional[datetime] = None


class ReservationList(BaseModel):
    dataset_id: str
    count: int
    reservations: list[ReservationView]


class ReservationRenewalView(BaseModel):
    """Immutable record of one reservation renewal.

    ``expires_at_before`` / ``expires_at_after`` are the reservation's expiry
    instants just before and just after the extension.  Replaying the same
    ``(dataset_id, renewal_id)`` with the same payload returns this stored
    record with ``idempotent_replay = true`` without extending again.
    """

    renewal_id: str
    dataset_id: str
    reservation_id: str
    extend_seconds: int
    expires_at_before: datetime
    expires_at_after: datetime
    idempotent_replay: bool = False
    created_at: datetime


class ReservationAdjustmentCreate(BaseModel):
    """Replace a granted, non-expired reservation's epsilon/delta quota.

    The reserved ``ttl_seconds`` and expiry instant are left untouched; only
    the live ``epsilon`` / ``delta`` accounting values move, atomically under
    the dataset cap.
    """

    adjustment_id: str = Field(
        ...,
        min_length=1,
        max_length=200,
        examples=["adj-20260925-0001"],
        description="Client-generated idempotency key; unique per dataset.",
    )
    epsilon: float = Field(..., gt=0.0, le=1_000.0, examples=[0.1])
    delta: float = Field(
        ...,
        ge=0.0,
        lt=1.0,
        examples=[1e-7],
        description="New reserved delta; use 0 for pure epsilon-DP.",
    )

    _eps_finite = _finite("epsilon")
    _delta_finite = _finite("delta")

    @field_validator("adjustment_id")
    @classmethod
    def _valid_id(cls, v: str) -> str:
        if not _DATASET_ID_PATTERN.fullmatch(v):
            raise ValueError(
                "may contain only letters, digits, '.', '_' and '-'"
            )
        return v


class ReservationAdjustmentView(BaseModel):
    """Immutable record of one reservation quota adjustment.

    A refused adjustment is returned with HTTP 201 and ``allowed = false``:
    the attempt was processed atomically and recorded, but the reservation's
    quota stayed unchanged (the ``*_after`` quota fields then equal the
    ``*_before`` ones).  Replaying the same ``(dataset_id, adjustment_id)``
    with the same payload returns this stored record with
    ``idempotent_replay = true`` without adjusting again.
    """

    adjustment_id: str
    dataset_id: str
    reservation_id: str
    # Live quota in effect just before the attempt.
    epsilon_before: float
    delta_before: float
    # Requested new quota.
    requested_epsilon: float
    requested_delta: float
    # Live quota in effect just after the attempt (unchanged on rejection).
    epsilon_after: float
    delta_after: float
    allowed: bool
    status: Literal["adjusted", "rejected"]
    reason: Optional[str] = None
    epsilon_cap: float
    # Composed epsilon occupied by granted requests + non-expired
    # reservations before the attempt and with the new quota in place.
    epsilon_committed_before: float
    epsilon_committed_after: float
    epsilon_remaining: float
    rdp_best_order: float
    idempotent_replay: bool = False
    created_at: datetime


class ForecastSnapshot(BaseModel):
    """Budget state for one set of still-valid reservations.

    All epsilon values use the same composed-RDP accounting as
    :class:`BudgetView`: granted requests are composed together with the
    reservations that are still locked at that instant.
    """

    epsilon_consumed: float
    epsilon_reserved: float
    epsilon_remaining: float
    rdp_best_order: float
    active_reservations: int


class ReleaseForecastEvent(ForecastSnapshot):
    """One simulated expiry instant.

    All reservations sharing the same ``expires_at`` are removed together at
    this instant; the budget fields describe the state *after* their removal.
    """

    expires_at: datetime
    released_count: int
    released_reservation_ids: list[str]


class ReleaseForecast(BaseModel):
    """Read-only simulation of TTL-driven budget releases up to a deadline."""

    dataset_id: str
    generated_at: datetime
    deadline: datetime
    # State computed from reservations that are currently valid (now), before
    # any simulated expiry.
    initial: ForecastSnapshot
    # One entry per distinct expires_at <= deadline, chronological.  Reservations
    # still valid at the deadline never appear here.
    events: list[ReleaseForecastEvent]
    # State with every expires_at <= deadline removed; reservations expiring
    # strictly after the deadline (or never) keep counting.
    at_deadline: ForecastSnapshot


# ------------------------------------------------------------- transfers ---


class BudgetTransferRequest(BaseModel):
    """Move epsilon cap between two datasets.

    Only ``epsilon_cap`` is moved; delta caps and all already-recorded
    analysis requests / reservations are left untouched.
    """

    transfer_id: str = Field(
        ...,
        min_length=1,
        max_length=200,
        examples=["xfer-20260923-0001"],
        description="Client-generated idempotency key; globally unique.",
    )
    source_dataset_id: str = Field(
        ...,
        min_length=1,
        max_length=200,
        examples=["sales-2025"],
        description="Dataset whose epsilon cap is debited.",
    )
    target_dataset_id: str = Field(
        ...,
        min_length=1,
        max_length=200,
        examples=["sales-2026"],
        description="Dataset whose epsilon cap is credited.",
    )
    epsilon_amount: float = Field(
        ...,
        gt=0.0,
        le=MAX_EPSILON_CAP,
        examples=[0.25],
        description="Positive epsilon cap amount transferred source -> target.",
    )

    _eps_finite = _finite("epsilon_amount")

    @field_validator("transfer_id", "source_dataset_id", "target_dataset_id")
    @classmethod
    def _valid_id(cls, v: str) -> str:
        if not _DATASET_ID_PATTERN.fullmatch(v):
            raise ValueError(
                "may contain only letters, digits, '.', '_' and '-'"
            )
        return v

    @model_validator(mode="after")
    def _distinct_endpoints(self) -> "BudgetTransferRequest":
        if self.source_dataset_id == self.target_dataset_id:
            raise ValueError(
                "source_dataset_id and target_dataset_id must differ"
            )
        return self


class BudgetTransferSide(BaseModel):
    """One endpoint's cap/occupancy around a transfer."""

    dataset_id: str
    epsilon_cap: float
    # Composed epsilon occupied by granted requests + non-expired
    # reservations, evaluated under the existing RDP rules.
    epsilon_committed: float
    epsilon_remaining: float


class BudgetTransferView(BaseModel):
    """Result of a budget-transfer attempt.

    A refused transfer is returned with HTTP 201 and ``allowed = false``:
    the attempt was processed atomically and recorded, but neither cap
    changed.  Replaying the same ``transfer_id`` returns the stored record
    (``idempotent_replay = true``) without transferring again.
    """

    transfer_id: str
    source_dataset_id: str
    target_dataset_id: str
    epsilon_amount: float
    allowed: bool
    status: Literal["transferred", "rejected"]
    reason: Optional[str] = None
    source: BudgetTransferSide
    target: BudgetTransferSide
    idempotent_replay: bool = False
    created_at: datetime


# ----------------------------------------------------------- selection plans --


# A read-only selection trial enumerates every subset, so the candidate count
# is capped tightly (2**12 = 4096 subsets at most).
MIN_SELECTION_CANDIDATES = 1
MAX_SELECTION_CANDIDATES = 12


class SelectionPlanCandidate(BaseModel):
    """One candidate analysis item for a read-only selection trial."""

    candidate_id: str = Field(
        ...,
        min_length=1,
        max_length=200,
        examples=["cand-20260924-0001"],
        description="Client-chosen identifier; unique within one request.",
    )
    epsilon: float = Field(..., gt=0.0, le=1_000.0, examples=[0.1])
    delta: float = Field(
        ...,
        ge=0.0,
        lt=1.0,
        examples=[1e-7],
        description="Candidate delta; use 0 for pure epsilon-DP.",
    )
    value: int = Field(
        ...,
        ge=1,
        examples=[10],
        description="Positive integer payoff maximised by the chosen subset.",
    )

    _eps_finite = _finite("epsilon")
    _delta_finite = _finite("delta")


class SelectionPlanRequest(BaseModel):
    """1-12 candidate items for a read-only feasible-subset trial."""

    candidates: list[SelectionPlanCandidate] = Field(
        ...,
        min_length=MIN_SELECTION_CANDIDATES,
        max_length=MAX_SELECTION_CANDIDATES,
    )

    @model_validator(mode="after")
    def _unique_candidate_ids(self) -> "SelectionPlanRequest":
        seen: set[str] = set()
        for candidate in self.candidates:
            if candidate.candidate_id in seen:
                raise ValueError(
                    f"candidate_id {candidate.candidate_id!r} occurs more than "
                    "once; candidate_id must be unique within the request"
                )
            seen.add(candidate.candidate_id)
        return self


class SelectionPlanView(BaseModel):
    """Result of a read-only best-subset selection trial.

    Nothing is persisted: no analysis request, reservation or verdict record
    is ever created, and every call recomputes against the current ledger
    (granted non-reversed requests plus non-expired granted reservations).
    When no non-empty subset fits, the selection is empty and the occupancy
    fields simply describe the current ledger.
    """

    dataset_id: str
    generated_at: datetime
    candidate_count: int
    # Chosen / skipped candidates, each sorted by candidate_id ascending.
    selected_candidate_ids: list[str]
    unselected_candidate_ids: list[str]
    # Sum of the chosen candidates' values.
    total_value: int
    epsilon_cap: float
    # Composed epsilon currently occupied before any candidate is added.
    epsilon_committed: float
    # Composed epsilon projected for committed entries + the chosen subset.
    projected_epsilon: float
    # cap - projected_epsilon, floored at zero like the live budget views.
    epsilon_remaining: float
    rdp_best_order: float


# -------------------------------------------------------- cost attributions --


# A read-only Shapley attribution enumerates every subset of the group, so the
# selected request count is capped tightly (2**10 = 1024 subsets at most).
MIN_ATTRIBUTION_REQUESTS = 1
MAX_ATTRIBUTION_REQUESTS = 10


class CostAttributionRequest(BaseModel):
    """1-10 unique granted, non-reversed requests to attribute as one group."""

    request_ids: list[str] = Field(
        ...,
        min_length=MIN_ATTRIBUTION_REQUESTS,
        max_length=MAX_ATTRIBUTION_REQUESTS,
        examples=[["req-20260924-0001", "req-20260924-0002"]],
        description="1-10 client-chosen, mutually distinct request_ids.  Each "
        "must identify a granted, not-yet-reversed request of this dataset.",
    )

    @field_validator("request_ids")
    @classmethod
    def _valid_request_ids(cls, v: list[str]) -> list[str]:
        for request_id in v:
            if not 1 <= len(request_id) <= 200:
                raise ValueError(
                    "each request_id must be a string of 1-200 characters"
                )
        return v

    @model_validator(mode="after")
    def _unique_request_ids(self) -> "CostAttributionRequest":
        seen: set[str] = set()
        for request_id in self.request_ids:
            if request_id in seen:
                raise ValueError(
                    f"request_id {request_id!r} occurs more than once; "
                    "request_id must be unique within the request"
                )
            seen.add(request_id)
        return self


class CostAttributionItem(BaseModel):
    """One selected request with its Shapley share of the group increment."""

    request_id: str
    epsilon: float
    delta: float
    # Standard Shapley-weighted average of this request's marginal epsilon
    # increments over the coalitions of the other selected requests.
    attributed_epsilon: float


class CostAttributionView(BaseModel):
    """Read-only Shapley attribution of one group of granted requests.

    Nothing is persisted and no reservation is swept: every call recomputes
    against the live ledger.  The baseline occupancy composes every *other*
    granted, non-reversed request together with the currently non-expired
    granted reservations; the full-group occupancy additionally composes every
    selected request.  Each selected request is charged its standard Shapley
    share of the resulting epsilon increment, so the per-item shares sum (up to
    floating-point error) to ``epsilon_full_group - epsilon_baseline``.
    """

    dataset_id: str
    generated_at: datetime
    request_count: int
    # Selected ids, echoed in the request's supplied order.
    request_ids: list[str]
    epsilon_cap: float
    # Occupancy with every *other* non-reversed granted request and the
    # currently non-expired granted reservations, but without the group.
    epsilon_baseline: float
    # Occupancy once every selected request is additionally composed in.
    epsilon_full_group: float
    # epsilon_full_group - epsilon_baseline (the attributed group increment).
    epsilon_increment: float
    rdp_best_order_baseline: float
    rdp_best_order_full_group: float
    # Sum of the per-item attributed epsilon values.
    attributed_total_epsilon: float
    attributions: list[CostAttributionItem]


# -------------------------------------------------------- repeat-run trials --


# A read-only repeat-run trial treats a fixed group of 1-5 analysis items as
# one workload "run" and asks how many complete runs fit; repetitions can be
# numerous (up to 100_000), so feasibility is binary-searched over run counts
# instead of re-composing the ledger run by run.
MIN_REPEAT_RUN_ITEMS = 1
MAX_REPEAT_RUN_ITEMS = 5
MIN_REPEAT_RUN_MAX_RUNS = 1
MAX_REPEAT_RUN_MAX_RUNS = 100_000


class RepeatRunWorkloadItem(BaseModel):
    """One analysis item inside a repeated workload group."""

    epsilon: float = Field(..., gt=0.0, le=1_000.0, examples=[0.1])
    delta: float = Field(
        ...,
        ge=0.0,
        lt=1.0,
        examples=[1e-7],
        description="Item delta; use 0 for pure epsilon-DP.",
    )

    _eps_finite = _finite("epsilon")
    _delta_finite = _finite("delta")


class RepeatRunTrialRequest(BaseModel):
    """A workload group of 1-5 items repeated up to ``max_runs`` times."""

    items: list[RepeatRunWorkloadItem] = Field(
        ...,
        min_length=MIN_REPEAT_RUN_ITEMS,
        max_length=MAX_REPEAT_RUN_ITEMS,
    )
    max_runs: int = Field(
        ...,
        ge=MIN_REPEAT_RUN_MAX_RUNS,
        le=MAX_REPEAT_RUN_MAX_RUNS,
        examples=[1000],
        description="Upper bound on the number of complete runs to consider "
        "(1..100000).  The trial returns the largest run count within this "
        "bound whose projected epsilon stays within the cap.",
    )


class RepeatRunProjection(BaseModel):
    """Projected occupancy after ``runs`` complete runs on top of the ledger.

    ``feasible`` uses exactly the same comparison as live admission: projected
    epsilon ``<= epsilon_cap + EPSILON_TOLERANCE``.
    """

    runs: int
    epsilon_projected: float
    epsilon_remaining: float
    rdp_best_order: float
    feasible: bool


class RepeatRunTrialView(BaseModel):
    """Result of a read-only repeated-workload capacity trial.

    Nothing is persisted: no analysis request, reservation or verdict record
    is ever created and reservation expiry is never swept; every call
    recomputes against the current ledger (granted non-reversed requests plus
    non-expired granted reservations).  The whole item group is composed once
    per run with the existing RDP rules (pure epsilon stays exactly additive)
    and repeated; ``max_complete_runs`` is the largest count within
    ``max_runs`` whose projection fits -- it may be 0, in which case even one
    run does not fit and ``next_run_projection`` reports the over-cap
    occupancy of a single run.  When ``max_complete_runs == max_runs`` the
    supplied bound is reached and ``next_run_projection`` is null (no run
    beyond the bound is evaluated).
    """

    dataset_id: str
    generated_at: datetime
    item_count: int
    # Submitted items echoed back in their request order.
    items: list[RepeatRunWorkloadItem]
    max_runs: int
    epsilon_cap: float
    # Composed epsilon of the current ledger before any simulated run.
    epsilon_committed: float
    # Largest run count in [0, max_runs] whose projection fits the cap.
    max_complete_runs: int
    # True iff max_complete_runs == max_runs (the supplied bound was reached).
    at_max_runs: bool
    # Projection at exactly max_complete_runs runs (always feasible).
    projection: RepeatRunProjection
    # Projection at max_complete_runs + 1 runs; null once max_runs is reached.
    next_run_projection: Optional[RepeatRunProjection] = None


# ---------------------------------------------------------- reversal plans --


# A read-only reversal-plan trial enumerates every reversal subset of the
# candidate requests, so the candidate count is capped tightly (2**12 = 4096
# subsets at most).
MIN_REVERSAL_PLAN_CANDIDATES = 1
MAX_REVERSAL_PLAN_CANDIDATES = 12


class ReversalPlanItem(BaseModel):
    """The pending (not yet submitted) analysis item a reversal plan trials."""

    epsilon: float = Field(..., gt=0.0, le=1_000.0, examples=[0.1])
    delta: float = Field(
        ...,
        ge=0.0,
        lt=1.0,
        examples=[1e-7],
        description="Item delta; use 0 for pure epsilon-DP.",
    )

    _eps_finite = _finite("epsilon")
    _delta_finite = _finite("delta")


class ReversalPlanRequest(BaseModel):
    """One pending analysis item plus 1-12 distinct reversal candidates."""

    item: ReversalPlanItem = Field(
        ...,
        description="Pending analysis item (epsilon, delta) trialled on top "
        "of each candidate reversal subset; it is never submitted.",
    )
    request_ids: list[str] = Field(
        ...,
        min_length=MIN_REVERSAL_PLAN_CANDIDATES,
        max_length=MAX_REVERSAL_PLAN_CANDIDATES,
        examples=[["req-20260924-0001", "req-20260924-0002"]],
        description="1-12 client-chosen, mutually distinct request_ids that "
        "may be reversed to make room.  Each must identify a granted, "
        "not-yet-reversed request of this dataset.",
    )

    @field_validator("request_ids")
    @classmethod
    def _valid_request_ids(cls, v: list[str]) -> list[str]:
        for request_id in v:
            if not 1 <= len(request_id) <= 200:
                raise ValueError(
                    "each request_id must be a string of 1-200 characters"
                )
        return v

    @model_validator(mode="after")
    def _unique_request_ids(self) -> "ReversalPlanRequest":
        seen: set[str] = set()
        for request_id in self.request_ids:
            if request_id in seen:
                raise ValueError(
                    f"request_id {request_id!r} occurs more than once; "
                    "request_id must be unique within the request"
                )
            seen.add(request_id)
        return self


class ReversalPlanView(BaseModel):
    """Result of a read-only reversal-plan trial.

    Nothing is persisted and no reservation is swept: every call recomputes
    against the live ledger.  ``epsilon_committed`` is the occupancy before
    any reversal (all candidates still granted); ``epsilon_after_reversal``
    is the ledger occupancy once the suggested requests are reversed;
    ``epsilon_with_pending`` additionally composes the pending item on top.
    When no reversal subset lets the pending item fit, ``feasible`` is false
    and the plan-specific fields are null.
    """

    dataset_id: str
    generated_at: datetime
    # Pending analysis item echoed back as submitted.
    item: ReversalPlanItem
    candidate_count: int
    # Candidate ids, echoed in the request's supplied order.
    candidate_request_ids: list[str]
    epsilon_cap: float
    # Occupancy before any reversal: every other non-reversed granted
    # request plus the currently non-expired reservations plus every
    # candidate (all still granted), i.e. the live ledger occupancy.
    epsilon_committed: float
    rdp_best_order_committed: float
    feasible: bool
    # Human-readable explanation when no reversal subset fits; null otherwise.
    reason: Optional[str] = None
    # Suggested requests to reverse, sorted ascending; empty when infeasible
    # or when the pending item already fits without any reversal.
    suggested_request_ids: list[str]
    reversal_count: Optional[int] = None
    # Ledger occupancy once the suggested requests are reversed.
    epsilon_after_reversal: Optional[float] = None
    # Occupancy once the pending item is additionally composed on top.
    epsilon_with_pending: Optional[float] = None
    # epsilon_cap - epsilon_with_pending, floored at zero.
    epsilon_remaining: Optional[float] = None
    rdp_best_order_after_reversal: Optional[float] = None
    rdp_best_order_with_pending: Optional[float] = None


def utcnow() -> datetime:
    return datetime.now(timezone.utc)
