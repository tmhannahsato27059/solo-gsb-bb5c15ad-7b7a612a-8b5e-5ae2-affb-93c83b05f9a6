"""FastAPI application: differential-privacy budget ledger."""
from __future__ import annotations

from typing import Annotated

from contextlib import asynccontextmanager
from datetime import datetime

from fastapi import FastAPI, Path, Query, Request, status
from fastapi.responses import JSONResponse

from . import db, ledger
from .schemas import (
    AnalysisRequest,
    BatchAnalysisRequest,
    BatchVerdict,
    BudgetTransferRequest,
    BudgetTransferView,
    CostAttributionRequest,
    CostAttributionView,
    DatasetInfo,
    DatasetRegister,
    ReleaseForecast,
    RequestList,
    ReversalCreate,
    ReversalList,
    ReversalPlanRequest,
    ReversalPlanView,
    ReversalView,
    ReservationAdjustmentCreate,
    ReservationAdjustmentView,
    ReservationConfirm,
    ReservationCreate,
    ReservationList,
    ReservationRenewalCreate,
    ReservationRenewalView,
    ReservationView,
    RepeatRunTrialRequest,
    RepeatRunTrialView,
    SelectionPlanRequest,
    SelectionPlanView,
    Verdict,
    BudgetView,
)

DATASET_ID = Annotated[
    str,
    Path(
        min_length=1,
        max_length=200,
        pattern=r"^[A-Za-z0-9._\-]+$",
        description="Client-chosen dataset identifier.",
    ),
]
RESERVATION_ID = Annotated[
    str,
    Path(
        min_length=1,
        max_length=200,
        pattern=r"^[A-Za-z0-9._\-]+$",
        description="Client-chosen reservation identifier.",
    ),
]
REQUEST_ID = Annotated[
    str,
    Path(
        min_length=1,
        max_length=200,
        pattern=r"^[A-Za-z0-9._\-]+$",
        description="Client-chosen analysis request identifier.",
    ),
]
REVERSAL_ID = Annotated[
    str,
    Path(
        min_length=1,
        max_length=200,
        pattern=r"^[A-Za-z0-9._\-]+$",
        description="Client-chosen reversal identifier.",
    ),
]
BATCH_ID = Annotated[
    str,
    Path(
        min_length=1,
        max_length=200,
        pattern=r"^[A-Za-z0-9._\-]+$",
        description="Client-chosen atomic analysis-batch identifier.",
    ),
]


@asynccontextmanager
async def lifespan(_: FastAPI):
    await db.init_db()
    yield


app = FastAPI(
    title="Differential-Privacy Budget Ledger",
    version="1.11.0",
    lifespan=lifespan,
    description=(
        "Register epsilon/delta caps per dataset and submit analysis "
        "requests.  Composition is computed on a grid of Rényi-DP orders and "
        "converted back to (epsilon, delta)-DP.  Repeated request_ids are "
        "idempotent; over-budget submissions are atomically rejected under "
        "concurrency.  An all-or-nothing atomic batch endpoint additionally "
        "admits 2-20 analysis items under one batch_id in a single write "
        "transaction: the whole batch is granted only if its joint RDP "
        "composition fits, otherwise no item occupies any budget; batch_id is "
        "idempotent and results are queryable by batch_id.  Budget can "
        "additionally be reserved up-front: reservations lock capacity "
        "atomically (with TTL-based expiry), are idempotent per "
        "reservation_id, and are confirmed into analysis requests or released.  "
        "A granted, not-yet-expired reservation can also be renewed: a "
        "dataset-unique renewal_id and a positive extend_seconds move its "
        "expiry instant forward (never more than 90 days past creation) in "
        "one write transaction that stores an immutable renewal record, "
        "leaving the reserved amount and original ttl_seconds unchanged and "
        "occupying no extra budget; same-payload replays return the first "
        "renewal's before/after expiry instants.  A granted, not-yet-expired "
        "reservation can additionally have its reserved quota adjusted: a "
        "dataset-unique adjustment_id and new legal epsilon/delta values "
        "replace the old quota in one write transaction, recomposed under the "
        "same RDP rules with the non-reversed granted requests and every other "
        "valid reservation; an over-cap projection is rejected, recorded and "
        "leaves the quota unchanged, same-payload replays return the first "
        "verdict with its quota snapshots while a different payload is a 409, "
        "and confirmations, budget queries and release forecasts afterwards "
        "use the new quota (the original reservation-creation replay still "
        "matches the creation-time payload).  A read-only release "
        "forecast simulates future TTL expiries up to a "
        "caller-supplied UTC deadline without changing any reservation or "
        "database state.  Epsilon cap can also be transferred between two "
        "registered datasets in one atomic, idempotent transaction: the source "
        "must stay positive and cover its current occupancy, the target may "
        "never exceed the registered cap ceiling of 1000, and a rejected "
        "attempt leaves both caps unchanged.  A read-only selection-plan trial "
        "also accepts 1-12 candidate analyses (each with a unique candidate_id, "
        "epsilon, delta and positive integer value): it enumerates every "
        "feasible subset under the current RDP occupancy from non-reversed "
        "granted requests and non-expired reservations, returns the "
        "highest-value subset (ties broken by smaller projected epsilon, then "
        "ascending candidate-id lexicographic order) with the remaining budget, "
        "and never creates requests, reservations or verdict records -- "
        "repeated calls recompute against the latest ledger.  A read-only "
        "cost-attribution endpoint additionally accepts 1-10 distinct granted, "
        "non-reversed request_ids: it takes the other non-reversed granted "
        "requests plus the currently non-expired reservations as the baseline, "
        "projects every subset of the selected group with the same RDP "
        "accounting, and splits the group's epsilon increment between the "
        "selected requests by each one's marginal increment under standard "
        "Shapley weights, returning the baseline and full-group occupancy "
        "alongside the per-request shares and their exact-sum total; it "
        "creates no records and never sweeps reservations.  A read-only "
        "repeat-run trial additionally accepts 1-5 analysis items (each with "
        "epsilon and delta) together with a max_runs of 1-100000: the whole "
        "group is treated as one workload run repeated on top of the current "
        "non-reversed granted requests and non-expired reservations, and the "
        "endpoint returns the largest complete-run count within max_runs "
        "whose RDP-projected epsilon stays within the cap (zero is a valid "
        "answer), the projection at that count and one more run beyond it "
        "(omitted once max_runs is reached), with pure-epsilon items still "
        "composed by exact addition; it creates no requests, does not sweep "
        "reservations and recomputes against the latest ledger on every call.  "
        "A read-only reversal-plan trial additionally accepts one pending "
        "analysis item (epsilon, delta) together with 1-12 distinct granted, "
        "non-reversed request_ids: taking the other non-reversed granted "
        "requests plus the currently non-expired reservations as a fixed "
        "baseline, it enumerates every candidate reversal subset and projects "
        "the pending item with the same RDP accounting, then suggests a "
        "feasible subset that keeps the composed epsilon within the cap -- "
        "fewest reversals first, then the largest post-reversal ledger "
        "occupancy, then the lexicographically smallest ascending request_id "
        "list -- returning the suggested ids with the before-reversal, "
        "after-reversal and with-pending occupancies, or an explicit "
        "infeasible answer when no subset fits; it reverses nothing, submits "
        "nothing and never sweeps reservations.  Finally, a "
        "granted analysis request can be reversed with a globally unique "
        "reversal_id and reason: the original verdict and reservation history "
        "stay untouched while an immutable reversal record removes the request "
        "from live RDP composition, freeing its budget."
    ),
)


@app.exception_handler(db.DatasetNotFound)
async def _not_found(_: Request, exc: db.DatasetNotFound) -> JSONResponse:
    return JSONResponse(
        status_code=status.HTTP_404_NOT_FOUND,
        content={"error": "dataset_not_found", "dataset_id": exc.args[0]},
    )


@app.exception_handler(db.DatasetExists)
async def _exists(_: Request, exc: db.DatasetExists) -> JSONResponse:
    return JSONResponse(
        status_code=status.HTTP_409_CONFLICT,
        content={"error": "dataset_already_exists", "dataset_id": exc.args[0]},
    )


@app.exception_handler(db.RequestConflict)
async def _conflict(_: Request, exc: db.RequestConflict) -> JSONResponse:
    return JSONResponse(
        status_code=status.HTTP_409_CONFLICT,
        content={
            "error": "request_id_conflict",
            "request_id": exc.args[0],
            "detail": (
                "request_id was already used for a different request payload"
            ),
        },
    )


@app.exception_handler(db.ReservationNotFound)
async def _reservation_not_found(_: Request, exc: db.ReservationNotFound) -> JSONResponse:
    return JSONResponse(
        status_code=status.HTTP_404_NOT_FOUND,
        content={"error": "reservation_not_found", "reservation_id": exc.args[0]},
    )


@app.exception_handler(db.ReservationConflict)
async def _reservation_conflict(_: Request, exc: db.ReservationConflict) -> JSONResponse:
    return JSONResponse(
        status_code=status.HTTP_409_CONFLICT,
        content={
            "error": "reservation_id_conflict",
            "reservation_id": exc.args[0],
            "detail": (
                "reservation_id was already used for a different reservation "
                "payload (epsilon/delta/ttl_seconds)"
            ),
        },
    )


@app.exception_handler(db.ReservationStateConflict)
async def _reservation_state_conflict(
    _: Request, exc: db.ReservationStateConflict
) -> JSONResponse:
    return JSONResponse(
        status_code=status.HTTP_409_CONFLICT,
        content={
            "error": exc.args[0],
            "detail": exc.args[1],
        },
    )


@app.exception_handler(db.RenewalConflict)
async def _renewal_conflict(_: Request, exc: db.RenewalConflict) -> JSONResponse:
    return JSONResponse(
        status_code=status.HTTP_409_CONFLICT,
        content={
            "error": "renewal_id_conflict",
            "renewal_id": exc.args[0],
            "detail": (
                "renewal_id was already used for a different renewal payload "
                "(reservation or extend_seconds differs)"
            ),
        },
    )


@app.exception_handler(db.AdjustmentConflict)
async def _adjustment_conflict(
    _: Request, exc: db.AdjustmentConflict
) -> JSONResponse:
    return JSONResponse(
        status_code=status.HTTP_409_CONFLICT,
        content={
            "error": "adjustment_id_conflict",
            "adjustment_id": exc.args[0],
            "detail": (
                "adjustment_id was already used for a different adjustment "
                "payload (reservation, epsilon or delta differs)"
            ),
        },
    )


@app.exception_handler(ledger.InvalidForecastDeadline)
async def _invalid_forecast_deadline(
    _: Request, exc: ledger.InvalidForecastDeadline
) -> JSONResponse:
    return JSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        content={"error": "invalid_forecast_deadline", "detail": exc.args[0]},
    )


@app.exception_handler(db.TransferConflict)
async def _transfer_conflict(_: Request, exc: db.TransferConflict) -> JSONResponse:
    return JSONResponse(
        status_code=status.HTTP_409_CONFLICT,
        content={
            "error": "transfer_id_conflict",
            "transfer_id": exc.args[0],
            "detail": (
                "transfer_id was already used for a different transfer payload"
            ),
        },
    )


@app.exception_handler(db.RequestNotFound)
async def _request_not_found(_: Request, exc: db.RequestNotFound) -> JSONResponse:
    return JSONResponse(
        status_code=status.HTTP_404_NOT_FOUND,
        content={"error": "request_not_found", "request_id": exc.args[0]},
    )


@app.exception_handler(db.ReversalNotFound)
async def _reversal_not_found(_: Request, exc: db.ReversalNotFound) -> JSONResponse:
    return JSONResponse(
        status_code=status.HTTP_404_NOT_FOUND,
        content={"error": "reversal_not_found", "reversal_id": exc.args[0]},
    )


@app.exception_handler(db.ReversalConflict)
async def _reversal_conflict(_: Request, exc: db.ReversalConflict) -> JSONResponse:
    return JSONResponse(
        status_code=status.HTTP_409_CONFLICT,
        content={
            "error": "reversal_id_conflict",
            "reversal_id": exc.args[0],
            "detail": (
                "reversal_id was already used for a different reversal "
                "(target request or reason differs)"
            ),
        },
    )


@app.exception_handler(db.ReversalStateConflict)
async def _reversal_state_conflict(
    _: Request, exc: db.ReversalStateConflict
) -> JSONResponse:
    return JSONResponse(
        status_code=status.HTTP_409_CONFLICT,
        content={
            "error": exc.args[0],
            "detail": exc.args[1],
        },
    )


@app.exception_handler(db.BatchNotFound)
async def _batch_not_found(_: Request, exc: db.BatchNotFound) -> JSONResponse:
    return JSONResponse(
        status_code=status.HTTP_404_NOT_FOUND,
        content={"error": "batch_not_found", "batch_id": exc.args[0]},
    )


@app.exception_handler(db.BatchConflict)
async def _batch_conflict(_: Request, exc: db.BatchConflict) -> JSONResponse:
    return JSONResponse(
        status_code=status.HTTP_409_CONFLICT,
        content={
            "error": "batch_id_conflict",
            "batch_id": exc.args[0],
            "detail": (
                "batch_id was already used for a different batch payload"
            ),
        },
    )


@app.exception_handler(db.BatchItemDuplicate)
async def _batch_item_duplicate(
    _: Request, exc: db.BatchItemDuplicate
) -> JSONResponse:
    return JSONResponse(
        status_code=status.HTTP_409_CONFLICT,
        content={
            "error": "batch_item_request_id_duplicate",
            "request_id": exc.args[0],
            "detail": (
                "request_id occurs more than once inside the same batch; no "
                "items were admitted"
            ),
        },
    )


@app.get("/health", tags=["meta"])
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post(
    "/datasets/{dataset_id}",
    response_model=DatasetInfo,
    status_code=status.HTTP_201_CREATED,
    tags=["datasets"],
)
async def register_dataset(dataset_id: DATASET_ID, payload: DatasetRegister) -> dict:
    return await ledger.register_dataset(dataset_id, payload)


@app.get(
    "/datasets/{dataset_id}",
    response_model=DatasetInfo,
    tags=["datasets"],
)
async def get_dataset(dataset_id: DATASET_ID) -> dict:
    return await ledger.get_dataset_info(dataset_id)


@app.get(
    "/datasets/{dataset_id}/budget",
    response_model=BudgetView,
    tags=["datasets"],
)
async def get_budget(dataset_id: DATASET_ID) -> dict:
    info = await ledger.get_dataset_info(dataset_id)
    return info["budget"]


@app.get(
    "/datasets/{dataset_id}/budget/release-forecast",
    response_model=ReleaseForecast,
    tags=["reservations"],
    summary="Simulate future TTL-driven budget releases (read-only)",
)
async def get_release_forecast(
    dataset_id: DATASET_ID,
    deadline: Annotated[
        datetime,
        Query(
            description=(
                "Future UTC instant to forecast up to, as an ISO-8601 "
                "timestamp (e.g. 2026-09-24T00:00:00Z).  Reservations "
                "expiring at or before this instant are simulated as "
                "released; the endpoint never modifies stored state."
            ),
        ),
    ],
) -> dict:
    return await ledger.release_forecast(dataset_id, deadline)


@app.post(
    "/datasets/{dataset_id}/requests",
    response_model=Verdict,
    tags=["requests"],
)
async def submit_request(dataset_id: DATASET_ID, payload: AnalysisRequest):
    verdict, http_status = await ledger.submit_analysis(dataset_id, payload)
    return JSONResponse(status_code=http_status, content=verdict)


@app.post(
    "/datasets/{dataset_id}/selection-plans",
    response_model=SelectionPlanView,
    tags=["selection-plans"],
    summary="Trial the best feasible subset of candidates (read-only)",
)
async def create_selection_plan(
    dataset_id: DATASET_ID, payload: SelectionPlanRequest
) -> dict:
    return await ledger.selection_plan(dataset_id, payload.candidates)


@app.post(
    "/datasets/{dataset_id}/cost-attributions",
    response_model=CostAttributionView,
    tags=["cost-attributions"],
    summary="Attribute a granted-request group's epsilon increment (read-only)",
)
async def create_cost_attribution(
    dataset_id: DATASET_ID, payload: CostAttributionRequest
) -> dict:
    return await ledger.cost_attribution(dataset_id, payload)


@app.post(
    "/datasets/{dataset_id}/repeat-run-trials",
    response_model=RepeatRunTrialView,
    tags=["repeat-run-trials"],
    summary="Trial how many repeated workload runs fit the cap (read-only)",
)
async def create_repeat_run_trial(
    dataset_id: DATASET_ID, payload: RepeatRunTrialRequest
) -> dict:
    return await ledger.repeat_run_trial(dataset_id, payload)


@app.post(
    "/datasets/{dataset_id}/reversal-plans",
    response_model=ReversalPlanView,
    tags=["reversal-plans"],
    summary="Trial the smallest reversal subset admitting a pending item "
    "(read-only)",
)
async def create_reversal_plan(
    dataset_id: DATASET_ID, payload: ReversalPlanRequest
) -> dict:
    return await ledger.reversal_plan(dataset_id, payload)


@app.get(
    "/datasets/{dataset_id}/requests",
    response_model=RequestList,
    tags=["requests"],
)
async def get_requests(dataset_id: DATASET_ID) -> dict:
    return await ledger.list_requests(dataset_id)


@app.post(
    "/datasets/{dataset_id}/request-batches",
    response_model=BatchVerdict,
    tags=["request-batches"],
    summary="Atomically admit or reject 2-20 analysis items as one batch",
)
async def submit_request_batch(
    dataset_id: DATASET_ID, payload: BatchAnalysisRequest
):
    verdict, http_status = await ledger.submit_analysis_batch(
        dataset_id, payload
    )
    return JSONResponse(status_code=http_status, content=verdict)


@app.get(
    "/datasets/{dataset_id}/request-batches/{batch_id}",
    response_model=BatchVerdict,
    tags=["request-batches"],
)
async def get_request_batch(
    dataset_id: DATASET_ID, batch_id: BATCH_ID
) -> dict:
    return await ledger.get_request_batch_view(dataset_id, batch_id)


# --------------------------------------------------------------- reversals --


@app.post(
    "/datasets/{dataset_id}/requests/{request_id}/reversals",
    response_model=ReversalView,
    status_code=status.HTTP_201_CREATED,
    tags=["reversals"],
)
async def create_reversal(
    dataset_id: DATASET_ID, request_id: REQUEST_ID, payload: ReversalCreate
):
    view, http_status = await ledger.reverse_request(
        dataset_id, request_id, payload
    )
    return JSONResponse(status_code=http_status, content=view)


@app.get(
    "/datasets/{dataset_id}/reversals",
    response_model=ReversalList,
    tags=["reversals"],
)
async def list_reversals(dataset_id: DATASET_ID) -> dict:
    return await ledger.list_reversals(dataset_id)


@app.get(
    "/datasets/{dataset_id}/requests/{request_id}/reversals/{reversal_id}",
    response_model=ReversalView,
    tags=["reversals"],
)
async def get_reversal(
    dataset_id: DATASET_ID, request_id: REQUEST_ID, reversal_id: REVERSAL_ID
) -> dict:
    return await ledger.get_reversal(dataset_id, request_id, reversal_id)


# -------------------------------------------------------------- reservations


@app.post(
    "/datasets/{dataset_id}/reservations",
    response_model=ReservationView,
    tags=["reservations"],
)
async def create_reservation(dataset_id: DATASET_ID, payload: ReservationCreate):
    view, http_status = await ledger.create_reservation(dataset_id, payload)
    return JSONResponse(status_code=http_status, content=view)


@app.get(
    "/datasets/{dataset_id}/reservations",
    response_model=ReservationList,
    tags=["reservations"],
)
async def list_reservations(dataset_id: DATASET_ID) -> dict:
    return await ledger.list_reservations(dataset_id)


@app.get(
    "/datasets/{dataset_id}/reservations/{reservation_id}",
    response_model=ReservationView,
    tags=["reservations"],
)
async def get_reservation(dataset_id: DATASET_ID, reservation_id: RESERVATION_ID) -> dict:
    return await ledger.get_reservation(dataset_id, reservation_id)


@app.post(
    "/datasets/{dataset_id}/reservations/{reservation_id}/confirm",
    response_model=Verdict,
    tags=["reservations"],
)
async def confirm_reservation(
    dataset_id: DATASET_ID, reservation_id: RESERVATION_ID, payload: ReservationConfirm
):
    verdict, http_status = await ledger.confirm_reservation(
        dataset_id, reservation_id, payload
    )
    return JSONResponse(status_code=http_status, content=verdict)


@app.post(
    "/datasets/{dataset_id}/reservations/{reservation_id}/release",
    response_model=ReservationView,
    tags=["reservations"],
)
async def release_reservation(dataset_id: DATASET_ID, reservation_id: RESERVATION_ID):
    view, http_status = await ledger.release_reservation(dataset_id, reservation_id)
    return JSONResponse(status_code=http_status, content=view)


@app.post(
    "/datasets/{dataset_id}/reservations/{reservation_id}/renewals",
    response_model=ReservationRenewalView,
    status_code=status.HTTP_201_CREATED,
    tags=["reservations"],
    summary="Extend a granted reservation's expiry (idempotent per renewal_id)",
)
async def create_reservation_renewal(
    dataset_id: DATASET_ID,
    reservation_id: RESERVATION_ID,
    payload: ReservationRenewalCreate,
):
    view, http_status = await ledger.renew_reservation(
        dataset_id, reservation_id, payload
    )
    return JSONResponse(status_code=http_status, content=view)


@app.post(
    "/datasets/{dataset_id}/reservations/{reservation_id}/adjustments",
    response_model=ReservationAdjustmentView,
    status_code=status.HTTP_201_CREATED,
    tags=["reservations"],
    summary="Adjust a granted reservation's epsilon/delta quota "
    "(idempotent per adjustment_id)",
)
async def create_reservation_adjustment(
    dataset_id: DATASET_ID,
    reservation_id: RESERVATION_ID,
    payload: ReservationAdjustmentCreate,
):
    view, http_status = await ledger.adjust_reservation(
        dataset_id, reservation_id, payload
    )
    return JSONResponse(status_code=http_status, content=view)


# -------------------------------------------------------------- transfers --


@app.post(
    "/budget-transfers",
    response_model=BudgetTransferView,
    status_code=status.HTTP_201_CREATED,
    tags=["transfers"],
)
async def create_budget_transfer(payload: BudgetTransferRequest):
    view, http_status = await ledger.transfer_budget(payload)
    return JSONResponse(status_code=http_status, content=view)
