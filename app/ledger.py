"""Ledger business logic: atomic admission control and idempotency."""
from __future__ import annotations

import asyncio
import math
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

import aiosqlite

from . import db, rdp
from .config import settings
from .schemas import (
    AnalysisRequest,
    BatchAnalysisRequest,
    BudgetTransferRequest,
    CostAttributionRequest,
    DatasetRegister,
    MAX_EPSILON_CAP,
    MAX_TTL_SECONDS,
    RepeatRunTrialRequest,
    RepeatRunWorkloadItem,
    ReversalCreate,
    ReversalPlanRequest,
    ReservationAdjustmentCreate,
    ReservationConfirm,
    ReservationCreate,
    ReservationRenewalCreate,
    SelectionPlanCandidate,
    utcnow,
)

# Serialises all writers inside this process.  Combined with
# BEGIN IMMEDIATE + busy_timeout, two concurrent submissions can never both
# observe the same pre-update balance, even across multiple processes.
# Locks are per-event-loop (asyncio.Lock binds to the loop of its first use),
# so they are created lazily and cached by loop.
_write_locks: dict[asyncio.AbstractEventLoop, asyncio.Lock] = {}


class InvalidForecastDeadline(Exception):
    """Release-forecast deadline is not a future UTC instant (HTTP 422)."""


def _write_lock() -> asyncio.Lock:
    loop = asyncio.get_running_loop()
    lock = _write_locks.get(loop)
    if lock is None:
        lock = asyncio.Lock()
        _write_locks[loop] = lock
    return lock


# ----------------------------------------------------------------- helpers --


async def _committed_entries(
    conn: aiosqlite.Connection, dataset_id: str, now_iso: str
) -> list[tuple[float, float]]:
    """Granted analysis requests plus non-expired granted reservations.

    Calling this also lazily sweeps reservations whose TTL has elapsed, so
    expired rows never occupy budget in admission checks or queries.
    """
    granted = await db.list_granted_entries(conn, dataset_id)
    reserved = await db.list_active_reservation_entries(
        conn, dataset_id, now_iso=now_iso
    )
    return granted + reserved


def _snapshot(
    granted: list[tuple[float, float]],
    reserved: list[tuple[float, float]],
    cap: float,
    target_delta: float,
) -> dict[str, Any]:
    """Compose granted requests + active reservations in one RDP pass.

    Returns the same epsilon/count fields as ``BudgetView`` (plus the composed
    curve) so that live budget views and read-only release forecasts share one
    accounting path.
    """
    _, consumed, _ = rdp.evaluate_entries(granted, target_delta)
    committed_curve, committed, best_order = rdp.evaluate_entries(
        granted + reserved, target_delta
    )
    consumed = max(0.0, consumed)
    return {
        "epsilon_consumed": consumed,
        # Extra projected epsilon locked by active reservations alone.
        "epsilon_reserved": max(0.0, committed - consumed),
        "epsilon_remaining": max(0.0, cap - committed),
        "rdp_best_order": best_order,
        "active_reservations": len(reserved),
        "_committed_curve": committed_curve,
    }


async def _budget(db_conn: aiosqlite.Connection, dataset: aiosqlite.Row) -> dict[str, Any]:
    now_iso = utcnow().isoformat()
    dataset_id = dataset["dataset_id"]

    granted = await db.list_granted_entries(db_conn, dataset_id)
    reserved_entries = await db.list_active_reservation_entries(
        db_conn, dataset_id, now_iso=now_iso
    )
    # Read-only endpoints still run the lazy-expiry UPDATE above; persist it.
    if db_conn.in_transaction:
        await db_conn.commit()

    snap = _snapshot(
        granted, reserved_entries, dataset["epsilon_cap"], dataset["delta_cap"]
    )
    return {
        "epsilon_cap": dataset["epsilon_cap"],
        "delta_cap": dataset["delta_cap"],
        "epsilon_consumed": snap["epsilon_consumed"],
        "epsilon_reserved": snap["epsilon_reserved"],
        "epsilon_remaining": snap["epsilon_remaining"],
        "rdp_best_order": snap["rdp_best_order"],
        "granted_requests": len(granted),
        "active_reservations": snap["active_reservations"],
        "rdp_curve": rdp.sample_curve(snap["_committed_curve"]),
    }


def _dataset_view(dataset: aiosqlite.Row, budget: dict[str, Any]) -> dict[str, Any]:
    return {
        "dataset_id": dataset["dataset_id"],
        "description": dataset["description"],
        "created_at": dataset["created_at"],
        "budget": budget,
    }


async def register_dataset(dataset_id: str, payload: DatasetRegister) -> dict[str, Any]:
    created_at = utcnow().isoformat()
    async with _write_lock():
        await db.create_dataset(
            dataset_id,
            payload.epsilon_cap,
            payload.delta_cap,
            payload.description,
            created_at,
        )
    async with db.connect() as conn:
        dataset = await db.require_dataset(conn, dataset_id)
        budget = await _budget(conn, dataset)
    return _dataset_view(dataset, budget)


async def get_dataset_info(dataset_id: str) -> dict[str, Any]:
    async with db.connect() as conn:
        dataset = await db.require_dataset(conn, dataset_id)
        budget = await _budget(conn, dataset)
    return _dataset_view(dataset, budget)


# --------------------------------------------------------------- submission --


async def submit_analysis(
    dataset_id: str, payload: AnalysisRequest
) -> tuple[dict[str, Any], int]:
    """Idempotently submit an analysis request.

    Returns ``(verdict_dict, http_status)``.  http_status is 201 for a newly
    stored verdict and 200 for an idempotent replay / conflict-free lookup.
    """
    created_at = utcnow().isoformat()

    async with _write_lock():
        async with db.connect() as conn:
            await db.begin_immediate(conn)
            try:
                dataset = await db.require_dataset(conn, dataset_id)

                existing = await db.get_request(conn, dataset_id, payload.request_id)
                if existing is not None:
                    # Idempotent replay: same request_id must be identical.
                    same = (
                        existing["query_type"] == payload.query_type
                        and existing["epsilon"] == payload.epsilon
                        and existing["delta"] == payload.delta
                    )
                    if not same:
                        raise db.RequestConflict(payload.request_id)
                    verdict = _verdict_from_row(existing, dataset, replay=True)
                    await conn.commit()
                    return verdict, 200

                # --- admission check inside the same atomic transaction ---
                # Outstanding reservations occupy budget too: only what is
                # left after granted requests *and* active reservations may
                # be admitted.
                now_iso = utcnow().isoformat()
                entries = await _committed_entries(conn, dataset_id, now_iso)
                _, committed, best_order = rdp.evaluate_entries(
                    entries, dataset["delta_cap"]
                )

                projected, proj_order = rdp.project_entries(
                    entries,
                    payload.epsilon,
                    payload.delta,
                    dataset["delta_cap"],
                )

                cap = dataset["epsilon_cap"]
                if projected <= cap + settings.epsilon_tolerance:
                    status = "granted"
                    reason = None
                    after = projected
                    order_used = proj_order
                else:
                    status = "rejected"
                    reason = (
                        f"budget exceeded: projected epsilon {projected:.6g} "
                        f"would surpass cap {cap:.6g} "
                        f"(committed {max(0.0, committed):.6g})"
                    )
                    # A rejection consumes nothing.
                    after = max(0.0, committed)
                    order_used = best_order

                await db.insert_request(
                    conn,
                    dataset_id=dataset_id,
                    request_id=payload.request_id,
                    query_type=payload.query_type,
                    epsilon=payload.epsilon,
                    delta=payload.delta,
                    status=status,
                    reason=reason,
                    epsilon_before=max(0.0, committed),
                    epsilon_after=after,
                    rdp_best_order=order_used,
                    created_at=created_at,
                )
                await conn.commit()
            except Exception:
                await conn.rollback()
                raise

            row = await db.get_request(conn, dataset_id, payload.request_id)
            verdict = _verdict_from_row(row, dataset, replay=False)
            return verdict, 201


def _verdict_from_row(row: aiosqlite.Row, dataset: aiosqlite.Row, *, replay: bool) -> dict[str, Any]:
    remaining = max(0.0, dataset["epsilon_cap"] - row["epsilon_after"])
    return {
        "dataset_id": row["dataset_id"],
        "request_id": row["request_id"],
        "query_type": row["query_type"],
        "epsilon": row["epsilon"],
        "delta": row["delta"],
        "allowed": row["status"] == "granted",
        "status": row["status"],
        "reason": row["reason"],
        "epsilon_before": row["epsilon_before"],
        "epsilon_after": row["epsilon_after"],
        "epsilon_remaining": remaining,
        "rdp_best_order": row["rdp_best_order"],
        "idempotent_replay": replay,
        "created_at": row["created_at"],
    }


async def list_requests(dataset_id: str) -> dict[str, Any]:
    async with db.connect() as conn:
        await db.require_dataset(conn, dataset_id)
        rows = await db.list_requests(conn, dataset_id)
    return {
        "dataset_id": dataset_id,
        "count": len(rows),
        "requests": [
            {
                "dataset_id": r["dataset_id"],
                "request_id": r["request_id"],
                "query_type": r["query_type"],
                "epsilon": r["epsilon"],
                "delta": r["delta"],
                "status": r["status"],
                "reason": r["reason"],
                "epsilon_before": r["epsilon_before"],
                "epsilon_after": r["epsilon_after"],
                "created_at": r["created_at"],
                "reversed": r["reversal_reversal_id"] is not None,
                "reversal_id": r["reversal_reversal_id"],
                "reversal_reason": r["reversal_reason"],
                "reversed_at": r["reversed_at"],
            }
            for r in rows
        ],
    }


# --------------------------------------------------------- analysis batches --


def _batch_payload_matches(
    items_rows: list[aiosqlite.Row], payload: BatchAnalysisRequest
) -> bool:
    """Whether a replay carries exactly the stored items, in the same order."""
    if len(items_rows) != len(payload.items):
        return False
    for row, item in zip(items_rows, payload.items):
        if (
            row["request_id"] != item.request_id
            or row["query_type"] != item.query_type
            or row["epsilon"] != item.epsilon
            or row["delta"] != item.delta
        ):
            return False
    return True


def _batch_view_from_rows(
    batch: aiosqlite.Row,
    item_rows: list[aiosqlite.Row],
    *,
    replay: bool,
) -> dict[str, Any]:
    allowed = batch["status"] == "granted"
    cap = batch["epsilon_cap"]
    reason = batch["reason"]
    items: list[dict[str, Any]] = []
    for row in item_rows:
        if allowed:
            before = row["epsilon_before"]
            after = row["epsilon_after"]
            remaining = max(0.0, cap - after)
            item_reason: Optional[str] = None
        else:
            # A rejected batch consumed nothing: per-item snapshots are unset.
            before = after = remaining = None
            item_reason = reason
        items.append(
            {
                "request_id": row["request_id"],
                "query_type": row["query_type"],
                "epsilon": row["epsilon"],
                "delta": row["delta"],
                "allowed": allowed,
                "status": batch["status"],
                "reason": item_reason,
                "epsilon_before": before,
                "epsilon_after": after,
                "epsilon_remaining": remaining,
                "rdp_best_order": row["rdp_best_order"],
                "created_at": row["created_at"],
            }
        )
    return {
        "dataset_id": batch["dataset_id"],
        "batch_id": batch["batch_id"],
        "allowed": allowed,
        "status": batch["status"],
        "reason": reason,
        "item_count": len(items),
        "epsilon_cap": cap,
        "epsilon_before": batch["epsilon_before"],
        "epsilon_after": batch["epsilon_after"],
        "epsilon_remaining": max(0.0, cap - batch["epsilon_after"]),
        "rdp_best_order": batch["rdp_best_order"],
        "idempotent_replay": replay,
        "created_at": batch["created_at"],
        "items": items,
    }


async def submit_analysis_batch(
    dataset_id: str, payload: BatchAnalysisRequest
) -> tuple[dict[str, Any], int]:
    """Atomically admit or reject a whole batch of analysis items.

    All items are composed together with the currently committed mechanisms
    (granted non-reversed requests plus non-expired granted reservations) in a
    single RDP projection.  If the whole batch fits, every item is inserted as
    an ordinary granted request in the same ``BEGIN IMMEDIATE`` transaction as
    the batch record.  If the projection exceeds the cap, nothing is inserted
    into ``requests`` -- the batch occupies zero budget -- and the rejected
    verdict is persisted instead.

    Replaying the same ``batch_id`` with an identical ordered payload returns
    the stored verdict (HTTP 200); a different payload is a 409.  A request_id
    that appears twice in the payload, or is already owned by another request,
    is a 409 and leaves no partial records behind.
    """
    created_at = utcnow().isoformat()

    async with _write_lock():
        async with db.connect() as conn:
            await db.begin_immediate(conn)
            try:
                dataset = await db.require_dataset(conn, dataset_id)

                existing_batch = await db.get_request_batch(
                    conn, dataset_id, payload.batch_id
                )
                if existing_batch is not None:
                    existing_items = await db.list_request_batch_items(
                        conn, existing_batch["id"]
                    )
                    if not _batch_payload_matches(existing_items, payload):
                        raise db.BatchConflict(payload.batch_id)
                    view = _batch_view_from_rows(
                        existing_batch, existing_items, replay=True
                    )
                    await conn.commit()
                    return view, 200

                # In-batch duplicate request_ids are always a conflict, even on
                # a replay key that has never been seen before.
                seen: set[str] = set()
                for item in payload.items:
                    if item.request_id in seen:
                        raise db.BatchItemDuplicate(item.request_id)
                    seen.add(item.request_id)
                    row = await db.get_request(
                        conn, dataset_id, item.request_id
                    )
                    if row is not None:
                        # Owned by an ordinary request, a reservation
                        # confirmation, or an earlier *granted* batch.
                        raise db.RequestConflict(item.request_id)

                # --- joint admission check inside the atomic transaction ---
                now_iso = utcnow().isoformat()
                entries = await _committed_entries(conn, dataset_id, now_iso)
                _, committed, best_order = rdp.evaluate_entries(
                    entries, dataset["delta_cap"]
                )
                committed = max(0.0, committed)
                cap = dataset["epsilon_cap"]

                projected_curve = [*entries]
                # Cumulative composed epsilon per item (in submission order);
                # kept so the stored verdict survives later budget changes.
                item_after: list[float] = []
                item_orders: list[float] = []
                for item in payload.items:
                    projected_curve.append((item.epsilon, item.delta))
                    _, eps, order = rdp.evaluate_entries(
                        projected_curve, dataset["delta_cap"]
                    )
                    item_after.append(max(0.0, eps))
                    item_orders.append(order)
                projected, proj_order = (
                    item_after[-1],
                    item_orders[-1],
                )

                if projected <= cap + settings.epsilon_tolerance:
                    status = "granted"
                    reason: Optional[str] = None
                    after = projected
                    order_used = proj_order
                else:
                    status = "rejected"
                    reason = (
                        f"batch rejected: projected epsilon {projected:.6g} "
                        f"for {len(payload.items)} items would surpass cap "
                        f"{cap:.6g} (committed {committed:.6g})"
                    )
                    # Nothing is admitted: zero occupancy from this batch.
                    after = committed
                    order_used = best_order

                batch_pk = await db.insert_request_batch(
                    conn,
                    dataset_id=dataset_id,
                    batch_id=payload.batch_id,
                    status=status,
                    reason=reason,
                    epsilon_cap=cap,
                    epsilon_before=committed,
                    epsilon_after=after,
                    rdp_best_order=order_used,
                    created_at=created_at,
                )

                running_before = committed
                for position, (item, item_after_eps, item_order) in enumerate(
                    zip(payload.items, item_after, item_orders)
                ):
                    if status == "granted":
                        before = running_before
                        # The granted request rows are the live accounting
                        # records; their snapshots match the batch items.
                        await db.insert_request(
                            conn,
                            dataset_id=dataset_id,
                            request_id=item.request_id,
                            query_type=item.query_type,
                            epsilon=item.epsilon,
                            delta=item.delta,
                            status="granted",
                            reason=None,
                            epsilon_before=before,
                            epsilon_after=item_after_eps,
                            rdp_best_order=item_order,
                            created_at=created_at,
                        )
                        running_before = item_after_eps
                        item_before_value: Optional[float] = before
                        item_after_value: Optional[float] = item_after_eps
                        item_order_value: Optional[float] = item_order
                    else:
                        # Rejected batch: items stay without snapshots, no
                        # request rows are written, the batch occupies nothing.
                        item_before_value = None
                        item_after_value = None
                        item_order_value = None
                    await db.insert_request_batch_item(
                        conn,
                        batch_pk=batch_pk,
                        position=position,
                        request_id=item.request_id,
                        query_type=item.query_type,
                        epsilon=item.epsilon,
                        delta=item.delta,
                        epsilon_before=item_before_value,
                        epsilon_after=item_after_value,
                        rdp_best_order=item_order_value,
                        created_at=created_at,
                    )
                await conn.commit()
            except Exception:
                await conn.rollback()
                raise

            batch_row = await db.get_request_batch(
                conn, dataset_id, payload.batch_id
            )
            item_rows = await db.list_request_batch_items(conn, batch_row["id"])
            return _batch_view_from_rows(
                batch_row, item_rows, replay=False
            ), 201


async def get_request_batch_view(
    dataset_id: str, batch_id: str
) -> dict[str, Any]:
    async with db.connect() as conn:
        await db.require_dataset(conn, dataset_id)
        batch = await db.get_request_batch(conn, dataset_id, batch_id)
        if batch is None:
            raise db.BatchNotFound(batch_id)
        item_rows = await db.list_request_batch_items(conn, batch["id"])
    return _batch_view_from_rows(batch, item_rows, replay=False)


# --------------------------------------------------------------- reversals --


def _reversal_view_from_row(
    row: aiosqlite.Row, *, replay: bool
) -> dict[str, Any]:
    return {
        "reversal_id": row["reversal_id"],
        "dataset_id": row["dataset_id"],
        "request_id": row["request_id"],
        "reason": row["reason"],
        "epsilon": row["epsilon"],
        "delta": row["delta"],
        "epsilon_cap": row["epsilon_cap"],
        "epsilon_before": row["epsilon_before"],
        "epsilon_after": row["epsilon_after"],
        "epsilon_remaining": max(
            0.0, row["epsilon_cap"] - row["epsilon_after"]
        ),
        "rdp_best_order": row["rdp_best_order"],
        "idempotent_replay": replay,
        "created_at": row["created_at"],
    }


async def reverse_request(
    dataset_id: str, request_id: str, payload: ReversalCreate
) -> tuple[dict[str, Any], int]:
    """Reverse a granted, not-yet-reversed analysis request.

    The original request and any reservation-confirmation history stay exactly
    as they were.  An immutable reversal record is inserted in the same
    ``BEGIN IMMEDIATE`` transaction; afterwards the target request is excluded
    from the RDP composition everywhere (budget queries, new requests,
    reservation confirmation, cap transfers, release forecasts), while active
    reservations and the other granted requests keep counting.

    Replaying the same ``reversal_id`` with the same target and reason returns
    the stored record; a different payload on that id is a 409, as is trying
    to reverse a rejected or already-reversed request.
    """
    now_iso = utcnow().isoformat()

    async with _write_lock():
        async with db.connect() as conn:
            await db.begin_immediate(conn)
            try:
                # Look the idempotency key up first, so retries replay the
                # original record even when the target request is missing from
                # this dataset (or was already reversed).
                by_id = await db.get_reversal_by_id(conn, payload.reversal_id)
                if by_id is not None:
                    same = (
                        by_id["dataset_id"] == dataset_id
                        and by_id["request_id"] == request_id
                        and by_id["reason"] == payload.reason
                    )
                    if not same:
                        raise db.ReversalConflict(payload.reversal_id)
                    view = _reversal_view_from_row(by_id, replay=True)
                    await conn.commit()
                    return view, 200

                dataset = await db.require_dataset(conn, dataset_id)
                request_row = await db.get_request(conn, dataset_id, request_id)
                if request_row is None:
                    raise db.RequestNotFound(request_id)
                existing = await db.get_reversal_for_request(
                    conn, dataset_id, request_id
                )
                if existing is not None:
                    raise db.ReversalStateConflict(
                        "request_already_reversed",
                        f"request {request_id!r} was already reversed by "
                        f"reversal {existing['reversal_id']!r}",
                    )
                if request_row["status"] != "granted":
                    raise db.ReversalStateConflict(
                        "request_not_granted",
                        f"request {request_id!r} is "
                        f"{request_row['status']} and cannot be reversed",
                    )

                # Composed occupancy with the target request still in the
                # granted set (it has no reversal row yet), and after removing
                # exactly its (epsilon, delta) entry.  RDP composition is
                # order-independent, so position does not matter.
                entries = await _committed_entries(conn, dataset_id, now_iso)
                _, epsilon_before, _ = rdp.evaluate_entries(
                    entries, dataset["delta_cap"]
                )
                target = (request_row["epsilon"], request_row["delta"])
                others = list(entries)
                try:
                    others.remove(target)
                except ValueError:
                    # Defensive: granted-entry set and row state diverged.
                    others = [e for e in entries if e != target]
                _, epsilon_after, best_order = rdp.evaluate_entries(
                    others, dataset["delta_cap"]
                )
                epsilon_before = max(0.0, epsilon_before)
                epsilon_after = max(0.0, epsilon_after)

                await db.insert_request_reversal(
                    conn,
                    reversal_id=payload.reversal_id,
                    dataset_id=dataset_id,
                    request_id=request_id,
                    reason=payload.reason,
                    epsilon=request_row["epsilon"],
                    delta=request_row["delta"],
                    epsilon_cap=dataset["epsilon_cap"],
                    epsilon_before=epsilon_before,
                    epsilon_after=epsilon_after,
                    rdp_best_order=best_order,
                    created_at=now_iso,
                )
                await conn.commit()
            except Exception:
                await conn.rollback()
                raise

            row = await db.get_reversal_by_id(conn, payload.reversal_id)
            return _reversal_view_from_row(row, replay=False), 201


async def get_reversal(
    dataset_id: str, request_id: str, reversal_id: str
) -> dict[str, Any]:
    async with db.connect() as conn:
        await db.require_dataset(conn, dataset_id)
        request_row = await db.get_request(conn, dataset_id, request_id)
        if request_row is None:
            raise db.RequestNotFound(request_id)
        row = await db.get_reversal_for_request(conn, dataset_id, request_id)
        if row is None or row["reversal_id"] != reversal_id:
            raise db.ReversalNotFound(reversal_id)
        return _reversal_view_from_row(row, replay=False)


async def list_reversals(dataset_id: str) -> dict[str, Any]:
    async with db.connect() as conn:
        await db.require_dataset(conn, dataset_id)
        rows = await db.list_reversals(conn, dataset_id)
    return {
        "dataset_id": dataset_id,
        "count": len(rows),
        "reversals": [
            _reversal_view_from_row(row, replay=False) for row in rows
        ],
    }


# ------------------------------------------------------------ reservations --


def _reservation_view(
    row: aiosqlite.Row, dataset: aiosqlite.Row, *, replay: bool
) -> dict[str, Any]:
    remaining = max(0.0, dataset["epsilon_cap"] - row["epsilon_after"])
    return {
        "dataset_id": row["dataset_id"],
        "reservation_id": row["reservation_id"],
        "epsilon": row["epsilon"],
        "delta": row["delta"],
        "ttl_seconds": row["ttl_seconds"],
        "allowed": row["status"] in ("granted", "confirmed"),
        "status": row["status"],
        "reason": row["reason"],
        "epsilon_before": row["epsilon_before"],
        "epsilon_after": row["epsilon_after"],
        "epsilon_remaining": remaining,
        "rdp_best_order": row["rdp_best_order"],
        "idempotent_replay": replay,
        "created_at": row["created_at"],
        "expires_at": row["expires_at"],
        "expired_at": row["expired_at"],
        "confirmed_at": row["confirmed_at"],
        "confirmed_request_id": row["confirmed_request_id"],
        "released_at": row["released_at"],
    }


async def _refresh_reservation_row(
    conn: aiosqlite.Connection,
    row: aiosqlite.Row,
    now_iso: str,
) -> aiosqlite.Row:
    """If row is a granted reservation past its TTL, mark it expired and refetch."""
    if (
        row["status"] == "granted"
        and row["expires_at"] is not None
        and row["expires_at"] <= now_iso
    ):
        await db.expire_reservation(
            conn,
            dataset_id=row["dataset_id"],
            reservation_id=row["reservation_id"],
            expired_at=now_iso,
        )
        refreshed = await db.get_reservation(
            conn, row["dataset_id"], row["reservation_id"]
        )
        assert refreshed is not None
        return refreshed
    return row


async def create_reservation(
    dataset_id: str, payload: ReservationCreate
) -> tuple[dict[str, Any], int]:
    """Atomically reserve budget; idempotent on ``reservation_id``."""
    now = utcnow()
    now_iso = now.isoformat()

    async with _write_lock():
        async with db.connect() as conn:
            await db.begin_immediate(conn)
            try:
                dataset = await db.require_dataset(conn, dataset_id)

                existing = await db.get_reservation(
                    conn, dataset_id, payload.reservation_id
                )
                if existing is not None:
                    # Idempotent replay: same reservation_id, same *creation*
                    # payload.  A later quota adjustment rewrites the live
                    # epsilon/delta, so the creation-time original_* columns
                    # are the authority here.
                    same = (
                        existing["original_epsilon"] == payload.epsilon
                        and existing["original_delta"] == payload.delta
                        and existing["ttl_seconds"] == payload.ttl_seconds
                    )
                    if not same:
                        raise db.ReservationConflict(payload.reservation_id)
                    existing = await _refresh_reservation_row(
                        conn, existing, now_iso
                    )
                    view = _reservation_view(existing, dataset, replay=True)
                    await conn.commit()
                    return view, 200

                entries = await _committed_entries(conn, dataset_id, now_iso)
                _, committed, best_order = rdp.evaluate_entries(
                    entries, dataset["delta_cap"]
                )
                projected, proj_order = rdp.project_entries(
                    entries,
                    payload.epsilon,
                    payload.delta,
                    dataset["delta_cap"],
                )

                cap = dataset["epsilon_cap"]
                expires_at = (now + timedelta(seconds=payload.ttl_seconds)).isoformat()
                if projected <= cap + settings.epsilon_tolerance:
                    status = "granted"
                    reason: Optional[str] = None
                    after = projected
                    order_used = proj_order
                else:
                    status = "rejected"
                    reason = (
                        f"budget exceeded: projected epsilon {projected:.6g} "
                        f"would surpass cap {cap:.6g} "
                        f"(committed {max(0.0, committed):.6g})"
                    )
                    # A rejected reservation locks nothing and never expires.
                    after = max(0.0, committed)
                    order_used = best_order
                    expires_at = None  # type: ignore[assignment]

                await db.insert_reservation(
                    conn,
                    dataset_id=dataset_id,
                    reservation_id=payload.reservation_id,
                    epsilon=payload.epsilon,
                    delta=payload.delta,
                    ttl_seconds=payload.ttl_seconds,
                    status=status,
                    reason=reason,
                    epsilon_before=max(0.0, committed),
                    epsilon_after=after,
                    rdp_best_order=order_used,
                    created_at=now_iso,
                    expires_at=expires_at,
                )
                await conn.commit()
            except Exception:
                await conn.rollback()
                raise

            row = await db.get_reservation(conn, dataset_id, payload.reservation_id)
            return _reservation_view(row, dataset, replay=False), 201


async def confirm_reservation(
    dataset_id: str, reservation_id: str, payload: ReservationConfirm
) -> tuple[dict[str, Any], int]:
    """Turn an active reservation into an analysis request under RDP accounting.

    The reserved (epsilon, delta) becomes a granted request with the client
    supplied ``request_id`` / ``query_type``; the locked budget is not charged
    twice (the reservation leaves the active set as soon as it is confirmed).
    """
    now_iso = utcnow().isoformat()

    async with _write_lock():
        async with db.connect() as conn:
            await db.begin_immediate(conn)
            try:
                dataset = await db.require_dataset(conn, dataset_id)
                row = await db.get_reservation(conn, dataset_id, reservation_id)
                if row is None:
                    raise db.ReservationNotFound(reservation_id)
                row = await _refresh_reservation_row(conn, row, now_iso)

                # Idempotent re-confirmation: the reservation has already been
                # turned into a request.  Replay that verdict when the same
                # request_id is supplied; a different request_id is a 409.
                if row["status"] == "confirmed":
                    if payload.request_id != row["confirmed_request_id"]:
                        raise db.ReservationStateConflict(
                            "reservation_confirmed",
                            "reservation was already confirmed as request "
                            f"{row['confirmed_request_id']!r}",
                        )
                    stored = await db.get_request(
                        conn, dataset_id, payload.request_id
                    )
                    if (
                        stored is not None
                        and stored["query_type"] == payload.query_type
                    ):
                        verdict = _verdict_from_row(stored, dataset, replay=True)
                        await conn.commit()
                        return verdict, 200
                    raise db.RequestConflict(payload.request_id)

                if row["status"] != "granted":
                    raise db.ReservationStateConflict(
                        f"reservation_{row['status']}",
                        f"reservation is {row['status']} and cannot be confirmed",
                    )

                existing = await db.get_request(conn, dataset_id, payload.request_id)
                if existing is not None:
                    # While the reservation is still merely granted, no request
                    # may own this request_id yet; re-confirms are handled in
                    # the 'confirmed' branch above.
                    raise db.RequestConflict(payload.request_id)

                # Project committed-with-self: other live reservations and all
                # granted requests, plus the (epsilon, delta) this reservation
                # locked at creation.
                active = await _committed_entries(conn, dataset_id, now_iso)
                # The reservation being confirmed is itself in the active set.
                # Remove exactly its entry (by identity position) to obtain the
                # "before" composition; after confirmation the same composition
                # is represented by the new granted request, so total
                # committed epsilon is unchanged and the budget is never
                # charged twice.
                target = (row["epsilon"], row["delta"])
                others = list(active)
                try:
                    others.remove(target)
                except ValueError:
                    # Defensive: active set and row state diverged.
                    others = active
                _, epsilon_before, _ = rdp.evaluate_entries(
                    others, dataset["delta_cap"]
                )
                _, epsilon_after, best_order = rdp.evaluate_entries(
                    active, dataset["delta_cap"]
                )
                epsilon_before = max(0.0, epsilon_before)
                epsilon_after = max(0.0, epsilon_after)

                await db.insert_request(
                    conn,
                    dataset_id=dataset_id,
                    request_id=payload.request_id,
                    query_type=payload.query_type,
                    epsilon=row["epsilon"],
                    delta=row["delta"],
                    status="granted",
                    reason=None,
                    epsilon_before=epsilon_before,
                    epsilon_after=epsilon_after,
                    rdp_best_order=best_order,
                    created_at=now_iso,
                )
                await db.confirm_reservation(
                    conn,
                    dataset_id=dataset_id,
                    reservation_id=reservation_id,
                    confirmed_request_id=payload.request_id,
                    confirmed_at=now_iso,
                    epsilon_before=epsilon_before,
                    epsilon_after=epsilon_after,
                    rdp_best_order=best_order,
                )
                await conn.commit()
            except Exception:
                await conn.rollback()
                raise

            request_row = await db.get_request(conn, dataset_id, payload.request_id)
            return _verdict_from_row(request_row, dataset, replay=False), 201


async def release_reservation(
    dataset_id: str, reservation_id: str
) -> tuple[dict[str, Any], int]:
    """Idempotently cancel a reservation; its budget is freed immediately.

    Releasing an already released / expired reservation replays the final
    state.  Confirmed reservations are consumed and cannot be released;
    rejected reservations never held any budget.
    """
    now_iso = utcnow().isoformat()

    async with _write_lock():
        async with db.connect() as conn:
            await db.begin_immediate(conn)
            try:
                dataset = await db.require_dataset(conn, dataset_id)
                row = await db.get_reservation(conn, dataset_id, reservation_id)
                if row is None:
                    raise db.ReservationNotFound(reservation_id)
                row = await _refresh_reservation_row(conn, row, now_iso)

                if row["status"] in ("released", "expired"):
                    view = _reservation_view(row, dataset, replay=True)
                    await conn.commit()
                    return view, 200
                if row["status"] != "granted":
                    raise db.ReservationStateConflict(
                        f"reservation_{row['status']}",
                        f"reservation is {row['status']} and cannot be released",
                    )

                await db.release_reservation(
                    conn,
                    dataset_id=dataset_id,
                    reservation_id=reservation_id,
                    released_at=now_iso,
                )
                await conn.commit()
            except Exception:
                await conn.rollback()
                raise

            row = await db.get_reservation(conn, dataset_id, reservation_id)
            return _reservation_view(row, dataset, replay=False), 200


# ---------------------------------------------------------------- renewals --


def _renewal_view_from_row(row: aiosqlite.Row, *, replay: bool) -> dict[str, Any]:
    return {
        "renewal_id": row["renewal_id"],
        "dataset_id": row["dataset_id"],
        "reservation_id": row["reservation_id"],
        "extend_seconds": row["extend_seconds"],
        "expires_at_before": row["expires_at_before"],
        "expires_at_after": row["expires_at_after"],
        "idempotent_replay": replay,
        "created_at": row["created_at"],
    }


async def renew_reservation(
    dataset_id: str, reservation_id: str, payload: ReservationRenewalCreate
) -> tuple[dict[str, Any], int]:
    """Extend a granted, non-expired reservation's expiry instant.

    The reserved ``(epsilon, delta)`` and the original ``ttl_seconds`` are
    left untouched and no additional budget is occupied: the reservation
    simply keeps its existing lock for ``extend_seconds`` longer.  The new
    expiry may never be more than :data:`MAX_TTL_SECONDS` (90 days) after the
    reservation's ``created_at``.  The state check, the ``expires_at`` update
    and the immutable renewal record all happen in one ``BEGIN IMMEDIATE``
    transaction, so a renewal can never race a confirm / release / expiry
    sweep into an inconsistent state.

    Replaying the same ``(dataset_id, renewal_id)`` with the same payload
    returns the stored before/after expiry instants (HTTP 200) -- even if the
    reservation has since expired; a different payload on that id is a 409,
    as is a *new* renewal on an expired or terminal reservation.
    """
    now_iso = utcnow().isoformat()

    async with _write_lock():
        async with db.connect() as conn:
            await db.begin_immediate(conn)
            try:
                await db.require_dataset(conn, dataset_id)

                # Idempotency key first, so a retry replays the original
                # record even when the reservation has since expired.
                existing = await db.get_reservation_renewal(
                    conn, dataset_id, payload.renewal_id
                )
                if existing is not None:
                    same = (
                        existing["reservation_id"] == reservation_id
                        and existing["extend_seconds"] == payload.extend_seconds
                    )
                    if not same:
                        raise db.RenewalConflict(payload.renewal_id)
                    view = _renewal_view_from_row(existing, replay=True)
                    await conn.commit()
                    return view, 200

                row = await db.get_reservation(conn, dataset_id, reservation_id)
                if row is None:
                    raise db.ReservationNotFound(reservation_id)
                row = await _refresh_reservation_row(conn, row, now_iso)
                if row["status"] != "granted":
                    raise db.ReservationStateConflict(
                        f"reservation_{row['status']}",
                        f"reservation is {row['status']} and cannot be renewed",
                    )

                created_at = datetime.fromisoformat(row["created_at"])
                expires_before = datetime.fromisoformat(row["expires_at"])
                expires_after = expires_before + timedelta(
                    seconds=payload.extend_seconds
                )
                if (
                    expires_after - created_at
                    > timedelta(seconds=MAX_TTL_SECONDS)
                ):
                    raise db.ReservationStateConflict(
                        "renewal_horizon_exceeded",
                        f"new expiry {expires_after.isoformat()} would be more "
                        f"than {MAX_TTL_SECONDS} seconds (90 days) after the "
                        f"reservation was created ({row['created_at']})",
                    )

                await db.extend_reservation_expiry(
                    conn,
                    dataset_id=dataset_id,
                    reservation_id=reservation_id,
                    expires_at=expires_after.isoformat(),
                )
                await db.insert_reservation_renewal(
                    conn,
                    dataset_id=dataset_id,
                    reservation_id=reservation_id,
                    renewal_id=payload.renewal_id,
                    extend_seconds=payload.extend_seconds,
                    expires_at_before=row["expires_at"],
                    expires_at_after=expires_after.isoformat(),
                    created_at=now_iso,
                )
                await conn.commit()
            except Exception:
                await conn.rollback()
                raise

            renewal_row = await db.get_reservation_renewal(
                conn, dataset_id, payload.renewal_id
            )
            return _renewal_view_from_row(renewal_row, replay=False), 201


# ------------------------------------------------------------- adjustments --


def _adjustment_view_from_row(
    row: aiosqlite.Row, *, replay: bool
) -> dict[str, Any]:
    return {
        "adjustment_id": row["adjustment_id"],
        "dataset_id": row["dataset_id"],
        "reservation_id": row["reservation_id"],
        "epsilon_before": row["epsilon_before"],
        "delta_before": row["delta_before"],
        "requested_epsilon": row["requested_epsilon"],
        "requested_delta": row["requested_delta"],
        "epsilon_after": row["epsilon_after"],
        "delta_after": row["delta_after"],
        "allowed": row["status"] == "adjusted",
        "status": row["status"],
        "reason": row["reason"],
        "epsilon_cap": row["epsilon_cap"],
        "epsilon_committed_before": row["epsilon_committed_before"],
        "epsilon_committed_after": row["epsilon_committed_after"],
        "epsilon_remaining": max(
            0.0, row["epsilon_cap"] - row["epsilon_committed_after"]
        ),
        "rdp_best_order": row["rdp_best_order"],
        "idempotent_replay": replay,
        "created_at": row["created_at"],
    }


async def adjust_reservation(
    dataset_id: str,
    reservation_id: str,
    payload: ReservationAdjustmentCreate,
) -> tuple[dict[str, Any], int]:
    """Replace a granted, non-expired reservation's epsilon/delta quota.

    The state check, the RDP feasibility projection, the reservation-row
    rewrite and the immutable adjustment record all happen in one
    ``BEGIN IMMEDIATE`` transaction, so an adjustment can never race a
    confirm / release / expiry sweep into an inconsistent state.  The
    committed occupancy is composed from the non-reversed granted requests,
    the *other* non-expired granted reservations and the new quota under the
    existing RDP rules.  When the projection stays within the dataset's
    epsilon cap the live quota is replaced; otherwise the attempt is rejected
    and recorded while the reservation keeps its original epsilon/delta.

    Replaying the same ``(dataset_id, adjustment_id)`` with the same payload
    (same reservation, epsilon and delta) returns the stored first verdict
    with its quota snapshots (HTTP 200); a different payload on that id is a
    409, as is a *new* adjustment on an expired or terminal reservation.  The
    original reservation-creation replay is unaffected: it keeps matching the
    creation-time payload even after a successful adjustment.
    """
    now_iso = utcnow().isoformat()

    async with _write_lock():
        async with db.connect() as conn:
            await db.begin_immediate(conn)
            try:
                # Idempotency key first, so a retry replays the original
                # record even when the reservation has since expired.
                existing = await db.get_reservation_adjustment(
                    conn, dataset_id, payload.adjustment_id
                )
                if existing is not None:
                    same = (
                        existing["reservation_id"] == reservation_id
                        and existing["requested_epsilon"] == payload.epsilon
                        and existing["requested_delta"] == payload.delta
                    )
                    if not same:
                        raise db.AdjustmentConflict(payload.adjustment_id)
                    view = _adjustment_view_from_row(existing, replay=True)
                    await conn.commit()
                    return view, 200

                dataset = await db.require_dataset(conn, dataset_id)
                row = await db.get_reservation(conn, dataset_id, reservation_id)
                if row is None:
                    raise db.ReservationNotFound(reservation_id)
                row = await _refresh_reservation_row(conn, row, now_iso)
                if row["status"] != "granted":
                    raise db.ReservationStateConflict(
                        f"reservation_{row['status']}",
                        f"reservation is {row['status']} and its quota cannot "
                        "be adjusted",
                    )

                # Committed occupancy including this reservation's current
                # live quota; the lazy expiry sweep runs inside the tx.
                entries = await _committed_entries(conn, dataset_id, now_iso)
                _, committed_before, order_before = rdp.evaluate_entries(
                    entries, dataset["delta_cap"]
                )
                committed_before = max(0.0, committed_before)

                # Swap exactly this reservation's entry for the requested new
                # quota and recompose under the same RDP accounting.  RDP
                # composition is order-independent, so position does not
                # matter.  ``entries`` already contains the reservation's
                # current entry (the lazy sweep above only removed expired
                # rows); duplicate (eps, delta) entries from other
                # reservations are left untouched.
                old_quota = (row["epsilon"], row["delta"])
                swapped = list(entries)
                for idx, entry in enumerate(swapped):
                    if entry == old_quota:
                        swapped[idx] = (payload.epsilon, payload.delta)
                        break
                else:  # Defensive: active set and row state diverged.
                    swapped.append((payload.epsilon, payload.delta))
                _, committed_after, order_after = rdp.evaluate_entries(
                    swapped, dataset["delta_cap"]
                )
                committed_after = max(0.0, committed_after)

                # Occupancy with this reservation itself removed, used to
                # refresh the reservation row's epsilon_before snapshot.
                others = list(entries)
                try:
                    others.remove(old_quota)
                except ValueError:
                    others = [e for e in entries if e != old_quota]
                _, without_self, _ = rdp.evaluate_entries(
                    others, dataset["delta_cap"]
                )
                without_self = max(0.0, without_self)

                cap = dataset["epsilon_cap"]
                old_epsilon = row["epsilon"]
                old_delta = row["delta"]
                if committed_after <= cap + settings.epsilon_tolerance:
                    status = "adjusted"
                    reason: Optional[str] = None
                    final_epsilon = payload.epsilon
                    final_delta = payload.delta
                    order_used = order_after
                    await db.adjust_reservation_quota(
                        conn,
                        dataset_id=dataset_id,
                        reservation_id=reservation_id,
                        epsilon=payload.epsilon,
                        delta=payload.delta,
                        epsilon_before=without_self,
                        epsilon_after=committed_after,
                        rdp_best_order=order_after,
                    )
                else:
                    status = "rejected"
                    reason = (
                        f"adjustment rejected: projected epsilon "
                        f"{committed_after:.6g} would surpass cap {cap:.6g} "
                        f"(committed {committed_before:.6g}); reservation "
                        "quota unchanged"
                    )
                    # A rejected adjustment moves nothing.
                    final_epsilon = old_epsilon
                    final_delta = old_delta
                    committed_after = committed_before
                    order_used = order_before

                await db.insert_reservation_adjustment(
                    conn,
                    dataset_id=dataset_id,
                    reservation_id=reservation_id,
                    adjustment_id=payload.adjustment_id,
                    epsilon_before=old_epsilon,
                    delta_before=old_delta,
                    requested_epsilon=payload.epsilon,
                    requested_delta=payload.delta,
                    epsilon_after=final_epsilon,
                    delta_after=final_delta,
                    status=status,
                    reason=reason,
                    epsilon_cap=cap,
                    epsilon_committed_before=committed_before,
                    epsilon_committed_after=committed_after,
                    rdp_best_order=order_used,
                    created_at=now_iso,
                )
                await conn.commit()
            except Exception:
                await conn.rollback()
                raise

            stored = await db.get_reservation_adjustment(
                conn, dataset_id, payload.adjustment_id
            )
            return _adjustment_view_from_row(stored, replay=False), 201


async def get_reservation(dataset_id: str, reservation_id: str) -> dict[str, Any]:
    now_iso = utcnow().isoformat()
    async with db.connect() as conn:
        dataset = await db.require_dataset(conn, dataset_id)
        row = await db.get_reservation(conn, dataset_id, reservation_id)
        if row is None:
            raise db.ReservationNotFound(reservation_id)
        row = await _refresh_reservation_row(conn, row, now_iso)
        if conn.in_transaction:
            await conn.commit()
        return _reservation_view(row, dataset, replay=False)


async def list_reservations(dataset_id: str) -> dict[str, Any]:
    now_iso = utcnow().isoformat()
    async with db.connect() as conn:
        await db.require_dataset(conn, dataset_id)
        # Sweep expired rows before listing.
        await db.list_active_reservation_entries(
            conn, dataset_id, now_iso=now_iso
        )
        if conn.in_transaction:
            await conn.commit()
        rows = await db.list_reservations(conn, dataset_id)
        dataset = await db.require_dataset(conn, dataset_id)
    return {
        "dataset_id": dataset_id,
        "count": len(rows),
        "reservations": [
            _reservation_view(r, dataset, replay=False) for r in rows
        ],
    }


# ---------------------------------------------------------- release forecast --


def _forecast_snapshot_view(snap: dict[str, Any]) -> dict[str, Any]:
    """Public fields of a ``_snapshot`` result (drops the internal curve)."""
    return {
        key: snap[key]
        for key in (
            "epsilon_consumed",
            "epsilon_reserved",
            "epsilon_remaining",
            "rdp_best_order",
            "active_reservations",
        )
    }


async def release_forecast(
    dataset_id: str, deadline: datetime
) -> dict[str, Any]:
    """Read-only simulation of reservation expiries up to ``deadline``.

    Starting from the reservations that are valid *now*, those sharing an
    ``expires_at`` are removed together at that instant and the remaining
    epsilon / active-reservation counts are recomputed with the normal RDP
    composition.  Reservations still valid at the deadline keep counting.
    Nothing is written: in particular the lazy expiry sweep never runs here,
    and the supplied deadline never affects stored reservation state.
    """
    if deadline.tzinfo is None or deadline.utcoffset() is None:
        raise InvalidForecastDeadline(
            "deadline must be a timezone-aware UTC ISO-8601 instant, e.g. "
            "2026-09-23T12:00:00Z"
        )
    deadline = deadline.astimezone(timezone.utc)

    now = utcnow()
    if deadline <= now:
        raise InvalidForecastDeadline("deadline must be a future UTC instant")

    now_iso = now.isoformat()
    deadline_iso = deadline.isoformat()

    async with db.connect() as conn:
        dataset = await db.require_dataset(conn, dataset_id)
        # Pure SELECTs only: list_granted_reservation_rows performs no expiry
        # UPDATE, so this forecast cannot mutate the database.
        granted = await db.list_granted_entries(conn, dataset_id)
        rows = await db.list_granted_reservation_rows(
            conn, dataset_id, now_iso=now_iso
        )
        assert not conn.in_transaction

    cap = dataset["epsilon_cap"]
    target_delta = dataset["delta_cap"]

    def _entries_for(released_ids: set[str]) -> list[tuple[float, float]]:
        return [
            (row["epsilon"], row["delta"])
            for row in rows
            if row["reservation_id"] not in released_ids
        ]

    # Distinct expiry instants up to (and including) the deadline; NULL / later
    # expiries stay locked for the whole horizon.
    expiring: dict[str, list[aiosqlite.Row]] = {}
    for row in rows:
        expires_at = row["expires_at"]
        if expires_at is not None and expires_at <= deadline_iso:
            expiring.setdefault(expires_at, []).append(row)

    initial = _snapshot(granted, _entries_for(set()), cap, target_delta)

    events: list[dict[str, Any]] = []
    released_ids: set[str] = set()
    for expires_at_iso in sorted(expiring):
        group = expiring[expires_at_iso]
        released_ids.update(row["reservation_id"] for row in group)
        snap = _snapshot(granted, _entries_for(released_ids), cap, target_delta)
        events.append(
            {
                "expires_at": datetime.fromisoformat(expires_at_iso),
                "released_count": len(group),
                "released_reservation_ids": [
                    row["reservation_id"] for row in group
                ],
                **_forecast_snapshot_view(snap),
            }
        )

    at_deadline = _snapshot(granted, _entries_for(released_ids), cap, target_delta)
    return {
        "dataset_id": dataset_id,
        "generated_at": now,
        "deadline": deadline,
        "initial": _forecast_snapshot_view(initial),
        "events": events,
        "at_deadline": _forecast_snapshot_view(at_deadline),
    }


# -------------------------------------------------------- selection plans --


async def selection_plan(
    dataset_id: str, candidates: list[SelectionPlanCandidate]
) -> dict[str, Any]:
    """Read-only trial: pick the feasible candidate subset with best value.

    The current ledger occupancy is computed with the exact same RDP
    accounting as admission checks -- granted, non-reversed requests plus
    granted reservations that have not expired *right now* -- but, like the
    release forecast, this path issues pure SELECTs only: it never runs the
    lazy expiry sweep and creates no request, reservation, batch, reversal or
    any other record.  Every call therefore recomputes against the latest
    committed ledger state.

    Every subset of the 1-12 candidates is enumerated (at most 4096) and each
    feasible subset (projected epsilon <= cap + tolerance) competes by:

    1. maximum total ``value``;
    2. then the smaller composed projected epsilon;
    3. then the smaller candidate-id list in ascending lexicographic order.
    """
    now = utcnow()
    now_iso = now.isoformat()

    async with db.connect() as conn:
        dataset = await db.require_dataset(conn, dataset_id)
        # Pure SELECTs only (no lazy expiry UPDATE), so the trial is strictly
        # read-only; reservations past their TTL simply do not qualify.
        granted = await db.list_granted_entries(conn, dataset_id)
        reservation_rows = await db.list_granted_reservation_rows(
            conn, dataset_id, now_iso=now_iso
        )
        assert not conn.in_transaction

    cap = dataset["epsilon_cap"]
    target_delta = dataset["delta_cap"]

    # Decompose the committed occupancy once so each subset projection is a
    # cheap per-order sum instead of recomposing the whole ledger 2**n times.
    committed_entries: list[tuple[float, float]] = granted + [
        (row["epsilon"], row["delta"]) for row in reservation_rows
    ]
    _, committed, _ = rdp.evaluate_entries(
        committed_entries, target_delta
    )
    committed = max(0.0, committed)

    base_pure = sum(eps for eps, d in committed_entries if d == 0.0)
    base_approx_curve = rdp.compose_rdp(
        [
            rdp.single_rdp_curve(eps, d)
            for eps, d in committed_entries
            if d > 0.0
        ]
    )
    has_approx = any(d > 0.0 for _, d in committed_entries)

    # Per-candidate contributions: a flat pure-DP offset and an RDP curve for
    # approximate-DP candidates.
    cand_pure = [
        c.epsilon if c.delta == 0.0 else 0.0 for c in candidates
    ]
    cand_curves = [
        rdp.single_rdp_curve(c.epsilon, c.delta) if c.delta > 0.0 else None
        for c in candidates
    ]
    cand_ids = [c.candidate_id for c in candidates]
    cand_values = [c.value for c in candidates]

    def _project(indices: tuple[int, ...]) -> tuple[float, float]:
        """Composed (epsilon, best_order) if these candidates were granted."""
        pure_sum = base_pure + sum(cand_pure[i] for i in indices)
        approx_here = has_approx or any(
            cand_curves[i] is not None for i in indices
        )
        if not approx_here:
            if pure_sum == 0.0:
                # Mirror evaluate_entries([]): no mechanisms at all.
                return 0.0, rdp.RDP_ORDERS[-1]
            # Exact pure-DP basic composition.
            return pure_sum, rdp.RDP_ORDERS[0]
        composed = [*base_approx_curve]
        for i in indices:
            curve = cand_curves[i]
            if curve is not None:
                for j, part in enumerate(curve):
                    composed[j] += part
        epsilon, order = rdp.rdp_to_approx_dp(composed, target_delta)
        return max(0.0, pure_sum + epsilon), order

    # The empty subset is always feasible; selection starts there.
    best_indices: tuple[int, ...] = ()
    best_value = 0
    best_epsilon, best_order = _project(())
    for mask in range(1, 1 << len(candidates)):
        indices = tuple(i for i in range(len(candidates)) if mask & (1 << i))
        projected, order = _project(indices)
        if projected > cap + settings.epsilon_tolerance:
            continue
        total_value = sum(cand_values[i] for i in indices)
        key_ids = sorted(cand_ids[i] for i in indices)
        if (
            total_value > best_value
            or (
                total_value == best_value
                and projected < best_epsilon - settings.epsilon_tolerance
            )
            or (
                total_value == best_value
                and abs(projected - best_epsilon) <= settings.epsilon_tolerance
                and key_ids < sorted(cand_ids[i] for i in best_indices)
            )
        ):
            best_indices = indices
            best_value = total_value
            best_epsilon, best_order = projected, order

    selected = sorted(cand_ids[i] for i in best_indices)
    selected_set = set(selected)
    unselected = [cid for cid in cand_ids if cid not in selected_set]
    return {
        "dataset_id": dataset_id,
        "generated_at": now,
        "candidate_count": len(candidates),
        "selected_candidate_ids": selected,
        "unselected_candidate_ids": unselected,
        "total_value": best_value,
        "epsilon_cap": cap,
        "epsilon_committed": committed,
        "projected_epsilon": best_epsilon,
        "epsilon_remaining": max(0.0, cap - best_epsilon),
        "rdp_best_order": best_order,
    }


# ------------------------------------------------------------- transfers ---


def _transfer_side_view(
    dataset_id: str, cap_after: float, committed: float
) -> dict[str, Any]:
    return {
        "dataset_id": dataset_id,
        "epsilon_cap": cap_after,
        "epsilon_committed": max(0.0, committed),
        "epsilon_remaining": max(0.0, cap_after - committed),
    }


def _transfer_view_from_row(
    row: aiosqlite.Row, *, replay: bool
) -> dict[str, Any]:
    """Reconstruct the API verdict from a stored transfer record."""
    allowed = row["status"] == "transferred"
    return {
        "transfer_id": row["transfer_id"],
        "source_dataset_id": row["source_dataset_id"],
        "target_dataset_id": row["target_dataset_id"],
        "epsilon_amount": row["epsilon_amount"],
        "allowed": allowed,
        "status": row["status"],
        "reason": row["reason"],
        "source": _transfer_side_view(
            row["source_dataset_id"],
            row["source_epsilon_cap_after"],
            row["source_epsilon_committed"],
        ),
        "target": _transfer_side_view(
            row["target_dataset_id"],
            row["target_epsilon_cap_after"],
            row["target_epsilon_committed"],
        ),
        "idempotent_replay": replay,
        "created_at": row["created_at"],
    }


async def transfer_budget(
    payload: BudgetTransferRequest,
) -> tuple[dict[str, Any], int]:
    """Atomically move epsilon cap from one dataset to another.

    Admission (all inside one ``BEGIN IMMEDIATE`` transaction):

    * source after the debit must stay strictly positive and must still cover
      its granted requests + non-expired reservations under RDP;
    * target after the credit must not exceed the registered ceiling
      :data:`MAX_EPSILON_CAP`;
    * otherwise the whole transfer is rejected and **both caps stay unchanged**
      (the rejected attempt is still recorded for auditing).

    ``delta_cap`` and every already-recorded request/reservation are left
    untouched on both sides.
    """
    now_iso = utcnow().isoformat()

    async with _write_lock():
        async with db.connect() as conn:
            await db.begin_immediate(conn)
            try:
                existing = await db.get_budget_transfer(conn, payload.transfer_id)
                if existing is not None:
                    same = (
                        existing["source_dataset_id"] == payload.source_dataset_id
                        and existing["target_dataset_id"] == payload.target_dataset_id
                        and existing["epsilon_amount"] == payload.epsilon_amount
                    )
                    if not same:
                        raise db.TransferConflict(payload.transfer_id)
                    view = _transfer_view_from_row(existing, replay=True)
                    await conn.commit()
                    return view, 200

                source = await db.require_dataset(conn, payload.source_dataset_id)
                target = await db.require_dataset(conn, payload.target_dataset_id)

                # Occupancy is evaluated with the exact same RDP admission path
                # as ordinary requests/reservations (the lazy reservation sweep
                # runs inside this transaction too).
                source_entries = await _committed_entries(
                    conn, payload.source_dataset_id, now_iso
                )
                _, source_committed, _ = rdp.evaluate_entries(
                    source_entries, source["delta_cap"]
                )
                target_entries = await _committed_entries(
                    conn, payload.target_dataset_id, now_iso
                )
                _, target_committed, _ = rdp.evaluate_entries(
                    target_entries, target["delta_cap"]
                )
                source_committed = max(0.0, source_committed)
                target_committed = max(0.0, target_committed)

                amount = payload.epsilon_amount
                source_cap_before = source["epsilon_cap"]
                target_cap_before = target["epsilon_cap"]
                source_cap_after = source_cap_before - amount
                target_cap_after = target_cap_before + amount

                reasons: list[str] = []
                if source_cap_after <= 0.0:
                    reasons.append(
                        f"source cap must stay positive: {source_cap_before:g} "
                        f"- {amount:g} = {source_cap_after:g}"
                    )
                if source_cap_after < source_committed - settings.epsilon_tolerance:
                    reasons.append(
                        f"source cap {source_cap_after:g} after the debit would "
                        f"not cover its committed epsilon "
                        f"{source_committed:.6g}"
                    )
                if target_cap_after > MAX_EPSILON_CAP + settings.epsilon_tolerance:
                    reasons.append(
                        f"target cap {target_cap_after:g} after the credit would "
                        f"exceed the registered ceiling {MAX_EPSILON_CAP:g}"
                    )

                if not reasons:
                    status = "transferred"
                    reason: Optional[str] = None
                    final_source_cap = source_cap_after
                    final_target_cap = target_cap_after
                    await db.update_dataset_epsilon_cap(
                        conn,
                        dataset_id=payload.source_dataset_id,
                        epsilon_cap=source_cap_after,
                    )
                    await db.update_dataset_epsilon_cap(
                        conn,
                        dataset_id=payload.target_dataset_id,
                        epsilon_cap=target_cap_after,
                    )
                else:
                    status = "rejected"
                    reason = "; ".join(reasons)
                    # Rejected transfer: caps unchanged on both sides.
                    final_source_cap = source_cap_before
                    final_target_cap = target_cap_before

                await db.insert_budget_transfer(
                    conn,
                    transfer_id=payload.transfer_id,
                    source_dataset_id=payload.source_dataset_id,
                    target_dataset_id=payload.target_dataset_id,
                    epsilon_amount=amount,
                    status=status,
                    reason=reason,
                    source_epsilon_cap_before=source_cap_before,
                    source_epsilon_cap_after=final_source_cap,
                    source_epsilon_committed=source_committed,
                    target_epsilon_cap_before=target_cap_before,
                    target_epsilon_cap_after=final_target_cap,
                    target_epsilon_committed=target_committed,
                    created_at=now_iso,
                )
                await conn.commit()
            except Exception:
                await conn.rollback()
                raise

            row = await db.get_budget_transfer(conn, payload.transfer_id)
            return _transfer_view_from_row(row, replay=False), 201


# -------------------------------------------------------- cost attributions --


async def cost_attribution(
    dataset_id: str, payload: CostAttributionRequest
) -> dict[str, Any]:
    """Read-only Shapley attribution of the group increment of selected requests.

    The baseline is composed from every *other* granted, non-reversed request
    plus the granted reservations that have not expired *right now*.  For the
    selected group, every subset is projected on top of the baseline using the
    existing RDP accounting, and each selected request is charged its standard
    Shapley share

        phi_i = sum_{S subseteq N\\{i}} |S|! (n-1-|S|)! / n!
                * ( V(S u {i}) - V(S) )

    where ``V(S)`` is the composed projected epsilon of the baseline plus the
    requests in subset ``S``.  Because RDP composition is additive, V is
    monotone, so every marginal (and hence every share) is non-negative, and
    the shares sum to ``V(N) - V({})`` up to floating-point round-off.

    Like the release forecast and selection plan this path issues pure
    SELECTs only: it never sweeps reservation expiry and never writes any
    record, so repeated calls always recompute against the live ledger.
    """
    now = utcnow()
    now_iso = now.isoformat()
    selected_ids = payload.request_ids

    async with db.connect() as conn:
        dataset = await db.require_dataset(conn, dataset_id)
        # Pure SELECTs only (no lazy expiry UPDATE), so the attribution is
        # strictly read-only; reservations past their TTL simply do not
        # qualify.
        eligible_rows = await db.list_granted_request_rows(conn, dataset_id)
        reservation_rows = await db.list_granted_reservation_rows(
            conn, dataset_id, now_iso=now_iso
        )
        # Fetch the live row (granted/rejected, with or without a reversal)
        # for each selected id so the validation reports 404/409 precisely.
        selected_rows = [
            await db.get_request(conn, dataset_id, request_id)
            for request_id in selected_ids
        ]
        assert not conn.in_transaction

    # Validate the whole selection against the live ledger.  A missing id is
    # 404; a rejected or already-reversed id is 409, reported in request order.
    by_id = {row["request_id"]: row for row in eligible_rows}
    for request_id, row in zip(selected_ids, selected_rows):
        if row is None:
            raise db.RequestNotFound(request_id)
        if row["status"] != "granted":
            raise db.ReversalStateConflict(
                "request_not_granted",
                f"request {request_id!r} is {row['status']} and cannot be "
                "attributed; only granted requests participate",
            )
        if request_id not in by_id:
            # Granted but carrying a reversal row: already reversed.
            raise db.ReversalStateConflict(
                "request_already_reversed",
                f"request {request_id!r} was already reversed and no longer "
                "occupies any budget",
            )

    target_delta = dataset["delta_cap"]
    cap = dataset["epsilon_cap"]

    selected_set = set(selected_ids)
    baseline_entries = [
        (row["epsilon"], row["delta"])
        for row in eligible_rows
        if row["request_id"] not in selected_set
    ]
    baseline_entries.extend(
        (row["epsilon"], row["delta"]) for row in reservation_rows
    )

    # Decompose the baseline composition once so each of the 2**n subset
    # projections is a cheap per-order sum instead of recomposing the ledger.
    base_pure = sum(eps for eps, d in baseline_entries if d == 0.0)
    base_approx_curve = rdp.compose_rdp(
        [
            rdp.single_rdp_curve(eps, d)
            for eps, d in baseline_entries
            if d > 0.0
        ]
    )
    n_orders = len(rdp.RDP_ORDERS)

    selected = [by_id[request_id] for request_id in selected_ids]
    sel_pure = [
        row["epsilon"] if row["delta"] == 0.0 else 0.0 for row in selected
    ]
    sel_curves = [
        rdp.single_rdp_curve(row["epsilon"], row["delta"])
        if row["delta"] > 0.0
        else None
        for row in selected
    ]
    baseline_has_approx = any(d > 0.0 for _, d in baseline_entries)
    n = len(selected)

    def _project(mask: int) -> tuple[float, float]:
        """Composed (epsilon, best_order) for baseline + the subset in mask."""
        pure_sum = base_pure + sum(
            sel_pure[i] for i in range(n) if mask & (1 << i)
        )
        approx_here = baseline_has_approx or any(
            sel_curves[i] is not None for i in range(n) if mask & (1 << i)
        )
        if not approx_here:
            if pure_sum == 0.0:
                # Mirror evaluate_entries([]): no mechanisms at all.
                return 0.0, rdp.RDP_ORDERS[-1]
            # Exact pure-DP basic composition.
            return pure_sum, rdp.RDP_ORDERS[0]
        composed = list(base_approx_curve)
        for i in range(n):
            if mask & (1 << i):
                curve = sel_curves[i]
                if curve is not None:
                    for j in range(n_orders):
                        composed[j] += curve[j]
        epsilon, order = rdp.rdp_to_approx_dp(composed, target_delta)
        return max(0.0, pure_sum + epsilon), order

    # Every coalition value exactly once; Shapley marginals read from this map.
    values: dict[int, float] = {}
    orders: dict[int, float] = {}
    for mask in range(1 << n):
        values[mask], orders[mask] = _project(mask)

    baseline_epsilon = values[0]
    full_epsilon = values[(1 << n) - 1]
    baseline_order = orders[0]
    full_order = orders[(1 << n) - 1]

    attributions: list[dict[str, Any]] = []
    total = 0.0
    full_mask = (1 << n) - 1
    for i, row in enumerate(selected):
        bit = 1 << i
        others = full_mask ^ bit
        share = 0.0
        # Enumerate every subset S of the other n-1 players as submasks.
        sub = others
        while True:
            size = sub.bit_count()
            weight = (
                math.factorial(size)
                * math.factorial(n - 1 - size)
                / math.factorial(n)
            )
            marginal = values[sub | bit] - values[sub]
            share += weight * marginal
            if sub == 0:
                break
            sub = (sub - 1) & others
        share = max(0.0, share)
        total += share
        attributions.append(
            {
                "request_id": row["request_id"],
                "epsilon": row["epsilon"],
                "delta": row["delta"],
                "attributed_epsilon": share,
            }
        )

    increment = max(0.0, full_epsilon - baseline_epsilon)
    return {
        "dataset_id": dataset_id,
        "generated_at": now,
        "request_count": n,
        "request_ids": list(selected_ids),
        "epsilon_cap": cap,
        "epsilon_baseline": baseline_epsilon,
        "epsilon_full_group": full_epsilon,
        "epsilon_increment": increment,
        "rdp_best_order_baseline": baseline_order,
        "rdp_best_order_full_group": full_order,
        "attributed_total_epsilon": total,
        "attributions": attributions,
    }


# --------------------------------------------------------- repeat-run trials --


async def repeat_run_trial(
    dataset_id: str, payload: RepeatRunTrialRequest
) -> dict[str, Any]:
    """Read-only trial: how many complete repetitions of a workload fit.

    The request supplies 1-5 analysis items (each an ``(epsilon, delta)``) and
    ``max_runs`` between 1 and 100000.  The whole item group is treated as one
    workload "run" and composed, with the existing RDP accounting, on top of
    the current ledger occupancy -- granted non-reversed requests plus granted
    reservations that have not expired *right now*.  RDP curves are additive
    per order, so ``runs`` repetitions of the identical group amount to the
    group's pure-DP epsilon offset and its approximate-DP RDP curve both
    multiplied by ``runs``; pure-epsilon items therefore stay on the exact
    addition path for every run count.

    Feasibility (projected epsilon <= cap + tolerance) is monotone in the run
    count, so the largest feasible count in ``[0, max_runs]`` is found by
    binary search.  The answer is also valid when zero runs fit.

    Like the release forecast, selection plan and cost attribution, this path
    issues pure SELECTs only: it never sweeps reservation expiry, creates no
    request, reservation, batch, reversal or any other record, and each call
    recomputes against the latest committed ledger state.
    """
    now = utcnow()
    now_iso = now.isoformat()

    async with db.connect() as conn:
        dataset = await db.require_dataset(conn, dataset_id)
        # Pure SELECTs only (no lazy expiry UPDATE), so the trial is strictly
        # read-only; reservations past their TTL simply do not qualify.
        granted = await db.list_granted_entries(conn, dataset_id)
        reservation_rows = await db.list_granted_reservation_rows(
            conn, dataset_id, now_iso=now_iso
        )
        assert not conn.in_transaction

    cap = dataset["epsilon_cap"]
    target_delta = dataset["delta_cap"]

    committed_entries: list[tuple[float, float]] = granted + [
        (row["epsilon"], row["delta"]) for row in reservation_rows
    ]

    # Decompose the current ledger occupancy once, exactly like the selection
    # plan: a flat pure-DP offset plus one composed approximate-DP RDP curve.
    base_pure = sum(eps for eps, d in committed_entries if d == 0.0)
    base_approx_curve = rdp.compose_rdp(
        [
            rdp.single_rdp_curve(eps, d)
            for eps, d in committed_entries
            if d > 0.0
        ]
    )
    base_has_approx = any(d > 0.0 for _, d in committed_entries)

    group_entries = [(item.epsilon, item.delta) for item in payload.items]
    group_pure = sum(eps for eps, d in group_entries if d == 0.0)
    group_approx_curve = rdp.compose_rdp(
        [
            rdp.single_rdp_curve(eps, d)
            for eps, d in group_entries
            if d > 0.0
        ]
    )
    group_has_approx = any(d > 0.0 for _, d in group_entries)
    n_orders = len(rdp.RDP_ORDERS)

    def _project(runs: int) -> tuple[float, float]:
        """Composed (epsilon, best_order) after ``runs`` identical group runs."""
        pure_sum = base_pure + runs * group_pure
        if not (base_has_approx or (group_has_approx and runs > 0)):
            if pure_sum == 0.0:
                # Mirror evaluate_entries([]): no mechanisms at all.
                return 0.0, rdp.RDP_ORDERS[-1]
            # Exact pure-DP basic composition.
            return pure_sum, rdp.RDP_ORDERS[0]
        composed = list(base_approx_curve)
        if runs > 0:
            for j in range(n_orders):
                composed[j] += runs * group_approx_curve[j]
        epsilon, order = rdp.rdp_to_approx_dp(composed, target_delta)
        return max(0.0, pure_sum + epsilon), order

    def _feasible(runs: int) -> bool:
        epsilon, _ = _project(runs)
        return epsilon <= cap + settings.epsilon_tolerance

    committed, _ = _project(0)

    # Feasibility is monotone non-increasing in the run count, so binary-search
    # the largest feasible count in [0, max_runs].
    lo, hi = 0, payload.max_runs
    while lo < hi:
        # Upper midpoint: advance lo while the midpoint is feasible.
        mid = (lo + hi + 1) // 2
        if _feasible(mid):
            lo = mid
        else:
            hi = mid - 1
    max_runs_feasible = lo

    def _view(runs: int) -> dict[str, Any]:
        epsilon, order = _project(runs)
        return {
            "runs": runs,
            "epsilon_projected": epsilon,
            "epsilon_remaining": max(0.0, cap - epsilon),
            "rdp_best_order": order,
            "feasible": epsilon <= cap + settings.epsilon_tolerance,
        }

    at_max_runs = max_runs_feasible >= payload.max_runs
    return {
        "dataset_id": dataset_id,
        "generated_at": now,
        "item_count": len(payload.items),
        "items": [item.model_dump() for item in payload.items],
        "max_runs": payload.max_runs,
        "epsilon_cap": cap,
        "epsilon_committed": committed,
        "max_complete_runs": max_runs_feasible,
        "at_max_runs": at_max_runs,
        "projection": _view(max_runs_feasible),
        # No run beyond the supplied bound is evaluated once it is reached;
        # otherwise (also when zero runs fit) report one-more-run occupancy.
        "next_run_projection": None
        if at_max_runs
        else _view(max_runs_feasible + 1),
    }


# ------------------------------------------------------------ reversal plans --


async def reversal_plan(
    dataset_id: str, payload: ReversalPlanRequest
) -> dict[str, Any]:
    """Read-only trial: the smallest reversal subset admitting a pending item.

    The baseline is composed from every *other* granted, non-reversed request
    plus the granted reservations that have not expired *right now*; the 1-12
    candidate requests (each granted and non-reversed) are the only requests
    that may be reversed.  Every reversal subset of the candidates is
    enumerated and the pending analysis item is projected on top of the
    baseline plus the candidates that would remain, using the exact same RDP
    accounting as admission checks.  A subset is feasible when that
    projection stays within ``epsilon_cap + EPSILON_TOLERANCE``.

    Among the feasible subsets the plan picks, in order:

    1. the fewest reversed requests;
    2. then the largest post-reversal ledger occupancy (the reversal that
       frees the least budget);
    3. then the lexicographically smallest ascending request_id list.

    The empty subset competes too, so a pending item that already fits is
    answered with zero suggested reversals.  When no subset -- not even
    reversing every candidate -- fits, the plan is explicitly infeasible.

    Like the release forecast, selection plan, cost attribution and
    repeat-run trial this path issues pure SELECTs only: it never sweeps
    reservation expiry, never reverses or submits anything and creates no
    record of any kind, so every call recomputes against the live ledger.
    """
    now = utcnow()
    now_iso = now.isoformat()
    selected_ids = payload.request_ids

    async with db.connect() as conn:
        dataset = await db.require_dataset(conn, dataset_id)
        # Pure SELECTs only (no lazy expiry UPDATE), so the trial is strictly
        # read-only; reservations past their TTL simply do not qualify.
        eligible_rows = await db.list_granted_request_rows(conn, dataset_id)
        reservation_rows = await db.list_granted_reservation_rows(
            conn, dataset_id, now_iso=now_iso
        )
        # Fetch the live row (granted/rejected, with or without a reversal)
        # for each candidate id so the validation reports 404/409 precisely.
        selected_rows = [
            await db.get_request(conn, dataset_id, request_id)
            for request_id in selected_ids
        ]
        assert not conn.in_transaction

    # Validate the whole candidate set against the live ledger.  A missing id
    # is 404; a rejected or already-reversed id is 409, reported in request
    # order.
    by_id = {row["request_id"]: row for row in eligible_rows}
    for request_id, row in zip(selected_ids, selected_rows):
        if row is None:
            raise db.RequestNotFound(request_id)
        if row["status"] != "granted":
            raise db.ReversalStateConflict(
                "request_not_granted",
                f"request {request_id!r} is {row['status']} and cannot be "
                "reversed; only granted requests are reversal candidates",
            )
        if request_id not in by_id:
            # Granted but carrying a reversal row: already reversed.
            raise db.ReversalStateConflict(
                "request_already_reversed",
                f"request {request_id!r} was already reversed and no longer "
                "occupies any budget",
            )

    cap = dataset["epsilon_cap"]
    target_delta = dataset["delta_cap"]

    selected_set = set(selected_ids)
    baseline_entries = [
        (row["epsilon"], row["delta"])
        for row in eligible_rows
        if row["request_id"] not in selected_set
    ]
    baseline_entries.extend(
        (row["epsilon"], row["delta"]) for row in reservation_rows
    )

    # Decompose the fixed baseline once so each of the 2**n subset projections
    # is a cheap per-order sum instead of recomposing the whole ledger.
    base_pure = sum(eps for eps, d in baseline_entries if d == 0.0)
    base_approx_curve = rdp.compose_rdp(
        [
            rdp.single_rdp_curve(eps, d)
            for eps, d in baseline_entries
            if d > 0.0
        ]
    )
    baseline_has_approx = any(d > 0.0 for _, d in baseline_entries)
    n_orders = len(rdp.RDP_ORDERS)

    selected = [by_id[request_id] for request_id in selected_ids]
    cand_ids = [row["request_id"] for row in selected]
    cand_pure = [
        row["epsilon"] if row["delta"] == 0.0 else 0.0 for row in selected
    ]
    cand_curves = [
        rdp.single_rdp_curve(row["epsilon"], row["delta"])
        if row["delta"] > 0.0
        else None
        for row in selected
    ]

    item = payload.item
    pending_pure = item.epsilon if item.delta == 0.0 else 0.0
    pending_curve = (
        rdp.single_rdp_curve(item.epsilon, item.delta)
        if item.delta > 0.0
        else None
    )

    n = len(selected)
    full_mask = (1 << n) - 1

    def _project(kept_mask: int, include_pending: bool) -> tuple[float, float]:
        """Composed (epsilon, best_order) of baseline + kept candidates.

        ``kept_mask`` selects the candidates that remain granted (i.e. are
        *not* reversed); ``include_pending`` additionally composes the
        pending analysis item on top.
        """
        pure_sum = base_pure
        approx_here = baseline_has_approx
        composed = list(base_approx_curve)
        for i in range(n):
            if not kept_mask & (1 << i):
                continue
            pure_sum += cand_pure[i]
            curve = cand_curves[i]
            if curve is not None:
                approx_here = True
                for j in range(n_orders):
                    composed[j] += curve[j]
        if include_pending:
            pure_sum += pending_pure
            if pending_curve is not None:
                approx_here = True
                for j in range(n_orders):
                    composed[j] += pending_curve[j]
        if not approx_here:
            if pure_sum == 0.0:
                # Mirror evaluate_entries([]): no mechanisms at all.
                return 0.0, rdp.RDP_ORDERS[-1]
            # Exact pure-DP basic composition.
            return pure_sum, rdp.RDP_ORDERS[0]
        epsilon, order = rdp.rdp_to_approx_dp(composed, target_delta)
        return max(0.0, pure_sum + epsilon), order

    # Occupancy before any reversal: baseline + every candidate (all still
    # granted), i.e. the live ledger occupancy.
    committed, committed_order = _project(full_mask, include_pending=False)

    best_mask: Optional[int] = None  # bitmask of the reversed candidates
    best_count = 0
    best_after = 0.0
    best_after_order = rdp.RDP_ORDERS[-1]
    best_with = 0.0
    best_with_order = rdp.RDP_ORDERS[-1]
    best_ids: list[str] = []
    for mask in range(1 << n):
        kept_mask = full_mask ^ mask
        with_epsilon, with_order = _project(kept_mask, include_pending=True)
        if with_epsilon > cap + settings.epsilon_tolerance:
            continue
        after_epsilon, after_order = _project(
            kept_mask, include_pending=False
        )
        count = mask.bit_count()
        ids = sorted(cand_ids[i] for i in range(n) if mask & (1 << i))
        if (
            best_mask is None
            or count < best_count
            or (
                count == best_count
                and after_epsilon > best_after + settings.epsilon_tolerance
            )
            or (
                count == best_count
                and abs(after_epsilon - best_after)
                <= settings.epsilon_tolerance
                and ids < best_ids
            )
        ):
            best_mask = mask
            best_count = count
            best_after, best_after_order = after_epsilon, after_order
            best_with, best_with_order = with_epsilon, with_order
            best_ids = ids

    feasible = best_mask is not None
    if feasible:
        reason: Optional[str] = None
        remaining: Optional[float] = max(0.0, cap - best_with)
        reversal_count: Optional[int] = best_count
        after_value: Optional[float] = best_after
        with_value: Optional[float] = best_with
        after_order_value: Optional[float] = best_after_order
        with_order_value: Optional[float] = best_with_order
    else:
        reason = (
            "no feasible reversal subset: even reversing every candidate "
            f"leaves the pending item above epsilon_cap {cap:g}"
        )
        remaining = None
        reversal_count = None
        after_value = None
        with_value = None
        after_order_value = None
        with_order_value = None

    return {
        "dataset_id": dataset_id,
        "generated_at": now,
        "item": {"epsilon": item.epsilon, "delta": item.delta},
        "candidate_count": n,
        "candidate_request_ids": list(selected_ids),
        "epsilon_cap": cap,
        "epsilon_committed": committed,
        "rdp_best_order_committed": committed_order,
        "feasible": feasible,
        "reason": reason,
        "suggested_request_ids": best_ids if feasible else [],
        "reversal_count": reversal_count,
        "epsilon_after_reversal": after_value,
        "epsilon_with_pending": with_value,
        "epsilon_remaining": remaining,
        "rdp_best_order_after_reversal": after_order_value,
        "rdp_best_order_with_pending": with_order_value,
    }
