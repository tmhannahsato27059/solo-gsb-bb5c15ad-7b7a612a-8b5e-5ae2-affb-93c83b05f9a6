"""End-to-end tests for the read-only reversal-plan trial endpoint.

POST /datasets/{dataset_id}/reversal-plans
"""
from __future__ import annotations

import itertools
import os
import tempfile
from datetime import datetime, timedelta, timezone

import pytest
import pytest_asyncio

# Point the ledger at an isolated database before importing the app.
_tmp = tempfile.mkdtemp(prefix="dpledger-test-reversal-plan-")
os.environ["DATABASE_PATH"] = os.path.join(_tmp, "reversal-plan.db")

from httpx import ASGITransport, AsyncClient  # noqa: E402

from app import db, rdp  # noqa: E402
from app.main import app  # noqa: E402

_counter = itertools.count()
_rev_counter = itertools.count()

TOL = 1e-12


def _new_dataset_id(name: str) -> str:
    return f"dsrp-{name}-{next(_counter)}"


def _new_reversal_id() -> str:
    return f"rev-rp-{next(_rev_counter)}"


@pytest_asyncio.fixture()
async def client():
    await db.init_db()
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac


async def _register(client, dataset_id: str, eps: float = 1.0, delta: float = 1e-6):
    return await client.post(
        f"/datasets/{dataset_id}",
        json={"epsilon_cap": eps, "delta_cap": delta,
              "description": "test dataset"},
    )


async def _grant(client, ds: str, request_id: str, epsilon: float,
                 delta: float = 1e-7, query_type: str = "q"):
    return await client.post(
        f"/datasets/{ds}/requests",
        json={"request_id": request_id, "query_type": query_type,
              "epsilon": epsilon, "delta": delta},
    )


async def _plan(client, ds: str, item: dict, request_ids: list[str]):
    return await client.post(
        f"/datasets/{ds}/reversal-plans",
        json={"item": item, "request_ids": request_ids},
    )


async def _backdate(dataset_id: str, reservation_id: str,
                    expires_at: datetime) -> None:
    async with db.connect() as conn:
        await db.backdate_reservation_expiry(
            conn,
            dataset_id=dataset_id,
            reservation_id=reservation_id,
            expires_at=expires_at.isoformat(),
        )
        await conn.commit()


def _independent_plan(
    baseline: list[tuple[float, float]],
    candidates: list[tuple[str, float, float]],
    item: tuple[float, float],
    cap: float,
    target_delta: float,
    tol: float = TOL,
):
    """Brute-force the expected plan directly with rdp.evaluate_entries.

    Returns ``(suggested_ids, committed, after, with_pending)`` where the
    occupancy triple is ``None`` when no reversal subset is feasible.
    """
    n = len(candidates)

    def project(kept: tuple[int, ...], include_pending: bool) -> float:
        entries = baseline + [
            (candidates[i][1], candidates[i][2]) for i in kept
        ]
        if include_pending:
            entries = entries + [item]
        return max(0.0, rdp.evaluate_entries(entries, target_delta)[1])

    committed = project(tuple(range(n)), include_pending=False)
    best = None
    for mask in range(1 << n):
        kept = tuple(i for i in range(n) if not mask & (1 << i))
        with_pending = project(kept, include_pending=True)
        if with_pending > cap + tol:
            continue
        after = project(kept, include_pending=False)
        count = mask.bit_count()
        ids = sorted(
            candidates[i][0] for i in range(n) if mask & (1 << i)
        )
        if (
            best is None
            or count < best[0]
            or (count == best[0] and after > best[1] + tol)
            or (
                count == best[0]
                and abs(after - best[1]) <= tol
                and ids < best[2]
            )
        ):
            best = (count, after, ids, with_pending)
    if best is None:
        return None, committed, None, None
    return best[2], committed, best[1], best[3]


# ----------------------------------------------------------------- behaviour --


@pytest.mark.asyncio
async def test_pending_item_fitting_as_is_needs_no_reversal(client):
    ds = _new_dataset_id("fits")
    await _register(client, ds, eps=1.0, delta=1e-6)
    await _grant(client, ds, "a", 0.1, delta=0.0)
    await _grant(client, ds, "b", 0.2, delta=0.0)

    r = await _plan(client, ds, {"epsilon": 0.3, "delta": 0.0}, ["a", "b"])
    assert r.status_code == 200
    body = r.json()
    assert body["feasible"] is True
    assert body["reason"] is None
    assert body["suggested_request_ids"] == []
    assert body["reversal_count"] == 0
    assert body["epsilon_committed"] == pytest.approx(0.3, abs=TOL)
    # No reversal: the ledger keeps every candidate.
    assert body["epsilon_after_reversal"] == pytest.approx(0.3, abs=TOL)
    assert body["epsilon_with_pending"] == pytest.approx(0.6, abs=TOL)
    assert body["epsilon_remaining"] == pytest.approx(0.4, abs=TOL)
    assert body["item"] == {"epsilon": 0.3, "delta": 0.0}
    assert body["candidate_count"] == 2
    assert body["candidate_request_ids"] == ["a", "b"]


@pytest.mark.asyncio
async def test_fewest_reversals_wins(client):
    ds = _new_dataset_id("fewest")
    await _register(client, ds, eps=1.0, delta=1e-6)
    await _grant(client, ds, "a", 0.2, delta=0.0)
    await _grant(client, ds, "b", 0.2, delta=0.0)
    await _grant(client, ds, "c", 0.2, delta=0.0)

    # 0.6 committed; pending 0.55 does not fit (1.15 > 1.0) but fits after
    # reversing any single request (0.4 + 0.55 = 0.95).
    body = (await _plan(
        client, ds, {"epsilon": 0.55, "delta": 0.0}, ["a", "b", "c"]
    )).json()
    assert body["feasible"] is True
    assert body["reversal_count"] == 1
    assert len(body["suggested_request_ids"]) == 1
    assert body["epsilon_after_reversal"] == pytest.approx(0.4, abs=TOL)
    assert body["epsilon_with_pending"] == pytest.approx(0.95, abs=TOL)


@pytest.mark.asyncio
async def test_same_count_picks_largest_post_reversal_occupancy(client):
    ds = _new_dataset_id("maxkeep")
    await _register(client, ds, eps=1.0, delta=1e-6)
    await _grant(client, ds, "small", 0.1, delta=0.0)
    await _grant(client, ds, "large", 0.5, delta=0.0)

    # Pending 0.45 fits after reversing either one; reversing "small" keeps
    # the larger post-reversal occupancy (0.5 vs 0.1).
    body = (await _plan(
        client, ds, {"epsilon": 0.45, "delta": 0.0}, ["small", "large"]
    )).json()
    assert body["feasible"] is True
    assert body["suggested_request_ids"] == ["small"]
    assert body["reversal_count"] == 1
    assert body["epsilon_after_reversal"] == pytest.approx(0.5, abs=TOL)
    assert body["epsilon_with_pending"] == pytest.approx(0.95, abs=TOL)


@pytest.mark.asyncio
async def test_full_tie_picks_lexicographically_smallest_id_list(client):
    ds = _new_dataset_id("lextie")
    await _register(client, ds, eps=1.0, delta=1e-6)
    # Identical (epsilon, delta): every single reversal frees the same amount.
    await _grant(client, ds, "zz", 0.1, delta=0.0)
    await _grant(client, ds, "aa", 0.1, delta=0.0)
    await _grant(client, ds, "mm", 0.1, delta=0.0)

    # 0.3 committed; pending 0.75 needs exactly one reversal (0.2 + 0.75 =
    # 0.95 <= 1.0) and every single reversal frees the same amount.
    body = (await _plan(
        client, ds, {"epsilon": 0.75, "delta": 0.0}, ["zz", "aa", "mm"]
    )).json()
    assert body["feasible"] is True
    assert body["reversal_count"] == 1
    assert body["suggested_request_ids"] == ["aa"]


@pytest.mark.asyncio
async def test_multiple_reversals_needed(client):
    ds = _new_dataset_id("multi")
    await _register(client, ds, eps=1.0, delta=1e-6)
    for rid in ("a", "b", "c"):
        await _grant(client, ds, rid, 0.2, delta=0.0)

    # Pending 0.7: one reversal leaves 0.4 + 0.7 = 1.1 > 1.0; two leave 0.9.
    body = (await _plan(
        client, ds, {"epsilon": 0.7, "delta": 0.0}, ["a", "b", "c"]
    )).json()
    assert body["feasible"] is True
    assert body["reversal_count"] == 2
    assert body["suggested_request_ids"] == ["a", "b"]
    assert body["epsilon_after_reversal"] == pytest.approx(0.2, abs=TOL)
    assert body["epsilon_with_pending"] == pytest.approx(0.9, abs=TOL)


@pytest.mark.asyncio
async def test_reversing_every_candidate_is_considered(client):
    ds = _new_dataset_id("all")
    await _register(client, ds, eps=1.0, delta=1e-6)
    await _grant(client, ds, "a", 0.4, delta=0.0)
    await _grant(client, ds, "b", 0.4, delta=0.0)

    body = (await _plan(
        client, ds, {"epsilon": 0.95, "delta": 0.0}, ["a", "b"]
    )).json()
    assert body["feasible"] is True
    assert body["suggested_request_ids"] == ["a", "b"]
    assert body["reversal_count"] == 2
    assert body["epsilon_after_reversal"] == pytest.approx(0.0, abs=TOL)
    assert body["epsilon_with_pending"] == pytest.approx(0.95, abs=TOL)


@pytest.mark.asyncio
async def test_infeasible_when_nothing_helps(client):
    ds = _new_dataset_id("infeasible")
    await _register(client, ds, eps=1.0, delta=1e-6)
    await _grant(client, ds, "a", 0.1, delta=0.0)
    await _grant(client, ds, "b", 0.2, delta=0.0)

    r = await _plan(client, ds, {"epsilon": 5.0, "delta": 0.0}, ["a", "b"])
    assert r.status_code == 200
    body = r.json()
    assert body["feasible"] is False
    assert body["reason"]
    assert body["suggested_request_ids"] == []
    assert body["reversal_count"] is None
    assert body["epsilon_after_reversal"] is None
    assert body["epsilon_with_pending"] is None
    assert body["epsilon_remaining"] is None
    assert body["rdp_best_order_after_reversal"] is None
    assert body["rdp_best_order_with_pending"] is None
    # The before-reversal occupancy is still reported.
    assert body["epsilon_committed"] == pytest.approx(0.3, abs=TOL)


@pytest.mark.asyncio
async def test_baseline_includes_other_requests_and_active_reservations(client):
    ds = _new_dataset_id("baseline")
    target_delta = 1e-6
    await _register(client, ds, eps=2.0, delta=target_delta)
    # Baseline entries that may not be reversed: another granted request and
    # a live reservation.
    await _grant(client, ds, "other", 0.1, 1e-8)
    await client.post(
        f"/datasets/{ds}/reservations",
        json={"reservation_id": "res1", "epsilon": 0.3, "delta": 1e-8,
              "ttl_seconds": 3600},
    )
    await _grant(client, ds, "a", 0.15, 1e-8)
    await _grant(client, ds, "b", 0.25, 1e-9)

    item = {"epsilon": 0.12, "delta": 1e-8}
    body = (await _plan(client, ds, item, ["a", "b"])).json()
    baseline = [(0.1, 1e-8), (0.3, 1e-8)]
    candidates = [("a", 0.15, 1e-8), ("b", 0.25, 1e-9)]
    ids, committed, after, with_pending = _independent_plan(
        baseline, candidates, (0.12, 1e-8), 2.0, target_delta
    )
    assert body["suggested_request_ids"] == ids
    assert body["epsilon_committed"] == pytest.approx(committed, abs=TOL)
    assert body["epsilon_after_reversal"] == pytest.approx(after, abs=TOL)
    assert body["epsilon_with_pending"] == pytest.approx(with_pending, abs=TOL)

    # The before-reversal occupancy equals the live budget's committed value.
    budget = (await client.get(f"/datasets/{ds}/budget")).json()
    live_committed = budget["epsilon_consumed"] + budget["epsilon_reserved"]
    assert body["epsilon_committed"] == pytest.approx(live_committed, abs=TOL)


@pytest.mark.asyncio
async def test_matches_independent_brute_force_mixed_dp(client):
    ds = _new_dataset_id("brute")
    target_delta = 1e-6
    await _register(client, ds, eps=0.9, delta=target_delta)
    await _grant(client, ds, "base", 0.07, 1e-9)
    specs = [
        ("c1", 0.2, 1e-8),
        ("c2", 0.05, 0.0),
        ("c3", 0.3, 1e-7),
        ("c4", 0.1, 1e-10),
    ]
    for rid, e, d in specs:
        await _grant(client, ds, rid, e, d)

    item = (0.18, 1e-8)
    body = (await _plan(
        client, ds, {"epsilon": item[0], "delta": item[1]},
        [s[0] for s in specs],
    )).json()
    ids, committed, after, with_pending = _independent_plan(
        [(0.07, 1e-9)], specs, item, 0.9, target_delta
    )
    assert body["feasible"] is True
    assert body["suggested_request_ids"] == ids
    assert body["reversal_count"] == len(ids)
    assert body["epsilon_committed"] == pytest.approx(committed, abs=TOL)
    assert body["epsilon_after_reversal"] == pytest.approx(after, abs=TOL)
    assert body["epsilon_with_pending"] == pytest.approx(with_pending, abs=TOL)
    assert body["epsilon_remaining"] == pytest.approx(
        max(0.0, 0.9 - with_pending), abs=TOL
    )


@pytest.mark.asyncio
async def test_pure_dp_planning_is_exactly_additive(client):
    ds = _new_dataset_id("pure")
    await _register(client, ds, eps=1.0, delta=1e-6)
    await _grant(client, ds, "a", 0.25, delta=0.0)
    await _grant(client, ds, "b", 0.35, delta=0.0)

    # Pure epsilon composition: 0.6 committed, pending 0.5 needs one reversal;
    # reversing "a" keeps 0.35 + 0.5 = 0.85, reversing "b" keeps 0.75.
    body = (await _plan(
        client, ds, {"epsilon": 0.5, "delta": 0.0}, ["a", "b"]
    )).json()
    assert body["suggested_request_ids"] == ["a"]
    assert body["epsilon_after_reversal"] == pytest.approx(0.35, abs=TOL)
    assert body["epsilon_with_pending"] == pytest.approx(0.85, abs=TOL)
    assert body["rdp_best_order_with_pending"] == rdp.RDP_ORDERS[0]


@pytest.mark.asyncio
async def test_expired_reservation_excluded_without_being_swept(client):
    ds = _new_dataset_id("expsweep")
    await _register(client, ds, eps=1.0, delta=1e-6)
    await _grant(client, ds, "a", 0.2, delta=0.0)
    await client.post(
        f"/datasets/{ds}/reservations",
        json={"reservation_id": "exp", "epsilon": 0.5, "delta": 0.0,
              "ttl_seconds": 3600},
    )
    # Backdating directly does not sweep: no budget-touching endpoint runs
    # between the backdate and the read-only plan.
    await _backdate(ds, "exp", datetime.now(timezone.utc) - timedelta(seconds=5))

    # The past-TTL reservation does not count: 0.2 committed, pending 0.7
    # fits as-is.
    body = (await _plan(client, ds, {"epsilon": 0.7, "delta": 0.0}, ["a"])).json()
    assert body["feasible"] is True
    assert body["suggested_request_ids"] == []
    assert body["epsilon_committed"] == pytest.approx(0.2, abs=TOL)
    # ...but the read-only plan must not sweep it itself.
    async with db.connect() as conn:
        row = await db.get_reservation(conn, ds, "exp")
        assert row["status"] == "granted"
        assert row["expired_at"] is None
    again = (await _plan(client, ds, {"epsilon": 0.7, "delta": 0.0}, ["a"])).json()
    assert again["epsilon_committed"] == pytest.approx(0.2, abs=TOL)
    async with db.connect() as conn:
        row = await db.get_reservation(conn, ds, "exp")
        assert row["status"] == "granted"


@pytest.mark.asyncio
async def test_already_reversed_requests_stay_out_of_baseline(client):
    ds = _new_dataset_id("revbase")
    await _register(client, ds, eps=1.0, delta=1e-6)
    await _grant(client, ds, "gone", 0.6, delta=0.0)
    await client.post(
        f"/datasets/{ds}/requests/gone/reversals",
        json={"reversal_id": _new_reversal_id(), "reason": "cleanup"},
    )
    await _grant(client, ds, "a", 0.2, delta=0.0)

    # The reversed request occupies nothing: pending 0.7 fits as-is.
    body = (await _plan(client, ds, {"epsilon": 0.7, "delta": 0.0}, ["a"])).json()
    assert body["feasible"] is True
    assert body["suggested_request_ids"] == []
    assert body["epsilon_committed"] == pytest.approx(0.2, abs=TOL)


@pytest.mark.asyncio
async def test_plan_is_strictly_read_only(client):
    ds = _new_dataset_id("readonly")
    await _register(client, ds, eps=1.0, delta=1e-6)
    await _grant(client, ds, "a", 0.2, delta=0.0)
    await _grant(client, ds, "b", 0.3, delta=0.0)

    first = (await _plan(client, ds, {"epsilon": 0.6, "delta": 0.0}, ["a", "b"])).json()
    second = (await _plan(client, ds, {"epsilon": 0.6, "delta": 0.0}, ["a", "b"])).json()
    for key in (
        "item", "candidate_count", "candidate_request_ids", "epsilon_cap",
        "epsilon_committed", "rdp_best_order_committed", "feasible", "reason",
        "suggested_request_ids", "reversal_count", "epsilon_after_reversal",
        "epsilon_with_pending", "epsilon_remaining",
        "rdp_best_order_after_reversal", "rdp_best_order_with_pending",
    ):
        assert first[key] == second[key]

    # No new records of any kind, and nothing was reversed or submitted.
    assert (await client.get(f"/datasets/{ds}/requests")).json()["count"] == 2
    assert (
        await client.get(f"/datasets/{ds}/reservations")
    ).json()["count"] == 0
    assert (await client.get(f"/datasets/{ds}/reversals")).json()["count"] == 0
    budget = (await client.get(f"/datasets/{ds}/budget")).json()
    assert budget["granted_requests"] == 2
    assert budget["epsilon_consumed"] == pytest.approx(0.5, abs=TOL)


@pytest.mark.asyncio
async def test_plan_recomputes_against_live_ledger(client):
    ds = _new_dataset_id("live")
    await _register(client, ds, eps=1.0, delta=1e-6)
    await _grant(client, ds, "a", 0.2, delta=0.0)

    before = (await _plan(client, ds, {"epsilon": 0.7, "delta": 0.0}, ["a"])).json()
    assert before["suggested_request_ids"] == []

    # A newly granted request joins the baseline on the next call, so the
    # pending item no longer fits without reversing "a".
    await _grant(client, ds, "later", 0.2, delta=0.0)
    after = (await _plan(client, ds, {"epsilon": 0.7, "delta": 0.0}, ["a"])).json()
    assert after["suggested_request_ids"] == ["a"]
    assert after["epsilon_committed"] == pytest.approx(0.4, abs=TOL)


@pytest.mark.asyncio
async def test_twelve_candidate_boundary(client):
    ds = _new_dataset_id("twelve")
    await _register(client, ds, eps=10.0, delta=1e-6)
    ids = [f"r{i:02d}" for i in range(12)]
    for rid in ids:
        await _grant(client, ds, rid, 0.05, delta=0.0)

    r = await _plan(client, ds, {"epsilon": 0.5, "delta": 0.0}, ids)
    assert r.status_code == 200
    body = r.json()
    assert body["candidate_count"] == 12
    assert body["feasible"] is True
    assert body["suggested_request_ids"] == []
    assert body["epsilon_committed"] == pytest.approx(0.6, abs=TOL)
    assert body["epsilon_with_pending"] == pytest.approx(1.1, abs=TOL)


# ---------------------------------------------------------------- validation --


@pytest.mark.asyncio
async def test_missing_dataset_404(client):
    r = await _plan(client, "no-such-dataset", {"epsilon": 0.1, "delta": 0.0}, ["a"])
    assert r.status_code == 404
    assert r.json()["error"] == "dataset_not_found"


@pytest.mark.asyncio
async def test_missing_request_404(client):
    ds = _new_dataset_id("missing")
    await _register(client, ds)
    r = await _plan(client, ds, {"epsilon": 0.1, "delta": 0.0}, ["nope"])
    assert r.status_code == 404
    assert r.json()["error"] == "request_not_found"
    assert r.json()["request_id"] == "nope"


@pytest.mark.asyncio
async def test_rejected_candidate_409(client):
    ds = _new_dataset_id("rejected")
    await _register(client, ds, eps=0.01, delta=1e-6)
    await client.post(
        f"/datasets/{ds}/requests",
        json={"request_id": "rej", "query_type": "q",
              "epsilon": 5.0, "delta": 1e-8},
    )
    r = await _plan(client, ds, {"epsilon": 0.1, "delta": 0.0}, ["rej"])
    assert r.status_code == 409
    assert r.json()["error"] == "request_not_granted"


@pytest.mark.asyncio
async def test_already_reversed_candidate_409(client):
    ds = _new_dataset_id("reversed")
    await _register(client, ds)
    await _grant(client, ds, "a", 0.1, 1e-8)
    await client.post(
        f"/datasets/{ds}/requests/a/reversals",
        json={"reversal_id": _new_reversal_id(), "reason": "x"},
    )
    r = await _plan(client, ds, {"epsilon": 0.1, "delta": 0.0}, ["a"])
    assert r.status_code == 409
    assert r.json()["error"] == "request_already_reversed"


@pytest.mark.asyncio
async def test_first_invalid_candidate_reported_in_order(client):
    ds = _new_dataset_id("ordering")
    await _register(client, ds)
    await _grant(client, ds, "ok", 0.1, 1e-8)
    r = await _plan(
        client, ds, {"epsilon": 0.1, "delta": 0.0}, ["ok", "gone", "also-gone"]
    )
    assert r.status_code == 404
    assert r.json()["request_id"] == "gone"


@pytest.mark.asyncio
async def test_duplicate_request_ids_422(client):
    ds = _new_dataset_id("dup")
    await _register(client, ds)
    await _grant(client, ds, "a", 0.1, 1e-8)
    r = await _plan(client, ds, {"epsilon": 0.1, "delta": 0.0}, ["a", "a"])
    assert r.status_code == 422


@pytest.mark.asyncio
async def test_empty_and_oversized_lists_422(client):
    ds = _new_dataset_id("sizes")
    await _register(client, ds)
    item = {"epsilon": 0.1, "delta": 0.0}
    assert (await _plan(client, ds, item, [])).status_code == 422
    assert (
        await _plan(client, ds, item, [f"x{i}" for i in range(13)])
    ).status_code == 422


@pytest.mark.asyncio
async def test_invalid_request_id_shape_422(client):
    ds = _new_dataset_id("badshape")
    await _register(client, ds)
    item = {"epsilon": 0.1, "delta": 0.0}
    assert (await _plan(client, ds, item, [""])).status_code == 422
    assert (await _plan(client, ds, item, ["x" * 201])).status_code == 422


@pytest.mark.asyncio
async def test_invalid_item_payload_422(client):
    ds = _new_dataset_id("baditem")
    await _register(client, ds)
    await _grant(client, ds, "a", 0.1, 1e-8)
    # epsilon / delta out of range, missing item, non-finite epsilon.
    assert (await _plan(
        client, ds, {"epsilon": 0.0, "delta": 0.0}, ["a"]
    )).status_code == 422
    assert (await _plan(
        client, ds, {"epsilon": 0.1, "delta": 1.0}, ["a"]
    )).status_code == 422
    assert (await _plan(
        client, ds, {"epsilon": -0.5, "delta": 0.0}, ["a"]
    )).status_code == 422
    r = await client.post(
        f"/datasets/{ds}/reversal-plans", json={"request_ids": ["a"]}
    )
    assert r.status_code == 422
    r = await client.post(
        f"/datasets/{ds}/reversal-plans",
        json={"item": {"epsilon": "nan", "delta": 0.0}, "request_ids": ["a"]},
    )
    assert r.status_code == 422
