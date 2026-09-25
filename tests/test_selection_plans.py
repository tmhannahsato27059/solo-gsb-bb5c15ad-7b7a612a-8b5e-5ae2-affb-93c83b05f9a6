"""End-to-end tests for the read-only selection-plan trial endpoint."""
from __future__ import annotations

import itertools
import os
import tempfile
from datetime import datetime, timedelta, timezone

import pytest
import pytest_asyncio

# Point the ledger at an isolated database before importing the app.
_tmp = tempfile.mkdtemp(prefix="dpledger-test-selection-")
os.environ["DATABASE_PATH"] = os.path.join(_tmp, "selection.db")

from httpx import ASGITransport, AsyncClient  # noqa: E402

from app import db, rdp  # noqa: E402
from app.main import app  # noqa: E402

_counter = itertools.count()


def _new_dataset_id(name: str) -> str:
    return f"dssp-{name}-{next(_counter)}"


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


def _cand(cid: str, epsilon: float, value: int, delta: float = 1e-7) -> dict:
    return {"candidate_id": cid, "epsilon": epsilon, "delta": delta,
            "value": value}


async def _plan(client, dataset_id: str, candidates: list[dict]):
    return await client.post(
        f"/datasets/{dataset_id}/selection-plans",
        json={"candidates": candidates},
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


def _independent_best(
    committed: list[tuple[float, float]],
    candidates: list[dict],
    cap: float,
    target_delta: float,
    tol: float = 1e-12,
):
    """Brute-force the expected winner directly with rdp.evaluate_entries."""
    best = ((), 0, 0.0)
    for mask in range(1 << len(candidates)):
        indices = tuple(i for i in range(len(candidates)) if mask & (1 << i))
        entries = committed + [
            (candidates[i]["epsilon"], candidates[i]["delta"]) for i in indices
        ]
        _, projected, order = rdp.evaluate_entries(entries, target_delta)
        projected = max(0.0, projected)
        if projected > cap + tol:
            continue
        value = sum(candidates[i]["value"] for i in indices)
        key_ids = sorted(candidates[i]["candidate_id"] for i in indices)
        _, bv, be = best
        b_ids = sorted(candidates[i]["candidate_id"] for i in best[0])
        replace = (
            value > bv
            or (value == bv and projected < be - tol)
            or (value == bv and abs(projected - be) <= tol and key_ids < b_ids)
        )
        if replace:
            best = (indices, value, projected)
    return best


# ----------------------------------------------------------------- behaviour --


@pytest.mark.asyncio
async def test_selection_plan_empty_dataset_picks_all_when_fitting(client):
    ds = _new_dataset_id("allfit")
    await _register(client, ds, eps=1.0, delta=1e-6)
    candidates = [
        _cand("a", 0.1, 10),
        _cand("b", 0.2, 20),
        _cand("c", 0.05, 5, delta=0.0),
    ]
    r = await _plan(client, ds, candidates)
    assert r.status_code == 200
    body = r.json()
    assert body["dataset_id"] == ds
    assert body["selected_candidate_ids"] == ["a", "b", "c"]
    assert body["unselected_candidate_ids"] == []
    assert body["total_value"] == 35
    assert body["candidate_count"] == 3
    assert body["epsilon_committed"] == pytest.approx(0.0)
    assert body["projected_epsilon"] > 0.0
    assert body["epsilon_remaining"] == pytest.approx(
        1.0 - body["projected_epsilon"]
    )
    # Cross-check the projection with an independent rdp.py computation.
    _, expected_eps, expected_order = rdp.evaluate_entries(
        [(0.1, 1e-7), (0.2, 1e-7), (0.05, 0.0)], 1e-6
    )
    assert body["projected_epsilon"] == pytest.approx(
        max(0.0, expected_eps), abs=1e-12
    )
    assert body["rdp_best_order"] == expected_order


@pytest.mark.asyncio
async def test_selection_plan_maximises_value_not_epsilon_count(client):
    ds = _new_dataset_id("value")
    await _register(client, ds, eps=0.22, delta=1e-6)
    # A single expensive candidate is worth more than the two cheap ones.
    candidates = [
        _cand("cheap-1", 0.1, 4),
        _cand("cheap-2", 0.1, 4),
        _cand("expensive", 0.2, 9),
    ]
    body = (await _plan(client, ds, candidates)).json()
    assert body["selected_candidate_ids"] == ["expensive"]
    assert sorted(body["unselected_candidate_ids"]) == ["cheap-1", "cheap-2"]
    assert body["total_value"] == 9


@pytest.mark.asyncio
async def test_selection_plan_knapsack_combination(client):
    ds = _new_dataset_id("knapsack")
    # Pure epsilon-DP makes feasibility a plain additive epsilon check.
    await _register(client, ds, eps=0.5, delta=1e-6)
    candidates = [
        _cand("x1", 0.3, 7, delta=0.0),
        _cand("x2", 0.3, 7, delta=0.0),
        _cand("x3", 0.2, 6, delta=0.0),
        _cand("x4", 0.2, 6, delta=0.0),
    ]
    body = (await _plan(client, ds, candidates)).json()
    # 0.3 + 0.2 = 0.5 -> value 13 beats the single 0.3 -> 7.
    assert body["selected_candidate_ids"] == ["x1", "x3"]
    assert body["total_value"] == 13
    assert body["projected_epsilon"] == pytest.approx(0.5)
    assert body["epsilon_remaining"] == pytest.approx(0.0)


@pytest.mark.asyncio
async def test_selection_plan_value_tie_prefers_smaller_projected_epsilon(client):
    ds = _new_dataset_id("tieeps")
    # Tight enough that the two candidates cannot be chosen together, so the
    # equal-value singletons (10 each) compete on projected epsilon.
    await _register(client, ds, eps=0.45, delta=1e-6)
    candidates = [
        _cand("big", 0.4, 10),
        _cand("small", 0.1, 10),
    ]
    body = (await _plan(client, ds, candidates)).json()
    # Same value (10); the smaller projected epsilon wins.
    assert body["selected_candidate_ids"] == ["small"]
    assert body["unselected_candidate_ids"] == ["big"]
    assert body["total_value"] == 10


@pytest.mark.asyncio
async def test_selection_plan_value_and_epsilon_tie_uses_id_lexicographic(client):
    ds = _new_dataset_id("tieids")
    # Only one 0.1 candidate fits, so equal-value/epsilon singletons compete
    # on the lexicographic order of the ascending candidate-id list.
    await _register(client, ds, eps=0.15, delta=1e-6)
    candidates = [
        _cand("zeta", 0.1, 5, delta=0.0),
        _cand("alpha", 0.1, 5, delta=0.0),
        _cand("beta", 0.1, 5, delta=0.0),
    ]
    body = (await _plan(client, ds, candidates)).json()
    assert body["selected_candidate_ids"] == ["alpha"]
    assert sorted(body["unselected_candidate_ids"]) == ["beta", "zeta"]


@pytest.mark.asyncio
async def test_selection_plan_no_feasible_nonempty_returns_empty_selection(client):
    ds = _new_dataset_id("nonefit")
    await _register(client, ds, eps=0.05, delta=1e-6)
    candidates = [_cand("huge", 1.0, 100), _cand("also-huge", 0.9, 50)]
    body = (await _plan(client, ds, candidates)).json()
    assert body["selected_candidate_ids"] == []
    assert sorted(body["unselected_candidate_ids"]) == ["also-huge", "huge"]
    assert body["total_value"] == 0
    assert body["projected_epsilon"] == pytest.approx(0.0)
    assert body["epsilon_committed"] == pytest.approx(0.0)
    assert body["epsilon_remaining"] == pytest.approx(0.05)
    assert body["rdp_best_order"] == 8192.0


@pytest.mark.asyncio
async def test_selection_plan_accounts_for_current_granted_requests(client):
    ds = _new_dataset_id("occupied")
    await _register(client, ds, eps=0.5, delta=1e-6)
    granted = await client.post(
        f"/datasets/{ds}/requests",
        json={"request_id": "q1", "query_type": "q",
              "epsilon": 0.2, "delta": 1e-8},
    )
    committed = granted.json()["epsilon_after"]

    candidates = [
        _cand("fits", 0.1, 10),
        _cand("wontfit", 0.4, 100),
    ]
    body = (await _plan(client, ds, candidates)).json()
    assert body["epsilon_committed"] == pytest.approx(committed, abs=1e-12)
    assert body["selected_candidate_ids"] == ["fits"]
    # projected_epsilon covers the committed request plus the chosen candidate.
    assert body["projected_epsilon"] > committed


@pytest.mark.asyncio
async def test_selection_plan_accounts_for_active_reservation(client):
    ds = _new_dataset_id("reserved")
    await _register(client, ds, eps=0.5, delta=1e-6)
    await client.post(
        f"/datasets/{ds}/reservations",
        json={"reservation_id": "r1", "epsilon": 0.3, "delta": 1e-8,
              "ttl_seconds": 3600},
    )
    body = (await _plan(
        client, ds, [_cand("c", 0.3, 1), _cand("d", 0.05, 1)]
    )).json()
    # The reservation locks most of the cap: only the tiny candidate fits.
    assert body["selected_candidate_ids"] == ["d"]
    budget = (await client.get(f"/datasets/{ds}/budget")).json()
    assert body["epsilon_committed"] == pytest.approx(
        budget["epsilon_consumed"] + budget["epsilon_reserved"], abs=1e-12
    )


@pytest.mark.asyncio
async def test_selection_plan_ignores_expired_and_reversed(client):
    ds = _new_dataset_id("released")
    await _register(client, ds, eps=0.5, delta=1e-6)

    # An expired reservation (created earlier and backdated) must not count;
    # creating it sweeps expiry at that point in time.
    await client.post(
        f"/datasets/{ds}/reservations",
        json={"reservation_id": "exp", "epsilon": 0.4, "delta": 1e-8,
              "ttl_seconds": 3600},
    )
    await _backdate(
        ds, "exp", datetime.now(timezone.utc) - timedelta(seconds=5)
    )

    # Granted request that is then reversed must not count either; the
    # reversal transaction sweeps the expired row but excludes both from
    # committed occupancy.
    await client.post(
        f"/datasets/{ds}/requests",
        json={"request_id": "q1", "query_type": "q",
              "epsilon": 0.2, "delta": 1e-8},
    )
    await client.post(
        f"/datasets/{ds}/requests/q1/reversals",
        json={"reversal_id": "rev1", "reason": "trial setup cleanup"},
    )

    body = (await _plan(client, ds, [_cand("c", 0.4, 9)])).json()
    assert body["selected_candidate_ids"] == ["c"]
    assert body["epsilon_committed"] == pytest.approx(0.0, abs=1e-12)


@pytest.mark.asyncio
async def test_selection_plan_does_not_sweep_expired_reservations(client):
    ds = _new_dataset_id("nosweep")
    await _register(client, ds, eps=0.5, delta=1e-6)
    await client.post(
        f"/datasets/{ds}/reservations",
        json={"reservation_id": "exp", "epsilon": 0.4, "delta": 0.0,
              "ttl_seconds": 3600},
    )
    await _backdate(
        ds, "exp", datetime.now(timezone.utc) - timedelta(seconds=5)
    )

    # The trial treats the past-TTL reservation as inactive, but must not
    # write the lazy expiry itself (exactly like the release forecast).
    body = (await _plan(client, ds, [_cand("c", 0.4, 9, delta=0.0)])).json()
    assert body["selected_candidate_ids"] == ["c"]
    async with db.connect() as conn:
        row = await db.get_reservation(conn, ds, "exp")
        assert row["status"] == "granted"
        assert row["expired_at"] is None
    # Repeating the trial keeps returning the same, still-unswept picture.
    again = (await _plan(client, ds, [_cand("c", 0.4, 9, delta=0.0)])).json()
    assert again["selected_candidate_ids"] == ["c"]


@pytest.mark.asyncio
async def test_selection_plan_is_strictly_read_only(client):
    ds = _new_dataset_id("readonly")
    await _register(client, ds, eps=1.0, delta=1e-6)
    candidates = [_cand("a", 0.1, 10), _cand("b", 0.9, 1)]

    first = (await _plan(client, ds, candidates)).json()
    second = (await _plan(client, ds, candidates)).json()
    for key in (
        "selected_candidate_ids", "unselected_candidate_ids", "total_value",
        "epsilon_committed", "projected_epsilon", "epsilon_remaining",
    ):
        assert first[key] == second[key]

    # No requests, reservations, batches, transfers or reversals exist.
    assert (await client.get(f"/datasets/{ds}/requests")).json()["count"] == 0
    assert (
        await client.get(f"/datasets/{ds}/reservations")
    ).json()["count"] == 0
    assert (await client.get(f"/datasets/{ds}/reversals")).json()["count"] == 0
    budget = (await client.get(f"/datasets/{ds}/budget")).json()
    assert budget["granted_requests"] == 0
    assert budget["epsilon_consumed"] == 0.0
    assert budget["epsilon_remaining"] == pytest.approx(1.0)


@pytest.mark.asyncio
async def test_selection_plan_recomputes_as_ledger_changes(client):
    ds = _new_dataset_id("fresh")
    await _register(client, ds, eps=0.4, delta=1e-6)
    candidates = [_cand("c", 0.25, 10, delta=0.0)]

    before = (await _plan(client, ds, candidates)).json()
    assert before["selected_candidate_ids"] == ["c"]

    # Occupy budget with another request; the next trial recomputes.
    await client.post(
        f"/datasets/{ds}/requests",
        json={"request_id": "holder", "query_type": "q",
              "epsilon": 0.1, "delta": 0.0},
    )
    after = (await _plan(client, ds, candidates)).json()
    assert after["selected_candidate_ids"] == ["c"]
    assert after["epsilon_committed"] == pytest.approx(0.1)

    # Once the committed epsilon leaves no room, the candidate drops out.
    await client.post(
        f"/datasets/{ds}/requests",
        json={"request_id": "holder2", "query_type": "q",
              "epsilon": 0.1, "delta": 0.0},
    )
    full = (await _plan(client, ds, candidates)).json()
    assert full["selected_candidate_ids"] == []
    assert full["unselected_candidate_ids"] == ["c"]
    assert full["epsilon_committed"] == pytest.approx(0.2)


@pytest.mark.asyncio
async def test_selection_plan_brute_force_matches_independent_accounting(client):
    ds = _new_dataset_id("brute")
    cap, target_delta = 1.0, 1e-6
    await _register(client, ds, eps=cap, delta=target_delta)
    # Some pre-existing occupancy mixing pure and approximate DP.
    await client.post(
        f"/datasets/{ds}/requests",
        json={"request_id": "base", "query_type": "q",
              "epsilon": 0.1, "delta": 1e-9},
    )
    candidates = [
        _cand("k1", 0.15, 3),
        _cand("k2", 0.25, 5),
        _cand("k3", 0.1, 2, delta=0.0),
        _cand("k4", 0.4, 8),
        _cand("k5", 0.05, 1),
    ]
    body = (await _plan(client, ds, candidates)).json()

    committed = [(0.1, 1e-9)]
    indices, value, projected = _independent_best(
        committed, candidates, cap, target_delta
    )
    expected_ids = sorted(candidates[i]["candidate_id"] for i in indices)
    assert body["selected_candidate_ids"] == expected_ids
    assert body["total_value"] == value
    assert body["projected_epsilon"] == pytest.approx(projected, abs=1e-12)
    assert body["epsilon_committed"] == pytest.approx(
        max(0.0, rdp.evaluate_entries(committed, target_delta)[1]), abs=1e-12
    )


@pytest.mark.asyncio
async def test_selection_plan_twelve_candidate_boundary(client):
    ds = _new_dataset_id("twelve")
    await _register(client, ds, eps=10.0, delta=1e-6)
    candidates = [
        _cand(f"c-{i:02d}", 0.1, i + 1, delta=0.0) for i in range(12)
    ]
    r = await _plan(client, ds, candidates)
    assert r.status_code == 200
    body = r.json()
    assert len(body["selected_candidate_ids"]) == 12
    assert body["total_value"] == sum(range(1, 13))


# ---------------------------------------------------------------- validation --


@pytest.mark.asyncio
async def test_selection_plan_missing_dataset_404(client):
    r = await _plan(client, "no-such-dataset", [_cand("a", 0.1, 1)])
    assert r.status_code == 404
    assert r.json()["error"] == "dataset_not_found"


@pytest.mark.asyncio
async def test_selection_plan_empty_candidate_list_422(client):
    ds = _new_dataset_id("empty")
    await _register(client, ds)
    r = await _plan(client, ds, [])
    assert r.status_code == 422


@pytest.mark.asyncio
async def test_selection_plan_more_than_twelve_candidates_422(client):
    ds = _new_dataset_id("toomany")
    await _register(client, ds)
    candidates = [_cand(f"c{i}", 0.01, 1, delta=0.0) for i in range(13)]
    r = await _plan(client, ds, candidates)
    assert r.status_code == 422


@pytest.mark.asyncio
async def test_selection_plan_duplicate_candidate_ids_422(client):
    ds = _new_dataset_id("dup")
    await _register(client, ds)
    r = await _plan(client, ds, [_cand("same", 0.1, 1), _cand("same", 0.2, 2)])
    assert r.status_code == 422


@pytest.mark.asyncio
async def test_selection_plan_value_must_be_positive_integer(client):
    ds = _new_dataset_id("badvalue")
    await _register(client, ds)
    r = await _plan(client, ds, [_cand("a", 0.1, 0)])
    assert r.status_code == 422
    r2 = await _plan(client, ds, [_cand("b", 0.1, -3)])
    assert r2.status_code == 422
    # A float value is not accepted either.
    r3 = await _plan(client, ds, [{"candidate_id": "c", "epsilon": 0.1,
                                  "delta": 0.0, "value": 1.5}])
    assert r3.status_code == 422


@pytest.mark.asyncio
async def test_selection_plan_invalid_epsilon_delta_422(client):
    ds = _new_dataset_id("badefd")
    await _register(client, ds)
    assert (await _plan(client, ds, [_cand("a", 0.0, 1)])).status_code == 422
    assert (
        await _plan(client, ds, [_cand("b", 0.1, 1, delta=1.0)])
    ).status_code == 422


@pytest.mark.asyncio
async def test_selection_plan_does_not_create_verdict_records(client):
    ds = _new_dataset_id("norecords")
    await _register(client, ds, eps=0.01, delta=1e-6)
    # Both rejected-by-budget and fitting candidates appear in one trial.
    r = await _plan(
        client,
        ds,
        [_cand("fits", 0.001, 1, delta=0.0),
         _cand("huge", 5.0, 99)],
    )
    assert r.status_code == 200
    # The over-budget candidate leaves no rejected request behind.
    requests = (await client.get(f"/datasets/{ds}/requests")).json()
    assert requests["count"] == 0
