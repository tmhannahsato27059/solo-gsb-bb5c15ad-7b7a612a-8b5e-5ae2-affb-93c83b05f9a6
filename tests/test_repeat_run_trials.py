"""End-to-end tests for the read-only repeated-workload capacity trial."""
from __future__ import annotations

import itertools
import math
import os
import tempfile
from datetime import datetime, timedelta, timezone

import pytest
import pytest_asyncio

# Point the ledger at an isolated database before importing the app.
_tmp = tempfile.mkdtemp(prefix="dpledger-test-repeat-")
os.environ["DATABASE_PATH"] = os.path.join(_tmp, "repeat.db")

from httpx import ASGITransport, AsyncClient  # noqa: E402

from app import db, rdp  # noqa: E402
from app.main import app  # noqa: E402

_counter = itertools.count()


def _new_dataset_id(name: str) -> str:
    return f"dsrr-{name}-{next(_counter)}"


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


async def _trial(client, dataset_id: str, items: list[dict], max_runs: int = 1000):
    return await client.post(
        f"/datasets/{dataset_id}/repeat-run-trials",
        json={"items": items, "max_runs": max_runs},
    )


def _item(epsilon: float, delta: float = 1e-7) -> dict:
    return {"epsilon": epsilon, "delta": delta}


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


def _independent_runs(
    committed: list[tuple[float, float]],
    group: list[tuple[float, float]],
    runs: int,
    target_delta: float,
) -> float:
    """Projected epsilon of committed + ``runs`` group copies via rdp.py."""
    entries = list(committed) + group * runs
    _, eps, _ = rdp.evaluate_entries(entries, target_delta)
    return max(0.0, eps)


def _independent_max_runs(
    committed: list[tuple[float, float]],
    group: list[tuple[float, float]],
    max_runs: int,
    target_delta: float,
    cap: float,
    tol: float = 1e-12,
) -> int:
    """Brute-force the largest feasible run count directly with rdp.py.

    Composes the group incrementally so each extra run adds one compose pass
    instead of recomposing every copy from scratch.
    """
    entries = list(committed)
    best = 0
    for k in range(0, max_runs + 1):
        _, eps, _ = rdp.evaluate_entries(entries, target_delta)
        if max(0.0, eps) > cap + tol:
            break
        best = k
        entries.extend(group)
    return best


# ---------------------------------------------------------------- behaviour --


@pytest.mark.asyncio
async def test_repeat_run_trial_pure_epsilon_exact_addition(client):
    ds = _new_dataset_id("pure")
    cap = 1.0
    await _register(client, ds, eps=cap, delta=1e-6)
    # One pure-epsilon item of 0.15 per run: floor(1.0 / 0.15) = 6 runs.
    r = await _trial(client, ds, [_item(0.15, 0.0)], max_runs=100)
    assert r.status_code == 200
    body = r.json()
    assert body["max_complete_runs"] == 6
    assert body["at_max_runs"] is False
    assert body["item_count"] == 1
    assert body["items"] == [{"epsilon": 0.15, "delta": 0.0}]
    proj = body["projection"]
    assert proj["runs"] == 6
    assert proj["epsilon_projected"] == pytest.approx(0.9)
    assert proj["epsilon_remaining"] == pytest.approx(0.1)
    assert proj["feasible"] is True
    nxt = body["next_run_projection"]
    assert nxt["runs"] == 7
    assert nxt["epsilon_projected"] == pytest.approx(1.05)
    assert nxt["epsilon_remaining"] == pytest.approx(0.0)
    assert nxt["feasible"] is False
    # Pure-epsilon composition uses the minimum RDP order (exact addition).
    assert proj["rdp_best_order"] == rdp.RDP_ORDERS[0]


@pytest.mark.asyncio
async def test_repeat_run_trial_pure_group_multiple_items(client):
    ds = _new_dataset_id("puregroup")
    await _register(client, ds, eps=1.0, delta=1e-6)
    # Each run costs 0.1 + 0.2 = 0.3 pure epsilon: 3 full runs (0.9), not 4.
    r = await _trial(
        client, ds, [_item(0.1, 0.0), _item(0.2, 0.0)], max_runs=10
    )
    body = r.json()
    assert body["max_complete_runs"] == 3
    assert body["projection"]["epsilon_projected"] == pytest.approx(0.9)
    assert body["next_run_projection"]["epsilon_projected"] == pytest.approx(1.2)
    assert body["next_run_projection"]["feasible"] is False


@pytest.mark.asyncio
async def test_repeat_run_trial_approximate_matches_independent_accounting(client):
    ds = _new_dataset_id("approx")
    cap, target_delta = 1.0, 1e-6
    await _register(client, ds, eps=cap, delta=target_delta)
    group = [(0.05, 1e-8), (0.02, 1e-10), (0.01, 0.0)]
    r = await _trial(
        client,
        ds,
        [{"epsilon": e, "delta": d} for e, d in group],
        max_runs=100,
    )
    body = r.json()
    n = body["max_complete_runs"]
    # Brute-force the same answer directly with rdp.evaluate_entries.
    expected_n = _independent_max_runs([], group, 100, target_delta, cap)
    assert n == expected_n
    assert body["projection"]["epsilon_projected"] == pytest.approx(
        _independent_runs([], group, n, target_delta), abs=1e-12
    )
    assert body["next_run_projection"]["epsilon_projected"] == pytest.approx(
        _independent_runs([], group, n + 1, target_delta), abs=1e-12
    )
    assert body["next_run_projection"]["feasible"] is False
    # The feasible count is below the bound and zero-runs state is reported.
    assert 0 < n < 100
    assert body["epsilon_committed"] == pytest.approx(0.0)


@pytest.mark.asyncio
async def test_repeat_run_trial_with_tiny_delta_stays_finite(client):
    # Regression: 1/delta overflows float64 for delta=1e-309; the log-domain
    # curve keeps every projection finite so the trial answers normally.
    ds = _new_dataset_id("tinydelta")
    cap, target_delta = 1.0, 1e-6
    await _register(client, ds, eps=cap, delta=target_delta)
    group = [(0.1, 1e-309)]
    r = await _trial(
        client, ds, [{"epsilon": e, "delta": d} for e, d in group], max_runs=100
    )
    assert r.status_code == 200
    body = r.json()
    n = body["max_complete_runs"]
    expected_n = _independent_max_runs([], group, 100, target_delta, cap)
    assert n == expected_n
    assert 0 < n < 100
    assert math.isfinite(body["projection"]["epsilon_projected"])
    assert body["projection"]["epsilon_projected"] == pytest.approx(
        _independent_runs([], group, n, target_delta), abs=1e-12
    )
    assert body["next_run_projection"]["epsilon_projected"] == pytest.approx(
        _independent_runs([], group, n + 1, target_delta), abs=1e-12
    )


@pytest.mark.asyncio
async def test_repeat_run_trial_zero_runs_feasible_is_reported(client):
    ds = _new_dataset_id("zero")
    await _register(client, ds, eps=0.1, delta=1e-6)
    # A single run already exceeds the cap.
    r = await _trial(client, ds, [_item(0.5, 1e-8)], max_runs=100)
    assert r.status_code == 200
    body = r.json()
    assert body["max_complete_runs"] == 0
    assert body["at_max_runs"] is False
    proj = body["projection"]
    assert proj["runs"] == 0
    assert proj["epsilon_projected"] == pytest.approx(0.0)
    assert proj["epsilon_remaining"] == pytest.approx(0.1)
    assert proj["feasible"] is True
    nxt = body["next_run_projection"]
    assert nxt["runs"] == 1
    assert nxt["epsilon_projected"] > 0.1
    assert nxt["feasible"] is False


@pytest.mark.asyncio
async def test_repeat_run_trial_zero_runs_with_prior_committed_occupancy(client):
    ds = _new_dataset_id("zerofull")
    await _register(client, ds, eps=0.3, delta=1e-6)
    # Fill the cap almost entirely with a pure-epsilon request.
    await client.post(
        f"/datasets/{ds}/requests",
        json={"request_id": "holder", "query_type": "q",
              "epsilon": 0.25, "delta": 0.0},
    )
    # 0.05 remains; a 0.1 pure-epsilon run cannot fit even once.
    body = (await _trial(client, ds, [_item(0.1, 0.0)], max_runs=50)).json()
    assert body["max_complete_runs"] == 0
    assert body["epsilon_committed"] == pytest.approx(0.25)
    assert body["projection"]["epsilon_projected"] == pytest.approx(0.25)
    assert body["next_run_projection"]["epsilon_projected"] == pytest.approx(0.35)
    assert body["next_run_projection"]["feasible"] is False


@pytest.mark.asyncio
async def test_repeat_run_trial_reaches_max_runs_and_omits_next_projection(client):
    ds = _new_dataset_id("atmax")
    await _register(client, ds, eps=1000.0, delta=1e-6)
    max_runs = 7
    body = (
        await _trial(client, ds, [_item(0.001, 0.0)], max_runs=max_runs)
    ).json()
    assert body["max_complete_runs"] == max_runs
    assert body["at_max_runs"] is True
    assert body["projection"]["runs"] == max_runs
    assert body["projection"]["epsilon_projected"] == pytest.approx(0.007)
    assert body["projection"]["feasible"] is True
    # Reaching the bound means no run beyond it is evaluated.
    assert body["next_run_projection"] is None


@pytest.mark.asyncio
async def test_repeat_run_trial_max_runs_bound_100000(client):
    ds = _new_dataset_id("bound100k")
    await _register(client, ds, eps=10.0, delta=1e-6)
    # Pure-epsilon 1e-4 per run: 100000 runs cost 10 -> the whole bound fits.
    body = (
        await _trial(client, ds, [_item(1e-4, 0.0)], max_runs=100_000)
    ).json()
    assert body["max_runs"] == 100_000
    assert body["max_complete_runs"] == 100_000
    assert body["at_max_runs"] is True
    assert body["next_run_projection"] is None


@pytest.mark.asyncio
async def test_repeat_run_trial_accounts_for_committed_requests(client):
    ds = _new_dataset_id("committed")
    cap = 1.0
    await _register(client, ds, eps=cap, delta=1e-6)
    verdict = (
        await client.post(
            f"/datasets/{ds}/requests",
            json={"request_id": "q1", "query_type": "q",
                  "epsilon": 0.4, "delta": 0.0},
        )
    ).json()
    assert verdict["allowed"] is True

    body = (await _trial(client, ds, [_item(0.1, 0.0)], max_runs=100)).json()
    # 0.4 committed + k * 0.1 <= 1.0 -> k = 6.
    assert body["max_complete_runs"] == 6
    assert body["epsilon_committed"] == pytest.approx(0.4)
    assert body["projection"]["epsilon_projected"] == pytest.approx(1.0)
    assert body["next_run_projection"]["epsilon_projected"] == pytest.approx(1.1)


@pytest.mark.asyncio
async def test_repeat_run_trial_accounts_for_active_reservation(client):
    ds = _new_dataset_id("reserved")
    await _register(client, ds, eps=1.0, delta=1e-6)
    await client.post(
        f"/datasets/{ds}/reservations",
        json={"reservation_id": "r1", "epsilon": 0.3, "delta": 1e-8,
              "ttl_seconds": 3600},
    )
    body = (await _trial(client, ds, [_item(0.1, 0.0)], max_runs=100)).json()
    budget = (await client.get(f"/datasets/{ds}/budget")).json()
    committed = budget["epsilon_consumed"] + budget["epsilon_reserved"]
    assert body["epsilon_committed"] == pytest.approx(committed, abs=1e-12)
    # The locked reservation reduces the feasible number of 0.1-epsilon runs.
    assert 0 < body["max_complete_runs"] < 10
    expected_proj = body["epsilon_committed"] + 0.1 * body["max_complete_runs"]
    assert body["projection"]["epsilon_projected"] == pytest.approx(
        expected_proj, abs=1e-10
    )


@pytest.mark.asyncio
async def test_repeat_run_trial_ignores_expired_and_reversed(client):
    ds = _new_dataset_id("ignored")
    await _register(client, ds, eps=1.0, delta=1e-6)
    await client.post(
        f"/datasets/{ds}/reservations",
        json={"reservation_id": "exp", "epsilon": 0.4, "delta": 1e-8,
              "ttl_seconds": 3600},
    )
    await _backdate(
        ds, "exp", datetime.now(timezone.utc) - timedelta(seconds=5)
    )
    await client.post(
        f"/datasets/{ds}/requests",
        json={"request_id": "q1", "query_type": "q",
              "epsilon": 0.2, "delta": 1e-8},
    )
    await client.post(
        f"/datasets/{ds}/requests/q1/reversals",
        json={"reversal_id": "rr-rev-expired-reversed",
              "reason": "trial setup cleanup"},
    )
    body = (await _trial(client, ds, [_item(0.1, 0.0)], max_runs=100)).json()
    # Neither the expired reservation nor the reversed request occupies budget.
    assert body["epsilon_committed"] == pytest.approx(0.0, abs=1e-12)
    assert body["max_complete_runs"] == 10


@pytest.mark.asyncio
async def test_repeat_run_trial_does_not_sweep_expired_reservations(client):
    ds = _new_dataset_id("nosweep")
    await _register(client, ds, eps=1.0, delta=1e-6)
    await client.post(
        f"/datasets/{ds}/reservations",
        json={"reservation_id": "exp", "epsilon": 0.4, "delta": 0.0,
              "ttl_seconds": 3600},
    )
    await _backdate(
        ds, "exp", datetime.now(timezone.utc) - timedelta(seconds=5)
    )
    first = (await _trial(client, ds, [_item(0.1, 0.0)], 100)).json()
    async with db.connect() as conn:
        row = await db.get_reservation(conn, ds, "exp")
        assert row["status"] == "granted"
        assert row["expired_at"] is None
    second = (await _trial(client, ds, [_item(0.1, 0.0)], 100)).json()
    assert second["max_complete_runs"] == first["max_complete_runs"]


@pytest.mark.asyncio
async def test_repeat_run_trial_is_strictly_read_only(client):
    ds = _new_dataset_id("readonly")
    await _register(client, ds, eps=0.05, delta=1e-6)
    r = await _trial(client, ds, [_item(0.5, 1e-8), _item(0.001, 0.0)], 100)
    assert r.status_code == 200
    assert r.json()["max_complete_runs"] == 0
    # No request, reservation or reversal records were created.
    assert (await client.get(f"/datasets/{ds}/requests")).json()["count"] == 0
    assert (
        await client.get(f"/datasets/{ds}/reservations")
    ).json()["count"] == 0
    assert (await client.get(f"/datasets/{ds}/reversals")).json()["count"] == 0
    budget = (await client.get(f"/datasets/{ds}/budget")).json()
    assert budget["granted_requests"] == 0
    assert budget["epsilon_consumed"] == 0.0
    assert budget["epsilon_remaining"] == pytest.approx(0.05)


@pytest.mark.asyncio
async def test_repeat_run_trial_recomputes_as_ledger_changes(client):
    ds = _new_dataset_id("fresh")
    await _register(client, ds, eps=1.0, delta=1e-6)
    item = _item(0.3, 0.0)

    before = (await _trial(client, ds, [item], 100)).json()
    assert before["max_complete_runs"] == 3
    assert before["epsilon_committed"] == pytest.approx(0.0)

    await client.post(
        f"/datasets/{ds}/requests",
        json={"request_id": "holder", "query_type": "q",
              "epsilon": 0.4, "delta": 0.0},
    )
    after = (await _trial(client, ds, [item], 100)).json()
    # 0.4 committed + k * 0.3 <= 1.0 -> k = 2.
    assert after["epsilon_committed"] == pytest.approx(0.4)
    assert after["max_complete_runs"] == 2
    assert after["projection"]["epsilon_projected"] == pytest.approx(1.0)


@pytest.mark.asyncio
async def test_repeat_run_trial_boundary_capacity_inclusive_with_tolerance(client):
    ds = _new_dataset_id("exact")
    await _register(client, ds, eps=1.0, delta=1e-6)
    # 4 * 0.25 == cap exactly: the fourth run is still feasible.
    body = (await _trial(client, ds, [_item(0.25, 0.0)], 10)).json()
    assert body["max_complete_runs"] == 4
    assert body["projection"]["feasible"] is True
    assert body["next_run_projection"]["runs"] == 5
    assert body["next_run_projection"]["feasible"] is False


@pytest.mark.asyncio
async def test_repeat_run_trial_five_items_and_mixed_group(client):
    ds = _new_dataset_id("fiveitems")
    cap, target_delta = 1.0, 1e-6
    await _register(client, ds, eps=cap, delta=target_delta)
    group = [
        (0.02, 1e-8),
        (0.01, 1e-10),
        (0.005, 0.0),
        (0.03, 1e-9),
        (0.015, 0.0),
    ]
    body = (
        await _trial(
            client,
            ds,
            [{"epsilon": e, "delta": d} for e, d in group],
            max_runs=100,
        )
    ).json()
    n = body["max_complete_runs"]
    expected = _independent_max_runs([], group, 100, target_delta, cap)
    assert n == expected
    assert body["item_count"] == 5
    assert body["projection"]["epsilon_projected"] == pytest.approx(
        _independent_runs([], group, n, target_delta), abs=1e-12
    )


@pytest.mark.asyncio
async def test_repeat_run_trial_with_committed_mix_matches_independent(client):
    ds = _new_dataset_id("committedmix")
    cap, target_delta = 1.0, 1e-6
    await _register(client, ds, eps=cap, delta=target_delta)
    committed = [(0.1, 1e-9), (0.05, 0.0)]
    await client.post(
        f"/datasets/{ds}/requests",
        json={"request_id": "approx-base", "query_type": "q",
              "epsilon": 0.1, "delta": 1e-9},
    )
    await client.post(
        f"/datasets/{ds}/requests",
        json={"request_id": "pure-base", "query_type": "q",
              "epsilon": 0.05, "delta": 0.0},
    )
    group = [(0.04, 1e-8), (0.01, 0.0)]
    body = (
        await _trial(
            client,
            ds,
            [{"epsilon": e, "delta": d} for e, d in group],
            max_runs=100,
        )
    ).json()
    n = body["max_complete_runs"]
    expected = _independent_max_runs(committed, group, 100, target_delta, cap)
    assert n == expected
    assert body["epsilon_committed"] == pytest.approx(
        _independent_runs(committed, [], 0, target_delta), abs=1e-12
    )
    assert body["projection"]["epsilon_projected"] == pytest.approx(
        _independent_runs(committed, group, n, target_delta), abs=1e-12
    )
    assert body["next_run_projection"]["epsilon_projected"] == pytest.approx(
        _independent_runs(committed, group, n + 1, target_delta), abs=1e-12
    )


# ---------------------------------------------------------------- validation --


@pytest.mark.asyncio
async def test_repeat_run_trial_missing_dataset_404(client):
    r = await _trial(client, "no-such-dataset", [_item(0.1, 0.0)], 10)
    assert r.status_code == 404
    assert r.json()["error"] == "dataset_not_found"


@pytest.mark.asyncio
async def test_repeat_run_trial_empty_items_422(client):
    ds = _new_dataset_id("empty")
    await _register(client, ds)
    r = await _trial(client, ds, [], 10)
    assert r.status_code == 422


@pytest.mark.asyncio
async def test_repeat_run_trial_six_items_422(client):
    ds = _new_dataset_id("six")
    await _register(client, ds)
    r = await _trial(client, ds, [_item(0.01, 0.0)] * 6, 10)
    assert r.status_code == 422


@pytest.mark.asyncio
async def test_repeat_run_trial_max_runs_bounds_422(client):
    ds = _new_dataset_id("badruns")
    await _register(client, ds)
    assert (await _trial(client, ds, [_item(0.1, 0.0)], 0)).status_code == 422
    assert (
        await _trial(client, ds, [_item(0.1, 0.0)], 100_001)
    ).status_code == 422
    # A float is not an accepted integer either.
    r = await client.post(
        f"/datasets/{ds}/repeat-run-trials",
        json={"items": [_item(0.1, 0.0)], "max_runs": 1.5},
    )
    assert r.status_code == 422


@pytest.mark.asyncio
async def test_repeat_run_trial_invalid_epsilon_delta_422(client):
    ds = _new_dataset_id("badefd")
    await _register(client, ds)
    assert (await _trial(client, ds, [_item(0.0, 0.0)], 10)).status_code == 422
    assert (
        await _trial(client, ds, [_item(0.1, 1.0)], 10)
    ).status_code == 422


@pytest.mark.asyncio
async def test_repeat_run_trial_does_not_create_verdict_records(client):
    ds = _new_dataset_id("norecords")
    await _register(client, ds, eps=1.0, delta=1e-6)
    # Over-cap workload on every run still leaves nothing behind.
    r = await _trial(
        client,
        ds,
        [_item(5.0, 1e-8), _item(2.0, 0.0)],
        max_runs=10,
    )
    assert r.status_code == 200
    assert r.json()["max_complete_runs"] == 0
    assert (await client.get(f"/datasets/{ds}/requests")).json()["count"] == 0
