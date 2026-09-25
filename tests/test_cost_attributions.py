"""End-to-end tests for the read-only cost-attribution endpoint.

POST /datasets/{dataset_id}/cost-attributions
"""
from __future__ import annotations

import itertools
import math
import os
import tempfile
from datetime import datetime, timedelta, timezone

import pytest
import pytest_asyncio

# Point the ledger at an isolated database before importing the app.
_tmp = tempfile.mkdtemp(prefix="dpledger-test-attribution-")
os.environ["DATABASE_PATH"] = os.path.join(_tmp, "attribution.db")

from httpx import ASGITransport, AsyncClient  # noqa: E402

from app import db, rdp  # noqa: E402
from app.main import app  # noqa: E402

_counter = itertools.count()
_rev_counter = itertools.count()


def _new_dataset_id(name: str) -> str:
    return f"dsca-{name}-{next(_counter)}"


def _new_reversal_id() -> str:
    return f"rev-ca-{next(_rev_counter)}"


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


async def _attribute(client, ds: str, request_ids: list[str]):
    return await client.post(
        f"/datasets/{ds}/cost-attributions",
        json={"request_ids": request_ids},
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


def _independent_shapley(
    baseline: list[tuple[float, float]],
    selected: list[tuple[float, float]],
    target_delta: float,
) -> tuple[float, float, list[float]]:
    """Brute-force baseline/full occupancy and Shapley shares via rdp.py."""
    def v(indices: tuple[int, ...]) -> float:
        entries = baseline + [selected[i] for i in indices]
        return max(0.0, rdp.evaluate_entries(entries, target_delta)[1])

    n = len(selected)
    base = v(())
    full = v(tuple(range(n)))

    def _value(mask: int) -> float:
        return v(tuple(i for i in range(n) if mask & (1 << i)))

    values = {mask: _value(mask) for mask in range(1 << n)}
    shares: list[float] = []
    for i in range(n):
        bit = 1 << i
        others = ((1 << n) - 1) ^ bit
        share = 0.0
        sub = others
        while True:
            size = sub.bit_count()
            weight = (
                math.factorial(size)
                * math.factorial(n - 1 - size)
                / math.factorial(n)
            )
            share += weight * (values[sub | bit] - values[sub])
            if sub == 0:
                break
            sub = (sub - 1) & others
        shares.append(max(0.0, share))
    return base, full, shares


# ----------------------------------------------------------------- behaviour --


@pytest.mark.asyncio
async def test_single_request_attributes_its_full_increment(client):
    ds = _new_dataset_id("single")
    await _register(client, ds, eps=1.0, delta=1e-6)
    await _grant(client, ds, "a", 0.1, 1e-8)
    r = await _attribute(client, ds, ["a"])
    assert r.status_code == 200
    body = r.json()
    assert body["request_count"] == 1
    assert body["request_ids"] == ["a"]
    assert body["epsilon_baseline"] == pytest.approx(0.0, abs=1e-12)
    expected = rdp.evaluate_entries([(0.1, 1e-8)], 1e-6)[1]
    assert body["epsilon_full_group"] == pytest.approx(expected, abs=1e-12)
    assert body["epsilon_increment"] == pytest.approx(expected, abs=1e-12)
    item = body["attributions"][0]
    assert item["request_id"] == "a"
    assert item["epsilon"] == 0.1
    assert item["delta"] == 1e-8
    assert item["attributed_epsilon"] == pytest.approx(expected, abs=1e-12)
    assert body["attributed_total_epsilon"] == pytest.approx(expected, abs=1e-12)


@pytest.mark.asyncio
async def test_shares_sum_to_full_group_minus_baseline(client):
    ds = _new_dataset_id("sum")
    await _register(client, ds, eps=10.0, delta=1e-6)
    # Other granted requests that form part of the baseline.
    await _grant(client, ds, "base1", 0.08, 1e-9)
    await _grant(client, ds, "base2", 0.03, delta=0.0)
    selected_specs = [
        ("a", 0.1, 1e-8),
        ("b", 0.2, 1e-8),
        ("c", 0.05, 0.0),
        ("d", 0.12, 1e-10),
    ]
    for rid, e, d in selected_specs:
        await _grant(client, ds, rid, e, d)

    body = (await _attribute(client, ds, [s[0] for s in selected_specs])).json()
    total = sum(item["attributed_epsilon"] for item in body["attributions"])
    increment = body["epsilon_full_group"] - body["epsilon_baseline"]
    assert body["epsilon_increment"] == pytest.approx(increment, abs=1e-12)
    assert total == pytest.approx(increment, abs=1e-12)
    assert body["attributed_total_epsilon"] == pytest.approx(total, abs=1e-12)
    # RDP composition is monotone: no share is negative.
    assert all(item["attributed_epsilon"] >= 0.0 for item in body["attributions"])


@pytest.mark.asyncio
async def test_shares_match_independent_brute_force_shapley(client):
    ds = _new_dataset_id("brute")
    target_delta = 1e-6
    await _register(client, ds, eps=10.0, delta=target_delta)
    # Mixed-DP baseline from other requests.
    await _grant(client, ds, "base1", 0.1, 1e-9)
    await _grant(client, ds, "base2", 0.04, delta=0.0)
    specs = [
        ("a", 0.15, 1e-8),
        ("b", 0.05, 0.0),
        ("c", 0.22, 1e-7),
    ]
    for rid, e, d in specs:
        await _grant(client, ds, rid, e, d)

    body = (await _attribute(client, ds, [s[0] for s in specs])).json()
    base, full, shares = _independent_shapley(
        baseline=[(0.1, 1e-9), (0.04, 0.0)],
        selected=[(e, d) for _, e, d in specs],
        target_delta=target_delta,
    )
    assert body["epsilon_baseline"] == pytest.approx(base, abs=1e-12)
    assert body["epsilon_full_group"] == pytest.approx(full, abs=1e-12)
    for item, expected_share in zip(body["attributions"], shares):
        assert item["attributed_epsilon"] == pytest.approx(
            expected_share, abs=1e-12
        )


@pytest.mark.asyncio
async def test_pure_dp_shares_are_exact_and_order_independent(client):
    ds = _new_dataset_id("pure")
    await _register(client, ds, eps=1.0, delta=1e-6)
    await _grant(client, ds, "base", 0.1, delta=0.0)
    specs = [("a", 0.05, 0.0), ("b", 0.2, 0.0), ("c", 0.1, 0.0)]
    for rid, e, d in specs:
        await _grant(client, ds, rid, e, d)

    body = (await _attribute(client, ds, ["a", "b", "c"])).json()
    # With pure epsilon-DP every marginal is the request's own epsilon,
    # regardless of coalition, so each share equals its epsilon.
    for item, (_, e, _) in zip(body["attributions"], specs):
        assert item["attributed_epsilon"] == pytest.approx(e, abs=1e-12)
    assert body["epsilon_baseline"] == pytest.approx(0.1, abs=1e-12)
    assert body["epsilon_full_group"] == pytest.approx(0.45, abs=1e-12)
    assert body["epsilon_increment"] == pytest.approx(0.35, abs=1e-12)

    # Supplying the same ids in another order only reorders the items.
    reordered = (await _attribute(client, ds, ["c", "a", "b"])).json()
    assert [i["request_id"] for i in reordered["attributions"]] == ["c", "a", "b"]
    by_id = {i["request_id"]: i["attributed_epsilon"]
             for i in reordered["attributions"]}
    assert by_id == pytest.approx({"a": 0.05, "b": 0.2, "c": 0.1}, abs=1e-12)
    assert reordered["epsilon_baseline"] == pytest.approx(
        body["epsilon_baseline"], abs=1e-12
    )
    assert reordered["epsilon_full_group"] == pytest.approx(
        body["epsilon_full_group"], abs=1e-12
    )


@pytest.mark.asyncio
async def test_baseline_includes_other_requests_and_active_reservations(client):
    ds = _new_dataset_id("baseline")
    target_delta = 1e-6
    await _register(client, ds, eps=2.0, delta=target_delta)
    await _grant(client, ds, "other", 0.1, 1e-8)
    await client.post(
        f"/datasets/{ds}/reservations",
        json={"reservation_id": "res1", "epsilon": 0.3, "delta": 1e-8,
              "ttl_seconds": 3600},
    )
    await _grant(client, ds, "a", 0.1, 1e-8)
    await _grant(client, ds, "b", 0.1, 1e-8)

    body = (await _attribute(client, ds, ["a", "b"])).json()
    expected_baseline = max(
        0.0,
        rdp.evaluate_entries(
            [(0.1, 1e-8), (0.3, 1e-8)], target_delta
        )[1],
    )
    expected_full = max(
        0.0,
        rdp.evaluate_entries(
            [(0.1, 1e-8), (0.3, 1e-8), (0.1, 1e-8), (0.1, 1e-8)],
            target_delta,
        )[1],
    )
    assert body["epsilon_baseline"] == pytest.approx(
        expected_baseline, abs=1e-12
    )
    assert body["epsilon_full_group"] == pytest.approx(expected_full, abs=1e-12)

    # The live budget's committed occupancy (granted + active reservation)
    # equals the full-group occupancy, since the selected requests are
    # themselves granted and non-reversed.
    budget = (await client.get(f"/datasets/{ds}/budget")).json()
    committed = budget["epsilon_consumed"] + budget["epsilon_reserved"]
    assert body["epsilon_full_group"] == pytest.approx(committed, abs=1e-12)


@pytest.mark.asyncio
async def test_expired_reservation_excluded_without_being_swept(client):
    ds = _new_dataset_id("expsweep")
    await _register(client, ds, eps=1.0, delta=1e-6)
    await _grant(client, ds, "a", 0.1, delta=0.0)
    await client.post(
        f"/datasets/{ds}/reservations",
        json={"reservation_id": "exp", "epsilon": 0.4, "delta": 0.0,
              "ttl_seconds": 3600},
    )
    # Backdating directly does not sweep: no budget-touching endpoint runs
    # between the backdate and the read-only attribution.
    await _backdate(ds, "exp", datetime.now(timezone.utc) - timedelta(seconds=5))

    body = (await _attribute(client, ds, ["a"])).json()
    # The past-TTL reservation is treated as inactive, so baseline is empty.
    assert body["epsilon_baseline"] == pytest.approx(0.0, abs=1e-12)
    # ...but the read-only attribution must not sweep it itself.
    async with db.connect() as conn:
        row = await db.get_reservation(conn, ds, "exp")
        assert row["status"] == "granted"
        assert row["expired_at"] is None
    # Repeating the attribution keeps the same, still-unswept picture.
    again = (await _attribute(client, ds, ["a"])).json()
    assert again["epsilon_baseline"] == pytest.approx(0.0, abs=1e-12)
    async with db.connect() as conn:
        row = await db.get_reservation(conn, ds, "exp")
        assert row["status"] == "granted"


@pytest.mark.asyncio
async def test_reversed_requests_excluded_from_baseline(client):
    ds = _new_dataset_id("revbase")
    await _register(client, ds, eps=1.0, delta=1e-6)
    await _grant(client, ds, "gone", 0.2, 1e-8)
    await client.post(
        f"/datasets/{ds}/requests/gone/reversals",
        json={"reversal_id": _new_reversal_id(), "reason": "cleanup"},
    )
    await _grant(client, ds, "a", 0.1, 1e-8)

    body = (await _attribute(client, ds, ["a"])).json()
    assert body["epsilon_baseline"] == pytest.approx(0.0, abs=1e-12)
    expected = rdp.evaluate_entries([(0.1, 1e-8)], 1e-6)[1]
    assert body["epsilon_full_group"] == pytest.approx(expected, abs=1e-12)


@pytest.mark.asyncio
async def test_attribution_is_strictly_read_only(client):
    ds = _new_dataset_id("readonly")
    await _register(client, ds, eps=1.0, delta=1e-6)
    await _grant(client, ds, "a", 0.1, 1e-8)
    await _grant(client, ds, "b", 0.2, 1e-8)

    first = (await _attribute(client, ds, ["a", "b"])).json()
    second = (await _attribute(client, ds, ["a", "b"])).json()
    for key in (
        "request_count", "request_ids", "epsilon_cap", "epsilon_baseline",
        "epsilon_full_group", "epsilon_increment", "rdp_best_order_baseline",
        "rdp_best_order_full_group", "attributed_total_epsilon",
        "attributions",
    ):
        assert first[key] == second[key]

    # No new records of any kind, and budget occupancy is unchanged.
    assert (await client.get(f"/datasets/{ds}/requests")).json()["count"] == 2
    assert (
        await client.get(f"/datasets/{ds}/reservations")
    ).json()["count"] == 0
    assert (await client.get(f"/datasets/{ds}/reversals")).json()["count"] == 0
    budget = (await client.get(f"/datasets/{ds}/budget")).json()
    assert budget["granted_requests"] == 2
    assert budget["epsilon_consumed"] == pytest.approx(
        second["epsilon_full_group"], abs=1e-12
    )


@pytest.mark.asyncio
async def test_attribution_recomputes_against_live_ledger(client):
    ds = _new_dataset_id("live")
    await _register(client, ds, eps=1.0, delta=1e-6)
    await _grant(client, ds, "a", 0.1, 1e-8)

    before = (await _attribute(client, ds, ["a"])).json()
    assert before["epsilon_baseline"] == pytest.approx(0.0, abs=1e-12)

    # A newly granted request joins the baseline on the next call.
    await _grant(client, ds, "later", 0.1, 1e-8)
    after = (await _attribute(client, ds, ["a"])).json()
    expected_baseline = rdp.evaluate_entries([(0.1, 1e-8)], 1e-6)[1]
    assert after["epsilon_baseline"] == pytest.approx(
        expected_baseline, abs=1e-12
    )


@pytest.mark.asyncio
async def test_ten_request_boundary(client):
    ds = _new_dataset_id("ten")
    await _register(client, ds, eps=10.0, delta=1e-6)
    ids = [f"r{i:02d}" for i in range(10)]
    for rid in ids:
        await _grant(client, ds, rid, 0.01, delta=0.0)
    r = await _attribute(client, ds, ids)
    assert r.status_code == 200
    body = r.json()
    assert body["request_count"] == 10
    assert len(body["attributions"]) == 10
    assert body["epsilon_full_group"] == pytest.approx(0.1, abs=1e-12)
    assert sum(
        i["attributed_epsilon"] for i in body["attributions"]
    ) == pytest.approx(0.1, abs=1e-12)


# ---------------------------------------------------------------- validation --


@pytest.mark.asyncio
async def test_missing_dataset_404(client):
    r = await _attribute(client, "no-such-dataset", ["a"])
    assert r.status_code == 404
    assert r.json()["error"] == "dataset_not_found"


@pytest.mark.asyncio
async def test_missing_request_404(client):
    ds = _new_dataset_id("missing")
    await _register(client, ds)
    r = await _attribute(client, ds, ["nope"])
    assert r.status_code == 404
    assert r.json()["error"] == "request_not_found"
    assert r.json()["request_id"] == "nope"


@pytest.mark.asyncio
async def test_rejected_request_409(client):
    ds = _new_dataset_id("rejected")
    await _register(client, ds, eps=0.01, delta=1e-6)
    await client.post(
        f"/datasets/{ds}/requests",
        json={"request_id": "rej", "query_type": "q",
              "epsilon": 5.0, "delta": 1e-8},
    )
    r = await _attribute(client, ds, ["rej"])
    assert r.status_code == 409
    assert r.json()["error"] == "request_not_granted"


@pytest.mark.asyncio
async def test_reversed_request_409(client):
    ds = _new_dataset_id("reversed")
    await _register(client, ds)
    await _grant(client, ds, "a", 0.1, 1e-8)
    await client.post(
        f"/datasets/{ds}/requests/a/reversals",
        json={"reversal_id": _new_reversal_id(), "reason": "x"},
    )
    r = await _attribute(client, ds, ["a"])
    assert r.status_code == 409
    assert r.json()["error"] == "request_already_reversed"


@pytest.mark.asyncio
async def test_duplicate_request_ids_422(client):
    ds = _new_dataset_id("dup")
    await _register(client, ds)
    await _grant(client, ds, "a", 0.1, 1e-8)
    r = await _attribute(client, ds, ["a", "a"])
    assert r.status_code == 422


@pytest.mark.asyncio
async def test_empty_and_oversized_lists_422(client):
    ds = _new_dataset_id("sizes")
    await _register(client, ds)
    assert (await _attribute(client, ds, [])).status_code == 422
    assert (
        await _attribute(client, ds, [f"x{i}" for i in range(11)])
    ).status_code == 422


@pytest.mark.asyncio
async def test_invalid_request_id_shape_422(client):
    ds = _new_dataset_id("badshape")
    await _register(client, ds)
    assert (await _attribute(client, ds, [""])).status_code == 422
    assert (await _attribute(client, ds, ["x" * 201])).status_code == 422
