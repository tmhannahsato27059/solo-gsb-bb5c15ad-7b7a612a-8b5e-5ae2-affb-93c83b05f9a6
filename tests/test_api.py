"""End-to-end tests against the FastAPI ASGI app."""
from __future__ import annotations

import asyncio
import math
import os
import tempfile

import pytest
import pytest_asyncio

# Point the ledger at an isolated database before importing the app.
_tmp = tempfile.mkdtemp(prefix="dpledger-test-")
os.environ["DATABASE_PATH"] = os.path.join(_tmp, "test.db")

from httpx import ASGITransport, AsyncClient  # noqa: E402

from app import db, rdp  # noqa: E402
from app.main import app  # noqa: E402


@pytest_asyncio.fixture()
async def client():
    # ASGITransport does not run lifespan/startup events automatically.
    await db.init_db()
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac


import itertools

_counter = itertools.count()


def _new_dataset_id(name: str) -> str:
    # Unique per call so a shared DB never leaks state between tests.
    return f"ds-{name}-{next(_counter)}"


async def _register(client, dataset_id: str, eps: float = 1.0, delta: float = 1e-6):
    return await client.post(
        f"/datasets/{dataset_id}",
        json={"epsilon_cap": eps, "delta_cap": delta,
              "description": "test dataset"},
    )


# ------------------------------------------------------------- basic flows --


@pytest.mark.asyncio
async def test_health(client):
    r = await client.get("/health")
    assert r.status_code == 200
    assert r.json() == {"status": "ok"}


@pytest.mark.asyncio
async def test_register_and_empty_budget(client):
    ds = _new_dataset_id("empty")
    r = await _register(client, ds, eps=1.0, delta=1e-6)
    assert r.status_code == 201
    body = r.json()
    assert body["dataset_id"] == ds
    assert body["budget"]["epsilon_consumed"] == 0.0
    assert body["budget"]["epsilon_remaining"] == 1.0
    assert body["budget"]["granted_requests"] == 0

    r2 = await client.get(f"/datasets/{ds}/budget")
    assert r2.status_code == 200
    assert r2.json()["epsilon_remaining"] == 1.0


@pytest.mark.asyncio
async def test_duplicate_dataset_registration_conflicts(client):
    ds = _new_dataset_id("dupds")
    assert (await _register(client, ds)).status_code == 201
    r = await _register(client, ds)
    assert r.status_code == 409
    assert r.json()["error"] == "dataset_already_exists"


@pytest.mark.asyncio
async def test_request_on_missing_dataset_404(client):
    r = await client.post(
        "/datasets/does-not-exist/requests",
        json={"request_id": "x", "query_type": "q", "epsilon": 0.1,
              "delta": 1e-7},
    )
    assert r.status_code == 404


@pytest.mark.asyncio
async def test_grant_then_reject_over_budget(client):
    ds = _new_dataset_id("grantrej")
    await _register(client, ds, eps=0.5, delta=1e-6)

    ok = await client.post(
        f"/datasets/{ds}/requests",
        json={"request_id": "r1", "query_type": "gaussian_mean",
              "epsilon": 0.2, "delta": 1e-7},
    )
    assert ok.status_code == 201
    body = ok.json()
    assert body["allowed"] is True
    assert body["status"] == "granted"
    assert body["idempotent_replay"] is False
    assert body["epsilon_after"] >= body["epsilon_before"]

    huge = await client.post(
        f"/datasets/{ds}/requests",
        json={"request_id": "r2", "query_type": "gaussian_mean",
              "epsilon": 5.0, "delta": 1e-7},
    )
    assert huge.status_code == 201
    h = huge.json()
    assert h["allowed"] is False
    assert h["status"] == "rejected"
    assert "budget exceeded" in h["reason"]
    # Rejection reserves nothing: balance must be unchanged.
    budget = (await client.get(f"/datasets/{ds}/budget")).json()
    assert budget["granted_requests"] == 1
    assert budget["epsilon_consumed"] == pytest.approx(h["epsilon_after"], abs=1e-12)


@pytest.mark.asyncio
async def test_pure_epsilon_dp_composition_is_additive(client):
    ds = _new_dataset_id("pure")
    await _register(client, ds, eps=1.0, delta=1e-12)

    for rid, e in [("a", 0.1), ("b", 0.2), ("c", 0.3)]:
        r = await client.post(
            f"/datasets/{ds}/requests",
            json={"request_id": rid, "query_type": "laplace",
                  "epsilon": e, "delta": 0.0},
        )
        assert r.status_code == 201
        assert r.json()["allowed"] is True

    budget = (await client.get(f"/datasets/{ds}/budget")).json()
    assert budget["granted_requests"] == 3
    assert budget["epsilon_consumed"] == pytest.approx(0.6, abs=1e-9)
    assert budget["epsilon_remaining"] == pytest.approx(0.4, abs=1e-9)


@pytest.mark.asyncio
async def test_tiny_delta_request_is_granted_not_overflow_rejected(client):
    # Regression: 1/delta overflows float64 for delta=1e-309, which used to
    # poison the RDP curve with inf and reject the request as over budget.
    # The log-domain curve keeps the projection finite (~0.1886 << cap 1.0).
    ds = _new_dataset_id("tinydelta")
    await _register(client, ds, eps=1.0, delta=1e-6)

    r = await client.post(
        f"/datasets/{ds}/requests",
        json={"request_id": "r-tiny", "query_type": "gaussian_mean",
              "epsilon": 0.1, "delta": 1e-309},
    )
    assert r.status_code == 201
    body = r.json()
    assert body["allowed"] is True
    assert body["status"] == "granted"
    _, expected, expected_order = rdp.evaluate_entries([(0.1, 1e-309)], 1e-6)
    assert body["epsilon_before"] == 0.0
    assert body["epsilon_after"] == pytest.approx(expected, abs=1e-12)
    assert body["epsilon_remaining"] == pytest.approx(1.0 - expected, abs=1e-12)
    assert body["rdp_best_order"] == expected_order

    # Same-key replay returns the stored verdict unchanged.
    replay = await client.post(
        f"/datasets/{ds}/requests",
        json={"request_id": "r-tiny", "query_type": "gaussian_mean",
              "epsilon": 0.1, "delta": 1e-309},
    )
    assert replay.status_code == 200
    assert replay.json()["idempotent_replay"] is True
    assert replay.json()["epsilon_after"] == body["epsilon_after"]

    # The live budget view reports the same corrected, finite occupancy.
    budget = (await client.get(f"/datasets/{ds}/budget")).json()
    assert budget["epsilon_consumed"] == pytest.approx(expected, abs=1e-12)
    assert budget["epsilon_remaining"] == pytest.approx(1.0 - expected, abs=1e-12)
    assert all(math.isfinite(v) for v in budget["rdp_curve"].values())


# ------------------------------------------------------------ idempotency --


@pytest.mark.asyncio
async def test_idempotent_replay_returns_same_verdict(client):
    ds = _new_dataset_id("idem")
    await _register(client, ds, eps=1.0, delta=1e-6)
    payload = {"request_id": "idem-1", "query_type": "histogram",
               "epsilon": 0.1, "delta": 1e-7}

    first = await client.post(f"/datasets/{ds}/requests", json=payload)
    assert first.status_code == 201
    assert first.json()["idempotent_replay"] is False

    for _ in range(3):
        again = await client.post(f"/datasets/{ds}/requests", json=payload)
        assert again.status_code == 200
        body = again.json()
        assert body["idempotent_replay"] is True
        assert body["allowed"] == first.json()["allowed"]
        assert body["epsilon_after"] == first.json()["epsilon_after"]

    budget = (await client.get(f"/datasets/{ds}/budget")).json()
    assert budget["granted_requests"] == 1

    listing = (await client.get(f"/datasets/{ds}/requests")).json()
    assert listing["count"] == 1


@pytest.mark.asyncio
async def test_same_request_id_different_payload_conflicts(client):
    ds = _new_dataset_id("idemconf")
    await _register(client, ds, eps=1.0, delta=1e-6)

    r1 = await client.post(
        f"/datasets/{ds}/requests",
        json={"request_id": "k", "query_type": "q1",
              "epsilon": 0.1, "delta": 1e-7},
    )
    assert r1.status_code == 201

    r2 = await client.post(
        f"/datasets/{ds}/requests",
        json={"request_id": "k", "query_type": "q2",
              "epsilon": 0.1, "delta": 1e-7},
    )
    assert r2.status_code == 409
    assert r2.json()["error"] == "request_id_conflict"


@pytest.mark.asyncio
async def test_rejected_request_is_also_idempotent(client):
    ds = _new_dataset_id("idemrej")
    await _register(client, ds, eps=0.01, delta=1e-6)
    payload = {"request_id": "big", "query_type": "q",
               "epsilon": 0.5, "delta": 1e-7}

    first = await client.post(f"/datasets/{ds}/requests", json=payload)
    assert first.json()["allowed"] is False
    second = await client.post(f"/datasets/{ds}/requests", json=payload)
    assert second.status_code == 200
    assert second.json()["idempotent_replay"] is True
    assert second.json()["allowed"] is False


# ------------------------------------------------------------- concurrency --


@pytest.mark.asyncio
async def test_concurrent_submissions_never_overshoot(client):
    ds = _new_dataset_id("conc")
    cap_eps, cap_delta = 1.0, 1e-6
    await _register(client, ds, eps=cap_eps, delta=cap_delta)

    n = 60
    payloads = [
        {"request_id": f"c-{i:03d}", "query_type": "gaussian_mean",
         "epsilon": 0.05, "delta": 1e-8}
        for i in range(n)
    ]

    results = await asyncio.gather(*[
        client.post(f"/datasets/{ds}/requests", json=p) for p in payloads
    ])
    assert all(r.status_code in (200, 201) for r in results)
    verdicts = [r.json() for r in results]
    granted = [v for v in verdicts if v["allowed"]]
    rejected = [v for v in verdicts if not v["allowed"]]
    assert len(granted) + len(rejected) == n

    # Compute independently what the server *should* have admitted.
    curves = [rdp.single_rdp_curve(0.05, 1e-8) for _ in granted]
    consumed, _ = rdp.composed_epsilon(curves, cap_delta)
    assert consumed <= cap_eps + 1e-12

    budget = (await client.get(f"/datasets/{ds}/budget")).json()
    assert budget["granted_requests"] == len(granted)
    assert budget["epsilon_consumed"] == pytest.approx(consumed, abs=1e-9)
    assert budget["epsilon_consumed"] <= cap_eps + 1e-12
    assert budget["epsilon_remaining"] >= -1e-9

    # One more request of the same size must be rejected if full, or
    # granted only if it provably fits.
    extra_curve = curves + [rdp.single_rdp_curve(0.05, 1e-8)]
    projected, _ = rdp.composed_epsilon(extra_curve, cap_delta)
    last = await client.post(
        f"/datasets/{ds}/requests",
        json={"request_id": "c-final", "query_type": "gaussian_mean",
              "epsilon": 0.05, "delta": 1e-8},
    )
    if projected > cap_eps + 1e-12:
        assert last.json()["allowed"] is False
    else:
        assert last.json()["allowed"] is True


@pytest.mark.asyncio
async def test_concurrent_duplicate_request_ids_are_serialised(client):
    ds = _new_dataset_id("concidem")
    await _register(client, ds, eps=1.0, delta=1e-6)
    payload = {"request_id": "same", "query_type": "q",
               "epsilon": 0.1, "delta": 1e-7}

    results = await asyncio.gather(*[
        client.post(f"/datasets/{ds}/requests", json=payload)
        for _ in range(20)
    ])
    created = [r for r in results if r.status_code == 201]
    replays = [r for r in results if r.status_code == 200]
    assert len(created) == 1
    assert len(replays) == 19
    budget = (await client.get(f"/datasets/{ds}/budget")).json()
    assert budget["granted_requests"] == 1
