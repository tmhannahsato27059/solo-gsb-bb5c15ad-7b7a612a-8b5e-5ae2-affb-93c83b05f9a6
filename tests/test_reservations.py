"""End-to-end tests for budget reservations."""
from __future__ import annotations

import asyncio
import dataclasses
import os
import tempfile
from datetime import datetime, timedelta, timezone

import aiosqlite
import pytest
import pytest_asyncio

# Point the ledger at an isolated database before importing the app.
_tmp = tempfile.mkdtemp(prefix="dpledger-test-res-")
os.environ["DATABASE_PATH"] = os.path.join(_tmp, "reservations.db")

from httpx import ASGITransport, AsyncClient  # noqa: E402

from app import db, rdp  # noqa: E402
from app.config import settings  # noqa: E402
from app.main import app  # noqa: E402

import itertools

_counter = itertools.count()


def _new_dataset_id(name: str) -> str:
    return f"dsres-{name}-{next(_counter)}"


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


def _res(reservation_id: str, epsilon: float = 0.1,
         delta: float = 1e-7, ttl: int = 600) -> dict:
    return {
        "reservation_id": reservation_id,
        "epsilon": epsilon,
        "delta": delta,
        "ttl_seconds": ttl,
    }


async def _backdate(client_unused, dataset_id: str, reservation_id: str,
                    expires_at: datetime) -> None:
    """Force a reservation's expiry instant (expiry is TTL based in production)."""
    async with db.connect() as conn:
        await db.backdate_reservation_expiry(
            conn,
            dataset_id=dataset_id,
            reservation_id=reservation_id,
            expires_at=expires_at.isoformat(),
        )
        await conn.commit()


# ------------------------------------------------------------- create flows --


@pytest.mark.asyncio
async def test_granted_reservation_locks_budget(client):
    ds = _new_dataset_id("lock")
    await _register(client, ds, eps=1.0, delta=1e-6)

    r = await client.post(
        f"/datasets/{ds}/reservations", json=_res("rv1", epsilon=0.2)
    )
    assert r.status_code == 201
    body = r.json()
    assert body["allowed"] is True
    assert body["status"] == "granted"
    assert body["idempotent_replay"] is False
    assert body["epsilon_after"] > body["epsilon_before"]
    assert body["epsilon_remaining"] < 1.0
    assert body["expires_at"] is not None

    created = datetime.fromisoformat(body["created_at"])
    expires = datetime.fromisoformat(body["expires_at"])
    assert (expires - created) == timedelta(seconds=body["ttl_seconds"])

    budget = (await client.get(f"/datasets/{ds}/budget")).json()
    assert budget["granted_requests"] == 0
    assert budget["active_reservations"] == 1
    assert budget["epsilon_consumed"] == 0.0
    assert budget["epsilon_reserved"] > 0.0
    assert (
        budget["epsilon_remaining"]
        == pytest.approx(body["epsilon_remaining"], abs=1e-9)
    )


@pytest.mark.asyncio
async def test_tiny_delta_reservation_is_granted_not_overflow_rejected(client):
    # Regression: 1/delta overflows float64 for delta=1e-309; the log-domain
    # curve keeps the projection finite so the reservation locks its budget.
    ds = _new_dataset_id("tinydelta")
    await _register(client, ds, eps=1.0, delta=1e-6)

    r = await client.post(
        f"/datasets/{ds}/reservations",
        json=_res("rv-tiny", epsilon=0.1, delta=1e-309),
    )
    assert r.status_code == 201
    body = r.json()
    assert body["allowed"] is True
    assert body["status"] == "granted"
    _, expected, _ = rdp.evaluate_entries([(0.1, 1e-309)], 1e-6)
    assert body["epsilon_after"] == pytest.approx(expected, abs=1e-12)
    assert body["epsilon_remaining"] == pytest.approx(1.0 - expected, abs=1e-12)

    budget = (await client.get(f"/datasets/{ds}/budget")).json()
    assert budget["active_reservations"] == 1
    assert budget["epsilon_reserved"] == pytest.approx(expected, abs=1e-12)
    assert budget["epsilon_remaining"] == pytest.approx(1.0 - expected, abs=1e-12)

    # Confirming turns the same finite occupancy into a granted request.
    conf = await client.post(
        f"/datasets/{ds}/reservations/rv-tiny/confirm",
        json={"request_id": "req-tiny", "query_type": "gaussian_mean"},
    )
    assert conf.status_code == 201
    assert conf.json()["allowed"] is True
    assert conf.json()["epsilon_after"] == pytest.approx(expected, abs=1e-12)


@pytest.mark.asyncio
async def test_over_budget_reservation_is_rejected_and_locks_nothing(client):
    ds = _new_dataset_id("rej")
    await _register(client, ds, eps=0.01, delta=1e-6)

    r = await client.post(
        f"/datasets/{ds}/reservations", json=_res("big", epsilon=0.5)
    )
    assert r.status_code == 201
    body = r.json()
    assert body["allowed"] is False
    assert body["status"] == "rejected"
    assert body["expires_at"] is None
    assert "budget exceeded" in body["reason"]

    budget = (await client.get(f"/datasets/{ds}/budget")).json()
    assert budget["active_reservations"] == 0
    assert budget["epsilon_consumed"] == 0.0
    assert budget["epsilon_remaining"] == pytest.approx(0.01)


@pytest.mark.asyncio
async def test_reservation_blocks_direct_request_until_released(client):
    ds = _new_dataset_id("block")
    await _register(client, ds, eps=0.3, delta=1e-6)

    rv = await client.post(
        f"/datasets/{ds}/reservations", json=_res("rv", epsilon=0.25)
    )
    assert rv.json()["allowed"] is True

    blocked = await client.post(
        f"/datasets/{ds}/requests",
        json={"request_id": "q1", "query_type": "q",
              "epsilon": 0.2, "delta": 1e-7},
    )
    assert blocked.json()["allowed"] is False

    released = await client.post(
        f"/datasets/{ds}/reservations/rv/release"
    )
    assert released.status_code == 200
    assert released.json()["status"] == "released"
    assert released.json()["released_at"] is not None

    ok = await client.post(
        f"/datasets/{ds}/requests",
        json={"request_id": "q2", "query_type": "q",
              "epsilon": 0.2, "delta": 1e-7},
    )
    assert ok.status_code == 201
    assert ok.json()["allowed"] is True
    # The earlier rejected request remains a rejected, idempotent record.
    replay = await client.post(
        f"/datasets/{ds}/requests",
        json={"request_id": "q1", "query_type": "q",
              "epsilon": 0.2, "delta": 1e-7},
    )
    assert replay.status_code == 200
    assert replay.json()["allowed"] is False
    budget = (await client.get(f"/datasets/{ds}/budget")).json()
    assert budget["active_reservations"] == 0
    assert budget["granted_requests"] == 1


@pytest.mark.asyncio
async def test_reservation_on_missing_dataset_404(client):
    r = await client.post(
        "/datasets/no-such-dataset/reservations", json=_res("x")
    )
    assert r.status_code == 404


@pytest.mark.asyncio
async def test_validation_ttl_bounds(client):
    ds = _new_dataset_id("ttl")
    await _register(client, ds)
    bad = await client.post(
        f"/datasets/{ds}/reservations", json=_res("x", ttl=0)
    )
    assert bad.status_code == 422
    bad2 = await client.post(
        f"/datasets/{ds}/reservations", json=_res("y", ttl=10**9)
    )
    assert bad2.status_code == 422


# ------------------------------------------------------------ idempotency --


@pytest.mark.asyncio
async def test_reservation_replay_is_idempotent(client):
    ds = _new_dataset_id("idem")
    await _register(client, ds, eps=1.0, delta=1e-6)
    payload = _res("rv", epsilon=0.1, ttl=120)

    first = await client.post(f"/datasets/{ds}/reservations", json=payload)
    assert first.status_code == 201
    assert first.json()["idempotent_replay"] is False

    for _ in range(3):
        again = await client.post(f"/datasets/{ds}/reservations", json=payload)
        assert again.status_code == 200
        body = again.json()
        assert body["idempotent_replay"] is True
        assert body["allowed"] == first.json()["allowed"]
        assert body["epsilon_after"] == first.json()["epsilon_after"]
        assert body["created_at"] == first.json()["created_at"]

    budget = (await client.get(f"/datasets/{ds}/budget")).json()
    assert budget["active_reservations"] == 1


@pytest.mark.asyncio
async def test_same_reservation_id_different_payload_conflicts(client):
    ds = _new_dataset_id("idemconf")
    await _register(client, ds, eps=1.0, delta=1e-6)
    base = _res("k", epsilon=0.1, ttl=60)
    assert (
        await client.post(f"/datasets/{ds}/reservations", json=base)
    ).status_code == 201

    for changed in ("epsilon", "delta", "ttl_seconds"):
        p = dict(base)
        if changed == "epsilon":
            p["epsilon"] = 0.2
        elif changed == "delta":
            p["delta"] = 1e-6
        else:
            p["ttl_seconds"] = 120
        r = await client.post(f"/datasets/{ds}/reservations", json=p)
        assert r.status_code == 409, (changed, r.status_code, r.text)
        assert r.json()["error"] == "reservation_id_conflict"


@pytest.mark.asyncio
async def test_rejected_reservation_is_also_idempotent(client):
    ds = _new_dataset_id("idemrej")
    await _register(client, ds, eps=0.01, delta=1e-6)
    payload = _res("big", epsilon=0.5)

    first = await client.post(f"/datasets/{ds}/reservations", json=payload)
    assert first.json()["allowed"] is False
    second = await client.post(f"/datasets/{ds}/reservations", json=payload)
    assert second.status_code == 200
    assert second.json()["idempotent_replay"] is True
    assert second.json()["allowed"] is False


# --------------------------------------------------------------- confirm --


@pytest.mark.asyncio
async def test_confirm_turns_reservation_into_granted_request(client):
    ds = _new_dataset_id("confirm")
    await _register(client, ds, eps=1.0, delta=1e-6)

    rv = await client.post(
        f"/datasets/{ds}/reservations", json=_res("rv", epsilon=0.2)
    )
    assert rv.json()["allowed"] is True
    committed_before = rv.json()["epsilon_after"]

    verdict = await client.post(
        f"/datasets/{ds}/reservations/rv/confirm",
        json={"request_id": "req-1", "query_type": "gaussian_mean"},
    )
    assert verdict.status_code == 201
    v = verdict.json()
    assert v["allowed"] is True
    assert v["status"] == "granted"
    assert v["request_id"] == "req-1"
    assert v["query_type"] == "gaussian_mean"
    assert v["epsilon"] == pytest.approx(0.2)
    # Confirmation must not double-charge the reserved budget.
    assert v["epsilon_after"] == pytest.approx(committed_before, abs=1e-9)
    assert v["epsilon_before"] <= v["epsilon_after"]

    got = await client.get(f"/datasets/{ds}/reservations/rv")
    assert got.json()["status"] == "confirmed"
    assert got.json()["confirmed_request_id"] == "req-1"
    assert got.json()["confirmed_at"] is not None

    budget = (await client.get(f"/datasets/{ds}/budget")).json()
    assert budget["active_reservations"] == 0
    assert budget["granted_requests"] == 1
    assert budget["epsilon_consumed"] == pytest.approx(
        committed_before, abs=1e-9
    )
    assert budget["epsilon_reserved"] == pytest.approx(0.0, abs=1e-12)

    listing = (await client.get(f"/datasets/{ds}/requests")).json()
    assert listing["count"] == 1
    assert listing["requests"][0]["request_id"] == "req-1"


@pytest.mark.asyncio
async def test_confirm_is_idempotent(client):
    ds = _new_dataset_id("confirmidem")
    await _register(client, ds, eps=1.0, delta=1e-6)
    await client.post(
        f"/datasets/{ds}/reservations", json=_res("rv", epsilon=0.1)
    )
    body = {"request_id": "req-1", "query_type": "q"}

    first = await client.post(
        f"/datasets/{ds}/reservations/rv/confirm", json=body
    )
    assert first.status_code == 201
    for _ in range(3):
        again = await client.post(
            f"/datasets/{ds}/reservations/rv/confirm", json=body
        )
        assert again.status_code == 200
        assert again.json()["idempotent_replay"] is True
        assert again.json()["request_id"] == "req-1"

    assert (await client.get(f"/datasets/{ds}/requests")).json()["count"] == 1


@pytest.mark.asyncio
async def test_confirm_conflicting_request_id_returns_409(client):
    ds = _new_dataset_id("confirmconf")
    await _register(client, ds, eps=1.0, delta=1e-6)
    await client.post(
        f"/datasets/{ds}/reservations", json=_res("rv", epsilon=0.1)
    )
    # request_id already used by a direct request.
    direct = await client.post(
        f"/datasets/{ds}/requests",
        json={"request_id": "used", "query_type": "q",
              "epsilon": 0.05, "delta": 1e-8},
    )
    assert direct.status_code == 201

    r = await client.post(
        f"/datasets/{ds}/reservations/rv/confirm",
        json={"request_id": "used", "query_type": "q"},
    )
    assert r.status_code == 409
    assert r.json()["error"] == "request_id_conflict"
    # Reservation stays active and still holds its budget.
    assert (await client.get(
        f"/datasets/{ds}/reservations/rv")).json()["status"] == "granted"


@pytest.mark.asyncio
async def test_confirm_replay_with_different_payload_conflicts(client):
    ds = _new_dataset_id("confirmdiff")
    await _register(client, ds, eps=1.0, delta=1e-6)
    await client.post(
        f"/datasets/{ds}/reservations", json=_res("rv", epsilon=0.1)
    )
    first = await client.post(
        f"/datasets/{ds}/reservations/rv/confirm",
        json={"request_id": "req-1", "query_type": "q1"},
    )
    assert first.status_code == 201

    other = await client.post(
        f"/datasets/{ds}/reservations/rv/confirm",
        json={"request_id": "req-2", "query_type": "q2"},
    )
    assert other.status_code == 409


@pytest.mark.asyncio
async def test_confirm_unknown_or_terminal_reservation(client):
    ds = _new_dataset_id("confirmstate")
    await _register(client, ds, eps=1.0, delta=1e-6)

    missing = await client.post(
        f"/datasets/{ds}/reservations/nope/confirm",
        json={"request_id": "x", "query_type": "q"},
    )
    assert missing.status_code == 404
    assert missing.json()["error"] == "reservation_not_found"

    rejected = await client.post(
        f"/datasets/{ds}/reservations", json=_res("bad", epsilon=5.0)
    )
    assert rejected.json()["status"] == "rejected"
    r = await client.post(
        f"/datasets/{ds}/reservations/bad/confirm",
        json={"request_id": "x", "query_type": "q"},
    )
    assert r.status_code == 409
    assert r.json()["error"] == "reservation_rejected"


# --------------------------------------------------------------- release --


@pytest.mark.asyncio
async def test_release_is_idempotent(client):
    ds = _new_dataset_id("releaseidem")
    await _register(client, ds, eps=1.0, delta=1e-6)
    await client.post(
        f"/datasets/{ds}/reservations", json=_res("rv", epsilon=0.1)
    )

    first = await client.post(f"/datasets/{ds}/reservations/rv/release")
    assert first.status_code == 200
    assert first.json()["idempotent_replay"] is False

    for _ in range(3):
        again = await client.post(f"/datasets/{ds}/reservations/rv/release")
        assert again.status_code == 200
        assert again.json()["status"] == "released"
        assert again.json()["idempotent_replay"] is True

    budget = (await client.get(f"/datasets/{ds}/budget")).json()
    assert budget["active_reservations"] == 0
    assert budget["epsilon_remaining"] == pytest.approx(1.0)


@pytest.mark.asyncio
async def test_release_terminal_states(client):
    ds = _new_dataset_id("releasestate")
    await _register(client, ds, eps=1.0, delta=1e-6)

    # Confirmed reservations are consumed and cannot be released.
    await client.post(
        f"/datasets/{ds}/reservations", json=_res("rv", epsilon=0.1)
    )
    await client.post(
        f"/datasets/{ds}/reservations/rv/confirm",
        json={"request_id": "req", "query_type": "q"},
    )
    r = await client.post(f"/datasets/{ds}/reservations/rv/release")
    assert r.status_code == 409
    assert r.json()["error"] == "reservation_confirmed"

    # Rejected reservations never held budget.
    await client.post(
        f"/datasets/{ds}/reservations", json=_res("bad", epsilon=5.0)
    )
    r2 = await client.post(f"/datasets/{ds}/reservations/bad/release")
    assert r2.status_code == 409
    assert r2.json()["error"] == "reservation_rejected"

    missing = await client.post(
        f"/datasets/{ds}/reservations/nope/release"
    )
    assert missing.status_code == 404


# ---------------------------------------------------------------- expiry --


@pytest.mark.asyncio
async def test_expired_reservation_stops_occupying_budget(client):
    ds = _new_dataset_id("expire")
    await _register(client, ds, eps=0.3, delta=1e-6)

    await client.post(
        f"/datasets/{ds}/reservations", json=_res("rv", epsilon=0.25, ttl=10)
    )
    past = datetime.now(timezone.utc) - timedelta(seconds=1)
    await _backdate(client, ds, "rv", past)

    # GET reports status expired and frees the budget.
    got = await client.get(f"/datasets/{ds}/reservations/rv")
    assert got.json()["status"] == "expired"
    assert got.json()["expired_at"] is not None

    budget = (await client.get(f"/datasets/{ds}/budget")).json()
    assert budget["active_reservations"] == 0
    assert budget["epsilon_reserved"] == pytest.approx(0.0, abs=1e-12)
    assert budget["epsilon_remaining"] == pytest.approx(0.3)

    # A new reservation now sees the freed capacity.
    ok = await client.post(
        f"/datasets/{ds}/reservations", json=_res("rv2", epsilon=0.25)
    )
    assert ok.json()["allowed"] is True


@pytest.mark.asyncio
async def test_expired_reservation_swept_on_create(client):
    ds = _new_dataset_id("sweep")
    await _register(client, ds, eps=0.3, delta=1e-6)
    await client.post(
        f"/datasets/{ds}/reservations", json=_res("old", epsilon=0.25, ttl=1)
    )
    past = datetime.now(timezone.utc) - timedelta(seconds=10)
    await _backdate(client, ds, "old", past)

    fresh = await client.post(
        f"/datasets/{ds}/reservations", json=_res("new", epsilon=0.25)
    )
    assert fresh.status_code == 201
    assert fresh.json()["allowed"] is True

    listing = (await client.get(f"/datasets/{ds}/reservations")).json()
    statuses = {r["reservation_id"]: r["status"] for r in listing["reservations"]}
    assert statuses["old"] == "expired"
    assert statuses["new"] == "granted"


@pytest.mark.asyncio
async def test_expired_reservation_cannot_be_confirmed_but_release_is_idempotent(client):
    ds = _new_dataset_id("expirestate")
    await _register(client, ds, eps=1.0, delta=1e-6)
    await client.post(
        f"/datasets/{ds}/reservations", json=_res("rv", epsilon=0.1, ttl=1)
    )
    past = datetime.now(timezone.utc) - timedelta(seconds=1)
    await _backdate(client, ds, "rv", past)

    confirm = await client.post(
        f"/datasets/{ds}/reservations/rv/confirm",
        json={"request_id": "req", "query_type": "q"},
    )
    assert confirm.status_code == 409
    assert confirm.json()["error"] == "reservation_expired"

    release = await client.post(f"/datasets/{ds}/reservations/rv/release")
    assert release.status_code == 200
    assert release.json()["status"] == "expired"
    assert release.json()["idempotent_replay"] is True


# ------------------------------------------------------------- concurrency --


def _committed_epsilon(budget_rows, cap_delta):
    """Independent RDP accounting over granted requests + active reservations."""
    curves = [rdp.single_rdp_curve(eps, d) for eps, d in budget_rows]
    consumed, _ = rdp.composed_epsilon(curves, cap_delta)
    return consumed


@pytest.mark.asyncio
async def test_concurrent_reservations_never_overshoot(client):
    ds = _new_dataset_id("concrv")
    cap_eps, cap_delta = 1.0, 1e-6
    await _register(client, ds, eps=cap_eps, delta=cap_delta)

    n = 60
    payloads = [
        _res(f"rv-{i:03d}", epsilon=0.05, delta=1e-8, ttl=3600)
        for i in range(n)
    ]
    results = await asyncio.gather(*[
        client.post(f"/datasets/{ds}/reservations", json=p) for p in payloads
    ])
    assert all(r.status_code in (200, 201) for r in results)
    granted = [r.json() for r in results if r.json()["allowed"]]

    budget = (await client.get(f"/datasets/{ds}/budget")).json()
    assert budget["active_reservations"] == len(granted)
    assert budget["epsilon_consumed"] == 0.0
    committed = _committed_epsilon(
        [(g["epsilon"], g["delta"]) for g in granted], cap_delta
    )
    assert committed <= cap_eps + 1e-12
    assert budget["epsilon_remaining"] >= -1e-9


@pytest.mark.asyncio
async def test_concurrent_duplicate_reservation_ids_serialised(client):
    ds = _new_dataset_id("concrvidem")
    await _register(client, ds, eps=1.0, delta=1e-6)
    payload = _res("same", epsilon=0.1)

    results = await asyncio.gather(*[
        client.post(f"/datasets/{ds}/reservations", json=payload)
        for _ in range(20)
    ])
    created = [r for r in results if r.status_code == 201]
    replays = [r for r in results if r.status_code == 200]
    assert len(created) == 1
    assert len(replays) == 19
    budget = (await client.get(f"/datasets/{ds}/budget")).json()
    assert budget["active_reservations"] == 1


@pytest.mark.asyncio
async def test_concurrent_confirms_serialised_and_never_double_charged(client):
    ds = _new_dataset_id("concconfirm")
    cap_eps, cap_delta = 1.0, 1e-6
    await _register(client, ds, eps=cap_eps, delta=cap_delta)
    await client.post(
        f"/datasets/{ds}/reservations", json=_res("rv", epsilon=0.2)
    )
    payload = {"request_id": "req-1", "query_type": "q"}

    results = await asyncio.gather(*[
        client.post(f"/datasets/{ds}/reservations/rv/confirm", json=payload)
        for _ in range(15)
    ])
    created = [r for r in results if r.status_code == 201]
    replays = [r for r in results if r.status_code == 200]
    assert len(created) == 1
    assert len(replays) == 14

    budget = (await client.get(f"/datasets/{ds}/budget")).json()
    assert budget["granted_requests"] == 1
    assert budget["active_reservations"] == 0
    assert budget["epsilon_consumed"] <= cap_eps + 1e-12
    assert (await client.get(f"/datasets/{ds}/requests")).json()["count"] == 1


@pytest.mark.asyncio
async def test_mixed_concurrent_reserve_confirm_release_stays_within_cap(client):
    ds = _new_dataset_id("concmix")
    cap_eps, cap_delta = 1.0, 1e-6
    await _register(client, ds, eps=cap_eps, delta=cap_delta)

    # 40 reservations race; then half are confirmed, a quarter released.
    n = 40
    reserves = await asyncio.gather(*[
        client.post(
            f"/datasets/{ds}/reservations",
            json=_res(f"rv-{i:03d}", epsilon=0.05, delta=1e-8, ttl=3600),
        )
        for i in range(n)
    ])
    granted_ids = [
        r.json()["reservation_id"]
        for r in reserves if r.json()["allowed"]
    ]
    assert granted_ids, "expected at least some grants"

    to_confirm = granted_ids[::2]
    to_release = granted_ids[1::2]

    async def confirm(rid, i):
        return await client.post(
            f"/datasets/{ds}/reservations/{rid}/confirm",
            json={"request_id": f"req-{i:03d}", "query_type": "q"},
        )

    async def release(rid):
        return await client.post(
            f"/datasets/{ds}/reservations/{rid}/release"
        )

    mixed = await asyncio.gather(
        *[confirm(rid, i) for i, rid in enumerate(to_confirm)],
        *[release(rid) for rid in to_release],
    )
    assert all(r.status_code in (200, 201) for r in mixed)

    budget = (await client.get(f"/datasets/{ds}/budget")).json()
    assert budget["active_reservations"] == 0
    assert budget["granted_requests"] == len(to_confirm)
    assert budget["epsilon_consumed"] <= cap_eps + 1e-12
    assert budget["epsilon_remaining"] >= -1e-9

    requests = (await client.get(f"/datasets/{ds}/requests")).json()
    assert requests["count"] == len(to_confirm)


@pytest.mark.asyncio
async def test_direct_requests_race_with_reservations_within_cap(client):
    ds = _new_dataset_id("concdirect")
    cap_eps, cap_delta = 1.0, 1e-6
    await _register(client, ds, eps=cap_eps, delta=cap_delta)

    async def reserve(i):
        return await client.post(
            f"/datasets/{ds}/reservations",
            json=_res(f"rv-{i:02d}", epsilon=0.05, delta=1e-8, ttl=3600),
        )

    async def request(i):
        return await client.post(
            f"/datasets/{ds}/requests",
            json={"request_id": f"req-{i:02d}", "query_type": "q",
                  "epsilon": 0.05, "delta": 1e-8},
        )

    results = await asyncio.gather(
        *[reserve(i) for i in range(30)],
        *[request(i) for i in range(30)],
    )
    assert all(r.status_code in (200, 201) for r in results)

    budget = (await client.get(f"/datasets/{ds}/budget")).json()
    active = [
        (r["epsilon"], r["delta"])
        for r in (await client.get(f"/datasets/{ds}/reservations")).json()["reservations"]
        if r["status"] == "granted"
    ]
    requests_rows = [
        (r["epsilon"], r["delta"])
        for r in (await client.get(f"/datasets/{ds}/requests")).json()["requests"]
        if r["status"] == "granted"
    ]
    committed = _committed_epsilon(active + requests_rows, cap_delta)
    assert committed <= cap_eps + 1e-12
    assert budget["active_reservations"] == len(active)


# ------------------------------------------------------------- migration --


@pytest.mark.asyncio
async def test_migration_from_v1_schema(tmp_path, monkeypatch):
    old_db_path = str(tmp_path / "old.db")

    # Build a database with the pre-reservation schema and some data.
    conn = await aiosqlite.connect(old_db_path)
    try:
        await conn.executescript(db.SCHEMA_V1)
        await conn.execute(
            "INSERT INTO datasets "
            "(dataset_id, epsilon_cap, delta_cap, description, created_at) "
            "VALUES ('legacy', 1.0, 1e-6, 'old', '2026-01-01T00:00:00+00:00')"
        )
        await conn.execute(
            "INSERT INTO requests "
            "(dataset_id, request_id, query_type, epsilon, delta, status, "
            "reason, epsilon_before, epsilon_after, rdp_best_order, created_at) "
            "VALUES ('legacy', 'r1', 'q', 0.1, 0.0, 'granted', NULL, "
            "0.0, 0.1, 1.1, '2026-01-01T00:00:01+00:00')"
        )
        await conn.commit()
    finally:
        await conn.close()

    patched = dataclasses.replace(settings, db_path=old_db_path)
    monkeypatch.setattr(db, "settings", patched)
    await db.init_db()

    async with db.connect() as c:
        async with c.execute("PRAGMA user_version") as cur:
            version = (await cur.fetchone())[0]
        assert version == db.LATEST_VERSION
        async with c.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='reservations'"
        ) as cur:
            assert await cur.fetchone() is not None
        dataset = await db.require_dataset(c, "legacy")
        assert dataset["epsilon_cap"] == 1.0
        entries = await db.list_granted_entries(c, "legacy")
        assert entries == [(0.1, 0.0)]
        active = await db.list_active_reservation_entries(
            c, "legacy", now_iso=datetime.now(timezone.utc).isoformat()
        )
        assert active == []
