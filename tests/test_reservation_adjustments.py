"""End-to-end tests for reservation quota adjustments."""
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
_tmp = tempfile.mkdtemp(prefix="dpledger-test-adjustment-")
os.environ["DATABASE_PATH"] = os.path.join(_tmp, "adjustments.db")

from httpx import ASGITransport, AsyncClient  # noqa: E402

from app import db, rdp  # noqa: E402
from app.config import settings  # noqa: E402
from app.main import app  # noqa: E402

import itertools

_counter = itertools.count()


def _new_dataset_id(name: str) -> str:
    return f"dsadj-{name}-{next(_counter)}"


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


def _adj(adjustment_id: str, epsilon: float = 0.2,
         delta: float = 1e-7) -> dict:
    return {
        "adjustment_id": adjustment_id,
        "epsilon": epsilon,
        "delta": delta,
    }


async def _backdate(dataset_id: str, reservation_id: str,
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


async def _budget(client, ds: str) -> dict:
    return (await client.get(f"/datasets/{ds}/budget")).json()


# ------------------------------------------------------------- happy path --


@pytest.mark.asyncio
async def test_adjustment_replaces_live_quota(client):
    ds = _new_dataset_id("replace")
    await _register(client, ds, eps=1.0, delta=1e-6)
    await client.post(f"/datasets/{ds}/reservations", json=_res("rv", epsilon=0.1))

    r = await client.post(
        f"/datasets/{ds}/reservations/rv/adjustments",
        json=_adj("adj-1", epsilon=0.2),
    )
    assert r.status_code == 201
    body = r.json()
    assert body["idempotent_replay"] is False
    assert body["allowed"] is True
    assert body["status"] == "adjusted"
    assert body["reason"] is None
    assert body["epsilon_before"] == pytest.approx(0.1)
    assert body["delta_before"] == pytest.approx(1e-7)
    assert body["requested_epsilon"] == pytest.approx(0.2)
    assert body["requested_delta"] == pytest.approx(1e-7)
    assert body["epsilon_after"] == pytest.approx(0.2)
    assert body["delta_after"] == pytest.approx(1e-7)
    assert body["epsilon_committed_before"] > 0.0
    assert body["epsilon_committed_after"] > body["epsilon_committed_before"]
    assert body["epsilon_remaining"] == pytest.approx(
        body["epsilon_cap"] - body["epsilon_committed_after"]
    )

    got = (await client.get(f"/datasets/{ds}/reservations/rv")).json()
    assert got["epsilon"] == pytest.approx(0.2)
    assert got["delta"] == pytest.approx(1e-7)
    assert got["ttl_seconds"] == 600
    assert got["status"] == "granted"
    assert body["created_at"] >= got["created_at"]


@pytest.mark.asyncio
async def test_adjustment_accounting_matches_rdp_module(client):
    ds = _new_dataset_id("rdpcross")
    await _register(client, ds, eps=2.0, delta=1e-6)
    # Two granted requests plus two active reservations make a non-trivial set.
    await client.post(
        f"/datasets/{ds}/requests",
        json={"request_id": "req-a", "query_type": "q",
              "epsilon": 0.15, "delta": 1e-8},
    )
    await client.post(
        f"/datasets/{ds}/requests",
        json={"request_id": "req-b", "query_type": "q",
              "epsilon": 0.1, "delta": 0.0},
    )
    await client.post(f"/datasets/{ds}/reservations", json=_res("rv-1", 0.2, 1e-9))
    await client.post(f"/datasets/{ds}/reservations", json=_res("rv-2", 0.05, 0.0))

    new_epsilon, new_delta = 0.3, 1e-7
    r = await client.post(
        f"/datasets/{ds}/reservations/rv-1/adjustments",
        json=_adj("adj-1", new_epsilon, new_delta),
    )
    assert r.status_code == 201
    assert r.json()["allowed"] is True

    # Independent recomposition with rdp.py of the live set after the swap.
    expected_entries = [
        (0.15, 1e-8),
        (0.1, 0.0),
        (new_epsilon, new_delta),
        (0.05, 0.0),
    ]
    _, expected_after, expected_order = rdp.evaluate_entries(
        expected_entries, 1e-6
    )
    body = r.json()
    assert body["epsilon_committed_after"] == pytest.approx(
        expected_after, abs=1e-12
    )
    assert body["rdp_best_order"] == pytest.approx(expected_order)

    budget = await _budget(client, ds)
    assert budget["active_reservations"] == 2
    assert budget["granted_requests"] == 2
    assert budget["epsilon_reserved"] > 0.0
    assert budget["epsilon_remaining"] == pytest.approx(
        2.0 - expected_after, abs=1e-9
    )


@pytest.mark.asyncio
async def test_adjustment_downwards_releases_budget(client):
    ds = _new_dataset_id("down")
    await _register(client, ds, eps=1.0, delta=1e-6)
    await client.post(f"/datasets/{ds}/reservations", json=_res("rv", epsilon=0.4))
    before = await _budget(client, ds)

    r = await client.post(
        f"/datasets/{ds}/reservations/rv/adjustments",
        json=_adj("adj-1", epsilon=0.1),
    )
    assert r.status_code == 201
    assert r.json()["allowed"] is True
    assert r.json()["epsilon_committed_after"] < r.json()["epsilon_committed_before"]

    after = await _budget(client, ds)
    assert after["epsilon_remaining"] > before["epsilon_remaining"]

    # The freed room can be taken by a fresh request.
    req = await client.post(
        f"/datasets/{ds}/requests",
        json={"request_id": "req-new", "query_type": "q",
              "epsilon": 0.2, "delta": 1e-7},
    )
    assert req.status_code == 201
    assert req.json()["allowed"] is True


@pytest.mark.asyncio
async def test_adjustment_can_change_delta(client):
    ds = _new_dataset_id("chdelta")
    await _register(client, ds, eps=1.0, delta=1e-6)
    # Pure-epsilon reservation, then given an approximate-DP delta.
    await client.post(
        f"/datasets/{ds}/reservations", json=_res("rv", epsilon=0.2, delta=0.0)
    )
    r = await client.post(
        f"/datasets/{ds}/reservations/rv/adjustments",
        json=_adj("adj-1", epsilon=0.2, delta=1e-7),
    )
    assert r.status_code == 201
    assert r.json()["allowed"] is True

    got = (await client.get(f"/datasets/{ds}/reservations/rv")).json()
    assert got["delta"] == pytest.approx(1e-7)

    # Back to pure epsilon with a smaller epsilon.
    r2 = await client.post(
        f"/datasets/{ds}/reservations/rv/adjustments",
        json=_adj("adj-2", epsilon=0.1, delta=0.0),
    )
    assert r2.status_code == 201
    assert r2.json()["delta_after"] == pytest.approx(0.0)
    assert r2.json()["epsilon_committed_after"] == pytest.approx(0.1)


@pytest.mark.asyncio
async def test_chained_adjustments_replace_step_by_step(client):
    ds = _new_dataset_id("chain")
    await _register(client, ds, eps=1.0, delta=1e-6)
    await client.post(f"/datasets/{ds}/reservations", json=_res("rv", 0.1))

    first = await client.post(
        f"/datasets/{ds}/reservations/rv/adjustments",
        json=_adj("adj-1", epsilon=0.2),
    )
    assert first.status_code == 201
    second = await client.post(
        f"/datasets/{ds}/reservations/rv/adjustments",
        json=_adj("adj-2", epsilon=0.3),
    )
    assert second.status_code == 201
    assert second.json()["epsilon_before"] == pytest.approx(0.2)
    assert second.json()["epsilon_after"] == pytest.approx(0.3)

    got = (await client.get(f"/datasets/{ds}/reservations/rv")).json()
    assert got["epsilon"] == pytest.approx(0.3)
    assert got["delta"] == pytest.approx(1e-7)


# --------------------------------------------------------------- rejection --


@pytest.mark.asyncio
async def test_over_cap_adjustment_is_rejected_and_recorded(client):
    ds = _new_dataset_id("overcap")
    await _register(client, ds, eps=1.0, delta=1e-6)
    created = await client.post(
        f"/datasets/{ds}/reservations", json=_res("rv", epsilon=0.2)
    )
    original_expires = created.json()["expires_at"]
    budget_before = await _budget(client, ds)

    r = await client.post(
        f"/datasets/{ds}/reservations/rv/adjustments",
        json=_adj("adj-1", epsilon=5.0),
    )
    assert r.status_code == 201
    body = r.json()
    assert body["allowed"] is False
    assert body["status"] == "rejected"
    assert body["reason"] is not None
    assert "budget" in body["reason"].lower() or "cap" in body["reason"].lower()
    # Quota snapshots are unchanged on rejection.
    assert body["epsilon_before"] == pytest.approx(0.2)
    assert body["epsilon_after"] == pytest.approx(0.2)
    assert body["delta_after"] == pytest.approx(1e-7)
    assert body["requested_epsilon"] == pytest.approx(5.0)
    assert body["epsilon_committed_after"] == pytest.approx(
        body["epsilon_committed_before"]
    )
    assert body["epsilon_remaining"] == pytest.approx(
        body["epsilon_cap"] - body["epsilon_committed_before"]
    )

    # The reservation row itself kept its original quota and expiry.
    got = (await client.get(f"/datasets/{ds}/reservations/rv")).json()
    assert got["epsilon"] == pytest.approx(0.2)
    assert got["delta"] == pytest.approx(1e-7)
    assert datetime.fromisoformat(got["expires_at"]) == datetime.fromisoformat(
        original_expires
    )

    budget_after = await _budget(client, ds)
    assert budget_after["epsilon_remaining"] == pytest.approx(
        budget_before["epsilon_remaining"], abs=1e-12
    )


@pytest.mark.asyncio
async def test_other_valid_reservations_count_into_adjustment_cap(client):
    ds = _new_dataset_id("others")
    await _register(client, ds, eps=1.0, delta=1e-6)
    await client.post(f"/datasets/{ds}/reservations", json=_res("rv-a", 0.4))
    await client.post(f"/datasets/{ds}/reservations", json=_res("rv-b", 0.4))

    # 0.4 + 0.4 fit; raising rv-a to 0.7 would compose beyond the cap with
    # rv-b still locked, so it must be rejected.
    r = await client.post(
        f"/datasets/{ds}/reservations/rv-a/adjustments",
        json=_adj("adj-1", epsilon=0.7),
    )
    assert r.status_code == 201
    assert r.json()["allowed"] is False

    got = (await client.get(f"/datasets/{ds}/reservations/rv-a")).json()
    assert got["epsilon"] == pytest.approx(0.4)

    # Once rv-b is released the same adjustment fits.
    await client.post(f"/datasets/{ds}/reservations/rv-b/release")
    r2 = await client.post(
        f"/datasets/{ds}/reservations/rv-a/adjustments",
        json=_adj("adj-2", epsilon=0.7),
    )
    assert r2.status_code == 201
    assert r2.json()["allowed"] is True


@pytest.mark.asyncio
async def test_rejected_adjustment_does_not_block_later_fit(client):
    ds = _new_dataset_id("rejthenfit")
    await _register(client, ds, eps=1.0, delta=1e-6)
    await client.post(f"/datasets/{ds}/reservations", json=_res("rv", 0.6))

    # A single 0.6 reservation occupies 0.603... under RDP; raising it all the
    # way to the nominal cap is over cap once the conversion overhead counts.
    over = await client.post(
        f"/datasets/{ds}/reservations/rv/adjustments",
        json=_adj("adj-big", epsilon=1.0),
    )
    assert over.json()["allowed"] is False

    ok = await client.post(
        f"/datasets/{ds}/reservations/rv/adjustments",
        json=_adj("adj-small", epsilon=0.2),
    )
    assert ok.status_code == 201
    assert ok.json()["allowed"] is True
    assert ok.json()["epsilon_after"] == pytest.approx(0.2)


# ------------------------------------------------------------ idempotency --


@pytest.mark.asyncio
async def test_adjustment_replay_returns_first_verdict(client):
    ds = _new_dataset_id("idem")
    await _register(client, ds, eps=1.0, delta=1e-6)
    await client.post(f"/datasets/{ds}/reservations", json=_res("rv", 0.2))

    first = await client.post(
        f"/datasets/{ds}/reservations/rv/adjustments",
        json=_adj("adj-1", epsilon=0.3),
    )
    assert first.status_code == 201

    for _ in range(3):
        again = await client.post(
            f"/datasets/{ds}/reservations/rv/adjustments",
            json=_adj("adj-1", epsilon=0.3),
        )
        assert again.status_code == 200
        body = again.json()
        assert body["idempotent_replay"] is True
        for key in (
            "epsilon_before", "delta_before", "requested_epsilon",
            "requested_delta", "epsilon_after", "delta_after", "status",
            "reason", "epsilon_committed_before", "epsilon_committed_after",
            "rdp_best_order", "created_at",
        ):
            assert body[key] == first.json()[key]

    got = (await client.get(f"/datasets/{ds}/reservations/rv")).json()
    assert got["epsilon"] == pytest.approx(0.3)


@pytest.mark.asyncio
async def test_rejected_adjustment_replay_returns_rejection(client):
    ds = _new_dataset_id("idemrej")
    await _register(client, ds, eps=1.0, delta=1e-6)
    await client.post(f"/datasets/{ds}/reservations", json=_res("rv", 0.4))
    await client.post(f"/datasets/{ds}/reservations", json=_res("rv2", 0.4))

    # 0.4 + 0.7 composes beyond the 1.0 cap while rv2 stays locked.
    first = await client.post(
        f"/datasets/{ds}/reservations/rv/adjustments",
        json=_adj("adj-1", epsilon=0.7),
    )
    assert first.json()["allowed"] is False

    # Even after room frees up, the same key keeps replaying the first
    # (rejected) verdict and moves no quota.
    await client.post(f"/datasets/{ds}/reservations/rv2/release")
    again = await client.post(
        f"/datasets/{ds}/reservations/rv/adjustments",
        json=_adj("adj-1", epsilon=0.7),
    )
    assert again.status_code == 200
    assert again.json()["idempotent_replay"] is True
    assert again.json()["status"] == "rejected"
    got = (await client.get(f"/datasets/{ds}/reservations/rv")).json()
    assert got["epsilon"] == pytest.approx(0.4)


@pytest.mark.asyncio
async def test_adjustment_replay_after_expiry_still_returns_record(client):
    ds = _new_dataset_id("idemexp")
    await _register(client, ds)
    await client.post(f"/datasets/{ds}/reservations", json=_res("rv", ttl=10))
    first = await client.post(
        f"/datasets/{ds}/reservations/rv/adjustments",
        json=_adj("adj-1", epsilon=0.05),
    )
    assert first.status_code == 201

    past = datetime.now(timezone.utc) - timedelta(seconds=1)
    await _backdate(ds, "rv", past)
    replay = await client.post(
        f"/datasets/{ds}/reservations/rv/adjustments",
        json=_adj("adj-1", epsilon=0.05),
    )
    assert replay.status_code == 200
    assert replay.json()["idempotent_replay"] is True
    assert replay.json()["created_at"] == first.json()["created_at"]


@pytest.mark.asyncio
async def test_same_adjustment_id_different_payload_conflicts(client):
    ds = _new_dataset_id("idemconf")
    await _register(client, ds)
    await client.post(f"/datasets/{ds}/reservations", json=_res("rv"))
    await client.post(f"/datasets/{ds}/reservations", json=_res("rv2"))
    first = await client.post(
        f"/datasets/{ds}/reservations/rv/adjustments",
        json=_adj("adj-1", epsilon=0.2),
    )
    assert first.status_code == 201

    # Different epsilon on the same adjustment_id.
    r = await client.post(
        f"/datasets/{ds}/reservations/rv/adjustments",
        json=_adj("adj-1", epsilon=0.21),
    )
    assert r.status_code == 409
    assert r.json()["error"] == "adjustment_id_conflict"

    # Different delta.
    r = await client.post(
        f"/datasets/{ds}/reservations/rv/adjustments",
        json=_adj("adj-1", epsilon=0.2, delta=1e-8),
    )
    assert r.status_code == 409

    # Same adjustment_id aimed at another reservation.
    r2 = await client.post(
        f"/datasets/{ds}/reservations/rv2/adjustments",
        json=_adj("adj-1", epsilon=0.2),
    )
    assert r2.status_code == 409

    # Neither conflict moved any quota.
    got = (await client.get(f"/datasets/{ds}/reservations/rv")).json()
    assert got["epsilon"] == pytest.approx(0.2)
    got2 = (await client.get(f"/datasets/{ds}/reservations/rv2")).json()
    assert got2["epsilon"] == pytest.approx(0.1)


@pytest.mark.asyncio
async def test_adjustment_id_is_scoped_per_dataset(client):
    ds_a = _new_dataset_id("scope-a")
    ds_b = _new_dataset_id("scope-b")
    await _register(client, ds_a)
    await _register(client, ds_b)
    await client.post(f"/datasets/{ds_a}/reservations", json=_res("rv"))
    await client.post(f"/datasets/{ds_b}/reservations", json=_res("rv"))

    ra = await client.post(
        f"/datasets/{ds_a}/reservations/rv/adjustments",
        json=_adj("adj-1", epsilon=0.2),
    )
    assert ra.status_code == 201
    rb = await client.post(
        f"/datasets/{ds_b}/reservations/rv/adjustments",
        json=_adj("adj-1", epsilon=0.3),
    )
    assert rb.status_code == 201
    assert rb.json()["epsilon_after"] == pytest.approx(0.3)


# --------------------------------------------------------- state conflicts --


@pytest.mark.asyncio
async def test_adjustment_missing_dataset_or_reservation_404(client):
    ds = _new_dataset_id("notfound")
    r = await client.post(
        "/datasets/no-such-ds/reservations/rv/adjustments",
        json=_adj("adj-1"),
    )
    assert r.status_code == 404
    assert r.json()["error"] == "dataset_not_found"

    await _register(client, ds)
    r2 = await client.post(
        f"/datasets/{ds}/reservations/nope/adjustments",
        json=_adj("adj-1"),
    )
    assert r2.status_code == 404
    assert r2.json()["error"] == "reservation_not_found"


@pytest.mark.asyncio
async def test_adjustment_terminal_states_conflict(client):
    ds = _new_dataset_id("states")
    await _register(client, ds, eps=1.0, delta=1e-6)

    # confirmed
    await client.post(f"/datasets/{ds}/reservations", json=_res("rv-conf"))
    await client.post(
        f"/datasets/{ds}/reservations/rv-conf/confirm",
        json={"request_id": "req-1", "query_type": "q"},
    )
    r = await client.post(
        f"/datasets/{ds}/reservations/rv-conf/adjustments",
        json=_adj("adj-c", epsilon=0.05),
    )
    assert r.status_code == 409
    assert r.json()["error"] == "reservation_confirmed"

    # released
    await client.post(f"/datasets/{ds}/reservations", json=_res("rv-rel"))
    await client.post(f"/datasets/{ds}/reservations/rv-rel/release")
    r = await client.post(
        f"/datasets/{ds}/reservations/rv-rel/adjustments",
        json=_adj("adj-r", epsilon=0.05),
    )
    assert r.status_code == 409
    assert r.json()["error"] == "reservation_released"

    # rejected reservation never held budget and cannot be adjusted
    await client.post(
        f"/datasets/{ds}/reservations", json=_res("rv-rej", epsilon=5.0)
    )
    r = await client.post(
        f"/datasets/{ds}/reservations/rv-rej/adjustments",
        json=_adj("adj-j", epsilon=0.05),
    )
    assert r.status_code == 409
    assert r.json()["error"] == "reservation_rejected"

    # expired (lazily swept inside the adjustment transaction)
    await client.post(
        f"/datasets/{ds}/reservations", json=_res("rv-exp", ttl=10)
    )
    past = datetime.now(timezone.utc) - timedelta(seconds=1)
    await _backdate(ds, "rv-exp", past)
    r = await client.post(
        f"/datasets/{ds}/reservations/rv-exp/adjustments",
        json=_adj("adj-e", epsilon=0.05),
    )
    assert r.status_code == 409
    assert r.json()["error"] == "reservation_expired"
    got = (await client.get(f"/datasets/{ds}/reservations/rv-exp")).json()
    assert got["status"] == "expired"


# -------------------------------------------------------------- validation --


@pytest.mark.asyncio
async def test_adjustment_validation_errors(client):
    ds = _new_dataset_id("validation")
    await _register(client, ds)
    await client.post(f"/datasets/{ds}/reservations", json=_res("rv"))

    for bad_epsilon in (0.0, -0.1, 1001.0):
        r = await client.post(
            f"/datasets/{ds}/reservations/rv/adjustments",
            json={"adjustment_id": "adj-x", "epsilon": bad_epsilon,
                  "delta": 1e-7},
        )
        assert r.status_code == 422, bad_epsilon

    for bad_delta in (-1e-9, 1.0):
        r = await client.post(
            f"/datasets/{ds}/reservations/rv/adjustments",
            json={"adjustment_id": "adj-x", "epsilon": 0.1,
                  "delta": bad_delta},
        )
        assert r.status_code == 422, bad_delta

    bad_id = await client.post(
        f"/datasets/{ds}/reservations/rv/adjustments",
        json={"adjustment_id": "bad id!", "epsilon": 0.1, "delta": 1e-7},
    )
    assert bad_id.status_code == 422

    missing = await client.post(
        f"/datasets/{ds}/reservations/rv/adjustments",
        json={"epsilon": 0.1, "delta": 1e-7},
    )
    assert missing.status_code == 422

    # Nothing was changed by the rejected payloads.
    got = (await client.get(f"/datasets/{ds}/reservations/rv")).json()
    assert got["epsilon"] == pytest.approx(0.1)
    assert got["delta"] == pytest.approx(1e-7)


# ------------------------------------------- interplay with existing flows --


@pytest.mark.asyncio
async def test_creation_replay_after_adjustment_uses_creation_payload(client):
    ds = _new_dataset_id("createplay")
    await _register(client, ds, eps=1.0, delta=1e-6)
    await client.post(
        f"/datasets/{ds}/reservations", json=_res("rv", epsilon=0.1, ttl=600)
    )
    await client.post(
        f"/datasets/{ds}/reservations/rv/adjustments",
        json=_adj("adj-1", epsilon=0.3),
    )

    # Replaying the *original* creation payload is still the same reservation.
    replay = await client.post(
        f"/datasets/{ds}/reservations", json=_res("rv", epsilon=0.1, ttl=600)
    )
    assert replay.status_code == 200
    assert replay.json()["idempotent_replay"] is True
    # The live view keeps the adjusted quota.
    assert replay.json()["epsilon"] == pytest.approx(0.3)

    # A creation call carrying the adjusted quota is a payload conflict.
    conflict = await client.post(
        f"/datasets/{ds}/reservations", json=_res("rv", epsilon=0.3, ttl=600)
    )
    assert conflict.status_code == 409
    assert conflict.json()["error"] == "reservation_id_conflict"


@pytest.mark.asyncio
async def test_confirm_after_adjustment_uses_new_quota(client):
    ds = _new_dataset_id("confirm")
    await _register(client, ds, eps=1.0, delta=1e-6)
    rv = await client.post(
        f"/datasets/{ds}/reservations", json=_res("rv", epsilon=0.2)
    )
    assert rv.status_code == 201
    adj = await client.post(
        f"/datasets/{ds}/reservations/rv/adjustments",
        json=_adj("adj-1", epsilon=0.4),
    )
    assert adj.status_code == 201
    committed = adj.json()["epsilon_committed_after"]

    verdict = await client.post(
        f"/datasets/{ds}/reservations/rv/confirm",
        json={"request_id": "req-1", "query_type": "q"},
    )
    assert verdict.status_code == 201
    assert verdict.json()["allowed"] is True
    assert verdict.json()["epsilon"] == pytest.approx(0.4)
    assert verdict.json()["epsilon_after"] == pytest.approx(committed, abs=1e-9)

    budget = await _budget(client, ds)
    assert budget["active_reservations"] == 0
    assert budget["granted_requests"] == 1

    # The confirmed request itself keeps the adjusted quota.
    got = (await client.get(f"/datasets/{ds}/reservations/rv")).json()
    assert got["status"] == "confirmed"
    assert got["epsilon"] == pytest.approx(0.4)


@pytest.mark.asyncio
async def test_release_after_adjustment_frees_new_quota(client):
    ds = _new_dataset_id("release")
    await _register(client, ds, eps=1.0, delta=1e-6)
    await client.post(
        f"/datasets/{ds}/reservations", json=_res("rv", epsilon=0.2)
    )
    adj = await client.post(
        f"/datasets/{ds}/reservations/rv/adjustments",
        json=_adj("adj-1", epsilon=0.5),
    )
    assert adj.json()["allowed"] is True

    await client.post(f"/datasets/{ds}/reservations/rv/release")
    budget = await _budget(client, ds)
    assert budget["active_reservations"] == 0
    assert budget["epsilon_remaining"] == pytest.approx(1.0)

    req = await client.post(
        f"/datasets/{ds}/requests",
        json={"request_id": "req-big", "query_type": "q",
              "epsilon": 0.8, "delta": 1e-7},
    )
    assert req.json()["allowed"] is True


@pytest.mark.asyncio
async def test_release_forecast_uses_adjusted_quota(client):
    ds = _new_dataset_id("forecast")
    await _register(client, ds, eps=1.0, delta=1e-6)
    rv = await client.post(
        f"/datasets/{ds}/reservations", json=_res("rv", epsilon=0.2, ttl=600)
    )
    expiry = datetime.fromisoformat(rv.json()["expires_at"])
    await client.post(
        f"/datasets/{ds}/reservations/rv/adjustments",
        json=_adj("adj-1", epsilon=0.5),
    )

    mid = (expiry - timedelta(seconds=60)).isoformat()
    fc = await client.get(
        f"/datasets/{ds}/budget/release-forecast", params={"deadline": mid}
    )
    assert fc.status_code == 200
    body = fc.json()
    assert body["at_deadline"]["active_reservations"] == 1
    # The simulated occupancy is the new (larger) quota.
    expected = await _budget(client, ds)
    assert body["initial"]["epsilon_reserved"] == pytest.approx(
        expected["epsilon_reserved"], abs=1e-12
    )

    far = (expiry + timedelta(seconds=60)).isoformat()
    fc2 = await client.get(
        f"/datasets/{ds}/budget/release-forecast", params={"deadline": far}
    )
    assert len(fc2.json()["events"]) == 1
    assert fc2.json()["events"][0]["released_reservation_ids"] == ["rv"]
    assert fc2.json()["at_deadline"]["active_reservations"] == 0


@pytest.mark.asyncio
async def test_adjustment_and_renewal_coexist(client):
    ds = _new_dataset_id("renewal")
    await _register(client, ds, eps=1.0, delta=1e-6)
    rv = await client.post(
        f"/datasets/{ds}/reservations", json=_res("rv", epsilon=0.1, ttl=600)
    )
    original_expiry = rv.json()["expires_at"]
    ren = await client.post(
        f"/datasets/{ds}/reservations/rv/renewals",
        json={"renewal_id": "ren-1", "extend_seconds": 300},
    )
    assert ren.status_code == 201
    adj = await client.post(
        f"/datasets/{ds}/reservations/rv/adjustments",
        json=_adj("adj-1", epsilon=0.2),
    )
    assert adj.status_code == 201

    got = (await client.get(f"/datasets/{ds}/reservations/rv")).json()
    assert got["epsilon"] == pytest.approx(0.2)
    assert got["ttl_seconds"] == 600
    assert datetime.fromisoformat(got["expires_at"]) == datetime.fromisoformat(
        ren.json()["expires_at_after"]
    )
    assert datetime.fromisoformat(got["expires_at"]) != datetime.fromisoformat(
        original_expiry
    )


@pytest.mark.asyncio
async def test_adjusted_reservation_still_expires_on_schedule(client):
    ds = _new_dataset_id("lazyexp")
    await _register(client, ds, eps=1.0, delta=1e-6)
    await client.post(
        f"/datasets/{ds}/reservations", json=_res("rv", epsilon=0.1, ttl=600)
    )
    await client.post(
        f"/datasets/{ds}/reservations/rv/adjustments",
        json=_adj("adj-1", epsilon=0.2),
    )

    # Expiry is TTL based in production; rewriting the instant is the
    # test/admin hook.  An instant one second in the past sweeps it once,
    # regardless of the (untouched) ttl_seconds.
    past = datetime.now(timezone.utc) - timedelta(seconds=1)
    await _backdate(ds, "rv", past)
    got = (await client.get(f"/datasets/{ds}/reservations/rv")).json()
    assert got["status"] == "expired"
    assert got["epsilon"] == pytest.approx(0.2)
    budget = await _budget(client, ds)
    assert budget["active_reservations"] == 0
    assert budget["epsilon_remaining"] == pytest.approx(1.0)


@pytest.mark.asyncio
async def test_adjustment_keeps_other_reservation_snapshots_untouched(client):
    ds = _new_dataset_id("snapshots")
    await _register(client, ds, eps=2.0, delta=1e-6)
    a = await client.post(
        f"/datasets/{ds}/reservations", json=_res("rv-a", epsilon=0.2)
    )
    b = await client.post(
        f"/datasets/{ds}/reservations", json=_res("rv-b", epsilon=0.1)
    )
    await client.post(
        f"/datasets/{ds}/reservations/rv-a/adjustments",
        json=_adj("adj-1", epsilon=0.5),
    )
    # rv-b keeps its own quota and stored admission verdict; only rv-a moves.
    got_b = (await client.get(f"/datasets/{ds}/reservations/rv-b")).json()
    assert got_b["epsilon"] == pytest.approx(0.1)
    assert got_b["delta"] == pytest.approx(1e-7)
    assert got_b["epsilon_before"] == pytest.approx(a.json()["epsilon_after"], abs=1e-12)
    assert got_b["epsilon_after"] == pytest.approx(b.json()["epsilon_after"], abs=1e-12)
    assert datetime.fromisoformat(got_b["created_at"]) == datetime.fromisoformat(
        b.json()["created_at"]
    )


# ------------------------------------------------------------- concurrency --


@pytest.mark.asyncio
async def test_concurrent_duplicate_adjustments_apply_once(client):
    ds = _new_dataset_id("concdup")
    await _register(client, ds, eps=1.0, delta=1e-6)
    await client.post(f"/datasets/{ds}/reservations", json=_res("rv", 0.1))

    results = await asyncio.gather(*[
        client.post(
            f"/datasets/{ds}/reservations/rv/adjustments",
            json=_adj("adj-1", epsilon=0.2),
        )
        for _ in range(20)
    ])
    created = [r for r in results if r.status_code == 201]
    replays = [r for r in results if r.status_code == 200]
    assert len(created) == 1
    assert len(replays) == 19
    assert created[0].json()["status"] == "adjusted"

    got = (await client.get(f"/datasets/{ds}/reservations/rv")).json()
    assert got["epsilon"] == pytest.approx(0.2)
    async with db.connect() as conn:
        row = await db.get_reservation_adjustment(conn, ds, "adj-1")
        assert row is not None


@pytest.mark.asyncio
async def test_concurrent_distinct_adjustments_serialise(client):
    ds = _new_dataset_id("concdist")
    await _register(client, ds, eps=10.0, delta=1e-6)
    await client.post(f"/datasets/{ds}/reservations", json=_res("rv", 0.1))

    n = 10
    results = await asyncio.gather(*[
        client.post(
            f"/datasets/{ds}/reservations/rv/adjustments",
            json=_adj(f"adj-{i:02d}", epsilon=0.2 + 0.1 * i),
        )
        for i in range(n)
    ])
    assert all(r.status_code == 201 for r in results)
    assert all(r.json()["allowed"] is True for r in results)

    got = (await client.get(f"/datasets/{ds}/reservations/rv")).json()
    assert got["epsilon"] == pytest.approx(0.2 + 0.1 * (n - 1))
    async with db.connect() as conn:
        for i in range(n):
            assert await db.get_reservation_adjustment(
                conn, ds, f"adj-{i:02d}"
            ) is not None


# ------------------------------------------------------------- migration --


@pytest.mark.asyncio
async def test_migration_from_v6_schema(tmp_path, monkeypatch):
    old_db_path = str(tmp_path / "old.db")

    # Build a database at the pre-adjustment schema with some data.
    conn = await aiosqlite.connect(old_db_path)
    try:
        for schema in (
            db.SCHEMA_V1, db.SCHEMA_V2, db.SCHEMA_V3, db.SCHEMA_V4,
            db.SCHEMA_V5, db.SCHEMA_V6,
        ):
            await conn.executescript(schema)
        await conn.execute(
            "INSERT INTO datasets "
            "(dataset_id, epsilon_cap, delta_cap, description, created_at) "
            "VALUES ('legacy', 1.0, 1e-6, 'old', '2026-01-01T00:00:00+00:00')"
        )
        await conn.execute(
            "INSERT INTO reservations "
            "(dataset_id, reservation_id, epsilon, delta, ttl_seconds, status, "
            "reason, epsilon_before, epsilon_after, rdp_best_order, created_at, "
            "expires_at) VALUES ('legacy', 'rv1', 0.1, 1e-7, 600, 'granted', "
            "NULL, 0.0, 0.1, 1.1, '2026-01-01T00:00:01+00:00', "
            "'2026-09-01T00:10:01+00:00')"
        )
        await conn.execute("PRAGMA user_version = 6")
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
            "SELECT name FROM sqlite_master WHERE type='table' "
            "AND name='reservation_adjustments'"
        ) as cur:
            assert await cur.fetchone() is not None
        # Legacy rows are backfilled: original quota equals the live quota.
        row = await db.get_reservation(c, "legacy", "rv1")
        assert row["epsilon"] == pytest.approx(0.1)
        assert row["original_epsilon"] == pytest.approx(0.1)
        assert row["original_delta"] == pytest.approx(1e-7)
