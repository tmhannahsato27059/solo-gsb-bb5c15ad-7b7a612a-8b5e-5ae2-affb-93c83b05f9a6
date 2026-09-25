"""End-to-end tests for reservation renewals (TTL extension)."""
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
_tmp = tempfile.mkdtemp(prefix="dpledger-test-renewal-")
os.environ["DATABASE_PATH"] = os.path.join(_tmp, "renewals.db")

from httpx import ASGITransport, AsyncClient  # noqa: E402

from app import db  # noqa: E402
from app.config import settings  # noqa: E402
from app.main import app  # noqa: E402
from app.schemas import MAX_TTL_SECONDS  # noqa: E402

import itertools

_counter = itertools.count()


def _new_dataset_id(name: str) -> str:
    return f"dsren-{name}-{next(_counter)}"


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


def _ren(renewal_id: str, extend_seconds: int = 300) -> dict:
    return {"renewal_id": renewal_id, "extend_seconds": extend_seconds}


def _ts(value: str) -> datetime:
    """Parse an ISO-8601 instant; GET views emit ``Z``, POST views ``+00:00``."""
    return datetime.fromisoformat(value)


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


# ------------------------------------------------------------- happy path --


@pytest.mark.asyncio
async def test_renewal_extends_expiry_without_recharging_budget(client):
    ds = _new_dataset_id("extend")
    await _register(client, ds, eps=1.0, delta=1e-6)
    rv = await client.post(
        f"/datasets/{ds}/reservations", json=_res("rv", epsilon=0.2, ttl=600)
    )
    assert rv.status_code == 201
    created = rv.json()
    budget_before = (await client.get(f"/datasets/{ds}/budget")).json()

    r = await client.post(
        f"/datasets/{ds}/reservations/rv/renewals", json=_ren("ren-1", 300)
    )
    assert r.status_code == 201
    body = r.json()
    assert body["idempotent_replay"] is False
    assert body["renewal_id"] == "ren-1"
    assert body["dataset_id"] == ds
    assert body["reservation_id"] == "rv"
    assert body["extend_seconds"] == 300
    before = datetime.fromisoformat(body["expires_at_before"])
    after = datetime.fromisoformat(body["expires_at_after"])
    assert before == datetime.fromisoformat(created["expires_at"])
    assert after - before == timedelta(seconds=300)

    # The reservation itself reflects the new expiry; amount/TTL unchanged.
    got = (await client.get(f"/datasets/{ds}/reservations/rv")).json()
    assert _ts(got["expires_at"]) == after
    assert got["ttl_seconds"] == created["ttl_seconds"]
    assert got["epsilon"] == pytest.approx(created["epsilon"])
    assert got["delta"] == pytest.approx(created["delta"])
    assert got["status"] == "granted"

    # Budget is not occupied twice: identical reservation accounting.
    budget_after = (await client.get(f"/datasets/{ds}/budget")).json()
    assert budget_after["active_reservations"] == 1
    assert budget_after["epsilon_reserved"] == pytest.approx(
        budget_before["epsilon_reserved"], abs=1e-12
    )
    assert budget_after["epsilon_remaining"] == pytest.approx(
        budget_before["epsilon_remaining"], abs=1e-12
    )


@pytest.mark.asyncio
async def test_chained_renewals_move_expiry_step_by_step(client):
    ds = _new_dataset_id("chain")
    await _register(client, ds)
    rv = await client.post(
        f"/datasets/{ds}/reservations", json=_res("rv", ttl=100)
    )
    original_expiry = rv.json()["expires_at"]

    first = await client.post(
        f"/datasets/{ds}/reservations/rv/renewals", json=_ren("ren-1", 50)
    )
    assert first.status_code == 201
    assert first.json()["expires_at_before"] == original_expiry

    second = await client.post(
        f"/datasets/{ds}/reservations/rv/renewals", json=_ren("ren-2", 70)
    )
    assert second.status_code == 201
    assert (
        second.json()["expires_at_before"] == first.json()["expires_at_after"]
    )
    after = datetime.fromisoformat(second.json()["expires_at_after"])
    created = datetime.fromisoformat(rv.json()["created_at"])
    assert after - created == timedelta(seconds=100 + 50 + 70)

    got = (await client.get(f"/datasets/{ds}/reservations/rv")).json()
    assert _ts(got["expires_at"]) == _ts(second.json()["expires_at_after"])


# ------------------------------------------------------------ idempotency --


@pytest.mark.asyncio
async def test_renewal_replay_returns_first_extension(client):
    ds = _new_dataset_id("idem")
    await _register(client, ds)
    await client.post(f"/datasets/{ds}/reservations", json=_res("rv", ttl=600))

    first = await client.post(
        f"/datasets/{ds}/reservations/rv/renewals", json=_ren("ren-1", 300)
    )
    assert first.status_code == 201

    for _ in range(3):
        again = await client.post(
            f"/datasets/{ds}/reservations/rv/renewals", json=_ren("ren-1", 300)
        )
        assert again.status_code == 200
        body = again.json()
        assert body["idempotent_replay"] is True
        assert body["expires_at_before"] == first.json()["expires_at_before"]
        assert body["expires_at_after"] == first.json()["expires_at_after"]
        assert body["created_at"] == first.json()["created_at"]

    # The expiry moved exactly once.
    got = (await client.get(f"/datasets/{ds}/reservations/rv")).json()
    assert _ts(got["expires_at"]) == _ts(first.json()["expires_at_after"])


@pytest.mark.asyncio
async def test_renewal_replay_after_expiry_still_returns_record(client):
    ds = _new_dataset_id("idemexp")
    await _register(client, ds)
    await client.post(f"/datasets/{ds}/reservations", json=_res("rv", ttl=10))
    first = await client.post(
        f"/datasets/{ds}/reservations/rv/renewals", json=_ren("ren-1", 5)
    )
    assert first.status_code == 201

    # Force the (extended) expiry into the past, then retry the same renewal:
    # the stored record is replayed instead of a 409 state conflict.
    past = datetime.now(timezone.utc) - timedelta(seconds=1)
    await _backdate(ds, "rv", past)
    replay = await client.post(
        f"/datasets/{ds}/reservations/rv/renewals", json=_ren("ren-1", 5)
    )
    assert replay.status_code == 200
    assert replay.json()["idempotent_replay"] is True
    assert replay.json()["expires_at_after"] == first.json()["expires_at_after"]


@pytest.mark.asyncio
async def test_same_renewal_id_different_payload_conflicts(client):
    ds = _new_dataset_id("idemconf")
    await _register(client, ds)
    await client.post(f"/datasets/{ds}/reservations", json=_res("rv", ttl=600))
    await client.post(f"/datasets/{ds}/reservations", json=_res("rv2", ttl=600))
    first = await client.post(
        f"/datasets/{ds}/reservations/rv/renewals", json=_ren("ren-1", 300)
    )
    assert first.status_code == 201

    # Different extend_seconds on the same renewal_id.
    r = await client.post(
        f"/datasets/{ds}/reservations/rv/renewals", json=_ren("ren-1", 301)
    )
    assert r.status_code == 409
    assert r.json()["error"] == "renewal_id_conflict"

    # Same renewal_id aimed at another reservation of this dataset.
    r2 = await client.post(
        f"/datasets/{ds}/reservations/rv2/renewals", json=_ren("ren-1", 300)
    )
    assert r2.status_code == 409
    assert r2.json()["error"] == "renewal_id_conflict"

    # Neither conflict moved any expiry.
    got = (await client.get(f"/datasets/{ds}/reservations/rv")).json()
    assert _ts(got["expires_at"]) == _ts(first.json()["expires_at_after"])
    got2 = (await client.get(f"/datasets/{ds}/reservations/rv2")).json()
    assert _ts(got2["expires_at"]) != _ts(first.json()["expires_at_after"])


@pytest.mark.asyncio
async def test_renewal_id_is_scoped_per_dataset(client):
    ds_a = _new_dataset_id("scope-a")
    ds_b = _new_dataset_id("scope-b")
    await _register(client, ds_a)
    await _register(client, ds_b)
    await client.post(f"/datasets/{ds_a}/reservations", json=_res("rv"))
    await client.post(f"/datasets/{ds_b}/reservations", json=_res("rv"))

    ra = await client.post(
        f"/datasets/{ds_a}/reservations/rv/renewals", json=_ren("ren-1", 100)
    )
    assert ra.status_code == 201
    # The same renewal_id is an independent key in another dataset.
    rb = await client.post(
        f"/datasets/{ds_b}/reservations/rv/renewals", json=_ren("ren-1", 200)
    )
    assert rb.status_code == 201
    assert rb.json()["extend_seconds"] == 200


# --------------------------------------------------------- state conflicts --


@pytest.mark.asyncio
async def test_renewal_missing_dataset_or_reservation_404(client):
    ds = _new_dataset_id("notfound")
    r = await client.post(
        "/datasets/no-such-ds/reservations/rv/renewals", json=_ren("ren-1")
    )
    assert r.status_code == 404
    assert r.json()["error"] == "dataset_not_found"

    await _register(client, ds)
    r2 = await client.post(
        f"/datasets/{ds}/reservations/nope/renewals", json=_ren("ren-1")
    )
    assert r2.status_code == 404
    assert r2.json()["error"] == "reservation_not_found"


@pytest.mark.asyncio
async def test_renewal_terminal_states_conflict(client):
    ds = _new_dataset_id("states")
    await _register(client, ds, eps=1.0, delta=1e-6)

    # confirmed
    await client.post(f"/datasets/{ds}/reservations", json=_res("rv-conf"))
    await client.post(
        f"/datasets/{ds}/reservations/rv-conf/confirm",
        json={"request_id": "req-1", "query_type": "q"},
    )
    r = await client.post(
        f"/datasets/{ds}/reservations/rv-conf/renewals", json=_ren("ren-c")
    )
    assert r.status_code == 409
    assert r.json()["error"] == "reservation_confirmed"

    # released
    await client.post(f"/datasets/{ds}/reservations", json=_res("rv-rel"))
    await client.post(f"/datasets/{ds}/reservations/rv-rel/release")
    r = await client.post(
        f"/datasets/{ds}/reservations/rv-rel/renewals", json=_ren("ren-r")
    )
    assert r.status_code == 409
    assert r.json()["error"] == "reservation_released"

    # rejected
    await client.post(
        f"/datasets/{ds}/reservations", json=_res("rv-rej", epsilon=5.0)
    )
    r = await client.post(
        f"/datasets/{ds}/reservations/rv-rej/renewals", json=_ren("ren-j")
    )
    assert r.status_code == 409
    assert r.json()["error"] == "reservation_rejected"

    # expired (lazily swept inside the renewal transaction)
    await client.post(
        f"/datasets/{ds}/reservations", json=_res("rv-exp", ttl=10)
    )
    past = datetime.now(timezone.utc) - timedelta(seconds=1)
    await _backdate(ds, "rv-exp", past)
    r = await client.post(
        f"/datasets/{ds}/reservations/rv-exp/renewals", json=_ren("ren-e")
    )
    assert r.status_code == 409
    assert r.json()["error"] == "reservation_expired"
    got = (await client.get(f"/datasets/{ds}/reservations/rv-exp")).json()
    assert got["status"] == "expired"


@pytest.mark.asyncio
async def test_renewal_horizon_capped_at_90_days_from_creation(client):
    ds = _new_dataset_id("horizon")
    await _register(client, ds)

    # A reservation already at the 90-day ceiling cannot be extended at all.
    await client.post(
        f"/datasets/{ds}/reservations",
        json=_res("rv-max", ttl=MAX_TTL_SECONDS),
    )
    r = await client.post(
        f"/datasets/{ds}/reservations/rv-max/renewals", json=_ren("ren-1", 1)
    )
    assert r.status_code == 409
    assert r.json()["error"] == "renewal_horizon_exceeded"
    got = (await client.get(f"/datasets/{ds}/reservations/rv-max")).json()
    created = datetime.fromisoformat(got["created_at"])
    expires = datetime.fromisoformat(got["expires_at"])
    assert expires - created == timedelta(seconds=MAX_TTL_SECONDS)

    # Extending exactly to the ceiling is allowed; one second beyond is not.
    await client.post(
        f"/datasets/{ds}/reservations",
        json=_res("rv-edge", ttl=MAX_TTL_SECONDS - 100),
    )
    ok = await client.post(
        f"/datasets/{ds}/reservations/rv-edge/renewals", json=_ren("ren-2", 100)
    )
    assert ok.status_code == 201
    got = (await client.get(f"/datasets/{ds}/reservations/rv-edge")).json()
    created = datetime.fromisoformat(got["created_at"])
    expires = datetime.fromisoformat(got["expires_at"])
    assert expires - created == timedelta(seconds=MAX_TTL_SECONDS)

    over = await client.post(
        f"/datasets/{ds}/reservations/rv-edge/renewals", json=_ren("ren-3", 1)
    )
    assert over.status_code == 409
    assert over.json()["error"] == "renewal_horizon_exceeded"
    # The failed renewal left the expiry untouched.
    got = (await client.get(f"/datasets/{ds}/reservations/rv-edge")).json()
    assert _ts(got["expires_at"]) == _ts(ok.json()["expires_at_after"])


# -------------------------------------------------------------- validation --


@pytest.mark.asyncio
async def test_renewal_validation_errors(client):
    ds = _new_dataset_id("validation")
    await _register(client, ds)
    await client.post(f"/datasets/{ds}/reservations", json=_res("rv"))

    for bad in (0, -5, 1.5, None):
        r = await client.post(
            f"/datasets/{ds}/reservations/rv/renewals",
            json={"renewal_id": "ren-x", "extend_seconds": bad},
        )
        assert r.status_code == 422, (bad, r.status_code, r.text)

    too_long = await client.post(
        f"/datasets/{ds}/reservations/rv/renewals",
        json={"renewal_id": "ren-x", "extend_seconds": MAX_TTL_SECONDS + 1},
    )
    assert too_long.status_code == 422

    bad_id = await client.post(
        f"/datasets/{ds}/reservations/rv/renewals",
        json={"renewal_id": "bad id!", "extend_seconds": 10},
    )
    assert bad_id.status_code == 422

    missing_id = await client.post(
        f"/datasets/{ds}/reservations/rv/renewals",
        json={"extend_seconds": 10},
    )
    assert missing_id.status_code == 422

    missing_seconds = await client.post(
        f"/datasets/{ds}/reservations/rv/renewals",
        json={"renewal_id": "ren-x"},
    )
    assert missing_seconds.status_code == 422

    # Nothing was extended by any of the rejected payloads.
    got = (await client.get(f"/datasets/{ds}/reservations/rv")).json()
    created = datetime.fromisoformat(got["created_at"])
    expires = datetime.fromisoformat(got["expires_at"])
    assert expires - created == timedelta(seconds=got["ttl_seconds"])


# ------------------------------------------- interplay with existing flows --


@pytest.mark.asyncio
async def test_confirm_after_renewal_still_not_double_charged(client):
    ds = _new_dataset_id("confirm")
    await _register(client, ds, eps=1.0, delta=1e-6)
    rv = await client.post(
        f"/datasets/{ds}/reservations", json=_res("rv", epsilon=0.2, ttl=600)
    )
    committed = rv.json()["epsilon_after"]
    ren = await client.post(
        f"/datasets/{ds}/reservations/rv/renewals", json=_ren("ren-1", 600)
    )
    assert ren.status_code == 201

    verdict = await client.post(
        f"/datasets/{ds}/reservations/rv/confirm",
        json={"request_id": "req-1", "query_type": "q"},
    )
    assert verdict.status_code == 201
    assert verdict.json()["allowed"] is True
    assert verdict.json()["epsilon_after"] == pytest.approx(committed, abs=1e-9)
    budget = (await client.get(f"/datasets/{ds}/budget")).json()
    assert budget["active_reservations"] == 0
    assert budget["granted_requests"] == 1


@pytest.mark.asyncio
async def test_release_forecast_uses_extended_expiry(client):
    ds = _new_dataset_id("forecast")
    await _register(client, ds, eps=1.0, delta=1e-6)
    rv = await client.post(
        f"/datasets/{ds}/reservations", json=_res("rv", epsilon=0.2, ttl=600)
    )
    old_expiry = datetime.fromisoformat(rv.json()["expires_at"])

    ren = await client.post(
        f"/datasets/{ds}/reservations/rv/renewals", json=_ren("ren-1", 3600)
    )
    assert ren.status_code == 201
    new_expiry = datetime.fromisoformat(ren.json()["expires_at_after"])

    # Deadline between the original and the renewed expiry: the reservation
    # stays active for the whole horizon and no expiry event is simulated.
    mid = (old_expiry + timedelta(seconds=60)).isoformat()
    fc = await client.get(
        f"/datasets/{ds}/budget/release-forecast", params={"deadline": mid}
    )
    assert fc.status_code == 200
    body = fc.json()
    assert body["events"] == []
    assert body["at_deadline"]["active_reservations"] == 1
    assert body["at_deadline"]["epsilon_reserved"] > 0.0

    # Deadline past the renewed expiry: the event sits at the new instant.
    far = (new_expiry + timedelta(seconds=60)).isoformat()
    fc2 = await client.get(
        f"/datasets/{ds}/budget/release-forecast", params={"deadline": far}
    )
    body2 = fc2.json()
    assert len(body2["events"]) == 1
    event = body2["events"][0]
    assert datetime.fromisoformat(event["expires_at"]) == new_expiry
    assert event["released_reservation_ids"] == ["rv"]
    assert body2["at_deadline"]["active_reservations"] == 0


@pytest.mark.asyncio
async def test_renewed_reservation_expires_only_at_new_expiry(client):
    ds = _new_dataset_id("lazyexp")
    await _register(client, ds, eps=0.3, delta=1e-6)
    rv = await client.post(
        f"/datasets/{ds}/reservations", json=_res("rv", epsilon=0.25, ttl=10)
    )
    ren = await client.post(
        f"/datasets/{ds}/reservations/rv/renewals", json=_ren("ren-1", 3600)
    )
    assert ren.status_code == 201

    # Simulate "now" landing between the original and the renewed expiry:
    # rewrite the expiry to just after the original one (still in the future).
    original = datetime.fromisoformat(rv.json()["expires_at"])
    still_valid = original + timedelta(seconds=5)
    await _backdate(ds, "rv", still_valid)
    got = (await client.get(f"/datasets/{ds}/reservations/rv")).json()
    assert got["status"] == "granted"
    budget = (await client.get(f"/datasets/{ds}/budget")).json()
    assert budget["active_reservations"] == 1

    # Once the renewed expiry itself has passed, the reservation is swept.
    past = datetime.now(timezone.utc) - timedelta(seconds=1)
    await _backdate(ds, "rv", past)
    got = (await client.get(f"/datasets/{ds}/reservations/rv")).json()
    assert got["status"] == "expired"
    budget = (await client.get(f"/datasets/{ds}/budget")).json()
    assert budget["active_reservations"] == 0
    assert budget["epsilon_remaining"] == pytest.approx(0.3)


# ------------------------------------------------------------- concurrency --


@pytest.mark.asyncio
async def test_concurrent_duplicate_renewals_extend_once(client):
    ds = _new_dataset_id("concdup")
    await _register(client, ds)
    rv = await client.post(
        f"/datasets/{ds}/reservations", json=_res("rv", ttl=600)
    )
    original = datetime.fromisoformat(rv.json()["expires_at"])

    results = await asyncio.gather(*[
        client.post(f"/datasets/{ds}/reservations/rv/renewals",
                    json=_ren("ren-1", 300))
        for _ in range(20)
    ])
    created = [r for r in results if r.status_code == 201]
    replays = [r for r in results if r.status_code == 200]
    assert len(created) == 1
    assert len(replays) == 19

    got = (await client.get(f"/datasets/{ds}/reservations/rv")).json()
    final = datetime.fromisoformat(got["expires_at"])
    assert final - original == timedelta(seconds=300)


@pytest.mark.asyncio
async def test_concurrent_distinct_renewals_all_apply(client):
    ds = _new_dataset_id("concdist")
    await _register(client, ds)
    rv = await client.post(
        f"/datasets/{ds}/reservations", json=_res("rv", ttl=600)
    )
    original = datetime.fromisoformat(rv.json()["expires_at"])

    n = 10
    results = await asyncio.gather(*[
        client.post(f"/datasets/{ds}/reservations/rv/renewals",
                    json=_ren(f"ren-{i:02d}", 60))
        for i in range(n)
    ])
    assert all(r.status_code == 201 for r in results)

    got = (await client.get(f"/datasets/{ds}/reservations/rv")).json()
    final = datetime.fromisoformat(got["expires_at"])
    assert final - original == timedelta(seconds=60 * n)

    async with db.connect() as conn:
        count = 0
        for i in range(n):
            row = await db.get_reservation_renewal(conn, ds, f"ren-{i:02d}")
            assert row is not None
            count += 1
        assert count == n


# ------------------------------------------------------------- migration --


@pytest.mark.asyncio
async def test_migration_from_v5_schema(tmp_path, monkeypatch):
    old_db_path = str(tmp_path / "old.db")

    # Build a database at the pre-renewal schema with some data.
    conn = await aiosqlite.connect(old_db_path)
    try:
        for schema in (
            db.SCHEMA_V1, db.SCHEMA_V2, db.SCHEMA_V3, db.SCHEMA_V4, db.SCHEMA_V5
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
            "'2026-01-01T00:10:01+00:00')"
        )
        await conn.execute("PRAGMA user_version = 5")
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
            "AND name='reservation_renewals'"
        ) as cur:
            assert await cur.fetchone() is not None
        row = await db.get_reservation(c, "legacy", "rv1")
        assert row["status"] == "granted"
        assert row["expires_at"] == "2026-01-01T00:10:01+00:00"
