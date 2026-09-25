"""End-to-end tests for granted-request reversals.

POST /datasets/{dataset_id}/requests/{request_id}/reversals
"""
from __future__ import annotations

import asyncio
import dataclasses
import itertools
import os
import tempfile

import aiosqlite
import pytest
import pytest_asyncio

# Point the ledger at an isolated database before importing the app.
_tmp = tempfile.mkdtemp(prefix="dpledger-test-rev-")
os.environ["DATABASE_PATH"] = os.path.join(_tmp, "reversals.db")

from httpx import ASGITransport, AsyncClient  # noqa: E402

from app import db, rdp  # noqa: E402
from app.config import settings  # noqa: E402
from app.main import app  # noqa: E402

_counter = itertools.count()


def _new_dataset_id(name: str) -> str:
    return f"dsr-{name}-{next(_counter)}"


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
                 delta: float = 1e-7, query_type: str = "laplace"):
    return await client.post(
        f"/datasets/{ds}/requests",
        json={"request_id": request_id, "query_type": query_type,
              "epsilon": epsilon, "delta": delta},
    )


async def _reverse(client, ds: str, request_id: str, reversal_id: str,
                   reason: str = "mistake"):
    return await client.post(
        f"/datasets/{ds}/requests/{request_id}/reversals",
        json={"reversal_id": reversal_id, "reason": reason},
    )


async def _budget(client, ds: str) -> dict:
    r = await client.get(f"/datasets/{ds}/budget")
    assert r.status_code == 200
    return r.json()


# ------------------------------------------------------------------ happy --


@pytest.mark.asyncio
async def test_reverse_granted_pure_dp_request_frees_budget(client):
    ds = _new_dataset_id("pure")
    await _register(client, ds, eps=1.0)
    for rid, eps in (("a", 0.1), ("b", 0.2), ("c", 0.3)):
        assert (await _grant(client, ds, rid, eps, 0.0)).json()["allowed"]

    before = await _budget(client, ds)
    assert before["epsilon_consumed"] == pytest.approx(0.6, abs=1e-12)

    r = await _reverse(client, ds, "b", "rev-1", "duplicate query")
    assert r.status_code == 201
    body = r.json()
    assert body["reversal_id"] == "rev-1"
    assert body["dataset_id"] == ds
    assert body["request_id"] == "b"
    assert body["reason"] == "duplicate query"
    assert body["epsilon"] == pytest.approx(0.2, abs=1e-12)
    assert body["delta"] == 0.0
    assert body["epsilon_cap"] == pytest.approx(1.0, abs=1e-12)
    assert body["epsilon_before"] == pytest.approx(0.6, abs=1e-12)
    assert body["epsilon_after"] == pytest.approx(0.4, abs=1e-12)
    assert body["epsilon_remaining"] == pytest.approx(0.6, abs=1e-12)
    assert body["idempotent_replay"] is False

    after = await _budget(client, ds)
    assert after["granted_requests"] == 2
    assert after["epsilon_consumed"] == pytest.approx(0.4, abs=1e-9)
    assert after["epsilon_remaining"] == pytest.approx(0.6, abs=1e-9)


@pytest.mark.asyncio
async def test_reverse_approx_dp_request_recomposes_rdp(client):
    ds = _new_dataset_id("rdp")
    await _register(client, ds, eps=1.0, delta=1e-6)
    assert (await _grant(client, ds, "a", 0.1, 1e-8)).json()["allowed"]
    assert (await _grant(client, ds, "b", 0.2, 1e-9)).json()["allowed"]

    r = await _reverse(client, ds, "a", "rev-rdp")
    assert r.status_code == 201

    # Cross-check the stored epsilon_after with independent RDP accounting of
    # the single surviving granted request.
    _, expected, expected_order = rdp.evaluate_entries([(0.2, 1e-9)], 1e-6)
    assert r.json()["epsilon_after"] == pytest.approx(expected, abs=1e-12)
    assert r.json()["rdp_best_order"] == pytest.approx(expected_order, abs=1e-9)

    budget = await _budget(client, ds)
    assert budget["epsilon_consumed"] == pytest.approx(expected, abs=1e-9)


@pytest.mark.asyncio
async def test_original_request_record_is_kept_unchanged(client):
    ds = _new_dataset_id("keep")
    await _register(client, ds, eps=1.0)
    verdict = (await _grant(client, ds, "a", 0.1, 0.0)).json()

    assert (await _reverse(client, ds, "a", "rev-keep")).status_code == 201

    listing = (await client.get(f"/datasets/{ds}/requests")).json()
    assert listing["count"] == 1
    rec = listing["requests"][0]
    # Historical admission fields are untouched.
    assert rec["status"] == "granted"
    assert rec["epsilon_before"] == verdict["epsilon_before"]
    assert rec["epsilon_after"] == verdict["epsilon_after"]
    # ... but the listing marks it as reversed.
    assert rec["reversed"] is True
    assert rec["reversal_id"] == "rev-keep"
    assert rec["reversal_reason"] == "mistake"
    assert rec["reversed_at"] is not None

    # Non-reversed requests carry null reversal markers.
    assert (await _grant(client, ds, "b", 0.1, 0.0)).status_code == 201
    recs = (await client.get(f"/datasets/{ds}/requests")).json()["requests"]
    b = next(r for r in recs if r["request_id"] == "b")
    assert b["reversed"] is False
    assert b["reversal_id"] is None
    assert b["reversal_reason"] is None
    assert b["reversed_at"] is None


@pytest.mark.asyncio
async def test_freed_budget_admits_a_follow_up_request(client):
    ds = _new_dataset_id("refill")
    await _register(client, ds, eps=0.3)
    assert (await _grant(client, ds, "big", 0.3, 0.0)).json()["allowed"]
    # The cap is exhausted: another 0.3 request is rejected.
    blocked = await _grant(client, ds, "next", 0.3, 0.0)
    assert blocked.json()["allowed"] is False

    assert (await _reverse(client, ds, "big", "rev-refill")).status_code == 201

    # Replaying the rejected request_id still replays the original rejection.
    replay = await _grant(client, ds, "next", 0.3, 0.0)
    assert replay.status_code == 200
    assert replay.json()["idempotent_replay"] is True
    assert replay.json()["allowed"] is False

    # A fresh request_id now fits in the released budget.
    admitted = await _grant(client, ds, "next2", 0.3, 0.0)
    assert admitted.status_code == 201
    assert admitted.json()["allowed"] is True
    budget = await _budget(client, ds)
    assert budget["granted_requests"] == 1
    assert budget["epsilon_consumed"] == pytest.approx(0.3, abs=1e-12)


@pytest.mark.asyncio
async def test_reversed_confirmed_reservation_frees_budget(client):
    ds = _new_dataset_id("confrev")
    await _register(client, ds, eps=1.0)
    rv = await client.post(
        f"/datasets/{ds}/reservations",
        json={"reservation_id": "rv", "epsilon": 0.4, "delta": 0.0,
              "ttl_seconds": 600},
    )
    assert rv.json()["allowed"] is True
    confirm = await client.post(
        f"/datasets/{ds}/reservations/rv/confirm",
        json={"request_id": "req-from-rv", "query_type": "histogram"},
    )
    assert confirm.status_code == 201

    r = await _reverse(client, ds, "req-from-rv", "rev-conf")
    assert r.status_code == 201
    assert r.json()["epsilon_after"] == pytest.approx(0.0, abs=1e-12)

    budget = await _budget(client, ds)
    assert budget["epsilon_consumed"] == pytest.approx(0.0, abs=1e-12)
    assert budget["epsilon_remaining"] == pytest.approx(1.0, abs=1e-12)

    # The reservation history itself is unchanged: confirmed, not released.
    got = await client.get(f"/datasets/{ds}/reservations/rv")
    assert got.json()["status"] == "confirmed"
    assert got.json()["confirmed_request_id"] == "req-from-rv"

    # New admissions can use the released capacity.
    assert (await _grant(client, ds, "fresh", 0.9, 0.0)).json()["allowed"]


@pytest.mark.asyncio
async def test_active_reservation_still_counts_after_reversal(client):
    ds = _new_dataset_id("mix")
    await _register(client, ds, eps=1.0)
    assert (await _grant(client, ds, "a", 0.3, 0.0)).json()["allowed"]
    rv = await client.post(
        f"/datasets/{ds}/reservations",
        json={"reservation_id": "rv", "epsilon": 0.4, "delta": 0.0,
              "ttl_seconds": 600},
    )
    assert rv.json()["allowed"] is True

    assert (await _reverse(client, ds, "a", "rev-mix")).status_code == 201

    budget = await _budget(client, ds)
    assert budget["epsilon_consumed"] == pytest.approx(0.0, abs=1e-12)
    # The still-active reservation keeps locking its 0.4.
    assert budget["epsilon_reserved"] == pytest.approx(0.4, abs=1e-9)
    assert budget["active_reservations"] == 1
    assert budget["epsilon_remaining"] == pytest.approx(0.6, abs=1e-9)

    # A new request that would fit without the reservation but not with it
    # must be rejected.
    blocked = await _grant(client, ds, "big", 0.7, 0.0)
    assert blocked.json()["allowed"] is False
    assert (await _grant(client, ds, "small", 0.6, 0.0)).json()["allowed"]


@pytest.mark.asyncio
async def test_reversal_reflected_in_release_forecast(client):
    ds = _new_dataset_id("fc")
    await _register(client, ds, eps=1.0)
    assert (await _grant(client, ds, "a", 0.2, 0.0)).json()["allowed"]
    assert (await _reverse(client, ds, "a", "rev-fc")).status_code == 201

    forecast = (
        await client.get(
            f"/datasets/{ds}/budget/release-forecast",
            params={"deadline": "2030-01-01T00:00:00Z"},
        )
    ).json()
    # Reversed requests are absent from the granted set of the simulation too.
    assert forecast["initial"]["epsilon_consumed"] == pytest.approx(0.0, abs=1e-12)
    assert forecast["at_deadline"]["epsilon_consumed"] == pytest.approx(0.0, abs=1e-12)


@pytest.mark.asyncio
async def test_reversal_reflected_in_budget_transfer_occupancy(client):
    ds = _new_dataset_id("xsrc")
    dst = _new_dataset_id("xdst")
    await _register(client, ds, eps=1.0)
    await _register(client, dst, eps=1.0)
    assert (await _grant(client, ds, "a", 0.8, 0.0)).json()["allowed"]

    # Before reversal, transferring 0.9 leaves 0.1 < 0.8 committed: rejected.
    bad = await client.post(
        "/budget-transfers",
        json={"transfer_id": "x-rev-bad", "source_dataset_id": ds,
              "target_dataset_id": dst, "epsilon_amount": 0.9},
    )
    assert bad.json()["allowed"] is False

    assert (await _reverse(client, ds, "a", "rev-x")).status_code == 201

    # After reversal the source occupancy is zero; the same shape of transfer
    # (new transfer_id) is admitted.
    ok = await client.post(
        "/budget-transfers",
        json={"transfer_id": "x-rev-ok", "source_dataset_id": ds,
              "target_dataset_id": dst, "epsilon_amount": 0.9},
    )
    assert ok.status_code == 201
    assert ok.json()["allowed"] is True
    assert ok.json()["source"]["epsilon_cap"] == pytest.approx(0.1, abs=1e-12)
    assert ok.json()["source"]["epsilon_committed"] == pytest.approx(0.0, abs=1e-12)


# ------------------------------------------------------------ state checks --


@pytest.mark.asyncio
async def test_reject_reversal_of_rejected_request(client):
    ds = _new_dataset_id("notgranted")
    await _register(client, ds, eps=0.01)
    rj = await _grant(client, ds, "no", 5.0, 1e-7)
    assert rj.json()["allowed"] is False

    r = await _reverse(client, ds, "no", "rev-rj")
    assert r.status_code == 409
    body = r.json()
    assert body["error"] == "request_not_granted"

    # Budget unchanged and no reversal stored.
    assert (await _budget(client, ds))["granted_requests"] == 0
    assert (await client.get(f"/datasets/{ds}/reversals")).json()["count"] == 0


@pytest.mark.asyncio
async def test_reversing_twice_conflicts(client):
    ds = _new_dataset_id("twice")
    await _register(client, ds, eps=1.0)
    assert (await _grant(client, ds, "a", 0.1, 0.0)).json()["allowed"]
    assert (await _reverse(client, ds, "a", "rev-twice-1")).status_code == 201

    again = await _reverse(client, ds, "a", "rev-twice-2")
    assert again.status_code == 409
    assert again.json()["error"] == "request_already_reversed"

    listing = (await client.get(f"/datasets/{ds}/reversals")).json()
    assert listing["count"] == 1
    assert listing["reversals"][0]["reversal_id"] == "rev-twice-1"


@pytest.mark.asyncio
async def test_reverse_missing_request_404(client):
    ds = _new_dataset_id("noreq")
    await _register(client, ds, eps=1.0)
    r = await _reverse(client, ds, "ghost", "rev-ghost")
    assert r.status_code == 404
    assert r.json()["error"] == "request_not_found"
    assert r.json()["request_id"] == "ghost"


@pytest.mark.asyncio
async def test_reverse_missing_dataset_404(client):
    r = await client.post(
        "/datasets/no-such-dataset/requests/a/reversals",
        json={"reversal_id": "rev-nods-1", "reason": "why not"},
    )
    assert r.status_code == 404
    assert r.json()["error"] == "dataset_not_found"


@pytest.mark.asyncio
async def test_invalid_reversal_payloads_are_422(client):
    ds = _new_dataset_id("badpayload")
    await _register(client, ds, eps=1.0)
    assert (await _grant(client, ds, "a", 0.1, 0.0)).json()["allowed"]
    base = f"/datasets/{ds}/requests/a/reversals"
    for payload in (
        {"reversal_id": "", "reason": "x"},
        {"reversal_id": "bad id", "reason": "x"},
        {"reversal_id": "r"},  # missing reason
        {"reversal_id": "r", "reason": ""},  # empty reason
        {"reversal_id": "r", "reason": "x" * 2001},
    ):
        r = await client.post(base, json=payload)
        assert r.status_code == 422, payload


# ------------------------------------------------------------ idempotency --


@pytest.mark.asyncio
async def test_same_reversal_id_same_payload_replays(client):
    ds = _new_dataset_id("idem")
    await _register(client, ds, eps=1.0)
    assert (await _grant(client, ds, "a", 0.2, 0.0)).json()["allowed"]
    payload = {"reversal_id": "rev-idem", "reason": "same reason"}

    first = await client.post(
        f"/datasets/{ds}/requests/a/reversals", json=payload
    )
    assert first.status_code == 201
    assert first.json()["idempotent_replay"] is False

    for _ in range(3):
        again = await client.post(
            f"/datasets/{ds}/requests/a/reversals", json=payload
        )
        assert again.status_code == 200
        body = again.json()
        assert body["idempotent_replay"] is True
        for key in (
            "reversal_id", "dataset_id", "request_id", "reason", "epsilon",
            "delta", "epsilon_cap", "epsilon_before", "epsilon_after",
            "rdp_best_order", "created_at",
        ):
            assert body[key] == first.json()[key]

    # Budget released exactly once and one record exists.
    budget = await _budget(client, ds)
    assert budget["epsilon_consumed"] == pytest.approx(0.0, abs=1e-12)
    listing = (await client.get(f"/datasets/{ds}/reversals")).json()
    assert listing["count"] == 1


@pytest.mark.asyncio
async def test_same_reversal_id_different_payload_conflicts(client):
    ds = _new_dataset_id("idemcf")
    other = _new_dataset_id("idemcf-other")
    await _register(client, ds, eps=1.0)
    await _register(client, other, eps=1.0)
    for rid in ("a", "b"):
        assert (await _grant(client, ds, rid, 0.1, 0.0)).json()["allowed"]
    assert (await _grant(client, other, "c", 0.1, 0.0)).json()["allowed"]

    first = await client.post(
        f"/datasets/{ds}/requests/a/reversals",
        json={"reversal_id": "rev-dup", "reason": "first"},
    )
    assert first.status_code == 201

    # Different reason on the same id -> 409.
    r_reason = await client.post(
        f"/datasets/{ds}/requests/a/reversals",
        json={"reversal_id": "rev-dup", "reason": "different"},
    )
    assert r_reason.status_code == 409
    assert r_reason.json()["error"] == "reversal_id_conflict"

    # Different target request -> 409.
    r_target = await client.post(
        f"/datasets/{ds}/requests/b/reversals",
        json={"reversal_id": "rev-dup", "reason": "first"},
    )
    assert r_target.status_code == 409
    assert r_target.json()["error"] == "reversal_id_conflict"

    # Different dataset (global idempotency key) -> 409.
    r_ds = await client.post(
        f"/datasets/{other}/requests/c/reversals",
        json={"reversal_id": "rev-dup", "reason": "first"},
    )
    assert r_ds.status_code == 409
    assert r_ds.json()["error"] == "reversal_id_conflict"

    # Conflicting attempts changed nothing.
    listing = (await client.get(f"/datasets/{ds}/reversals")).json()
    assert listing["count"] == 1
    assert listing["reversals"][0]["request_id"] == "a"
    assert listing["reversals"][0]["reason"] == "first"
    assert (await _budget(client, ds))["epsilon_consumed"] == pytest.approx(
        0.1, abs=1e-9
    )


@pytest.mark.asyncio
async def test_original_request_id_replay_still_returns_original_verdict(client):
    ds = _new_dataset_id("reqreplay")
    await _register(client, ds, eps=0.3)
    payload = {"request_id": "r", "query_type": "laplace",
               "epsilon": 0.3, "delta": 0.0}
    first = await client.post(f"/datasets/{ds}/requests", json=payload)
    assert first.status_code == 201
    assert (await _reverse(client, ds, "r", "rev-r")).status_code == 201

    replay = await client.post(f"/datasets/{ds}/requests", json=payload)
    assert replay.status_code == 200
    body = replay.json()
    assert body["idempotent_replay"] is True
    assert body["epsilon_after"] == first.json()["epsilon_after"]
    # The replay does not re-occupy budget.
    budget = await _budget(client, ds)
    assert budget["granted_requests"] == 0
    assert budget["epsilon_consumed"] == pytest.approx(0.0, abs=1e-12)


# ---------------------------------------------------------------- queries --


@pytest.mark.asyncio
async def test_list_and_get_reversal_records(client):
    ds = _new_dataset_id("query")
    await _register(client, ds, eps=1.0)
    assert (await _grant(client, ds, "a", 0.1, 0.0)).json()["allowed"]
    assert (await _grant(client, ds, "b", 0.2, 0.0)).json()["allowed"]
    assert (await _reverse(client, ds, "a", "rev-a", "why a")).status_code == 201
    assert (await _reverse(client, ds, "b", "rev-b", "why b")).status_code == 201

    listing = await client.get(f"/datasets/{ds}/reversals")
    assert listing.status_code == 200
    body = listing.json()
    assert body["dataset_id"] == ds
    assert body["count"] == 2
    assert [r["reversal_id"] for r in body["reversals"]] == ["rev-a", "rev-b"]
    assert [r["request_id"] for r in body["reversals"]] == ["a", "b"]

    one = await client.get(
        f"/datasets/{ds}/requests/b/reversals/rev-b"
    )
    assert one.status_code == 200
    got = one.json()
    assert got["reversal_id"] == "rev-b"
    assert got["request_id"] == "b"
    assert got["reason"] == "why b"
    assert got["idempotent_replay"] is False

    # Mismatched reversal_id on a reversed request -> 404.
    wrong = await client.get(
        f"/datasets/{ds}/requests/b/reversals/rev-a"
    )
    assert wrong.status_code == 404
    assert wrong.json()["error"] == "reversal_not_found"

    # Reversal id on a request that does not exist -> request 404.
    missing_req = await client.get(
        f"/datasets/{ds}/requests/nope/reversals/rev-a"
    )
    assert missing_req.status_code == 404
    assert missing_req.json()["error"] == "request_not_found"


@pytest.mark.asyncio
async def test_reversal_listing_on_missing_dataset_404(client):
    r = await client.get("/datasets/no-such-dataset/reversals")
    assert r.status_code == 404
    assert r.json()["error"] == "dataset_not_found"


# ------------------------------------------------------------- concurrency --


@pytest.mark.asyncio
async def test_concurrent_duplicate_reversal_ids_create_one_record(client):
    ds = _new_dataset_id("concidem")
    await _register(client, ds, eps=1.0)
    assert (await _grant(client, ds, "a", 0.1, 0.0)).json()["allowed"]
    payload = {"reversal_id": "rev-same", "reason": "concurrent"}

    results = await asyncio.gather(*[
        client.post(
            f"/datasets/{ds}/requests/a/reversals", json=payload
        )
        for _ in range(20)
    ])
    created = [r for r in results if r.status_code == 201]
    replays = [r for r in results if r.status_code == 200]
    assert len(created) == 1
    assert len(replays) == 19
    assert (await client.get(f"/datasets/{ds}/reversals")).json()["count"] == 1


@pytest.mark.asyncio
async def test_concurrent_distinct_reversals_only_one_wins(client):
    ds = _new_dataset_id("concstate")
    await _register(client, ds, eps=1.0)
    assert (await _grant(client, ds, "a", 0.1, 0.0)).json()["allowed"]

    results = await asyncio.gather(*[
        client.post(
            f"/datasets/{ds}/requests/a/reversals",
            json={"reversal_id": f"rev-conc-{i:02d}", "reason": "race"},
        )
        for i in range(15)
    ])
    created = [r for r in results if r.status_code == 201]
    conflicts = [r for r in results if r.status_code == 409]
    assert len(created) == 1
    assert len(conflicts) == 14
    assert all(
        r.json()["error"] == "request_already_reversed" for r in conflicts
    )
    assert (await client.get(f"/datasets/{ds}/reversals")).json()["count"] == 1
    assert (await _budget(client, ds))["epsilon_consumed"] == pytest.approx(
        0.0, abs=1e-12
    )


# ------------------------------------------------------------- migration --


@pytest.mark.asyncio
async def test_migration_from_v3_schema_adds_reversals_table(
    tmp_path, monkeypatch
):
    old_db_path = str(tmp_path / "v3.db")

    conn = await aiosqlite.connect(old_db_path)
    try:
        await conn.executescript(db.SCHEMA_V1)
        await conn.executescript(db.SCHEMA_V2)
        await conn.executescript(db.SCHEMA_V3)
        await conn.execute("PRAGMA user_version = 3")
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
            assert (await cur.fetchone())[0] == db.LATEST_VERSION
        async with c.execute(
            "SELECT name FROM sqlite_master "
            "WHERE type='table' AND name='request_reversals'"
        ) as cur:
            assert await cur.fetchone() is not None
        # Pre-existing granted request keeps counting until reversed.
        entries = await db.list_granted_entries(c, "legacy")
        assert entries == [(0.1, 0.0)]
