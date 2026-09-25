"""End-to-end tests for epsilon-cap budget transfers (POST /budget-transfers)."""
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
_tmp = tempfile.mkdtemp(prefix="dpledger-test-xfer-")
os.environ["DATABASE_PATH"] = os.path.join(_tmp, "transfers.db")

from httpx import ASGITransport, AsyncClient  # noqa: E402

from app import db, rdp  # noqa: E402
from app.config import settings  # noqa: E402
from app.main import app  # noqa: E402

_counter = itertools.count()


def _new_dataset_id(name: str) -> str:
    return f"dsx-{name}-{next(_counter)}"


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


def _xfer(transfer_id: str, source: str, target: str, amount: float) -> dict:
    return {
        "transfer_id": transfer_id,
        "source_dataset_id": source,
        "target_dataset_id": target,
        "epsilon_amount": amount,
    }


async def _cap(client, dataset_id: str) -> dict:
    r = await client.get(f"/datasets/{dataset_id}/budget")
    assert r.status_code == 200
    return r.json()


# ------------------------------------------------------------------ happy --


@pytest.mark.asyncio
async def test_transfer_moves_epsilon_caps_and_nothing_else(client):
    src = _new_dataset_id("sx-static-src")
    dst = _new_dataset_id("sx-static-dst")
    await _register(client, src, eps=1.0, delta=1e-6)
    await _register(client, dst, eps=0.5, delta=1e-8)

    # Pre-existing granted request on the source survives the transfer.
    req = await client.post(
        f"/datasets/{src}/requests",
        json={"request_id": "r1", "query_type": "laplace",
              "epsilon": 0.1, "delta": 0.0},
    )
    assert req.status_code == 201
    assert req.json()["allowed"] is True

    r = await client.post("/budget-transfers", json=_xfer("xmove-1", src, dst, 0.25))
    assert r.status_code == 201
    body = r.json()
    assert body["allowed"] is True
    assert body["status"] == "transferred"
    assert body["reason"] is None
    assert body["idempotent_replay"] is False
    assert body["epsilon_amount"] == 0.25
    assert body["source"] == {
        "dataset_id": src,
        "epsilon_cap": 0.75,
        "epsilon_committed": pytest.approx(0.1, abs=1e-12),
        "epsilon_remaining": pytest.approx(0.65, abs=1e-12),
    }
    assert body["target"]["dataset_id"] == dst
    assert body["target"]["epsilon_cap"] == 0.75
    assert body["target"]["epsilon_committed"] == 0.0
    assert body["target"]["epsilon_remaining"] == pytest.approx(0.75, abs=1e-12)

    # Budget views reflect the new caps; delta caps and recorded requests
    # are untouched.
    src_b = await _cap(client, src)
    assert src_b["epsilon_cap"] == 0.75
    assert src_b["delta_cap"] == 1e-6
    assert src_b["granted_requests"] == 1
    assert src_b["epsilon_consumed"] == pytest.approx(0.1, abs=1e-12)

    dst_b = await _cap(client, dst)
    assert dst_b["epsilon_cap"] == 0.75
    assert dst_b["delta_cap"] == 1e-8
    assert dst_b["granted_requests"] == 0

    listing = (await client.get(f"/datasets/{src}/requests")).json()
    assert listing["count"] == 1
    assert listing["requests"][0]["request_id"] == "r1"


@pytest.mark.asyncio
async def test_transfer_keeps_total_epsilon_constant_both_directions(client):
    a = _new_dataset_id("ab-a")
    b = _new_dataset_id("ab-b")
    await _register(client, a, eps=1.0)
    await _register(client, b, eps=1.0)

    r1 = await client.post("/budget-transfers", json=_xfer("xboth-1", a, b, 0.3))
    assert r1.json()["allowed"] is True
    r2 = await client.post("/budget-transfers", json=_xfer("xboth-2", b, a, 0.12))
    assert r2.json()["allowed"] is True

    ca = (await _cap(client, a))["epsilon_cap"]
    cb = (await _cap(client, b))["epsilon_cap"]
    assert ca == pytest.approx(0.82, abs=1e-12)
    assert cb == pytest.approx(1.18, abs=1e-12)
    assert ca + cb == pytest.approx(2.0, abs=1e-12)


# --------------------------------------------------------------- rejection --


@pytest.mark.asyncio
async def test_reject_when_source_cap_would_cover_no_occupancy_boundary(client):
    src = _new_dataset_id("zero")
    dst = _new_dataset_id("zero-dst")
    await _register(client, src, eps=0.5)
    await _register(client, dst, eps=0.5)

    # Debiting the whole cap would leave zero, which is not positive.
    r = await client.post("/budget-transfers", json=_xfer("xzero-1", src, dst, 0.5))
    assert r.status_code == 201
    body = r.json()
    assert body["allowed"] is False
    assert body["status"] == "rejected"
    assert "positive" in body["reason"]
    # Both caps unchanged, and the view reports the unchanged caps.
    assert body["source"]["epsilon_cap"] == 0.5
    assert body["target"]["epsilon_cap"] == 0.5

    assert (await _cap(client, src))["epsilon_cap"] == 0.5
    assert (await _cap(client, dst))["epsilon_cap"] == 0.5


@pytest.mark.asyncio
async def test_reject_when_debit_overdraws_committed_occupancy(client):
    src = _new_dataset_id("occ")
    dst = _new_dataset_id("occ-dst")
    await _register(client, src, eps=1.0, delta=1e-6)
    await _register(client, dst, eps=0.2, delta=1e-6)

    # A 0.2 pure-DP request occupies exactly 0.2; leaving a 0.2 cap would
    # still cover it, leaving 0.1 would not.
    granted = await client.post(
        f"/datasets/{src}/requests",
        json={"request_id": "g", "query_type": "laplace",
              "epsilon": 0.2, "delta": 0.0},
    )
    assert granted.json()["allowed"] is True

    ok = await client.post(
        "/budget-transfers", json=_xfer("xocc-ok", src, dst, 0.8)
    )
    assert ok.json()["allowed"] is True
    assert ok.json()["source"]["epsilon_cap"] == pytest.approx(0.2, abs=1e-12)
    assert ok.json()["source"]["epsilon_committed"] == pytest.approx(0.2, abs=1e-12)
    assert ok.json()["source"]["epsilon_remaining"] == pytest.approx(0.0, abs=1e-9)
    # Occupancy is RDP-projected and can exceed the nominal request epsilon.

    bad = await client.post(
        "/budget-transfers", json=_xfer("xocc-bad", src, dst, 0.1)
    )
    body = bad.json()
    assert body["allowed"] is False
    assert "committed epsilon" in body["reason"]
    # Caps stay as they were after the successful transfer.
    assert body["source"]["epsilon_cap"] == pytest.approx(0.2, abs=1e-12)
    assert body["target"]["epsilon_cap"] == pytest.approx(1.0, abs=1e-12)
    assert (await _cap(client, src))["epsilon_cap"] == pytest.approx(0.2, abs=1e-12)
    assert (await _cap(client, dst))["epsilon_cap"] == pytest.approx(1.0, abs=1e-12)


@pytest.mark.asyncio
async def test_active_reservation_occupies_the_source_cap(client):
    src = _new_dataset_id("res")
    dst = _new_dataset_id("res-dst")
    await _register(client, src, eps=1.0)
    await _register(client, dst, eps=0.5)

    rv = await client.post(
        f"/datasets/{src}/reservations",
        json={"reservation_id": "rv", "epsilon": 0.4, "delta": 0.0,
              "ttl_seconds": 600},
    )
    assert rv.json()["allowed"] is True

    # Leaving 0.5 cap covers the 0.4 locked reservation; leaving 0.3 does not.
    ok = await client.post("/budget-transfers", json=_xfer("xres-ok", src, dst, 0.5))
    assert ok.json()["allowed"] is True
    assert ok.json()["source"]["epsilon_committed"] == pytest.approx(0.4, abs=1e-12)

    bad = await client.post("/budget-transfers", json=_xfer("xres-bad", src, dst, 0.2))
    assert bad.json()["allowed"] is False
    assert "committed epsilon" in bad.json()["reason"]
    assert (await _cap(client, src))["epsilon_cap"] == pytest.approx(0.5, abs=1e-12)


@pytest.mark.asyncio
async def test_reject_when_target_would_exceed_registered_ceiling(client):
    src = _new_dataset_id("ceil-src")
    dst = _new_dataset_id("ceil-dst")
    await _register(client, src, eps=2.0)
    await _register(client, dst, eps=999.5)

    over = await client.post(
        "/budget-transfers", json=_xfer("xceil-over", src, dst, 0.6)
    )
    body = over.json()
    assert body["allowed"] is False
    assert "1000" in body["reason"]
    assert body["source"]["epsilon_cap"] == 2.0
    assert body["target"]["epsilon_cap"] == 999.5

    # Exactly to the ceiling is fine.
    exact = await client.post(
        "/budget-transfers", json=_xfer("xceil-exact", src, dst, 0.5)
    )
    assert exact.json()["allowed"] is True
    assert exact.json()["target"]["epsilon_cap"] == pytest.approx(1000.0, abs=1e-9)
    assert exact.json()["source"]["epsilon_cap"] == 1.5


@pytest.mark.asyncio
async def test_rejected_transfer_does_not_block_later_valid_one(client):
    src = _new_dataset_id("retry-src")
    dst = _new_dataset_id("retry-dst")
    await _register(client, src, eps=0.5)
    await _register(client, dst, eps=0.5)

    bad = await client.post(
        "/budget-transfers", json=_xfer("xretry-big", src, dst, 0.5)
    )
    assert bad.json()["allowed"] is False
    good = await client.post(
        "/budget-transfers", json=_xfer("xretry-small", src, dst, 0.1)
    )
    assert good.json()["allowed"] is True
    assert good.json()["source"]["epsilon_cap"] == 0.4
    assert good.json()["target"]["epsilon_cap"] == 0.6


# ------------------------------------------------------------- validation --


@pytest.mark.asyncio
async def test_source_dataset_missing_404(client):
    dst = _new_dataset_id("only-dst")
    await _register(client, dst, eps=1.0)
    r = await client.post(
        "/budget-transfers",
        json=_xfer("x404-1", "no-such-source", dst, 0.1),
    )
    assert r.status_code == 404
    assert r.json()["error"] == "dataset_not_found"
    assert r.json()["dataset_id"] == "no-such-source"


@pytest.mark.asyncio
async def test_target_dataset_missing_404(client):
    src = _new_dataset_id("only-src")
    await _register(client, src, eps=1.0)
    r = await client.post(
        "/budget-transfers",
        json=_xfer("x404-2", src, "no-such-target", 0.1),
    )
    assert r.status_code == 404
    assert r.json()["error"] == "dataset_not_found"
    assert r.json()["dataset_id"] == "no-such-target"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "payload",
    [
        {"transfer_id": "z", "source_dataset_id": "a",
         "target_dataset_id": "a", "epsilon_amount": 0.1},
        {"transfer_id": "z", "source_dataset_id": "a",
         "target_dataset_id": "b", "epsilon_amount": 0.0},
        {"transfer_id": "z", "source_dataset_id": "a",
         "target_dataset_id": "b", "epsilon_amount": -0.1},
        {"transfer_id": "", "source_dataset_id": "a",
         "target_dataset_id": "b", "epsilon_amount": 0.1},
        {"transfer_id": "bad id", "source_dataset_id": "a",
         "target_dataset_id": "b", "epsilon_amount": 0.1},
        {"transfer_id": "z", "source_dataset_id": "bad id",
         "target_dataset_id": "b", "epsilon_amount": 0.1},
    ],
)
async def test_invalid_payloads_are_422(client, payload):
    r = await client.post("/budget-transfers", json=payload)
    assert r.status_code == 422


# ------------------------------------------------------------ idempotency --


@pytest.mark.asyncio
async def test_idempotent_replay_returns_original_record_without_new_transfer(
    client,
):
    src = _new_dataset_id("idem-src")
    dst = _new_dataset_id("idem-dst")
    await _register(client, src, eps=1.0)
    await _register(client, dst, eps=1.0)
    payload = _xfer("xidem-1", src, dst, 0.2)

    first = await client.post("/budget-transfers", json=payload)
    assert first.status_code == 201
    assert first.json()["idempotent_replay"] is False

    for _ in range(3):
        again = await client.post("/budget-transfers", json=payload)
        assert again.status_code == 200
        body = again.json()
        assert body["idempotent_replay"] is True
        for key in (
            "transfer_id", "source_dataset_id", "target_dataset_id",
            "epsilon_amount", "allowed", "status", "reason",
            "source", "target", "created_at",
        ):
            assert body[key] == first.json()[key]

    # Exactly one debit happened.
    assert (await _cap(client, src))["epsilon_cap"] == pytest.approx(0.8, abs=1e-12)
    assert (await _cap(client, dst))["epsilon_cap"] == pytest.approx(1.2, abs=1e-12)


@pytest.mark.asyncio
async def test_rejected_transfer_replay_is_idempotent(client):
    src = _new_dataset_id("rj-src")
    dst = _new_dataset_id("rj-dst")
    await _register(client, src, eps=0.5)
    await _register(client, dst, eps=0.5)
    payload = _xfer("xrj-1", src, dst, 0.5)  # leaves zero -> rejected

    first = await client.post("/budget-transfers", json=payload)
    assert first.status_code == 201
    assert first.json()["allowed"] is False
    assert first.json()["idempotent_replay"] is False

    again = await client.post("/budget-transfers", json=payload)
    assert again.status_code == 200
    assert again.json()["idempotent_replay"] is True
    assert again.json()["allowed"] is False
    assert again.json()["reason"] == first.json()["reason"]
    assert (await _cap(client, src))["epsilon_cap"] == 0.5
    assert (await _cap(client, dst))["epsilon_cap"] == 0.5


@pytest.mark.asyncio
async def test_same_transfer_id_different_payload_conflicts_409(client):
    src = _new_dataset_id("cf-src")
    dst = _new_dataset_id("cf-dst")
    other = _new_dataset_id("cf-other")
    for ds in (src, dst, other):
        await _register(client, ds, eps=1.0)

    first = _xfer("xdup-1", src, dst, 0.1)
    assert (await client.post("/budget-transfers", json=first)).status_code == 201

    for mutated in (
        _xfer("xdup-1", src, other, 0.1),
        _xfer("xdup-1", other, dst, 0.1),
        _xfer("xdup-1", src, dst, 0.2),
    ):
        r = await client.post("/budget-transfers", json=mutated)
        assert r.status_code == 409
        assert r.json()["error"] == "transfer_id_conflict"
        assert r.json()["transfer_id"] == "xdup-1"

    # Caps follow only the original transfer.
    assert (await _cap(client, src))["epsilon_cap"] == 0.9
    assert (await _cap(client, dst))["epsilon_cap"] == 1.1
    assert (await _cap(client, other))["epsilon_cap"] == 1.0


# ------------------------------------------------------------- concurrency --


@pytest.mark.asyncio
async def test_concurrent_transfers_never_overdraw(client):
    src = _new_dataset_id("cc-src")
    dst = _new_dataset_id("cc-dst")
    await _register(client, src, eps=1.0, delta=1e-6)
    await _register(client, dst, eps=0.2, delta=1e-6)

    n = 40
    results = await asyncio.gather(*[
        client.post(
            "/budget-transfers",
            json=_xfer(f"xconc-{i:03d}", src, dst, 0.04),
        )
        for i in range(n)
    ])
    assert all(r.status_code in (200, 201) for r in results)
    verdicts = [r.json() for r in results]
    transferred = [v for v in verdicts if v["allowed"]]
    rejected = [v for v in verdicts if not v["allowed"]]

    # Each successful debit of 0.04 must leave a strictly positive source
    # cap: 24 debits leave 0.04, a 25th would leave zero, so exactly 24 land.
    assert len(transferred) == 24
    assert len(rejected) == n - 24

    src_cap = (await _cap(client, src))["epsilon_cap"]
    dst_cap = (await _cap(client, dst))["epsilon_cap"]
    assert src_cap > 0.0
    assert src_cap == pytest.approx(
        1.0 - 0.04 * len(transferred), abs=1e-9
    )
    assert dst_cap == pytest.approx(
        0.2 + 0.04 * len(transferred), abs=1e-9
    )
    assert src_cap + dst_cap == pytest.approx(1.2, abs=1e-9)

    # A further debit larger than the remaining cap is refused, and the
    # rejected verdict stored cap snapshots equal the live caps.
    last = await client.post(
        "/budget-transfers", json=_xfer("xconc-final", src, dst, src_cap + 0.01)
    )
    assert last.json()["allowed"] is False
    assert last.json()["source"]["epsilon_cap"] == pytest.approx(src_cap, abs=1e-12)
    assert (await _cap(client, src))["epsilon_cap"] == pytest.approx(src_cap, abs=1e-12)


@pytest.mark.asyncio
async def test_concurrent_transfers_and_requests_never_overdraw(client):
    src = _new_dataset_id("mix-src")
    dst = _new_dataset_id("mix-dst")
    await _register(client, src, eps=1.0, delta=1e-6)
    await _register(client, dst, eps=0.2, delta=1e-6)

    async def transfer(i: int):
        return await client.post(
            "/budget-transfers",
            json=_xfer(f"xmix-{i:03d}", src, dst, 0.05),
        )

    async def request(i: int):
        return await client.post(
            f"/datasets/{src}/requests",
            json={"request_id": f"r-{i:03d}", "query_type": "laplace",
                  "epsilon": 0.05, "delta": 0.0},
        )

    xfer_results, request_results = await asyncio.gather(
        asyncio.gather(*[transfer(i) for i in range(20)]),
        asyncio.gather(*[request(i) for i in range(20)]),
    )
    results = [*xfer_results, *request_results]
    assert all(r.status_code in (200, 201) for r in results)

    budget = await _cap(client, src)
    # Granted requests must fit inside whatever cap the transfers left.
    assert budget["epsilon_consumed"] <= budget["epsilon_cap"] + 1e-9
    assert budget["epsilon_remaining"] >= -1e-9

    # Independent recomposition of the granted requests stays under the cap.
    granted = (await client.get(f"/datasets/{src}/requests")).json()["requests"]
    entries = [(r["epsilon"], r["delta"]) for r in granted
               if r["status"] == "granted"]
    _, committed, _ = rdp.evaluate_entries(entries, 1e-6)
    assert committed <= budget["epsilon_cap"] + 1e-9

    transfers_done = sum(
        1 for r in xfer_results if r.json().get("allowed")
    )
    dst_cap = (await _cap(client, dst))["epsilon_cap"]
    assert dst_cap == pytest.approx(0.2 + 0.05 * transfers_done, abs=1e-9)
    assert budget["epsilon_cap"] + dst_cap == pytest.approx(1.2, abs=1e-9)


@pytest.mark.asyncio
async def test_concurrent_duplicate_transfer_ids_create_one_record(client):
    src = _new_dataset_id("ci-src")
    dst = _new_dataset_id("ci-dst")
    await _register(client, src, eps=1.0)
    await _register(client, dst, eps=1.0)
    payload = _xfer("xsame-1", src, dst, 0.1)

    results = await asyncio.gather(*[
        client.post("/budget-transfers", json=payload) for _ in range(20)
    ])
    created = [r for r in results if r.status_code == 201]
    replays = [r for r in results if r.status_code == 200]
    assert len(created) == 1
    assert len(replays) == 19
    assert (await _cap(client, src))["epsilon_cap"] == pytest.approx(0.9, abs=1e-12)
    assert (await _cap(client, dst))["epsilon_cap"] == pytest.approx(1.1, abs=1e-12)


# ------------------------------------------------------------- migration --


@pytest.mark.asyncio
async def test_migration_adds_budget_transfers_table(tmp_path, monkeypatch):
    old_db_path = str(tmp_path / "v2.db")

    conn = await aiosqlite.connect(old_db_path)
    try:
        await conn.executescript(db.SCHEMA_V1)
        await conn.executescript(db.SCHEMA_V2)
        await conn.execute("PRAGMA user_version = 2")
        await conn.execute(
            "INSERT INTO datasets "
            "(dataset_id, epsilon_cap, delta_cap, description, created_at) "
            "VALUES ('legacy-a', 1.0, 1e-6, 'old', '2026-01-01T00:00:00+00:00'),"
            "       ('legacy-b', 0.5, 1e-6, 'old', '2026-01-01T00:00:00+00:00')"
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
            "WHERE type='table' AND name='budget_transfers'"
        ) as cur:
            assert await cur.fetchone() is not None
        for ds_id, cap in (("legacy-a", 1.0), ("legacy-b", 0.5)):
            row = await db.require_dataset(c, ds_id)
            assert row["epsilon_cap"] == cap
