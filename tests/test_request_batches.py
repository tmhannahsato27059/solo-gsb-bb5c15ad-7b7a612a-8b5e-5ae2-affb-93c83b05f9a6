"""End-to-end tests for atomic analysis batches."""
from __future__ import annotations

import asyncio
import dataclasses
import itertools
import math
import os
import tempfile
from datetime import datetime

import aiosqlite
import pytest
import pytest_asyncio

_tmp = tempfile.mkdtemp(prefix="dpledger-batch-test-")
os.environ["DATABASE_PATH"] = os.path.join(_tmp, "batch-test.db")

from httpx import ASGITransport, AsyncClient  # noqa: E402

from app import db, rdp  # noqa: E402
from app.config import settings  # noqa: E402
from app.main import app  # noqa: E402


@pytest_asyncio.fixture()
async def client():
    await db.init_db()
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac


_counter = itertools.count()


def _ds(name: str) -> str:
    return f"ds-batch-{name}-{next(_counter)}"


async def _register(client, dataset_id: str, eps: float = 1.0, delta: float = 1e-6):
    return await client.post(
        f"/datasets/{dataset_id}",
        json={"epsilon_cap": eps, "delta_cap": delta,
              "description": "test dataset"},
    )


def _batch(batch_id: str, items):
    return {"batch_id": batch_id, "items": items}


def _item(rid: str, epsilon: float, delta: float = 1e-7, query_type: str = "q"):
    return {"request_id": rid, "query_type": query_type,
            "epsilon": epsilon, "delta": delta}


async def _budget(client, ds):
    return (await client.get(f"/datasets/{ds}/budget")).json()


# --------------------------------------------------------------- happy path --


@pytest.mark.asyncio
async def test_granted_batch_admits_all_items_with_joint_composition(client):
    ds = _ds("grant")
    cap_eps, cap_delta = 1.0, 1e-6
    await _register(client, ds, eps=cap_eps, delta=cap_delta)

    payload = _batch("b1", [_item("a", 0.1), _item("b", 0.15), _item("c", 0.2)])
    r = await client.post(f"/datasets/{ds}/request-batches", json=payload)
    assert r.status_code == 201
    body = r.json()
    assert body["allowed"] is True
    assert body["status"] == "granted"
    assert body["reason"] is None
    assert body["idempotent_replay"] is False
    assert body["item_count"] == 3

    # Independent RDP cross-check of the joint composition.
    expected, expected_order = rdp.composed_epsilon(
        [rdp.single_rdp_curve(it["epsilon"], it["delta"]) for it in payload["items"]],
        cap_delta,
    )
    assert body["epsilon_before"] == pytest.approx(0.0, abs=1e-12)
    assert body["epsilon_after"] == pytest.approx(expected, abs=1e-12)
    assert body["rdp_best_order"] == expected_order
    assert body["epsilon_remaining"] == pytest.approx(cap_eps - expected, abs=1e-12)

    # Per-item cumulative composed-epsilon snapshots are monotonically growing.
    befores = [it["epsilon_before"] for it in body["items"]]
    afters = [it["epsilon_after"] for it in body["items"]]
    assert befores[0] == pytest.approx(0.0, abs=1e-12)
    assert afters[-1] == pytest.approx(expected, abs=1e-12)
    for earlier, later in zip(afters, afters[1:]):
        assert later > earlier
    for before, after, item in zip(befores, afters, body["items"]):
        assert after > before
        assert item["allowed"] is True
        assert item["status"] == "granted"
        assert item["reason"] is None
        assert item["epsilon_remaining"] == pytest.approx(
            cap_eps - after, abs=1e-12
        )

    budget = await _budget(client, ds)
    assert budget["granted_requests"] == 3
    assert budget["epsilon_consumed"] == pytest.approx(expected, abs=1e-12)

    # Every item is an ordinary granted analysis request.
    listing = (await client.get(f"/datasets/{ds}/requests")).json()
    assert [(x["request_id"], x["status"]) for x in listing["requests"]] == [
        ("a", "granted"), ("b", "granted"), ("c", "granted"),
    ]


@pytest.mark.asyncio
async def test_pure_epsilon_batch_composition_is_additive(client):
    ds = _ds("pure")
    await _register(client, ds, eps=1.0, delta=1e-12)
    r = await client.post(
        f"/datasets/{ds}/request-batches",
        json=_batch("b", [_item("a", 0.1, 0.0, "laplace"),
                          _item("b", 0.2, 0.0, "laplace")]),
    )
    assert r.status_code == 201
    assert r.json()["epsilon_after"] == pytest.approx(0.3, abs=1e-12)
    budget = await _budget(client, ds)
    assert budget["epsilon_consumed"] == pytest.approx(0.3, abs=1e-12)
    assert budget["epsilon_remaining"] == pytest.approx(0.7, abs=1e-12)


@pytest.mark.asyncio
async def test_batch_with_tiny_delta_items_is_granted(client):
    # Regression: 1/delta overflows float64 for delta=1e-309; the log-domain
    # curve keeps the joint projection finite so the whole batch is admitted.
    ds = _ds("tinydelta")
    cap_eps, cap_delta = 1.0, 1e-6
    await _register(client, ds, eps=cap_eps, delta=cap_delta)

    payload = _batch(
        "b-tiny", [_item("ta", 0.1, 1e-309), _item("tb", 0.1, 1e-309)]
    )
    r = await client.post(f"/datasets/{ds}/request-batches", json=payload)
    assert r.status_code == 201
    body = r.json()
    assert body["allowed"] is True
    assert body["status"] == "granted"

    expected, expected_order = rdp.composed_epsilon(
        [rdp.single_rdp_curve(it["epsilon"], it["delta"]) for it in payload["items"]],
        cap_delta,
    )
    assert math.isfinite(expected)
    assert body["epsilon_after"] == pytest.approx(expected, abs=1e-12)
    assert body["rdp_best_order"] == expected_order
    assert all(
        math.isfinite(item["epsilon_after"]) for item in body["items"]
    )

    budget = await _budget(client, ds)
    assert budget["granted_requests"] == 2
    assert budget["epsilon_consumed"] == pytest.approx(expected, abs=1e-12)


# -------------------------------------------------------------- all-or-none --


@pytest.mark.asyncio
async def test_over_budget_batch_admits_nothing_and_is_persisted(client):
    ds = _ds("reject")
    await _register(client, ds, eps=0.5, delta=1e-6)

    # First item alone would fit (0.1), the two together do not.
    payload = _batch("big", [_item("a", 0.1), _item("b", 0.9)])
    r = await client.post(f"/datasets/{ds}/request-batches", json=payload)
    assert r.status_code == 201
    body = r.json()
    assert body["allowed"] is False
    assert body["status"] == "rejected"
    assert body["idempotent_replay"] is False
    assert "batch rejected" in body["reason"]
    assert body["epsilon_after"] == pytest.approx(0.0, abs=1e-12)
    assert body["epsilon_remaining"] == pytest.approx(0.5, abs=1e-12)
    assert body["item_count"] == 2
    for item in body["items"]:
        assert item["allowed"] is False
        assert item["status"] == "rejected"
        assert item["epsilon_before"] is None
        assert item["epsilon_after"] is None
        assert item["epsilon_remaining"] is None
        assert item["rdp_best_order"] is None
        assert item["reason"] == body["reason"]

    # Nothing was admitted: no request rows, zero consumption.
    budget = await _budget(client, ds)
    assert budget["granted_requests"] == 0
    assert budget["epsilon_consumed"] == 0.0
    listing = (await client.get(f"/datasets/{ds}/requests")).json()
    assert listing["count"] == 0

    # The rejected verdict itself is persisted and queryable.
    got = await client.get(f"/datasets/{ds}/request-batches/big")
    assert got.status_code == 200
    stored = got.json()
    assert stored["allowed"] is False
    assert stored["status"] == "rejected"
    assert stored["reason"] == body["reason"]
    assert stored["idempotent_replay"] is False
    assert [it["request_id"] for it in stored["items"]] == ["a", "b"]
    assert datetime.fromisoformat(stored["created_at"]) == datetime.fromisoformat(
        body["created_at"]
    )

    # Rejected item ids never became occupied: a fresh batch may reuse them.
    reuse = await client.post(
        f"/datasets/{ds}/request-batches",
        json=_batch("reuse", [_item("a", 0.1), _item("b", 0.1)]),
    )
    assert reuse.status_code == 201
    assert reuse.json()["allowed"] is True


@pytest.mark.asyncio
async def test_rejected_batch_does_not_block_later_fitting_batch(client):
    ds = _ds("reject-then-fit")
    await _register(client, ds, eps=0.5, delta=1e-6)
    bad = await client.post(
        f"/datasets/{ds}/request-batches",
        json=_batch("bad", [_item("a", 0.4), _item("b", 0.4)]),
    )
    assert bad.json()["allowed"] is False
    good = await client.post(
        f"/datasets/{ds}/request-batches",
        json=_batch("good", [_item("c", 0.1), _item("d", 0.1)]),
    )
    assert good.status_code == 201
    assert good.json()["allowed"] is True
    assert (await _budget(client, ds))["granted_requests"] == 2


# ------------------------------------------------------------- replay / GET --


@pytest.mark.asyncio
async def test_batch_replay_same_payload_returns_first_verdict(client):
    ds = _ds("replay")
    await _register(client, ds, eps=1.0, delta=1e-6)
    payload = _batch("b1", [_item("a", 0.1), _item("b", 0.2)])

    first = await client.post(f"/datasets/{ds}/request-batches", json=payload)
    assert first.status_code == 201
    # Consume more budget elsewhere so a fresh decision would differ; the
    # replay must still return the first verdict and its composition values.
    fill = await client.post(
        f"/datasets/{ds}/requests",
        json=_item("fill", 0.1, 0.0, "laplace"),
    )
    assert fill.json()["allowed"] is True

    again = await client.post(f"/datasets/{ds}/request-batches", json=payload)
    assert again.status_code == 200
    body = again.json()
    assert body["idempotent_replay"] is True
    assert body["epsilon_after"] == first.json()["epsilon_after"]
    assert body["epsilon_before"] == first.json()["epsilon_before"]
    assert datetime.fromisoformat(body["created_at"]) == datetime.fromisoformat(
        first.json()["created_at"]
    )
    assert [it["epsilon_after"] for it in body["items"]] == [
        it["epsilon_after"] for it in first.json()["items"]
    ]

    # GET returns the stored verdict without marking it a replay.
    got = await client.get(f"/datasets/{ds}/request-batches/b1")
    assert got.status_code == 200
    assert got.json()["idempotent_replay"] is False
    assert got.json()["epsilon_after"] == first.json()["epsilon_after"]

    # Only the two batch items plus the fill-up request exist.
    assert (await _budget(client, ds))["granted_requests"] == 3


@pytest.mark.asyncio
async def test_rejected_batch_replay_is_idempotent(client):
    ds = _ds("replay-rej")
    await _register(client, ds, eps=0.1, delta=1e-6)
    payload = _batch("b", [_item("a", 0.1), _item("b", 0.1, 0.0, "laplace")])
    first = await client.post(f"/datasets/{ds}/request-batches", json=payload)
    assert first.json()["allowed"] is False

    again = await client.post(f"/datasets/{ds}/request-batches", json=payload)
    assert again.status_code == 200
    assert again.json()["idempotent_replay"] is True
    assert again.json()["allowed"] is False
    assert again.json()["reason"] == first.json()["reason"]
    assert (await _budget(client, ds))["granted_requests"] == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mutate",
    [
        lambda items: [{**items[0], "epsilon": 0.11}, items[1]],
        lambda items: [{**items[0], "delta": 1e-8}, items[1]],
        lambda items: [{**items[0], "query_type": "other"}, items[1]],
        lambda items: [{**items[0], "request_id": "z"}, items[1]],
        lambda items: [items[1], items[0]],
        lambda items: items + [_item("c", 0.01)],
    ],
)
async def test_batch_replay_with_different_payload_conflicts(client, mutate):
    ds = _ds(f"replay-conf-{next(_counter)}")
    await _register(client, ds, eps=1.0, delta=1e-6)
    original = _batch("b", [_item("a", 0.1), _item("b", 0.2)])
    first = await client.post(f"/datasets/{ds}/request-batches", json=original)
    assert first.status_code == 201

    changed_items = mutate(original["items"])
    r = await client.post(
        f"/datasets/{ds}/request-batches",
        json=_batch("b", changed_items),
    )
    assert r.status_code == 409
    assert r.json()["error"] == "batch_id_conflict"
    assert r.json()["batch_id"] == "b"

    # Stored verdict untouched; no extra requests.
    got = await client.get(f"/datasets/{ds}/request-batches/b")
    assert [it["request_id"] for it in got.json()["items"]] == ["a", "b"]
    assert (await _budget(client, ds))["granted_requests"] == 2


# ------------------------------------------------------------- item conflicts --


@pytest.mark.asyncio
async def test_duplicate_request_id_inside_batch_conflicts_and_leaves_nothing(client):
    ds = _ds("dup-in-batch")
    await _register(client, ds, eps=1.0, delta=1e-6)
    r = await client.post(
        f"/datasets/{ds}/request-batches",
        json=_batch("b", [_item("x", 0.1), _item("x", 0.2)]),
    )
    assert r.status_code == 409
    body = r.json()
    assert body["error"] == "batch_item_request_id_duplicate"
    assert body["request_id"] == "x"

    # No batch record, no request records, no occupancy.
    assert (await client.get(f"/datasets/{ds}/request-batches/b")).status_code == 404
    assert (await _budget(client, ds))["granted_requests"] == 0
    assert (await client.get(f"/datasets/{ds}/requests")).json()["count"] == 0


@pytest.mark.asyncio
async def test_batch_item_id_occupied_by_direct_request_conflicts(client):
    ds = _ds("occupied-direct")
    await _register(client, ds, eps=1.0, delta=1e-6)
    direct = await client.post(
        f"/datasets/{ds}/requests", json=_item("taken", 0.1)
    )
    assert direct.status_code == 201

    r = await client.post(
        f"/datasets/{ds}/request-batches",
        json=_batch("b", [_item("taken", 0.1), _item("free", 0.1)]),
    )
    assert r.status_code == 409
    assert r.json()["error"] == "request_id_conflict"
    assert r.json()["request_id"] == "taken"
    assert (await client.get(f"/datasets/{ds}/request-batches/b")).status_code == 404
    # The other ("free") item must not have been admitted either.
    assert (await _budget(client, ds))["granted_requests"] == 1


@pytest.mark.asyncio
async def test_batch_item_id_occupied_by_rejected_direct_request_conflicts(client):
    ds = _ds("occupied-rejected")
    await _register(client, ds, eps=0.01, delta=1e-6)
    direct = await client.post(
        f"/datasets/{ds}/requests", json=_item("taken", 0.5)
    )
    assert direct.json()["allowed"] is False

    r = await client.post(
        f"/datasets/{ds}/request-batches",
        json=_batch("b", [_item("taken", 0.001, 0.0, "laplace"),
                          _item("free", 0.001, 0.0, "laplace")]),
    )
    assert r.status_code == 409
    assert r.json()["error"] == "request_id_conflict"


@pytest.mark.asyncio
async def test_batch_item_id_occupied_by_earlier_granted_batch_conflicts(client):
    ds = _ds("occupied-batch")
    await _register(client, ds, eps=1.0, delta=1e-6)
    first = await client.post(
        f"/datasets/{ds}/request-batches",
        json=_batch("b1", [_item("a", 0.1), _item("b", 0.1)]),
    )
    assert first.status_code == 201

    second = await client.post(
        f"/datasets/{ds}/request-batches",
        json=_batch("b2", [_item("b", 0.1), _item("c", 0.1)]),
    )
    assert second.status_code == 409
    assert second.json()["error"] == "request_id_conflict"
    assert second.json()["request_id"] == "b"
    assert (await client.get(f"/datasets/{ds}/request-batches/b2")).status_code == 404
    assert (await _budget(client, ds))["granted_requests"] == 2


@pytest.mark.asyncio
async def test_batch_item_id_occupied_by_confirmed_reservation_conflicts(client):
    ds = _ds("occupied-res")
    await _register(client, ds, eps=1.0, delta=1e-6)
    await client.post(
        f"/datasets/{ds}/reservations",
        json={"reservation_id": "r1", "epsilon": 0.1, "delta": 1e-7,
              "ttl_seconds": 600},
    )
    confirm = await client.post(
        f"/datasets/{ds}/reservations/r1/confirm",
        json={"request_id": "confirmed-req", "query_type": "q"},
    )
    assert confirm.status_code == 201

    r = await client.post(
        f"/datasets/{ds}/request-batches",
        json=_batch("b", [_item("confirmed-req", 0.01, 0.0, "laplace"),
                          _item("other", 0.01, 0.0, "laplace")]),
    )
    assert r.status_code == 409
    assert r.json()["error"] == "request_id_conflict"


# ------------------------------------------------------- single-item semantics --


@pytest.mark.asyncio
async def test_single_item_endpoint_semantics_preserved_around_batches(client):
    ds = _ds("single-preserved")
    await _register(client, ds, eps=1.0, delta=1e-6)
    batch = await client.post(
        f"/datasets/{ds}/request-batches",
        json=_batch("b", [_item("a", 0.1), _item("b", 0.2)]),
    )
    assert batch.status_code == 201

    # Replaying a batch-admitted request_id on the single-item endpoint returns
    # the original grant (idempotent), it does not consume budget again.
    replay = await client.post(f"/datasets/{ds}/requests", json=_item("a", 0.1))
    assert replay.status_code == 200
    assert replay.json()["idempotent_replay"] is True
    assert replay.json()["allowed"] is True
    assert (await _budget(client, ds))["granted_requests"] == 2

    # Different payload on that id is still a 409.
    conflict = await client.post(
        f"/datasets/{ds}/requests", json=_item("a", 0.12)
    )
    assert conflict.status_code == 409

    # A fresh single-item submission still works and composes on top.
    fresh = await client.post(
        f"/datasets/{ds}/requests", json=_item("c", 0.1, 0.0, "laplace")
    )
    assert fresh.status_code == 201
    assert (await _budget(client, ds))["granted_requests"] == 3


@pytest.mark.asyncio
async def test_batch_item_can_be_reversed_and_batch_verdict_is_historical(client):
    ds = _ds("batch-reverse")
    await _register(client, ds, eps=1.0, delta=1e-6)
    batch = await client.post(
        f"/datasets/{ds}/request-batches",
        json=_batch("b", [_item("a", 0.1, 0.0, "laplace"),
                          _item("b", 0.2, 0.0, "laplace")]),
    )
    assert batch.status_code == 201
    after = batch.json()["epsilon_after"]
    assert after == pytest.approx(0.3, abs=1e-12)

    rev = await client.post(
        f"/datasets/{ds}/requests/a/reversals",
        json={"reversal_id": "rev-batch-1", "reason": "mistake"},
    )
    assert rev.status_code == 201

    # Live budget reflects the freed item...
    budget = await _budget(client, ds)
    assert budget["granted_requests"] == 1
    assert budget["epsilon_consumed"] == pytest.approx(0.2, abs=1e-12)

    # ...while the stored batch verdict keeps its original composition values.
    got = await client.get(f"/datasets/{ds}/request-batches/b")
    assert got.json()["status"] == "granted"
    assert got.json()["epsilon_after"] == pytest.approx(0.3, abs=1e-12)
    assert [it["epsilon_after"] for it in got.json()["items"]][-1] == pytest.approx(
        0.3, abs=1e-12
    )


# --------------------------------------------------------- reservations mix --


@pytest.mark.asyncio
async def test_active_reservation_counts_against_batch_admission(client):
    ds = _ds("batch-res")
    await _register(client, ds, eps=0.5, delta=1e-6)
    res = await client.post(
        f"/datasets/{ds}/reservations",
        json={"reservation_id": "r1", "epsilon": 0.4, "delta": 1e-8,
              "ttl_seconds": 600},
    )
    assert res.json()["allowed"] is True

    # Two 0.2 items would compose to 0.4 on their own but not on top of the
    # 0.4 locked by the reservation.
    r = await client.post(
        f"/datasets/{ds}/request-batches",
        json=_batch("b", [_item("a", 0.2, 1e-8), _item("b", 0.2, 1e-8)]),
    )
    assert r.status_code == 201
    assert r.json()["allowed"] is False
    assert (await _budget(client, ds))["granted_requests"] == 0

    # A small batch that fits alongside the reservation is admitted.
    ok = await client.post(
        f"/datasets/{ds}/request-batches",
        json=_batch("ok", [_item("c", 0.01, 1e-8), _item("d", 0.01, 1e-8)]),
    )
    assert ok.json()["allowed"] is True
    assert (await _budget(client, ds))["granted_requests"] == 2


# --------------------------------------------------------------- validation --


@pytest.mark.asyncio
async def test_batch_size_bounds_are_validated(client):
    ds = _ds("bounds")
    await _register(client, ds)

    one = await client.post(
        f"/datasets/{ds}/request-batches",
        json=_batch("one", [_item("a", 0.01)]),
    )
    assert one.status_code == 422

    twenty_one = await client.post(
        f"/datasets/{ds}/request-batches",
        json=_batch("big", [_item(f"i{i:02d}", 0.001) for i in range(21)]),
    )
    assert twenty_one.status_code == 422

    # Exactly 2 and exactly 20 are accepted.
    two = await client.post(
        f"/datasets/{ds}/request-batches",
        json=_batch("two", [_item("a", 1e-4, 0.0, "laplace"),
                            _item("b", 1e-4, 0.0, "laplace")]),
    )
    assert two.status_code == 201
    twenty = await client.post(
        f"/datasets/{ds}/request-batches",
        json=_batch("twenty", [_item(f"j{i:02d}", 1e-6, 0.0, "laplace")
                               for i in range(20)]),
    )
    assert twenty.status_code == 201
    assert twenty.json()["item_count"] == 20


@pytest.mark.asyncio
async def test_batch_id_and_item_fields_validated(client):
    ds = _ds("fields")
    await _register(client, ds)

    bad_id = await client.post(
        f"/datasets/{ds}/request-batches",
        json=_batch("bad id!", [_item("a", 0.01), _item("b", 0.01)]),
    )
    assert bad_id.status_code == 422

    bad_eps = await client.post(
        f"/datasets/{ds}/request-batches",
        json=_batch("b1", [_item("a", 0.0), _item("b", 0.01)]),
    )
    assert bad_eps.status_code == 422

    bad_delta = await client.post(
        f"/datasets/{ds}/request-batches",
        json=_batch("b2", [_item("a", 0.01, 1.0), _item("b", 0.01)]),
    )
    assert bad_delta.status_code == 422


@pytest.mark.asyncio
async def test_batch_on_missing_dataset_404(client):
    payload = _batch("b", [_item("a", 0.1), _item("b", 0.1)])
    post = await client.post("/datasets/no-such-dataset/request-batches",
                             json=payload)
    assert post.status_code == 404
    assert post.json()["error"] == "dataset_not_found"

    get = await client.get("/datasets/no-such-dataset/request-batches/b")
    assert get.status_code == 404
    assert get.json()["error"] == "dataset_not_found"


@pytest.mark.asyncio
async def test_get_unknown_batch_404(client):
    ds = _ds("unknown")
    await _register(client, ds)
    r = await client.get(f"/datasets/{ds}/request-batches/ghost")
    assert r.status_code == 404
    assert r.json()["error"] == "batch_not_found"
    assert r.json()["batch_id"] == "ghost"


# --------------------------------------------------------------- concurrency --


@pytest.mark.asyncio
async def test_concurrent_batches_never_overshoot(client):
    ds = _ds("conc-fit")
    cap_eps, cap_delta = 1.0, 1e-6
    await _register(client, ds, eps=cap_eps, delta=cap_delta)

    # 30 batches of 2 pure-DP items of 0.04 each (0.08 per batch) race for a
    # cap of 1.0: joint all-or-nothing admission must never overshoot.
    payloads = [
        _batch(f"b{i:03d}", [
            _item(f"b{i:03d}-a", 0.04, 0.0, "laplace"),
            _item(f"b{i:03d}-b", 0.04, 0.0, "laplace"),
        ])
        for i in range(30)
    ]
    results = await asyncio.gather(*[
        client.post(f"/datasets/{ds}/request-batches", json=p)
        for p in payloads
    ])
    assert all(r.status_code in (200, 201) for r in results)
    verdicts = [r.json() for r in results]
    granted = [v for v in verdicts if v["allowed"]]

    budget = await _budget(client, ds)
    assert budget["granted_requests"] == 2 * len(granted)
    assert budget["epsilon_consumed"] == pytest.approx(
        0.08 * len(granted), abs=1e-9
    )
    assert budget["epsilon_consumed"] <= cap_eps + 1e-12

    # Every rejected batch left zero requests behind.
    listing = (await client.get(f"/datasets/{ds}/requests")).json()
    assert listing["count"] == 2 * len(granted)
    assert all(x["status"] == "granted" for x in listing["requests"])


@pytest.mark.asyncio
async def test_concurrent_duplicate_batch_ids_serialise_to_one_record(client):
    ds = _ds("conc-replay")
    await _register(client, ds, eps=5.0, delta=1e-6)
    payload = _batch("same-batch", [_item("a", 0.1), _item("b", 0.1)])
    results = await asyncio.gather(*[
        client.post(f"/datasets/{ds}/request-batches", json=payload)
        for _ in range(20)
    ])
    created = [r for r in results if r.status_code == 201]
    replays = [r for r in results if r.status_code == 200]
    assert len(created) == 1
    assert len(replays) == 19
    assert (await _budget(client, ds))["granted_requests"] == 2


@pytest.mark.asyncio
async def test_concurrent_batches_sharing_item_ids_leave_no_partial(client):
    ds = _ds("conc-shared-id")
    await _register(client, ds, eps=5.0, delta=1e-6)
    # All batches share request_id "shared"; at most one batch may be admitted.
    payloads = [
        _batch(f"b{i:02d}", [_item("shared", 0.1), _item(f"uniq{i:02d}", 0.1)])
        for i in range(15)
    ]
    results = await asyncio.gather(*[
        client.post(f"/datasets/{ds}/request-batches", json=p)
        for p in payloads
    ])
    created = [r for r in results if r.status_code == 201]
    conflicts = [r for r in results if r.status_code == 409]
    assert len(created) == 1
    assert len(conflicts) == 14
    assert all(r.json()["error"] == "request_id_conflict" for r in conflicts)
    budget = await _budget(client, ds)
    assert budget["granted_requests"] == 2
    listing = (await client.get(f"/datasets/{ds}/requests")).json()
    ids = {x["request_id"] for x in listing["requests"]}
    assert "shared" in ids
    assert sum(rid.startswith("uniq") for rid in ids) == 1


# ------------------------------------------------------------- migration v5 --


@pytest.mark.asyncio
async def test_migration_from_v4_schema_adds_batch_tables(
    tmp_path, monkeypatch
):
    old_db_path = str(tmp_path / "v4.db")

    conn = await aiosqlite.connect(old_db_path)
    try:
        for v in (1, 2, 3, 4):
            await conn.executescript(db.MIGRATIONS[v])
        await conn.execute("PRAGMA user_version = 4")
        await conn.commit()
    finally:
        await conn.close()

    patched = dataclasses.replace(settings, db_path=old_db_path)
    monkeypatch.setattr(db, "settings", patched)
    await db.init_db()

    async with db.connect() as c:
        async with c.execute("PRAGMA user_version") as cur:
            assert (await cur.fetchone())[0] == db.LATEST_VERSION
        for table in ("request_batches", "request_batch_items"):
            async with c.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                (table,),
            ) as cur:
                assert await cur.fetchone() is not None
