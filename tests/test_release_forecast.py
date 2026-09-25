"""End-to-end tests for the read-only reservation release forecast."""
from __future__ import annotations

import os
import tempfile
from datetime import datetime, timedelta, timezone

import pytest
import pytest_asyncio

# Point the ledger at an isolated database before importing the app.
_tmp = tempfile.mkdtemp(prefix="dpledger-test-forecast-")
os.environ["DATABASE_PATH"] = os.path.join(_tmp, "forecast.db")

from httpx import ASGITransport, AsyncClient  # noqa: E402

from app import db, rdp  # noqa: E402
from app.main import app  # noqa: E402

import itertools

_counter = itertools.count()


def _new_dataset_id(name: str) -> str:
    return f"dsfc-{name}-{next(_counter)}"


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
         delta: float = 1e-7, ttl: int = 3600) -> dict:
    return {
        "reservation_id": reservation_id,
        "epsilon": epsilon,
        "delta": delta,
        "ttl_seconds": ttl,
    }


async def _backdate(dataset_id: str, reservation_id: str,
                    expires_at: datetime) -> None:
    """Force a reservation's expiry instant without going through an endpoint."""
    async with db.connect() as conn:
        await db.backdate_reservation_expiry(
            conn,
            dataset_id=dataset_id,
            reservation_id=reservation_id,
            expires_at=expires_at.isoformat(),
        )
        await conn.commit()


async def _snapshot_eps(
    granted: list[tuple[float, float]],
    reserved: list[tuple[float, float]],
    cap_delta: float,
) -> tuple[float, float, float]:
    """Independent recomputation of a snapshot's epsilon fields via rdp.py."""
    _, consumed, _ = rdp.evaluate_entries(granted, cap_delta)
    _, committed, _ = rdp.evaluate_entries(granted + reserved, cap_delta)
    consumed, committed = max(0.0, consumed), max(0.0, committed)
    return consumed, max(0.0, committed - consumed), committed


# ----------------------------------------------------------------- behaviour --


@pytest.mark.asyncio
async def test_forecast_empty_dataset(client):
    ds = _new_dataset_id("empty")
    await _register(client, ds, eps=1.0, delta=1e-6)
    deadline = datetime.now(timezone.utc) + timedelta(hours=1)

    r = await client.get(
        f"/datasets/{ds}/budget/release-forecast",
        params={"deadline": deadline.isoformat()},
    )
    assert r.status_code == 200
    body = r.json()
    assert body["dataset_id"] == ds
    assert body["events"] == []
    for snap in (body["initial"], body["at_deadline"]):
        assert snap == {
            "epsilon_consumed": 0.0,
            "epsilon_reserved": 0.0,
            "epsilon_remaining": pytest.approx(1.0),
            "rdp_best_order": 8192.0,
            "active_reservations": 0,
        }
    assert datetime.fromisoformat(body["deadline"]) == deadline


@pytest.mark.asyncio
async def test_forecast_groups_same_instant_expiries_and_recomputes(client):
    ds = _new_dataset_id("groups")
    cap_eps, cap_delta = 1.0, 1e-6
    await _register(client, ds, eps=cap_eps, delta=cap_delta)

    now = datetime.now(timezone.utc)
    t1 = now + timedelta(minutes=10)
    t2 = now + timedelta(minutes=30)

    # Two reservations share t1; one expires at t2; one outlives the deadline.
    setup = [
        ("r-a", 0.1, 1e-8, t1),
        ("r-b", 0.2, 0.0, t1),
        ("r-c", 0.15, 1e-8, t2),
        ("r-d", 0.05, 0.0, now + timedelta(hours=2)),
    ]
    for rid, eps, delta, exp in setup:
        created = await client.post(
            f"/datasets/{ds}/reservations", json=_res(rid, eps, delta, 600)
        )
        assert created.status_code == 201
        await _backdate(ds, rid, exp)

    # One directly granted request stays consumed throughout.
    direct = await client.post(
        f"/datasets/{ds}/requests",
        json={"request_id": "q1", "query_type": "q",
              "epsilon": 0.1, "delta": 1e-9},
    )
    assert direct.json()["allowed"] is True

    deadline = now + timedelta(minutes=45)
    r = await client.get(
        f"/datasets/{ds}/budget/release-forecast",
        params={"deadline": deadline.isoformat()},
    )
    assert r.status_code == 200
    body = r.json()

    # Initial snapshot reflects all four live reservations.
    assert body["initial"]["active_reservations"] == 4
    budget = (await client.get(f"/datasets/{ds}/budget")).json()
    assert body["initial"]["epsilon_consumed"] == pytest.approx(
        budget["epsilon_consumed"], abs=1e-12
    )
    assert body["initial"]["epsilon_reserved"] == pytest.approx(
        budget["epsilon_reserved"], abs=1e-12
    )
    assert body["initial"]["epsilon_remaining"] == pytest.approx(
        budget["epsilon_remaining"], abs=1e-12
    )

    # Two distinct expiry instants; same-instant items leave together.
    assert [datetime.fromisoformat(e["expires_at"]) for e in body["events"]] == (
        [t1, t2]
    )
    first, second = body["events"]
    assert first["released_count"] == 2
    assert sorted(first["released_reservation_ids"]) == ["r-a", "r-b"]
    assert first["active_reservations"] == 2
    assert second["released_count"] == 1
    assert second["released_reservation_ids"] == ["r-c"]
    assert second["active_reservations"] == 1

    # Budget never shrinks as reservations expire.
    seen = [body["initial"]] + body["events"] + [body["at_deadline"]]
    for earlier, later in zip(seen, seen[1:]):
        assert later["epsilon_remaining"] >= earlier["epsilon_remaining"] - 1e-12
        assert later["epsilon_reserved"] <= earlier["epsilon_reserved"] + 1e-12
        assert later["epsilon_consumed"] == pytest.approx(
            earlier["epsilon_consumed"], abs=1e-12
        )

    # Cross-check every snapshot against an independent RDP recomputation.
    granted = [(0.1, 1e-9)]
    by_id = {rid: (eps, delta) for rid, eps, delta, _ in setup}
    still = [by_id["r-a"], by_id["r-b"], by_id["r-c"], by_id["r-d"]]
    expected_initial = await _snapshot_eps(granted, still, cap_delta)
    assert body["initial"]["epsilon_consumed"] == pytest.approx(
        expected_initial[0], abs=1e-12
    )
    assert body["initial"]["epsilon_reserved"] == pytest.approx(
        expected_initial[1], abs=1e-12
    )

    after_first = [by_id["r-c"], by_id["r-d"]]
    consumed, reserved, committed = await _snapshot_eps(
        granted, after_first, cap_delta
    )
    assert first["epsilon_consumed"] == pytest.approx(consumed, abs=1e-12)
    assert first["epsilon_reserved"] == pytest.approx(reserved, abs=1e-12)
    assert first["epsilon_remaining"] == pytest.approx(
        max(0.0, cap_eps - committed), abs=1e-12
    )

    after_second = [by_id["r-d"]]
    consumed, reserved, committed = await _snapshot_eps(
        granted, after_second, cap_delta
    )
    assert second["epsilon_remaining"] == pytest.approx(
        max(0.0, cap_eps - committed), abs=1e-12
    )

    # At the deadline r-d still holds budget; consumed by the grant remains.
    assert body["at_deadline"]["active_reservations"] == 1
    assert body["at_deadline"]["epsilon_consumed"] == pytest.approx(
        consumed, abs=1e-12
    )
    _, reserved_final, committed_final = await _snapshot_eps(
        granted, after_second, cap_delta
    )
    assert body["at_deadline"]["epsilon_reserved"] == pytest.approx(
        reserved_final, abs=1e-12
    )
    assert body["at_deadline"]["epsilon_remaining"] == pytest.approx(
        max(0.0, cap_eps - committed_final), abs=1e-12
    )
    assert second["epsilon_remaining"] == pytest.approx(
        body["at_deadline"]["epsilon_remaining"], abs=1e-12
    )


@pytest.mark.asyncio
async def test_forecast_items_expiring_exactly_at_deadline_are_removed(client):
    ds = _new_dataset_id("boundary")
    await _register(client, ds, eps=1.0, delta=1e-6)
    boundary = datetime.now(timezone.utc) + timedelta(minutes=5)

    created = await client.post(
        f"/datasets/{ds}/reservations", json=_res("edge", epsilon=0.1)
    )
    await _backdate(ds, "edge", boundary)

    r = await client.get(
        f"/datasets/{ds}/budget/release-forecast",
        params={"deadline": boundary.isoformat()},
    )
    assert r.status_code == 200
    body = r.json()
    assert body["initial"]["active_reservations"] == 1
    assert len(body["events"]) == 1
    assert datetime.fromisoformat(body["events"][0]["expires_at"]) == boundary
    assert body["events"][0]["released_reservation_ids"] == ["edge"]
    assert body["at_deadline"]["active_reservations"] == 0
    assert body["at_deadline"]["epsilon_remaining"] == pytest.approx(1.0)


@pytest.mark.asyncio
async def test_forecast_horizon_before_any_expiry_changes_nothing(client):
    ds = _new_dataset_id("early")
    await _register(client, ds, eps=1.0, delta=1e-6)
    created = await client.post(
        f"/datasets/{ds}/reservations",
        json=_res("rv", epsilon=0.2, ttl=3600),
    )
    assert created.json()["allowed"] is True

    soon = datetime.now(timezone.utc) + timedelta(seconds=30)
    r = await client.get(
        f"/datasets/{ds}/budget/release-forecast",
        params={"deadline": soon.isoformat()},
    )
    assert r.status_code == 200
    body = r.json()
    assert body["events"] == []
    assert body["at_deadline"] == body["initial"]
    assert body["at_deadline"]["active_reservations"] == 1


@pytest.mark.asyncio
async def test_forecast_is_read_only_and_does_not_sweep_expired(client):
    ds = _new_dataset_id("readonly")
    await _register(client, ds, eps=0.5, delta=1e-6)
    created = await client.post(
        f"/datasets/{ds}/reservations", json=_res("rv", epsilon=0.25, ttl=10)
    )
    assert created.json()["allowed"] is True
    past = datetime.now(timezone.utc) - timedelta(seconds=1)
    await _backdate(ds, "rv", past)

    deadline = datetime.now(timezone.utc) + timedelta(hours=1)
    r = await client.get(
        f"/datasets/{ds}/budget/release-forecast",
        params={"deadline": deadline.isoformat()},
    )
    assert r.status_code == 200

    # The forecast ignored the already-expired row and changed no state:
    # the reservation is still stored as "granted"; the first normal read is
    # what performs the lazy sweep afterwards.
    async with db.connect() as conn:
        row = await db.get_reservation(conn, ds, "rv")
        assert row["status"] == "granted"
        assert row["expired_at"] is None
        active_before_sweep = await db.list_granted_reservation_rows(
            conn, ds, now_iso=datetime.now(timezone.utc).isoformat()
        )
        assert active_before_sweep == []

    got = await client.get(f"/datasets/{ds}/reservations/rv")
    assert got.json()["status"] == "expired"
    assert got.json()["expired_at"] is not None


@pytest.mark.asyncio
async def test_forecast_repeated_calls_are_identical(client):
    ds = _new_dataset_id("stable")
    await _register(client, ds, eps=1.0, delta=1e-6)
    await client.post(
        f"/datasets/{ds}/reservations", json=_res("rv", epsilon=0.2, ttl=3600)
    )
    deadline = datetime.now(timezone.utc) + timedelta(hours=2)
    params = {"deadline": deadline.isoformat()}

    first = (await client.get(
        f"/datasets/{ds}/budget/release-forecast", params=params
    )).json()
    second = (await client.get(
        f"/datasets/{ds}/budget/release-forecast", params=params
    )).json()
    # generated_at may move, the forecast content must not.
    for key in ("initial", "events", "at_deadline", "deadline"):
        assert first[key] == second[key]

    listing = (await client.get(f"/datasets/{ds}/reservations")).json()
    assert [r["status"] for r in listing["reservations"]] == ["granted"]


# ---------------------------------------------------------------- validation --


@pytest.mark.asyncio
async def test_forecast_missing_dataset_404(client):
    deadline = datetime.now(timezone.utc) + timedelta(hours=1)
    r = await client.get(
        "/datasets/no-such-dataset/budget/release-forecast",
        params={"deadline": deadline.isoformat()},
    )
    assert r.status_code == 404
    assert r.json()["error"] == "dataset_not_found"


@pytest.mark.asyncio
async def test_forecast_requires_deadline(client):
    ds = _new_dataset_id("nodeadline")
    await _register(client, ds)
    r = await client.get(f"/datasets/{ds}/budget/release-forecast")
    assert r.status_code == 422


@pytest.mark.asyncio
async def test_forecast_rejects_unparseable_deadline(client):
    ds = _new_dataset_id("baddate")
    await _register(client, ds)
    r = await client.get(
        f"/datasets/{ds}/budget/release-forecast",
        params={"deadline": "next-tuesday"},
    )
    assert r.status_code == 422


@pytest.mark.asyncio
async def test_forecast_rejects_naive_deadline(client):
    ds = _new_dataset_id("naive")
    await _register(client, ds)
    naive = (datetime.now(timezone.utc) + timedelta(hours=1)).replace(tzinfo=None)
    r = await client.get(
        f"/datasets/{ds}/budget/release-forecast",
        params={"deadline": naive.isoformat()},
    )
    assert r.status_code == 422
    assert r.json()["error"] == "invalid_forecast_deadline"
    assert "UTC" in r.json()["detail"]


@pytest.mark.asyncio
async def test_forecast_rejects_past_and_equal_instant(client):
    ds = _new_dataset_id("past")
    await _register(client, ds)

    past = datetime.now(timezone.utc) - timedelta(seconds=1)
    r = await client.get(
        f"/datasets/{ds}/budget/release-forecast",
        params={"deadline": past.isoformat()},
    )
    assert r.status_code == 422
    assert r.json()["error"] == "invalid_forecast_deadline"

    # An instant equal to "now" is not in the future either.
    r2 = await client.get(
        f"/datasets/{ds}/budget/release-forecast",
        params={"deadline": datetime.now(timezone.utc).isoformat()},
    )
    assert r2.status_code == 422


@pytest.mark.asyncio
async def test_forecast_accepts_offset_aware_non_utc_deadline(client):
    ds = _new_dataset_id("offset")
    await _register(client, ds)
    await client.post(
        f"/datasets/{ds}/reservations", json=_res("rv", epsilon=0.1)
    )
    # +02:00 offset must be normalised to UTC; ~30 min in the future there.
    future_local = (
        datetime.now(timezone.utc) + timedelta(minutes=30)
    ).astimezone(timezone(timedelta(hours=2)))
    r = await client.get(
        f"/datasets/{ds}/budget/release-forecast",
        params={"deadline": future_local.isoformat()},
    )
    assert r.status_code == 200
    parsed_deadline = datetime.fromisoformat(r.json()["deadline"])
    assert parsed_deadline.utcoffset() == timedelta(0)
    assert parsed_deadline == future_local


@pytest.mark.asyncio
async def test_forecast_rejected_and_terminal_reservations_are_ignored(client):
    ds = _new_dataset_id("terminal")
    await _register(client, ds, eps=0.3, delta=1e-6)

    # Over-budget rejected reservation never enters the simulation.
    rejected = await client.post(
        f"/datasets/{ds}/reservations", json=_res("nope", epsilon=5.0)
    )
    assert rejected.json()["allowed"] is False

    # A released reservation is not currently valid either.
    await client.post(
        f"/datasets/{ds}/reservations", json=_res("gone", epsilon=0.1)
    )
    await client.post(f"/datasets/{ds}/reservations/gone/release")

    deadline = datetime.now(timezone.utc) + timedelta(hours=1)
    body = (await client.get(
        f"/datasets/{ds}/budget/release-forecast",
        params={"deadline": deadline.isoformat()},
    )).json()
    assert body["initial"]["active_reservations"] == 0
    assert body["events"] == []
    assert body["at_deadline"]["epsilon_remaining"] == pytest.approx(0.3)
