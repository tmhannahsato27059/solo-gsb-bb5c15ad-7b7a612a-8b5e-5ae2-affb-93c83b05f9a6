"""Regression tests: startup recovery of a half-migrated V7 database.

The pre-atomic migrator committed the V7 DDL script and the ``user_version``
bump separately, so a process kill in between could leave a database with
some V7 artefacts committed while ``user_version`` stayed at 6; the next
start re-ran the ALTERs and died on ``duplicate column name``.  These tests
build databases at every such crash point and assert that ``init_db``
completes the migration atomically, preserves existing reservations and
their original quotas, and leaves the API serving on repeated starts.
"""
from __future__ import annotations

import dataclasses
import os
import tempfile

import aiosqlite
import pytest

# Point the ledger at an isolated database before importing the app.
_tmp = tempfile.mkdtemp(prefix="dpledger-test-migration-")
os.environ["DATABASE_PATH"] = os.path.join(_tmp, "migration.db")

from httpx import ASGITransport, AsyncClient  # noqa: E402

from app import db  # noqa: E402
from app.config import settings  # noqa: E402
from app.main import app  # noqa: E402

_CREATED_AT = "2026-01-01T00:00:00+00:00"
# Far-future expiry so the recovered reservation stays active.
_EXPIRES_AT = "2027-01-01T00:00:00+00:00"

_ALTER_EPSILON = (
    "ALTER TABLE reservations ADD COLUMN original_epsilon REAL"
)
_ALTER_DELTA = "ALTER TABLE reservations ADD COLUMN original_delta REAL"
_BACKFILL = (
    "UPDATE reservations SET original_epsilon = epsilon, "
    "original_delta = delta WHERE original_epsilon IS NULL"
)
_CREATE_ADJUSTMENTS = """
CREATE TABLE IF NOT EXISTS reservation_adjustments (
    id                        INTEGER PRIMARY KEY AUTOINCREMENT,
    dataset_id                TEXT NOT NULL,
    reservation_id            TEXT NOT NULL,
    adjustment_id             TEXT NOT NULL,
    epsilon_before            REAL NOT NULL,
    delta_before              REAL NOT NULL,
    requested_epsilon         REAL NOT NULL,
    requested_delta           REAL NOT NULL,
    epsilon_after             REAL NOT NULL,
    delta_after               REAL NOT NULL,
    status                    TEXT NOT NULL CHECK (status IN (
                                  'adjusted', 'rejected'
                              )),
    reason                    TEXT,
    epsilon_cap               REAL NOT NULL,
    epsilon_committed_before  REAL NOT NULL,
    epsilon_committed_after   REAL NOT NULL,
    rdp_best_order            REAL NOT NULL,
    created_at                TEXT NOT NULL,
    UNIQUE (dataset_id, adjustment_id),
    FOREIGN KEY (dataset_id, reservation_id)
        REFERENCES reservations (dataset_id, reservation_id)
)
"""


async def _build_v6_database(
    db_path: str, crash_debris: tuple[str, ...] = ()
) -> None:
    """A v6 ledger with one dataset + one granted reservation, plus the
    V7 artefacts the crashed migrator managed to commit (``crash_debris``).
    """
    conn = await aiosqlite.connect(db_path)
    try:
        for schema in (
            db.SCHEMA_V1, db.SCHEMA_V2, db.SCHEMA_V3, db.SCHEMA_V4,
            db.SCHEMA_V5, db.SCHEMA_V6,
        ):
            await conn.executescript(schema)
        await conn.execute(
            "INSERT INTO datasets "
            "(dataset_id, epsilon_cap, delta_cap, description, created_at) "
            "VALUES ('legacy', 1.0, 1e-6, 'old', ?)",
            (_CREATED_AT,),
        )
        await conn.execute(
            "INSERT INTO reservations "
            "(dataset_id, reservation_id, epsilon, delta, ttl_seconds, "
            "status, reason, epsilon_before, epsilon_after, rdp_best_order, "
            "created_at, expires_at) VALUES "
            "('legacy', 'rv1', 0.1, 1e-7, 600, 'granted', NULL, "
            "0.0, 0.1, 1.1, ?, ?)",
            (_CREATED_AT, _EXPIRES_AT),
        )
        for statement in crash_debris:
            await conn.execute(statement)
        await conn.execute("PRAGMA user_version = 6")
        await conn.commit()
    finally:
        await conn.close()


async def _recover(db_path: str, monkeypatch) -> None:
    patched = dataclasses.replace(settings, db_path=db_path)
    monkeypatch.setattr(db, "settings", patched)
    await db.init_db()


async def _reservation_row(db_path: str) -> aiosqlite.Row:
    async with aiosqlite.connect(db_path) as conn:
        conn.row_factory = aiosqlite.Row
        async with conn.execute(
            "SELECT * FROM reservations WHERE dataset_id = 'legacy' "
            "AND reservation_id = 'rv1'"
        ) as cur:
            row = await cur.fetchone()
    assert row is not None
    return row


async def _schema_state(db_path: str) -> tuple[int, set[str], set[str]]:
    async with aiosqlite.connect(db_path) as conn:
        version = (await (await conn.execute("PRAGMA user_version")).fetchone())[0]
        cols = {
            r[1]
            for r in await (
                await conn.execute("PRAGMA table_info(reservations)")
            ).fetchall()
        }
        tables = {
            r[0]
            for r in await (
                await conn.execute(
                    "SELECT name FROM sqlite_master WHERE type = 'table'"
                )
            ).fetchall()
        }
    return version, cols, tables


def _client() -> AsyncClient:
    return AsyncClient(transport=ASGITransport(app=app), base_url="http://test")


# --------------------------------------------------------------- recovery --


@pytest.mark.asyncio
async def test_interrupted_v7_after_both_alters_recovers(
    tmp_path, monkeypatch
):
    """Crash after both ALTERs committed, before backfill/version bump."""
    db_path = str(tmp_path / "half.db")
    await _build_v6_database(db_path, (_ALTER_EPSILON, _ALTER_DELTA))

    await _recover(db_path, monkeypatch)

    version, cols, tables = await _schema_state(db_path)
    assert version == db.LATEST_VERSION
    assert {"original_epsilon", "original_delta"} <= cols
    assert "reservation_adjustments" in tables
    row = await _reservation_row(db_path)
    # Live quota untouched; original quota backfilled from it.
    assert row["epsilon"] == pytest.approx(0.1)
    assert row["delta"] == pytest.approx(1e-7)
    assert row["original_epsilon"] == pytest.approx(0.1)
    assert row["original_delta"] == pytest.approx(1e-7)
    assert row["status"] == "granted"
    assert row["expires_at"] == _EXPIRES_AT


@pytest.mark.asyncio
async def test_interrupted_v7_between_alters_recovers(tmp_path, monkeypatch):
    """Crash between the two ALTERs: only original_epsilon exists."""
    db_path = str(tmp_path / "half.db")
    await _build_v6_database(db_path, (_ALTER_EPSILON,))

    await _recover(db_path, monkeypatch)

    version, cols, tables = await _schema_state(db_path)
    assert version == db.LATEST_VERSION
    assert {"original_epsilon", "original_delta"} <= cols
    assert "reservation_adjustments" in tables
    row = await _reservation_row(db_path)
    assert row["original_epsilon"] == pytest.approx(0.1)
    assert row["original_delta"] == pytest.approx(1e-7)


@pytest.mark.asyncio
async def test_interrupted_v7_preserves_stamped_originals(
    tmp_path, monkeypatch
):
    """Crash after the backfill: already-stamped original quotas survive.

    Simulates a row whose original creation payload differs from the live
    quota (as after a quota adjustment): recovery must not rewrite it.
    """
    db_path = str(tmp_path / "half.db")
    await _build_v6_database(
        db_path, (_ALTER_EPSILON, _ALTER_DELTA, _BACKFILL)
    )
    # Diverge live vs. original quota, as a completed adjustment would.
    conn = await aiosqlite.connect(db_path)
    try:
        await conn.execute(
            "UPDATE reservations SET epsilon = 0.4, delta = 1e-8, "
            "original_epsilon = 0.1, original_delta = 1e-7 "
            "WHERE dataset_id = 'legacy' AND reservation_id = 'rv1'"
        )
        await conn.commit()
    finally:
        await conn.close()

    await _recover(db_path, monkeypatch)

    version, _, tables = await _schema_state(db_path)
    assert version == db.LATEST_VERSION
    assert "reservation_adjustments" in tables
    row = await _reservation_row(db_path)
    assert row["epsilon"] == pytest.approx(0.4)
    assert row["delta"] == pytest.approx(1e-8)
    assert row["original_epsilon"] == pytest.approx(0.1)
    assert row["original_delta"] == pytest.approx(1e-7)


@pytest.mark.asyncio
async def test_interrupted_v7_with_all_artefacts_recovers(
    tmp_path, monkeypatch
):
    """Crash with every V7 artefact committed but the version bump lost."""
    db_path = str(tmp_path / "half.db")
    await _build_v6_database(
        db_path,
        (_ALTER_EPSILON, _ALTER_DELTA, _BACKFILL, _CREATE_ADJUSTMENTS),
    )

    await _recover(db_path, monkeypatch)

    version, _, tables = await _schema_state(db_path)
    assert version == db.LATEST_VERSION
    assert "reservation_adjustments" in tables
    row = await _reservation_row(db_path)
    assert row["original_epsilon"] == pytest.approx(0.1)
    assert row["original_delta"] == pytest.approx(1e-7)


@pytest.mark.asyncio
async def test_repeated_starts_after_recovery_serve_api(
    tmp_path, monkeypatch
):
    """Repeated startups are idempotent and the API answers normally."""
    db_path = str(tmp_path / "half.db")
    await _build_v6_database(db_path, (_ALTER_EPSILON, _ALTER_DELTA))

    await _recover(db_path, monkeypatch)
    # Second and third startups must be no-ops, not errors.
    await db.init_db()
    await db.init_db()

    async with _client() as client:
        health = await client.get("/health")
        assert health.status_code == 200

        listing = await client.get("/datasets/legacy/reservations")
        assert listing.status_code == 200
        reservations = listing.json()["reservations"]
        assert [r["reservation_id"] for r in reservations] == ["rv1"]
        assert reservations[0]["epsilon"] == pytest.approx(0.1)
        assert reservations[0]["status"] == "granted"

        # The creation payload replays idempotently against the recovered
        # original quota (not against any later live value).
        replay = await client.post(
            "/datasets/legacy/reservations",
            json={
                "reservation_id": "rv1",
                "epsilon": 0.1,
                "delta": 1e-7,
                "ttl_seconds": 600,
            },
        )
        assert replay.status_code == 200
        assert replay.json()["idempotent_replay"] is True

        # A different payload for the same id is still a conflict.
        conflict = await client.post(
            "/datasets/legacy/reservations",
            json={
                "reservation_id": "rv1",
                "epsilon": 0.2,
                "delta": 1e-7,
                "ttl_seconds": 600,
            },
        )
        assert conflict.status_code == 409
        assert conflict.json()["error"] == "reservation_id_conflict"

        # Budget accounting includes the recovered reservation.
        budget = await client.get("/datasets/legacy/budget")
        assert budget.status_code == 200
        assert budget.json()["active_reservations"] == 1


@pytest.mark.asyncio
async def test_pristine_v6_database_migrates_atomically(
    tmp_path, monkeypatch
):
    """A v6 database without any V7 debris migrates in one pass."""
    db_path = str(tmp_path / "pristine.db")
    await _build_v6_database(db_path)

    await _recover(db_path, monkeypatch)

    version, cols, tables = await _schema_state(db_path)
    assert version == db.LATEST_VERSION
    assert {"original_epsilon", "original_delta"} <= cols
    assert "reservation_adjustments" in tables
    row = await _reservation_row(db_path)
    assert row["original_epsilon"] == pytest.approx(0.1)
    assert row["original_delta"] == pytest.approx(1e-7)
