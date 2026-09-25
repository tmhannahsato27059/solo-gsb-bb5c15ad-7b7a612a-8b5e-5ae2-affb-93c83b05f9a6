"""Tests for the ``python -m app.backup`` online-backup command."""
from __future__ import annotations

import os
import sqlite3
import subprocess
import sys
import threading

import pytest

from app import backup
from app import db as app_db
from app.config import Settings

_CREATED_AT = "2026-09-25T00:00:00+00:00"


@pytest.fixture()
def ledger_db(tmp_path, monkeypatch):
    """A real ledger database (latest schema, WAL mode) under a /data-like dir."""
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    db_path = data_dir / "ledger.db"
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode = WAL")
    for version in sorted(app_db.MIGRATIONS):
        conn.executescript(app_db.MIGRATIONS[version])
        # Same constant-internal literal as app.db._migrate.
        conn.execute(f"PRAGMA user_version = {version}")
    conn.commit()
    conn.close()
    monkeypatch.setattr(backup, "settings", Settings(db_path=str(db_path)))
    return db_path


def _insert_pair(conn, dataset_id, request_id):
    """One dataset + one granted request in a single transaction."""
    conn.execute("BEGIN IMMEDIATE")
    conn.execute(
        "INSERT INTO datasets "
        "(dataset_id, epsilon_cap, delta_cap, description, created_at) "
        "VALUES (?, 1.0, 1e-6, '', ?)",
        (dataset_id, _CREATED_AT),
    )
    conn.execute(
        "INSERT INTO requests "
        "(dataset_id, request_id, query_type, epsilon, delta, status, reason, "
        "epsilon_before, epsilon_after, rdp_best_order, created_at) "
        "VALUES (?, ?, 'gaussian_mean', 0.1, 1e-8, 'granted', NULL, "
        "0.0, 0.1, 8192.0, ?)",
        (dataset_id, request_id, _CREATED_AT),
    )
    conn.commit()


def _add_pair(db_path, dataset_id, request_id):
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA busy_timeout = 10000")
    _insert_pair(conn, dataset_id, request_id)
    conn.close()


def _count(conn, table):
    return conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]


def _leftover_temps(directory):
    return [p.name for p in directory.iterdir() if p.name.startswith(".ledger-backup-")]


# ----------------------------------------------------------------- snapshot --


def test_snapshot_matches_source(ledger_db):
    _add_pair(ledger_db, "ds-a", "req-a")
    out = ledger_db.parent / "backups" / "snap.db"  # nested dir auto-created
    published = backup.run(str(out))
    assert published == str(out)
    assert os.path.isfile(out)

    src = sqlite3.connect(ledger_db)
    snap = sqlite3.connect(out)
    assert _count(snap, "datasets") == _count(src, "datasets") == 1
    assert _count(snap, "requests") == _count(src, "requests") == 1
    # Schema version and all tables carried over.
    assert snap.execute("PRAGMA user_version").fetchone()[0] == (
        src.execute("PRAGMA user_version").fetchone()[0]
    )
    assert _count(snap, "reservations") == _count(src, "reservations")
    assert _count(snap, "budget_transfers") == _count(src, "budget_transfers")
    # The published file passes the same integrity check the command ran.
    assert snap.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
    src.close()
    snap.close()


def test_uncommitted_transaction_never_copied(ledger_db):
    _add_pair(ledger_db, "ds-committed", "req-committed")

    # A writer holds BEGIN IMMEDIATE with an uncommitted row.  The online
    # backup must still succeed (WAL readers never block) and must contain
    # only the committed state — never a half transaction.
    writer = sqlite3.connect(ledger_db)
    writer.execute("PRAGMA busy_timeout = 10000")
    writer.execute("BEGIN IMMEDIATE")
    writer.execute(
        "INSERT INTO datasets "
        "(dataset_id, epsilon_cap, delta_cap, description, created_at) "
        "VALUES ('ds-pending', 1.0, 1e-6, '', ?)",
        (_CREATED_AT,),
    )
    out = ledger_db.parent / "backups" / "during.db"
    backup.run(str(out))
    snap = sqlite3.connect(out)
    ids = {r[0] for r in snap.execute("SELECT dataset_id FROM datasets")}
    assert ids == {"ds-committed"}
    snap.close()

    # After the commit, a new snapshot does see the row.
    writer.commit()
    writer.close()
    out2 = ledger_db.parent / "backups" / "after.db"
    backup.run(str(out2))
    snap2 = sqlite3.connect(out2)
    ids2 = {r[0] for r in snap2.execute("SELECT dataset_id FROM datasets")}
    assert ids2 == {"ds-committed", "ds-pending"}
    snap2.close()


def test_concurrent_commits_stay_transactionally_consistent(ledger_db):
    # Preload enough rows that the copy overlaps with concurrent commits.
    conn = sqlite3.connect(ledger_db)
    conn.execute("PRAGMA busy_timeout = 10000")
    for i in range(500):
        _insert_pair(conn, f"ds-pre-{i}", f"req-pre-{i}")
    conn.close()

    stop = threading.Event()
    errors = []

    def writer():
        n = 0
        w = sqlite3.connect(ledger_db)
        w.execute("PRAGMA busy_timeout = 10000")
        while not stop.is_set():
            try:
                _insert_pair(w, f"ds-live-{n}", f"req-live-{n}")
                n += 1
            except sqlite3.Error as exc:  # pragma: no cover - defensive
                errors.append(exc)
                break
        w.close()

    thread = threading.Thread(target=writer)
    thread.start()
    try:
        out = ledger_db.parent / "backups" / "live.db"
        backup.run(str(out))
    finally:
        stop.set()
        thread.join(timeout=30)
    assert not errors

    # Every committed transaction wrote exactly one dataset + one request,
    # so a torn copy would break this equality.
    snap = sqlite3.connect(out)
    assert _count(snap, "datasets") == _count(snap, "requests")
    assert snap.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
    snap.close()


# ------------------------------------------------------------------ publish --


def test_existing_output_is_never_overwritten(ledger_db):
    out = ledger_db.parent / "backups" / "snap.db"
    out.parent.mkdir()
    out.write_bytes(b"previous backup")
    with pytest.raises(backup.BackupError, match="already exists"):
        backup.run(str(out))
    assert out.read_bytes() == b"previous backup"
    assert _leftover_temps(out.parent) == []


def test_output_directory_is_created(ledger_db):
    out = ledger_db.parent / "backups" / "2026" / "09" / "snap.db"
    backup.run(str(out))
    assert os.path.isfile(out)


def test_no_sidecar_files_left_behind(ledger_db):
    # The source is in WAL mode; the published artifact must be a plain
    # self-contained file with no -wal/-shm/-journal sidecars next to it.
    out = ledger_db.parent / "backups" / "snap.db"
    backup.run(str(out))
    assert sorted(p.name for p in out.parent.iterdir()) == ["snap.db"]


def test_output_path_that_is_a_directory(ledger_db):
    with pytest.raises(backup.BackupError, match="is a directory"):
        backup.run(str(ledger_db.parent))


def test_missing_source_database(tmp_path, monkeypatch):
    monkeypatch.setattr(
        backup, "settings", Settings(db_path=str(tmp_path / "nope.db"))
    )
    with pytest.raises(backup.BackupError, match="not found"):
        backup.run(str(tmp_path / "out" / "snap.db"))
    # The output directory is not even created when the source is missing.
    assert not (tmp_path / "out").exists()


def test_failed_check_cleans_temp_and_leaves_no_output(ledger_db, monkeypatch):
    out = ledger_db.parent / "backups" / "snap.db"

    def boom(_tmp_path):
        raise backup.BackupError("integrity_check failed on the snapshot: forced")

    monkeypatch.setattr(backup, "_verify", boom)
    with pytest.raises(backup.BackupError, match="forced"):
        backup.run(str(out))
    assert not out.exists()
    assert _leftover_temps(out.parent) == []


def test_publish_never_clobbers(tmp_path):
    # The race the early existence check cannot cover: the destination
    # appears between check and publish -> os.link must refuse atomically.
    tmp = tmp_path / ".ledger-backup-x.tmp"
    tmp.write_bytes(b"new snapshot")
    out = tmp_path / "snap.db"
    out.write_bytes(b"previous backup")
    with pytest.raises(backup.BackupError, match="already exists"):
        backup._publish(str(tmp), str(out))
    assert out.read_bytes() == b"previous backup"

    # Success path: atomic link + temp removed.
    backup._publish(str(tmp), str(tmp_path / "ok.db"))
    assert (tmp_path / "ok.db").read_bytes() == b"new snapshot"
    assert not tmp.exists()


# ----------------------------------------------------------------------- cli --


def test_main_success(ledger_db, capsys):
    _add_pair(ledger_db, "ds-cli", "req-cli")
    out = ledger_db.parent / "backups" / "cli.db"
    rc = backup.main(["--output", str(out)])
    assert rc == 0
    assert os.path.isfile(out)
    captured = capsys.readouterr()
    assert str(out) in captured.out
    assert captured.err == ""  # inside the db dir: no persistence warning


def test_main_existing_output_returns_1(ledger_db, capsys):
    out = ledger_db.parent / "backups" / "snap.db"
    out.parent.mkdir()
    out.write_bytes(b"old")
    assert backup.main(["--output", str(out)]) == 1
    assert out.read_bytes() == b"old"
    assert "already exists" in capsys.readouterr().err


def test_main_missing_source_returns_1(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(
        backup, "settings", Settings(db_path=str(tmp_path / "nope.db"))
    )
    rc = backup.main(["--output", str(tmp_path / "snap.db")])
    assert rc == 1
    assert "not found" in capsys.readouterr().err


def test_main_warns_when_output_leaves_persistent_dir(ledger_db, tmp_path, capsys):
    out = tmp_path / "elsewhere" / "snap.db"  # outside the db's /data-like dir
    rc = backup.main(["--output", str(out)])
    assert rc == 0
    assert os.path.isfile(out)
    assert "warning" in capsys.readouterr().err


def test_module_invocation(ledger_db):
    """``python -m app.backup --output PATH`` works as a real subprocess."""
    out = ledger_db.parent / "backups" / "module.db"
    repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    env = dict(os.environ, DATABASE_PATH=str(ledger_db))
    proc = subprocess.run(
        [sys.executable, "-m", "app.backup", "--output", str(out)],
        capture_output=True,
        text=True,
        cwd=repo_root,
        env=env,
    )
    assert proc.returncode == 0, proc.stderr
    assert os.path.isfile(out)
    assert str(out) in proc.stdout

    # A second run against the same PATH fails without touching the backup.
    before = out.read_bytes()
    proc2 = subprocess.run(
        [sys.executable, "-m", "app.backup", "--output", str(out)],
        capture_output=True,
        text=True,
        cwd=repo_root,
        env=env,
    )
    assert proc2.returncode == 1
    assert "already exists" in proc2.stderr
    assert out.read_bytes() == before
