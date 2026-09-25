"""Consistent online backup of the ledger SQLite database.

Run inside the container (or anywhere ``DATABASE_PATH`` is reachable)::

    python -m app.backup --output /data/backups/ledger-20260925T120000.db

Consistency
-----------
The snapshot is produced with SQLite's **online backup API**
(``sqlite3.Connection.backup``).  While the copy runs, the service may keep
admitting requests, reservations and budget transfers: pages dirtied by a
concurrent commit are re-copied, so the destination always ends up holding a
*transactionally consistent* snapshot — a half-committed transaction can
never appear in the backup.  The source is only read, never locked against
writers (WAL mode) and never modified.

Publish protocol
----------------
1. The snapshot is streamed into a temporary file in the **same directory**
   as the requested output path, then normalised to the rollback journal so
   the artifact is a plain self-contained file (no ``-wal``/``-shm``
   sidecars can ever appear next to the published backup).
2. The copy is verified with ``PRAGMA integrity_check`` and fsynced.
3. Only then is it published to the output path with an atomic hard-link
   rename that **never overwrites** an existing backup.

Any failure removes the temporary file, leaves a pre-existing file at the
output path untouched, and exits non-zero with the reason on stderr.
"""
from __future__ import annotations

import argparse
import os
import sqlite3
import sys
import tempfile

from .config import settings


class BackupError(Exception):
    """Fatal backup problem; the message is shown on stderr (exit code 1)."""


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="python -m app.backup",
        description=(
            "Write a transactionally consistent snapshot of the ledger "
            "database (DATABASE_PATH) to PATH using SQLite's online backup "
            "API.  PATH must not exist yet: an existing backup is never "
            "overwritten."
        ),
    )
    parser.add_argument(
        "--output", "-o",
        required=True,
        metavar="PATH",
        help="destination file for the backup (keep it under /data so it "
             "survives container rebuilds)",
    )
    return parser.parse_args(argv)


def _open_source(path: str) -> sqlite3.Connection:
    """Open the live database, preferring a read-only handle.

    The backup API only ever *reads* the source; the read-write fallback
    covers the rare case where WAL recovery would need to create the
    ``-shm`` file in a directory this process may not write to.
    """
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    try:
        # sqlite3 connects lazily: force the open so an unreadable source is
        # reported here instead of halfway through the copy.
        conn.execute("SELECT count(*) FROM sqlite_master").fetchone()
        return conn
    except sqlite3.Error:
        conn.close()
        # mode=rw (not the default) so a missing file is never created.
        conn = sqlite3.connect(f"file:{path}?mode=rw", uri=True)
        conn.execute("SELECT count(*) FROM sqlite_master").fetchone()
        return conn


def _copy_snapshot(source_path: str, tmp_path: str) -> None:
    """Stream a consistent snapshot of ``source_path`` into ``tmp_path``."""
    try:
        source = _open_source(source_path)
    except sqlite3.Error as exc:
        raise BackupError(f"cannot open source database {source_path}: {exc}") from exc
    try:
        source.execute(f"PRAGMA busy_timeout = {int(settings.busy_timeout_ms)}")
        dest = sqlite3.connect(tmp_path)
        try:
            # Online backup API: pages dirtied by concurrent commits are
            # re-copied, so the destination is transactionally consistent.
            source.backup(dest)
        finally:
            dest.close()
        # The copy inherits the source's WAL-mode header bytes, which would
        # make every later open spawn -wal/-shm sidecars next to the backup.
        # Normalise the artifact to the rollback journal: a plain
        # self-contained file.  The service re-enables WAL itself when the
        # backup is restored as DATABASE_PATH.
        conn = sqlite3.connect(tmp_path)
        try:
            mode = conn.execute("PRAGMA journal_mode=DELETE").fetchone()[0]
        finally:
            conn.close()
        if str(mode).lower() != "delete":
            raise BackupError(
                f"could not normalise journal mode of the snapshot (got {mode!r})"
            )
    except sqlite3.Error as exc:
        raise BackupError(f"online backup of {source_path} failed: {exc}") from exc
    finally:
        source.close()


def _verify(tmp_path: str) -> None:
    """Run ``PRAGMA integrity_check`` on the copied snapshot."""
    conn = sqlite3.connect(f"file:{tmp_path}?mode=ro", uri=True)
    try:
        rows = conn.execute("PRAGMA integrity_check").fetchall()
    except sqlite3.Error as exc:
        raise BackupError(f"integrity check could not run: {exc}") from exc
    finally:
        conn.close()
    problems = [row[0] for row in rows if row[0] != "ok"]
    if problems:
        detail = "; ".join(problems[:5])
        raise BackupError(f"integrity_check failed on the snapshot: {detail}")


def _fsync_file(path: str) -> None:
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    except OSError as exc:
        raise BackupError(f"cannot fsync {path}: {exc}") from exc
    finally:
        os.close(fd)


def _publish(tmp_path: str, output_path: str) -> None:
    """Atomically publish the verified snapshot, never clobbering.

    ``os.link`` creates the destination directory entry atomically and fails
    with ``FileExistsError`` if it already exists, so a pre-existing backup
    can never be replaced — not even by a crash between check and rename.
    """
    try:
        os.link(tmp_path, output_path)
    except FileExistsError as exc:
        raise BackupError(
            f"output path already exists, refusing to overwrite: {output_path}"
        ) from exc
    except OSError as exc:
        raise BackupError(
            f"cannot publish backup to {output_path}: {exc}"
        ) from exc
    os.unlink(tmp_path)
    # Best-effort directory fsync so the new directory entry is durable.
    try:
        dir_fd = os.open(os.path.dirname(output_path), os.O_RDONLY)
        try:
            os.fsync(dir_fd)
        finally:
            os.close(dir_fd)
    except OSError:
        pass


def _remove_quietly(*paths: str) -> None:
    for path in paths:
        try:
            os.unlink(path)
        except OSError:
            pass


def run(output: str) -> str:
    """Create the backup and return the published absolute path."""
    source_path = settings.db_path
    if not os.path.isfile(source_path):
        raise BackupError(
            f"source database not found: {source_path} "
            "(is DATABASE_PATH set correctly?)"
        )
    output_path = os.path.abspath(output)
    output_dir = os.path.dirname(output_path)
    if os.path.isdir(output_path):
        raise BackupError(f"output path is a directory, not a file: {output_path}")
    if os.path.lexists(output_path):
        raise BackupError(
            f"output path already exists, refusing to overwrite: {output_path}"
        )
    try:
        os.makedirs(output_dir, exist_ok=True)
    except OSError as exc:
        raise BackupError(
            f"cannot create output directory {output_dir}: {exc}"
        ) from exc
    # The temporary file sits next to the destination (same filesystem), so
    # the final publish is a true atomic rename and a crash can never leave
    # a half-written file at the output path.
    try:
        fd, tmp_path = tempfile.mkstemp(
            prefix=".ledger-backup-", suffix=".tmp", dir=output_dir
        )
    except OSError as exc:
        raise BackupError(
            f"cannot create temporary file in {output_dir}: {exc}"
        ) from exc
    os.close(fd)
    try:
        _copy_snapshot(source_path, tmp_path)
        _verify(tmp_path)
        _fsync_file(tmp_path)
        _publish(tmp_path, output_path)
    except BaseException:
        # Whatever went wrong, the temporary file (and any SQLite sidecar)
        # must not survive, and a pre-existing backup at the output path
        # must remain untouched.
        _remove_quietly(
            tmp_path,
            tmp_path + "-journal",
            tmp_path + "-wal",
            tmp_path + "-shm",
        )
        raise
    # Belt and braces: no SQLite sidecar of the temporary file may linger
    # next to the published backup.
    _remove_quietly(tmp_path + "-journal", tmp_path + "-wal", tmp_path + "-shm")
    return output_path


def _warn_if_not_persistent(output_path: str) -> None:
    """Warn when the backup leaves the database's (persistent) directory."""
    db_dir = os.path.dirname(os.path.abspath(settings.db_path))
    output_dir = os.path.dirname(output_path)
    try:
        inside = os.path.commonpath((output_dir, db_dir)) == db_dir
    except ValueError:
        inside = False
    if not inside:
        print(
            f"backup: warning: {output_path} is outside {db_dir}; inside "
            "the container only /data is persistent — backups elsewhere are "
            "lost on container rebuild",
            file=sys.stderr,
        )


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    try:
        output_path = run(args.output)
    except (BackupError, OSError) as exc:
        print(f"backup: error: {exc}", file=sys.stderr)
        return 1
    _warn_if_not_persistent(output_path)
    size = os.path.getsize(output_path)
    print(f"backup: wrote consistent snapshot to {output_path} ({size} bytes)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
