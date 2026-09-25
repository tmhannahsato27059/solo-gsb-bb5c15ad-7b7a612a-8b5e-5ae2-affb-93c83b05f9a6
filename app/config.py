"""Application configuration, read once from environment variables."""
from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    db_path: str = os.getenv("DATABASE_PATH", "/data/ledger.db")
    host: str = os.getenv("HOST", "0.0.0.0")
    port: int = int(os.getenv("PORT", "8000"))
    # Busy timeout (milliseconds) for SQLite writers under BEGIN IMMEDIATE.
    busy_timeout_ms: int = int(os.getenv("SQLITE_BUSY_TIMEOUT_MS", "10000"))
    # Numerical slack used when comparing composed epsilon against the cap,
    # so accumulated floating-point round-off never rejects a valid request.
    epsilon_tolerance: float = float(os.getenv("EPSILON_TOLERANCE", "1e-12"))


settings = Settings()
