"""db/schema.py — All table definitions including users and invite codes."""

from db.connection import get_connection
from core.config import DB_ENGINE

_SQLITE_SCHEMA = """
CREATE TABLE IF NOT EXISTS users (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    full_name     TEXT NOT NULL,
    email         TEXT NOT NULL UNIQUE,
    password_hash TEXT NOT NULL,
    company_name  TEXT NOT NULL,
    is_active     INTEGER NOT NULL DEFAULT 1,
    tc_accepted   INTEGER NOT NULL DEFAULT 0,
    tc_accepted_at TEXT,
    created_at    TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS invite_codes (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    code       TEXT NOT NULL UNIQUE,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    expires_at TEXT,
    is_used    INTEGER NOT NULL DEFAULT 0,
    used_by    INTEGER REFERENCES users(id),
    used_at    TEXT
);

CREATE TABLE IF NOT EXISTS companies (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    name       TEXT NOT NULL UNIQUE,
    user_id    INTEGER REFERENCES users(id),
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS statements (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id         INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    company_id      INTEGER REFERENCES companies(id),
    ledger_type     TEXT NOT NULL,
    from_date       TEXT NOT NULL,
    to_date         TEXT NOT NULL,
    interest_rate   REAL NOT NULL,
    debit_days      INTEGER NOT NULL DEFAULT 0,
    credit_days     INTEGER NOT NULL DEFAULT 0,
    manual_interest REAL NOT NULL DEFAULT 0,
    input_filename  TEXT NOT NULL,
    output_filename TEXT NOT NULL,
    output_path     TEXT NOT NULL,
    created_at      TEXT NOT NULL DEFAULT (datetime('now'))
);
"""

def initialise_db() -> None:
    conn = get_connection()
    try:
        conn.executescript(_SQLITE_SCHEMA)
        conn.commit()
    finally:
        conn.close()
