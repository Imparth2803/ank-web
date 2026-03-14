"""
db/connection.py — Database connection factory.

On Railway: set DATABASE_URL env var to a postgres:// URL for Postgres.
Locally:    SQLite at ~/ANK_Generator/ank_data.db (or override with DB_PATH env).
"""

import os
import sqlite3
from pathlib import Path
from core.config import DB_PATH

DATABASE_URL = os.getenv("DATABASE_URL", "")


def get_connection():
    """Return a DB connection. Uses Postgres on Railway, SQLite locally."""
    if DATABASE_URL.startswith("postgres"):
        try:
            import psycopg2
            import psycopg2.extras
        except ImportError:
            raise RuntimeError(
                "psycopg2 not installed. Add 'psycopg2-binary' to requirements.txt"
            )
        conn = psycopg2.connect(DATABASE_URL, cursor_factory=psycopg2.extras.RealDictCursor)
        conn.autocommit = False
        return _PgWrapper(conn)

    # SQLite (local / default)
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


class _PgWrapper:
    """Thin wrapper so Postgres behaves like sqlite3 (executescript, commit, close)."""
    def __init__(self, conn):
        self._conn = conn

    def execute(self, sql, params=()):
        cur = self._conn.cursor()
        # Convert SQLite ? placeholders to Postgres %s
        sql = sql.replace("?", "%s")
        cur.execute(sql, params)
        return _CursorWrapper(cur)

    def executescript(self, script):
        """Run a multi-statement script (for schema init)."""
        cur = self._conn.cursor()
        for stmt in script.split(";"):
            s = stmt.strip()
            if s:
                # SQLite-specific → Postgres equivalents
                s = s.replace("INTEGER PRIMARY KEY AUTOINCREMENT",
                               "SERIAL PRIMARY KEY")
                s = s.replace("datetime('now')", "NOW()")
                cur.execute(s)

    def commit(self):   self._conn.commit()
    def rollback(self): self._conn.rollback()
    def close(self):    self._conn.close()


class _CursorWrapper:
    def __init__(self, cur): self._cur = cur
    def fetchone(self):  return self._cur.fetchone()
    def fetchall(self):  return self._cur.fetchall()
    @property
    def lastrowid(self): return self._cur.fetchone()[0] if self._cur.rowcount else None
