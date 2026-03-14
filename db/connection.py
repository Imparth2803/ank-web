"""
db/connection.py — SQLite locally, Postgres on Railway (via DATABASE_URL).
"""
import os
import sqlite3
from pathlib import Path
from core.config import DB_PATH

DATABASE_URL = os.getenv("DATABASE_URL", "")


def get_connection():
    if DATABASE_URL.startswith("postgres"):
        try:
            import psycopg2
            import psycopg2.extras
        except ImportError:
            raise RuntimeError("psycopg2-binary not installed.")
        conn = psycopg2.connect(DATABASE_URL,
                                cursor_factory=psycopg2.extras.RealDictCursor)
        conn.autocommit = False
        return _PgConn(conn)

    # SQLite
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


class _PgConn:
    def __init__(self, conn):
        self._c = conn
        self._last_id = None

    def execute(self, sql, params=()):
        cur = self._c.cursor()
        sql = sql.replace("?", "%s")
        is_insert = sql.strip().upper().startswith("INSERT")
        if is_insert and "RETURNING" not in sql.upper():
            sql = sql.rstrip().rstrip(";") + " RETURNING id"
        cur.execute(sql, params)
        if is_insert:
            try:
                row = cur.fetchone()
                self._last_id = row["id"] if row else None
            except Exception:
                self._last_id = None
        return _PgCur(cur, self._last_id)

    def executescript(self, script):
        cur = self._c.cursor()
        for stmt in script.split(";"):
            s = stmt.strip()
            if s:
                cur.execute(s)

    def commit(self):   self._c.commit()
    def rollback(self): self._c.rollback()
    def close(self):    self._c.close()


class _PgCur:
    def __init__(self, cur, last_id=None):
        self._cur = cur
        self._last_id = last_id

    def fetchone(self):
        try:
            return self._cur.fetchone()
        except Exception:
            return None

    def fetchall(self):
        try:
            return self._cur.fetchall()
        except Exception:
            return []

    @property
    def lastrowid(self):
        return self._last_id
