"""
db/repository.py — All database read/write operations.

Clean API: the rest of the app never writes SQL directly.
All methods work with both SQLite and MySQL via get_connection().
"""

from __future__ import annotations
from dataclasses import dataclass
from typing import Optional
from db.connection import get_connection


# ---------------------------------------------------------------------------
# Data classes (plain Python — no ORM dependency)
# ---------------------------------------------------------------------------

@dataclass
class Company:
    id:         int
    name:       str
    created_at: str


@dataclass
class StatementRecord:
    id:              int
    company_id:      int
    company_name:    str
    ledger_type:     str
    from_date:       str
    to_date:         str
    interest_rate:   float
    debit_days:      int
    credit_days:     int
    manual_interest: float
    input_filename:  str
    output_filename: str
    output_path:     str
    created_at:      str


# ---------------------------------------------------------------------------
# Company operations
# ---------------------------------------------------------------------------

def get_all_companies() -> list[Company]:
    conn = get_connection()
    try:
        rows = conn.execute(
            "SELECT id, name, created_at FROM companies ORDER BY name"
        ).fetchall()
        return [Company(id=r["id"], name=r["name"], created_at=r["created_at"]) for r in rows]
    finally:
        conn.close()


def add_company(name: str) -> Company:
    """Add a new company. Raises ValueError if name already exists."""
    name = name.strip()
    if not name:
        raise ValueError("Company name cannot be empty.")
    conn = get_connection()
    try:
        conn.execute("INSERT INTO companies (name) VALUES (?)", (name,))
        conn.commit()
        row = conn.execute(
            "SELECT id, name, created_at FROM companies WHERE name = ?", (name,)
        ).fetchone()
        return Company(id=row["id"], name=row["name"], created_at=row["created_at"])
    except Exception as e:
        conn.rollback()
        if "UNIQUE" in str(e).upper():
            raise ValueError(f"Company '{name}' already exists.")
        raise
    finally:
        conn.close()


def delete_company(company_id: int) -> None:
    conn = get_connection()
    try:
        conn.execute("DELETE FROM companies WHERE id = ?", (company_id,))
        conn.commit()
    finally:
        conn.close()


def rename_company(company_id: int, new_name: str) -> None:
    new_name = new_name.strip()
    if not new_name:
        raise ValueError("Company name cannot be empty.")
    conn = get_connection()
    try:
        conn.execute("UPDATE companies SET name = ? WHERE id = ?", (new_name, company_id))
        conn.commit()
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Statement operations
# ---------------------------------------------------------------------------

def save_statement(
    company_id:      int,
    ledger_type:     str,
    from_date:       str,
    to_date:         str,
    interest_rate:   float,
    debit_days:      int,
    credit_days:     int,
    manual_interest: float,
    input_filename:  str,
    output_filename: str,
    output_path:     str,
    user_id:         int = 0,
) -> int:
    """Insert a statement record. Returns the new row id."""
    conn = get_connection()
    try:
        cursor = conn.execute(
            """INSERT INTO statements
               (user_id, company_id, ledger_type, from_date, to_date, interest_rate,
                debit_days, credit_days, manual_interest,
                input_filename, output_filename, output_path)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
            (user_id, company_id, ledger_type, from_date, to_date, interest_rate,
             debit_days, credit_days, manual_interest,
             input_filename, output_filename, output_path)
        )
        conn.commit()
        return cursor.lastrowid
    finally:
        conn.close()


def get_statements_for_company(company_id: int) -> list[StatementRecord]:
    conn = get_connection()
    try:
        rows = conn.execute(
            """SELECT s.*, c.name as company_name
               FROM statements s
               JOIN companies c ON c.id = s.company_id
               WHERE s.company_id = ?
               ORDER BY s.created_at DESC""",
            (company_id,)
        ).fetchall()
        return [_row_to_statement(r) for r in rows]
    finally:
        conn.close()


def get_all_statements() -> list[StatementRecord]:
    conn = get_connection()
    try:
        rows = conn.execute(
            """SELECT s.*, c.name as company_name
               FROM statements s
               JOIN companies c ON c.id = s.company_id
               ORDER BY s.created_at DESC"""
        ).fetchall()
        return [_row_to_statement(r) for r in rows]
    finally:
        conn.close()


def _row_to_statement(r) -> StatementRecord:
    return StatementRecord(
        id=r["id"], company_id=r["company_id"], company_name=r["company_name"],
        ledger_type=r["ledger_type"], from_date=r["from_date"], to_date=r["to_date"],
        interest_rate=r["interest_rate"], debit_days=r["debit_days"],
        credit_days=r["credit_days"], manual_interest=r["manual_interest"],
        input_filename=r["input_filename"], output_filename=r["output_filename"],
        output_path=r["output_path"], created_at=r["created_at"],
    )
