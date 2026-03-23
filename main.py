"""
main.py
-------
FastAPI backend for ANK Statement Generator (web version).

Routes:
    POST /api/validate-key          → validate access key, return session token
    POST /api/upload                → load Excel, return autofilled meta + preview
    POST /api/process               → transform + summarize, store result
    GET  /api/download/{job_id}     → stream the generated .xlsx
    POST /api/admin/create-key      → generate a new access key (admin only)

All routes except /api/validate-key and /api/admin/create-key require
a valid session token in the Authorization header:
    Authorization: Bearer <token>
"""

from __future__ import annotations

import io
import logging
import os
import tempfile
import uuid
from pathlib import Path

import pandas as pd
from fastapi import FastAPI, File, Form, Header, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles

from db.auth import (
    create_access_key,
    create_session_token,
    validate_access_key,
    verify_session_token,
)
from db.schema import initialise_db
from core.exporter import ExportMeta, export
from core.loader import LoadError, load_ledger
from core.summarizer import summarize
from core.transformer import transform

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# App setup
# ---------------------------------------------------------------------------

app = FastAPI(title="ANK Statement Generator", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# In-memory job store: {job_id: {"path": Path, "filename": str}}
# For 2-5 concurrent users this is fine. Replace with Redis for scale.
_jobs: dict[str, dict] = {}

ADMIN_SECRET = os.getenv("ADMIN_SECRET", "admin-change-me")


@app.on_event("startup")
def startup():
    initialise_db()
    log.info("Database initialised")


# ---------------------------------------------------------------------------
# Auth dependency
# ---------------------------------------------------------------------------

def require_session(authorization: str = Header(default="")) -> dict:
    """FastAPI dependency — validates Bearer token on every protected route."""
    if not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing or invalid Authorization header.")

    token  = authorization.removeprefix("Bearer ").strip()
    result = verify_session_token(token)

    if not result["valid"]:
        raise HTTPException(status_code=403, detail=result["reason"])

    return result


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.post("/api/validate-key")
async def validate_key(body: dict):
    """
    Validate an access key and return a session token.

    Body: {"key": "TYP-XXXX-XXXX-XXXX"}
    Returns: {"token": "...", "expires_at": "20 Mar 2026"}
    """
    key    = body.get("key", "").strip().upper()
    result = validate_access_key(key)

    if not result["valid"]:
        raise HTTPException(status_code=403, detail=result["reason"])

    token = create_session_token(result["key_id"])
    log.info("validate-key | success | key=%s", key)

    return {
        "token":      token,
        "expires_at": result["expires_at"],
    }


@app.post("/api/upload")
async def upload_file(
    file: UploadFile = File(...),
    authorization: str = Header(default=""),
):
    """
    Upload an Excel/CSV file. Returns autofilled meta extracted from file.

    Returns:
    {
        "company_name": "...",
        "client_name":  "...",
        "from_date":    "01-04-2024",
        "to_date":      "31-03-2025",
        "interest_rate": null,    ← null means not found, user must enter
        "debit_days":   0,
        "credit_days":  0,
        "row_count":    8,
    }
    """
    session = require_session(authorization)

    suffix = Path(file.filename).suffix.lower()
    if suffix not in (".xlsx", ".xls", ".csv"):
        raise HTTPException(status_code=400, detail="Only .xlsx, .xls, .csv files accepted.")

    # Save upload to temp file
    content = await file.read()
    with tempfile.NamedTemporaryFile(suffix=suffix, delete=False) as tmp:
        tmp.write(content)
        tmp_path = Path(tmp.name)

    try:
        df, meta = load_ledger(tmp_path)
    except LoadError as exc:
        tmp_path.unlink(missing_ok=True)
        raise HTTPException(status_code=422, detail=str(exc))
    finally:
        tmp_path.unlink(missing_ok=True)

    log.info("upload | rows=%d meta=%s", len(df), {k: str(v)[:20] for k, v in meta.items()})

    def fmt_date(ts) -> str:
        if ts is None:
            return ""
        return pd.Timestamp(ts).strftime("%d-%m-%Y")

    return {
        "company_name":  meta.get("company_name") or "",
        "client_name":   meta.get("client_name")  or "",
        "from_date":     fmt_date(meta.get("from_date")),
        "to_date":       fmt_date(meta.get("to_date")),
        "interest_rate": meta.get("interest_rate"),   # null if not in file
        "debit_days":    meta.get("debit_days", 0),
        "credit_days":   meta.get("credit_days", 0),
        "row_count":     len(df),
    }


@app.post("/api/process")
async def process(
    file:           UploadFile = File(...),
    company_name:   str        = Form(...),
    client_name:    str        = Form(...),
    from_date:      str        = Form(...),
    to_date:        str        = Form(...),
    interest_rate:  float      = Form(...),
    debit_days:     int        = Form(0),
    credit_days:    int        = Form(0),
    manual_interest: float     = Form(0.0),
    output_filename: str       = Form("ANK_Statement"),
    ledger_type:    str        = Form("sale"),
    authorization:  str        = Header(default=""),
):
    """
    Full pipeline: load → transform → summarize → export.
    Stores output file in memory, returns job_id for download.

    Returns summary preview + job_id.
    """
    session = require_session(authorization)

    suffix  = Path(file.filename).suffix.lower()
    content = await file.read()

    with tempfile.NamedTemporaryFile(suffix=suffix, delete=False) as tmp:
        tmp.write(content)
        tmp_path = Path(tmp.name)

    try:
        # Parse to_date
        to_date_ts = _parse_date(to_date)

        # Load
        df, _ = load_ledger(tmp_path)

        # Transform
        df = transform(df, to_date_ts, debit_days=debit_days, credit_days=credit_days)

        # Summarize
        summary = summarize(df, interest_rate)

        # Override interest if manual value provided
        if manual_interest > 0:
            from dataclasses import replace
            avg = (summary.net_ank / summary.net_balance) if summary.net_balance != 0 else 0.0
            summary = replace(summary, interest=manual_interest, avg_days=avg)

        # Export to bytes buffer
        meta = ExportMeta(
            company_name    = company_name,
            client_name     = client_name,
            from_date       = from_date,
            to_date         = to_date,
            interest_rate   = interest_rate,
            debit_days      = debit_days,
            credit_days     = credit_days,
            manual_interest = manual_interest,
            ledger_type     = ledger_type,
        )

        fname    = output_filename.strip().rstrip(".xlsx") + ".xlsx"
        out_path = Path(tempfile.mktemp(suffix=".xlsx"))
        export(df, summary, meta, out_path)

        # Store job
        job_id = str(uuid.uuid4())
        _jobs[job_id] = {"path": out_path, "filename": fname}

        log.info("process | job_id=%s file=%s rows=%d", job_id, fname, len(df))

    except LoadError as exc:
        raise HTTPException(status_code=422, detail=str(exc))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    finally:
        tmp_path.unlink(missing_ok=True)

    return {
        "job_id":           job_id,
        "filename":         fname,
        "total_debit":      round(summary.total_debit, 2),
        "total_credit":     round(summary.total_credit, 2),
        "net_balance":      round(summary.net_balance, 2),
        "net_balance_label": summary.net_balance_label,
        "net_ank":          round(summary.net_ank, 2),
        "net_ank_label":    summary.net_ank_label,
        "interest":         round(summary.interest, 2),
        "avg_days":         round(summary.avg_days or 0, 2),
        "row_count":        len(df),
    }


@app.get("/api/download/{job_id}")
async def download(job_id: str, authorization: str = Header(default="")):
    """Stream the generated .xlsx file."""
    session = require_session(authorization)

    job = _jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="File not found or already downloaded.")

    path     = job["path"]
    filename = job["filename"]

    if not path.exists():
        raise HTTPException(status_code=404, detail="File no longer available.")

    def iterfile():
        with open(path, "rb") as f:
            yield from f
        path.unlink(missing_ok=True)
        _jobs.pop(job_id, None)

    headers = {"Content-Disposition": f'attachment; filename="{filename}"'}
    return StreamingResponse(
        iterfile(),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers=headers,
    )


@app.post("/api/admin/create-key")
async def admin_create_key(body: dict):
    """
    Generate a new access key. Protected by ADMIN_SECRET.

    Body: {"secret": "...", "label": "Client Name", "days": 7}
    Returns: {"key": "TYP-...", "expires_at": "20-03-2026"}
    """
    if body.get("secret") != ADMIN_SECRET:
        raise HTTPException(status_code=403, detail="Invalid admin secret.")

    label = body.get("label", "")
    days  = int(body.get("days", 7))
    result = create_access_key(label=label, days=days)

    log.info("admin/create-key | label=%s days=%d key=%s", label, days, result["key"])
    return result


# ---------------------------------------------------------------------------
# Serve frontend
# ---------------------------------------------------------------------------

app.mount("/", StaticFiles(directory="static", html=True), name="static")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _parse_date(s: str) -> pd.Timestamp:
    for fmt in ("%d-%m-%Y", "%Y-%m-%d", "%d/%m/%Y"):
        try:
            return pd.Timestamp(pd.to_datetime(s, format=fmt))
        except (ValueError, TypeError):
            pass
    raise ValueError(f"Cannot parse date: {s!r}. Use DD-MM-YYYY format.")
