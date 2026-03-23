"""
main.py — FastAPI backend for ANK Statement Generator (web version).

Mirrors desktop app flow:
  Screen 1 → ledger type (sale/purchase)
  Screen 2 → upload 1-5 files
  Screen 3 → parameters (company, client, dates, interest, day offsets)
  Screen 4 → rename output files
  Screen 5 → generate & download all
"""
from __future__ import annotations
import hashlib, json, base64, logging, os, tempfile, uuid
from pathlib import Path

import pandas as pd
from fastapi import Depends, FastAPI, File, Form, Header, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from fastapi.staticfiles import StaticFiles

from core.config import LedgerType
from core.loader import load_ledger, LoadError
from core.transformer import transform
from core.summarizer import summarize
from core.exporter import export, ExportMeta
from db.schema import initialise_db
from db.auth import (login_user, register_user, generate_invite_code,
                     get_all_invite_codes, delete_invite_code,
                     get_all_users, set_user_active)
from db.repository import (get_all_companies, add_company,
                            delete_company, save_statement)

# ---------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S")
log = logging.getLogger(__name__)

app = FastAPI(title="ANK Statement Generator", version="2.0.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"],
                   allow_methods=["*"], allow_headers=["*"])

_jobs: dict[str, dict] = {}
SECRET_KEY   = os.getenv("SECRET_KEY",   "change-me-in-production")
ADMIN_SECRET = os.getenv("ADMIN_SECRET", "admin@ank2024")

@app.on_event("startup")
def startup():
    initialise_db()
    log.info("DB initialised")

# ---------------------------------------------------------------------------
# Token helpers
# ---------------------------------------------------------------------------
def _sign(payload: str) -> str:
    return hashlib.sha256(f"{SECRET_KEY}{payload}".encode()).hexdigest()[:16]

def make_token(user_id: int, email: str) -> str:
    p   = json.dumps({"user_id": user_id, "email": email})
    b64 = base64.urlsafe_b64encode(p.encode()).decode()
    return f"{b64}.{_sign(b64)}"

def decode_token(token: str) -> dict:
    try:
        b64, sig = token.rsplit(".", 1)
        if _sign(b64) != sig:
            raise ValueError
        return json.loads(base64.urlsafe_b64decode(b64).decode())
    except Exception:
        raise HTTPException(401, "Invalid or expired session.")

def require_auth(authorization: str = Header(default="")) -> dict:
    if not authorization.startswith("Bearer "):
        raise HTTPException(401, "Missing Authorization header.")
    return decode_token(authorization.removeprefix("Bearer ").strip())

def _require_admin(pw: str):
    if pw != ADMIN_SECRET:
        raise HTTPException(403, "Invalid admin password.")

def _parse_date(s: str) -> pd.Timestamp:
    for fmt in ("%d-%m-%Y", "%Y-%m-%d", "%d/%m/%Y"):
        try:
            return pd.Timestamp(pd.to_datetime(s, format=fmt))
        except (ValueError, TypeError):
            pass
    raise ValueError(f"Cannot parse date: {s!r}. Use DD-MM-YYYY.")

# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------
@app.post("/api/auth/login")
async def api_login(body: dict):
    try:
        user  = login_user(body.get("email", ""), body.get("password", ""))
        return {"token": make_token(user.id, user.email),
                "user_id": user.id, "full_name": user.full_name,
                "email": user.email, "company_name": user.company_name}
    except ValueError as e:
        raise HTTPException(401, str(e))

@app.post("/api/auth/register")
async def api_register(body: dict):
    if not body.get("tc_accepted"):
        raise HTTPException(400, "You must accept the Terms & Conditions.")
    try:
        user = register_user(
            full_name=body.get("full_name",""), email=body.get("email",""),
            password=body.get("password",""), company_name=body.get("company_name",""),
            invite_code=body.get("invite_code",""))
        return {"token": make_token(user.id, user.email),
                "user_id": user.id, "full_name": user.full_name,
                "email": user.email, "company_name": user.company_name}
    except ValueError as e:
        raise HTTPException(400, str(e))

# ---------------------------------------------------------------------------
# Companies
# ---------------------------------------------------------------------------
@app.get("/api/companies")
async def api_companies(session=Depends(require_auth)):
    return [{"id":c.id,"name":c.name,"created_at":c.created_at}
            for c in get_all_companies()]

@app.post("/api/companies")
async def api_add_company(body: dict, session=Depends(require_auth)):
    try:
        c = add_company(body.get("name",""))
        return {"id":c.id,"name":c.name}
    except ValueError as e:
        raise HTTPException(400, str(e))

@app.delete("/api/companies/{cid}")
async def api_del_company(cid: int, session=Depends(require_auth)):
    delete_company(cid)
    return {"deleted": cid}

# ---------------------------------------------------------------------------
# Upload (validate files, return row counts — Screen 2)
# ---------------------------------------------------------------------------
@app.post("/api/upload")
async def api_upload(files: list[UploadFile] = File(...),
                     authorization: str = Header(default="")):
    session    = require_auth(authorization)
    if len(files) > 5:
        raise HTTPException(400, "Maximum 5 files allowed.")
    results = []
    for f in files:
        suffix = Path(f.filename).suffix.lower()
        if suffix not in (".xlsx",".xls",".csv"):
            raise HTTPException(400, f"{f.filename}: only .xlsx/.xls/.csv accepted.")
        content = await f.read()
        with tempfile.NamedTemporaryFile(suffix=suffix, delete=False) as tmp:
            tmp.write(content); tmp_path = Path(tmp.name)
        try:
            df, _ = load_ledger(tmp_path)
            results.append({"original_name":f.filename,"row_count":len(df),"status":"ok"})
        except LoadError as e:
            results.append({"original_name":f.filename,"row_count":0,
                            "status":"error","error":str(e)})
        finally:
            tmp_path.unlink(missing_ok=True)
    return {"files": results}

# ---------------------------------------------------------------------------
# Process (full pipeline — Screen 5)
# ---------------------------------------------------------------------------
@app.post("/api/process")
async def api_process(
    files:            list[UploadFile] = File(...),
    company_name:     str   = Form(...),
    client_name:      str   = Form(...),
    from_date:        str   = Form(...),
    to_date:          str   = Form(...),
    interest_rate:    float = Form(...),
    ledger_type:      str   = Form(LedgerType.SALE),
    debit_days:       int   = Form(0),
    credit_days:      int   = Form(0),
    manual_interest:  float = Form(0.0),
    output_filenames: str   = Form(...),  # JSON array
    company_id:       int   = Form(0),
    authorization:    str   = Header(default=""),
):
    session    = require_auth(authorization)
    to_date_ts = _parse_date(to_date)
    names = json.loads(output_filenames)

    if len(files) != len(names):
        raise HTTPException(400, "files count must match output_filenames count.")

    results = []
    for f, fname in zip(files, names):
        suffix  = Path(f.filename).suffix.lower()
        content = await f.read()
        with tempfile.NamedTemporaryFile(suffix=suffix, delete=False) as tmp:
            tmp.write(content); tmp_path = Path(tmp.name)
        try:
            df, _   = load_ledger(tmp_path)
            df      = transform(df, to_date_ts, ledger_type=ledger_type,
                                 debit_days=debit_days, credit_days=credit_days)
            summary = summarize(df, interest_rate)

            if manual_interest > 0:
                from dataclasses import replace as _replace
                avg     = (summary.receivable / manual_interest) if manual_interest else 0.0
                summary = _replace(summary, interest=manual_interest, avg_days=avg)

            meta = ExportMeta(
                company_name=company_name, client_name=client_name,
                from_date=from_date, to_date=to_date,
                interest_rate=interest_rate, ledger_type=ledger_type,
                debit_days=debit_days, credit_days=credit_days,
                manual_interest=manual_interest, output_filename=fname)

            out_path = Path(tempfile.mktemp(suffix=".xlsx"))
            export(df, summary, meta, out_path)

            job_id = str(uuid.uuid4())
            _jobs[job_id] = {"path": out_path, "filename": fname}

            if company_id:
                save_statement(
                    company_id=company_id, ledger_type=ledger_type,
                    from_date=from_date, to_date=to_date,
                    interest_rate=interest_rate, debit_days=debit_days,
                    credit_days=credit_days, manual_interest=manual_interest,
                    input_filename=f.filename, output_filename=fname,
                    output_path=str(out_path),
                    user_id=session.get("user_id", 0))

            results.append({
                "filename": fname, "job_id": job_id, "status": "success",
                "row_count":         len(df),
                "total_debit":       round(float(summary.total_debit), 2),
                "total_credit":      round(float(summary.total_credit), 2),
                "net_balance":       round(float(summary.net_balance), 2),
                "net_balance_label": summary.net_balance_label,
                "net_ank":           round(float(summary.net_ank), 2),
                "net_ank_label":     summary.net_ank_label,
                "interest":          round(float(summary.interest), 2),
                "avg_days":          round(float(summary.avg_days or 0), 2),
            })
        except Exception as e:
            log.exception("process | CRASH | file=%s", fname)
            results.append({"filename":fname,"job_id":None,"status":"error","error":str(e)})
        finally:
            tmp_path.unlink(missing_ok=True)

    return {"results": results}

@app.get("/api/download/{job_id}")
async def api_download(job_id: str, authorization: str = Header(default="")):
    session    = require_auth(authorization)
    job = _jobs.get(job_id)
    if not job or not job["path"].exists():
        raise HTTPException(404, "File not found or already downloaded.")
    path, filename = job["path"], job["filename"]
    def iterfile():
        with open(path,"rb") as fh: yield from fh
        path.unlink(missing_ok=True); _jobs.pop(job_id, None)
    return StreamingResponse(iterfile(),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'})

# ---------------------------------------------------------------------------
# Admin
# ---------------------------------------------------------------------------
@app.post("/api/admin/invite")
async def api_gen_invite(body: dict):
    _require_admin(body.get("admin_password",""))
    code = generate_invite_code()
    log.info("admin | invite generated: %s", code)
    return {"code": code}

@app.post("/api/admin/invites")
async def api_list_invites(body: dict):
    _require_admin(body.get("admin_password",""))
    return {"invites": get_all_invite_codes()}

@app.post("/api/admin/invites/{code_id}/delete")
async def api_del_invite(code_id: int, body: dict):
    _require_admin(body.get("admin_password",""))
    delete_invite_code(code_id)
    return {"deleted": code_id}

@app.post("/api/admin/users")
async def api_list_users(body: dict):
    _require_admin(body.get("admin_password",""))
    users = get_all_users()
    return {"users": [{"id":u.id,"full_name":u.full_name,"email":u.email,
                        "company_name":u.company_name,"is_active":u.is_active,
                        "tc_accepted":u.tc_accepted,"created_at":u.created_at}
                       for u in users]}

@app.post("/api/admin/users/{user_id}/toggle")
async def api_toggle_user(user_id: int, body: dict):
    _require_admin(body.get("admin_password",""))
    active = body.get("is_active", True)
    set_user_active(user_id, active)
    return {"user_id": user_id, "is_active": active}

# ---------------------------------------------------------------------------
app.mount("/", StaticFiles(directory="static", html=True), name="static")
