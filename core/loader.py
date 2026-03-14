"""
core/loader.py — File ingestion and column normalisation.

Handles Excel files with top metadata rows (Company Name, From/To Date,
Client Name) before the actual data table.

The table can start at any row AND any column — the loader finds the header
row by scanning for recognised column aliases, identifies which columns map
to date/bill_no/debit/credit, and slices only those columns.

Returns: (df, meta)
  df   — normalised DataFrame with canonical columns: date, bill_no, debit, credit
  meta — dict: company_name, client_name, from_date, to_date, interest_rate,
               debit_days, credit_days
"""

import re
import pandas as pd
from pathlib import Path
from core.config import COLUMN_ALIASES


class LoadError(Exception):
    pass


# Build alias → canonical lookup  {"DATE": "date", "BILL NO.": "bill_no", ...}
_ALIAS_TO_CANONICAL: dict[str, str] = {
    alias.strip(): canonical
    for canonical, aliases in COLUMN_ALIASES.items()
    for alias in aliases
}

# Meta-row patterns
_RE_COMPANY  = re.compile(r'company\s*name\s*[:\-]?\s*(.*)', re.I)
_RE_CLIENT   = re.compile(r'client\s*name\s*[:\-]?\s*(.*)', re.I)
_RE_FROM     = re.compile(r'from\s*date\s*[:\-]?', re.I)
_RE_TO       = re.compile(r'to\s*date\s*[:\-]?', re.I)
_RE_INTEREST = re.compile(r'interest\s*[:\-]?\s*([\d.]+)', re.I)
_RE_DR_DAYS  = re.compile(r'dr\s*days\s*[:\-]?\s*([\d]+)', re.I)
_RE_CR_DAYS  = re.compile(r'cr\s*days\s*[:\-]?\s*([\d]+)', re.I)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def load_ledger(filepath: str | Path) -> tuple[pd.DataFrame, dict]:
    path = Path(filepath)
    if not path.exists():
        raise LoadError(f"File not found: {path}")

    try:
        raw = (pd.read_csv(path, header=None, dtype=str)
               if path.suffix.lower() == ".csv"
               else pd.read_excel(path, header=None, dtype=str))
    except Exception as exc:
        raise LoadError(f"Could not read file: {exc}") from exc

    # Find the header row and which columns hold our 4 canonical fields
    header_row, col_map = _find_header(raw)
    if header_row is None:
        raise LoadError(
            "Could not find column headers (DATE, BILL NO., DEBIT AMT., CREDIT AMT.) "
            "in the file. Please check the format."
        )

    meta = _extract_meta(raw, header_row)
    df   = _build_df(raw, header_row, col_map)
    df   = _clean_dtypes(df)
    return df, meta


# ---------------------------------------------------------------------------
# Header detection — returns (row_index, {canonical: col_index})
# ---------------------------------------------------------------------------

def _find_header(raw: pd.DataFrame) -> tuple[int | None, dict]:
    for i, row in raw.iterrows():
        col_map = {}
        for j, cell in enumerate(row):
            s = str(cell).strip() if pd.notna(cell) else ""
            canonical = _ALIAS_TO_CANONICAL.get(s)
            if canonical and canonical not in col_map:
                col_map[canonical] = j

        # Accept if we found at least date + one of debit/credit
        if "date" in col_map and ("debit" in col_map or "credit" in col_map):
            # Fill in bill_no with a dummy col if not present
            return i, col_map

    return None, {}


# ---------------------------------------------------------------------------
# Build the 4-column DataFrame
# ---------------------------------------------------------------------------

def _build_df(raw: pd.DataFrame, header_row: int, col_map: dict) -> pd.DataFrame:
    data = raw.iloc[header_row + 1:].copy().reset_index(drop=True)

    result = pd.DataFrame()
    for canonical in ["date", "bill_no", "debit", "credit"]:
        if canonical in col_map:
            result[canonical] = data.iloc[:, col_map[canonical]].values
        else:
            result[canonical] = ""   # bill_no missing → empty string; debit/credit → 0

    # Drop rows where everything is NaN / empty
    result = result[~result.apply(
        lambda r: all(str(v).strip() in ("", "nan", "None", "NaN")
                      for v in r), axis=1
    )].reset_index(drop=True)

    return result


# ---------------------------------------------------------------------------
# Meta extraction (rows above the header row)
# ---------------------------------------------------------------------------

def _extract_meta(raw: pd.DataFrame, header_row: int) -> dict:
    meta: dict = {}

    for i in range(header_row):
        row = raw.iloc[i]
        cells = [(j, str(v).strip()) for j, v in enumerate(row)
                 if pd.notna(v) and str(v).strip() not in ("", "nan")]

        for j, cell in cells:
            # Company name
            m = _RE_COMPANY.match(cell)
            if m:
                name = m.group(1).strip()
                if name and "company" not in name.lower():
                    meta.setdefault("company_name", name)
                elif not name:
                    nxt = _next_nonempty(row, j)
                    if nxt:
                        meta.setdefault("company_name", nxt)

            # Client name
            m = _RE_CLIENT.match(cell)
            if m:
                name = m.group(1).strip()
                if name:
                    meta.setdefault("client_name", name)
                else:
                    nxt = _next_nonempty(row, j)
                    if nxt:
                        meta.setdefault("client_name", nxt)

            # From Date
            if _RE_FROM.match(cell):
                nxt = _next_nonempty(row, j)
                if nxt:
                    meta.setdefault("from_date", _safe_ts(nxt))

            # To Date
            if _RE_TO.match(cell):
                nxt = _next_nonempty(row, j)
                if nxt:
                    meta.setdefault("to_date", _safe_ts(nxt))

            # Interest rate
            m = _RE_INTEREST.search(cell)
            if m:
                val = float(m.group(1))
                # stored as 0.12 → convert to 12, stored as 12 → keep as 12
                meta.setdefault("interest_rate", val if val > 1 else round(val * 100, 4))

            # DR / CR days
            m = _RE_DR_DAYS.search(cell)
            if m:
                meta.setdefault("debit_days", int(m.group(1)))
            m = _RE_CR_DAYS.search(cell)
            if m:
                meta.setdefault("credit_days", int(m.group(1)))

    return meta


def _next_nonempty(row, col_idx: int) -> str | None:
    for val in row.iloc[col_idx + 1:]:
        s = str(val).strip() if pd.notna(val) else ""
        if s and s.lower() not in ("nan", "none"):
            return s
    return None


def _safe_ts(s: str):
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y"):
        try:
            return pd.Timestamp(pd.to_datetime(s, format=fmt))
        except (ValueError, TypeError):
            pass
    try:
        return pd.Timestamp(pd.to_datetime(s, errors="coerce"))
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Type cleanup
# ---------------------------------------------------------------------------

def _clean_dtypes(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["date"]    = pd.to_datetime(df["date"], dayfirst=True, errors="coerce")
    df["debit"]   = pd.to_numeric(df["debit"].replace("", 0).fillna(0),  errors="coerce").fillna(0)
    df["credit"]  = pd.to_numeric(df["credit"].replace("", 0).fillna(0), errors="coerce").fillna(0)
    df["bill_no"] = df["bill_no"].fillna("").astype(str).str.strip()

    # Drop Total/summary rows and rows with no valid date
    is_total   = df["bill_no"].str.lower().str.contains(r'total', na=False)
    has_no_date = df["date"].isna()
    is_empty   = (df["debit"] == 0) & (df["credit"] == 0)
    return df[~is_total & ~has_no_date & ~(has_no_date & is_empty)].reset_index(drop=True)
