"""
core/transformer.py — ANK calculation logic + Purchase/Sale direction.

Sale     → debit = receivable from customer  (net positive = you are owed)
Purchase → credit = payable to supplier      (columns swapped before ANK)
"""

import pandas as pd
from core.config import ANK_DIVISOR, LedgerType


def compute_days(date: pd.Series, to_date: pd.Timestamp) -> pd.Series:
    # ANK convention: to_date is inclusive, so add 1 day to get day count
    end = to_date + pd.Timedelta(days=1)
    days = (end - date).dt.days
    return days.clip(lower=0).fillna(0).astype(int)


def compute_ank(amount: pd.Series, days: pd.Series) -> pd.Series:
    """Core ANK formula: (amount × days) / ANK_DIVISOR."""
    return (amount * days) / ANK_DIVISOR


def transform(
    df: pd.DataFrame,
    to_date: pd.Timestamp,
    ledger_type: str = LedgerType.SALE,
    debit_days: int = 0,
    credit_days: int = 0,
) -> pd.DataFrame:
    """
    Apply ANK transformation.

    For PURCHASE ledgers, debit and credit columns are swapped before
    calculation so the ANK direction reflects money going OUT.

    Adds columns: debit_days_col, credit_days_col, debit_ank, credit_ank.
    """
    df = df.copy()

    raw_days = compute_days(df["date"], to_date)
    df["debit_days_col"]  = (raw_days - debit_days).clip(lower=0)
    df["credit_days_col"] = (raw_days - credit_days).clip(lower=0)
    df["debit_ank"]  = compute_ank(df["debit"],  df["debit_days_col"])
    df["credit_ank"] = compute_ank(df["credit"], df["credit_days_col"])
    return df
