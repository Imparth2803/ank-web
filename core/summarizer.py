"""core/summarizer.py — Summary statistics from transformed ledger."""

from dataclasses import dataclass
import pandas as pd


@dataclass(frozen=True)
class Summary:
    total_debit:      float
    total_credit:     float
    total_debit_ank:  float
    total_credit_ank: float
    net_balance:      float
    net_ank:          float
    receivable:       float
    interest:         float
    avg_days:         float
    rate:             float

    @property
    def net_balance_label(self) -> str:
        return "Dr" if self.net_balance >= 0 else "Cr"

    @property
    def net_ank_label(self) -> str:
        return "Dr" if self.net_ank >= 0 else "Cr"


def summarize(df: pd.DataFrame, rate_percent: float) -> Summary:
    rate             = rate_percent / 100
    total_debit      = df["debit"].sum()
    total_credit     = df["credit"].sum()
    total_debit_ank  = df["debit_ank"].sum()
    total_credit_ank = df["credit_ank"].sum()
    net_balance      = total_debit - total_credit
    net_ank          = total_debit_ank - total_credit_ank
    receivable       = net_ank * rate
    interest         = (total_debit * rate) / 30
    avg_days         = (receivable / interest) if interest != 0 else 0.0
    return Summary(
        total_debit=total_debit, total_credit=total_credit,
        total_debit_ank=total_debit_ank, total_credit_ank=total_credit_ank,
        net_balance=net_balance, net_ank=net_ank,
        receivable=receivable, interest=interest, avg_days=avg_days, rate=rate,
    )
