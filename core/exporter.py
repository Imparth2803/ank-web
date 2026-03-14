"""core/exporter.py — Excel output builder. No ANK logic here."""

from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path

import pandas as pd
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter

from core.config import EXCEL_STYLE as S, LedgerType
from core.summarizer import Summary


@dataclass
class ExportMeta:
    company_name:    str
    client_name:     str
    from_date:       str
    to_date:         str
    interest_rate:   float
    ledger_type:     str   = LedgerType.SALE
    debit_days:      int   = 0
    credit_days:     int   = 0
    manual_interest: float = 0.0
    output_filename: str   = ""   # Screen 4 rename


def export(df: pd.DataFrame, summary: Summary, meta: ExportMeta,
           output_path: str | Path) -> Path:
    wb = Workbook()
    ws = wb.active
    ws.title = "ANK Statement"
    _set_column_widths(ws)
    row = 1
    row = _write_title(ws, meta, row)
    row = _write_meta_rows(ws, meta, row)
    row += 1
    row = _write_column_headers(ws, row)
    data_start = row
    row = _write_data_rows(ws, df, row)
    data_end = row - 1
    row = _write_totals(ws, data_start, data_end, row)
    row += 1
    row = _write_summary(ws, summary, meta, row)
    output_path = Path(output_path)
    wb.save(output_path)
    return output_path


def _write_title(ws, meta: ExportMeta, row: int) -> int:
    label = f"{'SALE' if meta.ledger_type == LedgerType.SALE else 'PURCHASE'} — Company: {meta.company_name}"
    cell = ws.cell(row=row, column=1, value=label)
    cell.font      = Font(name=S.header_font_name, bold=True, size=13, color=S.accent_font)
    cell.fill      = PatternFill("solid", start_color=S.accent_fill)
    cell.alignment = Alignment(horizontal="center", vertical="center")
    ws.row_dimensions[row].height = 22
    ws.merge_cells(start_row=row, start_column=1, end_row=row, end_column=8)
    return row + 1


def _write_meta_rows(ws, meta: ExportMeta, row: int) -> int:
    for col, (label, value) in enumerate([
        ("From Date:", meta.from_date), ("To Date:", meta.to_date),
        ("INTEREST:", f"{meta.interest_rate}%"),
        ("DR DAYS:", str(meta.debit_days) if meta.debit_days else "-"),
    ], start=1):
        _bold_cell(ws, row, (col - 1) * 2 + 1, label)
        _plain_cell(ws, row, (col - 1) * 2 + 2, value)
    row += 1
    _bold_cell(ws, row, 1, "Client Name:")
    _plain_cell(ws, row, 2, meta.client_name)
    _bold_cell(ws, row, 5, "CR DAYS:")
    _plain_cell(ws, row, 6, str(meta.credit_days) if meta.credit_days else "-")
    if meta.manual_interest:
        _bold_cell(ws, row, 7, "Manual Int.:")
        _plain_cell(ws, row, 8, f"{meta.manual_interest:.2f}")
    return row + 1


def _write_column_headers(ws, row: int) -> int:
    for col, h in enumerate(
        ["DATE", "BILL NO.", "DEBIT AMT.", "DAYS", "ANK AMT.", "CREDIT AMT.", "DAYS", "ANK AMT."], 1
    ):
        cell = ws.cell(row=row, column=col, value=h)
        cell.font      = Font(name=S.header_font_name, bold=True, color=S.header_font, size=10)
        cell.fill      = PatternFill("solid", start_color=S.header_fill)
        cell.alignment = Alignment(horizontal="center")
        cell.border    = Border(bottom=Side(style="thin", color="CCCCCC"))
    ws.row_dimensions[row].height = 18
    return row + 1


def _write_data_rows(ws, df: pd.DataFrame, row: int) -> int:
    for _, r in df.iterrows():
        ws.cell(row=row, column=1, value=r["date"].strftime("%d-%m-%Y") if pd.notna(r["date"]) else "")
        ws.cell(row=row, column=2, value=r["bill_no"])
        if r["debit"] > 0:
            _num_cell(ws, row, 3, r["debit"], color=S.debit_font)
            _plain_cell(ws, row, 4, f"{int(r['debit_days_col'])} Days")
            _num_cell(ws, row, 5, round(r["debit_ank"]))
        if r["credit"] > 0:
            _num_cell(ws, row, 6, r["credit"], color=S.credit_font)
            _plain_cell(ws, row, 7, f"{int(r['credit_days_col'])} Days")
            _num_cell(ws, row, 8, round(r["credit_ank"]))
        row += 1
    return row


def _write_totals(ws, data_start: int, data_end: int, row: int) -> int:
    fill = PatternFill("solid", start_color=S.total_fill)
    ws.cell(row=row, column=1, value="TOTAL:").font = Font(name=S.body_font_name, bold=True)
    ws.cell(row=row, column=1).fill = fill
    for col, letter, color in [(3,"C",S.debit_font),(5,"E",S.debit_font),(6,"F",S.credit_font),(8,"H",S.credit_font)]:
        c = ws.cell(row=row, column=col, value=f"=SUM({letter}{data_start}:{letter}{data_end})")
        c.font = Font(name=S.body_font_name, bold=True, color=color)
        c.fill = fill
        c.number_format = "#,##0"
    for col in (2, 4, 7):
        ws.cell(row=row, column=col).fill = fill
    return row + 1


def _write_summary(ws, s: Summary, meta: ExportMeta, row: int) -> int:
    tot_row = row - 2
    rate    = meta.interest_rate / 100
    _bold_cell(ws, row, 1, "Net Balance:")
    ws.cell(row=row, column=2, value=f"=C{tot_row}-F{tot_row}").number_format = "#,##0.00"
    _bold_cell(ws, row, 4, "Total ANK:")
    ws.cell(row=row, column=5, value=f"=E{tot_row}-H{tot_row}").number_format = "#,##0.00"
    row += 2
    _bold_cell(ws, row, 1, "Interest")
    if meta.manual_interest:
        c = ws.cell(row=row, column=3, value=meta.manual_interest)
        c.font = Font(name=S.body_font_name, color="0000FF")
    else:
        c = ws.cell(row=row, column=3, value=f"=(C{tot_row}*{rate})/30")
    c.number_format = "#,##0.00"
    int_ref = f"C{row}"
    row += 1
    _bold_cell(ws, row, 1, "Receivable:")
    ws.cell(row=row, column=3, value=f"=(E{tot_row}-H{tot_row})*{rate}").number_format = "#,##0.00"
    rec_ref = f"C{row}"
    row += 1
    _bold_cell(ws, row, 1, "Average Days:")
    c = ws.cell(row=row, column=3, value=f"=IFERROR({rec_ref}/{int_ref},0)")
    c.number_format = "#,##0.00"
    c.font = Font(name=S.body_font_name, bold=True)
    return row + 1


def _set_column_widths(ws) -> None:
    for i, w in enumerate([13, 10, 13, 11, 13, 13, 11, 13], 1):
        ws.column_dimensions[get_column_letter(i)].width = w

def _bold_cell(ws, row, col, value):
    c = ws.cell(row=row, column=col, value=value)
    c.font = Font(name=S.body_font_name, bold=True)
    return c

def _plain_cell(ws, row, col, value):
    c = ws.cell(row=row, column=col, value=value)
    c.font = Font(name=S.body_font_name)
    return c

def _num_cell(ws, row, col, value, color: str = "000000"):
    c = ws.cell(row=row, column=col, value=value)
    c.font = Font(name=S.body_font_name, color=color)
    c.number_format = "#,##0"
    c.alignment = Alignment(horizontal="right")
    return c
