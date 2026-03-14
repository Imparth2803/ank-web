"""core/config.py — Central configuration."""

from dataclasses import dataclass
from typing import List
from pathlib import Path

# ---------------------------------------------------------------------------
# ANK Formula
# ---------------------------------------------------------------------------
ANK_DIVISOR: int = 30

# ---------------------------------------------------------------------------
# Application Defaults
# ---------------------------------------------------------------------------
DEFAULT_FROM_DATE: str       = "01-04-2024"
DEFAULT_TO_DATE: str         = "31-03-2025"
DEFAULT_INTEREST_RATE: float = 1.5

# ---------------------------------------------------------------------------
# Ledger Type
# ---------------------------------------------------------------------------
class LedgerType:
    SALE     = "sale"
    PURCHASE = "purchase"

# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------
DB_ENGINE: str = "sqlite"          # "sqlite" | "mysql"
DB_PATH: Path  = Path.home() / "ANK_Generator" / "ank_data.db"

# MySQL (fill when migrating to Railway / Utho)
DB_HOST:     str = "localhost"
DB_PORT:     int = 3306
DB_NAME:     str = "ank_generator"
DB_USER:     str = "root"
DB_PASSWORD: str = ""

# ---------------------------------------------------------------------------
# Auth / Admin
# ---------------------------------------------------------------------------
ADMIN_PASSWORD: str = "admin@ank2024"   # Change before distribution
# Invite codes: how many days until they expire (0 = never)
INVITE_CODE_EXPIRY_DAYS: int = 30

# ---------------------------------------------------------------------------
# Column Aliases
# ---------------------------------------------------------------------------
COLUMN_ALIASES: dict[str, List[str]] = {
    "date":    ["DATE", "Date", "date", "Trans Date", "TRANS DATE"],
    "bill_no": ["BILL NO.", "BILL NO", "Bill No.", "Bill No", "Invoice No", "INVOICE NO"],
    "debit":   ["DEBIT AMT.", "DEBIT AMT", "Debit Amt", "Debit", "DR AMT", "DR"],
    "credit":  ["CREDIT AMT", "CREDIT AMT.", "Credit Amt", "Credit", "CR AMT", "CR"],
}

# ---------------------------------------------------------------------------
# Theme — Cream palette
# ---------------------------------------------------------------------------
class Theme:
    BG           = "#F5F0E8"   # cream background
    BG_DARK      = "#EDE8DF"   # slightly darker cream
    CARD         = "#FFFDF9"   # card surface
    NAVY         = "#1A1A2E"   # dark header
    ACCENT       = "#C8392B"   # primary red
    ACCENT_HOVER = "#A82D22"
    BLUE         = "#1E3A5F"
    BLUE_HOVER   = "#2A4F80"
    TEXT         = "#1A1A2E"
    TEXT_MUTED   = "#8A7F72"
    TEXT_LIGHT   = "#B0A898"
    SUCCESS      = "#2D7D46"
    WARNING      = "#E8A020"
    BORDER       = "#D4CFC6"
    SIDEBAR_BG   = "#1A1A2E"
    SIDEBAR_SEL  = "#2A2A4E"

# ---------------------------------------------------------------------------
# Excel Styling
# ---------------------------------------------------------------------------
@dataclass
class ExcelStyle:
    header_fill:      str = "1A1A2E"
    header_font:      str = "F5F0E8"
    total_fill:       str = "F0ECE4"
    debit_font:       str = "C8392B"
    credit_font:      str = "2D7D46"
    accent_fill:      str = "C8392B"
    accent_font:      str = "FFFFFF"
    body_font_name:   str = "Arial"
    header_font_name: str = "Arial"

EXCEL_STYLE = ExcelStyle()

# ---------------------------------------------------------------------------
# UI
# ---------------------------------------------------------------------------
@dataclass
class UIConfig:
    app_title:     str = "ANK Statement Generator"
    window_width:  int = 1100
    window_height: int = 720
    sidebar_width: int = 240
    theme:         str = "light"   # cream = light mode
    color_theme:   str = "blue"

UI_CONFIG = UIConfig()

DEFAULT_EXPORT_DIR: Path = Path.home() / "ANK_Generator" / "exports"

# ---------------------------------------------------------------------------
# Runtime overrides from environment variables (Railway / production)
# ---------------------------------------------------------------------------
import os as _os

_env_admin = _os.getenv("ADMIN_PASSWORD")
if _env_admin:
    ADMIN_PASSWORD = _env_admin

_env_db = _os.getenv("DB_PATH")
if _env_db:
    DB_PATH = Path(_env_db)
