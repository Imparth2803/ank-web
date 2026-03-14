"""
db/auth.py — User auth, invite code, and password operations.
Uses hashlib.pbkdf2_hmac (Python stdlib) — no extra install needed.
Drop-in compatible with bcrypt when migrating to cloud.
"""

from __future__ import annotations
import hashlib, secrets, string
from datetime import datetime, timedelta
from dataclasses import dataclass
from db.connection import get_connection
from core.config import INVITE_CODE_EXPIRY_DAYS


# ---------------------------------------------------------------------------
# Password hashing (PBKDF2-SHA256, 260k iterations — NIST recommended)
# ---------------------------------------------------------------------------

def hash_password(password: str) -> str:
    salt = secrets.token_hex(32)
    key  = hashlib.pbkdf2_hmac("sha256", password.encode(), salt.encode(), 260_000)
    return f"pbkdf2:{salt}:{key.hex()}"


def verify_password(password: str, stored_hash: str) -> bool:
    try:
        _, salt, key_hex = stored_hash.split(":")
        key = hashlib.pbkdf2_hmac("sha256", password.encode(), salt.encode(), 260_000)
        return secrets.compare_digest(key.hex(), key_hex)
    except Exception:
        return False


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class User:
    id:           int
    full_name:    str
    email:        str
    company_name: str
    is_active:    bool
    tc_accepted:  bool
    created_at:   str


# ---------------------------------------------------------------------------
# User operations
# ---------------------------------------------------------------------------

def register_user(
    full_name: str, email: str, password: str,
    company_name: str, invite_code: str
) -> User:
    """
    Register a new user after validating invite code.
    Raises ValueError with user-facing message on any failure.
    """
    email = email.strip().lower()
    code  = invite_code.strip().upper()

    conn = get_connection()
    try:
        # 1. Validate invite code
        row = conn.execute(
            "SELECT id, expires_at, is_used FROM invite_codes WHERE code = ?", (code,)
        ).fetchone()

        if not row:
            raise ValueError("Invalid invite code. Please check with your administrator.")
        if row["is_used"]:
            raise ValueError("This invite code has already been used.")
        if row["expires_at"]:
            exp = datetime.fromisoformat(row["expires_at"])
            if datetime.now() > exp:
                raise ValueError("This invite code has expired. Please request a new one.")

        # 2. Check email not already registered
        existing = conn.execute(
            "SELECT id FROM users WHERE email = ?", (email,)
        ).fetchone()
        if existing:
            raise ValueError("An account with this email already exists.")

        # 3. Create user
        pw_hash = hash_password(password)
        now     = datetime.now().isoformat()
        cursor  = conn.execute(
            """INSERT INTO users (full_name, email, password_hash, company_name,
               tc_accepted, tc_accepted_at)
               VALUES (?,?,?,?,1,?)""",
            (full_name.strip(), email, pw_hash, company_name.strip(), now)
        )
        user_id = cursor.lastrowid

        # 4. Mark invite code as used
        conn.execute(
            "UPDATE invite_codes SET is_used=1, used_by=?, used_at=? WHERE id=?",
            (user_id, now, row["id"])
        )
        conn.commit()

        return User(id=user_id, full_name=full_name.strip(), email=email,
                    company_name=company_name.strip(), is_active=True,
                    tc_accepted=True, created_at=now)
    except ValueError:
        conn.rollback()
        raise
    except Exception as e:
        conn.rollback()
        raise ValueError(f"Registration failed: {e}")
    finally:
        conn.close()


def login_user(email: str, password: str) -> User:
    """Authenticate user. Raises ValueError on failure."""
    email = email.strip().lower()
    conn  = get_connection()
    try:
        row = conn.execute(
            """SELECT id, full_name, email, password_hash, company_name,
                      is_active, tc_accepted, created_at
               FROM users WHERE email = ?""", (email,)
        ).fetchone()

        if not row:
            raise ValueError("No account found with this email.")
        if not verify_password(password, row["password_hash"]):
            raise ValueError("Incorrect password.")
        if not row["is_active"]:
            raise ValueError("Your account has been suspended. Contact support.")

        return User(
            id=row["id"], full_name=row["full_name"], email=row["email"],
            company_name=row["company_name"], is_active=bool(row["is_active"]),
            tc_accepted=bool(row["tc_accepted"]), created_at=row["created_at"],
        )
    finally:
        conn.close()


def get_all_users() -> list[User]:
    conn = get_connection()
    try:
        rows = conn.execute(
            "SELECT id,full_name,email,company_name,is_active,tc_accepted,created_at FROM users ORDER BY created_at DESC"
        ).fetchall()
        return [User(id=r["id"], full_name=r["full_name"], email=r["email"],
                     company_name=r["company_name"], is_active=bool(r["is_active"]),
                     tc_accepted=bool(r["tc_accepted"]), created_at=r["created_at"])
                for r in rows]
    finally:
        conn.close()


def set_user_active(user_id: int, active: bool) -> None:
    conn = get_connection()
    try:
        conn.execute("UPDATE users SET is_active=? WHERE id=?", (int(active), user_id))
        conn.commit()
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Invite code operations
# ---------------------------------------------------------------------------

def generate_invite_code() -> str:
    """Generate a unique 12-char alphanumeric invite code."""
    alphabet = string.ascii_uppercase + string.digits
    conn = get_connection()
    try:
        for _ in range(10):
            code = "".join(secrets.choice(alphabet) for _ in range(12))
            # Format as XXXX-XXXX-XXXX for readability
            code = f"{code[:4]}-{code[4:8]}-{code[8:]}"
            exists = conn.execute(
                "SELECT id FROM invite_codes WHERE code=?", (code,)
            ).fetchone()
            if not exists:
                expires = None
                if INVITE_CODE_EXPIRY_DAYS:
                    expires = (datetime.now() + timedelta(days=INVITE_CODE_EXPIRY_DAYS)).isoformat()
                conn.execute(
                    "INSERT INTO invite_codes (code, expires_at) VALUES (?,?)",
                    (code, expires)
                )
                conn.commit()
                return code
        raise RuntimeError("Could not generate unique code.")
    finally:
        conn.close()


def get_all_invite_codes() -> list[dict]:
    conn = get_connection()
    try:
        rows = conn.execute(
            """SELECT ic.id, ic.code, ic.created_at, ic.expires_at,
                      ic.is_used, ic.used_at, u.full_name as used_by_name
               FROM invite_codes ic
               LEFT JOIN users u ON u.id = ic.used_by
               ORDER BY ic.created_at DESC"""
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def delete_invite_code(code_id: int) -> None:
    conn = get_connection()
    try:
        conn.execute("DELETE FROM invite_codes WHERE id=? AND is_used=0", (code_id,))
        conn.commit()
    finally:
        conn.close()
