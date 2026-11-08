"""
auth.py - login website (role: admin / user)

* Password: PBKDF2-SHA256 (260,000 iterasaun + salt aleatóriu) iha tbl_users - nunka plain text.
* Konta dahuluk kria dala ida husi .env (PP_ADMIN_USERNAME/PASSWORD, PP_USER_USERNAME/PASSWORD)
  bainhira tbl_users mamuk; depois admin jere konta iha pájina "Jestaun Uzuáriu".
* Sesaun: token aleatóriu iha cookie `pp_session`, iha MySQL de'it SHA-256 husi token (tbl_user_sessions),
  entaun login la lakon bainhira pájina reload (ex. muda Night mode).
"""
from __future__ import annotations

import hashlib
import hmac
import secrets
from datetime import datetime, timedelta

import streamlit as st
from sqlalchemy import delete, func, insert, select, update

import config
from common import db

ROLES = {"admin": "Admin", "user": "User"}
COOKIE = "pp_session"
GUEST_COOKIE = "pp_guest"          # public website only (config.PUBLIC_GUEST)
GUEST = {"id": 0, "username": "konvidadu", "full_name": "Konvidadu", "role": "guest"}
SESSION_HOURS = 8
ITERATIONS = 260_000
MIN_PASSWORD = 8


# --------------------------------------------------------------------------- passwords
def hash_password(password: str) -> str:
    salt = secrets.token_hex(16)
    digest = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), bytes.fromhex(salt), ITERATIONS).hex()
    return f"pbkdf2_sha256${ITERATIONS}${salt}${digest}"


def verify_password(password: str, stored: str) -> bool:
    try:
        _, iterations, salt, digest = stored.split("$")
        calc = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), bytes.fromhex(salt), int(iterations)).hex()
    except (ValueError, TypeError):
        return False
    return hmac.compare_digest(calc, digest)


def _token_hash(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


# --------------------------------------------------------------------------- setup
@st.cache_resource
def ensure_tables() -> bool:
    """Create the account tables (if missing) and the first accounts from .env."""
    eng = db.get_engine()
    db.users.create(eng, checkfirst=True)
    db.user_sessions.create(eng, checkfirst=True)
    seed_from_env()
    return True


def seed_from_env() -> list[str]:
    created = []
    with db.begin() as conn:
        if conn.execute(select(func.count()).select_from(db.users)).scalar():
            return created
        for role, ukey, pkey, name in (("admin", "PP_ADMIN_USERNAME", "PP_ADMIN_PASSWORD", "Administrador"),
                                       ("user", "PP_USER_USERNAME", "PP_USER_PASSWORD", "Operador")):
            username, password = (config.env(ukey) or "").strip(), config.env(pkey) or ""
            if username and len(password) >= MIN_PASSWORD:
                conn.execute(insert(db.users).values(username=username, full_name=name, role=role,
                                                     password_hash=hash_password(password), is_active=True))
                created.append(username)
    return created


def has_accounts() -> bool:
    ensure_tables()
    with db.begin() as conn:
        return bool(conn.execute(select(func.count()).select_from(db.users)).scalar())


# --------------------------------------------------------------------------- session
def _public(row) -> dict:
    return {"id": int(row.id), "username": row.username, "full_name": row.full_name or row.username,
            "role": row.role}


def current_user() -> dict | None:
    """User of this browser session: session_state first, then the cookie token (after a reload)."""
    ensure_tables()
    if st.session_state.get("user"):
        return st.session_state["user"]
    if (config.PUBLIC_GUEST and st.context.cookies.get(GUEST_COOKIE) == "1"
            and not st.session_state.get("_guest_logged_out")):   # cookie is only re-read on reload
        st.session_state["user"] = dict(GUEST)
        return st.session_state["user"]
    token = st.context.cookies.get(COOKIE)
    if not token or config.PUBLIC_NO_LOGIN:
        return None
    with db.begin() as conn:
        row = conn.execute(
            select(db.users).join(db.user_sessions, db.user_sessions.c.user_id == db.users.c.id)
            .where(db.user_sessions.c.token_hash == _token_hash(token),
                   db.user_sessions.c.expires_at > datetime.now(), db.users.c.is_active.is_(True))
        ).first()
    if not row:
        return None
    st.session_state["user"], st.session_state["token"] = _public(row), token
    return st.session_state["user"]


def login(username: str, password: str, portal: str) -> tuple[dict | None, str]:
    """portal = 'admin' or 'user'. Returns (user, error message)."""
    if config.PUBLIC_NO_LOGIN:
        return None, "Login la ativu iha versaun públiku - uza 'Tama hanesan konvidadu'."
    with db.begin() as conn:
        row = conn.execute(select(db.users).where(db.users.c.username == username.strip())).first()
        if not row or not verify_password(password, row.password_hash):
            return None, "Username ka password la loos."
        if not row.is_active:
            return None, "Konta ne'e la ativu - kontaktu admin."
        if row.role != portal:
            return None, (f"Konta ne'e mak {ROLES[row.role]} - uza tab {ROLES[row.role]} atu login.")
        token = secrets.token_urlsafe(32)
        conn.execute(insert(db.user_sessions).values(token_hash=_token_hash(token), user_id=row.id,
                                                     expires_at=datetime.now() + timedelta(hours=SESSION_HOURS)))
        conn.execute(update(db.users).where(db.users.c.id == row.id).values(last_login=datetime.now()))
        conn.execute(delete(db.user_sessions).where(db.user_sessions.c.expires_at < datetime.now()))
    user = _public(row)
    st.session_state["user"], st.session_state["token"] = user, token
    st.session_state["_set_cookie"] = token
    return user, ""


def guest_login() -> None:
    """Read-only visitor of the public website: no account, no database session."""
    if not config.PUBLIC_GUEST:
        return
    st.session_state["user"] = dict(GUEST)
    st.session_state["_set_guest_cookie"] = "1"
    st.session_state.pop("_guest_logged_out", None)


def is_guest(user: dict | None = None) -> bool:
    user = user or st.session_state.get("user")
    return bool(user) and user.get("role") == "guest"


def logout() -> None:
    if is_guest():
        st.session_state.pop("user", None)
        st.session_state["_set_guest_cookie"] = ""
        st.session_state["_guest_logged_out"] = True
        return
    token = st.session_state.get("token") or st.context.cookies.get(COOKIE)
    if token:
        with db.begin() as conn:
            conn.execute(delete(db.user_sessions).where(db.user_sessions.c.token_hash == _token_hash(token)))
    for k in ("user", "token"):
        st.session_state.pop(k, None)
    st.session_state["_set_cookie"] = ""          # clear the cookie on the next run


def cookie_script() -> None:
    """Write / clear the session cookie (runs once after login or logout)."""
    if "_set_guest_cookie" in st.session_state:
        g = st.session_state.pop("_set_guest_cookie")
        st.html(f"<script>document.cookie = '{GUEST_COOKIE}={g}; path=/; max-age={86400 if g else 0}; SameSite=Strict';</script>",
                unsafe_allow_javascript=True, width="content")
    if "_set_cookie" not in st.session_state:
        return
    token = st.session_state.pop("_set_cookie")
    age = SESSION_HOURS * 3600 if token else 0
    st.html(f"<script>document.cookie = '{COOKIE}={token}; path=/; max-age={age}; SameSite=Strict';</script>",
            unsafe_allow_javascript=True, width="content")


def is_admin(user: dict | None = None) -> bool:
    user = user or st.session_state.get("user")
    return bool(user) and user.get("role") == "admin"


def require_admin() -> None:
    if not is_admin():
        st.error("Pájina ne'e ba Admin de'it.", icon=":material/lock:")
        st.stop()


# --------------------------------------------------------------------------- user management (admin)
def list_users():
    import pandas as pd
    with db.begin() as conn:
        rows = conn.execute(select(db.users.c.id, db.users.c.username, db.users.c.full_name, db.users.c.role,
                                   db.users.c.is_active, db.users.c.created_at, db.users.c.last_login)
                            .order_by(db.users.c.role, db.users.c.username)).fetchall()
    return pd.DataFrame(rows, columns=["id", "username", "full_name", "role", "is_active", "created_at", "last_login"])


def create_user(username: str, full_name: str, role: str, password: str) -> str:
    username = username.strip()
    if not username or len(username) > 40 or not username.replace("_", "").replace(".", "").isalnum():
        return "Username: letra, númeru, '.' ka '_' de'it (máximu 40)."
    if role not in ROLES:
        return "Role la loos."
    if len(password) < MIN_PASSWORD:
        return f"Password tenke iha karakter {MIN_PASSWORD} ka liu."
    with db.begin() as conn:
        if conn.execute(select(db.users.c.id).where(db.users.c.username == username)).first():
            return "Username ne'e iha ona."
        conn.execute(insert(db.users).values(username=username, full_name=full_name.strip() or username, role=role,
                                             password_hash=hash_password(password), is_active=True))
    return ""


def update_user(user_id: int, *, role: str | None = None, is_active: bool | None = None,
                password: str | None = None) -> str:
    values = {}
    if role is not None:
        values["role"] = role
    if is_active is not None:
        values["is_active"] = is_active
    if password is not None:
        if len(password) < MIN_PASSWORD:
            return f"Password tenke iha karakter {MIN_PASSWORD} ka liu."
        values["password_hash"] = hash_password(password)
    with db.begin() as conn:
        conn.execute(update(db.users).where(db.users.c.id == user_id).values(**values))
        if is_active is False or password is not None:          # force a new login
            conn.execute(delete(db.user_sessions).where(db.user_sessions.c.user_id == user_id))
    return ""


def delete_user(user_id: int) -> None:
    with db.begin() as conn:
        conn.execute(delete(db.user_sessions).where(db.user_sessions.c.user_id == user_id))
        conn.execute(delete(db.users).where(db.users.c.id == user_id))
