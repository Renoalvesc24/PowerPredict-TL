"""
Set a new password for the admin account(s) on the server (run once after the database is seeded).
The password is read from standard input (or NEW_ADMIN_PASSWORD), never from the command line:

    docker compose exec -T dashboard python deploy/set_admin_password.py < password_file
"""
import os
import sys
from pathlib import Path

sys.path[:0] = [str(Path(__file__).resolve().parents[1]), str(Path(__file__).resolve().parents[1] / "dashboard")]

from sqlalchemy import delete, select, update  # noqa: E402

import auth  # noqa: E402
from common import db  # noqa: E402

password = os.environ.get("NEW_ADMIN_PASSWORD") or sys.stdin.readline().strip()
if len(password) < 12:
    sys.exit("new admin password missing or shorter than 12 characters")
with db.begin() as conn:
    ids = [r.id for r in conn.execute(select(db.users.c.id).where(db.users.c.role == "admin"))]
    for uid in ids:
        conn.execute(update(db.users).where(db.users.c.id == uid).values(password_hash=auth.hash_password(password)))
        conn.execute(delete(db.user_sessions).where(db.user_sessions.c.user_id == uid))
print(f"admin password changed for {len(ids)} account(s)")
