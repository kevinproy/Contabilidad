from __future__ import annotations

from typing import Any, Dict, List, Optional

from flask import Blueprint, render_template, request, redirect, url_for, flash, session
from werkzeug.security import generate_password_hash, check_password_hash
from typing import Any, Dict, List
from services.db import db_connect, ensure_db_schema, seed_permissions_and_master


bp = Blueprint("auth", __name__)


@bp.get("/login")
def login():
    if session.get("user"):
        return redirect(url_for("index"))
    return render_template("login.html")


@bp.post("/login")
def login_post():
    username = request.form.get("username", "").strip()
    password = request.form.get("password", "")
    # Consultar en DB
    with db_connect() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id_user, username, password_hash, is_master, is_active FROM users WHERE username=%s", (username,))
            row = cur.fetchone()
    if not row or not check_password_hash(row.get("password_hash") if isinstance(row, dict) else row[2], password):
        flash("Usuario o contraseña inválidos.", "error")
        return redirect(url_for("auth.login"))
    is_master = bool(row.get("is_master") if isinstance(row, dict) else row[3])
    user_id = int(row.get("id_user") if isinstance(row, dict) else row[0])
    # cargar permisos
    with db_connect() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT perm_code FROM user_permissions WHERE id_user=%s", (user_id,))
            perms_rows = cur.fetchall() or []
    perms = [r.get("perm_code") if isinstance(r, dict) else r[0] for r in perms_rows]
    session["user"] = {"id": user_id, "username": username, "is_master": is_master, "perms": perms}
    flash(f"Bienvenido, {username}.", "success")
    return redirect(url_for("index"))


@bp.get("/logout")
def logout():
    session.pop("user", None)
    flash("Sesión cerrada.", "info")
    return redirect(url_for("auth.login"))


def init_auth(app):
    ensure_db_schema()
    seed_permissions_and_master()


