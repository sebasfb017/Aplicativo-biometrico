import bcrypt
import streamlit as st
from database_conn.connection import db_conn

def get_user(username: str):
    """Obtiene los datos de un usuario desde la base de datos."""
    conn = db_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT username, full_name, role, password_hash, active, managed_department
        FROM users_app WHERE username = ?
    """, (username,))
    row = cur.fetchone()
    conn.close()
    return row

def verify_login(username: str, password: str):
    """Verifica las credenciales contra el hash guardado."""
    row = get_user(username)
    if not row:
        return None
    _username, full_name, role, pw_hash, active, managed_dept = row
    if active != 1:
        return None
    if bcrypt.checkpw(password.encode("utf-8"), pw_hash):
        return {"username": _username, "full_name": full_name, "role": role, "managed_department": managed_dept}
    return None

def require_role(*allowed_roles):
    """Bloquea el acceso a vistas si el usuario no tiene el rol necesario."""
    user = st.session_state.get("user")
    if not user or user.get("role") not in allowed_roles:
        st.error("No tienes permisos para ver esta sección.")
        st.stop()