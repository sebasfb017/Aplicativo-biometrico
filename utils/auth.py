import bcrypt
import streamlit as st
import re # Importar el módulo re para expresiones regulares
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

def validate_password(password: str):
    """
    Valida que la contraseña cumpla con políticas de seguridad robustas.
    Retorna (True, "") si es válida, o (False, "Mensaje de error") si no lo es.
    """
    if len(password) < 8:
        return False, "La contraseña debe tener al menos 8 caracteres."
    if not re.search(r"[A-Z]", password):
        return False, "La contraseña debe contener al menos una letra mayúscula."
    if not re.search(r"[a-z]", password):
        return False, "La contraseña debe contener al menos una letra minúscula."
    if not re.search(r"\d", password):
        return False, "La contraseña debe contener al menos un número."
    if not re.search(r"[!@#$%^&*()_+\-=\[\]{}|;:'\",.<>/?]", password):
        return False, "La contraseña debe contener al menos un carácter especial."
    return True, ""
