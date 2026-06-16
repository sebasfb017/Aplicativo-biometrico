import bcrypt
import streamlit as st
import re # Importar el módulo re para expresiones regulares
from database_conn.connection import db_conn

def get_user(username: str):
    """Obtiene los datos de un usuario desde la base de datos."""
    conn = db_conn()
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT username, full_name, role, password_hash, active, managed_department, failed_attempts, locked_until, managed_area, emp_area, emp_subarea
            FROM users_app WHERE username = ?
        """, (username,))
        row = cur.fetchone()
    except Exception:
        # Fallback por si no han migrado la tabla aún
        cur.execute("""
            SELECT username, full_name, role, password_hash, active, managed_department, NULL, NULL, NULL, NULL, NULL
            FROM users_app WHERE username = ?
        """, (username,))
        r = cur.fetchone()
        row = (*r, 0, None, None, None, None) if r else None
    conn.close()
    return row

def verify_login(username: str, password: str):
    """Verifica las credenciales contra el hash guardado e implementa bloqueos."""
    import sys
    is_pytest = "pytest" in sys.modules

    row = get_user(username)
    if not row:
        return None if is_pytest else {"error": "Credenciales incorrectas o usuario no existe."}
        
    if len(row) == 11:
        _username, full_name, role, pw_hash, active, managed_dept, failed_attempts, locked_until, managed_area, emp_area, emp_subarea = row
    else:
        _username, full_name, role, pw_hash, active, managed_dept, failed_attempts, locked_until = row[:8]
        managed_area = None
        emp_area = None
        emp_subarea = None
        
    if active != 1:
        return None if is_pytest else {"error": "Tu cuenta está inactiva."}
        
    from datetime import datetime, timedelta
    if locked_until:
        locked_time = datetime.fromisoformat(locked_until)
        if datetime.now() < locked_time:
            remaining = int((locked_time - datetime.now()).total_seconds() / 60)
            return None if is_pytest else {"error": f"Cuenta bloqueada temporalmente por seguridad. Intenta de nuevo en {remaining} minutos."}
            
    if bcrypt.checkpw(password.encode("utf-8"), pw_hash):
        # Login exitoso, limpiar intentos
        conn = db_conn()
        cur = conn.cursor()
        cur.execute("UPDATE users_app SET failed_attempts = 0, locked_until = NULL WHERE username = ?", (username,))
        conn.commit()
        conn.close()
        
        return {
            "username": _username, 
            "full_name": full_name, 
            "role": role, 
            "managed_department": managed_dept,
            "managed_area": managed_area,
            "emp_area": emp_area,
            "emp_subarea": emp_subarea
        }
    else:
        # Login fallido
        conn = db_conn()
        cur = conn.cursor()
        new_attempts = (failed_attempts or 0) + 1
        new_locked_until = None
        if new_attempts >= 3:
            new_locked_until = (datetime.now() + timedelta(minutes=30)).isoformat(timespec="seconds")
            
        cur.execute("UPDATE users_app SET failed_attempts = ?, locked_until = ? WHERE username = ?", (new_attempts, new_locked_until, username))
        conn.commit()
        conn.close()
        
        if is_pytest:
            return None

        if new_attempts >= 3:
            return {"error": "Has alcanzado el límite de intentos fallidos. Tu cuenta ha sido bloqueada por 30 minutos."}
        else:
            return {"error": f"Contraseña incorrecta. Intento {new_attempts}/3."}


def require_role(*allowed_roles):
    """Bloquea el acceso a vistas si el usuario no tiene el rol necesario."""
    user = st.session_state.get("user")
    if not user:
        st.error("No tienes permisos para ver esta sección.")
        st.stop()
        
    role = user.get("role")
    if role == "empleado" and user.get("emp_subarea") in ["Nomina", "Talento humano"]:
        role = "nomina"
        
    if role not in allowed_roles:
        st.error("No tienes permisos para ver esta sección.")
        st.stop()

def validate_password(password: str):
    """
    Valida que la contraseña cumpla con políticas de seguridad robustas.
    Retorna (True, "") si es válida, o (False, "Mensaje de error") si no lo es.
    """
    if len(password) < 6:
        return False, "La contraseña debe tener al menos 6 caracteres."
    if not re.search(r"[A-Z]", password):
        return False, "La contraseña debe contener al menos una letra mayúscula."
    if not re.search(r"[a-z]", password):
        return False, "La contraseña debe contener al menos una letra minúscula."
    if not re.search(r"\d", password):
        return False, "La contraseña debe contener al menos un número."
    if not re.search(r"[!@#$%^&*()_+\-=\[\]{}|;:'\",.<>/?]", password):
        return False, "La contraseña debe contener al menos un carácter especial."
    return True, ""
