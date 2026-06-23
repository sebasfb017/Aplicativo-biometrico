import pandas as pd
import streamlit as st
from datetime import datetime, date
from database_conn.connection import db_conn, db_session

# --- GESTIÓN DE USUARIOS (Corrección de Errores y Consultas) ---

# CACHE: Carga infinita, se limpia manualmente al haber cambios.
@st.cache_data(show_spinner=False)
def get_users_by_role(roles_list):
    """Obtiene usuarios filtrados por una lista de roles para las tablas de administración."""
    conn = db_conn()
    roles_placeholders = ','.join(['?'] * len(roles_list))
    
    # Si solo buscamos empleados, usamos una consulta optimizada para esa vista
    if 'empleado' in roles_list and len(roles_list) == 1:
        query = """
            SELECT username, full_name, emp_area, emp_subarea, active, created_at 
            FROM users_app WHERE role = 'empleado' ORDER BY username
        """
        df = pd.read_sql_query(query, conn)
    else:
        query = f"""
            SELECT username, full_name, role, managed_department, managed_area, active, created_at 
            FROM users_app 
            WHERE role IN ({roles_placeholders}) 
            ORDER BY username
        """
        df = pd.read_sql_query(query, conn, params=roles_list)
    
    conn.close()
    return df

# CACHE: Hace que los selectores de pantalla (ej. listado de empleados) carguen al instante sin re-consultar a la BD.
@st.cache_data(show_spinner=False)
def get_all_employees():
    """Obtiene el listado maestro de empleados para selectores y diálogos."""
    conn = db_conn()
    df = pd.read_sql_query("SELECT user_id, full_name, department FROM employees ORDER BY full_name", conn)
    conn.close()
    return df


# --- GESTIÓN DE EMPLEADOS Y PERFILES ---

def upsert_employees_df(df: pd.DataFrame):
    """Carga o actualiza empleados desde un DataFrame (CSV o Manual)."""
    with db_session() as conn:
        cur = conn.cursor()
        for _, r in df.iterrows():
            profile_id = None
            if "profile_id" in df.columns and r.get("profile_id"):
                profile_val = r["profile_id"]
                if isinstance(profile_val, str):
                    cur.execute("SELECT profile_id FROM profiles WHERE name = ?", (profile_val.strip(),))
                    profile_row = cur.fetchone()
                    if profile_row:
                        profile_id = profile_row[0]
                else:
                    profile_id = int(profile_val)
            
            cur.execute("""
                INSERT INTO employees(user_id, full_name, email, department, profile_id, created_at)
                VALUES(?,?,?,?,?,?)
                ON CONFLICT(user_id) DO UPDATE SET
                    full_name=excluded.full_name, email=excluded.email,
                    department=excluded.department, profile_id=excluded.profile_id
            """, (r["user_id"], r["full_name"], r.get("email", ""), r.get("department", ""), 
                  profile_id, datetime.now().isoformat(timespec="seconds")))
    get_all_employees.clear()


# --- GESTIÓN DE TURNOS Y ASIGNACIONES ---

def is_holiday(date_obj: date) -> bool:
    """Verifica si una fecha existe en la tabla de festivos."""
    conn = db_conn()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM holidays WHERE date = ?", (date_obj.isoformat(),))
    count = cur.fetchone()[0]
    conn.close()
    return count > 0

# CACHE: Carga inmediata del catálogo general de horarios
@st.cache_data(show_spinner=False)
def get_shifts_df():
    """Retorna el catálogo completo de turnos configurados."""
    conn = db_conn()
    df = pd.read_sql_query("SELECT * FROM shifts ORDER BY name", conn)
    conn.close()
    return df

def upsert_shift(name, start_time, grace_minutes, **kwargs):
    """Crea o actualiza un turno en el catálogo maestro."""
    with db_session() as conn:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO shifts(name, start_time, end_time, grace_minutes, has_break, 
                               break_start, break_end, is_overnight, shift_code, created_at)
            VALUES(?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(name) DO UPDATE SET
                start_time=excluded.start_time, grace_minutes=excluded.grace_minutes,
                shift_code=excluded.shift_code
        """, (name.strip(), start_time.strip(), kwargs.get('end_time', ''), int(grace_minutes),
              1 if kwargs.get('has_break') else 0, kwargs.get('break_start', ''), 
              kwargs.get('break_end', ''), 1 if kwargs.get('is_overnight') else 0,
              kwargs.get('shift_code'), datetime.now().isoformat(timespec="seconds")))
        
        cur.execute("SELECT id FROM shifts WHERE name = ?", (name.strip(),))
        row = cur.fetchone()
        shift_id = row[0] if row else None
    get_shifts_df.clear()
    return shift_id

def assign_shift(user_id, week_start, dow, shift_id):
    """Asigna un turno a un empleado para un día de la semana específico."""
    with db_session() as conn:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO shift_assignments(user_id, week_start, dow, shift_id, created_at)
            VALUES(?,?,?,?,?)
            ON CONFLICT(user_id, week_start, dow) DO UPDATE SET shift_id=excluded.shift_id
        """, (str(user_id), week_start, int(dow), int(shift_id), 
              datetime.now().isoformat(timespec="seconds")))


# --- NOVEDADES Y PERMISOS (F-TH-012) ---

def upsert_exception(user_id, date_str, exc_type, notes):
    """Registra una novedad manual (incapacidad, vacaciones, etc.)."""
    with db_session() as conn:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO exceptions(user_id, date, type, notes, created_at)
            VALUES(?,?,?,?,?)
            ON CONFLICT(user_id, date) DO UPDATE SET type=excluded.type, notes=excluded.notes
        """, (user_id, date_str, exc_type, notes, datetime.now().isoformat(timespec="seconds")))

def get_exceptions_df():
    """Obtiene el histórico de todas las novedades para el visor administrativo."""
    conn = db_conn()
    df = pd.read_sql_query("""
        SELECT ex.id, ex.user_id, e.full_name, ex.date, ex.type, ex.notes, ex.created_at
        FROM exceptions ex
        LEFT JOIN employees e ON ex.user_id = e.user_id
        ORDER BY ex.date DESC
    """, conn)
    conn.close()
    return df

def db_create_leave_request(user_id, leave_start, leave_end, t_start, t_end, total_time, r_type, r_desc, makeup, is_paid, attachment_path=None):
    """Crea una solicitud digital y define el flujo de aprobación inicial."""
    role = st.session_state.get("user", {}).get("role", "empleado")
    
    # Por defecto inicia en el nivel más bajo (Coordinador)
    target_status = "PENDING_COORD"
    
    if r_type == "Incapacidad":
        target_status = "PENDING_RRHH"
    elif role == "coordinador":
        # Salta al siguiente paso: RRHH
        target_status = "PENDING_RRHH"
    elif role in ["admin", "nomina"]:
        # Si radica RRHH, salta al siguiente paso: Jefe de Área
        target_status = "PENDING_JEFE"
    elif role == "jefe_area":
        # Si radica el Jefe, se auto-aprueba (es la última instancia)
        target_status = "APPROVED"

    with db_session() as conn:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO leave_requests (
                user_id, request_date, leave_date_start, leave_date_end, start_time, end_time, 
                total_time, reason_type, reason_description, how_to_makeup, is_paid, created_at, status, attachment_path
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (user_id, datetime.now().date().isoformat(), leave_start.isoformat(), leave_end.isoformat(),
              t_start, t_end, total_time, r_type, r_desc, makeup, 1 if is_paid else 0,
              datetime.now().isoformat(timespec="seconds"), target_status, attachment_path))
        
        req_id = cur.lastrowid
    
    
    # Aquí podríamos notificar al coordinador de su departamento, 
    # pero eso lo haremos en las vistas para poder buscar su email.
    return req_id

def db_approve_leave_request_coord(req_id, coord_username):
    with db_session() as conn:
        cur = conn.cursor()
        cur.execute("""
            UPDATE leave_requests 
            SET status = 'PENDING_RRHH', approved_by_coord = ?, coord_approval_date = ?
            WHERE id = ? AND status = 'PENDING_COORD'
        """, (coord_username, datetime.now().isoformat(timespec="seconds"), req_id))

def db_approve_leave_request_jefe(req_id, jefe_username):
    with db_session() as conn:
        cur = conn.cursor()
        cur.execute("""
            UPDATE leave_requests 
            SET status = 'APPROVED', approved_by_jefe = ?, jefe_approval_date = ?
            WHERE id = ? AND status = 'PENDING_JEFE'
        """, (jefe_username, datetime.now().isoformat(timespec="seconds"), req_id))

def db_approve_leave_request_rrhh(req_id, approver_user, is_final=False):
    with db_session() as conn:
        cur = conn.cursor()
        now = datetime.now().isoformat(timespec="seconds")
        status = 'APPROVED' if is_final else 'PENDING_JEFE'
        cur.execute("""
            UPDATE leave_requests 
            SET status = ?, approved_by_rrhh = ?, rrhh_approval_date = ? 
            WHERE id = ?
        """, (status, approver_user, now, req_id))

def db_reject_leave_request(req_id, rejected_by, rejection_reason):
    with db_session() as conn:
        cur = conn.cursor()
        cur.execute("""
            UPDATE leave_requests 
            SET status = 'REJECTED', rejection_reason = ?
            WHERE id = ?
        """, (rejection_reason, req_id,))
        
        # Registrar el rechazo en la bitácora de auditoría
        cur.execute("""
            INSERT INTO audit_logs (user_id, action, details, timestamp)
            VALUES (?, ?, ?, ?)
        """, (rejected_by, "REJECT_LEAVE", f"Rechazó la solicitud #{req_id}. Motivo: {rejection_reason}", datetime.now().isoformat(timespec="seconds")))

def db_cancel_leave_request(req_id, user_id, reason):
    """
    Cancela una solicitud de permiso por parte del empleado.
    Solo se puede cancelar si no ha sido aprobada o rechazada definitivamente.
    """
    with db_session() as conn:
        cur = conn.cursor()
        
        cur.execute("""
            UPDATE leave_requests 
            SET status = 'CANCELLED', cancellation_reason = ?
            WHERE id = ? AND user_id = ? AND status IN ('PENDING_COORD', 'PENDING_JEFE', 'PENDING_RRHH')
        """, (reason, req_id, user_id))
        
        # Verificar si se actualizó alguna fila (por si el estado ya no era PENDING)
        if cur.rowcount > 0:
            cur.execute("""
                INSERT INTO audit_logs (user_id, action, details, timestamp)
                VALUES (?, ?, ?, ?)
            """, (user_id, "CANCEL_LEAVE", f"El empleado canceló la solicitud #{req_id}. Motivo: {reason}", datetime.now().isoformat(timespec="seconds")))
            success = True
        else:
            success = False
    return success

def db_hide_leave_request(req_id, user_id):
    """
    Oculta lógicamente una solicitud de permiso para el empleado (Soft Delete).
    Solo se puede ocultar si no está en un estado pendiente activo.
    """
    with db_session() as conn:
        cur = conn.cursor()
        cur.execute("""
            UPDATE leave_requests 
            SET hidden_by_employee = 1
            WHERE id = ? AND user_id = ? AND status NOT IN ('PENDING_COORD', 'PENDING_JEFE', 'PENDING_RRHH')
        """, (req_id, user_id))
        
        if cur.rowcount > 0:
            success = True
        else:
            success = False
    return success

def get_profile_by_name(name: str):
    """Obtiene los detalles de un perfil por su nombre."""
    conn = db_conn()
    cur = conn.cursor()
    cur.execute("SELECT name, works_holidays FROM profiles WHERE name = ?", (name,))
    row = cur.fetchone()
    conn.close()
    if row:
        return {"name": row[0], "works_holidays": row[1]}
    return None

def calculate_overnight_surcharge(start_time: str, end_time: str) -> float:
    """Calcula el recargo nocturno (35%) para turnos que cruzan o entran en horario nocturno (21:00-06:00)."""
    try:
        sh, sm = map(int, start_time.split(":"))
        eh, em = map(int, end_time.split(":"))
        
        # Un turno nocturno es el que cruza medianoche, o inicia/termina en periodo nocturno (21:00 a 06:00)
        if (eh < sh) or (sh >= 21 or sh <= 6) or (eh >= 21 or eh <= 6):
            return 1.35
    except Exception:
        pass
    return 1.0