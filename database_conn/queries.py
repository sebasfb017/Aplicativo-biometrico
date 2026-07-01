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
    with db_session() as conn:
        cur = conn.cursor()
        cur.execute("SELECT role, emp_subarea FROM users_app WHERE username = ?", (user_id,))
        row = cur.fetchone()
        role = row[0] if row else "empleado"
        subarea = row[1] if row else ""
    
    # Por defecto inicia en el nivel más bajo (Coordinador)
    target_status = "PENDING_COORD"
    
    if r_type == "Incapacidad":
        target_status = "PENDING_RRHH"
    elif role == "coordinador":
        # Salta al siguiente paso: Jefe de Área
        target_status = "PENDING_JEFE"
    elif role in ["admin", "nomina"]:
        # Si radica RRHH, salta al siguiente paso: Jefe de Área
        target_status = "PENDING_JEFE"
    elif role == "jefe_area":
        # Si radica el Jefe, se auto-aprueba (es la última instancia)
        target_status = "APPROVED"
    elif role == "empleado":
        # Si es empleado, pero no tiene coordinador activo para su subárea, pasa directo a RRHH
        has_coordinator = False
        if subarea:
            with db_session() as conn:
                cur = conn.cursor()
                cur.execute("SELECT managed_department FROM users_app WHERE role = 'coordinador' AND active = 1")
                coordinators = cur.fetchall()
                for c_row in coordinators:
                    c_depts = [d.strip() for d in c_row[0].split(',') if d.strip()]
                    if subarea in c_depts:
                        has_coordinator = True
                        break
        if not has_coordinator:
            target_status = "PENDING_RRHH"

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
    
    db_notify_next_approvers(req_id, user_id, target_status)
    return req_id

def db_notify_next_approvers(req_id, requester_id, status, actor_name=None):
    with db_session() as conn:
        cur = conn.cursor()
        
        # Get requester details
        cur.execute("SELECT full_name, emp_area, emp_subarea, role, managed_department FROM users_app WHERE username = ?", (requester_id,))
        req_row = cur.fetchone()
        if not req_row:
            return
        req_name, req_area, req_subarea, req_role, req_managed = req_row
        
        # 1. Notify the requester of the status change
        if status == 'PENDING_COORD':
            msg = f"Tu solicitud #{req_id} ha sido radicada y está pendiente del visto bueno de tu Coordinador."
            cur.execute("INSERT INTO notifications (user_id, title, message, created_at) VALUES (?, ?, ?, ?)",
                        (requester_id, "Solicitud Radicada", msg, datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
        elif status == 'PENDING_RRHH':
            if actor_name:
                msg = f"Tu solicitud #{req_id} fue aprobada por el Coordinador {actor_name} y pasó a validación de RRHH."
            else:
                msg = f"Tu solicitud #{req_id} ha sido radicada y pasó directo a validación de RRHH."
            cur.execute("INSERT INTO notifications (user_id, title, message, created_at) VALUES (?, ?, ?, ?)",
                        (requester_id, "Paso a RRHH", msg, datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
        elif status == 'PENDING_JEFE':
            if actor_name:
                msg = f"Tu solicitud #{req_id} fue pre-aprobada por RRHH ({actor_name}) y pasó a firma final del Jefe de Área."
            else:
                msg = f"Tu solicitud #{req_id} ha sido radicada y pasó a firma final del Jefe de Área."
            cur.execute("INSERT INTO notifications (user_id, title, message, created_at) VALUES (?, ?, ?, ?)",
                        (requester_id, "Paso a Jefe de Área", msg, datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
        elif status == 'APPROVED':
            msg = f"¡Felicidades! Tu solicitud #{req_id} ha sido APROBADA de forma definitiva."
            cur.execute("INSERT INTO notifications (user_id, title, message, created_at) VALUES (?, ?, ?, ?)",
                        (requester_id, "Solicitud Aprobada", msg, datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
        elif status == 'REJECTED':
            msg = f"Tu solicitud #{req_id} fue rechazada por {actor_name or 'un administrador'}."
            cur.execute("INSERT INTO notifications (user_id, title, message, created_at) VALUES (?, ?, ?, ?)",
                        (requester_id, "Solicitud Rechazada", msg, datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
        elif status == 'CANCELLED':
            msg = f"Tu solicitud #{req_id} ha sido cancelada."
            cur.execute("INSERT INTO notifications (user_id, title, message, created_at) VALUES (?, ?, ?, ?)",
                        (requester_id, "Solicitud Cancelada", msg, datetime.now().strftime("%Y-%m-%d %H:%M:%S")))

        # 2. Notify the approvers
        if status == 'PENDING_COORD' and req_subarea:
            # Find coordinators managing this subarea
            cur.execute("SELECT username FROM users_app WHERE role = 'coordinador' AND active = 1")
            coords = cur.fetchall()
            for c in coords:
                cur.execute("SELECT managed_department FROM users_app WHERE username = ?", (c[0],))
                m_dept = cur.fetchone()[0] or ""
                depts = [d.strip() for d in m_dept.split(',') if d.strip()]
                if req_subarea in depts:
                    cur.execute("INSERT INTO notifications (user_id, title, message, created_at) VALUES (?, ?, ?, ?)",
                                (c[0], "Permiso por Autorizar", f"Nueva solicitud #{req_id} de {req_name} ({req_subarea}) esperando tu aprobación.", datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
        
        elif status == 'PENDING_RRHH':
            # Notify all RRHH / admin users
            cur.execute("SELECT username FROM users_app WHERE role IN ('admin', 'nomina') AND active = 1")
            admins = cur.fetchall()
            for a in admins:
                cur.execute("INSERT INTO notifications (user_id, title, message, created_at) VALUES (?, ?, ?, ?)",
                            (a[0], "Validación RRHH", f"Solicitud #{req_id} de {req_name} ({req_subarea or req_area}) requiere revisión de RRHH.", datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
                            
        elif status == 'PENDING_JEFE':
            # Resolve Jefe's area
            target_jefe_area = req_area
            if req_subarea == 'Admisiones': target_jefe_area = 'Administrativo'
            elif req_subarea == 'Auditor Médico': target_jefe_area = 'Auditoria Médica'
            elif req_subarea == 'Control Interno': target_jefe_area = 'Control Interno'
            elif req_subarea == 'Enfermería': target_jefe_area = 'Control Interno'
            elif req_role == 'coordinador' and req_managed:
                c_depts = [d.strip() for d in req_managed.split(',') if d.strip()]
                if 'Enfermería' in c_depts:
                    target_jefe_area = 'Control Interno'
                    
            # Find Jefes of this area
            cur.execute("SELECT username FROM users_app WHERE role = 'jefe_area' AND managed_area = ? AND active = 1", (target_jefe_area,))
            jefes = cur.fetchall()
            for j in jefes:
                cur.execute("INSERT INTO notifications (user_id, title, message, created_at) VALUES (?, ?, ?, ?)",
                            (j[0], "Firma de Jefe Requerida", f"Solicitud #{req_id} de {req_name} pendiente de tu firma final.", datetime.now().strftime("%Y-%m-%d %H:%M:%S")))

def db_approve_leave_request_coord(req_id, coord_username):
    with db_session() as conn:
        cur = conn.cursor()
        cur.execute("SELECT user_id FROM leave_requests WHERE id = ?", (req_id,))
        uid_row = cur.fetchone()
        user_id = uid_row[0] if uid_row else None
        
        cur.execute("SELECT full_name FROM users_app WHERE username = ?", (coord_username,))
        coord_row = cur.fetchone()
        coord_name = coord_row[0] if coord_row else coord_username
        
        cur.execute("""
            UPDATE leave_requests 
            SET status = 'PENDING_RRHH', approved_by_coord = ?, coord_approval_date = ?
            WHERE id = ? AND status = 'PENDING_COORD'
        """, (coord_username, datetime.now().isoformat(timespec="seconds"), req_id))
        
        if cur.rowcount > 0 and user_id:
            db_notify_next_approvers(req_id, user_id, 'PENDING_RRHH', coord_name)

def db_approve_leave_request_jefe(req_id, jefe_username):
    with db_session() as conn:
        cur = conn.cursor()
        cur.execute("SELECT user_id FROM leave_requests WHERE id = ?", (req_id,))
        uid_row = cur.fetchone()
        user_id = uid_row[0] if uid_row else None
        
        cur.execute("SELECT full_name FROM users_app WHERE username = ?", (jefe_username,))
        j_row = cur.fetchone()
        jefe_name = j_row[0] if j_row else jefe_username
        
        cur.execute("""
            UPDATE leave_requests 
            SET status = 'APPROVED', approved_by_jefe = ?, jefe_approval_date = ?
            WHERE id = ? AND status = 'PENDING_JEFE'
        """, (jefe_username, datetime.now().isoformat(timespec="seconds"), req_id))
        
        if cur.rowcount > 0 and user_id:
            db_notify_next_approvers(req_id, user_id, 'APPROVED', jefe_name)

def db_approve_leave_request_rrhh(req_id, approver_user, is_final=False):
    with db_session() as conn:
        cur = conn.cursor()
        cur.execute("SELECT user_id FROM leave_requests WHERE id = ?", (req_id,))
        uid_row = cur.fetchone()
        user_id = uid_row[0] if uid_row else None
        
        cur.execute("SELECT full_name FROM users_app WHERE username = ?", (approver_user,))
        a_row = cur.fetchone()
        approver_name = a_row[0] if a_row else approver_user
        
        now = datetime.now().isoformat(timespec="seconds")
        status = 'APPROVED' if is_final else 'PENDING_JEFE'
        cur.execute("""
            UPDATE leave_requests 
            SET status = ?, approved_by_rrhh = ?, rrhh_approval_date = ? 
            WHERE id = ?
        """, (status, approver_user, now, req_id))
        
        if cur.rowcount > 0 and user_id:
            db_notify_next_approvers(req_id, user_id, status, approver_name)

def db_reject_leave_request(req_id, rejected_by, rejection_reason):
    with db_session() as conn:
        cur = conn.cursor()
        cur.execute("SELECT user_id FROM leave_requests WHERE id = ?", (req_id,))
        uid_row = cur.fetchone()
        user_id = uid_row[0] if uid_row else None
        
        cur.execute("SELECT full_name FROM users_app WHERE username = ?", (rejected_by,))
        r_row = cur.fetchone()
        rejecter_name = r_row[0] if r_row else rejected_by
        
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
        
        if cur.rowcount > 0 and user_id:
            db_notify_next_approvers(req_id, user_id, 'REJECTED', rejecter_name)

def db_cancel_leave_request(req_id, user_id, reason):
    """
    Cancela una solicitud de permiso por parte del empleado.
    Solo se puede cancelar si no ha sido aprobada o rechazada definitivamente.
    """
    with db_session() as conn:
        cur = conn.cursor()
        
        # Obtener el estado actual antes de cancelar para saber a quién notificar
        cur.execute("SELECT status FROM leave_requests WHERE id = ?", (req_id,))
        status_row = cur.fetchone()
        prev_status = status_row[0] if status_row else None
        
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
            
            db_notify_next_approvers(req_id, user_id, 'CANCELLED')
            
            # Notificar al revisor/jefe actual de la cancelación
            cur.execute("SELECT full_name FROM users_app WHERE username = ?", (user_id,))
            u_row = cur.fetchone()
            emp_name = u_row[0] if u_row else user_id
            
            cur.execute("SELECT emp_subarea, emp_area FROM users_app WHERE username = ?", (user_id,))
            sub_row = cur.fetchone()
            emp_subarea = sub_row[0] if sub_row else ""
            emp_area = sub_row[1] if sub_row else ""
            
            if prev_status == 'PENDING_COORD' and emp_subarea:
                cur.execute("SELECT username FROM users_app WHERE role = 'coordinador' AND active = 1")
                coords = cur.fetchall()
                for c in coords:
                    cur.execute("SELECT managed_department FROM users_app WHERE username = ?", (c[0],))
                    m_dept = cur.fetchone()[0] or ""
                    depts = [d.strip() for d in m_dept.split(',') if d.strip()]
                    if emp_subarea in depts:
                        cur.execute("INSERT INTO notifications (user_id, title, message, created_at) VALUES (?, ?, ?, ?)",
                                    (c[0], "Solicitud Cancelada", f"El empleado {emp_name} canceló su solicitud #{req_id}.", datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
            elif prev_status == 'PENDING_RRHH':
                cur.execute("SELECT username FROM users_app WHERE role IN ('admin', 'nomina') AND active = 1")
                admins = cur.fetchall()
                for a in admins:
                    cur.execute("INSERT INTO notifications (user_id, title, message, created_at) VALUES (?, ?, ?, ?)",
                                (a[0], "Solicitud Cancelada", f"El empleado {emp_name} canceló su solicitud #{req_id}.", datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
            elif prev_status == 'PENDING_JEFE':
                target_jefe_area = emp_area
                if emp_subarea == 'Admisiones': target_jefe_area = 'Administrativo'
                elif emp_subarea == 'Auditor Médico': target_jefe_area = 'Auditoria Médica'
                elif emp_subarea == 'Control Interno': target_jefe_area = 'Control Interno'
                elif emp_subarea == 'Enfermería': target_jefe_area = 'Control Interno'
                
                cur.execute("SELECT username FROM users_app WHERE role = 'jefe_area' AND managed_area = ? AND active = 1", (target_jefe_area,))
                jefes = cur.fetchall()
                for j in jefes:
                    cur.execute("INSERT INTO notifications (user_id, title, message, created_at) VALUES (?, ?, ?, ?)",
                                (j[0], "Solicitud Cancelada", f"El empleado {emp_name} canceló su solicitud #{req_id}.", datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
            
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

def db_create_notification(user_id, title, message):
    with db_session() as conn:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO notifications (user_id, title, message, is_read, created_at)
            VALUES (?, ?, ?, 0, ?)
        """, (user_id, title, message, datetime.now().strftime("%Y-%m-%d %H:%M:%S")))

def db_get_unread_notifications_count(user_id):
    with db_session() as conn:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM notifications WHERE user_id = ? AND is_read = 0", (user_id,))
        res = cur.fetchone()
        return res[0] if res else 0

def db_get_recent_notifications(user_id, limit=5):
    with db_session() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT id, title, message, is_read, created_at 
            FROM notifications 
            WHERE user_id = ? 
            ORDER BY id DESC LIMIT ?
        """, (user_id, limit))
        rows = cur.fetchall()
        return [{"id": r[0], "title": r[1], "message": r[2], "is_read": r[3], "created_at": r[4]} for r in rows]

def db_mark_all_notifications_read(user_id):
    with db_session() as conn:
        cur = conn.cursor()
        cur.execute("UPDATE notifications SET is_read = 1 WHERE user_id = ?", (user_id,))