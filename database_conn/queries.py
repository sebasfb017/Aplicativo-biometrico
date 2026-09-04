import secrets
from datetime import date, datetime, timedelta

import pandas as pd
import streamlit as st

from database_conn.connection import db_conn, db_session
from typing import List


def db_create_session(username: str) -> str:
    """Genera un token de sesión seguro, lo registra en la base de datos y retorna el token."""
    token = secrets.token_urlsafe(24)
    now = datetime.now()
    expires = now + timedelta(days=7)  # Validez de 7 días

    with db_session() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO user_sessions (token, username, created_at, expires_at)
            VALUES (%s, %s, %s, %s)
        """,
            (token, username, now.isoformat(), expires.isoformat()),
        )
    return token


def db_validate_session(token: str):
    """Valida un token de sesión y retorna la información completa del usuario si es válido."""
    now_str = datetime.now().isoformat()
    with db_session() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT u.username, u.full_name, u.role, u.active, u.emp_area, u.emp_subarea, u.managed_department, u.managed_area
            FROM user_sessions s
            JOIN users_app u ON s.username = u.username
            WHERE s.token = %s AND s.expires_at > %s AND u.active = 1
        """,
            (token, now_str),
        )
        row = cur.fetchone()

    if row:
        return {
            "username": row[0],
            "full_name": row[1],
            "role": row[2],
            "active": row[3],
            "emp_area": row[4],
            "emp_subarea": row[5],
            "managed_department": row[6],
            "managed_area": row[7],
        }
    return None


def db_delete_session(token: str):
    """Elimina la sesión correspondiente al token para cerrar la sesión."""
    with db_session() as conn:
        cur = conn.cursor()
        cur.execute("DELETE FROM user_sessions WHERE token = %s", (token,))


# --- GESTIÓN DE USUARIOS (Corrección de Errores y Consultas) ---


# CACHE: Carga infinita, se limpia manualmente al haber cambios.
@st.cache_data(show_spinner=False)
def get_users_by_role(roles_list):
    """Obtiene usuarios filtrados por una lista de roles para las tablas de administración."""
    conn = db_conn()
    roles_placeholders = ",".join(["%s"] * len(roles_list))

    # Si solo buscamos empleados, usamos una consulta optimizada para esa vista
    if "empleado" in roles_list and len(roles_list) == 1:
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
    df = pd.read_sql_query(
        "SELECT user_id, full_name, department FROM employees ORDER BY full_name", conn
    )
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
                    cur.execute(
                        "SELECT profile_id FROM profiles WHERE name = %s",
                        (profile_val.strip(),),
                    )
                    profile_row = cur.fetchone()
                    if profile_row:
                        profile_id = profile_row[0]
                else:
                    profile_id = int(profile_val)

            cur.execute(
                """
                INSERT INTO employees(user_id, full_name, email, department, profile_id, created_at)
                VALUES(%s,%s,%s,%s,%s,%s)
                ON CONFLICT(user_id) DO UPDATE SET
                    full_name = COALESCE(NULLIF(excluded.full_name, ''), employees.full_name),
                    email = COALESCE(NULLIF(excluded.email, ''), employees.email),
                    department = COALESCE(NULLIF(excluded.department, ''), employees.department),
                    profile_id = COALESCE(excluded.profile_id, employees.profile_id)
            """,
                (
                    r["user_id"],
                    r["full_name"],
                    r.get("email", ""),
                    r.get("department", ""),
                    profile_id,
                    datetime.now().isoformat(timespec="seconds"),
                ),
            )
    get_all_employees.clear()


# --- GESTIÓN DE TURNOS Y ASIGNACIONES ---


def is_holiday(date_obj: date) -> bool:
    """Verifica si una fecha existe en la tabla de festivos."""
    conn = db_conn()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM holidays WHERE date = %s", (date_obj.isoformat(),))
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
        cur.execute(
            """
            INSERT INTO shifts(name, start_time, end_time, grace_minutes, has_break, 
                               break_start, break_end, is_overnight, shift_code, created_at)
            VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT(name) DO UPDATE SET
                start_time=excluded.start_time, grace_minutes=excluded.grace_minutes,
                shift_code=excluded.shift_code
        """,
            (
                name.strip(),
                start_time.strip(),
                kwargs.get("end_time", ""),
                int(grace_minutes),
                1 if kwargs.get("has_break") else 0,
                kwargs.get("break_start", ""),
                kwargs.get("break_end", ""),
                1 if kwargs.get("is_overnight") else 0,
                kwargs.get("shift_code"),
                datetime.now().isoformat(timespec="seconds"),
            ),
        )

        cur.execute("SELECT id FROM shifts WHERE name = %s", (name.strip(),))
        row = cur.fetchone()
        shift_id = row[0] if row else None
    get_shifts_df.clear()
    return shift_id


def assign_shift(user_id, week_start, dow, shift_id):
    """Asigna un turno a un empleado para un día de la semana específico."""
    with db_session() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO shift_assignments(user_id, week_start, dow, shift_id, created_at)
            VALUES(%s,%s,%s,%s,%s)
            ON CONFLICT(user_id, week_start, dow) DO UPDATE SET shift_id=excluded.shift_id
        """,
            (
                str(user_id),
                week_start,
                int(dow),
                int(shift_id),
                datetime.now().isoformat(timespec="seconds"),
            ),
        )


# --- NOVEDADES Y PERMISOS (F-TH-012) ---


def upsert_exception(user_id, date_str, exc_type, notes):
    """Registra una novedad manual (incapacidad, vacaciones, etc.)."""
    with db_session() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO exceptions(user_id, date, type, notes, created_at)
            VALUES(%s,%s,%s,%s,%s)
            ON CONFLICT(user_id, date) DO UPDATE SET type=excluded.type, notes=excluded.notes
        """,
            (
                user_id,
                date_str,
                exc_type,
                notes,
                datetime.now().isoformat(timespec="seconds"),
            ),
        )


def get_exceptions_df():
    """Obtiene el histórico de todas las novedades agrupando días consecutivos."""
    conn = db_conn()
    df = pd.read_sql_query(
        """
        SELECT ex.id, ex.user_id, e.full_name, ex.date, ex.type, ex.notes, ex.created_at
        FROM exceptions ex
        LEFT JOIN employees e ON ex.user_id = e.user_id
        ORDER BY ex.user_id, ex.type, ex.date ASC
    """,
        conn,
    )
    conn.close()

    if df.empty:
        df["date_end"] = pd.Series(dtype="object")
        df["total_days"] = pd.Series(dtype="int")
        return df[
            [
                "id",
                "user_id",
                "full_name",
                "date",
                "date_end",
                "total_days",
                "type",
                "notes",
                "created_at",
            ]
        ]

    df["date_obj"] = pd.to_datetime(df["date"])

    df["grp"] = (
        (df["user_id"] != df["user_id"].shift())
        | (df["type"] != df["type"].shift())
        | ((df["notes"] != df["notes"].shift()) & (df["type"] != "Vacaciones"))
        | (
            (df["date_obj"].diff().dt.days > 1)
            & ~((df["type"] == "Vacaciones") & (df["date_obj"].diff().dt.days <= 5))
        )
        | (df["date_obj"].diff().dt.days <= 0)
    ).cumsum()

    grouped = (
        df.groupby(["user_id", "full_name", "type", "grp"])
        .agg(
            id=("id", "first"),
            date=("date", "min"),
            date_end=("date", "max"),
            total_days=("date", "count"),
            notes=(
                "notes",
                lambda x: (
                    ", ".join([str(i) for i in x.dropna().unique()])
                    if len(x.dropna()) > 0
                    else "Ingresado por cuadro de turnos"
                ),
            ),
            created_at=("created_at", "min"),
        )
        .reset_index()
    )

    final_df = grouped[
        [
            "id",
            "user_id",
            "full_name",
            "date",
            "date_end",
            "total_days",
            "type",
            "notes",
            "created_at",
        ]
    ].copy()
    final_df = final_df.sort_values("created_at", ascending=False)

    return final_df


def db_create_leave_request(
    user_id,
    leave_start,
    leave_end,
    t_start,
    t_end,
    total_time,
    r_type,
    r_desc,
    makeup,
    is_paid,
    attachment_path=None,
    specific_dates=None,
):
    """Crea una solicitud digital y define el flujo de aprobación inicial."""
    with db_session() as conn:
        cur = conn.cursor()
        
        # --- Prevenir solicitudes duplicadas ---
        # Comprueba si ya existe una solicitud idéntica creada en los últimos 5 minutos
        cur.execute(
            """
            SELECT id FROM leave_requests 
            WHERE user_id = %s 
              AND leave_date_start = %s 
              AND leave_date_end = %s 
              AND reason_type = %s
              AND created_at >= (NOW() - INTERVAL '5 minutes')::text
            LIMIT 1
            """,
            (user_id, leave_start.isoformat(), leave_end.isoformat(), r_type)
        )
        dup = cur.fetchone()
        if dup:
            # Si hay un duplicado en los últimos 5 minutos, devolvemos el ID existente 
            # para no insertar otro y evitar que salgan múltiples solicitudes en la bandeja
            return dup[0]
        # ----------------------------------------

        cur.execute(
            "SELECT role, emp_subarea, direct_routing FROM users_app WHERE username = %s", (user_id,)
        )
        row = cur.fetchone()
        role = row[0] if row else "empleado"
        subarea = row[1] if row else ""
        direct_routing = row[2] if row else None

    # Por defecto inicia en el nivel más bajo (Coordinador)
    target_status = "PENDING_COORD"

    if r_type == "Incapacidad":
        target_status = "PENDING_RRHH"
    elif direct_routing:
        if direct_routing == 'RRHH':
            target_status = "PENDING_RRHH"
        elif direct_routing == 'JEFE':
            target_status = "PENDING_JEFE"
        elif direct_routing == 'COORD':
            target_status = "PENDING_COORD"
    elif role == "coordinador":
        # Salta la aprobación de coordinador (ya que él es uno), va a RRHH
        target_status = "PENDING_RRHH"
    elif role in ["admin", "nomina"]:
        # Si radica RRHH, también debería registrarse formalmente o saltar a Jefe
        target_status = "PENDING_JEFE"
    elif role == "jefe_area":
        # Si radica el Jefe, se auto-aprueba por él mismo, pero DEBE pasar por RRHH
        target_status = "PENDING_RRHH"
    elif role == "empleado":
            # Si es empleado, pero no tiene coordinador activo para su subárea, pasa directo a RRHH
            has_coordinator = False
            if subarea:
                with db_session() as conn:
                    cur = conn.cursor()
                    cur.execute(
                        "SELECT managed_department FROM users_app WHERE role IN ('coordinador', 'nomina') AND active = 1"
                    )
                    coordinators = cur.fetchall()
                    for c_row in coordinators:
                        managed_dept = c_row[0] or ""
                        target_subarea = subarea
                        if target_subarea == "Servicios Generales":
                            target_subarea = "Calidad"
                        elif target_subarea == "Orientador":
                            target_subarea = "Seguridad"

                        if target_subarea in managed_dept or subarea in managed_dept:
                            has_coordinator = True
                            break
            if not has_coordinator:
                target_status = "PENDING_RRHH"

    with db_session() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO leave_requests (
                user_id, request_date, leave_date_start, leave_date_end, start_time, end_time, 
                total_time, reason_type, reason_description, how_to_makeup, is_paid, created_at, status, attachment_path, specific_dates, approved_by_jefe
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        """,
            (
                user_id,
                datetime.now().date().isoformat(),
                leave_start.isoformat(),
                leave_end.isoformat(),
                t_start,
                t_end,
                total_time,
                r_type,
                r_desc,
                makeup,
                1 if is_paid else 0,
                datetime.now().isoformat(timespec="seconds"),
                target_status,
                attachment_path,
                specific_dates,
                user_id if role == "jefe_area" else None,
            ),
        )

        req_id = cur.fetchone()[0]

    db_notify_next_approvers(req_id, user_id, target_status)
    get_cached_dataframe.clear()
    return req_id


def db_notify_next_approvers(req_id, requester_id, status, actor_name=None):
    with db_session() as conn:
        cur = conn.cursor()

        # Get requester details
        cur.execute(
            "SELECT full_name, emp_area, emp_subarea, role, managed_department FROM users_app WHERE username = %s",
            (requester_id,),
        )
        req_row = cur.fetchone()
        if not req_row:
            return
        req_name, req_area, req_subarea, req_role, req_managed = req_row

        # Obtener detalles de la solicitud para las notificaciones
        cur.execute(
            "SELECT reason_type, reason_description FROM leave_requests WHERE id = %s",
            (req_id,),
        )
        req_info = cur.fetchone()
        req_type = req_info[0] if req_info else "Permiso"
        req_desc = req_info[1] if (req_info and req_info[1]) else "Sin justificación."

        # 1. Notificar al solicitante sobre el cambio de estado
        if status == "PENDING_COORD":
            msg = f"Tu solicitud #{req_id} ha sido radicada y está pendiente del visto bueno de tu Coordinador."
            cur.execute(
                "INSERT INTO notifications (user_id, title, message, created_at) VALUES (%s, %s, %s, %s)",
                (
                    requester_id,
                    "Solicitud Radicada",
                    msg,
                    datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                ),
            )
        elif status == "PENDING_RRHH":
            if actor_name:
                msg = f"Tu solicitud #{req_id} fue aprobada por el Coordinador {actor_name} y pasó a validación de RRHH."
            else:
                msg = f"Tu solicitud #{req_id} ha sido radicada y pasó directo a validación de RRHH."
            cur.execute(
                "INSERT INTO notifications (user_id, title, message, created_at) VALUES (%s, %s, %s, %s)",
                (
                    requester_id,
                    "Paso a RRHH",
                    msg,
                    datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                ),
            )
        elif status == "PENDING_JEFE":
            if actor_name:
                msg = f"Tu solicitud #{req_id} fue pre-aprobada por RRHH ({actor_name}) y pasó a firma final del Jefe de Área."
            else:
                msg = f"Tu solicitud #{req_id} ha sido radicada y pasó a firma final del Jefe de Área."
            cur.execute(
                "INSERT INTO notifications (user_id, title, message, created_at) VALUES (%s, %s, %s, %s)",
                (
                    requester_id,
                    "Paso a Jefe de Área",
                    msg,
                    datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                ),
            )
        elif status == "APPROVED":
            msg = f"¡Felicidades! Tu solicitud #{req_id} ha sido APROBADA de forma definitiva."
            cur.execute(
                "INSERT INTO notifications (user_id, title, message, created_at) VALUES (%s, %s, %s, %s)",
                (
                    requester_id,
                    "Solicitud Aprobada",
                    msg,
                    datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                ),
            )
        elif status == "REJECTED":
            msg = f"Tu solicitud #{req_id} fue rechazada por {actor_name or 'un administrador'}."
            cur.execute(
                "INSERT INTO notifications (user_id, title, message, created_at) VALUES (%s, %s, %s, %s)",
                (
                    requester_id,
                    "Solicitud Rechazada",
                    msg,
                    datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                ),
            )
        elif status == "CANCELLED":
            msg = f"Tu solicitud #{req_id} ha sido cancelada."
            cur.execute(
                "INSERT INTO notifications (user_id, title, message, created_at) VALUES (%s, %s, %s, %s)",
                (
                    requester_id,
                    "Solicitud Cancelada",
                    msg,
                    datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                ),
            )

        # 2. Notificar a los aprobadores correspondientes
        if status == "PENDING_COORD":
            # Retrieve direct_routing
            cur.execute("SELECT direct_routing FROM users_app WHERE username = %s", (requester_id,))
            dr_row = cur.fetchone()
            direct_routing = dr_row[0] if dr_row else None

            if direct_routing == 'COORD':
                # Notify Angy Jaramillo (111644844) directly
                cur.execute(
                    "INSERT INTO notifications (user_id, title, message, created_at) VALUES (%s, %s, %s, %s)",
                    (
                        "111644844",
                        "Permiso por Autorizar (Sede Zarzal)",
                        f"Nueva solicitud #{req_id} ({req_type}) de {req_name} (Sede Zarzal) esperando tu aprobación.<br><b>Justificación:</b> {req_desc}",
                        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    ),
                )

            elif req_subarea:
                # Find coordinators managing this subarea
                cur.execute(
                    "SELECT username FROM users_app WHERE role IN ('coordinador', 'nomina') AND active = 1"
                )
                coords = cur.fetchall()
                for c in coords:
                    cur.execute(
                        "SELECT managed_department FROM users_app WHERE username = %s",
                        (c[0],),
                    )
                    m_dept = cur.fetchone()[0] or ""

                    target_subarea = req_subarea
                    if target_subarea == "Servicios Generales":
                        target_subarea = "Calidad"
                    elif target_subarea == "Orientador":
                        target_subarea = "Seguridad"

                    if target_subarea in m_dept or req_subarea in m_dept:
                        cur.execute(
                            "INSERT INTO notifications (user_id, title, message, created_at) VALUES (%s, %s, %s, %s)",
                            (
                                c[0],
                                "Permiso por Autorizar",
                                f"Nueva solicitud #{req_id} ({req_type}) de {req_name} ({req_subarea}) esperando tu aprobación.<br><b>Justificación:</b> {req_desc}",
                                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                            ),
                        )

        elif status == "PENDING_RRHH":
            # Notify all RRHH / admin users
            cur.execute(
                "SELECT username FROM users_app WHERE role IN ('admin', 'nomina') AND active = 1"
            )
            admins = cur.fetchall()
            for a in admins:
                cur.execute(
                    "INSERT INTO notifications (user_id, title, message, created_at) VALUES (%s, %s, %s, %s)",
                    (
                        a[0],
                        "Validación RRHH",
                        f"Solicitud #{req_id} ({req_type}) de {req_name} ({req_subarea or req_area}) requiere revisión de RRHH.<br><b>Justificación:</b> {req_desc}",
                        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    ),
                )

        elif status == "PENDING_JEFE":
            # --- RUTAS DE APROBACIÓN PERSONALIZADAS ---
            # Resolvemos qué Jefatura debe aprobar la solicitud basándonos en la sub-área.
            target_jefe_area = req_area

            if req_subarea in [
                "Rehabilitación",
                "Tecnólogo Rayos X",
                "Farmacia",
                "Mantenimiento",
                "Seguridad",
                "Orientador",
            ]:
                target_jefe_area = "Administrativo"
            elif req_subarea == "Admisiones":
                target_jefe_area = "Financiera"

            # 2. Estas sub-áreas (incluyendo Cirugía) saltan la jerarquía normal y pasan
            # de forma estricta por el escrutinio de Control Interno.
            elif req_subarea in [
                "Enfermería",
                "Auditor Médico",
                "Medico",
                "Control Interno",
                "Cirugía",
            ]:
                target_jefe_area = "Control Interno"
            elif req_role == "coordinador" and req_managed:
                c_depts = [d.strip() for d in req_managed.split(",") if d.strip()]
                if any(
                    dept in c_depts
                    for dept in [
                        "Rehabilitación",
                        "Tecnólogo Rayos X",
                        "Farmacia",
                        "Mantenimiento",
                        "Seguridad",
                        "Orientador",
                    ]
                ):
                    target_jefe_area = "Administrativo"
                elif "Admisiones" in c_depts:
                    target_jefe_area = "Financiera"
                elif any(
                    dept in c_depts
                    for dept in [
                        "Enfermería",
                        "Auditor Médico",
                        "Medico",
                        "Control Interno",
                        "Cirugía",
                    ]
                ):
                    target_jefe_area = "Control Interno"

            areas_to_notify = [target_jefe_area]
            if "Control Interno" not in areas_to_notify:
                areas_to_notify.append("Control Interno")

            # Find Jefes of these areas
            placeholders = ",".join(["%s"] * len(areas_to_notify))
            cur.execute(
                f"SELECT username FROM users_app WHERE role = 'jefe_area' AND managed_area IN ({placeholders}) AND active = 1",
                tuple(areas_to_notify),
            )
            jefes = cur.fetchall()
            for j in jefes:
                cur.execute(
                    "INSERT INTO notifications (user_id, title, message, created_at) VALUES (%s, %s, %s, %s)",
                    (
                        j[0],
                        "Firma de Jefe Requerida",
                        f"Solicitud #{req_id} ({req_type}) de {req_name} pendiente de tu firma final.<br><b>Justificación:</b> {req_desc}",
                        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    ),
                )


def db_approve_leave_request_coord(req_id, coord_username):
    with db_session() as conn:
        cur = conn.cursor()
        cur.execute("SELECT user_id FROM leave_requests WHERE id = %s", (req_id,))
        uid_row = cur.fetchone()
        user_id = uid_row[0] if uid_row else None

        cur.execute(
            "SELECT full_name FROM users_app WHERE username = %s", (coord_username,)
        )
        coord_row = cur.fetchone()
        coord_name = coord_row[0] if coord_row else coord_username

        cur.execute(
            """
            UPDATE leave_requests 
            SET status = 'PENDING_RRHH', approved_by_coord = %s, coord_approval_date = %s
            WHERE id = %s AND status = 'PENDING_COORD'
        """,
            (coord_username, datetime.now().isoformat(timespec="seconds"), req_id),
        )

        update_success = cur.rowcount > 0
        notify = update_success and user_id

    if notify:
        db_notify_next_approvers(req_id, user_id, "PENDING_RRHH", coord_name)
    get_cached_dataframe.clear()
    return update_success


def db_approve_leave_request_jefe(req_id, jefe_username):
    with db_session() as conn:
        cur = conn.cursor()
        cur.execute("SELECT user_id FROM leave_requests WHERE id = %s", (req_id,))
        uid_row = cur.fetchone()
        user_id = uid_row[0] if uid_row else None

        cur.execute(
            "SELECT full_name FROM users_app WHERE username = %s", (jefe_username,)
        )
        j_row = cur.fetchone()
        jefe_name = j_row[0] if j_row else jefe_username

        cur.execute(
            """
            UPDATE leave_requests 
            SET status = 'PENDING_RRHH', approved_by_jefe = %s, jefe_approval_date = %s
            WHERE id = %s AND status = 'PENDING_JEFE'
        """,
            (jefe_username, datetime.now().isoformat(timespec="seconds"), req_id),
        )

        update_success = cur.rowcount > 0
        notify = update_success and user_id

    if notify:
        db_notify_next_approvers(req_id, user_id, "PENDING_RRHH", jefe_name)
    get_cached_dataframe.clear()
    return update_success


def db_approve_leave_request_rrhh(req_id, approver_user, is_final=False):
    with db_session() as conn:
        cur = conn.cursor()
        cur.execute("SELECT user_id FROM leave_requests WHERE id = %s", (req_id,))
        uid_row = cur.fetchone()
        user_id = uid_row[0] if uid_row else None

        cur.execute(
            "SELECT full_name FROM users_app WHERE username = %s", (approver_user,)
        )
        a_row = cur.fetchone()
        approver_name = a_row[0] if a_row else approver_user

        now = datetime.now().isoformat(timespec="seconds")
        status = "APPROVED" if is_final else "PENDING_JEFE"
        cur.execute(
            """
            UPDATE leave_requests 
            SET status = %s, approved_by_rrhh = %s, rrhh_approval_date = %s 
            WHERE id = %s
        """,
            (status, approver_user, now, req_id),
        )

        update_success = cur.rowcount > 0
        notify = update_success and user_id

    if notify:
        db_notify_next_approvers(req_id, user_id, status, approver_name)
    get_cached_dataframe.clear()
    return update_success


def db_reject_leave_request(req_id, rejected_by, rejection_reason):
    with db_session() as conn:
        cur = conn.cursor()
        cur.execute("SELECT user_id FROM leave_requests WHERE id = %s", (req_id,))
        uid_row = cur.fetchone()
        user_id = uid_row[0] if uid_row else None

        cur.execute(
            "SELECT full_name FROM users_app WHERE username = %s", (rejected_by,)
        )
        r_row = cur.fetchone()
        rejecter_name = r_row[0] if r_row else rejected_by

        cur.execute(
            """
            UPDATE leave_requests 
            SET status = 'REJECTED', rejection_reason = %s
            WHERE id = %s
        """,
            (
                rejection_reason,
                req_id,
            ),
        )

        update_success = cur.rowcount > 0

        # Registrar el rechazo en la bitácora de auditoría
        cur.execute(
            """
            INSERT INTO audit_logs (user_id, action, details, timestamp)
            VALUES (%s, %s, %s, %s)
        """,
            (
                rejected_by,
                "REJECT_LEAVE",
                f"Rechazó la solicitud #{req_id}. Motivo: {rejection_reason}",
                datetime.now().isoformat(timespec="seconds"),
            ),
        )

        notify = update_success and user_id

    if notify:
        db_notify_next_approvers(req_id, user_id, "REJECTED", rejecter_name)
    get_cached_dataframe.clear()
    return update_success


def db_revert_leave_request(req_id, admin_user):
    """
    Revierte una solicitud finalizada (APPROVED o REJECTED) de vuelta a PENDING_RRHH.
    Si fue aprobada, elimina las excepciones del calendario y restaura las vacaciones si aplica.
    """
    with db_session() as conn:
        cur = conn.cursor()

        cur.execute(
            "SELECT user_id, status, reason_type, leave_date_start, leave_date_end, specific_dates FROM leave_requests WHERE id = %s",
            (req_id,),
        )
        req_row = cur.fetchone()
        if not req_row:
            return False

        user_id, status, reason_type, start_date, end_date, specific_dates = req_row

        if status not in ["APPROVED", "REJECTED"]:
            return False

        if status == "APPROVED":
            days_refunded = 0
            if specific_dates:
                dates_list = specific_dates.split(",")
                for d in dates_list:
                    cur.execute(
                        "DELETE FROM exceptions WHERE user_id = %s AND date = %s AND notes LIKE 'Aprobado de Portal%%'",
                        (user_id, d),
                    )
                    days_refunded += cur.rowcount
            else:
                cur.execute(
                    "DELETE FROM exceptions WHERE user_id = %s AND date >= %s AND date <= %s AND notes LIKE 'Aprobado de Portal%%'",
                    (user_id, start_date, end_date),
                )
                days_refunded += cur.rowcount

            if reason_type == "Vacaciones" and days_refunded > 0:
                cur.execute(
                    "UPDATE users_app SET vacation_balance = vacation_balance + %s WHERE username = %s",
                    (days_refunded, user_id),
                )

        cur.execute(
            """
            UPDATE leave_requests 
            SET status = 'PENDING_RRHH', 
                approved_by_rrhh = NULL, rrhh_approval_date = NULL,
                approved_by_jefe = NULL, jefe_approval_date = NULL,
                approved_by_coord = NULL, coord_approval_date = NULL,
                rejection_reason = NULL, cancellation_reason = NULL
            WHERE id = %s
        """,
            (req_id,),
        )

        now = datetime.now().isoformat(timespec="seconds")
        cur.execute(
            """
            INSERT INTO audit_logs (user_id, action, details, timestamp)
            VALUES (%s, %s, %s, %s)
        """,
            (
                admin_user,
                "REVERT_LEAVE",
                f"Revirtió la solicitud #{req_id} ({reason_type}) al estado PENDING_RRHH.",
                now,
            ),
        )

        cur.execute(
            """
            INSERT INTO notifications (user_id, title, message, created_at)
            VALUES (%s, %s, %s, %s)
        """,
            (
                user_id,
                "Solicitud Devuelta a Revisión",
                f"Tu solicitud de permiso #{req_id} ha sido devuelta a revisión por Gestión Humana.",
                now,
            ),
        )

        get_cached_dataframe.clear()
        return True


def db_cancel_leave_request(req_id, user_id, reason):
    """
    Cancela una solicitud de permiso por parte del empleado.
    Solo se puede cancelar si no ha sido aprobada o rechazada definitivamente.
    """
    with db_session() as conn:
        cur = conn.cursor()

        # Obtener el estado actual antes de cancelar para saber a quién notificar
        cur.execute("SELECT status FROM leave_requests WHERE id = %s", (req_id,))
        status_row = cur.fetchone()
        prev_status = status_row[0] if status_row else None

        cur.execute(
            """
            UPDATE leave_requests 
            SET status = 'CANCELLED', cancellation_reason = %s
            WHERE id = %s AND user_id = %s AND status IN ('PENDING_COORD', 'PENDING_JEFE', 'PENDING_RRHH')
        """,
            (reason, req_id, user_id),
        )

        # Verificar si se actualizó alguna fila (por si el estado ya no era PENDING)
        if cur.rowcount > 0:
            cur.execute(
                """
                INSERT INTO audit_logs (user_id, action, details, timestamp)
                VALUES (%s, %s, %s, %s)
            """,
                (
                    user_id,
                    "CANCEL_LEAVE",
                    f"El empleado canceló la solicitud #{req_id}. Motivo: {reason}",
                    datetime.now().isoformat(timespec="seconds"),
                ),
            )

            # Notificar al revisor/jefe actual de la cancelación
            cur.execute(
                "SELECT full_name FROM users_app WHERE username = %s", (user_id,)
            )
            u_row = cur.fetchone()
            emp_name = u_row[0] if u_row else user_id

            cur.execute(
                "SELECT emp_subarea, emp_area FROM users_app WHERE username = %s",
                (user_id,),
            )
            sub_row = cur.fetchone()
            emp_subarea = sub_row[0] if sub_row else ""
            emp_area = sub_row[1] if sub_row else ""

            if prev_status == "PENDING_COORD" and emp_subarea:
                cur.execute(
                    "SELECT username FROM users_app WHERE role IN ('coordinador', 'nomina') AND active = 1"
                )
                coords = cur.fetchall()
                for c in coords:
                    cur.execute(
                        "SELECT managed_department FROM users_app WHERE username = %s",
                        (c[0],),
                    )
                    m_dept = cur.fetchone()[0] or ""
                    depts = [d.strip() for d in m_dept.split(",") if d.strip()]
                    if emp_subarea in depts:
                        cur.execute(
                            "INSERT INTO notifications (user_id, title, message, created_at) VALUES (%s, %s, %s, %s)",
                            (
                                c[0],
                                "Solicitud Cancelada",
                                f"El empleado {emp_name} canceló su solicitud #{req_id}.",
                                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                            ),
                        )
            elif prev_status == "PENDING_RRHH":
                cur.execute(
                    "SELECT username FROM users_app WHERE role IN ('admin', 'nomina') AND active = 1"
                )
                admins = cur.fetchall()
                for a in admins:
                    cur.execute(
                        "INSERT INTO notifications (user_id, title, message, created_at) VALUES (%s, %s, %s, %s)",
                        (
                            a[0],
                            "Solicitud Cancelada",
                            f"El empleado {emp_name} canceló su solicitud #{req_id}.",
                            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        ),
                    )
            elif prev_status == "PENDING_JEFE":
                target_jefe_area = emp_area
                if emp_subarea in [
                    "Admisiones",
                    "Rehabilitación",
                    "Tecnólogo Rayos X",
                    "Farmacia",
                ]:
                    target_jefe_area = "Administrativo"
                elif emp_subarea in [
                    "Enfermería",
                    "Auditor Médico",
                    "Medico",
                    "Control Interno",
                    "Cirugía",
                ]:
                    target_jefe_area = "Control Interno"

                cur.execute(
                    "SELECT username FROM users_app WHERE role = 'jefe_area' AND managed_area = %s AND active = 1",
                    (target_jefe_area,),
                )
                jefes = cur.fetchall()
                for j in jefes:
                    cur.execute(
                        "INSERT INTO notifications (user_id, title, message, created_at) VALUES (%s, %s, %s, %s)",
                        (
                            j[0],
                            "Solicitud Cancelada",
                            f"El empleado {emp_name} canceló su solicitud #{req_id}.",
                            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        ),
                    )

            success = True
            notify_cancel = True
        else:
            success = False
            notify_cancel = False

    if notify_cancel:
        db_notify_next_approvers(req_id, user_id, "CANCELLED")

    return success


def db_hide_leave_request(req_id, user_id):
    """
    Oculta lógicamente una solicitud de permiso para el empleado (Soft Delete).
    Solo se puede ocultar si no está en un estado pendiente activo.
    """
    with db_session() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE leave_requests 
            SET hidden_by_employee = 1
            WHERE id = %s AND user_id = %s AND status NOT IN ('PENDING_COORD', 'PENDING_JEFE', 'PENDING_RRHH')
        """,
            (req_id, user_id),
        )

        if cur.rowcount > 0:
            success = True
        else:
            success = False
    return success


def get_profile_by_name(name: str):
    """Obtiene los detalles de un perfil por su nombre."""
    conn = db_conn()
    cur = conn.cursor()
    cur.execute("SELECT name, works_holidays FROM profiles WHERE name = %s", (name,))
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
        cur.execute(
            """
            INSERT INTO notifications (user_id, title, message, is_read, created_at)
            VALUES (%s, %s, %s, 0, %s)
        """,
            (user_id, title, message, datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
        )


def db_get_unread_notifications_count(user_id):
    with db_session() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT COUNT(*) FROM notifications WHERE user_id = %s AND is_read = 0",
            (user_id,),
        )
        res = cur.fetchone()
        return res[0] if res else 0


def db_get_recent_notifications(user_id, limit=5):
    with db_session() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT id, title, message, is_read, created_at 
            FROM notifications 
            WHERE user_id = %s 
            ORDER BY id DESC LIMIT %s
        """,
            (user_id, limit),
        )
        rows = cur.fetchall()
        return [
            {
                "id": r[0],
                "title": r[1],
                "message": r[2],
                "is_read": r[3],
                "created_at": r[4],
            }
            for r in rows
        ]


def db_mark_all_notifications_read(user_id):
    with db_session() as conn:
        cur = conn.cursor()
        cur.execute(
            "UPDATE notifications SET is_read = 1 WHERE user_id = %s", (user_id,)
        )


# --- CACHED QUERIES PARA ACELERACIÓN (NUEVO) ---


@st.cache_data(ttl=60, show_spinner=False)
def get_cached_employees():
    """Retorna el DataFrame con el directorio básico de empleados (caché 60s)."""
    with db_conn() as conn:
        return pd.read_sql_query(
            "SELECT user_id, full_name, department FROM employees ORDER BY full_name",
            conn,
        )


@st.cache_data(ttl=60, show_spinner=False)
def get_cached_users_app():
    """Retorna el DataFrame con los usuarios administrativos (caché 60s)."""
    with db_conn() as conn:
        return pd.read_sql_query(
            "SELECT username, full_name, role, emp_email, emp_area, emp_subarea, managed_department, active FROM users_app",
            conn,
        )


@st.cache_data(ttl=60, show_spinner=False)
def get_cached_profiles():
    """Retorna el DataFrame con los perfiles de horarios (caché 60s)."""
    with db_conn() as conn:
        return pd.read_sql_query(
            "SELECT profile_id, name, description, works_holidays FROM profiles ORDER BY name",
            conn,
        )


def db_run_vacation_accruals():
    """Calcula y asigna días de vacaciones (15 por año) a los empleados que cumplen año laboral."""
    with db_session() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT username, hire_date, vacation_balance, last_anniversary_year FROM users_app WHERE hire_date IS NOT NULL AND active = 1"
        )
        users = cur.fetchall()

        current_year = datetime.now().year
        current_date = datetime.now().date()

        for u in users:
            username, hire_date_str, bal, last_year = u
            if not hire_date_str:
                continue
            try:
                hire_date = datetime.strptime(hire_date_str, "%Y-%m-%d").date()
            except ValueError:
                continue

            if last_year is None:
                last_year = hire_date.year

            updates_made = False
            while last_year < current_year:
                try:
                    anniversary = date(last_year + 1, hire_date.month, hire_date.day)
                except ValueError:  # Bisiesto 29 feb
                    anniversary = date(
                        last_year + 1, hire_date.month, hire_date.day - 1
                    )

                if current_date >= anniversary:
                    bal = (bal or 0) + 15
                    last_year += 1
                    updates_made = True
                else:
                    break

            if updates_made:
                cur.execute(
                    "UPDATE users_app SET vacation_balance = %s, last_anniversary_year = %s WHERE username = %s",
                    (bal, last_year, username),
                )

def db_create_hr_procedure(user_id, procedure_type, details, attachment_path):
    with db_session() as conn:
        cur = conn.cursor()
        
        # --- Prevenir solicitudes duplicadas ---
        cur.execute(
            """
            SELECT id FROM hr_procedures 
            WHERE user_id = %s 
              AND procedure_type = %s
              AND created_at >= (NOW() - INTERVAL '5 minutes')::text
            LIMIT 1
            """,
            (user_id, procedure_type)
        )
        if cur.fetchone():
            return  # Si ya hay una en los últimos 5 mins, no hacemos nada
        # ----------------------------------------
        
        now_str = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        cur.execute(
            """
            INSERT INTO hr_procedures (user_id, procedure_type, details, attachment_path, status, created_at)
            VALUES (%s, %s, %s, %s, 'PENDING', %s)
            """,
            (user_id, procedure_type, details, attachment_path, now_str),
        )
        conn.commit()


def db_get_employee_procedures(user_id):
    with db_session() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT id, procedure_type, details, attachment_path, status, created_at
            FROM hr_procedures
            WHERE user_id = %s
            ORDER BY created_at DESC
            """,
            (user_id,),
        )
        return [
            dict(
                id=r[0],
                procedure_type=r[1],
                details=r[2],
                attachment_path=r[3],
                status=r[4],
                created_at=r[5],
            )
            for r in cur.fetchall()
        ]

def db_get_pending_hr_procedures():
    """Retrieve all pending or in-progress HR procedures for the dashboard."""
    with db_session() as conn:
        import pandas as pd
        
        query = """
        SELECT hp.id, hp.user_id, e.full_name, e.department, hp.procedure_type, 
               hp.details, hp.attachment_path, hp.status, hp.created_at
        FROM hr_procedures hp
        LEFT JOIN employees e ON hp.user_id = e.user_id
        WHERE hp.status IN ('PENDING', 'IN_PROGRESS')
        ORDER BY hp.created_at ASC
        """
        return pd.read_sql_query(query, conn)

def db_update_hr_procedure_status(procedure_id, status, notes=""):
    """Update the status of an HR procedure."""
    with db_session() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE hr_procedures
            SET status = %s
            WHERE id = %s
            """,
            (status, procedure_id),
        )
        conn.commit()

@st.cache_data(ttl=60, show_spinner=False)
def get_cached_dataframe(query: str, params: tuple = None):
    """
    Ejecuta una consulta SQL y retorna un DataFrame cacheado.
    Expira a los 60 segundos o cuando se limpia manualmente.
    """
    conn = db_conn()
    df = pd.read_sql_query(query, conn, params=params)
    conn.close()
    return df


def db_update_theme(username: str, theme: str):
    """Actualiza la preferencia de tema visual del usuario."""
    conn = db_conn()
    cur = conn.cursor()
    cur.execute(
        "UPDATE users_app SET theme_preference = %s WHERE username = %s",
        (theme, username)
    )
    conn.commit()
    conn.close()
