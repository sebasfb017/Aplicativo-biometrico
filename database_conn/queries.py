import os
import pandas as pd
import streamlit as st
from datetime import datetime, date, timedelta
from database_conn.connection import db_conn, DATA_DIR
from services.notifications import send_notification_email

def upsert_employees_df(df: pd.DataFrame):
    """Carga o actualiza empleados desde un DataFrame."""
    conn = db_conn()
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
    conn.commit()
    conn.close()

def db_create_leave_request(user_id, leave_start, leave_end, t_start, t_end, total_time, r_type, r_desc, makeup, is_paid):
    """Crea una solicitud F-TH-012 en la base de datos y define su estado inicial."""
    conn = db_conn()
    role = st.session_state.get("user", {}).get("role", "empleado")
    
    # Lógica de estados según tipo de permiso
    if r_type in ["Citas médicas", "Incapacidad", "Calamidad"]:
        target_status = "PENDING_RRHH"
    else:
        target_status = "PENDING_INMEDIATO"
        
    if role == "coordinador" and target_status == "PENDING_INMEDIATO":
        target_status = "PENDING_AREA"
    elif role == "jefe_area" or role in ["admin", "nomina"]:
        target_status = "PENDING_RRHH"

    cur = conn.cursor()
    cur.execute("""
        INSERT INTO leave_requests (
            user_id, request_date, leave_date_start, leave_date_end, start_time, end_time, 
            total_time, reason_type, reason_description, how_to_makeup, is_paid, created_at, status
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (user_id, datetime.now().date().isoformat(), leave_start.isoformat(), leave_end.isoformat(),
          t_start, t_end, total_time, r_type, r_desc, makeup, 1 if is_paid else 0,
          datetime.now().isoformat(timespec="seconds"), target_status))
    req_id = cur.lastrowid
    conn.commit()
    conn.close()
    
    # Notificación inmediata
    emp_email = st.session_state["user"].get("emp_email", "")
    if emp_email:
        send_notification_email(emp_email, f"Dolormed: Novedad Radicada #{req_id}", 
                                f"Tu solicitud de {r_type} ha sido radicada. Estado: {target_status}")

def is_holiday(date_obj: date) -> bool:
    """Verifica si una fecha es festivo en la base de datos."""
    conn = db_conn()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM holidays WHERE date = ?", (date_obj.isoformat(),))
    count = cur.fetchone()[0]
    conn.close()
    return count > 0

def get_shifts_df():
    """Retorna un DataFrame con todos los turnos configurados."""
    conn = db_conn()
    df = pd.read_sql_query("SELECT * FROM shifts ORDER BY name", conn)
    conn.close()
    return df

def upsert_shift(name, start_time, grace_minutes, **kwargs):
    """Crea o actualiza un turno en el catálogo."""
    conn = db_conn()
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
    conn.commit()
    conn.close()

def assign_shift(user_id, week_start, dow, shift_id):
    """Asigna un turno específico a un empleado para un día concreto."""
    conn = db_conn()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO shift_assignments(user_id, week_start, dow, shift_id, created_at)
        VALUES(?,?,?,?,?)
        ON CONFLICT(user_id, week_start, dow) DO UPDATE SET shift_id=excluded.shift_id
    """, (str(user_id), week_start, int(dow), int(shift_id), 
          datetime.now().isoformat(timespec="seconds")))
    conn.commit()
    conn.close()

def upsert_exception(user_id, date_str, exc_type, notes):
    """Registra una novedad (vacaciones, incapacidad, etc.) para un empleado."""
    conn = db_conn()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO exceptions(user_id, date, type, notes, created_at)
        VALUES(?,?,?,?,?)
        ON CONFLICT(user_id, date) DO UPDATE SET type=excluded.type, notes=excluded.notes
    """, (user_id, date_str, exc_type, notes, datetime.now().isoformat(timespec="seconds")))
    conn.commit()
    conn.close()

def get_exceptions_df():
    """Obtiene el listado de todas las novedades registradas."""
    conn = db_conn()
    df = pd.read_sql_query("""
        SELECT ex.id, ex.user_id, e.full_name, ex.date, ex.type, ex.notes, ex.created_at
        FROM exceptions ex
        LEFT JOIN employees e ON ex.user_id = e.user_id
        ORDER BY ex.date DESC
    """, conn)
    conn.close()
    return df                             