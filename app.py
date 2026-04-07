import os
import io
import yaml
import bcrypt
import sqlite3
import calendar
import pandas as pd
import streamlit as st
from streamlit_option_menu import option_menu

from datetime import datetime, date, time, timedelta

from database_conn.connection import db_conn, DATA_DIR, DB_PATH

# -----------------------------
# Auth
from utils.auth import get_user, verify_login, require_role

# -----------------------------
# DEVICES ZK
from services.zk_service import load_devices, save_devices, download_attendance_from_device, sync_device_time, upsert_attendance

# -----------------------------
# NOTIFICATIONS
from services.notifications import send_notification_email, notify_employee, log_audit, generate_fth012_html

# -----------------------------
# ANALYTICS
from services.analytics import compute_month_lateness, fetch_attendance_between, to_excel_bytes, schedule_for_user_date, schedule_for_date

# -----------------------------
# DB
from database_conn.queries import (
    upsert_employees_df, db_create_leave_request, is_holiday, 
    get_shifts_df, upsert_shift, assign_shift, 
    upsert_exception, get_exceptions_df,
    get_users_by_role, get_all_employees  # <-- Estas son las dos funciones que faltaban
)

# -----------------------------
# CONSTANTS
from utils.constants import ROLES, AREA_MAPPING

# -----------------------------
# VIEWS
from views.auth_view import page_login, register_employee_dialog
from views.dashboard_view import page_dashboard
from views.employees_view import page_employees
from views.sync_zkteco_view import page_sync
from views.employee_portal_view import page_employee_portal
from views.schedules_view import (
    page_schedules, page_shifts, page_assign_shifts, 
    ensure_schedules_columns, maybe_load_default_schedules
)
from views.exceptions_view import page_exceptions
from views.attendance_view import page_view_attendance
from views.month_report_view import page_lateness_report


APP_DIR = os.path.dirname(os.path.abspath(__file__))
# helper for default schedules file path (must be computed at runtime
# because DATA_DIR can be monkeypatched in tests).

# -----------------------------
# DB
def init_db():
    conn = db_conn()
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS users_app (
        username TEXT PRIMARY KEY,
        full_name TEXT NOT NULL,
        role TEXT NOT NULL CHECK(role IN ('admin','nomina')),
        password_hash BLOB NOT NULL,
        active INTEGER NOT NULL DEFAULT 1,
        created_at TEXT NOT NULL
    );
    """)

    # Perfiles de empleados (p.ej. Enfermería, Administrativo, etc.)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS profiles (
        profile_id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL UNIQUE,
        description TEXT,
        works_holidays INTEGER NOT NULL DEFAULT 0,
        created_at TEXT NOT NULL
    );
    """)

    # Festivos colombianos (para validaciones)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS holidays (
        holiday_id INTEGER PRIMARY KEY AUTOINCREMENT,
        date TEXT NOT NULL UNIQUE,
        description TEXT NOT NULL,
        created_at TEXT NOT NULL
    );
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS attendance_raw (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        device_name TEXT NOT NULL,
        device_ip TEXT NOT NULL,
        user_id TEXT NOT NULL,
        ts TEXT NOT NULL,          -- ISO local datetime string
        status INTEGER NOT NULL,
        punch INTEGER NOT NULL,
        uid INTEGER NOT NULL,
        downloaded_at TEXT NOT NULL,
        UNIQUE(device_ip, user_id, ts, status, punch, uid)
    );
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS schedules (
        week_start TEXT NOT NULL,  -- YYYY-MM-DD (Monday)
        dow INTEGER NOT NULL,      -- 0=Mon ... 6=Sun
        start_time TEXT NOT NULL,  -- HH:MM inicio primera franja
        end_time TEXT DEFAULT '',  -- HH:MM fin primera franja
        start_time_2 TEXT DEFAULT '', -- HH:MM inicio segunda franja
        end_time_2 TEXT DEFAULT '',   -- HH:MM fin segunda franja
        grace_minutes INTEGER NOT NULL DEFAULT 0,
        PRIMARY KEY (week_start, dow)
    );
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS employees (
        user_id TEXT PRIMARY KEY,
        full_name TEXT NOT NULL,
        email TEXT,
        department TEXT,
        profile_id INTEGER,
        created_at TEXT NOT NULL,
        FOREIGN KEY(profile_id) REFERENCES profiles(profile_id)
    );
    """)

    # Shifts: reusable named shifts con soporte para turnos complejos
    cur.execute("""
    CREATE TABLE IF NOT EXISTS shifts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL UNIQUE,
        start_time TEXT NOT NULL,
        end_time TEXT DEFAULT '',
        grace_minutes INTEGER NOT NULL DEFAULT 0,
        has_break INTEGER NOT NULL DEFAULT 0,
        break_start TEXT DEFAULT '',
        break_end TEXT DEFAULT '',
        is_overnight INTEGER NOT NULL DEFAULT 0,
        shift_code TEXT,
        created_at TEXT NOT NULL
    );
    """)

    # Auditoría de cambios de turnos
    cur.execute("""
    CREATE TABLE IF NOT EXISTS shift_logs (
        log_id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id TEXT NOT NULL,
        week_start TEXT NOT NULL,
        dow INTEGER NOT NULL,
        old_shift_id INTEGER,
        new_shift_id INTEGER,
        changed_by TEXT NOT NULL,
        change_reason TEXT,
        changed_at TEXT NOT NULL,
        FOREIGN KEY(old_shift_id) REFERENCES shifts(id),
        FOREIGN KEY(new_shift_id) REFERENCES shifts(id)
    );
    """)

    # Assignments: which shift a user has for a given week and day
    cur.execute("""
    CREATE TABLE IF NOT EXISTS shift_assignments (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id TEXT NOT NULL,
        week_start TEXT NOT NULL,   -- YYYY-MM-DD (Monday)
        dow INTEGER NOT NULL,       -- 0..6
        shift_id INTEGER NOT NULL,
        created_at TEXT NOT NULL,
        UNIQUE(user_id, week_start, dow),
        FOREIGN KEY(shift_id) REFERENCES shifts(id)
    );
    """)

    # Excepciones (Novedades, Permisos, Incapacidades)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS exceptions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id TEXT NOT NULL,
        date TEXT NOT NULL,  -- YYYY-MM-DD
        type TEXT NOT NULL,
        notes TEXT,
        created_at TEXT NOT NULL,
        UNIQUE(user_id, date)
    );
    """)

    # Permisos (Solicitudes Digitales F-TH-012)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS leave_requests (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id TEXT NOT NULL,
        request_date TEXT NOT NULL,
        leave_date_start TEXT NOT NULL,
        leave_date_end TEXT NOT NULL,
        start_time TEXT,
        end_time TEXT,
        total_time TEXT,
        reason_type TEXT,
        reason_description TEXT,
        how_to_makeup TEXT,
        is_paid INTEGER NOT NULL DEFAULT 0,
        status TEXT NOT NULL DEFAULT 'PENDING',
        created_at TEXT NOT NULL
    );
    """)

    # Auditoría (Registra acciones importantes en el sistema)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS audit_logs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id TEXT NOT NULL,
        action TEXT NOT NULL,
        details TEXT NOT NULL,
        timestamp TEXT NOT NULL
    );
    """)

    # create default admin if no users
    cur.execute("SELECT COUNT(*) FROM users_app;")
    n = cur.fetchone()[0]
    if n == 0:
        default_user = "admin"
        default_pass = "Cambiar123!"  # cámbiala en cuanto entres
        pw_hash = bcrypt.hashpw(default_pass.encode("utf-8"), bcrypt.gensalt())
        cur.execute("""
            INSERT INTO users_app(username, full_name, role, password_hash, active, created_at)
            VALUES(?,?,?,?,1,?)
        """, (default_user, "Administrador", "admin", pw_hash, datetime.now().isoformat(timespec="seconds")))
        conn.commit()

    # Crear perfiles predeterminados si no existen
    cur.execute("SELECT COUNT(*) FROM profiles;")
    if cur.fetchone()[0] == 0:
        profiles_data = [
            ("Enfermería", "Personal asistencial (Enfermeros, Auxiliares)", 1),
            ("Admisiones", "Personal de admisiones y acogida", 1),
            ("Tecnólogos RX", "Tecnólogos de Rayos X", 1),
            ("Administrativo", "Personal administrativo y servicios", 0),
        ]
        for profile_name, desc, works_holidays in profiles_data:
            cur.execute("""
                INSERT INTO profiles(name, description, works_holidays, created_at)
                VALUES(?,?,?,?)
            """, (profile_name, desc, works_holidays, datetime.now().isoformat(timespec="seconds")))
        conn.commit()

    # Cargar festivos colombianos para el año actual y próximo
    initialize_colombian_holidays(cur)
    conn.commit()

    # Crear turnos predeterminados basados en las reglas del sistema
    initialize_predefined_shifts(cur)
    conn.commit()

    conn.close()

    # run schema migration for schedules in case older DB misses new columns
    ensure_schedules_columns()
    migrate_schema_attendance_flags()
    # Schema migration for employees and shifts to add new columns
    migrate_schema_for_profiles()
    # Migration for coordinator role and managed_department
    migrate_schema_coordinators()
    # Migration for multi-level leave approvals (jefe_area)
    migrate_schema_multilevel()
    # if a default schedule file exists, load it now
    maybe_load_default_schedules()

def migrate_schema_attendance_flags():
    """Agrega las banderas para la modificación de marcaciones y auditoría."""
    conn = db_conn()
    cur = conn.cursor()
    try:
        cur.execute("PRAGMA table_info(attendance_raw)")
        columns = [row[1] for row in cur.fetchall()]
        if "is_ignored" not in columns:
            cur.execute("ALTER TABLE attendance_raw ADD COLUMN is_ignored INTEGER NOT NULL DEFAULT 0")
        if "is_manual" not in columns:
            cur.execute("ALTER TABLE attendance_raw ADD COLUMN is_manual INTEGER NOT NULL DEFAULT 0")
        conn.commit()
    except Exception as e:
        print(f"Error en migrate_schema_attendance_flags: {e}")
    finally:
        conn.close()

def migrate_schema_multilevel():
    """Migración para soportar el flujo multinivel de permisos (jefe_area)."""
    conn = db_conn()
    cur = conn.cursor()
    try:
        cur.execute("PRAGMA table_info(users_app)")
        columns = [row[1] for row in cur.fetchall()]
        if "managed_area" not in columns:
            cur.execute("ALTER TABLE users_app ADD COLUMN managed_area TEXT DEFAULT ''")
            conn.commit()
    except Exception as e:
        print(f"Error en migrate_schema_multilevel: {e}")
    finally:
        conn.close()

def migrate_schema_coordinators():
    """Migración para agregar managed_department y eliminar la restricción CHECK de roles antiguos."""
    conn = db_conn()
    cur = conn.cursor()
    try:
        cur.execute("PRAGMA table_info(users_app)")
        columns = [row[1] for row in cur.fetchall()]
        if "managed_department" not in columns:
            cur.execute("ALTER TABLE users_app ADD COLUMN managed_department TEXT DEFAULT ''")
            conn.commit()
            
        if "emp_area" not in columns:
            cur.execute("ALTER TABLE users_app ADD COLUMN emp_area TEXT DEFAULT ''")
            conn.commit()
            
        if "emp_subarea" not in columns:
            cur.execute("ALTER TABLE users_app ADD COLUMN emp_subarea TEXT DEFAULT ''")
            conn.commit()
            
        if "emp_phone" not in columns:
            cur.execute("ALTER TABLE users_app ADD COLUMN emp_phone TEXT DEFAULT ''")
            conn.commit()
            
        if "emp_email" not in columns:
            cur.execute("ALTER TABLE users_app ADD COLUMN emp_email TEXT DEFAULT ''")
            conn.commit()

        # Re-crear la tabla para quitar el constraint CHECK(role IN ('admin','nomina')) antiguo
        cur.execute("PRAGMA foreign_keys=off;")
        cur.execute("BEGIN TRANSACTION;")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS users_app_new (
                username TEXT PRIMARY KEY,
                full_name TEXT NOT NULL,
                role TEXT NOT NULL,
                password_hash BLOB NOT NULL,
                active INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL,
                managed_department TEXT DEFAULT '',
                emp_area TEXT DEFAULT '',
                emp_subarea TEXT DEFAULT '',
                emp_phone TEXT DEFAULT '',
                emp_email TEXT DEFAULT ''
            );
        """)
        cur.execute("""
            INSERT INTO users_app_new (username, full_name, role, password_hash, active, created_at, managed_department, emp_area, emp_subarea, emp_phone, emp_email)
            SELECT username, full_name, role, password_hash, active, created_at, managed_department, COALESCE(emp_area, ''), COALESCE(emp_subarea, ''), COALESCE(emp_phone, ''), COALESCE(emp_email, '') FROM users_app;
        """)
        cur.execute("DROP TABLE users_app;")
        cur.execute("ALTER TABLE users_app_new RENAME TO users_app;")
        cur.execute("COMMIT;")
        cur.execute("PRAGMA foreign_keys=on;")
    except Exception:
        pass
    finally:
        conn.close()


# -----------------------------
# Schedules
# -----------------------------
def upsert_schedule_df(df: pd.DataFrame):
    # allow optional columns for two shifts
    required = {"week_start", "dow", "start_time", "grace_minutes"}
    optional = {"end_time","start_time_2","end_time_2"}
    if not required.issubset(set(df.columns)):
        missing = required - set(df.columns)
        raise ValueError(f"Faltan columnas: {missing}")

    df = df.copy()
    df["week_start"] = df["week_start"].astype(str)
    df["dow"] = df["dow"].astype(int)
    if df["dow"].lt(0).any() or df["dow"].gt(6).any():
        raise ValueError("La columna 'dow' sólo puede tener valores entre 0 y 6")

    # normalize times
    df["start_time"] = df["start_time"].astype(str).str.slice(0, 5)
    for col in ["end_time","start_time_2","end_time_2"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.slice(0,5)
        else:
            df[col] = ""
    df["grace_minutes"] = df["grace_minutes"].fillna(0).astype(int)

    conn = db_conn()
    cur = conn.cursor()
    for _, r in df.iterrows():
        cur.execute("""
            INSERT INTO schedules(week_start, dow, start_time, end_time, start_time_2, end_time_2, grace_minutes)
            VALUES(?,?,?,?,?,?,?)
            ON CONFLICT(week_start, dow) DO UPDATE SET
                start_time=excluded.start_time,
                end_time=excluded.end_time,
                start_time_2=excluded.start_time_2,
                end_time_2=excluded.end_time_2,
                grace_minutes=excluded.grace_minutes
        """, (
            r["week_start"], int(r["dow"]), r["start_time"], r.get("end_time",""),
            r.get("start_time_2",""), r.get("end_time_2",""), int(r["grace_minutes"])))
    conn.commit()
    conn.close()


def upsert_shifts_from_code_csv(df: pd.DataFrame) -> dict:
    """Carga turnos usando CSV con shift_codes optimizado.
    
    Esperacolumnas: user_id, week_start, dow, shift_code
    Busca el perfil del empleado y resuelve el turno real.
    Retorna dict con success/errors.
    """
    required = {"user_id", "week_start", "dow", "shift_code"}
    if not required.issubset(set(df.columns)):
        missing = required - set(df.columns)
        raise ValueError(f"Faltan columnas requeridas: {missing}")
    
    df = df.copy()
    df["user_id"] = df["user_id"].astype(str)
    df["week_start"] = df["week_start"].astype(str)
    df["dow"] = df["dow"].astype(int)
    df["shift_code"] = df["shift_code"].astype(str).str.strip()
    
    conn = db_conn()
    cur = conn.cursor()
    
    assigned = 0
    errors = []
    skipped_holidays = 0
    
    for idx, r in df.iterrows():
        user_id = r["user_id"]
        week_start = r["week_start"]
        dow = int(r["dow"])
        shift_code = r["shift_code"]
        
        # Validar dow
        if dow < 0 or dow > 6:
            errors.append(f"Fila {idx}: dow={dow} inválido (0-6)")
            continue
        
        # Verificar si es festivo
        try:
            week_date = datetime.fromisoformat(week_start)
            target_date = (week_date + timedelta(days=dow)).date()
            
            if is_holiday(target_date):
                # Obtener perfil para verificar si trabaja festivos
                cur.execute("SELECT profile_id FROM employees WHERE user_id = ?", (user_id,))
                emp_row = cur.fetchone()
                if emp_row and emp_row[0]:
                    cur.execute("SELECT works_holidays FROM profiles WHERE profile_id = ?", (emp_row[0],))
                    prof_row = cur.fetchone()
                    if prof_row and not prof_row[0]:  # Si no trabaja festivos
                        skipped_holidays += 1
                        continue
        except Exception:
            pass
        
        # Resolver turno
        shift_id = resolve_shift_from_code(user_id, shift_code, week_start, dow)
        if not shift_id:
            errors.append(f"Fila {idx}: No se resolvió turno para {user_id} con código '{shift_code}'")
            continue
        
        # Asignar turno
        try:
            assign_shift(user_id, week_start, dow, shift_id)
            assigned += 1
        except Exception as e:
            errors.append(f"Fila {idx}: Error asignando turno: {str(e)}")
    
    conn.close()
    
    return {
        "assigned": assigned,
        "skipped_holidays": skipped_holidays,
        "errors": errors,
        "success": len(errors) == 0
    }

def migrate_schema_for_profiles():
    """Migración de esquema para agregar columns profile_id a employees y nuevas columnas a shifts."""
    conn = db_conn()
    cur = conn.cursor()
    try:
        cur.execute("PRAGMA table_info(employees)")
        existing = {row[1] for row in cur.fetchall()}
        if "profile_id" not in existing:
            cur.execute("ALTER TABLE employees ADD COLUMN profile_id INTEGER")
            conn.commit()
    except Exception:
        pass

    try:
        cur.execute("PRAGMA table_info(shifts)")
        existing = {row[1] for row in cur.fetchall()}
        columns_to_add = {
            "end_time": "end_time TEXT DEFAULT ''",
            "has_break": "has_break INTEGER NOT NULL DEFAULT 0",
            "break_start": "break_start TEXT DEFAULT ''",
            "break_end": "break_end TEXT DEFAULT ''",
            "is_overnight": "is_overnight INTEGER NOT NULL DEFAULT 0",
            "shift_code": "shift_code TEXT",
        }
        for col, ddl in columns_to_add.items():
            if col not in existing:
                try:
                    cur.execute(f"ALTER TABLE shifts ADD COLUMN {ddl}")
                except Exception:
                    pass
        conn.commit()
    except Exception:
        pass

    conn.close()


def initialize_colombian_holidays(cur):
    """Carga festivos colombianos estáticos en la tabla holidays.

    Para facilitar pruebas y operaciones históricas, cargamos un rango de
    años incluyendo el anterior, el actual y los dos siguientes. Los años
    cubiertos pueden ajustarse aquí.
    """
    start_year = datetime.now().year - 2  # incluir al menos dos años anteriores
    end_year = datetime.now().year + 2
    holidays = []

    for year in range(start_year, end_year + 1):
        holidays.extend([
            (f"{year}-01-01", "Año Nuevo"),
            (f"{year}-01-08", "Reyes Magos"),
            (f"{year}-02-12", "Lunes de Carnaval"),
            (f"{year}-02-13", "Martes de Carnaval"),
            (f"{year}-02-14", "Miércoles de Ceniza"),
            (f"{year}-03-28", "Jueves Santo"),
            (f"{year}-03-29", "Viernes Santo"),
            (f"{year}-05-01", "Día del Trabajo"),
            (f"{year}-06-10", "Corpus Christi"),
            (f"{year}-06-17", "Sagrado Corazón"),
            (f"{year}-07-01", "San Pedro y San Pablo"),
            (f"{year}-07-29", "Santa Marta"),
            (f"{year}-08-07", "Batalla de Boyacá"),
            (f"{year}-08-15", "Asunción de María"),
            (f"{year}-11-01", "Todos los Santos"),
            (f"{year}-11-11", "Independencia de Cartagena"),
            (f"{year}-12-08", "Inmaculada Concepción"),
            (f"{year}-12-25", "Navidad"),
        ])
    
    for date_str, description in holidays:
        try:
            cur.execute("""
                INSERT OR IGNORE INTO holidays(date, description, created_at)
                VALUES(?, ?, ?)
            """, (date_str, description, datetime.now().isoformat(timespec="seconds")))
        except Exception:
            pass


def initialize_predefined_shifts(cur):
    """Crea turnos predeterminados basados en las reglas del sistema."""
    shifts = [
        # Enfermería
        ("M - Mañana (Enf)", "07:00", "15:00", 0, 0, "", "", 0, "M"),
        ("T - Tarde (Enf)", "15:00", "23:00", 0, 0, "", "", 0, "T"),
        ("N - Noche (Enf)", "23:00", "07:00", 0, 0, "", "", 1, "N"),
        # Admisiones
        ("M - Mañana (Adm)", "06:00", "14:00", 0, 0, "", "", 0, "M_ADM"),
        ("T - Tarde (Adm)", "14:00", "22:00", 0, 0, "", "", 0, "T_ADM"),
        # Tecnólogos RX
        ("RX1 - Día", "07:00", "19:00", 0, 0, "", "", 0, "RX1"),
        ("RX2 - Noche", "19:00", "07:00", 0, 0, "", "", 1, "RX2"),
        # Administrativo
        ("OFICINA - Horario Partido", "08:00", "18:00", 0, 1, "12:00", "14:00", 0, "OFICINA"),
        ("C - Corrido", "10:00", "19:00", 0, 1, "14:00", "16:00", 0, "C"),
        # Día Libre
        ("L - Día Libre", "", "", 0, 0, "", "", 0, "L"),
    ]
    
    for name, start, end, grace, has_break, break_start, break_end, is_overnight, code in shifts:
        try:
            cur.execute("""
                INSERT OR IGNORE INTO shifts(name, start_time, end_time, grace_minutes, has_break, break_start, break_end, is_overnight, shift_code, created_at)
                VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (name, start, end, grace, has_break, break_start, break_end, is_overnight, code, datetime.now().isoformat(timespec="seconds")))
        except Exception:
            pass


def get_profile_by_name(profile_name: str):
    """Obtiene datos del perfil por nombre."""
    conn = db_conn()
    cur = conn.cursor()
    cur.execute("SELECT profile_id, name, works_holidays FROM profiles WHERE name = ?", (profile_name,))
    row = cur.fetchone()
    conn.close()
    if row:
        return {"profile_id": row[0], "name": row[1], "works_holidays": row[2]}
    return None

def resolve_shift_from_code(user_id: str, shift_code: str, week_start: str, dow: int):
    """Resuelve el turno real basado en el código (M, T, N, etc.) y el perfil del empleado.
    
    Retorna el shift_id correspondiente o None.
    """
    # Obtener perfil del empleado
    conn = db_conn()
    cur = conn.cursor()
    cur.execute("SELECT profile_id FROM employees WHERE user_id = ?", (str(user_id),))
    row = cur.fetchone()
    conn.close()
    
    if not row or not row[0]:
        return None
    
    profile_id = row[0]
    
    # Mapeo de (shift_code, profile_id) -> shift_name
    mapping = {
        # Enfermería (profile_id = 1)
        (1, "M"): "M - Mañana (Enf)",
        (1, "T"): "T - Tarde (Enf)",
        (1, "N"): "N - Noche (Enf)",
        # Admisiones (profile_id = 2)
        (2, "M"): "M - Mañana (Adm)",
        (2, "T"): "T - Tarde (Adm)",
        # Tecnólogos RX (profile_id = 3)
        (3, "RX1"): "RX1 - Día",
        (3, "RX2"): "RX2 - Noche",
        # Administrativo (profile_id = 4)
        (4, "OFICINA"): "OFICINA - Horario Partido",
        (4, "C"): "C - Corrido",
        # Día Libre (aplica a todos)
        (1, "L"): "L - Día Libre",
        (2, "L"): "L - Día Libre",
        (3, "L"): "L - Día Libre",
        (4, "L"): "L - Día Libre",
    }
    
    shift_name = mapping.get((profile_id, shift_code))
    if not shift_name:
        return None
    
    # Obtener shift_id
    conn = db_conn()
    cur = conn.cursor()
    cur.execute("SELECT id FROM shifts WHERE name = ?", (shift_name,))
    row = cur.fetchone()
    conn.close()
    
    return row[0] if row else None


def calculate_overnight_surcharge(start_time_str: str, end_time_str: str) -> float:
    """Calcula el porcentaje de recargo nocturno según ley colombiana.
    
    En Colombia, las horas trabajadas entre 21:00 y 05:59 tienen 35% recargo.
    Retorna 1.0 para sin recargo, 1.35 para con recargo.
    """
    try:
        start_hh, start_mm = map(int, start_time_str.split(":"))
        end_hh, end_mm = map(int, end_time_str.split(":"))
    except Exception:
        return 1.0
    
    start_min = start_hh * 60 + start_mm
    end_min_input = end_hh * 60 + end_mm
    
    # Si hay cruce de medianoche, end_min_input es menor que start_min
    if end_min_input < start_min:
        end_min = end_min_input + 24 * 60
    else:
        end_min = end_min_input
    
    # Rango nocturno: 21:00 (1260 min) a 05:59 (360 min)
    nocturno_inicio = 21 * 60
    nocturno_fin = 6 * 60
    
    # Verificar si hay intersección con el rango nocturno
    if start_min < nocturno_fin and end_min > nocturno_fin:
        return 1.35
    if start_min < nocturno_inicio + 24 * 60 and end_min > nocturno_inicio:
        return 1.35
    
    return 1.0


# -----------------------------
# Schedule helpers (including generation)
# -----------------------------

def generate_rotating_schedule(year: int, month: int, pattern: list[str], grace_minutes: int = 0) -> pd.DataFrame:
    """Genera un DataFrame con horarios para cada semana del mes.

    * `pattern` es una lista de cadenas HH:MM que se aplican cíclicamente a
      cada semana comenzando por el primer lunes del mes.
    * Solo se generan entradas para lunes-viernes; los sábados quedan libres.
    * El ``grace_minutes`` es uniforme para todas las filas.

    Ejemplo: pattern = ["08:00", "07:30"] alterna los horarios cada semana.
    """
    # empezar desde el primer día del mes
    first = date(year, month, 1)
    # buscar el primer lunes no anterior al mes
    current = first
    if current.weekday() != 0:
        # avanzar al siguiente lunes
        current += timedelta(days=(7 - current.weekday()))
    rows = []
    idx = 0
    while current.month == month:
        spec = pattern[idx % len(pattern)]
        # spec puede ser "08:00" o "08:00-12:30" o con segunda franja separado por '|'
        first, second = spec.split("|") if "|" in spec else (spec, "")
        start1 = first.split("-")[0]
        end1 = first.split("-")[1] if "-" in first else ""
        start2 = ""
        end2 = ""
        if second:
            parts = second.split("-")
            start2 = parts[0]
            end2 = parts[1] if len(parts) > 1 else ""
        for dow in range(0, 5):  # lunes=0 ... viernes=4
            d = current + timedelta(days=dow)
            if d.month != month:
                continue
            rows.append({
                "week_start": current.isoformat(),
                "dow": dow,
                "start_time": start1,
                "end_time": end1,
                "start_time_2": start2,
                "end_time_2": end2,
                "grace_minutes": grace_minutes,
            })
        idx += 1
        current += timedelta(days=7)
    return pd.DataFrame(rows)

# -----------------------------
# Shifts and assignments
# -----------------------------

# -----------------------------
# UI & Registration
# -----------------------------


def edit_user_dialog(username: str, emp_df: pd.DataFrame):
    conn = db_conn()
    df_u = pd.read_sql_query("SELECT * FROM users_app WHERE username = ?", conn, params=(username,))
    conn.close()
    
    if df_u.empty:
        st.error("Usuario no encontrado.")
        return
        
    u = df_u.iloc[0]
    
    st.markdown(f"### Editando ID/DNI: `{u['username']}`")
    st.markdown(f"**Nombre:** {u['full_name']}")
    
    roles_list = ["admin", "nomina", "jefe_area", "coordinador", "empleado"]
    role_idx = roles_list.index(u['role']) if u['role'] in roles_list else len(roles_list)-1
    new_role = st.selectbox("Rol", roles_list, index=role_idx)
    
    depts = sorted([d for d in emp_df['department'].unique() if d])
    dept_idx = 0
    if u.get('managed_department') in depts:
        dept_idx = depts.index(u['managed_department']) + 1
        
    new_managed_dept = st.selectbox("Departamento a Cargo (Aplica para Coordinadores)", options=[""] + depts, index=dept_idx)
    
    areas_list = list(AREA_MAPPING.keys())
    area_idx = 0
    if u.get('managed_area') in areas_list:
        area_idx = areas_list.index(u['managed_area']) + 1
        
    new_managed_area = st.selectbox("Área a Cargo (Aplica para Jefes de Área)", options=[""] + areas_list, index=area_idx)

    st.markdown("---")
    st.write("**Área y Sub-área del Empleado (Para el Portal)**")
    
    current_area = u.get("emp_area")
    if current_area not in AREA_MAPPING: current_area = "Administrativo"
        
    new_emp_area = st.selectbox("Área", list(AREA_MAPPING.keys()), index=list(AREA_MAPPING.keys()).index(current_area))
    
    current_subarea = u.get("emp_subarea")
    if current_subarea not in AREA_MAPPING[new_emp_area]: current_subarea = AREA_MAPPING[new_emp_area][0]
    
    new_emp_subarea = st.selectbox("Sub-área / Cargo", AREA_MAPPING[new_emp_area], index=AREA_MAPPING[new_emp_area].index(current_subarea))
    
    st.markdown("---")
    st.write("**Datos de Contacto**")
    c3, c4 = st.columns(2)
    with c3:
        new_emp_phone = st.text_input("Teléfono Móvil", value=u.get("emp_phone", ""))
    with c4:
        new_emp_email = st.text_input("Correo Electrónico", value=u.get("emp_email", ""))
        
    st.markdown("---")
    
    c1, c2 = st.columns(2)
    with c1:
        new_active = st.checkbox("Activo (Puede iniciar sesión)", value=bool(u['active']))
    with c2:
        new_pw = st.text_input("Nueva Contraseña (Dejar en blanco para no cambiar)", type="password")
        
    submit_edit = st.button("Guardar Cambios", type="primary")
        
    st.markdown("---")
    with st.expander("⚠️ Zona de Peligro"):
        st.warning("Eliminar a este usuario revocará su acceso al sistema de forma permanente.")
        confirm_delete = st.checkbox("Entiendo que esta acción es irreversible y quiero eliminar este usuario.")
        submit_delete = st.button("🗑️ Eliminar Usuario", type="primary", disabled=not confirm_delete)
        
    if submit_edit:
        if new_role != "coordinador":
            new_managed_dept = ""
        if new_role != "jefe_area":
            new_managed_area = ""
            
        conn = db_conn()
        cur = conn.cursor()
        
        try:
            if new_pw:
                pw_hash = bcrypt.hashpw(new_pw.encode("utf-8"), bcrypt.gensalt())
                cur.execute("""
                    UPDATE users_app 
                    SET role = ?, managed_department = ?, active = ?, password_hash = ?, emp_area = ?, emp_subarea = ?, emp_phone = ?, emp_email = ?, managed_area = ?
                    WHERE username = ?
                """, (new_role, new_managed_dept, 1 if new_active else 0, pw_hash, new_emp_area, new_emp_subarea, new_emp_phone, new_emp_email, new_managed_area, username))
            else:
                cur.execute("""
                    UPDATE users_app 
                    SET role = ?, managed_department = ?, active = ?, emp_area = ?, emp_subarea = ?, emp_phone = ?, emp_email = ?, managed_area = ?
                    WHERE username = ?
                """, (new_role, new_managed_dept, 1 if new_active else 0, new_emp_area, new_emp_subarea, new_emp_phone, new_emp_email, new_managed_area, username))
                
            conn.commit()
            log_audit("EDIT_USER", f"Usuario actualizado: {username} (Rol: {new_role})")
            st.success("✅ Cambios guardados correctamente.")
            st.rerun()
        except Exception as e:
            st.error(f"Error actualizando usuario: {e}")
        finally:
            conn.close()

    if submit_delete:
        conn = db_conn()
        cur = conn.cursor()
        cur.execute("DELETE FROM users_app WHERE username = ?", (username,))
        conn.commit()
        conn.close()
        log_audit("DELETE_USER", f"Usuario eliminado del sistema: {username}")
        st.success(f"🗑️ Usuario {username} eliminado del sistema.")
        st.rerun()

def page_users_admin():
    require_role("admin")
    st.title("👥 Gestión de Usuarios")
    st.write("Administra los accesos al portal de Nómina Dolormed.")
    
    tab1, tab2, tab3 = st.tabs(["📝 Registrar Nuevo", "👔 Portal Administrativo", "🛠️ Portal Empleados"])

    with tab1:
        st.subheader("Datos del Nuevo Usuario")
        conn = db_conn()
        emp_df = pd.read_sql_query("SELECT user_id, full_name, department FROM employees ORDER BY full_name", conn)
        conn.close()
        
        if emp_df.empty:
            st.warning("No hay empleados en el directorio. Importa empleados primero antes de crear usuarios.")
        else:
            selected_emp = st.selectbox(
                "Empleado (A quien se le creará el usuario)",
                options=emp_df['user_id'].tolist(),
                format_func=lambda uid: f"{uid} - {emp_df[emp_df['user_id']==uid]['full_name'].values[0]}"
            )
            role = st.selectbox("Rol", ["admin", "nomina", "jefe_area", "coordinador", "empleado"])
            
            depts = sorted([d for d in emp_df['department'].unique() if d])
            managed_dept = st.selectbox("Departamento a Cargo (Solo aplica para Coordinadores)", options=[""] + depts)
            
            areas = list(AREA_MAPPING.keys())
            managed_area = st.selectbox("Área a Cargo (Solo aplica para Jefes de Área)", options=[""] + areas)
            
            st.markdown("---")
            sel_area = st.selectbox("Área", list(AREA_MAPPING.keys()))
            sel_subarea = st.selectbox("Sub-área / Cargo", AREA_MAPPING[sel_area])
            st.markdown("---")
            
            c_p1, c_p2 = st.columns(2)
            with c_p1:
                new_phone = st.text_input("Teléfono Móvil (Opcional)")
            with c_p2:
                new_email = st.text_input("Correo Electrónico (Opcional)")
                
            st.markdown("---")
            
            pw = st.text_input("Contraseña inicial", type="password")
            active = st.checkbox("Activo", value=True)
            submit = st.button("Crear / Actualizar Usuario", type="primary")

            if submit:
                if not pw:
                    st.error("Completa la contraseña.")
                else:
                    u = str(selected_emp).strip()
                    full = emp_df[emp_df['user_id']==selected_emp]['full_name'].values[0]
                    
                    if role != "coordinador":
                        managed_dept = ""
                    if role != "jefe_area":
                        managed_area = ""

                    pw_hash = bcrypt.hashpw(pw.encode("utf-8"), bcrypt.gensalt())
                    conn = db_conn()
                    cur = conn.cursor()
                    cur.execute("""
                        INSERT INTO users_app(username, full_name, role, password_hash, active, created_at, managed_department, emp_area, emp_subarea, emp_phone, emp_email, managed_area)
                        VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
                        ON CONFLICT(username) DO UPDATE SET
                            full_name=excluded.full_name,
                            role=excluded.role,
                            password_hash=excluded.password_hash,
                            active=excluded.active,
                            managed_department=excluded.managed_department,
                            emp_area=excluded.emp_area,
                            emp_subarea=excluded.emp_subarea,
                            emp_phone=excluded.emp_phone,
                            emp_email=excluded.emp_email,
                            managed_area=excluded.managed_area
                    """, (u, full, role, pw_hash, 1 if active else 0, datetime.now().isoformat(timespec="seconds"), managed_dept, sel_area, sel_subarea, new_phone, new_email, managed_area))
                    conn.commit()
                    conn.close()
                    
                    log_audit("CREATE_USER", f"Usuario creado/actualizado: {u} ({full}) con rol {role}")
                    st.success(f"Usuario {u} ({full}) creado/actualizado correctamente.")

    with tab2:
        admin_df = get_users_by_role(['admin', 'nomina', 'jefe_area', 'coordinador'])
    
        if not admin_df.empty:
            admin_df_view = admin_df.copy()
            admin_df_view['active'] = admin_df_view['active'].apply(lambda x: '✅ Sí' if x == 1 else '❌ No')
            admin_df_view.columns = ["Usuario (DNI)", "Nombre Completo", "Rol", "Depto. a Cargo", "Activo", "Creado el"]
        
            st.info("💡 Haz clic en una fila para editar o eliminar.")
            event_admin = st.dataframe(
                admin_df_view, use_container_width=True, hide_index=True, 
                on_select="rerun", selection_mode="single-row", key="admin_users_table"
            )
        
            if len(event_admin.selection.rows) > 0:
                row_idx = event_admin.selection.rows[0]
                # Validación de seguridad para evitar el IndexError
                if row_idx < len(admin_df):
                    # USA "username" (nombre real en BD) en lugar de "Usuario (DNI)"
                    selected_username = str(admin_df.iloc[row_idx]["username"]) 
        
                # Limpiar la selección para evitar bucles
                st.session_state.admin_users_table.selection.rows.clear()
        
                emp_df = get_all_employees()
                edit_user_dialog(selected_username, emp_df)

    with tab3:
        emp_users_df = get_users_by_role(['empleado'])
    
        if not emp_users_df.empty:
            emp_users_df_view = emp_users_df.copy()
            emp_users_df_view['active'] = emp_users_df_view['active'].apply(lambda x: '✅ Sí' if x == 1 else '❌ No')
            emp_users_df_view.columns = ["Usuario (DNI)", "Nombre Completo", "Área", "Sub-área", "Activo", "Creado el"]
        
            event_emp = st.dataframe(
                emp_users_df_view, use_container_width=True, hide_index=True, 
                on_select="rerun", selection_mode="single-row", key="emp_users_table"
            )
        
            if len(event_emp.selection.rows) > 0:
                row_idx = event_emp.selection.rows[0]
                if row_idx < len(emp_users_df):
                    # USA "username" (nombre real en BD)
                    selected_username = str(emp_users_df.iloc[row_idx]["username"])
        
                    st.session_state.emp_users_table.selection.rows.clear()
        
                    emp_df = get_all_employees()
                    edit_user_dialog(selected_username, emp_df)

def toggle_theme_config():
    config_path = os.path.join(os.path.dirname(__file__), ".streamlit", "config.toml")
    is_dark = False
    if os.path.exists(config_path):
        with open(config_path, "r", encoding="utf-8") as f:
            if 'base="dark"' in f.read():
                is_dark = True
                
    new_mode = "light" if is_dark else "dark"
    new_content = f'''[theme]
primaryColor="#0D6EFD"
base="{new_mode}"
font="sans serif"
'''
    os.makedirs(os.path.dirname(config_path), exist_ok=True)
    with open(config_path, "w", encoding="utf-8") as f:
        f.write(new_content)


def main():
    st.set_page_config(page_title="Nómina Dolormed", layout="wide", page_icon="🏢")
    init_db()

    user = st.session_state.get("user")
    if not user:
        # Centrar login form
        col1, col2, col3 = st.columns([1,2,1])
        with col2:
            page_login()
        return

    # Sidebar Header
    st.sidebar.markdown(f"<h2 style='text-align: center; color: #0066cc;'>Dolormed RRHH</h2>", unsafe_allow_html=True)
    st.sidebar.markdown(f"<div style='text-align: center; color: gray; margin-bottom: 20px;'>Hola, <b>{user['full_name']}</b><br><small>({user['role'].upper()})</small></div>", unsafe_allow_html=True)

    if user["role"] == "admin":
        menu_options = ["Dashboard", "Reportes Mensuales", "Novedades y Excepciones", "Sincronizar Relojes", "Visualizar Data", "---", "Empleados", "Horarios", "Turnos y Asignación", "Usuarios"]
        menu_icons = ["house", "bar-chart-line", "journal-medical", "arrow-repeat", "table", "", "people", "clock", "calendar-check", "person-badge"]
    elif user["role"] == "empleado":
        menu_options = ["Mi Portal de Autogestión"]
        menu_icons = ["person-vcard"]
    elif user["role"] == "coordinador":
        menu_options = ["Dashboard", "Autorización de Permisos", "Visualizar Data"]
        menu_icons = ["house", "check2-square", "table"]
    else:
        menu_options = ["Dashboard", "Reportes Mensuales", "Novedades y Excepciones", "Sincronizar Relojes", "Visualizar Data"]
        menu_icons = ["house", "bar-chart-line", "journal-medical", "arrow-repeat", "table"]

    with st.sidebar:
        sel = option_menu(
            menu_title=None,
            options=menu_options,
            icons=menu_icons,
            menu_icon="cast",
            default_index=0,
            styles={
                "container": {"padding": "0!important", "background-color": "transparent"},
                "icon": {"color": "#0066cc", "font-size": "18px"},
                "nav-link": {"font-size": "15px", "text-align": "left", "margin":"0px", "--hover-color": "#e9ecef"},
                "nav-link-selected": {"background-color": "#0066cc", "color": "white", "icon-color":"white"},
            }
        )
        st.markdown("<br><br>", unsafe_allow_html=True)
        
        config_p = os.path.join(os.path.dirname(__file__), ".streamlit", "config.toml")
        current_mode = "☀️ Claro"
        if os.path.exists(config_p):
            with open(config_p, "r", encoding="utf-8") as f:
                if 'base="dark"' in f.read():
                    current_mode = "🌙 Oscuro"
                    
        if st.button(f"Cambiar Tema ({current_mode})", use_container_width=True):
            toggle_theme_config()
            import time
            time.sleep(0.3)
            st.rerun()
            
        if st.button("Cerrar Sesión", type="primary", use_container_width=True):
            st.session_state.pop("user", None)
            st.rerun()

    # Router
    if sel == "Dashboard":
        page_dashboard()
    elif sel == "Sincronizar Relojes":
        page_sync()
    elif sel == "Visualizar Data":
        page_view_attendance()
    elif sel == "Reportes Mensuales":
        page_lateness_report()
    elif sel in ["Novedades y Excepciones", "Autorización de Permisos"]:
        page_exceptions()
    elif sel == "Empleados":
        page_employees()
    elif sel == "Horarios":
        page_schedules()
    elif sel == "Turnos y Asignación":
        tab1, tab2 = st.tabs(["🏗️ Crear Turnos", "📝 Asignar a Empleados"])
        with tab1:
            page_shifts()
        with tab2:
            page_assign_shifts()
    elif sel == "Usuarios":
        page_users_admin()
    elif sel == "Mi Portal de Autogestión":
        page_employee_portal()


if __name__ == "__main__":
    main()
