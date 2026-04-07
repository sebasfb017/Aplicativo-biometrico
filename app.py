import os
import io
import yaml
import bcrypt
import sqlite3
import calendar
import pandas as pd
import streamlit as st
import plotly.express as px
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
    upsert_exception, get_exceptions_df
)

APP_DIR = os.path.dirname(os.path.abspath(__file__))
# helper for default schedules file path (must be computed at runtime
# because DATA_DIR can be monkeypatched in tests).
def default_schedules_path():
    return os.path.join(DATA_DIR, "default_schedules.csv")
ROLES = ("admin", "nomina")

AREA_MAPPING = {
    "Administrativo": ["Talento humano", "Calidad", "SST", "Dirección Administrativa", "Sistemas", "Servicios Generales", "Gerencia", "administrativa", "siau"],
    "Financiera": ["Facturación", "Glosas", "Cartera", "Nomina", "Contabilidad"],
    "Asistencial": ["Enfermería", "Farmacia"],
    "Medico": ["Medico"],
    "Rayos X": ["Tecnólogo Rayos X"]
}
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

def log_audit(action, details):
    """Registra una acción en la tabla de auditoría."""
    conn = db_conn()
    cur = conn.cursor()
    user_id = "SISTEMA"
    if "user" in st.session_state and st.session_state["user"]:
        user_id = st.session_state["user"].get("username", "SISTEMA")
        
    try:
        cur.execute("""
            INSERT INTO audit_logs (user_id, action, details, timestamp)
            VALUES (?, ?, ?, ?)
        """, (user_id, action, details, datetime.now().isoformat(timespec="seconds")))
        conn.commit()
    except Exception as e:
        print(f"Error escribiendo log de auditoria: {e}")
    finally:
        conn.close()

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


def maybe_load_default_schedules():
    """Read ``DEFAULT_SCHEDULES_FILE`` if present and insert rows.

    This is invoked from ``init_db`` so that a clean installation can
    ship with a CSV and have schedules populated on first run.
    """
    path = default_schedules_path()
    if not os.path.exists(path):
        return
    try:
        df = pd.read_csv(path)
        upsert_schedule_df(df)
    except Exception:
        # ignore errors - misformatted file shouldn't crash startup
        pass


def ensure_schedules_columns():
    """Make sure `schedules` table has new columns when upgrading old DBs.

    Older installations may lack end_time, start_time_2, end_time_2 or
    grace_minutes columns. This function adds them if missing without
    altering existing data.
    """
    conn = db_conn()
    cur = conn.cursor()
    try:
        cur.execute("PRAGMA table_info(schedules)")
        existing = {row[1] for row in cur.fetchall()}
    except Exception:
        existing = set()

    additions = {
        "end_time": "end_time TEXT DEFAULT ''",
        "start_time_2": "start_time_2 TEXT DEFAULT ''",
        "end_time_2": "end_time_2 TEXT DEFAULT ''",
        "grace_minutes": "grace_minutes INTEGER NOT NULL DEFAULT 0",
    }
    for col, ddl in additions.items():
        if col not in existing:
            try:
                cur.execute(f"ALTER TABLE schedules ADD COLUMN {ddl}")
            except Exception:
                pass
    conn.commit()
    conn.close()


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

def auto_assign_shifts_from_schedules():
    """Crea/actualiza turnos y asignaciones para cada empleado según 
    los registros de `schedules`.

    Se genera un turno por combinación de start_time (title = e.g. "08:00").
    Retorna número de asignaciones creadas.
    """
    # recuperar horarios únicos
    conn = db_conn()
    sched_df = pd.read_sql_query("SELECT week_start,dow,start_time,grace_minutes FROM schedules", conn)
    emp_df = pd.read_sql_query("SELECT user_id FROM employees", conn)
    conn.close()
    if sched_df.empty or emp_df.empty:
        return 0

    count = 0
    for _, row in sched_df.iterrows():
        name = row["start_time"]
        sid = upsert_shift(name, row["start_time"], row["grace_minutes"])
        for uid in emp_df["user_id"]:
            assign_shift(uid, row["week_start"], int(row["dow"]), sid)
            count += 1
    return count

def to_excel_bytes(summary_df: pd.DataFrame, detail_df: pd.DataFrame, raw_df: pd.DataFrame | None = None):
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        summary_df.to_excel(writer, index=False, sheet_name="RESUMEN_TARDANZAS")
        detail_df.to_excel(writer, index=False, sheet_name="DETALLE_TARDANZAS")
        if raw_df is not None and not raw_df.empty:
            raw_df.to_excel(writer, index=False, sheet_name="MARCACIONES_RAW")
    output.seek(0)
    return output.getvalue()


# -----------------------------
# UI & Registration
# -----------------------------

@st.dialog("📝 Registro en Portal de Empleados", width="large")
def register_employee_dialog():
    st.write("Crea tu cuenta segura para acceder al Portal de Autogestión.")
    
    if "reg_step" not in st.session_state:
        st.session_state["reg_step"] = 1
    if "reg_dni" not in st.session_state:
        st.session_state["reg_dni"] = ""
    if "reg_name" not in st.session_state:
        st.session_state["reg_name"] = ""
    if "reg_error" not in st.session_state:
        st.session_state["reg_error"] = ""
        
    def verify_cedula():
        st.session_state["reg_error"] = ""
        cedula_reg = st.session_state.get("reg_cedula_input", "").strip()
        
        if not cedula_reg:
            st.session_state["reg_error"] = "Por favor ingresa tu cédula."
            return
            
        conn = db_conn()
        emp_df = pd.read_sql_query("SELECT full_name FROM employees WHERE user_id = ?", conn, params=(cedula_reg,))
        if emp_df.empty:
            st.session_state["reg_error"] = f"❌ La cédula {cedula_reg} no se encuentra en el listado maestro de empleados. Pide a Recursos Humanos que te registre en la pestaña 'Empleados' del Área Administrativa."
            conn.close()
            return
            
        full_name = emp_df.iloc[0]['full_name']
        
        user_df = pd.read_sql_query("SELECT username FROM users_app WHERE username = ?", conn, params=(cedula_reg,))
        if not user_df.empty:
            st.session_state["reg_error"] = f"ℹ️ El usuario DNI {cedula_reg} ya se encuentra registrado. Si olvidaste tu contraseña, contacta a RRHH/Sistemas."
            conn.close()
            return
            
        conn.close()
        st.session_state["reg_dni"] = cedula_reg
        st.session_state["reg_name"] = full_name
        st.session_state["reg_step"] = 2

    def create_account():
        st.session_state["reg_error"] = ""
        pass1 = st.session_state.get("reg_pass1", "")
        pass2 = st.session_state.get("reg_pass2", "")
        sel_area = st.session_state.get("reg_sel_area", "Administrativo")
        sel_subarea = st.session_state.get("reg_sel_subarea", "")
        phone = st.session_state.get("reg_phone", "").strip()
        email = st.session_state.get("reg_email", "").strip()
        
        if not pass1 or not pass2 or not phone or not email:
            st.session_state["reg_error"] = "Todos los campos de Registro (Teléfono, Correo y Contraseñas) son obligatorios."
            return
            
        if pass1 != pass2:
            st.session_state["reg_error"] = "Las contraseñas no coinciden."
            return
            
        if len(pass1) < 4:
            st.session_state["reg_error"] = "La contraseña debe tener al menos 4 caracteres."
            return
            
        pw_hash = bcrypt.hashpw(pass1.encode("utf-8"), bcrypt.gensalt())
        conn = db_conn()
        cur = conn.cursor()
        try:
            cur.execute("""
                INSERT INTO users_app(username, full_name, role, password_hash, active, created_at, managed_department, emp_area, emp_subarea, emp_phone, emp_email)
                VALUES(?,?,?,?,?,?,?,?,?,?,?)
            """, (st.session_state["reg_dni"], st.session_state["reg_name"], "empleado", pw_hash, 1, datetime.now().isoformat(timespec="seconds"), "", sel_area, sel_subarea, phone, email))
            conn.commit()
            st.session_state["reg_step"] = 3
        except Exception as e:
            st.session_state["reg_error"] = f"Error al crear el usuario: {str(e)}"
        finally:
            conn.close()

    def go_back():
        st.session_state["reg_error"] = ""
        st.session_state["reg_step"] = 1

    def finish_process():
        st.session_state["reg_step"] = 1
        st.session_state["reg_dni"] = ""
        st.session_state["reg_name"] = ""
        st.session_state["reg_error"] = ""

    if st.session_state["reg_step"] == 1:
        st.info("Paso 1: Verificación de Identidad")
        st.text_input("Número de Cédula (DNI) registrado en la empresa", key="reg_cedula_input")
        
        if st.session_state["reg_error"]:
            st.error(st.session_state["reg_error"])
            
        st.button("Verificar Cédula", type="primary", on_click=verify_cedula)

    elif st.session_state["reg_step"] == 2:
        st.success(f"¡Hola, {st.session_state['reg_name']}! Completa tus datos para crear la cuenta.")
        st.info("Paso 2: Datos de Contacto, Área y Seguridad")
        
        col_c1, col_c2 = st.columns(2)
        with col_c1:
            st.text_input("Teléfono Móvil", key="reg_phone")
        with col_c2:
            st.text_input("Correo Electrónico", key="reg_email")
            
        st.markdown("---")
        st.selectbox("Área a la que perteneces", list(AREA_MAPPING.keys()), key="reg_sel_area")
        
        selected_a = st.session_state.get("reg_sel_area", "Administrativo")
        if selected_a not in AREA_MAPPING: selected_a = "Administrativo"
            
        st.selectbox("Sub-área / Cargo", AREA_MAPPING[selected_a], key="reg_sel_subarea")
        
        st.markdown("---")
        st.text_input("Ingresa una Contraseña nueva", type="password", key="reg_pass1")
        st.text_input("Confirma tu Contraseña", type="password", key="reg_pass2")
        
        if st.session_state["reg_error"]:
            st.error(st.session_state["reg_error"])
            
        col1, col2 = st.columns(2)
        with col1:
            st.button("Crear mi Cuenta", type="primary", use_container_width=True, on_click=create_account)
        with col2:
            st.button("Volver atrás", use_container_width=True, on_click=go_back)

    elif st.session_state["reg_step"] == 3:
        st.success(f"🎉 ¡Cuenta creada con éxito para {st.session_state['reg_name']}!")
        st.write("Tu usuario es tu número de cédula. Ya puedes cerrar esta ventana y utilizar tus nuevas credenciales para iniciar sesión en el Portal de Empleados.")
        st.button("Cerrar Ventana", type="primary", use_container_width=True, on_click=finish_process)

def page_login():
    # Global CSS injection for subtle polish:
    st.markdown("""
        <style>
        .stButton>button {
            border-radius: 8px;
            transition: all 0.2s ease-in-out;
        }
        .stButton>button:hover {
            transform: translateY(-2px);
            box-shadow: 0 4px 8px rgba(0,0,0,0.1);
        }
        /* Ajustar ancho máximo de las notificaciones */
        .stAlert {
            border-radius: 8px;
        }
        </style>
    """, unsafe_allow_html=True)

    st.markdown("<br><br>", unsafe_allow_html=True)
    _, col_center, _ = st.columns([1, 2, 1])
    
    with col_center:
        with st.container(border=True):
            st.markdown("<h1 style='text-align: center; font-size: 3.5rem; margin-bottom: 0; color: #0D6EFD;'>Dolormed</h1>", unsafe_allow_html=True)
            st.markdown("<p style='text-align: center; color: gray; font-size: 1.1rem; margin-top: 0;'>Portal Web de Empleados y Administración</p>", unsafe_allow_html=True)
            st.markdown("<br>", unsafe_allow_html=True)

            tab1, tab2 = st.tabs(["🔒 Ingreso Administrativo", "🧑‍⚕️ Portal de Empleados"])

            with tab1:
                st.write("**Credenciales Base:**")
                username = st.text_input("Usuario Administrativo")
                password = st.text_input("Contraseña", type="password")
                st.markdown("<br>", unsafe_allow_html=True)

                if st.button("Ingresar al Sistema", type="primary", use_container_width=True):
                    user = verify_login(username.strip(), password)
                    if user:
                        st.session_state["user"] = user
                        st.success(f"¡Bienvenido, {user['full_name']}!")
                        import time
                        time.sleep(0.5)
                        st.rerun()
                    else:
                        st.error("❌ Credenciales inválidas o acceso deshabilitado.")

            with tab2:
                st.write("**Acceso por Documento de Identidad:**")
                
                cedula_log = st.text_input("Número de Cédula de Ciudadanía", key="emp_login_ced")
                pw_log = st.text_input("Contraseña Personal", type="password", key="emp_login_pw")
                st.markdown("<br>", unsafe_allow_html=True)
                
                if st.button("Ingresar al Portal", type="primary", use_container_width=True):
                    if cedula_log.strip() and pw_log:
                        user = verify_login(cedula_log.strip(), pw_log)
                        if user:
                            st.session_state["user"] = user
                            st.success(f"Acceso exitoso: {user['full_name']}")
                            import time
                            time.sleep(0.5)
                            st.rerun()
                        else:
                            st.error("❌ Credenciales incorrectas.")
                    else:
                        st.warning("⚠️ Debes digitar tu número de documento completo y la contraseña.")
                        
                st.divider()
                st.write("¿Es tu primera vez entrando al portal digital?")
                if st.button("Registrar / Asignar mi primera Contraseña 🔑", use_container_width=True):
                    register_employee_dialog()


def page_dashboard():
    st.title("📊 Panel Principal - Dolormed")
    st.write("Resumen rápido del sistema de Recursos Humanos.")
    
    conn = db_conn()
    cur = conn.cursor()
    
    # KPIs
    cur.execute("SELECT COUNT(*) FROM employees")
    total_empleados = cur.fetchone()[0]
    
    cur.execute("SELECT COUNT(*) FROM leave_requests WHERE status LIKE 'PENDING%'")
    novedades_pend = cur.fetchone()[0]
    
    cur.execute("SELECT COUNT(*) FROM attendance_raw WHERE date(ts) = date('now', 'localtime') AND is_ignored = 0")
    marcaciones_hoy = cur.fetchone()[0]
    
    col1, col2, col3 = st.columns(3)
    with col1:
        with st.container(border=True):
            st.metric("👥 Empleados Activos", total_empleados, "Base de datos")
    with col2:
        with st.container(border=True):
            st.metric("⏱️ Marcaciones de Hoy", marcaciones_hoy, "Actividad de red")
    with col3:
        with st.container(border=True):
            st.metric("🔔 Permisos Pendientes", novedades_pend, "- Requieren revisión", delta_color="inverse")
    
    st.markdown("---")
    
    # Gráfico de Llegadas Tarde por Área (Este Mes)
    st.subheader("⏱️ Minutos de Retraso por Área (Mes Actual)")
    st.write("Cálculo dinámico cruzando horarios oficiales con marcaciones del biométrico.")
    
    try:
        summary_df, _ = compute_month_lateness(date.today().year, date.today().month)
        
        if summary_df.empty:
            st.success("¡Excelente! No hay tardanzas acumuladas este mes.")
        else:
            nombres = pd.read_sql_query("SELECT user_id, department FROM employees", conn)
            
            merged = pd.merge(summary_df, nombres, on="user_id", how="left")
            
            def get_main_area(dept):
                if not dept or pd.isna(dept): return "Sin Área"
                if " - " in str(dept): return str(dept).split(" - ")[0]
                return str(dept)
                
            merged['Area'] = merged['department'].apply(get_main_area)
            
            area_tarde = merged.groupby('Area')['minutos_tarde_total'].sum().reset_index()
            area_tarde = area_tarde[area_tarde['minutos_tarde_total'] > 0]
            
            if not area_tarde.empty:
                fig_tarde = px.bar(area_tarde, x='Area', y='minutos_tarde_total', color='Area',
                             labels={'Area': 'Área', 'minutos_tarde_total': 'Minutos Acumulados'},
                             title="")
                st.plotly_chart(fig_tarde, use_container_width=True)
            else:
                st.success("¡Excelente! No hay tardanzas acumuladas este mes.")
                
    except Exception as e:
        st.error(f"No se pudo cargar el gráfico de tardanzas: {e}")

    st.markdown("---")
    
    # Gráfico de Marcaciones Recientes (Últimos 7 días)
    st.subheader("📈 Actividad del Biométrico (Últimos 7 Días)")
    df_act = pd.read_sql_query("""
        SELECT date(ts) as fecha, COUNT(*) as cantidad 
        FROM attendance_raw 
        WHERE date(ts) >= date('now', '-7 days') AND is_ignored = 0
        GROUP BY date(ts)
        ORDER BY date(ts)
    """, conn)
    
    if not df_act.empty:
        fig = px.bar(df_act, x='fecha', y='cantidad', labels={'fecha':'Fecha', 'cantidad':'Marcaciones Totales'}, title="", color_discrete_sequence=['#0D6EFD'])
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No hay suficientes datos recientes para mostrar el gráfico.")

    st.markdown("---")
    st.subheader("🔔 Panel de Auto-Auditoría (Alertas RRHH)")
    st.write("El sistema analiza posibles incidencias para que no se te escapen.")
    
    col_a1, col_a2 = st.columns(2)
    with col_a1:
        st.markdown("**🚨 Posibles Faltas de Ayer**")
        yesterday = (date.today() - timedelta(days=1)).isoformat()
        # Query attendance for yesterday
        conn = db_conn()
        # Get employees that had a schedule yesterday but did not punch
        # Optimization: We check shift_assignments directly
        missing_query = """
            SELECT e.full_name, sa.user_id 
            FROM shift_assignments sa
            JOIN employees e ON sa.user_id = e.user_id
            WHERE sa.week_start = ? AND sa.dow = ? 
            AND sa.user_id NOT IN (
                SELECT user_id FROM attendance_raw WHERE date(ts) = ? AND is_ignored = 0
            )
            AND sa.user_id NOT IN (
                SELECT user_id FROM exceptions WHERE date = ?
            )
        """
        # week_start for yesterday
        y_date = date.today() - timedelta(days=1)
        y_ws = (y_date - timedelta(days=y_date.weekday())).isoformat()
        y_dow = y_date.weekday()
        
        miss_df = pd.read_sql_query(missing_query, conn, params=(y_ws, y_dow, yesterday, yesterday))
        
        if miss_df.empty:
            st.success(f"Todo en orden. No hay faltas detectadas para el {yesterday}.")
        else:
            for _, r in miss_df.iterrows():
                st.error(f"Falta ayer: {r['full_name']} (ID: {r['user_id']})")
                
    with col_a2:
        st.markdown("**⚠️ Empleados sin Turno Asignado**")
        this_week_start = (date.today() - timedelta(days=date.today().weekday())).isoformat()
        no_shift_query = """
            SELECT full_name, user_id
            FROM employees 
            WHERE user_id NOT IN (
                SELECT DISTINCT user_id FROM shift_assignments WHERE week_start = ?
            )
        """
        no_sch_df = pd.read_sql_query(no_shift_query, conn, params=(this_week_start,))
        conn.close()
        
        if no_sch_df.empty:
            st.success("Toda la planilla tiene turnos asignados esta semana.")
        else:
            if len(no_sch_df) > 10:
                st.warning(f"Hay {len(no_sch_df)} empleados sin asignación de horario esta semana.")
            else:
                for _, r in no_sch_df.iterrows():
                    st.warning(f"Sin horario: {r['full_name']} (ID: {r['user_id']})")


@st.dialog("✏️ Editar Usuario", width="large")
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


@st.dialog("⚙️ Editar Reloj Biométrico")
def edit_device_dialog(device_idx, devices_list):
    if device_idx is not None:
        d = devices_list[device_idx]
        is_new = False
    else:
        d = {"name": "", "ip": "", "port": 4370, "password": 0, "timeout": 10}
        is_new = True
        
    new_name = st.text_input("Nombre Visual", value=d.get("name", ""))
    new_ip = st.text_input("Dirección IP", value=d.get("ip", ""))
    
    col1, col2 = st.columns(2)
    with col1:
        new_port_str = st.text_input("Puerto (Default 4370)", value=str(d.get("port", 4370)))
    with col2:
        new_pwd_str = st.text_input("Contraseña", value=str(d.get("password", 0)))
        
    new_timeout = st.number_input("Timeout (s)", value=int(d.get("timeout", 10)), min_value=1, step=1)
    
    st.markdown("---")
    
    c1, c2 = st.columns(2)
    with c1:
        if st.button("💾 Guardar Cambios", type="primary", use_container_width=True):
            if not new_ip.strip():
                st.error("La Dirección IP es obligatoria.")
                return
                
            try:
                port_val = int(new_port_str)
            except ValueError:
                st.error("El puerto debe ser numérico.")
                return
                
            try:
                pwd_val = int(new_pwd_str)
            except ValueError:
                st.error("La contraseña debe ser numérica.")
                return
                
            updated_d = {
                "name": new_name.strip() if new_name.strip() else new_ip.strip(),
                "ip": new_ip.strip(),
                "port": port_val,
                "password": pwd_val,
                "timeout": int(new_timeout)
            }
            
            if is_new:
                devices_list.append(updated_d)
            else:
                devices_list[device_idx] = updated_d
                
            if save_devices(devices_list):
                st.success("✅ Dispositivo guardado correctamente. Cerrando...")
                import time
                time.sleep(0.75)
                if "devices_table" in st.session_state:
                    del st.session_state["devices_table"]
                st.rerun()
            else:
                st.error("Error al guardar en devices.yaml.")
                
    with c2:
        if not is_new:
            del_confirm = st.checkbox("Confirmar eliminación")
            if st.button("🗑️ Eliminar Reloj", type="secondary", disabled=not del_confirm, use_container_width=True):
                devices_list.pop(device_idx)
                if save_devices(devices_list):
                    st.success("🗑️ Dispositivo eliminado. Cerrando...")
                    import time
                    time.sleep(0.75)
                    if "devices_table" in st.session_state:
                        del st.session_state["devices_table"]
                    st.rerun()
                else:
                    st.error("Error al guardar en devices.yaml.")


def page_sync():
    require_role("admin", "nomina")
    st.title("🔄 Sincronización Biométrica")
    
    tab_sync, tab_conf = st.tabs(["⏬ Sincronizar Datos", "⚙️ Configurar Dispositivos"])
    
    with tab_sync:
        st.write("Selecciona los relojes de los que deseas descargar las marcaciones recientes.")

        devices = load_devices()
        if not devices:
            st.error("No hay dispositivos configurados. Ve a la pestaña '⚙️ Configurar Dispositivos' para crearlos.")
        else:
            with st.expander("🛠️ Lista de Relojes Biométricos Disponibles", expanded=True):
                st.write("Marca o desmarca los dispositivos que quieras sincronizar:")
                
                selected_devices = []
                for d in devices:
                    label = f"📱 {d.get('name', d['ip'])} (IP: {d['ip']})"
                    if st.checkbox(label, value=True, key=f"chk_{d['ip']}"):
                        selected_devices.append(d)
                        
                st.markdown("<br>", unsafe_allow_html=True)
                col_btn1, col_btn2 = st.columns(2)
                with col_btn1:
                    btn_sync = st.button("Descargar Marcaciones de los Relojes Seleccionados", type="primary", use_container_width=True)
                with col_btn2:
                    btn_set_time = st.button("Sincronizar Hora Biométricos", use_container_width=True)

            if btn_set_time:
                if not selected_devices:
                    st.warning("Debes dejar marcado al menos un reloj para hacer la sincronización.")
                else:
                    for d in selected_devices:
                        label = f"{d.get('name', d['ip'])} ({d['ip']})"
                        with st.spinner(f"Ajustando hora en: {label}"):
                            success, err = sync_device_time(d)
                            if success:
                                st.success(f"{label} ✅ Hora sincronizada con el servidor.")
                            else:
                                st.error(f"{label} ❌ Error de conexión al sincronizar: {err}")

            if btn_sync:
                if not selected_devices:
                    st.warning("Debes dejar marcado al menos un reloj para hacer la descarga.")
                else:
                    total_inserted = 0
                    total_skipped = 0

                    for d in selected_devices:
                        label = f"{d.get('name', d['ip'])} ({d['ip']})"

                        with st.spinner(f"Conectando y descargando datos de: {label}"):
                            rows, err = download_attendance_from_device(d)
                            if err:
                                st.error(f"{label} ❌ Error de conexión: {err}")
                                continue

                            ins, skp = upsert_attendance(rows)
                            total_inserted += ins
                            total_skipped += skp
                            st.success(f"{label} ✅ Correcto (Nuevos: {ins} | Duplicados ignorados: {skp} | Total leídos: {len(rows)})")

                    st.info(f"**RESUMEN TOTAL** -> Nuevas marcaciones: **{total_inserted}** | Ignoradas: **{total_skipped}**")

    with tab_conf:
        st.write("Agrega, edita o elimina los relojes biométricos de tu red.")
        
        devices_config = load_devices()
        
        cbtn1, _ = st.columns([1, 2])
        with cbtn1:
            if st.button("➕ Agregar Nuevo Reloj", type="primary", use_container_width=True):
                edit_device_dialog(None, devices_config)
                
        if not devices_config:
            st.info("No hay dispositivos registrados en el sistema. Haz clic en 'Agregar Nuevo Reloj'.")
            
        else:
            df_dev = pd.DataFrame(devices_config)
            if "name" not in df_dev.columns: df_dev["name"] = df_dev["ip"]
            if "port" not in df_dev.columns: df_dev["port"] = 4370
            if "timeout" not in df_dev.columns: df_dev["timeout"] = 10
            if "password" not in df_dev.columns: df_dev["password"] = 0
                
            df_show = df_dev[["name", "ip", "port", "password", "timeout"]]
            df_show.columns = ["Nombre Visual", "Dirección IP", "Puerto", "Contraseña", "Timeout(s)"]
            
            st.info("💡 Selecciona un reloj en la tabla de abajo para poder editarlo o eliminarlo.")
            event = st.dataframe(
                df_show, 
                use_container_width=True, 
                hide_index=True, 
                on_select="rerun", 
                selection_mode="single-row", 
                key="devices_table"
            )
            
            if len(event.selection.rows) > 0:
                row_idx = event.selection.rows[0]
                st.session_state.devices_table.selection.rows.clear()
                edit_device_dialog(row_idx, devices_config)


def page_schedules():
    require_role("admin")
    st.title("🕒 Maestro de Horarios")
    st.write("Configura y administra las rejillas horarias base para los empleados de Dolormed.")

    tab1, tab2, tab3, tab4 = st.tabs(["Horarios Actuales", "Carga por Códigos (CSV)", "Carga Detallada (CSV)", "Generador Automático"])

    with tab1:
        st.subheader("Matriz de Horarios")
        st.info("Por defecto se muestran las últimas 52 semanas para evitar lentitud. Activa la opción para ver el historial completo.")
        load_all = st.checkbox("Cargar todos los registros históricos", value=False)

        conn = db_conn()
        if not load_all:
            cutoff = (date.today() - timedelta(weeks=52)).isoformat()
            sch = pd.read_sql_query(
                "SELECT week_start,dow,start_time,end_time,start_time_2,end_time_2,grace_minutes FROM schedules WHERE week_start >= ? ORDER BY week_start,dow",
                conn, params=(cutoff,)
            )
        else:
            sch = pd.read_sql_query(
                "SELECT week_start,dow,start_time,end_time,start_time_2,end_time_2,grace_minutes FROM schedules ORDER BY week_start,dow",
                conn
            )
        conn.close()

        if sch.empty:
            st.warning("No hay horarios configurados en el sistema.")
        else:
            max_edit_rows = 1000
            if len(sch) > max_edit_rows:
                st.warning(f"Mostrando preview de {max_edit_rows} filas debido al tamaño ({len(sch)} total). Descarga el CSV para editar masivamente.")
                st.dataframe(sch.head(max_edit_rows), use_container_width=True, hide_index=True)
                csv_bytes = sch.to_csv(index=False).encode("utf-8")
                st.download_button("📥 Descargar Tabla Completa (CSV)", data=csv_bytes, file_name="horarios_completos.csv", mime="text/csv")
            else:
                edited = st.data_editor(sch, num_rows="dynamic", use_container_width=True)
                if st.button("Guardar Cambios en Pantalla", type="primary"):
                    try:
                        upsert_schedule_df(edited)
                        st.success("Cambios aplicados correctamente.")
                    except Exception as e:
                        st.error(f"Error: {e}")

            st.write("---")
            if st.button("Asincronizar (Auto-asignar) Turnos base"):
                count = auto_assign_shifts_from_schedules()
                st.success(f"Proceso concluido. {count} turnos re-asignados a perfiles.")


    with tab2:
        st.subheader("Carga Rápida por Códigos")
        st.write("Sube tu plantilla de programación usando códigos simples (`M, T, N, OFICINA, L, etc.`).")
        st.markdown("**Columnas requeridas:** `user_id, week_start, dow, shift_code`")
        
        csv_shifts = st.file_uploader("Arrastra tu archivo CSV aquí...", type=["csv"], key="shifts_code")
        if csv_shifts is not None:
            df_shifts = pd.read_csv(csv_shifts)
            try:
                result = upsert_shifts_from_code_csv(df_shifts)
                st.success(f"✅ Se han procesado {result['assigned']} asignaciones.")
                if result['skipped_holidays']:
                    st.info(f"⏭️ {result['skipped_holidays']} turnos ignorados por reglas de festivos del perfil.")
                if result['errors']:
                    with st.expander("⚠️ Ver lista de errores encontrados"):
                        for err in result['errors']:
                            st.write(f"- {err}")
            except Exception as e:
                st.error(f"El archivo tiene un formato inválido: {e}")


    with tab3:
        st.subheader("Carga y Predeterminados")
        path = default_schedules_path()
        if os.path.exists(path):
            st.success(f"Platilla por defecto activa en el sistema.")
            if st.button("Forzar Restauración de Plantilla"):
                try:
                    df_def = pd.read_csv(path)
                    upsert_schedule_df(df_def)
                    st.success("Restauración completa.")
                except Exception as e:
                    st.error(f"Error: {e}")
        else:
            st.info("Sin plantilla base configurada.")

        st.markdown("**Sube una actualización manual:** Columnas: `week_start,dow,start_time,end_time...`")
        csv_file = st.file_uploader("Archivo de horarios absolutos", type=["csv"], key="sched_abs")
        if csv_file is not None:
            df = pd.read_csv(csv_file)
            try:
                upsert_schedule_df(df)
                st.success("Registros procesados.")
                if st.button("Establecer como Plantilla Definitiva"):
                    os.makedirs(DATA_DIR, exist_ok=True)
                    df.to_csv(default_schedules_path(), index=False)
                    st.success("Guardado como plantilla por defecto.")
            except Exception as e:
                st.error(f"Error: {e}")

    with tab4:
        st.subheader("Asistente Rotativo Semanal")
        colA, colB = st.columns(2)
        with colA:
            gen_year = st.number_input("Año", min_value=2020, max_value=2100, value=date.today().year)
            pattern_str = st.text_input("Patrón Horario (ej. 08:00,07:30)", value="08:00,07:30")
        with colB:
            gen_month = st.number_input("Mes", min_value=1, max_value=12, value=date.today().month)
            grace = st.number_input("Tolerancia (Mins)", min_value=0, value=10)

        if st.button("Procesar Mes Completo"):
            try:
                pattern = [s.strip() for s in pattern_str.split(",") if s.strip()]
                df = generate_rotating_schedule(int(gen_year), int(gen_month), pattern, int(grace))
                if df.empty:
                    st.warning("Verifica los parámetros. No se generó data.")
                else:
                    upsert_schedule_df(df)
                    st.success(f"Batería de {len(df)} horarios rotativos insertada.")
            except Exception as e:
                st.error(f"Error interno: {e}")

    ensure_schedules_columns()

@st.dialog("✏️ Editar Marcación", width="large")
def edit_attendance_dialog(record_id: int):
    conn = db_conn()
    df_rec = pd.read_sql_query("""
        SELECT a.*, COALESCE(e.full_name, 'Sin registrar') as full_name
        FROM attendance_raw a 
        LEFT JOIN employees e ON a.user_id = e.user_id
        WHERE a.id = ?
    """, conn, params=(record_id,))
    
    if df_rec.empty:
        st.error("No se encontró el registro.")
        conn.close()
        return
        
    rec = df_rec.iloc[0]
    
    st.markdown(f"**Usuario:** {rec['full_name']} (ID: {rec['user_id']})")
    st.markdown(f"**Dispositivo:** {rec['device_name']} ({rec['device_ip']})")
    
    try:
        current_dt = datetime.fromisoformat(rec['ts'].replace(" ", "T"))
    except ValueError:
        # Fallback if format is unexpected
        current_dt = datetime.strptime(rec['ts'], "%Y-%m-%d %H:%M:%S")

    col1, col2 = st.columns(2)
    with col1:
        new_date = st.date_input("Fecha Marcación", value=current_dt.date())
    with col2:
        new_time = st.time_input("Hora Marcación", value=current_dt.time(), step=60)
        
    current_punch = int(rec['punch'])
    punch_opts = {0: "0 - Entrada", 1: "1 - Salida", 2: "2 - Break In", 3: "3 - Break Out", 4: "4 - OT In", 5: "5 - OT Out"}
    punch_keys = list(punch_opts.keys())
    
    punch_idx = punch_keys.index(current_punch) if current_punch in punch_keys else 0
    new_punch_key = st.selectbox("Tipo de Marcación (Punch)", options=punch_keys, format_func=lambda x: punch_opts.get(x, str(x)), index=punch_idx)
    
    st.markdown("---")
    
    c1, c2 = st.columns(2)
    with c1:
        submitted = st.button("Guardar Cambios", type="primary", use_container_width=True)
    with c2:
        confirm_del = st.checkbox("Confirmar eliminación", key=f"del_att_{record_id}")
        submitted_del = st.button("🗑️ Eliminar Registro", type="secondary", disabled=not confirm_del, use_container_width=True)

    if submitted:
        new_ts = datetime.combine(new_date, new_time).isoformat(sep=" ", timespec="seconds")
        try:
            cur = conn.cursor()
            is_manual = rec.get("is_manual", 0)
            if is_manual == 1:
                cur.execute("""
                    UPDATE attendance_raw 
                    SET ts = ?, punch = ? 
                    WHERE id = ?
                """, (new_ts, new_punch_key, record_id))
            else:
                cur.execute("UPDATE attendance_raw SET is_ignored = 1 WHERE id = ?", (record_id,))
                cur.execute("""
                    INSERT INTO attendance_raw (device_name, device_ip, user_id, ts, status, punch, uid, downloaded_at, is_ignored, is_manual)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, 1)
                """, (str(rec['device_name']), str(rec['device_ip']), str(rec['user_id']), new_ts, int(rec['status']), int(new_punch_key), int(rec['uid']), str(rec['downloaded_at'])))
            conn.commit()
            log_audit("EDIT_ATTENDANCE", f"Marcación #{record_id} modif: {rec['ts']} -> {new_ts}")
            st.success("✅ Marcación actualizada. Cerrando...")
            import time as time_mod
            time_mod.sleep(0.75)
            if "view_attendance_table" in st.session_state:
                del st.session_state["view_attendance_table"]
            st.rerun()
        except sqlite3.IntegrityError:
            st.error("Error: Ya existe un registro idéntico (mismo usuario, fecha, hora y tipo).")
        except Exception as e:
            st.error(f"Error al actualizar: {e}")
        finally:
            conn.close()

    if submitted_del:
        try:
            cur = conn.cursor()
            is_manual = rec.get("is_manual", 0)
            if is_manual == 1:
                cur.execute("DELETE FROM attendance_raw WHERE id = ?", (record_id,))
            else:
                cur.execute("UPDATE attendance_raw SET is_ignored = 1 WHERE id = ?", (record_id,))
            conn.commit()
            log_audit("DELETE_ATTENDANCE", f"Marcación #{record_id} eliminada/ignorada.")
            st.success("🗑️ Registro eliminado. Cerrando...")
            import time as time_mod
            time_mod.sleep(0.75)
            if "view_attendance_table" in st.session_state:
                del st.session_state["view_attendance_table"]
            st.rerun()
        except Exception as e:
            st.error(f"Error al eliminar: {e}")
        finally:
            conn.close()
            
    if conn:
        try:
            conn.close()
        except:
            pass

def page_view_attendance():
    require_role("admin", "nomina")
    st.title("🗂️ Visor Multidimensional de Marcaciones")
    st.write("Consulta y exporta la analítica en bruto de los accesos biométricos registrados.")

    conn = db_conn()
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT device_name FROM attendance_raw ORDER BY device_name")
    devices = [row[0] for row in cur.fetchall()]
    conn.close()

    with st.expander("🔍 Buscador y Filtros Avanzados", expanded=True):
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            start_date = st.date_input("Desde el", value=date.today() - timedelta(days=7))
        with col2:
            end_date = st.date_input("Hasta el", value=date.today())
        with col3:
            selected_device = st.selectbox("Reloj de Origen", options=["Todos los Dispositivos"] + devices)
        with col4:
            user_filter = st.text_input("Filtrar Empleado (DNI o Nombre)")

    conn = db_conn()
    start_dt = datetime.combine(start_date, time(0, 0))
    end_dt = datetime.combine(end_date + timedelta(days=1), time(0, 0))

    query = """
        SELECT 
            a.id,
            a.device_name, 
            a.device_ip, 
            a.user_id,
            COALESCE(e.full_name, 'Sin registrar') as employee_name,
            a.ts, 
            a.status, 
            a.punch, 
            a.downloaded_at
        FROM attendance_raw a
        LEFT JOIN employees e ON a.user_id = e.user_id
        WHERE a.ts >= ? AND a.ts < ? AND a.is_ignored = 0
    """
    params = [start_dt.isoformat(sep=" ", timespec="seconds"), 
              end_dt.isoformat(sep=" ", timespec="seconds")]

    if selected_device != "Todos los Dispositivos":
        query += " AND a.device_name = ?"
        params.append(selected_device)

    if user_filter.strip():
        query += " AND (a.user_id LIKE ? OR e.full_name LIKE ?)"
        search_term = f"%{user_filter.strip()}%"
        params.extend([search_term, search_term])

    query += " ORDER BY a.ts DESC"

    df = pd.read_sql_query(query, conn, params=params)
    conn.close()

    if df.empty:
        st.warning("No hay marcaciones en el rango seleccionado.")
    else:
        df = df.rename(columns={
            "id": "ID",
            "device_name": "Dispositivo",
            "device_ip": "IP",
            "user_id": "ID Usuario",
            "employee_name": "Nombre",
            "ts": "Hora Marcación",
            "status": "Status",
            "punch": "Tipo",
            "downloaded_at": "Descargado en"
        })
        
        # Mapear los valores de 0 y 1 a textos comprensibles
        punch_map = {0: "Entrada", 1: "Salida", 2: "Break In", 3: "Break Out", 4: "OT In", 5: "OT Out"}
        df["Tipo"] = df["Tipo"].map(punch_map).fillna(df["Tipo"])

        st.info(f"💡 Selecciona una marcación para editar manualmente su hora o eliminarla. Total de registros: {len(df)}")
        event = st.dataframe(df, use_container_width=True, hide_index=True, on_select="rerun", selection_mode="single-row", key="view_attendance_table")
        
        if len(event.selection.rows) > 0:
            row_idx = event.selection.rows[0]
            selected_id = int(df.iloc[row_idx]["ID"])
            # Clear selection so it doesn't pop up again
            st.session_state.view_attendance_table.selection.rows.clear()
            edit_attendance_dialog(selected_id)

        excel_bytes = io.BytesIO()
        with pd.ExcelWriter(excel_bytes, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name="Marcaciones")
        excel_bytes.seek(0)

        st.download_button(
            "📥 Descargar registros en Excel",
            data=excel_bytes.getvalue(),
            file_name=f"marcaciones_{start_date.isoformat()}_{end_date.isoformat()}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

@st.dialog("✏️ Editar Empleado", width="large")
def edit_employee_dialog(user_id):
    conn = db_conn()
    emp_df = pd.read_sql_query("SELECT full_name, department, profile_id FROM employees WHERE user_id = ?", conn, params=(user_id,))
    profiles_df = pd.read_sql_query("SELECT profile_id, name FROM profiles ORDER BY name", conn)
    conn.close()
    
    if emp_df.empty:
        st.error("No se encontró el empleado.")
        return
        
    e = emp_df.iloc[0]
    
    profile_opts = profiles_df["name"].tolist()
    
    current_prof_id = e.get("profile_id")
    current_prof_name = ""
    if pd.notna(current_prof_id):
        prof_row = profiles_df[profiles_df['profile_id'] == current_prof_id]
        if not prof_row.empty:
            current_prof_name = prof_row.iloc[0]["name"]
            
    prof_index = profile_opts.index(current_prof_name) if current_prof_name in profile_opts else 0
    
    current_dept = str(e['department']) if pd.notna(e['department']) else ""
    def_area = list(AREA_MAPPING.keys())[0]
    def_subarea = ""
    
    if " - " in current_dept:
        parts = current_dept.split(" - ", 1)
        if parts[0] in AREA_MAPPING:
            def_area = parts[0]
            def_subarea = parts[1]
    else:
        for k, v in AREA_MAPPING.items():
            if current_dept in v:
                def_area = k
                def_subarea = current_dept
                break
        else:
            if current_dept in AREA_MAPPING:
                def_area = current_dept
                
    st.write(f"**DNI / ID Biométrico:** {user_id}")
    new_name = st.text_input("Nombre Completo*", value=e['full_name'])
    
    area_opts = list(AREA_MAPPING.keys())
    area_idx = area_opts.index(def_area) if def_area in area_opts else 0
    new_area = st.selectbox("Área Principal*", options=area_opts, index=area_idx)
    
    subarea_opts = AREA_MAPPING[new_area]
    subarea_idx = subarea_opts.index(def_subarea) if def_subarea in subarea_opts else 0
    new_subarea = st.selectbox("Sub-área / Departamento*", options=subarea_opts, index=subarea_idx)
    
    new_prof_name = st.selectbox("Perfil Asignado*", options=profile_opts, index=prof_index)
    
    st.markdown("---")
    submitted = st.button("Guardar Cambios", type="primary")
    
    if submitted:
        if not new_name.strip() or not new_area or not new_subarea:
            st.error("Nombre, Área y Sub-área son obligatorios.")
            return
            
        new_prof_id = None
        prof_row = profiles_df[profiles_df['name'] == new_prof_name]
        if not prof_row.empty:
            new_prof_id = int(prof_row.iloc[0]["profile_id"])
            
        final_dept = f"{new_area} - {new_subarea}"
        
        conn = db_conn()
        cur = conn.cursor()
        try:
            cur.execute("""
                UPDATE employees 
                SET full_name = ?, department = ?, profile_id = ?
                WHERE user_id = ?
            """, (new_name.strip(), final_dept, new_prof_id, str(user_id)))
            conn.commit()
            st.success("✅ Cambios guardados correctamente.")
            st.rerun()
        except Exception as exc:
            st.error(f"Error al guardar los cambios: {exc}")
        finally:
            conn.close()
            
    with st.expander("🚨 Zona de Peligro - Eliminar Empleado"):
        st.warning("Esta acción es irreversible y eliminará el registro de empleado y sus accesos si existen.")
        confirm_del = st.checkbox("Entiendo que esta acción es permanente.", key=f"del_emp_{user_id}")
        
        if st.button("🗑️ Eliminar Definitivamente", type="primary", disabled=not confirm_del):
            conn = db_conn()
            cur = conn.cursor()
            try:
                cur.execute("DELETE FROM employees WHERE user_id = ?", (str(user_id),))
                cur.execute("DELETE FROM users_app WHERE username = ?", (str(user_id),))
                conn.commit()
                st.success("🗑️ Empleado eliminado.")
                st.rerun()
            except Exception as exc:
                st.error(f"Error al eliminar: {exc}")
            finally:
                conn.close()


def page_employees():
    require_role("admin")
    st.title("👨‍💼 Directorio de Empleados")
    st.write("Administra la plantilla de personal y asocia los perfiles de Dolormed.")

    # Mostrar perfiles disponibles
    conn = db_conn()
    profiles_df = pd.read_sql_query("SELECT profile_id, name, description, works_holidays FROM profiles ORDER BY name", conn)
    conn.close()
    
    with st.expander("ℹ️ Ver Perfiles y Reglas Base Creados en el Sistema"):
        if not profiles_df.empty:
            profiles_df['works_holidays'] = profiles_df['works_holidays'].apply(lambda x: '✅ Sí' if x == 1 else '❌ No')
            profiles_df.columns = ["ID", "Nombre Perfil", "Descripción", "¿Trabaja Festivos?"]
            st.dataframe(profiles_df, use_container_width=True, hide_index=True)
        else:
            st.info("No hay perfiles configurados.")

    st.markdown("---")
    tab1, tab2, tab3 = st.tabs(["👥 Plantilla Activa", "➕ Crear Empleado Manual", "📥 Importar CSV masivo"])

    with tab1:
        st.subheader("Directorio Actual")
        st.write("Selecciona una fila para editar la información del empleado.")
        conn = db_conn()
        emp = pd.read_sql_query("""
            SELECT e.user_id, e.full_name, e.department, COALESCE(p.name, 'Sin asignar') as profile, e.created_at
            FROM employees e
            LEFT JOIN profiles p ON e.profile_id = p.profile_id
            ORDER BY e.user_id
        """, conn)
        conn.close()
        
        if emp.empty:
            st.warning("El directorio está vacío.")
        else:
            emp.columns = ["DNI / ID Biométrico", "Nombre Completo", "Área / Departamento", "Perfil Asignado", "Fecha Registro"]
            
            # Filtro rápido
            deptss = ["Todos"] + list(emp["Área / Departamento"].dropna().unique())
            filtro_dep = st.selectbox("Filtrar por Departamento:", deptss)
            
            df_show = emp if filtro_dep == "Todos" else emp[emp["Área / Departamento"] == filtro_dep]
            
            st.metric("Total en vista", len(df_show))
            event = st.dataframe(df_show, use_container_width=True, hide_index=True, on_select="rerun", selection_mode="single-row")
            
            selected_rows = event.selection.rows
            if selected_rows:
                idx = selected_rows[0]
                selected_user_id = df_show.iloc[idx]["DNI / ID Biométrico"]
                edit_employee_dialog(selected_user_id)

    with tab2:
        st.subheader("Registrar Empleado Manualmente")
        st.info("Utiliza este formulario para crear un registro individual en la base de datos de manera inmediata.")
        c1, c2 = st.columns(2)
        with c1:
            e_id = st.text_input("Número de Documento (DNI)*", placeholder="Ej: 100123456")
            e_name = st.text_input("Nombre Completo*", placeholder="Apellidos y Nombres")
            e_email = st.text_input("Correo Electrónico (Opcional)", placeholder="usuario@dolormed.com")
        with c2:
            e_area_main = st.selectbox("Área Principal*", options=list(AREA_MAPPING.keys()))
            e_subarea = st.selectbox("Sub-área / Departamento*", options=AREA_MAPPING[e_area_main])
            e_prof = st.selectbox("Perfil Asignado*", options=[p for p in profiles_df["Nombre Perfil"].tolist()])
            
        submitted = st.button("Crear Empleado", type="primary")

        if submitted:
            e_dept = f"{e_area_main} - {e_subarea}"
            if not e_id.strip() or not e_name.strip() or not e_dept.strip():
                st.error("Por favor completa los campos obligatorios (*).")
            else:
                try:
                    df_new = pd.DataFrame([{
                        "user_id": e_id.strip(),
                        "full_name": e_name.strip(),
                        "email": e_email.strip(),
                        "department": e_dept.strip(),
                        "profile_id": e_prof
                    }])
                    upsert_employees_df(df_new)
                    st.success(f"✅ Empleado {e_name} creado exitosamente.")
                except Exception as e:
                    st.error(f"Fallo al registrar empleado: {e}")

    with tab3:
        st.subheader("📥 Cargar Plantilla Masiva CSV")
        st.info("Sube un archivo CSV con las columnas: `user_id, full_name, email, department` (Opcional: `profile_id` o el nombre del perfil exacto).")
        csv_file = st.file_uploader("Arrastra tu documento CSV aquí", type=["csv"], key="emp_csv")
        if csv_file is not None:
            df = pd.read_csv(csv_file)
            try:
                upsert_employees_df(df)
                st.success("✅ Base de datos de empleados actualizada satisfactoriamente.")
            except Exception as e:
                st.error(f"Fallo al procesar el documento: {e}")


def page_shifts():
    require_role("admin")
    st.title("🏭 Catálogo de Turnos Dolormed")
    st.write("Crea bloques horarios reutilizables (ej: Mañana Enfermería, Tarde, Noche, etc).")

    with st.form("create_shift"):
        sname = st.text_input("Nombre del turno")
        stime = st.text_input("Hora inicio (HH:MM)", value="08:00")
        etime = st.text_input("Hora fin (HH:MM, opcional)", value="")
        sgrace = st.number_input("Minutos de gracia", min_value=0, value=0, step=1)
        is_overnight = st.checkbox("Cruza medianoche (overnight)")
        shift_code = st.text_input("Código del turno (p.ej. M, T, N, RX1, OFICINA)")
        has_break = st.checkbox("Tiene break / horario partido")
        if has_break:
            break_start = st.text_input("Inicio de break (HH:MM)", value="12:00")
            break_end = st.text_input("Fin de break (HH:MM)", value="14:00")
        else:
            break_start = ""
            break_end = ""
        submit_shift = st.form_submit_button("Crear/Actualizar turno")

    if submit_shift:
        if not sname or not stime:
            st.error("Completa nombre y hora de inicio.")
        else:
            try:
                upsert_shift(
                    sname,
                    stime,
                    int(sgrace),
                    end_time=etime,
                    has_break=has_break,
                    break_start=break_start,
                    break_end=break_end,
                    is_overnight=is_overnight,
                    shift_code=shift_code if shift_code else None,
                )
                st.success("Turno creado/actualizado.")
            except Exception as e:
                st.error(f"Error al crear turno: {e}")

    shifts_df = get_shifts_df()
    if shifts_df.empty:
        st.warning("Aún no hay turnos definidos.")
    else:
        st.markdown("### Turnos existentes")
        st.dataframe(shifts_df, use_container_width=True)


def page_assign_shifts():
    require_role("admin")
    st.title("📝 Asignación Manual de Turnos")
    st.write("Forzar turno para empleados específicos en días concretos.")

    today = date.today()
    default_week_start = (today - timedelta(days=today.weekday()))
    
    col1, col2 = st.columns(2)
    with col1:
        week_start = st.date_input("Semana (Automáticamente toma el Lunes)", value=default_week_start)
    with col2:
        shifts_df = get_shifts_df()
        if shifts_df.empty:
            st.warning("Requiere configurar el catálogo de turnos primero.")
            return

        shift_options = {row['name']: int(row['id']) for _, row in shifts_df.iterrows()}
        sel_shift_name = st.selectbox("Turno a Aplicar", options=list(shift_options.keys()))

    conn = db_conn()
    emp_df = pd.read_sql_query("SELECT user_id, full_name, department FROM employees ORDER BY user_id", conn)
    conn.close()
    if emp_df.empty:
        st.warning("No hay empleados en el directorio.")
        return

    # department filter
    depts = sorted([d for d in emp_df['department'].unique() if d])
    dept_sel = st.selectbox("Filtrar destinatarios por área", options=["Todos los Departamentos"] + depts)

    if dept_sel != "Todos los Departamentos":
        emp_df_filtered = emp_df[emp_df['department'] == dept_sel]
    else:
        emp_df_filtered = emp_df

    st.markdown("---")
    st.write("**Selección de Personal y Días**")
    
    colA, colB = st.columns([2, 1])
    with colA:
        apply_all = st.checkbox("Asignar masivamente a todos los filtrados", value=False)
        if apply_all:
            selected_emps = emp_df_filtered['user_id'].tolist()
            st.success(f"{len(selected_emps)} empleados seleccionados.")
        else:
            selected_emps = st.multiselect(
                "Seleccionar Manualmente:",
                options=emp_df_filtered['user_id'].tolist(),
                format_func=lambda uid: f"{uid} - {emp_df_filtered[emp_df_filtered['user_id']==uid]['full_name'].values[0]}"
            )
    with colB:
        dow_sel = st.multiselect("Días Aplica", options=[0,1,2,3,4,5,6], default=[0,1,2,3,4], format_func=lambda x: ["Lunes","Martes","Miércoles","Jueves","Viernes","Sábado","Domingo"][x])

    st.markdown("<br>", unsafe_allow_html=True)
    if st.button("Aplicar Asignación Directa", type="primary", use_container_width=True):
        if not selected_emps:
            st.error("Debes incluir al menos a un empleado.")
        elif not dow_sel:
            st.error("Debes incluir al menos un día.")
        else:
            ws_iso = (week_start - timedelta(days=week_start.weekday())).isoformat()
            sid = shift_options[sel_shift_name]
            for uid in selected_emps:
                for dow in dow_sel:
                    assign_shift(uid, ws_iso, int(dow), sid)
            st.success("✅ Asignación procesada para la semana.")

    st.markdown("---")
    st.subheader("✨ Asignación Mágica (Clonar Semana)")
    st.write("Copia los turnos de una semana origen a la(s) siguiente(s) para evitar registrar uno por uno.")
    
    col_clone1, col_clone2 = st.columns(2)
    with col_clone1:
        source_week = st.date_input("Semana Base a Copiar (Automáticamente toma Lunes)", value=default_week_start - timedelta(days=7))
    with col_clone2:
        target_weeks = st.number_input("¿Semanas a generar hacia adelante?", min_value=1, max_value=12, value=1)
        
    if st.button("🚀 Iniciar Clonación", type="primary"):
        ws_source_iso = (source_week - timedelta(days=source_week.weekday())).isoformat()
        
        conn = db_conn()
        cur = conn.cursor()
        cur.execute("SELECT user_id, dow, shift_id FROM shift_assignments WHERE week_start = ?", (ws_source_iso,))
        source_assignments = cur.fetchall()
        conn.close()
        
        if not source_assignments:
            st.warning(f"No hay turnos asignados en la semana del {ws_source_iso}.")
        else:
            inserted = 0
            for i in range(1, target_weeks + 1):
                target_week_iso = (source_week - timedelta(days=source_week.weekday()) + timedelta(weeks=i)).isoformat()
                for uid, dow, shift_iid in source_assignments:
                    assign_shift(uid, target_week_iso, dow, shift_iid)
                    inserted += 1
            st.success(f"¡Magia completada! Se clonaron {inserted} asignaciones hacia {target_weeks} semana(s) destino.")



def notify_employee(user_id, subject, body):
    conn = db_conn()
    cur = conn.cursor()
    cur.execute("SELECT emp_email FROM users_app WHERE username = ?", (user_id,))
    row = cur.fetchone()
    conn.close()
    if row and row[0]:
        send_notification_email(row[0], subject, body)


@st.dialog("Detalles de Mi Solicitud (F-TH-012)")
def show_leave_request_details(req_id: int):
    conn = db_conn()
    df_req = pd.read_sql_query("SELECT * FROM leave_requests WHERE id = ?", conn, params=(req_id,))
    
    # Extraer trazabilidad de aprobadores
    df_audit = pd.read_sql_query("""
        SELECT user_id, action, timestamp 
        FROM audit_logs 
        WHERE details LIKE ? AND action LIKE 'APPROVE_%'
        ORDER BY timestamp ASC
    """, conn, params=(f"%Permiso #{req_id} %",))
    
    conn.close()
    
    if df_req.empty:
        st.error("No se encontró la solicitud.")
        return
        
    req = df_req.iloc[0]
    
    st.markdown(f"### Radicado: #{req['id']}")
    
    if req['status'] == 'APPROVED':
        st.success("✅ Esta solicitud ha sido Aprobada definitivamente.")
        html_fth012 = generate_fth012_html(req, df_audit)
        st.download_button(
            label="📄 Descargar F-TH-012 (HTML/PDF)",
            data=html_fth012,
            file_name=f"F-TH-012_{req['id']}.html",
            mime="text/html",
            type="primary",
            use_container_width=True
        )
    else:
        st.markdown(f"**Estado Actual:** `{req['status']}`")
    
    c1, c2 = st.columns(2)
    with c1:
        st.markdown(f"**Fecha de Solicitud:** {req['request_date']}")
        st.markdown(f"**Fechas de Ausencia:** {req['leave_date_start']} al {req['leave_date_end']}")
        st.markdown(f"**Remunerado:** {'✅ Sí' if req['is_paid'] else '❌ No'}")
    with c2:
        h_in = req['start_time'] if req['start_time'] else "N/A"
        h_out = req['end_time'] if req['end_time'] else "N/A"
        st.markdown(f"**Hora Salida:** {h_in}")
        st.markdown(f"**Hora Entrada:** {h_out}")
        st.markdown(f"**Tiempo Total:** {req['total_time']}")
        
    st.divider()
    st.write(f"**Motivo General:** {req['reason_type']}")
    
    st.markdown("**Mi Justificación / Detalles:**")
    st.info(req['reason_description'] if req['reason_description'] else "Sin detalles ingresados.")
    
    if not req['is_paid'] and req['how_to_makeup']:
        st.markdown("**Acuerdo de Reposición Prometido:**")
        st.warning(req['how_to_makeup'])

    if not df_audit.empty:
        st.divider()
        st.markdown("**Trazabilidad de Aprobaciones:**")
        for _, row_a in df_audit.iterrows():
            level = "Jefatura" if row_a['action'] == "APPROVE_LEAVE_L1" else "Gestión Humana"
            st.caption(f"✓ **{level}**: {row_a['user_id']} ({row_a['timestamp']})")

def page_employee_portal():
    user = st.session_state["user"]
    st.title("🧑‍⚕️ Mi Portal de Autogestión (F-TH-012)")
    st.write(f"Bienvenido/a **{user['full_name']}** | {user.get('department', 'Sin Área Definida')}")
    
    t1, t2 = st.tabs(["📝 Radicar Nuevo Permiso", "🗂️ Mis Solicitudes"])
    
    with t1:
        st.subheader("Solicitud de Permisos Laborales Y/O Personales")
        st.info("Llena el siguiente formulario digital equivalente al formato F-TH-012 físico.")
        
        with st.form("form_leave_request"):
            c1, c2 = st.columns(2)
            with c1:
                leave_dates = st.date_input("Fecha(s) del Permiso", value=[], help="Selecciona uno o varios días de ausencia.", format="YYYY-MM-DD")
                reason_type = st.selectbox("Motivo (Laboral o Personal)", ["Personal", "Laboral", "Cita Médica", "Calamidad", "Vacaciones", "Licencia"])
                is_paid = st.radio("¿Permiso Remunerado?", ["No", "Sí"], horizontal=True)
            with c2:
                time_s = st.time_input("Hora de Salida (Opcional)", value=None)
                time_e = st.time_input("Hora de Entrada (Opcional)", value=None)
                total_time = st.text_input("Tiempo Total del Permiso", placeholder="Ej: 4 Horas, 1 Día, Completo")
                
            r_desc = st.text_area("Justificación / Detalles del permiso")
            makeup = st.text_input("¿Cómo se repone el tiempo? (Dejar en blanco si es remunerado/laboral)")
            
            submitted = st.form_submit_button("Firmar y Enviar a RRHH", type="primary")
            
        if submitted:
            if not leave_dates:
                st.error("Debes seleccionar obligatoriamente al menos una fecha de inicio.")
            elif type(leave_dates) not in (tuple, list) and not leave_dates:
                 st.error("Revisa la fecha seleccionada.")
            else:
                d_start = leave_dates[0] if type(leave_dates) in (tuple, list) else leave_dates
                d_end = leave_dates[1] if (type(leave_dates) in (tuple, list) and len(leave_dates) > 1) else d_start
                str_ts = time_s.strftime("%H:%M") if time_s else ""
                str_te = time_e.strftime("%H:%M") if time_e else ""
                
                db_create_leave_request(
                    user["username"], d_start, d_end, str_ts, str_te,
                    total_time, reason_type, r_desc, makeup, is_paid == "Sí"
                )
                st.success("✅ Solicitud enviada exitosamente. Conserva tu historial en la pestaña 'Mis Solicitudes'.")
                
    with t2:
        conn = db_conn()
        df_reqs = pd.read_sql_query("""
            SELECT id as Radicado, request_date as Fecha_Solicitud, leave_date_start as Desde, leave_date_end as Hasta, reason_type as Motivo, status as Estado 
            FROM leave_requests WHERE user_id = ? ORDER BY id DESC
        """, conn, params=(user["username"],))
        conn.close()
        
        if df_reqs.empty:
            st.info("No tienes solicitudes históricas radicas.")
        else:
            st.info("💡 Haz clic en una solicitud para ver todos sus detalles.")
            
            # Using session_state for st.dataframe selection to allow clearing it
            event = st.dataframe(df_reqs, use_container_width=True, hide_index=True, on_select="rerun", selection_mode="single-row", key="emp_reqs_table")
            
            if len(event.selection.rows) > 0:
                row_idx = event.selection.rows[0]
                selected_req_id = int(df_reqs.iloc[row_idx]["Radicado"])
                # Before showing dialog, clear selection so on close it doesn't trigger again
                st.session_state.emp_reqs_table.selection.rows.clear()
                show_leave_request_details(selected_req_id)


@st.dialog("Detalles Completos de la Novedad/Permiso")
def show_exception_details(exc_id: int):
    conn = db_conn()
    
    # Obtener info basica de la novedad
    df_exc = pd.read_sql_query("""
        SELECT ex.user_id, e.full_name, ex.date, ex.type, ex.notes, ex.created_at
        FROM exceptions ex
        LEFT JOIN employees e ON ex.user_id = e.user_id
        WHERE ex.id = ?
    """, conn, params=(exc_id,))
    
    if df_exc.empty:
        st.error("No se encontró la novedad.")
        conn.close()
        return
        
    exc = df_exc.iloc[0]
    st.markdown(f"#### **Empleado:** {exc['full_name']} (ID: {exc['user_id']})")
    st.markdown(f"**Fecha Afectada:** {exc['date']} | **Tipo:** {exc['type']}")
    st.write(f"**Observación General:** {exc['notes']}")
    st.caption(f"Registrado el: {exc['created_at']}")
    
    st.divider()
    
    # Buscar si existe una solicitud digital de portal asociada
    df_req = pd.read_sql_query("""
        SELECT *
        FROM leave_requests
        WHERE user_id = ? AND status = 'APPROVED'
          AND leave_date_start <= ? AND leave_date_end >= ?
        ORDER BY id DESC LIMIT 1
    """, conn, params=(exc['user_id'], exc['date'], exc['date']))
    
    conn.close()
    
    if not df_req.empty:
        req = df_req.iloc[0]
        st.markdown("### 📄 Detalles de la Solicitud (Portal F-TH-012)")
        
        c1, c2 = st.columns(2)
        with c1:
            st.markdown(f"**Radicado:** #{req['id']}")
            st.markdown(f"**Fecha de Solicitud:** {req['request_date']}")
            st.markdown(f"**Remunerado:** {'✅ Sí' if req['is_paid'] else '❌ No'}")
        with c2:
            h_in = req['start_time'] if req['start_time'] else "N/A"
            h_out = req['end_time'] if req['end_time'] else "N/A"
            st.markdown(f"**Hora Salida:** {h_in}")
            st.markdown(f"**Hora Entrada:** {h_out}")
            st.markdown(f"**Tiempo Total:** {req['total_time']}")
            
        st.write(f"**Motivo Original:** {req['reason_type']}")
        
        st.markdown("**Justificación del Empleado:**")
        st.info(req['reason_description'] if req['reason_description'] else "Sin detalles adicionales.")
        
        if not req['is_paid'] and req['how_to_makeup']:
            st.markdown("**Acuerdo de Reposición (Tiempo):**")
            st.warning(req['how_to_makeup'])
            
        conn = db_conn()
        df_audit = pd.read_sql_query("""
            SELECT user_id, action, timestamp 
            FROM audit_logs 
            WHERE details LIKE ? AND action LIKE 'APPROVE_%'
            ORDER BY timestamp ASC
        """, conn, params=(f"%Permiso #{req['id']} %",))
        conn.close()
        
        if not df_audit.empty:
            st.divider()
            st.markdown("**Trazabilidad de Aprobaciones:**")
            for _, row_a in df_audit.iterrows():
                level = "Jefatura" if row_a['action'] == "APPROVE_LEAVE_L1" else "Gestión Humana"
                st.caption(f"✓ **{level}**: {row_a['user_id']} ({row_a['timestamp']})")
    else:
        st.info("ℹ️ Esta novedad no parece tener una solicitud digital asociada del portal de empleados (o fue ingresada manualmente).")

def page_exceptions():
    require_role("admin", "nomina", "jefe_area", "coordinador")
    st.title("🛡️ Novedades y Justificaciones")
    user = st.session_state["user"]
    conn = db_conn()

    if user["role"] in ["coordinador", "jefe_area"]:
        st.write(f"Bandeja de Aprobación para: **{user.get('managed_department') or user.get('managed_area')}**")
        
        if user["role"] == "coordinador":
            query = """
                SELECT lr.id, lr.user_id, e.full_name, lr.request_date, lr.leave_date_start, lr.leave_date_end,
                       lr.reason_type, lr.reason_description, lr.is_paid, lr.status
                FROM leave_requests lr
                JOIN employees e ON lr.user_id = e.user_id
                WHERE lr.status = 'PENDING_INMEDIATO' AND e.department = ?
                ORDER BY lr.id ASC
            """
            params = (user.get('managed_department', ''),)
        else:
            query = """
                SELECT lr.id, lr.user_id, e.full_name, lr.request_date, lr.leave_date_start, lr.leave_date_end,
                       lr.reason_type, lr.reason_description, lr.is_paid, lr.status
                FROM leave_requests lr
                JOIN employees e ON lr.user_id = e.user_id
                WHERE lr.status = 'PENDING_AREA' AND e.department LIKE ?
                ORDER BY lr.id ASC
            """
            params = (f"{user.get('managed_area', '')} - %",)
            
        df_pend = pd.read_sql_query(query, conn, params=params)
        
        if df_pend.empty:
            st.success("No hay solicitudes pendientes de revisión para tu área.")
        else:
            st.write(f"Tienes **{len(df_pend)}** solicitud(es) por revisar.")
            for _, r in df_pend.iterrows():
                with st.container(border=True):
                    cols = st.columns([3, 1])
                    with cols[0]:
                        st.markdown(f"**{r['full_name']}** (ID: {r['user_id']}) - *{r['reason_type']}*")
                        st.write(f"**Fechas:** {r['leave_date_start']} al {r['leave_date_end']} | **Remunerado:** {'Sí' if r['is_paid'] else 'No'}")
                        st.write(f"**Justificación:** {r['reason_description']}")
                    with cols[1]:
                        if st.button("👍 Aprobar", key=f"btn_acc_{r['id']}", type="primary", use_container_width=True):
                            cur = conn.cursor()
                            if user["role"] == "coordinador":
                                if r['reason_type'] in ["Vacaciones", "Día de la familia", "Votaciones", "Licencia de luto"]:
                                    next_status = 'PENDING_AREA'
                                else:
                                    next_status = 'PENDING_RRHH'
                            else:
                                next_status = 'PENDING_RRHH'
                                
                            cur.execute("UPDATE leave_requests SET status = ? WHERE id = ?", (next_status, r['id']))
                            conn.commit()
                            log_audit("APPROVE_LEAVE_L1", f"Permiso #{r['id']} ({r['reason_type']}) de {r['full_name']} pre-aprobado. Pasa a {next_status}")
                            notify_employee(r['user_id'], f"Dolormed: Novedad #{r['id']} Pre-Aprobada", f"Hola {r['full_name']},<br>Tu permiso de {r['reason_type']} fue pre-aprobado por tu jefatura. Pasa a estado: {next_status}.")
                            st.rerun()
                        if st.button("❌ Rechazar", key=f"btn_rej_{r['id']}", use_container_width=True):
                            cur = conn.cursor()
                            cur.execute("UPDATE leave_requests SET status = 'REJECTED' WHERE id = ?", (r['id'],))
                            conn.commit()
                            log_audit("REJECT_LEAVE_L1", f"Permiso #{r['id']} ({r['reason_type']}) de {r['full_name']} rechazado.")
                            notify_employee(r['user_id'], f"Dolormed: Novedad #{r['id']} Rechazada", f"Hola {r['full_name']},<br>Tu permiso de {r['reason_type']} fue RECHAZADO por tu jefatura.")
                            st.rerun()
        conn.close()
        return

    st.write("Registra permisos, incapacidades médicas o vacaciones. El sistema **no penalizará** a estos empleados en los reportes de tardanzas para los días seleccionados.")

    emp_df = pd.read_sql_query("SELECT user_id, full_name, department FROM employees ORDER BY full_name", conn)
    conn.close()

    if emp_df.empty:
        st.warning("No hay empleados en el directorio.")
        return

    tab1, tab2, tab3, tab4 = st.tabs(["📝 Registrar Novedad Manual", "📋 Listado de Novedades", "📥 Solicitudes Digitales de Empleados", "🌐 Monitoreo Global"])

    with tab1:
        with st.form("form_exceptions"):
            col1, col2 = st.columns(2)
            with col1:
                selected_emp = st.selectbox(
                    "Empleado",
                    options=emp_df['user_id'].tolist(),
                    format_func=lambda uid: f"{uid} - {emp_df[emp_df['user_id']==uid]['full_name'].values[0]}"
                )
                date_range = st.date_input("Rango de Fechas (Inicio y Fin)", value=[], help="Escoge el día de inicio y fin de la novedad.")
            with col2:
                exc_type = st.selectbox("Tipo de Novedad", ["Incapacidad Médica", "Vacaciones", "Permiso Remunerado", "Permiso NO Remunerado", "Licencia Luto/Maternidad"])
                notes = st.text_area("Observaciones", placeholder="Escribe detalles del permiso si es necesario...")
            
            submit_exc = st.form_submit_button("Guardar Novedad", type="primary")

        if submit_exc:
            if type(date_range) is not tuple and type(date_range) is not list:
                d_start = date_range
                d_end = date_range
            elif len(date_range) == 0:
                st.error("Debes seleccionar al menos una fecha (o un rango).")
                d_start = None
            else:
                d_start = date_range[0]
                d_end = date_range[1] if len(date_range) > 1 else d_start
                
            if d_start:
                delta = d_end - d_start
                for i in range(delta.days + 1):
                    day_to_log = (d_start + timedelta(days=i)).isoformat()
                    upsert_exception(selected_emp, day_to_log, exc_type, notes)
                st.success(f"Novedad registrada del {d_start} al {d_end} para el usuario {selected_emp}.")

    with tab2:
        st.info("💡 Haz clic en cualquier fila para ver los detalles completos del permiso o novedad.")
        df_exc = get_exceptions_df()
        if df_exc.empty:
            st.info("No hay novedades registradas.")
        else:
            df_exc.columns = ["ID", "Usuario", "Nombre", "Fecha", "Tipo", "Observaciones", "Registrado El"]
            event = st.dataframe(df_exc, use_container_width=True, hide_index=True, on_select="rerun", selection_mode="single-row", key="admin_exc_table")
            
            if len(event.selection.rows) > 0:
                row_idx = event.selection.rows[0]
                selected_id = int(df_exc.iloc[row_idx]["ID"])
                st.session_state.admin_exc_table.selection.rows.clear()
                show_exception_details(selected_id)

    with tab3:
        st.subheader("Bandeja de Aprobación Final de Permisos (Gestión Humana)")
        conn = db_conn()
        df_pend = pd.read_sql_query("""
            SELECT lr.id, lr.user_id, e.full_name, lr.request_date, lr.leave_date_start, lr.leave_date_end,
                   lr.reason_type, lr.reason_description, lr.is_paid, lr.status
            FROM leave_requests lr
            JOIN employees e ON lr.user_id = e.user_id
            WHERE lr.status = 'PENDING_RRHH'
            ORDER BY lr.id ASC
        """, conn)
        
        if df_pend.empty:
            st.success("No hay solicitudes pendientes de revisión final.")
        else:
            st.write(f"Tienes **{len(df_pend)}** solicitud(es) por procesar definitivamente.")
            for _, r in df_pend.iterrows():
                with st.container(border=True):
                    cols = st.columns([3, 1])
                    with cols[0]:
                        badge = "🟣 RRHH FINAL"
                        st.markdown(f"**{r['full_name']}** (ID: {r['user_id']}) - *{r['reason_type']}* | {badge}")
                        st.write(f"**Fechas:** {r['leave_date_start']} al {r['leave_date_end']} | **Remunerado:** {'Sí' if r['is_paid'] else 'No'}")
                        st.write(f"**Justificación:** {r['reason_description']}")
                    with cols[1]:
                        btn_label = "✅ Aprobar Final"
                        if st.button(btn_label, key=f"btn_acc_hr_{r['id']}", type="primary", use_container_width=True):
                            cur = conn.cursor()
                            cur.execute("UPDATE leave_requests SET status = 'APPROVED' WHERE id = ?", (r['id'],))
                            
                            # Inyectar en excepciones (eximir faltas)
                            d_start = date.fromisoformat(r['leave_date_start'])
                            d_end = date.fromisoformat(r['leave_date_end'])
                            delta = d_end - d_start
                            for i in range(delta.days + 1):
                                day_to_log = (d_start + timedelta(days=i)).isoformat()
                                cur.execute("""
                                    INSERT INTO exceptions(user_id, date, type, notes, created_at)
                                    VALUES(?,?,?,?,?)
                                    ON CONFLICT(user_id, date) DO UPDATE SET type=excluded.type, notes=excluded.notes
                                """, (r['user_id'], day_to_log, r['reason_type'], f"Aprobado de Portal: {r['reason_description']}", datetime.now().isoformat(timespec="seconds")))
                            conn.commit()
                            log_audit("APPROVE_LEAVE_FINAL", f"Permiso #{r['id']} ({r['reason_type']}) de {r['full_name']} APROBADO FINAL por RRHH.")
                            st.rerun()
                            
                        if st.button("❌ Rechazar Final", key=f"btn_rej_hr_{r['id']}", use_container_width=True):
                            cur = conn.cursor()
                            cur.execute("UPDATE leave_requests SET status = 'REJECTED' WHERE id = ?", (r['id'],))
                            conn.commit()
                            log_audit("REJECT_LEAVE_FINAL", f"Permiso #{r['id']} ({r['reason_type']}) de {r['full_name']} RECHAZADO FINAL por RRHH.")
        conn.close()

    with tab4:
        user_role = st.session_state["user"]["role"]
        if user_role in ["admin", "nomina"]:
            st.subheader("Monitoreo Global de Permisos (Todas las Áreas)")
            st.info("Vista exclusiva para directivos. Aquí observas el estado de **todas** las solicitudes en curso en toda la empresa.")
            
            conn = db_conn()
            df_g = pd.read_sql_query("""
                SELECT lr.id, lr.user_id, e.full_name, e.department, lr.reason_type, lr.status, lr.request_date
                FROM leave_requests lr
                JOIN employees e ON lr.user_id = e.user_id
                WHERE lr.status LIKE 'PENDING_%'
                ORDER BY lr.request_date DESC
            """, conn)
            conn.close()
            
            if df_g.empty:
                st.success("Toda la tubería está limpia. No hay solicitudes estancadas.")
            else:
                st.write(f"Hay **{len(df_g)}** solicitudes esperando aprobación en algún nivel.")
                df_g.columns = ["Radicado", "DNI", "Empleado", "Área/Departamento", "Tipo", "Estado de Aprobación", "Fecha Solicitud"]
                
                st.info("💡 Haz clic en cualquier fila para ver los detalles completos de la solicitud.")
                event_g = st.dataframe(df_g, use_container_width=True, hide_index=True, on_select="rerun", selection_mode="single-row", key="admin_global_table")
                
                if len(event_g.selection.rows) > 0:
                    row_idx = event_g.selection.rows[0]
                    # df_g["Radicado"] is the ID
                    req_id = int(df_g.iloc[row_idx]["Radicado"])
                    st.session_state.admin_global_table.selection.rows.clear()
                    show_leave_request_details(req_id)
        else:
            st.warning("No tienes permisos de Administrador para ver la panorámica global de todas las áreas.")

def page_lateness_report():
    require_role("admin", "nomina")
    st.title("📉 Reporte Consolidado de Incidencias y Retardos")
    st.write("Genera el archivo avalado para procesos disciplinarios y descuentos de nómina.")

    today = date.today()
    col1, col2, col3 = st.columns(3)
    with col1:
        year = st.number_input("Año Fiscal", min_value=2020, max_value=2100, value=today.year, step=1)
    with col2:
        month = st.number_input("Mes de Nómina", min_value=1, max_value=12, value=today.month, step=1)
    with col3:
        search_dni = st.text_input("Buscar DNI", value="", placeholder="Ej: 100646459")

    st.markdown("<br>", unsafe_allow_html=True)
    if st.button("⚙️ Generar Analítica de Tardanzas", type="primary"):
        with st.spinner("Procesando histórico de marcaciones cruzado con horarios..."):
            summary_df, detail_df = compute_month_lateness(int(year), int(month))

        if summary_df.empty:
            st.success("🎉 ¡Excelente! No se registran tardanzas (o faltan asignaciones previas).")
            return
            
        # Top 3 de personas con más faltas
        st.markdown("### 🏆 Top 3: Mayores Incidencias")
        top3 = summary_df.head(3)
        cols = st.columns(3)
        for i, (idx, row) in enumerate(top3.iterrows()):
            if i < 3:
                cols[i].metric(f"ID {row['user_id']}", f"{row['minutos_tarde_total']} mins", f"{row['dias_tarde']} días infractores", delta_color="inverse")

        st.markdown("---")
        st.markdown("### 📊 Tabla General Resumida")
        
        # Unir nombre en el summary temporalmente para mostrarlo bonito
        conn = db_conn()
        nombres = pd.read_sql_query("SELECT user_id, full_name, department FROM employees", conn)
        conn.close()
        
        summary_view = pd.merge(summary_df, nombres, on="user_id", how="left")
        
        if search_dni:
            summary_view = summary_view[summary_view["user_id"].astype(str).str.contains(search_dni.strip())]
            
        summary_view = summary_view[["user_id", "full_name", "department", "dias_tarde", "minutos_tarde_total", "fechas_tarde"]]
        summary_view.columns = ["DNI Empleado", "Nombre", "Departamento", "Días con Retraso", "Minutos Totales Adeudados", "Fechas de Retraso"]
        
        st.dataframe(summary_view, use_container_width=True, hide_index=True)

        first_day = date(int(year), int(month), 1)
        last_day = date(int(year), int(month), calendar.monthrange(int(year), int(month))[1])

        raw_df = fetch_attendance_between(datetime.combine(first_day, time(0, 0)),
                                          datetime.combine(last_day + timedelta(days=1), time(0, 0)))

        excel_bytes = to_excel_bytes(summary_df, detail_df, raw_df)
        st.markdown("<br>", unsafe_allow_html=True)
        st.download_button(
            "📥 Descargar Archivo Oficial (Excel Completo)",
            data=excel_bytes,
            file_name=f"Reporte_Tardanzas_Dolormed_{year}_{month:02d}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True
        )


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
