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

# pyzk
from zk import ZK

APP_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(APP_DIR, "data")
DB_PATH = os.path.join(DATA_DIR, "app.db")
DEVICES_YAML = os.path.join(APP_DIR, "devices.yaml")
# helper for default schedules file path (must be computed at runtime
# because DATA_DIR can be monkeypatched in tests).
def default_schedules_path():
    return os.path.join(DATA_DIR, "default_schedules.csv")

ROLES = ("admin", "nomina")


# -----------------------------
# DB
# -----------------------------
def db_conn():
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR, exist_ok=True)
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL;")
    return conn


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
    # Schema migration for employees and shifts to add new columns
    migrate_schema_for_profiles()
    # if a default schedule file exists, load it now
    maybe_load_default_schedules()

# -----------------------------
# Auth
# -----------------------------
def get_user(username: str):
    conn = db_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT username, full_name, role, password_hash, active
        FROM users_app WHERE username = ?
    """, (username,))
    row = cur.fetchone()
    conn.close()
    return row


def verify_login(username: str, password: str):
    row = get_user(username)
    if not row:
        return None
    _username, full_name, role, pw_hash, active = row
    if active != 1:
        return None
    if bcrypt.checkpw(password.encode("utf-8"), pw_hash):
        return {"username": _username, "full_name": full_name, "role": role}
    return None


def require_role(*allowed_roles):
    user = st.session_state.get("user")
    if not user or user.get("role") not in allowed_roles:
        st.error("No tienes permisos para ver esta sección.")
        st.stop()


# -----------------------------
# Devices
# -----------------------------
def load_devices():
    """Read and return the list of devices from the YAML configuration.

    If the file is missing, contains invalid YAML or does not define a
    ``devices`` list the function returns an empty list. Any entries that
    don't at least have an ``ip`` key are silently ignored.
    """
    if not os.path.exists(DEVICES_YAML):
        return []
    try:
        with open(DEVICES_YAML, "r", encoding="utf-8") as f:
            cfg = yaml.safe_load(f) or {}
    except yaml.YAMLError:
        return []

    devices = cfg.get("devices", [])
    if not isinstance(devices, list):
        return []

    valid = []
    for d in devices:
        if isinstance(d, dict) and d.get("ip"):
            valid.append(d)
    return valid


def download_attendance_from_device(device: dict):
    """
    Returns list[dict] with fields compatible with attendance_raw.
    """
    ip = device["ip"]
    try:
        port = int(device.get("port", 4370))
    except Exception:
        port = device.get("port", 4370)
    password = device.get("password", 0)
    try:
        password = int(password)
    except Exception:
        pass
    try:
        timeout = int(device.get("timeout", 10))
    except Exception:
        timeout = device.get("timeout", 10)
    name = device.get("name", ip)

    zk = ZK(ip, port=port, timeout=timeout, password=password)
    conn = None
    downloaded_at = datetime.now().isoformat(timespec="seconds")

    try:
        conn = zk.connect()
        conn.disable_device()  # evita actividad mientras descargas
        records = conn.get_attendance()  # list[Attendance]
        out = []
        for r in records:
            out.append({
                "device_name": name,
                "device_ip": ip,
                "user_id": str(r.user_id),
                "ts": r.timestamp.isoformat(sep=" ", timespec="seconds"),
                "status": int(r.status),
                "punch": int(r.punch),
                "uid": int(getattr(r, "uid", 0)),
                "downloaded_at": downloaded_at
            })
        return out, None
    except Exception as e:
        return [], str(e)
    finally:
        try:
            if conn:
                conn.enable_device()
                conn.disconnect()
        except Exception:
            pass


def upsert_attendance(rows: list[dict]):
    if not rows:
        return 0, 0
    
    data = [(
        r["device_name"], r["device_ip"], r["user_id"], r["ts"],
        r["status"], r["punch"], r["uid"], r["downloaded_at"]
    ) for r in rows]

    conn = db_conn()
    cur = conn.cursor()
    
    cur.executemany("""
        INSERT OR IGNORE INTO attendance_raw(device_name, device_ip, user_id, ts, status, punch, uid, downloaded_at)
        VALUES(?,?,?,?,?,?,?,?)
    """, data)
    
    inserted = cur.rowcount
    skipped = len(rows) - inserted

    conn.commit()
    conn.close()
    return inserted, skipped


# -----------------------------
# Employees
# -----------------------------
def upsert_employees_df(df: pd.DataFrame):
    """Carga o actualiza la tabla de empleados desde un DataFrame.

    Espera columnas: user_id (requerido), full_name (requerido), email, department, profile_id
    profile_id puede ser el ID del perfil o el nombre del perfil para auto-lookup.
    """
    required = {"user_id", "full_name"}
    if not required.issubset(set(df.columns)):
        missing = required - set(df.columns)
        raise ValueError(f"Faltan columnas requeridas: {missing}")

    df = df.copy()
    df["user_id"] = df["user_id"].astype(str)
    df["full_name"] = df["full_name"].astype(str)
    if "email" in df.columns:
        df["email"] = df["email"].astype(str)
    else:
        df["email"] = ""
    if "department" in df.columns:
        df["department"] = df["department"].astype(str)
    else:
        df["department"] = ""

    conn = db_conn()
    cur = conn.cursor()
    for _, r in df.iterrows():
        profile_id = None
        if "profile_id" in df.columns and r.get("profile_id"):
            profile_val = r["profile_id"]
            # Si es un string, buscar el perfil por nombre
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
                full_name=excluded.full_name,
                email=excluded.email,
                department=excluded.department,
                profile_id=excluded.profile_id
        """, (r["user_id"], r["full_name"], r.get("email", ""), r.get("department", ""), profile_id, datetime.now().isoformat(timespec="seconds")))
    conn.commit()
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


def is_holiday(date_obj: date) -> bool:
    """Verifica si una fecha es festivo en Colombia."""
    conn = db_conn()
    cur = conn.cursor()
    date_str = date_obj.isoformat()
    cur.execute("SELECT COUNT(*) FROM holidays WHERE date = ?", (date_str,))
    count = cur.fetchone()[0]
    conn.close()
    return count > 0


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

def upsert_shift(name: str,
                 start_time: str,
                 grace_minutes: int,
                 end_time: str = "",
                 has_break: bool = False,
                 break_start: str = "",
                 break_end: str = "",
                 is_overnight: bool = False,
                 shift_code: str | None = None):
    conn = db_conn()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO shifts(name, start_time, end_time, grace_minutes, has_break, break_start, break_end, is_overnight, shift_code, created_at)
        VALUES(?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(name) DO UPDATE SET
            start_time=excluded.start_time,
            end_time=excluded.end_time,
            grace_minutes=excluded.grace_minutes,
            has_break=excluded.has_break,
            break_start=excluded.break_start,
            break_end=excluded.break_end,
            is_overnight=excluded.is_overnight,
            shift_code=excluded.shift_code
    """, (
        name.strip(), start_time.strip(), end_time.strip(), int(grace_minutes),
        1 if has_break else 0, break_start.strip(), break_end.strip(), 1 if is_overnight else 0,
        shift_code.strip() if shift_code else None,
        datetime.now().isoformat(timespec="seconds")))
    conn.commit()
    cur.execute("SELECT id FROM shifts WHERE name = ?", (name.strip(),))
    sid = cur.fetchone()[0]
    conn.close()
    return sid


def get_shifts_df():
    conn = db_conn()
    df = pd.read_sql_query(
        "SELECT id, name, start_time, end_time, grace_minutes, has_break, break_start, break_end, is_overnight, shift_code, created_at FROM shifts ORDER BY name",
        conn)
    conn.close()
    return df


def assign_shift(user_id: str, week_start: str, dow: int, shift_id: int, conn=None):
    own_conn = False
    if conn is None:
        conn = db_conn()
        own_conn = True
        
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO shift_assignments(user_id, week_start, dow, shift_id, created_at)
        VALUES(?,?,?,?,?)
        ON CONFLICT(user_id, week_start, dow) DO UPDATE SET
            shift_id=excluded.shift_id
    """, (str(user_id), week_start, int(dow), int(shift_id), datetime.now().isoformat(timespec="seconds")))
    
    if own_conn:
        conn.commit()
        conn.close()


def upsert_exception(user_id: str, date_str: str, exc_type: str, notes: str):
    conn = db_conn()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO exceptions(user_id, date, type, notes, created_at)
        VALUES(?,?,?,?,?)
        ON CONFLICT(user_id, date) DO UPDATE SET
            type=excluded.type,
            notes=excluded.notes
    """, (user_id, date_str, exc_type, notes, datetime.now().isoformat(timespec="seconds")))
    conn.commit()
    conn.close()

def delete_exception(user_id: str, date_str: str):
    conn = db_conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM exceptions WHERE user_id = ? AND date = ?", (user_id, date_str))
    conn.commit()
    conn.close()

def get_exceptions_df():
    conn = db_conn()
    df = pd.read_sql_query("""
        SELECT ex.id, ex.user_id, e.full_name, ex.date, ex.type, ex.notes, ex.created_at
        FROM exceptions ex
        LEFT JOIN employees e ON ex.user_id = e.user_id
        ORDER BY ex.date DESC
    """, conn)
    conn.close()
    return df


def get_shift_for_user_date(user_id: str, d: date, conn=None):
    week_start = (d - timedelta(days=d.weekday())).isoformat()
    dow = d.weekday()
    own_conn = False
    if conn is None:
        conn = db_conn()
        own_conn = True
        
    cur = conn.cursor()
    cur.execute("""
        SELECT s.start_time, s.grace_minutes
        FROM shift_assignments sa
        JOIN shifts s ON sa.shift_id = s.id
        WHERE sa.user_id = ? AND sa.week_start = ? AND sa.dow = ?
        LIMIT 1
    """, (str(user_id), week_start, dow))
    row = cur.fetchone()
    
    if own_conn:
        conn.close()
        
    if not row:
        return None
    start_time_str, grace = row
    try:
        hh, mm = start_time_str.split(":")
        stime = time(int(hh), int(mm))
    except Exception:
        return None
    return {"start_time": stime, "grace_minutes": int(grace)}


def schedule_for_user_date(user_id: str, d: date, conn=None):
    # Prefer per-user shift assignment; fallback to schedules table
    s = get_shift_for_user_date(user_id, d, conn)
    if s:
        return s
    return schedule_for_date(d, conn)


def schedule_for_date(d: date, conn=None):
    week_start = d - timedelta(days=d.weekday())  # Monday
    own_conn = False
    if conn is None:
        conn = db_conn()
        own_conn = True
        
    cur = conn.cursor()
    cur.execute("""
        SELECT start_time, grace_minutes
        FROM schedules
        WHERE week_start = ? AND dow = ?
    """, (week_start.isoformat(), d.weekday()))
    row = cur.fetchone()
    
    if own_conn:
        conn.close()

    if not row:
        return None

    start_time_str, grace = row
    try:
        hh, mm = start_time_str.split(":")
        stime = time(int(hh), int(mm))
    except Exception:
        return None

    return {"start_time": stime, "grace_minutes": int(grace)}


# -----------------------------
# Reports
# -----------------------------
def fetch_attendance_between(start_dt: datetime, end_dt: datetime):
    conn = db_conn()
    df = pd.read_sql_query("""
        SELECT device_name, device_ip, user_id, ts, status, punch, uid, downloaded_at
        FROM attendance_raw
        WHERE ts >= ? AND ts < ?
        ORDER BY user_id, ts
    """, conn, params=(start_dt.isoformat(sep=" ", timespec="seconds"),
                       end_dt.isoformat(sep=" ", timespec="seconds")))
    conn.close()
    if not df.empty:
        df["ts"] = pd.to_datetime(df["ts"])
    return df


def compute_month_lateness(year: int, month: int):
    first_day = date(year, month, 1)
    last_day = date(year, month, calendar.monthrange(year, month)[1])

    start_dt = datetime.combine(first_day, time(0, 0))
    end_dt = datetime.combine(last_day + timedelta(days=1), time(0, 0))

    df = fetch_attendance_between(start_dt, end_dt)
    if df.empty:
        return pd.DataFrame(), pd.DataFrame()

    df["day"] = df["ts"].dt.date

    # Obtener excepciones para ignorar penalidades
    conn = db_conn()
    start_date_str = first_day.isoformat()
    end_date_str = last_day.isoformat()
    exc_df = pd.read_sql_query(
        "SELECT user_id, date, type FROM exceptions WHERE date >= ? AND date <= ?",
        conn, params=(start_date_str, end_date_str)
    )

    exceptions_set = set()
    for _, row in exc_df.iterrows():
        exceptions_set.add((str(row["user_id"]), row["date"]))

    # earliest mark per user/day
    first_punch = (df.sort_values("ts")
                     .groupby(["user_id", "day"], as_index=False)
                     .first()[["user_id", "day", "ts", "device_name", "device_ip"]])

    details = []
    
    # Optimizacion: reuse connection
    for _, r in first_punch.iterrows():
        d = r["day"]
        uid = str(r["user_id"])
        
        # Ignorar si tiene una Novedad/Excepción para ese día (vacaciones, incapacidad, etc.)
        if (uid, d.isoformat()) in exceptions_set:
            continue
            
        sched = schedule_for_user_date(uid, d, conn)
        if not sched:
            continue

        sched_start = datetime.combine(d, sched["start_time"])
        grace = timedelta(minutes=sched["grace_minutes"])
        late_after = sched_start + grace

        if r["ts"] > late_after:
            late_min = int((r["ts"] - late_after).total_seconds() // 60)
            details.append({
                "user_id": r["user_id"],
                "fecha": d.isoformat(),
                "hora_marcacion": r["ts"].strftime("%H:%M:%S"),
                "hora_inicio": sched_start.strftime("%H:%M"),
                "gracia_min": sched["grace_minutes"],
                "minutos_tarde": late_min,
                "device_name": r["device_name"],
                "device_ip": r["device_ip"],
            })
    conn.close()

    detail_df = pd.DataFrame(details)
    if detail_df.empty:
        return pd.DataFrame(), pd.DataFrame()

    summary_df = (detail_df.groupby("user_id", as_index=False)
                    .agg(dias_tarde=("fecha", "nunique"),
                         minutos_tarde_total=("minutos_tarde", "sum"))
                    .sort_values(["minutos_tarde_total", "dias_tarde"], ascending=False))
    return summary_df, detail_df


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
# UI
# -----------------------------
def page_login():
    st.markdown("<h1 style='text-align: center; color: #0066cc;'>Dolormed</h1>", unsafe_allow_html=True)
    st.markdown("<h3 style='text-align: center; color: gray;'>Gestión de Nómina y Asistencia</h3>", unsafe_allow_html=True)

    tab1, tab2 = st.tabs(["🏢 Ingreso Administrativo", "🧑‍⚕️ Portal Empleados"])

    with tab1:
        username = st.text_input("Usuario")
        password = st.text_input("Contraseña", type="password")

        if st.button("Ingresar a Dashboard", type="primary", use_container_width=True):
            user = verify_login(username.strip(), password)
            if user:
                st.session_state["user"] = user
                st.success(f"Bienvenido, {user['full_name']} ({user['role']})")
                st.rerun()
            else:
                st.error("Usuario/contraseña inválidos o usuario inactivo.")

    with tab2:
        st.write("Ingresa tu **Número de Cédula** para radicar permisos o consultar tus solicitudes.")
        cedula = st.text_input("Número de Documento (Cédula / ID)", key="emp_login")
        if st.button("Ingresar al Portal", type="secondary", use_container_width=True):
            if cedula.strip():
                conn = db_conn()
                cur = conn.cursor()
                cur.execute("SELECT user_id, full_name, department FROM employees WHERE user_id = ?", (cedula.strip(),))
                row = cur.fetchone()
                conn.close()
                
                if row:
                    st.session_state["user"] = {"username": row[0], "full_name": row[1], "role": "empleado", "department": row[2]}
                    st.rerun()
                else:
                    st.error("Documento no encontrado en el directorio activo de empleados. Solicita a RRHH que te registren.")
            else:
                st.warning("Escribe tu número de cédula.")


def page_dashboard():
    st.title("📊 Panel Principal - Dolormed")
    st.write("Resumen rápido del sistema de Recursos Humanos.")
    
    conn = db_conn()
    cur = conn.cursor()
    
    # KPIs
    cur.execute("SELECT COUNT(*) FROM employees")
    total_empleados = cur.fetchone()[0]
    
    cur.execute("SELECT COUNT(*) FROM attendance_raw WHERE date(ts) = date('now', 'localtime')")
    marcaciones_hoy = cur.fetchone()[0]
    
    cur.execute("SELECT COUNT(DISTINCT device_name) FROM attendance_raw")
    dispositivos_activos = cur.fetchone()[0]
    
    col1, col2, col3 = st.columns(3)
    col1.metric("Empleados Registrados", total_empleados, "Activos")
    col2.metric("Marcaciones Hoy", marcaciones_hoy)
    col3.metric("Relojes Biométricos Activos", dispositivos_activos)
    
    st.markdown("---")
    
    # Gráfico de Marcaciones Recientes (Últimos 7 días)
    st.subheader("Actividad de los últimos 7 días")
    df_act = pd.read_sql_query("""
        SELECT date(ts) as fecha, COUNT(*) as cantidad 
        FROM attendance_raw 
        WHERE date(ts) >= date('now', '-7 days')
        GROUP BY date(ts)
        ORDER BY date(ts)
    """, conn)
    conn.close()
    
    if not df_act.empty:
        fig = px.bar(df_act, x='fecha', y='cantidad', labels={'fecha':'Fecha', 'cantidad':'Marcaciones Totales'}, title="Frecuencia de Marcaciones por Día", color_discrete_sequence=['#0066cc'])
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
                SELECT user_id FROM attendance_raw WHERE date(ts) = ?
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


def page_users_admin():
    require_role("admin")
    st.title("👥 Gestión de Usuarios")
    st.write("Administra los accesos al portal de Nómina Dolormed.")
    
    tab1, tab2 = st.tabs(["📝 Registrar Nuevo", "📋 Lista de Usuarios"])

    with tab1:
        with st.form("create_user"):
            st.subheader("Datos del Nuevo Usuario")
            u = st.text_input("Username (sin espacios)")
            full = st.text_input("Nombre completo")
            role = st.selectbox("Rol", ["admin", "nomina"])
            pw = st.text_input("Contraseña inicial", type="password")
            active = st.checkbox("Activo", value=True)
            submit = st.form_submit_button("Crear / Actualizar Usuario", type="primary")

        if submit:
            if not u or not full or not pw:
                st.error("Completa username, nombre y contraseña.")
                return

            pw_hash = bcrypt.hashpw(pw.encode("utf-8"), bcrypt.gensalt())
            conn = db_conn()
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO users_app(username, full_name, role, password_hash, active, created_at)
                VALUES(?,?,?,?,?,?)
                ON CONFLICT(username) DO UPDATE SET
                    full_name=excluded.full_name,
                    role=excluded.role,
                    password_hash=excluded.password_hash,
                    active=excluded.active
            """, (u.strip(), full.strip(), role, pw_hash, 1 if active else 0, datetime.now().isoformat(timespec="seconds")))
            conn.commit()
            conn.close()
            st.success("Usuario creado/actualizado correctamente.")

    with tab2:
        conn = db_conn()
        users_df = pd.read_sql_query(
            "SELECT username, full_name, role, active, created_at FROM users_app ORDER BY username",
            conn
        )
        conn.close()
        if not users_df.empty:
            # Reemplazar 1 y 0 por Si y No
            users_df['active'] = users_df['active'].apply(lambda x: '✅ Sí' if x == 1 else '❌ No')
            users_df.columns = ["Usuario", "Nombre Completo", "Rol", "Activo", "Creado el"]
            st.dataframe(users_df, use_container_width=True, hide_index=True)


def page_sync():
    require_role("admin", "nomina")
    st.title("🔄 Sincronización Biométrica")
    st.write("Selecciona los relojes de los que deseas descargar las marcaciones recientes.")

    devices = load_devices()
    if not devices:
        st.error("No encontré 'devices.yaml' o no tiene dispositivos configurados.")
        return

    with st.expander("🛠️ Lista de Relojes Biométricos Disponibles", expanded=True):
        st.write("Marca o desmarca los dispositivos que quieras sincronizar:")
        
        # Crear un checkbox por cada dispositivo
        selected_devices = []
        for d in devices:
            label = f"📱 {d.get('name', d['ip'])} (IP: {d['ip']})"
            # Por defecto, todos vienen chequeados
            if st.checkbox(label, value=True, key=f"chk_{d['ip']}"):
                selected_devices.append(d)
                
        st.markdown("<br>", unsafe_allow_html=True)
        btn_sync = st.button("Descargar Marcaciones de los Relojes Seleccionados", type="primary")

    if btn_sync:
        if not selected_devices:
            st.warning("Debes dejar marcado al menos un reloj para hacer la descarga.")
            return
            
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
        WHERE a.ts >= ? AND a.ts < ?
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
            "device_name": "Dispositivo",
            "device_ip": "IP",
            "user_id": "ID Usuario",
            "employee_name": "Nombre",
            "ts": "Hora Marcación",
            "status": "Status",
            "punch": "Tipo",
            "downloaded_at": "Descargado en"
        })
        st.info(f"📊 Total de registros: {len(df)}")
        st.dataframe(df, use_container_width=True)

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
        conn = db_conn()
        emp = pd.read_sql_query("""
            SELECT e.user_id, e.full_name, e.email, e.department, COALESCE(p.name, 'Sin asignar') as profile, e.created_at
            FROM employees e
            LEFT JOIN profiles p ON e.profile_id = p.profile_id
            ORDER BY e.user_id
        """, conn)
        conn.close()
        
        if emp.empty:
            st.warning("El directorio está vacío.")
        else:
            emp.columns = ["DNI / ID Biométrico", "Nombre Completo", "Correo Electrónico", "Área / Departamento", "Perfil Asignado", "Fecha Registro"]
            
            # Filtro rápido
            deptss = ["Todos"] + list(emp["Área / Departamento"].dropna().unique())
            filtro_dep = st.selectbox("Filtrar por Departamento:", deptss)
            
            df_show = emp if filtro_dep == "Todos" else emp[emp["Área / Departamento"] == filtro_dep]
            
            st.metric("Total en vista", len(df_show))
            st.dataframe(df_show, use_container_width=True, hide_index=True)

    with tab2:
        st.subheader("Registrar Empleado Manualmente")
        st.info("Utiliza este formulario para crear un registro individual en la base de datos de manera inmediata.")
        with st.form("form_create_employee"):
            c1, c2 = st.columns(2)
            with c1:
                e_id = st.text_input("Número de Documento (DNI)*", placeholder="Ej: 100123456")
                e_name = st.text_input("Nombre Completo*", placeholder="Apellidos y Nombres")
                e_email = st.text_input("Correo Electrónico (Opcional)", placeholder="usuario@dolormed.com")
            with c2:
                e_dept = st.text_input("Área / Departamento*", placeholder="Ej: Urgencias, Admisiones")
                e_prof = st.selectbox("Perfil Asignado*", options=[p for p in profiles_df["Nombre Perfil"].tolist()])
                
            submitted = st.form_submit_button("Crear Empleado", type="primary")

        if submitted:
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


def db_create_leave_request(user_id, leave_start, leave_end, t_start, t_end, total_time, r_type, r_desc, makeup, is_paid):
    conn = db_conn()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO leave_requests (
            user_id, request_date, leave_date_start, leave_date_end, start_time, end_time, 
            total_time, reason_type, reason_description, how_to_makeup, is_paid, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        user_id, date.today().isoformat(), leave_start.isoformat(), leave_end.isoformat(),
        t_start, t_end, total_time, r_type, r_desc, makeup, 1 if is_paid else 0,
        datetime.now().isoformat(timespec="seconds")
    ))
    conn.commit()
    conn.close()

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
            st.dataframe(df_reqs, use_container_width=True, hide_index=True)


def page_exceptions():
    require_role("admin", "nomina")
    st.title("🛡️ Novedades y Justificaciones")
    st.write("Registra permisos, incapacidades médicas o vacaciones. El sistema **no penalizará** a estos empleados en los reportes de tardanzas para los días seleccionados.")

    conn = db_conn()
    emp_df = pd.read_sql_query("SELECT user_id, full_name, department FROM employees ORDER BY full_name", conn)
    conn.close()

    if emp_df.empty:
        st.warning("No hay empleados en el directorio.")
        return

    tab1, tab2, tab3 = st.tabs(["📝 Registrar Novedad Manual", "📋 Listado de Novedades", "📥 Solicitudes Digitales de Empleados"])

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
        df_exc = get_exceptions_df()
        if df_exc.empty:
            st.info("No hay novedades registradas.")
        else:
            df_exc.columns = ["ID", "Usuario", "Nombre", "Fecha", "Tipo", "Observaciones", "Registrado El"]
            st.dataframe(df_exc, use_container_width=True, hide_index=True)

    with tab3:
        st.subheader("Bandeja de Aprobación de Permisos (F-TH-012)")
        conn = db_conn()
        df_pend = pd.read_sql_query("""
            SELECT lr.id, lr.user_id, e.full_name, lr.request_date, lr.leave_date_start, lr.leave_date_end,
                   lr.reason_type, lr.reason_description, lr.is_paid, lr.status
            FROM leave_requests lr
            JOIN employees e ON lr.user_id = e.user_id
            WHERE lr.status = 'PENDING'
            ORDER BY lr.id ASC
        """, conn)
        
        if df_pend.empty:
            st.success("No hay solicitudes centralizadas pendientes de revisión.")
        else:
            st.write(f"Tienes **{len(df_pend)}** solicitud(es) por revisar del portal de empleados.")
            for _, r in df_pend.iterrows():
                with st.container(border=True):
                    cols = st.columns([3, 1])
                    with cols[0]:
                        st.markdown(f"**{r['full_name']}** (ID: {r['user_id']}) - *{r['reason_type']}*")
                        st.write(f"**Fechas:** {r['leave_date_start']} al {r['leave_date_end']} | **Remunerado:** {'Sí' if r['is_paid'] else 'No'}")
                        st.write(f"**Justificación:** {r['reason_description']}")
                    with cols[1]:
                        if st.button("✅ Aprobar", key=f"btn_acc_{r['id']}", type="primary", use_container_width=True):
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
                            st.rerun()
                            
                        if st.button("❌ Rechazar", key=f"btn_rej_{r['id']}", use_container_width=True):
                            cur = conn.cursor()
                            cur.execute("UPDATE leave_requests SET status = 'REJECTED' WHERE id = ?", (r['id'],))
                            conn.commit()
                            st.rerun()
        conn.close()


def page_lateness_report():
    require_role("admin", "nomina")
    st.title("📉 Reporte Consolidado de Incidencias y Retardos")
    st.write("Genera el archivo avalado para procesos disciplinarios y descuentos de nómina.")

    today = date.today()
    col1, col2 = st.columns(2)
    with col1:
        year = st.number_input("Año Fiscal", min_value=2020, max_value=2100, value=today.year, step=1)
    with col2:
        month = st.number_input("Mes de Nómina", min_value=1, max_value=12, value=today.month, step=1)

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
        summary_view = summary_view[["user_id", "full_name", "department", "dias_tarde", "minutos_tarde_total"]]
        summary_view.columns = ["DNI Empleado", "Nombre", "Departamento", "Días con Retraso", "Minutos Totales Adeudados"]
        
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
        if st.button("Cerrar sesión", use_container_width=True):
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
    elif sel == "Novedades y Excepciones":
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
