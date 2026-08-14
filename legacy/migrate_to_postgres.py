import sqlite3
import psycopg2
import os

SQLITE_DB_PATH = 'data/app.db'
POSTGRES_DSN = 'postgresql://nomina_user:nomina_password@localhost:5432/nomina_db'

PG_SCHEMA = """
DROP TABLE IF EXISTS attendance_raw CASCADE;
CREATE TABLE attendance_raw (
    id SERIAL PRIMARY KEY,
    device_name TEXT NOT NULL,
    device_ip TEXT NOT NULL,
    user_id TEXT NOT NULL,
    ts TEXT NOT NULL,
    status INTEGER NOT NULL,
    punch INTEGER NOT NULL,
    uid INTEGER NOT NULL,
    downloaded_at TEXT NOT NULL,
    is_ignored INTEGER NOT NULL DEFAULT 0,
    is_manual INTEGER NOT NULL DEFAULT 0,
    UNIQUE(device_ip, user_id, ts, status, punch, uid)
);

DROP TABLE IF EXISTS schedules CASCADE;
CREATE TABLE schedules (
    week_start TEXT NOT NULL,
    dow INTEGER NOT NULL,
    start_time TEXT NOT NULL,
    grace_minutes INTEGER NOT NULL DEFAULT 0,
    end_time TEXT DEFAULT '',
    start_time_2 TEXT DEFAULT '',
    end_time_2 TEXT DEFAULT '',
    PRIMARY KEY (week_start, dow)
);

DROP TABLE IF EXISTS employees CASCADE;
CREATE TABLE employees (
    user_id TEXT PRIMARY KEY,
    full_name TEXT NOT NULL,
    email TEXT,
    department TEXT,
    created_at TEXT NOT NULL,
    profile_id INTEGER
);

DROP TABLE IF EXISTS shifts CASCADE;
CREATE TABLE shifts (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    start_time TEXT NOT NULL,
    grace_minutes INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    end_time TEXT DEFAULT '',
    has_break INTEGER NOT NULL DEFAULT 0,
    break_start TEXT DEFAULT '',
    break_end TEXT DEFAULT '',
    is_overnight INTEGER NOT NULL DEFAULT 0,
    shift_code TEXT
);

DROP TABLE IF EXISTS shift_assignments CASCADE;
CREATE TABLE shift_assignments (
    id SERIAL PRIMARY KEY,
    user_id TEXT NOT NULL,
    week_start TEXT NOT NULL,
    dow INTEGER NOT NULL,
    shift_id INTEGER NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE(user_id, week_start, dow),
    FOREIGN KEY(shift_id) REFERENCES shifts(id)
);

DROP TABLE IF EXISTS profiles CASCADE;
CREATE TABLE profiles (
    profile_id SERIAL PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    description TEXT,
    works_holidays INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL
);

DROP TABLE IF EXISTS holidays CASCADE;
CREATE TABLE holidays (
    holiday_id SERIAL PRIMARY KEY,
    date TEXT NOT NULL UNIQUE,
    description TEXT NOT NULL,
    created_at TEXT NOT NULL
);

DROP TABLE IF EXISTS shift_logs CASCADE;
CREATE TABLE shift_logs (
    log_id SERIAL PRIMARY KEY,
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

DROP TABLE IF EXISTS exceptions CASCADE;
CREATE TABLE exceptions (
    id SERIAL PRIMARY KEY,
    user_id TEXT NOT NULL,
    date TEXT NOT NULL,
    type TEXT NOT NULL,
    notes TEXT,
    created_at TEXT NOT NULL,
    UNIQUE(user_id, date)
);

DROP TABLE IF EXISTS leave_requests CASCADE;
CREATE TABLE leave_requests (
    id SERIAL PRIMARY KEY,
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
    created_at TEXT NOT NULL,
    approved_by_coord TEXT,
    coord_approval_date TEXT,
    approved_by_jefe TEXT,
    jefe_approval_date TEXT,
    approved_by_rrhh TEXT,
    rrhh_approval_date TEXT,
    attachment_path TEXT,
    cancellation_reason TEXT,
    rejection_reason TEXT,
    hidden_by_employee INTEGER DEFAULT 0,
    specific_dates TEXT
);

DROP TABLE IF EXISTS audit_logs CASCADE;
CREATE TABLE audit_logs (
    id SERIAL PRIMARY KEY,
    user_id TEXT NOT NULL,
    action TEXT NOT NULL,
    details TEXT NOT NULL,
    timestamp TEXT NOT NULL
);

DROP TABLE IF EXISTS users_app CASCADE;
CREATE TABLE users_app (
    username TEXT PRIMARY KEY,
    full_name TEXT NOT NULL,
    role TEXT NOT NULL,
    password_hash BYTEA NOT NULL,
    active INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL,
    managed_department TEXT DEFAULT '',
    emp_area TEXT DEFAULT '',
    emp_subarea TEXT DEFAULT '',
    emp_phone TEXT DEFAULT '',
    emp_email TEXT DEFAULT '',
    managed_area TEXT DEFAULT '',
    failed_attempts INTEGER DEFAULT 0,
    locked_until TEXT,
    reset_pin TEXT,
    reset_expires TEXT,
    hire_date TEXT,
    vacation_balance INTEGER DEFAULT 0,
    last_anniversary_year INTEGER
);

DROP TABLE IF EXISTS notifications CASCADE;
CREATE TABLE notifications (
    id SERIAL PRIMARY KEY,
    user_id TEXT NOT NULL,
    title TEXT NOT NULL,
    message TEXT NOT NULL,
    is_read INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL
);

DROP TABLE IF EXISTS user_sessions CASCADE;
CREATE TABLE user_sessions (
    token TEXT PRIMARY KEY,
    username TEXT NOT NULL,
    created_at TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    FOREIGN KEY(username) REFERENCES users_app(username)
);

DROP TABLE IF EXISTS hr_procedures CASCADE;
CREATE TABLE hr_procedures (
    id SERIAL PRIMARY KEY,
    user_id TEXT NOT NULL,
    procedure_type TEXT NOT NULL,
    details TEXT,
    attachment_path TEXT,
    status TEXT NOT NULL DEFAULT 'PENDING',
    created_at TEXT NOT NULL,
    FOREIGN KEY(user_id) REFERENCES users_app(username)
);
"""

def migrate():
    print("Connecting to SQLite...")
    sqlite_conn = sqlite3.connect(SQLITE_DB_PATH)
    sqlite_cur = sqlite_conn.cursor()

    print("Connecting to PostgreSQL...")
    pg_conn = psycopg2.connect(POSTGRES_DSN)
    pg_cur = pg_conn.cursor()

    print("Creating PostgreSQL schema...")
    pg_cur.execute(PG_SCHEMA)
    pg_conn.commit()

    tables = [
        "attendance_raw", "schedules", "employees", "shifts",
        "shift_assignments", "profiles", "holidays", "shift_logs",
        "exceptions", "leave_requests", "audit_logs", "users_app",
        "notifications", "user_sessions", "hr_procedures"
    ]

    for table in tables:
        print(f"Migrating table {table}...")
        sqlite_cur.execute(f"SELECT * FROM {table}")
        rows = sqlite_cur.fetchall()
        if not rows:
            print(f"  Table {table} is empty.")
            continue
            
        # Get column names
        sqlite_cur.execute(f"PRAGMA table_info({table})")
        columns = [info[1] for info in sqlite_cur.fetchall()]
        
        placeholders = ','.join(['%s'] * len(columns))
        col_names = ','.join(columns)
        
        insert_query = f"INSERT INTO {table} ({col_names}) VALUES ({placeholders})"
        
        try:
            converted_rows = []
            for r in rows:
                l = list(r)
                for i, val in enumerate(l):
                    if isinstance(val, bytes):
                        if table == "users_app" and columns[i] == "password_hash":
                            l[i] = psycopg2.Binary(val)
                        else:
                            # Try to convert other random blobs (like corrupted uid) to int
                            try:
                                l[i] = int.from_bytes(val, 'little')
                            except:
                                l[i] = None
                converted_rows.append(tuple(l))
            
            pg_cur.executemany(insert_query, converted_rows)
            
            # Actualizar secuencias para columnas SERIAL (ID) para que las próximas inserciones no fallen
            if 'id' in columns or 'profile_id' in columns or 'holiday_id' in columns or 'log_id' in columns:
                id_col = 'id'
                if 'profile_id' in columns: id_col = 'profile_id'
                if 'holiday_id' in columns: id_col = 'holiday_id'
                if 'log_id' in columns: id_col = 'log_id'
                
                pg_cur.execute(f"SELECT setval(pg_get_serial_sequence('{table}', '{id_col}'), (SELECT MAX({id_col}) FROM {table}));")

        except Exception as e:
            print(f"Error migrating {table}: {e}")
            pg_conn.rollback()
            return
            
        print(f"  Migrated {len(rows)} rows.")
        
    pg_conn.commit()
    print("Migration completed successfully!")
    
    sqlite_conn.close()
    pg_conn.close()

if __name__ == "__main__":
    migrate()
