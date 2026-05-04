import bcrypt
from datetime import datetime
import holidays
from database_conn.connection import db_conn
from views.schedules_view import ensure_schedules_columns, maybe_load_default_schedules

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
        created_at TEXT NOT NULL,
        emp_department TEXT,
        emp_area TEXT,
        emp_subarea TEXT,
        emp_phone TEXT,
        emp_email TEXT,
        managed_department TEXT,
        managed_area TEXT,
        reset_pin TEXT,
        reset_expires TEXT
    );
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS profiles (
        profile_id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL UNIQUE,
        description TEXT,
        works_holidays INTEGER NOT NULL DEFAULT 0,
        created_at TEXT NOT NULL
    );
    """)

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
        ts TEXT NOT NULL,
        status INTEGER NOT NULL,
        punch INTEGER NOT NULL,
        uid INTEGER NOT NULL,
        downloaded_at TEXT NOT NULL,
        UNIQUE(device_ip, user_id, ts, status, punch, uid)
    );
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS schedules (
        week_start TEXT NOT NULL,
        dow INTEGER NOT NULL,
        start_time TEXT NOT NULL,
        end_time TEXT DEFAULT '',
        start_time_2 TEXT DEFAULT '',
        end_time_2 TEXT DEFAULT '',
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

    cur.execute("""
    CREATE TABLE IF NOT EXISTS shift_assignments (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id TEXT NOT NULL,
        week_start TEXT NOT NULL,
        dow INTEGER NOT NULL,
        shift_id INTEGER NOT NULL,
        created_at TEXT NOT NULL,
        UNIQUE(user_id, week_start, dow),
        FOREIGN KEY(shift_id) REFERENCES shifts(id)
    );
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS exceptions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id TEXT NOT NULL,
        date TEXT NOT NULL,
        type TEXT NOT NULL,
        notes TEXT,
        created_at TEXT NOT NULL,
        UNIQUE(user_id, date)
    );
    """)

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
        created_at TEXT NOT NULL,
        approved_by_coord TEXT,
        coord_approval_date TEXT,
        approved_by_jefe TEXT,
        jefe_approval_date TEXT,
        approved_by_rrhh TEXT,
        rrhh_approval_date TEXT
    );
    """)

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
        default_pass = "Cambiar123!" 
        pw_hash = bcrypt.hashpw(default_pass.encode("utf-8"), bcrypt.gensalt())
        cur.execute("""
            INSERT INTO users_app(username, full_name, role, password_hash, active, created_at)
            VALUES(?,?,?,?,1,?)
        """, (default_user, "Administrador", "admin", pw_hash, datetime.now().isoformat(timespec="seconds")))
        conn.commit()

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

    initialize_colombian_holidays(cur)
    conn.commit()

    conn.close()

    ensure_schedules_columns()
    migrate_schema_attendance_flags()
    migrate_schema_for_profiles()
    migrate_schema_coordinators()
    migrate_schema_multilevel()
    maybe_load_default_schedules()

def migrate_schema_attendance_flags():
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

def migrate_schema_for_profiles():
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
    start_year = datetime.now().year - 2
    end_year = datetime.now().year + 2
    
    co_holidays = holidays.Colombia(years=range(start_year, end_year + 1))
    
    for dt, name in sorted(co_holidays.items()):
        date_str = dt.strftime("%Y-%m-%d")
        try:
            cur.execute("""
                INSERT OR IGNORE INTO holidays(date, description, created_at)
                VALUES(?, ?, ?)
            """, (date_str, name, datetime.now().isoformat(timespec="seconds")))
        except Exception:
            pass

def initialize_predefined_shifts(cur):
    shifts = []
    
    for name, start, end, grace, has_break, break_start, break_end, is_overnight, code in shifts:
        try:
            cur.execute("""
                INSERT OR IGNORE INTO shifts(name, start_time, end_time, grace_minutes, has_break, break_start, break_end, is_overnight, shift_code, created_at)
                VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (name, start, end, grace, has_break, break_start, break_end, is_overnight, code, datetime.now().isoformat(timespec="seconds")))
        except Exception:
            pass