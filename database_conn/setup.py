"""Inicialización y aprovisionamiento del esquema de base de datos PostgreSQL.
DDL declarativo, reproducible e idempotente para Dolormed.
"""

import os
from datetime import datetime
import bcrypt
import holidays

from database_conn.connection import db_session


DDL_STATEMENTS = [
    """
    CREATE TABLE IF NOT EXISTS users_app (
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
        locked_until TEXT DEFAULT NULL,
        reset_pin TEXT DEFAULT NULL,
        reset_expires TEXT DEFAULT NULL,
        hire_date TEXT DEFAULT NULL,
        vacation_balance INTEGER DEFAULT 0,
        last_anniversary_year INTEGER DEFAULT NULL,
        theme_preference VARCHAR DEFAULT 'Oscuro',
        direct_routing VARCHAR DEFAULT NULL,
        skip_jefe_approval BOOLEAN DEFAULT FALSE,
        includes_direct_coord BOOLEAN DEFAULT FALSE,
        jefe_area_override TEXT DEFAULT NULL
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS profiles (
        profile_id SERIAL PRIMARY KEY,
        name TEXT UNIQUE NOT NULL,
        description TEXT,
        works_holidays INTEGER NOT NULL DEFAULT 0,
        created_at TEXT NOT NULL
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS employees (
        user_id TEXT PRIMARY KEY,
        full_name TEXT NOT NULL,
        email TEXT,
        department TEXT,
        created_at TEXT NOT NULL,
        profile_id INTEGER REFERENCES profiles(profile_id)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS attendance_raw (
        id SERIAL PRIMARY KEY,
        device_name TEXT NOT NULL,
        device_ip TEXT NOT NULL,
        user_id TEXT NOT NULL,
        ts TEXT NOT NULL,
        status INTEGER NOT NULL,
        punch INTEGER NOT NULL,
        uid INTEGER NOT NULL,
        downloaded_at TEXT NOT NULL,
        is_ignored INTEGER DEFAULT 0,
        is_manual INTEGER DEFAULT 0,
        UNIQUE (device_ip, user_id, ts, status, punch, uid)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS shifts (
        id SERIAL PRIMARY KEY,
        name TEXT UNIQUE NOT NULL,
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
    """,
    """
    CREATE TABLE IF NOT EXISTS shift_assignments (
        id SERIAL PRIMARY KEY,
        user_id TEXT NOT NULL,
        week_start TEXT NOT NULL,
        dow INTEGER NOT NULL,
        shift_id INTEGER NOT NULL REFERENCES shifts(id),
        created_at TEXT NOT NULL,
        UNIQUE (user_id, week_start, dow)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS shift_logs (
        log_id SERIAL PRIMARY KEY,
        user_id TEXT NOT NULL,
        week_start TEXT NOT NULL,
        dow INTEGER NOT NULL,
        old_shift_id INTEGER,
        new_shift_id INTEGER,
        changed_by TEXT NOT NULL,
        change_reason TEXT,
        changed_at TEXT NOT NULL
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS schedules (
        week_start TEXT NOT NULL,
        dow INTEGER NOT NULL,
        start_time TEXT NOT NULL,
        grace_minutes INTEGER NOT NULL DEFAULT 0,
        end_time TEXT DEFAULT '',
        start_time_2 TEXT DEFAULT '',
        end_time_2 TEXT DEFAULT '',
        PRIMARY KEY (week_start, dow)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS leave_requests (
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
    """,
    """
    CREATE TABLE IF NOT EXISTS exceptions (
        id SERIAL PRIMARY KEY,
        user_id TEXT NOT NULL,
        date TEXT NOT NULL,
        type TEXT NOT NULL,
        notes TEXT,
        created_at TEXT NOT NULL,
        UNIQUE (user_id, date)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS holidays (
        holiday_id SERIAL PRIMARY KEY,
        date TEXT UNIQUE NOT NULL,
        description TEXT NOT NULL,
        created_at TEXT NOT NULL
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS hr_pqrs (
        id SERIAL PRIMARY KEY,
        ticket_code TEXT UNIQUE NOT NULL,
        category TEXT NOT NULL,
        subject TEXT NOT NULL,
        description TEXT NOT NULL,
        sede TEXT,
        target_area TEXT,
        is_anonymous BOOLEAN DEFAULT TRUE,
        user_id TEXT,
        full_name TEXT,
        attachment_path TEXT,
        status TEXT NOT NULL DEFAULT 'PENDING',
        admin_response TEXT,
        responded_by TEXT,
        responded_at TEXT,
        created_at TEXT NOT NULL
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS hr_procedures (
        id SERIAL PRIMARY KEY,
        user_id TEXT NOT NULL REFERENCES users_app(username) ON DELETE CASCADE,
        procedure_type TEXT NOT NULL,
        details TEXT,
        attachment_path TEXT,
        status TEXT NOT NULL DEFAULT 'PENDING',
        created_at TEXT NOT NULL,
        response_path TEXT,
        admin_notes TEXT
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS notifications (
        id SERIAL PRIMARY KEY,
        user_id TEXT NOT NULL,
        title TEXT NOT NULL,
        message TEXT NOT NULL,
        is_read INTEGER NOT NULL DEFAULT 0,
        created_at TEXT NOT NULL
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS audit_logs (
        id SERIAL PRIMARY KEY,
        user_id TEXT NOT NULL,
        action TEXT NOT NULL,
        details TEXT NOT NULL,
        timestamp TEXT NOT NULL
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS biometric_id_map (
        id SERIAL PRIMARY KEY,
        device_name TEXT NOT NULL,
        bio_user_id TEXT NOT NULL,
        real_user_id TEXT NOT NULL,
        notes TEXT,
        created_at TEXT NOT NULL,
        UNIQUE (device_name, bio_user_id)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS user_sessions (
        token TEXT PRIMARY KEY,
        username TEXT NOT NULL REFERENCES users_app(username) ON DELETE CASCADE,
        created_at TEXT NOT NULL,
        expires_at TEXT NOT NULL
    );
    """,
    # B-Tree Indexes (Fase 4.2)
    """
    CREATE INDEX IF NOT EXISTS idx_leave_requests_user_status
    ON leave_requests (user_id, status);
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_attendance_raw_user_ts
    ON attendance_raw (user_id, ts);
    """
]


def init_db():
    """Ejecuta el DDL declarativo e inicializa las tablas e índices en PostgreSQL."""
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    with db_session() as conn:
        with conn.cursor() as cur:
            # 1. Crear tablas e índices
            for statement in DDL_STATEMENTS:
                cur.execute(statement)

            # 1.1 Asegurar integridad referencial en cascada para sesiones y trámites
            cur.execute(
                """
                DO $$
                BEGIN
                    IF EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'user_sessions_username_fkey') THEN
                        ALTER TABLE user_sessions DROP CONSTRAINT user_sessions_username_fkey;
                    END IF;
                    ALTER TABLE user_sessions ADD CONSTRAINT user_sessions_username_fkey 
                        FOREIGN KEY (username) REFERENCES users_app(username) ON DELETE CASCADE;

                    IF EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'hr_procedures_user_id_fkey') THEN
                        ALTER TABLE hr_procedures DROP CONSTRAINT hr_procedures_user_id_fkey;
                    END IF;
                    ALTER TABLE hr_procedures ADD CONSTRAINT hr_procedures_user_id_fkey 
                        FOREIGN KEY (user_id) REFERENCES users_app(username) ON DELETE CASCADE;
                END $$;
                """
            )

            # 2. Inicializar usuario administrador por defecto si no existe
            cur.execute("SELECT COUNT(*) FROM users_app WHERE role = 'admin'")
            admin_count = cur.fetchone()[0]
            if admin_count == 0:
                default_admin_pwd = os.getenv("DEFAULT_ADMIN_PASSWORD", "Cambiar123!")
                pwd_hash = bcrypt.hashpw(default_admin_pwd.encode("utf-8"), bcrypt.gensalt())
                cur.execute(
                    """
                    INSERT INTO users_app (username, full_name, role, password_hash, active, created_at)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (username) DO NOTHING
                    """,
                    ("admin", "Administrador del Sistema", "admin", pwd_hash, 1, now_str)
                )

            # 3. Poblar festivos de Colombia si la tabla está vacía
            cur.execute("SELECT COUNT(*) FROM holidays")
            holiday_count = cur.fetchone()[0]
            if holiday_count == 0:
                current_year = datetime.now().year
                years_to_load = list(range(current_year - 2, current_year + 3))
                co_holidays = holidays.Colombia(years=years_to_load)
                for h_date, h_name in co_holidays.items():
                    cur.execute(
                        """
                        INSERT INTO holidays (date, description, created_at)
                        VALUES (%s, %s, %s)
                        ON CONFLICT (date) DO NOTHING
                        """,
                        (h_date.strftime("%Y-%m-%d"), h_name, now_str)
                    )

            # 4. Poblar perfiles predefinidos si la tabla está vacía
            cur.execute("SELECT COUNT(*) FROM profiles")
            if cur.fetchone()[0] == 0:
                profiles_data = [
                    ("Enfermería", "Personal asistencial (Enfermeros, Auxiliares)", 1),
                    ("Admisiones", "Personal de admisiones y acogida", 1),
                    ("Tecnólogos RX", "Tecnólogos de Rayos X", 1),
                    ("Administrativo", "Personal administrativo y servicios", 0),
                ]
                for p_name, p_desc, p_holidays in profiles_data:
                    cur.execute(
                        """
                        INSERT INTO profiles (name, description, works_holidays, created_at)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (name) DO NOTHING
                        """,
                        (p_name, p_desc, p_holidays, now_str)
                    )

            # 5. Poblar turnos predefinidos si la tabla está vacía
            cur.execute("SELECT COUNT(*) FROM shifts")
            if cur.fetchone()[0] == 0:
                shifts_data = [
                    ("M - Mañana (Enf)", "07:00", "15:00", 0, 0, "", "", 0, "M"),
                    ("T - Tarde (Enf)", "15:00", "23:00", 0, 0, "", "", 0, "T"),
                    ("N - Noche (Enf)", "23:00", "07:00", 0, 0, "", "", 1, "N"),
                    ("M - Mañana (Adm)", "06:00", "14:00", 0, 0, "", "", 0, "M_ADM"),
                    ("T - Tarde (Adm)", "14:00", "22:00", 0, 0, "", "", 0, "T_ADM"),
                    ("RX1 - Día", "07:00", "19:00", 0, 0, "", "", 0, "RX1"),
                    ("RX2 - Noche", "19:00", "07:00", 0, 0, "", "", 1, "RX2"),
                    ("OFICINA - Horario Partido", "08:00", "18:00", 0, 1, "12:00", "14:00", 0, "OFICINA"),
                    ("C - Corrido", "10:00", "19:00", 0, 1, "14:00", "16:00", 0, "C"),
                    ("L - Día Libre", "", "", 0, 0, "", "", 0, "L"),
                ]
                for name, start, end, grace, has_break, break_start, break_end, is_overnight, code in shifts_data:
                    cur.execute(
                        """
                        INSERT INTO shifts (name, start_time, end_time, grace_minutes, has_break, break_start, break_end, is_overnight, shift_code, created_at)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (name) DO NOTHING
                        """,
                        (name, start, end, grace, has_break, break_start, break_end, is_overnight, code, now_str)
                    )

    # 6. Cargar horarios por defecto si existe el archivo
    try:
        from views.schedules_view import maybe_load_default_schedules
        maybe_load_default_schedules()
    except Exception:
        pass


if __name__ == "__main__":
    init_db()
    print("Base de datos inicializada correctamente.")
