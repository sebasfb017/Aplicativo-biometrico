import os
import pandas as pd
import yaml
import pytest
import sqlite3


# helper for writing a default schedules CSV

def write_default(tmp_path, df):
    path = tmp_path / "default_schedules.csv"
    df.to_csv(path, index=False)
    return str(path)

from datetime import datetime, date

import app


@pytest.fixture(autouse=True)
def clean_db(tmp_path, monkeypatch):
    # point the data directory to a temporary path so tests don't interfere
    monkeypatch.setattr(app, "DATA_DIR", str(tmp_path))
    monkeypatch.setattr(app, "DB_PATH", os.path.join(str(tmp_path), "app.db"))
    # ensure fresh database for each test
    if os.path.exists(app.DB_PATH):
        os.remove(app.DB_PATH)
    app.init_db()
    yield
    if os.path.exists(app.DB_PATH):
        os.remove(app.DB_PATH)


def test_default_user_created():
    u = app.get_user("admin")
    assert u is not None
    assert u[0] == "admin"
    assert app.verify_login("admin", "Cambiar123!")
    assert app.verify_login("admin", "wrong") is None


def test_user_lifecycle():
    # create a user
    conn = app.db_conn()
    cur = conn.cursor()
    pw = "foo123"
    pw_hash = app.bcrypt.hashpw(pw.encode(), app.bcrypt.gensalt())
    cur.execute("INSERT INTO users_app(username,full_name,role,password_hash,active,created_at) VALUES(?,?,?,?,1,?)",
                ("u1", "User One", "nomina", pw_hash, datetime.now().isoformat()))
    conn.commit()
    conn.close()
    assert app.verify_login("u1", pw)["role"] == "nomina"


def test_schedule_and_lateness():
    # insert schedule and a late punch
    sched = pd.DataFrame([{"week_start": "2025-01-06", "dow": 0, "start_time": "08:00", "grace_minutes": 5}])
    app.upsert_schedule_df(sched)
    # schedule_for_date
    sched_info = app.schedule_for_date(date(2025, 1, 6))
    assert sched_info["start_time"].hour == 8
    assert sched_info["grace_minutes"] == 5

    rows = [{
        "device_name": "x",
        "device_ip": "1.1",
        "user_id": "u1",
        "ts": "2025-01-06 08:10:00",  # 5 minutes late
        "status": 0,
        "punch": 0,
        "uid": 0,
        "downloaded_at": datetime.now().isoformat()
    }]
    inserted, skipped = app.upsert_attendance(rows)
    assert inserted == 1 and skipped == 0

    summary, detail = app.compute_month_lateness(2025, 1)
    assert not summary.empty
    assert summary.iloc[0]["minutos_tarde_total"] == 5


def test_upsert_schedule_df_invalid():
    df = pd.DataFrame([{"week_start": "2025-01-06", "dow": 7, "start_time": "08:00", "grace_minutes": 0}])
    with pytest.raises(ValueError):
        app.upsert_schedule_df(df)


def test_ensure_schedules_columns_creates_missing(tmp_path, monkeypatch):
    # simulate a database created before extra schedule columns
    monkeypatch.setattr(app, "DATA_DIR", str(tmp_path))
    monkeypatch.setattr(app, "DB_PATH", os.path.join(str(tmp_path), "app.db"))
    # the autouse fixture already created a full schema; remove it so we can recreate
    if os.path.exists(app.DB_PATH):
        os.remove(app.DB_PATH)

    conn = sqlite3.connect(app.DB_PATH)
    try:
        cur = conn.cursor()
        cur.execute(
            """
            CREATE TABLE schedules (
                week_start TEXT NOT NULL,
                dow INTEGER NOT NULL,
                start_time TEXT NOT NULL,
                PRIMARY KEY (week_start, dow)
            );
            """
        )
        conn.commit()
    finally:
        conn.close()

    # call the migration helper
    app.ensure_schedules_columns()

    conn = sqlite3.connect(app.DB_PATH)
    cur = conn.cursor()
    cur.execute("PRAGMA table_info(schedules)")
    cols = [r[1] for r in cur.fetchall()]
    conn.close()
    for expected in ["end_time", "start_time_2", "end_time_2", "grace_minutes"]:
        assert expected in cols


def test_load_devices(tmp_path):
    cfg = {"devices": [{"ip": "1.2.3.4"}, {"foo": "bar"}]}
    path = tmp_path / "devices.yaml"
    path.write_text(yaml.dump(cfg))
    # patch the constant
    app.DEVICES_YAML = str(path)

    devices = app.load_devices()
    assert len(devices) == 1
    assert devices[0]["ip"] == "1.2.3.4"


def test_to_excel_bytes():
    summary = pd.DataFrame([{"user_id": "u", "dias_tarde": 1, "minutos_tarde_total": 2}])
    detail = pd.DataFrame([{"user_id": "u", "fecha": "2025-01-01", "hora_marcacion": "08:10", "hora_inicio": "08:00", "gracia_min": 0, "minutos_tarde": 10, "device_name": "x", "device_ip": "1.1"}])
    raw = pd.DataFrame([{"foo": "bar"}])
    b = app.to_excel_bytes(summary, detail, raw)
    assert isinstance(b, (bytes, bytearray))
    assert len(b) > 0


def test_employees_lifecycle():
    # carga empleado
    df = pd.DataFrame([{"user_id": "emp001", "full_name": "Juan Perez", "email": "juan@example.com", "department": "Ventas"}])
    app.upsert_employees_df(df)
    
    # verifica en la DB
    conn = app.db_conn()
    cur = conn.cursor()
    cur.execute("SELECT full_name, email, department FROM employees WHERE user_id = ?", ("emp001",))
    row = cur.fetchone()
    conn.close()
    
    assert row is not None
    assert row[0] == "Juan Perez"
    assert row[1] == "juan@example.com"
    assert row[2] == "Ventas"


def test_attendance_with_employee_join():
    # carga empleado
    df_emp = pd.DataFrame([{"user_id": "emp001", "full_name": "Maria Garcia"}])
    app.upsert_employees_df(df_emp)
    
    # inserta marcacion
    rows = [{
        "device_name": "x",
        "device_ip": "1.1",
        "user_id": "emp001",
        "ts": "2025-01-06 08:10:00",
        "status": 0,
        "punch": 0,
        "uid": 0,
        "downloaded_at": app.datetime.now().isoformat()
    }]
    app.upsert_attendance(rows)
    
    # query con JOIN
    conn = app.db_conn()
    query = """
        SELECT a.user_id, COALESCE(e.full_name, 'Sin registrar') as employee_name
        FROM attendance_raw a
        LEFT JOIN employees e ON a.user_id = e.user_id
        WHERE a.user_id = ?
    """
    cur = conn.cursor()
    cur.execute(query, ("emp001",))
    row = cur.fetchone()
    conn.close()
    
    assert row is not None
    assert row[1] == "Maria Garcia"


def test_shifts_and_assignments():
    # crear un turno y asignarlo a un empleado
    sid = app.upsert_shift("Mañana", "09:00", 10)
    assert isinstance(sid, int)

    # comprobar que get_shifts_df lo lista
    shifts = app.get_shifts_df()
    assert not shifts.empty
    assert "Mañana" in shifts['name'].values

    # asignar a emp001 para lunes de la semana 2025-01-06
    app.assign_shift("emp001", "2025-01-06", 0, sid)

    # ver la asignación mediante la función de consulta
    info = app.get_shift_for_user_date("emp001", date(2025,1,6))
    assert info is not None
    assert info['start_time'].hour == 9
    assert info['grace_minutes'] == 10

    # cuando no hay turno asignado, debe usar el schedule
    sched = pd.DataFrame([{"week_start": "2025-01-06", "dow": 1, "start_time": "08:00", "grace_minutes": 0}])
    app.upsert_schedule_df(sched)
    # martes (dow=1) sin asignación de turno para emp001
    fallback = app.schedule_for_user_date("emp001", date(2025,1,7))
    assert fallback is not None
    assert fallback['start_time'].hour == 8

    # la función de tardanzas debe respetar el turno asignado para calcular
    rows = [{
        "device_name": "x",
        "device_ip": "1.1",
        "user_id": "emp001",
        "ts": "2025-01-06 09:15:00",  # 5 minutos tarde respecto a turno 9:00 con 10m gracia
        "status": 0,
        "punch": 0,
        "uid": 0,
        "downloaded_at": app.datetime.now().isoformat()
    }]
    app.upsert_attendance(rows)
    summary, detail = app.compute_month_lateness(2025, 1)
    assert not detail.empty
    # minutos tarde = 5
    assert detail.iloc[0]['minutos_tarde'] == 5


def test_generate_rotating_schedule():
    # febrero 2026: patrones alternos
    df = app.generate_rotating_schedule(2026, 2, ["08:00", "07:30"], grace_minutes=2)
    assert not df.empty
    # debe contener lunes de cada semana
    weeks = sorted(df['week_start'].unique())
    assert len(weeks) >= 4
    # el primer lunes debe corresponder al patrón[0]
    first = df[df['week_start']==weeks[0]]
    assert first.iloc[0]['start_time'] == "08:00"
    # alternancia: segunda semana debe tener 07:30
    if len(weeks) > 1:
        second = df[df['week_start']==weeks[1]]
        assert second.iloc[0]['start_time'] == "07:30"
    # también pruebo formato doble franja
    df2 = app.generate_rotating_schedule(2026, 2, ["08:00-12:30|14:00-18:00"], grace_minutes=1)
    assert not df2.empty
    row = df2.iloc[0]
    assert row["end_time"] == "12:30"
    assert row["start_time_2"] == "14:00"
    assert row["end_time_2"] == "18:00"


def test_default_schedule_file_applies(tmp_path, monkeypatch):
    # if a CSV named default_schedules.csv exists it should be loaded on init
    monkeypatch.setattr(app, "DATA_DIR", str(tmp_path))
    monkeypatch.setattr(app, "DB_PATH", os.path.join(str(tmp_path), "app.db"))

    # create the default file
    df = pd.DataFrame([{"week_start": "2025-01-06", "dow": 0, "start_time": "08:00", "grace_minutes": 0}])
    default_path = write_default(tmp_path, df)
    # ensure init_db will call maybe_load_default_schedules
    app.init_db()
    conn = app.db_conn()
    cur = conn.cursor()
    cur.execute("SELECT count(*) FROM schedules")
    assert cur.fetchone()[0] == 1
    conn.close()


def test_auto_assign_shifts():
    # preparar empleado y horario
    app.upsert_employees_df(pd.DataFrame([{"user_id":"e1","full_name":"Foo"}]))
    sched = pd.DataFrame([{"week_start":"2025-01-06","dow":0,"start_time":"08:00","grace_minutes":0}])
    app.upsert_schedule_df(sched)
    count = app.auto_assign_shifts_from_schedules()
    assert count == 1  # un empleado x un horario
    # verify shift created
    shifts = app.get_shifts_df()
    assert "08:00" in shifts['name'].values
    # verify assignment exists via internal query
    conn = app.db_conn()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM shift_assignments WHERE user_id = ?", ("e1",))
    assert cur.fetchone()[0] == 1
    conn.close()


# ========== NUEVOS TESTS PARA SISTEMA AVANZADO DE TURNOS ==========

def test_profiles_initialized():
    """Test que los perfiles predefinidos se crean al iniciar BD."""
    conn = app.db_conn()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM profiles")
    count = cur.fetchone()[0]
    conn.close()
    assert count == 4  # Enfermería, Admisiones, Tecnólogos RX, Administrativo


def test_holidays_initialized():
    """Test que los festivos colombianos se cargan."""
    conn = app.db_conn()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM holidays WHERE description = 'Navidad' AND date LIKE ?", 
                (f"{date.today().year}%",))
    count = cur.fetchone()[0]
    conn.close()
    assert count >= 1


def test_predefined_shifts_created():
    """Test que los turnos predefinidos se crean."""
    shifts = app.get_shifts_df()
    shift_names = shifts['name'].values
    assert "M - Mañana (Enf)" in shift_names
    assert "N - Noche (Enf)" in shift_names
    assert "OFICINA - Horario Partido" in shift_names
    assert "RX1 - Día" in shift_names
    assert "L - Día Libre" in shift_names


def test_get_profile_by_name():
    """Test búsqueda de perfil por nombre."""
    profile = app.get_profile_by_name("Enfermería")
    assert profile is not None
    assert profile["name"] == "Enfermería"
    assert profile["works_holidays"] == 1  # Sí trabaja festivos
    
    profile_admin = app.get_profile_by_name("Administrativo")
    assert profile_admin["works_holidays"] == 0  # NO trabaja festivos


def test_is_holiday():
    """Test verificación de festivos."""
    # Navidad 2024
    navidad = date(2024, 12, 25)
    assert app.is_holiday(navidad)  # Debe ser festivo
    
    # Un día random
    random_day = date(2024, 5, 15)
    assert not app.is_holiday(random_day)  # No es festivo


def test_calculate_overnight_surcharge():
    """Test cálculo de recargo nocturno."""
    # Turno que cruza medianoche (23:00-07:00)
    surcharge = app.calculate_overnight_surcharge("23:00", "07:00")
    assert surcharge == 1.35  # 35% recargo
    
    # Turno diurno (08:00-16:00)
    surcharge = app.calculate_overnight_surcharge("08:00", "16:00")
    assert surcharge == 1.0   # Sin recargo
    
    # Turno que incluye hora nocturna (21:00-05:00)
    surcharge = app.calculate_overnight_surcharge("21:00", "05:00")
    assert surcharge == 1.35  # Tiene horas nocturnas


def test_resolve_shift_from_code():
    """Test resolución de turno desde código incluye perfil."""
    # Crear empleado con perfil Enfermería
    emp_df = pd.DataFrame([
        {"user_id": "101", "full_name": "Juan", "profile_id": "Enfermería"}
    ])
    app.upsert_employees_df(emp_df)
    
    # Resolver código "M" para Enfermería
    shift_id = app.resolve_shift_from_code("101", "M", "2024-03-04", 0)
    assert shift_id is not None
    
    # Verificar que el turno es "M - Mañana (Enf)"
    conn = app.db_conn()
    cur = conn.cursor()
    cur.execute("SELECT name FROM shifts WHERE id = ?", (shift_id,))
    shift_name = cur.fetchone()[0]
    conn.close()
    assert shift_name == "M - Mañana (Enf)"


def test_resolve_shift_different_profiles():
    """Test que el mismo código resuelve diferente turno según perfil."""
    # Empleado Enfermería
    emp_df = pd.DataFrame([
        {"user_id": "201", "full_name": "Enf", "profile_id": "Enfermería"},
        {"user_id": "202", "full_name": "Adm", "profile_id": "Admisiones"}
    ])
    app.upsert_employees_df(emp_df)
    
    # Ambos tienen código "M" pero turno diferente
    shift_enf = app.resolve_shift_from_code("201", "M", "2024-03-04", 0)
    shift_adm = app.resolve_shift_from_code("202", "M", "2024-03-04", 0)
    
    assert shift_enf != shift_adm  # Turnos diferentes
    
    # Verificar nombres
    conn = app.db_conn()
    cur = conn.cursor()
    cur.execute("SELECT name FROM shifts WHERE id = ?", (shift_enf,))
    name_enf = cur.fetchone()[0]
    cur.execute("SELECT name FROM shifts WHERE id = ?", (shift_adm,))
    name_adm = cur.fetchone()[0]
    conn.close()
    
    assert "Enf" in name_enf
    assert "Adm" in name_adm


def test_upsert_shifts_from_code_csv_basic():
    """Test carga de CSV con shift_codes."""
    # Crear empleado
    emp_df = pd.DataFrame([
        {"user_id": "101", "full_name": "Juan", "profile_id": "Enfermería"}
    ])
    app.upsert_employees_df(emp_df)
    
    # Crear CSV de turnos
    shifts_csv = pd.DataFrame([
        {"user_id": "101", "week_start": "2024-03-04", "dow": 0, "shift_code": "M"},
        {"user_id": "101", "week_start": "2024-03-04", "dow": 1, "shift_code": "T"},
    ])
    
    result = app.upsert_shifts_from_code_csv(shifts_csv)
    
    assert result["assigned"] == 2
    assert result["success"] is True
    assert len(result["errors"]) == 0


def test_upsert_shifts_from_code_csv_with_holidays():
    """Test que turnos en festivos se saltan para perfiles sin work_holidays."""
    # Empleado Administrativo (NO trabaja festivos)
    emp_df = pd.DataFrame([
        {"user_id": "401", "full_name": "Admin", "profile_id": "Administrativo"}
    ])
    app.upsert_employees_df(emp_df)
    
    # Intenta asignar turno en Navidad (25-12-2024)
    shifts_csv = pd.DataFrame([
        {"user_id": "401", "week_start": "2024-12-23", "dow": 2, "shift_code": "OFICINA"},  # 25-12 es día 2 de esa semana
    ])
    
    result = app.upsert_shifts_from_code_csv(shifts_csv)
    
    assert result["skipped_holidays"] == 1  # Debe saltarse
    assert result["assigned"] == 0


def test_upsert_shifts_from_code_csv_invalid_code():
    """Test manejo de códigos de turno inválidos."""
    # Empleado
    emp_df = pd.DataFrame([
        {"user_id": "101", "full_name": "Juan", "profile_id": "Enfermería"}
    ])
    app.upsert_employees_df(emp_df)
    
    # CSV con código inválido
    shifts_csv = pd.DataFrame([
        {"user_id": "101", "week_start": "2024-03-04", "dow": 0, "shift_code": "INVALID"},
    ])
    
    result = app.upsert_shifts_from_code_csv(shifts_csv)
    
    assert result["assigned"] == 0
    assert len(result["errors"]) > 0


def test_upsert_employees_with_profile():
    """Test carga de empleados con perfil."""
    emp_df = pd.DataFrame([
        {"user_id": "501", "full_name": "Elena", "email": "elena@clinic.com", "department": "Radiología", "profile_id": "Tecnólogos RX"},
    ])
    
    app.upsert_employees_df(emp_df)
    
    # Verificar que se cargó con perfil
    conn = app.db_conn()
    cur = conn.cursor()
    cur.execute("SELECT profile_id FROM employees WHERE user_id = ?", ("501",))
    row = cur.fetchone()
    conn.close()
    
    assert row is not None
    assert row[0] is not None  # Tiene profile_id asignado


def test_shift_codes_in_spreadsheet():
    """Test que los shift_codes están correctos en la tablabase de datos."""
    shifts = app.get_shifts_df()
    
    # Verificar que cada turno tiene su código
    shift_dict = dict(zip(shifts['name'], shifts['shift_code']))
    
    assert shift_dict.get("M - Mañana (Enf)") == "M"
    assert shift_dict.get("N - Noche (Enf)") == "N"
    assert shift_dict.get("OFICINA - Horario Partido") == "OFICINA"
    assert shift_dict.get("RX1 - Día") == "RX1"

