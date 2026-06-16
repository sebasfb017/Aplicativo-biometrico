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
    assert summary.iloc[0]["minutos_tarde_total"] == 10


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
    # minutos tarde = 15
    assert detail.iloc[0]['minutos_tarde'] == 15


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


def test_get_late_punch_ids():
    # 1. Insert schedule and late punch
    sched = pd.DataFrame([{"week_start": "2025-01-06", "dow": 0, "start_time": "08:00", "grace_minutes": 5}])
    app.upsert_schedule_df(sched)
    
    rows = [
        {
            "device_name": "x",
            "device_ip": "1.1",
            "user_id": "emp_test_1",
            "ts": "2025-01-06 08:10:00",  # 5 minutes late (late_after is 08:05)
            "status": 0,
            "punch": 0,
            "uid": 100,
            "downloaded_at": datetime.now().isoformat()
        },
        {
            "device_name": "x",
            "device_ip": "1.1",
            "user_id": "emp_test_1",
            "ts": "2025-01-06 17:00:00",  # Exit punch, shouldn't count
            "status": 0,
            "punch": 1,
            "uid": 101,
            "downloaded_at": datetime.now().isoformat()
        },
        {
            "device_name": "x",
            "device_ip": "1.1",
            "user_id": "emp_test_2",
            "ts": "2025-01-06 08:00:00",  # On time (exactly at start time)
            "status": 0,
            "punch": 0,
            "uid": 102,
            "downloaded_at": datetime.now().isoformat()
        }
    ]
    app.upsert_attendance(rows)
    
    # Check that get_late_punch_ids returns the first record as late (5 mins) and not the others
    late_map = app.get_late_punch_ids(date(2025, 1, 6), date(2025, 1, 6))
    
    # We need to query the database to get the real IDs of the inserted punches
    conn = app.db_conn()
    cur = conn.cursor()
    cur.execute("SELECT id, ts, punch FROM attendance_raw WHERE user_id = 'emp_test_1'")
    records_1 = cur.fetchall()
    cur.execute("SELECT id, ts, punch FROM attendance_raw WHERE user_id = 'emp_test_2'")
    records_2 = cur.fetchall()
    conn.close()
    
    id_late = None
    id_exit = None
    for r_id, r_ts, r_punch in records_1:
        if r_punch == 0:
            id_late = r_id
        else:
            id_exit = r_id
            
    id_on_time = records_2[0][0]
    
    assert id_late in late_map
    assert late_map[id_late] == 10
    assert id_exit not in late_map
    assert id_on_time not in late_map


def test_process_bulk_shifts_new_codes():
    """Prueba la importación de nuevos códigos de turno y de excepción mediante process_bulk_shifts."""
    # Asegurar que exista un empleado de prueba en la base de datos
    emp_df = pd.DataFrame([
        {"user_id": "999", "full_name": "Test User", "profile_id": "Enfermería"}
    ])
    app.upsert_employees_df(emp_df)

    from views.schedules_view import process_bulk_shifts
    
    # Se probará un mes (ej. Junio 2026, 30 días)
    # Día 1: M (código de turno existente)
    # Día 2: TA (nuevo código de turno, 14:00 - 18:00)
    # Día 3: C1 (nuevo código de turno, 08:00 - 12:00)
    # Día 4: PR (nuevo código de excepción)
    # Día 5: LM (nuevo código de excepción)
    # Día 6: J (nuevo código de excepción)
    # Día 7: S (nuevo código de excepción)
    # Día 8: LNR (nuevo código de excepción)
    # Día 9: C10 (nuevo código de turno, 12:30 - 16:30)
    
    # Crear el dataframe de plantilla de prueba
    cols = ["Cedula"] + [str(d) for d in range(1, 31)]
    df = pd.DataFrame(columns=cols)
    row = ["999", "M", "TA", "C1", "PR", "LM", "J", "S", "LNR", "C10"] + [""] * 21
    df.loc[0] = row
    
    # Ejecutar el proceso masivo para Junio de 2026
    process_bulk_shifts(df, 2026, 6, 30)
    
    conn = app.db_conn()
    try:
        cur = conn.cursor()
        
        # Check shift_assignments
        cur.execute("""
            SELECT sa.week_start, sa.dow, s.name, s.start_time, s.end_time
            FROM shift_assignments sa
            JOIN shifts s ON sa.shift_id = s.id
            WHERE sa.user_id = '999'
            ORDER BY sa.week_start, sa.dow
        """)
        sa_rows = cur.fetchall()
        
        # Check exceptions
        cur.execute("""
            SELECT date, type FROM exceptions
            WHERE user_id = '999'
            ORDER BY date
        """)
        exc_rows = cur.fetchall()
    finally:
        conn.close()
    
    # Expected shift assignments:
    # 2026-06-01, dow 0 -> Turno_M (06:00 - 14:00)
    # 2026-06-01, dow 1 -> Turno_Ta (14:00 - 18:00)
    # 2026-06-01, dow 2 -> Turno_C1 (08:00 - 12:00)
    # 2026-06-08, dow 1 -> Turno_C10 (12:30 - 16:30)
    
    assert len(sa_rows) == 4
    
    # Verify exceptions in the database
    # June 4, 2026: PR -> Permiso Remunerado
    # June 5, 2026: LM -> Licencia Remunerada
    # June 6, 2026: J -> Licencia por Jurado de Votación
    # June 7, 2026: S -> Suspensión
    # June 8, 2026: LNR -> Licencia No Remunerada
    assert len(exc_rows) == 5
    
    assert exc_rows[0] == ("2026-06-04", "Permiso Remunerado")
    assert exc_rows[1] == ("2026-06-05", "Licencia Remunerada")
    assert exc_rows[2] == ("2026-06-06", "Licencia por Jurado de Votación")
    assert exc_rows[3] == ("2026-06-07", "Suspensión")
    assert exc_rows[4] == ("2026-06-08", "Licencia No Remunerada")


def test_lateness_on_break_return():
    """Prueba que se calcule y detecte correctamente la tardanza en el retorno del break."""
    # 1. Crear un empleado de prueba
    emp_df = pd.DataFrame([
        {"user_id": "emp_break_test", "full_name": "Nathalia Test", "profile_id": "Administrativo"}
    ])
    app.upsert_employees_df(emp_df)

    # 2. Crear un turno con break (horario partido)
    # Turno de 08:00 a 17:30, con break de 12:30 a 14:00 (has_break = 1)
    sid = app.upsert_shift(
        name="Turno Partido Test",
        start_time="08:00",
        grace_minutes=0,
        end_time="17:30",
        has_break=True,
        break_start="12:30",
        break_end="14:00",
        shift_code="TP_TEST"
    )
    assert sid is not None

    # Asignar el turno al empleado para el día lunes (dow = 0)
    # La semana que contiene al 2026-05-25 inicia el lunes 2026-05-25 (semana iso/lunes)
    app.assign_shift("emp_break_test", "2026-05-25", 0, sid)

    # 3. Insertar las marcaciones del día
    # Entrada mañana: 08:00 (A tiempo)
    # Salida break: 12:30 (Salida) -> punch=1
    # Entrada break (retorno): 14:03:00 (Llega tarde por 3 minutos) -> punch=0
    # Salida tarde: 17:30 (Salida) -> punch=1
    rows = [
        {
            "device_name": "x",
            "device_ip": "1.1",
            "user_id": "emp_break_test",
            "ts": "2026-05-25 08:00:00",
            "status": 0,
            "punch": 0,
            "uid": 200,
            "downloaded_at": datetime.now().isoformat()
        },
        {
            "device_name": "x",
            "device_ip": "1.1",
            "user_id": "emp_break_test",
            "ts": "2026-05-25 12:30:00",
            "status": 0,
            "punch": 1,
            "uid": 201,
            "downloaded_at": datetime.now().isoformat()
        },
        {
            "device_name": "x",
            "device_ip": "1.1",
            "user_id": "emp_break_test",
            "ts": "2026-05-25 14:03:00",
            "status": 0,
            "punch": 0,
            "uid": 202,
            "downloaded_at": datetime.now().isoformat()
        },
        {
            "device_name": "x",
            "device_ip": "1.1",
            "user_id": "emp_break_test",
            "ts": "2026-05-25 17:30:00",
            "status": 0,
            "punch": 1,
            "uid": 203,
            "downloaded_at": datetime.now().isoformat()
        }
    ]
    inserted, skipped = app.upsert_attendance(rows)
    assert inserted == 4

    # 4. Validar que get_late_punch_ids retorne la marcación del retorno del break como tardía
    late_map = app.get_late_punch_ids(date(2026, 5, 25), date(2026, 5, 25))
    
    # Consultar ID en BD de la marcación de las 14:03
    conn = app.db_conn()
    cur = conn.cursor()
    cur.execute("SELECT id FROM attendance_raw WHERE user_id = 'emp_break_test' AND ts = '2026-05-25 14:03:00'")
    punch_id_1403 = cur.fetchone()[0]
    conn.close()

    assert punch_id_1403 in late_map
    assert late_map[punch_id_1403] == 3

    # 5. Validar que compute_month_lateness registre 3 minutos tarde totales en el mes
    summary, detail = app.compute_month_lateness(2026, 5)
    assert not summary.empty
    
    emp_summary = summary[summary["user_id"] == "emp_break_test"]
    assert not emp_summary.empty
    assert emp_summary.iloc[0]["minutos_tarde_total"] == 3


def test_auxiliar_privilege_mapping(monkeypatch):
    # Simular el estado de sesión (session_state) de Streamlit usando monkeypatch
    session_state = {}
    monkeypatch.setattr(app.st, "session_state", session_state)
    
    # Caso 1: Usuario con rol 'empleado' (Auxiliar) y sub-área 'Talento humano'
    # Debería recibir acceso efectivo equivalente al rol 'nomina'.
    session_state["user"] = {
        "role": "empleado",
        "emp_subarea": "Talento humano"
    }
    
    error_called = False
    stop_called = False
    
    def mock_error(msg):
        nonlocal error_called
        error_called = True
        
    def mock_stop():
        nonlocal stop_called
        stop_called = True
        raise Exception("stop")
        
    monkeypatch.setattr(app.st, "error", mock_error)
    monkeypatch.setattr(app.st, "stop", mock_stop)
    
    # This should succeed without raising stop or calling error
    app.require_role("nomina")
    assert not error_called
    assert not stop_called
    
    # This should fail and trigger error/stop
    try:
        app.require_role("coordinador")
    except Exception as e:
        assert str(e) == "stop"
    assert error_called
    assert stop_called
    
    # 2. User with role 'empleado' and sub-area 'Nomina'
    error_called = False
    stop_called = False
    session_state["user"] = {
        "role": "empleado",
        "emp_subarea": "Nomina"
    }
    app.require_role("nomina")
    assert not error_called
    assert not stop_called
    
    # 3. User with role 'empleado' and unrelated subarea (e.g., 'Calidad')
    error_called = False
    stop_called = False
    session_state["user"] = {
        "role": "empleado",
        "emp_subarea": "Calidad"
    }
    try:
        app.require_role("nomina")
    except Exception as e:
        assert str(e) == "stop"
    assert error_called
    assert stop_called


