import os
import pandas as pd
import streamlit as st
from datetime import datetime, date, timedelta

from database_conn.connection import db_conn, DATA_DIR
from database_conn.queries import is_holiday, get_shifts_df, upsert_shift, assign_shift
from utils.auth import require_role

def default_schedules_path():
    return os.path.join(DATA_DIR, "default_schedules.csv")

def ensure_schedules_columns():
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

def maybe_load_default_schedules():
    path = default_schedules_path()
    if not os.path.exists(path):
        return
    try:
        df = pd.read_csv(path)
        upsert_schedule_df(df)
    except Exception:
        pass

def upsert_schedule_df(df: pd.DataFrame):
    required = {"week_start", "dow", "start_time", "grace_minutes"}
    if not required.issubset(set(df.columns)):
        missing = required - set(df.columns)
        raise ValueError(f"Faltan columnas: {missing}")

    df = df.copy()
    df["week_start"] = df["week_start"].astype(str)
    df["dow"] = df["dow"].astype(int)
    if df["dow"].lt(0).any() or df["dow"].gt(6).any():
        raise ValueError("La columna 'dow' sólo puede tener valores entre 0 y 6")

    df["start_time"] = df["start_time"].astype(str).str.slice(0, 5)
    for col in ["end_time","start_time_2","end_time_2"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.slice(0,5)
        else:
            df[col] = ""
    df["grace_minutes"] = df["grace_minutes"].fillna(0).astype(int)

    conn = db_conn()
    cur = conn.cursor()

    # --- INSERCIÓN EN BLOQUE OPTIMIZADA (BULK INSERT) ---
    # Convertimos el DataFrame directamente en unas tuplas para evitar iterar fila por fila en Python.
    # Pasamos de 40 líneas de bucle a 1 línea vectorizada usando executemany resolviéndose instantáneamente de lado del motor SQL.
    records = df[["week_start", "dow", "start_time", "end_time", "start_time_2", "end_time_2", "grace_minutes"]].itertuples(index=False, name=None)

    cur.executemany("""
        INSERT INTO schedules(week_start, dow, start_time, end_time, start_time_2, end_time_2, grace_minutes)
        VALUES(?,?,?,?,?,?,?)
        ON CONFLICT(week_start, dow) DO UPDATE SET
            start_time=excluded.start_time,
            end_time=excluded.end_time,
            start_time_2=excluded.start_time_2,
            end_time_2=excluded.end_time_2,
            grace_minutes=excluded.grace_minutes
    """, records)

    conn.commit()
    conn.close()

def resolve_shift_from_code(user_id: str, shift_code: str, week_start: str, dow: int):
    conn = db_conn()
    cur = conn.cursor()
    cur.execute("SELECT profile_id FROM employees WHERE user_id = ?", (str(user_id),))
    row = cur.fetchone()
    conn.close()
    
    if not row or not row[0]:
        return None
    
    profile_id = row[0]
    
    mapping = {
        (1, "M"): "M - Mañana (Enf)", (1, "T"): "T - Tarde (Enf)", (1, "N"): "N - Noche (Enf)",
        (2, "M"): "M - Mañana (Adm)", (2, "T"): "T - Tarde (Adm)",
        (3, "RX1"): "RX1 - Día", (3, "RX2"): "RX2 - Noche",
        (4, "OFICINA"): "OFICINA - Horario Partido", (4, "C"): "C - Corrido",
        (1, "L"): "L - Día Libre", (2, "L"): "L - Día Libre", (3, "L"): "L - Día Libre", (4, "L"): "L - Día Libre",
    }
    
    shift_name = mapping.get((profile_id, shift_code))
    if not shift_name:
        return None
    
    conn = db_conn()
    cur = conn.cursor()
    cur.execute("SELECT id FROM shifts WHERE name = ?", (shift_name,))
    row = cur.fetchone()
    conn.close()
    
    return row[0] if row else None

def upsert_shifts_from_code_csv(df: pd.DataFrame) -> dict:
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
        
        if dow < 0 or dow > 6:
            errors.append(f"Fila {idx}: dow={dow} inválido (0-6)")
            continue
        
        try:
            week_date = datetime.fromisoformat(week_start)
            target_date = (week_date + timedelta(days=dow)).date()
            if is_holiday(target_date):
                cur.execute("SELECT profile_id FROM employees WHERE user_id = ?", (user_id,))
                emp_row = cur.fetchone()
                if emp_row and emp_row[0]:
                    cur.execute("SELECT works_holidays FROM profiles WHERE profile_id = ?", (emp_row[0],))
                    prof_row = cur.fetchone()
                    if prof_row and not prof_row[0]:  
                        skipped_holidays += 1
                        continue
        except Exception:
            pass
        
        shift_id = resolve_shift_from_code(user_id, shift_code, week_start, dow)
        if not shift_id:
            errors.append(f"Fila {idx}: No se resolvió turno para {user_id} con código '{shift_code}'")
            continue
        
        try:
            assign_shift(user_id, week_start, dow, shift_id)
            assigned += 1
        except Exception as e:
            errors.append(f"Fila {idx}: Error asignando turno: {str(e)}")
            
    conn.close()
    return {"assigned": assigned, "skipped_holidays": skipped_holidays, "errors": errors, "success": len(errors) == 0}

def generate_rotating_schedule(year: int, month: int, pattern: list[str], grace_minutes: int = 0) -> pd.DataFrame:
    first = date(year, month, 1)
    current = first
    if current.weekday() != 0:
        current += timedelta(days=(7 - current.weekday()))
    rows = []
    idx = 0
    while current.month == month:
        spec = pattern[idx % len(pattern)]
        first_part, second_part = spec.split("|") if "|" in spec else (spec, "")
        start1 = first_part.split("-")[0]
        end1 = first_part.split("-")[1] if "-" in first_part else ""
        start2 = ""
        end2 = ""
        if second_part:
            parts = second_part.split("-")
            start2 = parts[0]
            end2 = parts[1] if len(parts) > 1 else ""
        for dow in range(0, 5): 
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

def auto_assign_shifts_from_schedules():
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