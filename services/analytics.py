import io
import calendar
import pandas as pd
from datetime import datetime, date, time, timedelta
from database_conn.connection import db_conn

def get_shift_for_user_date(user_id: str, d: date, conn=None):
    """Obtiene el turno específico asignado a un usuario en una fecha."""
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

def schedule_for_date(d: date, conn=None):
    """Busca el horario predeterminado general para un día."""
    week_start = d - timedelta(days=d.weekday())
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

def schedule_for_user_date(user_id: str, d: date, conn=None):
    """Prioriza turno asignado manualmente, si no, usa el general."""
    s = get_shift_for_user_date(user_id, d, conn)
    if s:
        return s
    return schedule_for_date(d, conn)
def deduplicate_attendance(df: pd.DataFrame) -> pd.DataFrame:
    """
    Elimina registros duplicados (mismo user_id y ts) y marcaciones dobles (double tap)
    que ocurren dentro de un intervalo de 2 minutos para el mismo usuario.
    """
    if df.empty:
        return df

    # Asegurar orden temporal ascendente por usuario para la deduplicación
    # Calcular diferencia de tiempo con la marcación anterior del mismo usuario
    # Ordenamos por user_id, ts, y también por id para garantizar un orden determinista
    # sin importar el orden original del DataFrame.
    if "id" in df.columns:
        df = df.sort_values(["user_id", "ts", "id"]).reset_index(drop=True)
    else:
        df = df.sort_values(["user_id", "ts"]).reset_index(drop=True)
        
    df["datetime_ts"] = pd.to_datetime(df["ts"])

    kept_indices = []
    last_ts_by_user = {}

    for idx, row in df.iterrows():
        uid = str(row["user_id"])
        current_ts = row["datetime_ts"]

        if uid in last_ts_by_user:
            time_diff = (current_ts - last_ts_by_user[uid]).total_seconds()
            if time_diff <= 120:  # 2 minutos (120 segundos)
                # Es un double tap o un registro duplicado exacto, ignorar
                continue

        last_ts_by_user[uid] = current_ts
        kept_indices.append(idx)

    df_clean = df.loc[kept_indices].copy()
    df_clean = df_clean.drop(columns=["datetime_ts"])
    
    # Volver a ordenar descendentemente por ts
    df_clean = df_clean.sort_values(by="ts", ascending=False)
    
    return df_clean

def fetch_attendance_between(start_dt: datetime, end_dt: datetime):
    """Extrae las marcaciones crudas ignorando las que fueron eliminadas/editadas y duplicadas."""
    conn = db_conn()
    df = pd.read_sql_query("""
        SELECT device_name, device_ip, user_id, ts, status, punch, uid, downloaded_at
        FROM attendance_raw
        WHERE ts >= ? AND ts < ? AND is_ignored = 0
        ORDER BY user_id, ts
    """, conn, params=(start_dt.isoformat(sep=" ", timespec="seconds"),
                       end_dt.isoformat(sep=" ", timespec="seconds")))
    conn.close()
    if not df.empty:
        df["ts"] = pd.to_datetime(df["ts"])
        df = deduplicate_attendance(df)
    return df

def compute_month_lateness(year: int, month: int):
    """Cálculo maestro: Cruza horarios, marcaciones y novedades para consolidar tardanzas."""
    first_day = date(year, month, 1)
    last_day = date(year, month, calendar.monthrange(year, month)[1])

    start_dt = datetime.combine(first_day, time(0, 0))
    end_dt = datetime.combine(last_day + timedelta(days=1), time(0, 0))

    df = fetch_attendance_between(start_dt, end_dt)
    if df.empty:
        return pd.DataFrame(), pd.DataFrame()

    df["day"] = df["ts"].dt.date

    # Obtener excepciones (Permisos, Vacaciones) para no contar tardanzas
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

    # --- CARGA MASIVA DE HORARIOS (CORRECCIÓN N+1) ---
    start_week_str = (first_day - timedelta(days=first_day.weekday())).isoformat()
    end_week_str = (last_day - timedelta(days=last_day.weekday())).isoformat()
    
    # 1. Cargar las asignaciones de turnos específicas del usuario
    sa_df = pd.read_sql_query("""
        SELECT sa.user_id, sa.week_start, sa.dow, s.start_time, s.grace_minutes, s.has_break, s.break_end
        FROM shift_assignments sa
        JOIN shifts s ON sa.shift_id = s.id
        WHERE sa.week_start >= ? AND sa.week_start <= ?
    """, conn, params=(start_week_str, end_week_str))
    
    user_shifts = {}
    for _, row in sa_df.iterrows():
        try:
            hh, mm = row["start_time"].split(":")
            be_time = None
            if row["has_break"] and row["break_end"] and ":" in str(row["break_end"]):
                be_hh, be_mm = str(row["break_end"]).split(":")
                be_time = time(int(be_hh), int(be_mm))
            user_shifts[(str(row["user_id"]), row["week_start"], int(row["dow"]))] = {
                "start_time": time(int(hh), int(mm)),
                "break_end": be_time,
                "grace_minutes": int(row["grace_minutes"])
            }
        except: pass
        
    # 2. Cargar horarios predeterminados
    def_sched_df = pd.read_sql_query("""
        SELECT week_start, dow, start_time, start_time_2, grace_minutes
        FROM schedules
        WHERE week_start >= ? AND week_start <= ?
    """, conn, params=(start_week_str, end_week_str))
    
    default_schedules = {}
    for _, row in def_sched_df.iterrows():
        try:
            hh, mm = row["start_time"].split(":")
            st2 = None
            start_time_2 = row.get("start_time_2", "")
            if start_time_2 and ":" in str(start_time_2):
                hh2, mm2 = str(start_time_2).split(":")
                st2 = time(int(hh2), int(mm2))
            default_schedules[(row["week_start"], int(row["dow"]))] = {
                "start_time": time(int(hh), int(mm)),
                "start_time_2": st2,
                "grace_minutes": int(row["grace_minutes"])
            }
        except: pass
        
    conn.close()
    # ------------------------------------

    entries_df = df.sort_values("ts")
    if entries_df.empty:
        return pd.DataFrame(), pd.DataFrame()

    grouped = entries_df.groupby(["user_id", "day"])
    details = []
    
    for (user_id, day), group in grouped:
        d = day
        uid = str(user_id)
        
        if (uid, d.isoformat()) in exceptions_set:
            continue
            
        week_start_str = (d - timedelta(days=d.weekday())).isoformat()
        dow = d.weekday()
        
        sched = user_shifts.get((uid, week_start_str, dow))
        if not sched:
            sched = default_schedules.get((week_start_str, dow))
            
        if not sched:
            continue
            
        punches = list(group.iterrows())
        if not punches:
            continue
            
        # 1. Mañana: Primer punch del día (sin importar si marcó 0 o 1)
        r0 = punches[0][1]
        expected_start = datetime.combine(d, sched["start_time"])
        late_min = int((r0["ts"] - expected_start).total_seconds() // 60)
        late_min -= sched.get("grace_minutes", 0)
        if late_min >= 1:
            details.append({
                "user_id": r0["user_id"],
                "fecha": d.isoformat(),
                "hora_marcacion": r0["ts"].strftime("%H:%M:%S"),
                "hora_inicio": expected_start.strftime("%H:%M"),
                "gracia_min": sched.get("grace_minutes", 0),
                "minutos_tarde": late_min,
                "device_name": r0["device_name"],
                "device_ip": r0["device_ip"],
            })
            
        # 2. Break: Buscar el siguiente punch que sea Entrada (0) o Break In (2)
        be = sched.get("break_end") or sched.get("start_time_2")
        if be:
            expected_be = datetime.combine(d, be)
            for _, r in punches[1:]:
                if r["punch"] in (0, 2):
                    late_be = int((r["ts"] - expected_be).total_seconds() // 60)
                    late_be -= sched.get("grace_minutes", 0)
                    if late_be >= 1:
                        details.append({
                            "user_id": r["user_id"],
                            "fecha": d.isoformat(),
                            "hora_marcacion": r["ts"].strftime("%H:%M:%S"),
                            "hora_inicio": expected_be.strftime("%H:%M"),
                            "gracia_min": sched.get("grace_minutes", 0),
                            "minutos_tarde": late_be,
                            "device_name": r["device_name"],
                            "device_ip": r["device_ip"],
                        })
                    break

    detail_df = pd.DataFrame(details)
    if detail_df.empty:
        return pd.DataFrame(), pd.DataFrame()

    summary_df = (detail_df.groupby("user_id", as_index=False)
                    .agg(dias_tarde=("fecha", "nunique"),
                         minutos_tarde_total=("minutos_tarde", "sum"),
                         fechas_tarde=("fecha", lambda x: ", ".join(sorted([str(i) for i in x.unique()]))))
                    .sort_values(["minutos_tarde_total", "dias_tarde"], ascending=False))
    return summary_df, detail_df

def to_excel_bytes(summary_df: pd.DataFrame, detail_df: pd.DataFrame, raw_df: pd.DataFrame | None = None):
    """Empaqueta los DataFrames de Pandas en un archivo Excel descargable."""
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        summary_df.to_excel(writer, index=False, sheet_name="RESUMEN_TARDANZAS")
        detail_df.to_excel(writer, index=False, sheet_name="DETALLE_TARDANZAS")
        if raw_df is not None and not raw_df.empty:
            raw_df.to_excel(writer, index=False, sheet_name="MARCACIONES_RAW")
    output.seek(0)
    return output.getvalue()

def get_late_punch_ids(start_date: date, end_date: date) -> dict:
    """
    Calcula qué marcaciones corresponden a llegadas tarde en un rango de fechas.
    Retorna un diccionario mapeando ID de la marcación (int) -> minutos tarde (int).
    """
    # 1. Ajustar el rango de tiempo de datetime para la consulta de marcaciones
    start_dt = datetime.combine(start_date, time(0, 0))
    end_dt = datetime.combine(end_date + timedelta(days=1), time(0, 0))
    
    # 2. Extraer todas las marcaciones crudas del periodo ordenadas por user_id y ts
    conn = db_conn()
    df = pd.read_sql_query("""
        SELECT id, user_id, ts, punch, device_name, device_ip
        FROM attendance_raw
        WHERE ts >= ? AND ts < ? AND is_ignored = 0
        ORDER BY user_id, ts
    """, conn, params=(start_dt.isoformat(sep=" ", timespec="seconds"),
                       end_dt.isoformat(sep=" ", timespec="seconds")))
    
    if df.empty:
        conn.close()
        return {}
        
    df["ts"] = pd.to_datetime(df["ts"])
    df = deduplicate_attendance(df)
    df["day"] = df["ts"].dt.date
    
    # 3. Cargar excepciones para ese periodo
    start_date_str = start_date.isoformat()
    end_date_str = end_date.isoformat()
    exc_df = pd.read_sql_query(
        "SELECT user_id, date FROM exceptions WHERE date >= ? AND date <= ?",
        conn, params=(start_date_str, end_date_str)
    )
    exceptions_set = set()
    for _, row in exc_df.iterrows():
        exceptions_set.add((str(row["user_id"]), row["date"]))
        
    # 4. Cargar horarios y asignaciones
    start_week_str = (start_date - timedelta(days=start_date.weekday())).isoformat()
    end_week_str = (end_date - timedelta(days=end_date.weekday())).isoformat()
    
    sa_df = pd.read_sql_query("""
        SELECT sa.user_id, sa.week_start, sa.dow, s.start_time, s.grace_minutes, s.has_break, s.break_end
        FROM shift_assignments sa
        JOIN shifts s ON sa.shift_id = s.id
        WHERE sa.week_start >= ? AND sa.week_start <= ?
    """, conn, params=(start_week_str, end_week_str))
    
    user_shifts = {}
    for _, row in sa_df.iterrows():
        try:
            hh, mm = row["start_time"].split(":")
            be_time = None
            if row["has_break"] and row["break_end"] and ":" in str(row["break_end"]):
                be_hh, be_mm = str(row["break_end"]).split(":")
                be_time = time(int(be_hh), int(be_mm))
            user_shifts[(str(row["user_id"]), row["week_start"], int(row["dow"]))] = {
                "start_time": time(int(hh), int(mm)),
                "break_end": be_time,
                "grace_minutes": int(row["grace_minutes"])
            }
        except: pass
        
    def_sched_df = pd.read_sql_query("""
        SELECT week_start, dow, start_time, start_time_2, grace_minutes
        FROM schedules
        WHERE week_start >= ? AND week_start <= ?
    """, conn, params=(start_week_str, end_week_str))
    
    default_schedules = {}
    for _, row in def_sched_df.iterrows():
        try:
            hh, mm = row["start_time"].split(":")
            st2 = None
            start_time_2 = row.get("start_time_2", "")
            if start_time_2 and ":" in str(start_time_2):
                hh2, mm2 = str(start_time_2).split(":")
                st2 = time(int(hh2), int(mm2))
            default_schedules[(row["week_start"], int(row["dow"]))] = {
                "start_time": time(int(hh), int(mm)),
                "start_time_2": st2,
                "grace_minutes": int(row["grace_minutes"])
            }
        except: pass
        
    conn.close()
    
    # 5. Agrupar por usuario y día
    entries_df = df.sort_values("ts")
    grouped = entries_df.groupby(["user_id", "day"])
                     
    late_dict = {}
    for (user_id, day), group in grouped:
        d = day
        uid = str(user_id)
        
        if (uid, d.isoformat()) in exceptions_set:
            continue
            
        week_start_str = (d - timedelta(days=d.weekday())).isoformat()
        dow = d.weekday()
        
        sched = user_shifts.get((uid, week_start_str, dow))
        if not sched:
            sched = default_schedules.get((week_start_str, dow))
            
        if not sched:
            continue
            
        punches = list(group.iterrows())
        if not punches:
            continue
            
        # 1. Mañana: Primer punch del día
        r0 = punches[0][1]
        expected_start = datetime.combine(d, sched["start_time"])
        late_min = int((r0["ts"] - expected_start).total_seconds() // 60)
        late_min -= sched.get("grace_minutes", 0)
        
        if late_min >= 1:
            late_dict[int(r0["id"])] = late_min
            
        # 2. Break: Buscar el siguiente punch que sea Entrada (0 o 2)
        be = sched.get("break_end") or sched.get("start_time_2")
        if be:
            expected_be = datetime.combine(d, be)
            for _, r in punches[1:]:
                if r["punch"] in (0, 2):
                    late_be = int((r["ts"] - expected_be).total_seconds() // 60)
                    late_be -= sched.get("grace_minutes", 0)
                    if late_be >= 1:
                        late_dict[int(r["id"])] = late_be
                    break
            
    return late_dict