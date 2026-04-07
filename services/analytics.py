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

def fetch_attendance_between(start_dt: datetime, end_dt: datetime):
    """Extrae las marcaciones crudas ignorando las que fueron eliminadas/editadas."""
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

    # Tomar solo el primer 'punch' del día (llegada)
    first_punch = (df.sort_values("ts")
                     .groupby(["user_id", "day"], as_index=False)
                     .first()[["user_id", "day", "ts", "device_name", "device_ip"]])

    details = []
    
    for _, r in first_punch.iterrows():
        d = r["day"]
        uid = str(r["user_id"])
        
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