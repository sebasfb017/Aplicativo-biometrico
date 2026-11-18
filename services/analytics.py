import calendar
import io
from datetime import date, datetime, time, timedelta

import pandas as pd
import streamlit as st

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
    cur.execute(
        """
        SELECT s.start_time, s.grace_minutes
        FROM shift_assignments sa
        JOIN shifts s ON sa.shift_id = s.id
        WHERE sa.user_id = %s AND sa.week_start = %s AND sa.dow = %s
        LIMIT 1
    """,
        (str(user_id), week_start, dow),
    )
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
    cur.execute(
        """
        SELECT start_time, grace_minutes
        FROM schedules
        WHERE week_start = %s AND dow = %s
    """,
        (week_start.isoformat(), d.weekday()),
    )
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
        punch_type = row.get("punch")

        if uid in last_ts_by_user:
            prev_info = last_ts_by_user[uid]
            time_diff = (current_ts - prev_info["ts"]).total_seconds()
            if time_diff <= 120:  # 2 minutos (120 segundos)
                # Si ocurrió en la tarde (>= 13:00) y el actual es Salida (punch 1) mientras el anterior era Entrada (punch 0),
                # el colaborador corrigió su marcación a Salida: preferir Salida
                if (
                    current_ts.time() >= time(13, 0)
                    and punch_type == 1
                    and prev_info["punch"] == 0
                ):
                    kept_indices[-1] = idx
                    last_ts_by_user[uid] = {
                        "ts": current_ts,
                        "punch": punch_type,
                    }
                continue

        last_ts_by_user[uid] = {"ts": current_ts, "punch": punch_type}
        kept_indices.append(idx)

    df_clean = df.loc[kept_indices].copy()
    df_clean = df_clean.drop(columns=["datetime_ts"])

    # Volver a ordenar descendentemente por ts
    df_clean = df_clean.sort_values(by="ts", ascending=False)

    return df_clean


def apply_business_punch_rules(df: pd.DataFrame) -> pd.DataFrame:
    """Aplica reglas de negocio sobre las marcaciones de asistencia en la etapa analítica.
    Regla de Reclasificación Horaria: Cualquier marcación que ocurra a las 2:00 PM (14:XX)
    es reclasificada como Entrada (Punch 0), garantizando que los datos originales en la BD
    permanezcan puros y fidedignos sin contaminación en la capa de hardware.
    """
    if df.empty or "ts" not in df.columns or "punch" not in df.columns:
        return df
    ts_series = pd.to_datetime(df["ts"])
    mask_14 = ts_series.dt.hour == 14
    if mask_14.any():
        df = df.copy()
        df.loc[mask_14, "punch"] = 0
    return df


def fetch_attendance_between(start_dt: datetime, end_dt: datetime):
    """Extrae las marcaciones crudas ignorando las que fueron eliminadas/editadas y duplicadas."""
    conn = db_conn()
    df = pd.read_sql_query(
        """
        SELECT id, device_name, device_ip, user_id, ts, status, punch, uid, downloaded_at
        FROM attendance_raw
        WHERE ts >= %s AND ts < %s AND is_ignored = 0
        ORDER BY user_id, ts
    """,
        conn,
        params=(
            start_dt.isoformat(sep=" ", timespec="seconds"),
            end_dt.isoformat(sep=" ", timespec="seconds"),
        ),
    )
    conn.close()
    if not df.empty:
        df["ts"] = pd.to_datetime(df["ts"])
        df = deduplicate_attendance(df)
        df = apply_business_punch_rules(df)
    return df



def _load_leaves_and_exceptions(
    start_date: date, end_date: date, conn, user_id: str | None = None
):
    """
    Carga excepciones directas (exceptions) y novedades/permisos (leave_requests)
    en el rango de fechas indicado (opcionalmente filtrado por user_id).
    Retorna:
      - exceptions_dict: dict[(user_id, date_str)] -> {'type': str, 'notes': str}
      - user_date_lrs: dict[(user_id, date)] -> list[dict]
    """
    exc_query = (
        "SELECT user_id, date, type, notes FROM exceptions WHERE date >= %s AND date <= %s"
    )
    exc_params = [start_date.isoformat(), end_date.isoformat()]
    if user_id:
        exc_query += " AND user_id = %s"
        exc_params.append(str(user_id))

    exc_df = pd.read_sql_query(exc_query, conn, params=tuple(exc_params))
    exceptions_dict = {
        (str(r["user_id"]), str(r["date"])): {
            "type": r["type"],
            "notes": r.get("notes") or "",
        }
        for _, r in exc_df.iterrows()
    }

    lrs_query = """
        SELECT id, user_id, leave_date_start, leave_date_end, specific_dates, 
               start_time, end_time, reason_type, reason_description, status
        FROM leave_requests
        WHERE status IN ('APPROVED', 'PENDING_COORD', 'PENDING_JEFE', 'PENDING_RRHH')
          AND (
              (leave_date_start <= %s AND leave_date_end >= %s)
              OR (specific_dates IS NOT NULL AND specific_dates != '' AND specific_dates != 'None')
          )
    """
    lrs_params = [end_date.isoformat(), start_date.isoformat()]
    if user_id:
        lrs_query += " AND user_id = %s"
        lrs_params.append(str(user_id))

    lrs_df = pd.read_sql_query(lrs_query, conn, params=tuple(lrs_params))

    user_date_lrs = {}
    for _, lr in lrs_df.iterrows():
        uid = str(lr["user_id"])
        spec = lr.get("specific_dates")
        dates = []
        if spec and str(spec).strip() not in ["", "None"]:
            for ds in str(spec).split(","):
                ds = ds.strip()
                if ds:
                    try:
                        dates.append(date.fromisoformat(ds))
                    except Exception:
                        pass
        else:
            try:
                d1 = date.fromisoformat(str(lr["leave_date_start"]).strip())
                d2 = date.fromisoformat(str(lr["leave_date_end"]).strip())
                delta = (d2 - d1).days
                if delta >= 0:
                    dates = [d1 + timedelta(days=i) for i in range(delta + 1)]
            except Exception:
                pass

        st_t = None
        et_t = None
        st_val = str(lr.get("start_time") or "").strip()
        et_val = str(lr.get("end_time") or "").strip()
        if ":" in st_val:
            try:
                h, m = st_val.split(":")[:2]
                st_t = time(int(h), int(m))
            except Exception:
                pass
        if ":" in et_val:
            try:
                h, m = et_val.split(":")[:2]
                et_t = time(int(h), int(m))
            except Exception:
                pass

        is_full = st_t is None or et_t is None
        if lr["reason_type"] in [
            "Vacaciones",
            "Incapacidad",
            "Licencia de Luto",
            "Licencia por Jurado de Votación",
        ]:
            is_full = True

        entry = {
            "id": lr["id"],
            "reason_type": lr["reason_type"],
            "reason_desc": lr.get("reason_description") or "",
            "start_time": st_t,
            "end_time": et_t,
            "status": lr["status"],
            "is_full_day": is_full,
        }
        for d in dates:
            if start_date <= d <= end_date:
                user_date_lrs.setdefault((uid, d), []).append(entry)

    return exceptions_dict, user_date_lrs


def _evaluate_punch_justification(
    punch_row, expected_dt, sched, exc, lrs, is_break=False
):
    """
    Evalúa si una marcación que excede la hora esperada está justificada
    por una excepción o solicitud de permiso en leave_requests.
    Retorna: (late_min, justification_dict)
    - Si no es tarde o está 100% justificada: (None, justification_dict o None)
    - Si es tarde y no hay justificación: (late_min, None)
    - Si es tarde pero excede el permiso otorgado: (late_min_excedente, justification_dict con exceeded=True)
    """
    p_ts = punch_row["ts"]
    d = p_ts.date()
    raw_late = int((p_ts - expected_dt).total_seconds() // 60)
    grace = sched.get("grace_minutes", 0)

    if raw_late <= grace:
        return None, None

    # 1. Comprobar excepción formal registrada en tabla exceptions
    if exc:
        return None, {
            "reason": exc["type"],
            "status": "APPROVED",
            "desc": exc.get("notes") or "",
            "time": "Día completo",
        }

    # 2. Comprobar solicitudes en leave_requests
    for lr in lrs or []:
        if lr["is_full_day"]:
            return None, {
                "reason": lr["reason_type"],
                "status": lr["status"],
                "desc": lr["reason_desc"],
                "time": "Día completo",
            }

        if not is_break and lr["reason_type"].lower() in [
            "llegada tarde",
            "llegada tarde ",
        ]:
            return None, {
                "reason": lr["reason_type"],
                "status": lr["status"],
                "desc": lr["reason_desc"],
                "time": "Llegada Tarde",
            }

        if lr["start_time"] and lr["end_time"]:
            pst_dt = datetime.combine(d, lr["start_time"])
            pet_dt = datetime.combine(d, lr["end_time"])
            time_str = f"{lr['start_time'].strftime('%H:%M')} - {lr['end_time'].strftime('%H:%M')}"

            # 1. ¿El permiso cubre toda la jornada laboral del turno?
            shift_start = sched.get("start_time")
            shift_end = sched.get("end_time")
            if shift_start and shift_end:
                sh_start_dt = datetime.combine(d, shift_start)
                sh_end_dt = datetime.combine(d, shift_end)
                if pst_dt <= sh_start_dt + timedelta(minutes=45) and pet_dt >= sh_end_dt - timedelta(minutes=30):
                    return None, {
                        "reason": lr["reason_type"],
                        "status": lr["status"],
                        "desc": lr["reason_desc"],
                        "time": time_str,
                    }

            # 2. Si la marcación se produjo dentro del rango del permiso (o en su tolerancia)
            if pst_dt <= p_ts <= pet_dt + timedelta(minutes=grace + 15):
                return None, {
                    "reason": lr["reason_type"],
                    "status": lr["status"],
                    "desc": lr["reason_desc"],
                    "time": time_str,
                }

            # 3. Evaluación para llegada de la mañana (is_break=False)
            if not is_break:
                if pst_dt <= expected_dt + timedelta(minutes=45):
                    be = sched.get("break_end") or sched.get("start_time_2")
                    bs = sched.get("break_start")
                    eff_expected = pet_dt

                    # Si el permiso finaliza al inicio o durante el horario de descanso / almuerzo:
                    if be:
                        be_dt = datetime.combine(d, be)
                        bs_dt = datetime.combine(d, bs) if bs else (be_dt - timedelta(minutes=90))
                        if pet_dt >= bs_dt - timedelta(minutes=45) and pet_dt <= be_dt + timedelta(minutes=15):
                            # El empleado no debe incorporarse durante el almuerzo sino al retorno del descanso
                            eff_expected = be_dt
                        elif pet_dt > be_dt:
                            eff_expected = pet_dt

                    if p_ts <= eff_expected + timedelta(minutes=grace):
                        return None, {
                            "reason": lr["reason_type"],
                            "status": lr["status"],
                            "desc": lr["reason_desc"],
                            "time": time_str,
                        }
                    else:
                        post_late = int((p_ts - eff_expected).total_seconds() // 60)
                        if be:
                            be_dt = datetime.combine(d, be)
                            bs_dt = datetime.combine(d, bs) if bs else (be_dt - timedelta(minutes=90))
                            if p_ts >= be_dt and eff_expected <= bs_dt:
                                break_mins = int((be_dt - bs_dt).total_seconds() // 60)
                                post_late = max(0, post_late - break_mins)
                        return post_late, {
                            "reason": lr["reason_type"],
                            "status": lr["status"],
                            "desc": lr["reason_desc"],
                            "time": time_str,
                            "exceeded": True,
                        }

            # 4. Evaluación para retorno de almuerzo / break (is_break=True)
            else:
                # El permiso debe cubrir o solapar la hora de reingreso de break
                if pst_dt <= expected_dt + timedelta(minutes=45) and pet_dt >= expected_dt - timedelta(minutes=15):
                    eff_expected = max(pet_dt, expected_dt)
                    if p_ts <= eff_expected + timedelta(minutes=grace):
                        return None, {
                            "reason": lr["reason_type"],
                            "status": lr["status"],
                            "desc": lr["reason_desc"],
                            "time": time_str,
                        }
                    else:
                        post_late = int((p_ts - eff_expected).total_seconds() // 60)
                        if be:
                            be_dt = datetime.combine(d, be)
                            bs_dt = datetime.combine(d, bs) if bs else (be_dt - timedelta(minutes=90))
                            if p_ts >= be_dt and eff_expected <= bs_dt:
                                break_mins = int((be_dt - bs_dt).total_seconds() // 60)
                                post_late = max(0, post_late - break_mins)
                        return post_late, {
                            "reason": lr["reason_type"],
                            "status": lr["status"],
                            "desc": lr["reason_desc"],
                            "time": time_str,
                            "exceeded": True,
                        }

    return raw_late, None


def _is_plausible_break_return(punch_ts: datetime, sched: dict, num_punches_day: int) -> bool:
    """
    Determina si una marcación posterior a la primera puede corresponder a un
    reingreso de almuerzo / break, descartando marcaciones de salida al final de la jornada.
    """
    be = sched.get("break_end") or sched.get("start_time_2")
    if not be:
        return False

    p_time = punch_ts.time()
    p_date = punch_ts.date()

    # 1. No puede ocurrir antes del inicio de almuerzo (o 1 hora antes de break_end)
    bs = sched.get("break_start")
    min_time = bs if bs else time(max(0, be.hour - 1), be.minute)
    if p_time < min_time:
        return False

    # 2. No puede ocurrir en la salida o cerca del fin de la jornada laboral
    end_time = sched.get("end_time")
    if end_time:
        cutoff_dt = datetime.combine(p_date, end_time) - timedelta(minutes=60)
        if punch_ts >= cutoff_dt:
            return False

    # 3. Límite máximo de retraso plausible en almuerzo (máximo 120 min después de break_end)
    max_be_dt = datetime.combine(p_date, be) + timedelta(minutes=120)
    if punch_ts > max_be_dt:
        return False

    # 4. Si solo hay 2 marcaciones en todo el día y la segunda es en la tarde (>= 14:30),
    # corresponde a la salida de la jornada, no a un reingreso de almuerzo
    if num_punches_day == 2 and p_time >= time(14, 30):
        return False

    return True


@st.cache_data(ttl=600, show_spinner=False)
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

    # Obtener excepciones (Permisos, Vacaciones) y novedades en leave_requests para no contar tardanzas
    conn = db_conn()
    exceptions_dict, user_date_lrs = _load_leaves_and_exceptions(
        first_day, last_day, conn
    )

    # --- CARGA MASIVA DE HORARIOS (CORRECCIÓN N+1) ---
    start_week_str = (first_day - timedelta(days=first_day.weekday())).isoformat()
    end_week_str = (last_day - timedelta(days=last_day.weekday())).isoformat()

    # 1. Cargar las asignaciones de turnos específicas del usuario
    sa_df = pd.read_sql_query(
        """
        SELECT sa.user_id, sa.week_start, sa.dow, s.start_time, s.end_time, s.grace_minutes, s.has_break, s.break_start, s.break_end
        FROM shift_assignments sa
        JOIN shifts s ON sa.shift_id = s.id
        WHERE sa.week_start >= %s AND sa.week_start <= %s
    """,
        conn,
        params=(start_week_str, end_week_str),
    )

    user_shifts = {}
    for _, row in sa_df.iterrows():
        try:
            hh, mm = row["start_time"].split(":")
            be_time = None
            if row["has_break"] and row["break_end"] and ":" in str(row["break_end"]):
                be_hh, be_mm = str(row["break_end"]).split(":")
                be_time = time(int(be_hh), int(be_mm))
            bs_time = None
            if row["has_break"] and row["break_start"] and ":" in str(row["break_start"]):
                bsh, bsm = str(row["break_start"]).split(":")
                bs_time = time(int(bsh), int(bsm))
            end_t = None
            if row["end_time"] and ":" in str(row["end_time"]):
                eh, em = str(row["end_time"]).split(":")
                end_t = time(int(eh), int(em))
            user_shifts[(str(row["user_id"]), row["week_start"], int(row["dow"]))] = {
                "start_time": time(int(hh), int(mm)),
                "end_time": end_t,
                "break_start": bs_time,
                "break_end": be_time,
                "grace_minutes": int(row["grace_minutes"]),
            }
        except:
            pass

    # 2. Cargar horarios predeterminados
    def_sched_df = pd.read_sql_query(
        """
        SELECT week_start, dow, start_time, end_time, start_time_2, grace_minutes
        FROM schedules
        WHERE week_start >= %s AND week_start <= %s
    """,
        conn,
        params=(start_week_str, end_week_str),
    )

    default_schedules = {}
    for _, row in def_sched_df.iterrows():
        try:
            hh, mm = row["start_time"].split(":")
            st2 = None
            start_time_2 = row.get("start_time_2", "")
            if start_time_2 and ":" in str(start_time_2):
                hh2, mm2 = str(start_time_2).split(":")
                st2 = time(int(hh2), int(mm2))
            end_t = None
            if row.get("end_time") and ":" in str(row["end_time"]):
                eh, em = str(row["end_time"]).split(":")
                end_t = time(int(eh), int(em))
            default_schedules[(row["week_start"], int(row["dow"]))] = {
                "start_time": time(int(hh), int(mm)),
                "end_time": end_t,
                "start_time_2": st2,
                "grace_minutes": int(row["grace_minutes"]),
            }
        except:
            pass

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

        exc = exceptions_dict.get((uid, d.isoformat()))
        lrs = user_date_lrs.get((uid, d), [])

        # 1. Mañana: Primer punch del día
        r0 = punches[0][1]
        expected_start = datetime.combine(d, sched["start_time"])
        late_min, _ = _evaluate_punch_justification(
            r0, expected_start, sched, exc, lrs, is_break=False
        )
        if late_min is not None and late_min > 0:
            details.append(
                {
                    "user_id": r0["user_id"],
                    "fecha": d.isoformat(),
                    "hora_marcacion": r0["ts"].strftime("%H:%M:%S"),
                    "hora_inicio": expected_start.strftime("%H:%M"),
                    "gracia_min": sched.get("grace_minutes", 0),
                    "minutos_tarde": late_min,
                    "device_name": r0["device_name"],
                    "device_ip": r0["device_ip"],
                }
            )

        # 2. Break: Buscar el siguiente punch que sea Entrada (0) o Break In (2) dentro de la ventana real
        be = sched.get("break_end") or sched.get("start_time_2")
        if be:
            expected_be = datetime.combine(d, be)
            num_punches = len(punches)
            for _, r in punches[1:]:
                if r["punch"] in (0, 2):
                    if not _is_plausible_break_return(r["ts"], sched, num_punches):
                        continue
                    late_be, _ = _evaluate_punch_justification(
                        r, expected_be, sched, exc, lrs, is_break=True
                    )
                    if late_be is not None and late_be > 0:
                        details.append(
                            {
                                "user_id": r["user_id"],
                                "fecha": d.isoformat(),
                                "hora_marcacion": r["ts"].strftime("%H:%M:%S"),
                                "hora_inicio": expected_be.strftime("%H:%M"),
                                "gracia_min": sched.get("grace_minutes", 0),
                                "minutos_tarde": late_be,
                                "device_name": r["device_name"],
                                "device_ip": r["device_ip"],
                            }
                        )
                    break

    detail_df = pd.DataFrame(details)
    if detail_df.empty:
        return pd.DataFrame(), pd.DataFrame()

    summary_df = (
        detail_df.groupby("user_id", as_index=False)
        .agg(
            dias_tarde=("fecha", "nunique"),
            minutos_tarde_total=("minutos_tarde", "sum"),
            fechas_tarde=(
                "fecha",
                lambda x: ", ".join(sorted([str(i) for i in x.unique()])),
            ),
        )
        .sort_values(["minutos_tarde_total", "dias_tarde"], ascending=False)
    )
    return summary_df, detail_df


def to_excel_bytes(
    summary_df: pd.DataFrame,
    detail_df: pd.DataFrame,
    raw_df: pd.DataFrame | None = None,
):
    """Empaqueta los DataFrames de Pandas en un archivo Excel descargable."""
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        summary_df.to_excel(writer, index=False, sheet_name="RESUMEN_TARDANZAS")
        detail_df.to_excel(writer, index=False, sheet_name="DETALLE_TARDANZAS")
        if raw_df is not None and not raw_df.empty:
            raw_df.to_excel(writer, index=False, sheet_name="MARCACIONES_RAW")
    output.seek(0)
    return output.getvalue()


def get_late_punch_ids(
    start_date: date,
    end_date: date,
    return_justified: bool = False,
    user_id: str | None = None,
) -> dict | tuple[dict, dict]:
    """
    Calcula qué marcaciones corresponden a llegadas tarde en un rango de fechas,
    tomando en cuenta excepciones y permisos/justificaciones (leave_requests).
    Opcionalmente se puede filtrar por un user_id específico.
    Retorna:
      - Si return_justified es False: diccionario {id_marcacion: minutos_tarde}
      - Si return_justified es True: tupla ({id_marcacion: minutos_tarde}, {id_marcacion: info_justificacion})
    """
    # 1. Ajustar el rango de tiempo de datetime para la consulta de marcaciones
    start_dt = datetime.combine(start_date, time(0, 0))
    end_dt = datetime.combine(end_date + timedelta(days=1), time(0, 0))

    # 2. Extraer todas las marcaciones crudas del periodo ordenadas por user_id y ts
    conn = db_conn()
    raw_query = """
        SELECT id, user_id, ts, punch, device_name, device_ip
        FROM attendance_raw
        WHERE ts >= %s AND ts < %s AND is_ignored = 0
    """
    raw_params = [
        start_dt.isoformat(sep=" ", timespec="seconds"),
        end_dt.isoformat(sep=" ", timespec="seconds"),
    ]
    if user_id:
        raw_query += " AND user_id = %s"
        raw_params.append(str(user_id))
    raw_query += " ORDER BY user_id, ts"

    df = pd.read_sql_query(raw_query, conn, params=tuple(raw_params))

    if df.empty:
        conn.close()
        return ({}, {}) if return_justified else {}

    df["ts"] = pd.to_datetime(df["ts"])
    df = deduplicate_attendance(df)
    df = apply_business_punch_rules(df)
    df["day"] = df["ts"].dt.date

    # 3. Cargar excepciones y novedades en leave_requests para ese periodo
    exceptions_dict, user_date_lrs = _load_leaves_and_exceptions(
        start_date, end_date, conn, user_id=user_id
    )

    # 4. Cargar horarios y asignaciones
    start_week_str = (start_date - timedelta(days=start_date.weekday())).isoformat()
    end_week_str = (end_date - timedelta(days=end_date.weekday())).isoformat()

    sa_query = """
        SELECT sa.user_id, sa.week_start, sa.dow, s.start_time, s.end_time, s.grace_minutes, s.has_break, s.break_start, s.break_end
        FROM shift_assignments sa
        JOIN shifts s ON sa.shift_id = s.id
        WHERE sa.week_start >= %s AND sa.week_start <= %s
    """
    sa_params = [start_week_str, end_week_str]
    if user_id:
        sa_query += " AND sa.user_id = %s"
        sa_params.append(str(user_id))

    sa_df = pd.read_sql_query(sa_query, conn, params=tuple(sa_params))

    user_shifts = {}
    for _, row in sa_df.iterrows():
        try:
            hh, mm = row["start_time"].split(":")
            be_time = None
            if row["has_break"] and row["break_end"] and ":" in str(row["break_end"]):
                be_hh, be_mm = str(row["break_end"]).split(":")
                be_time = time(int(be_hh), int(be_mm))
            bs_time = None
            if row["has_break"] and row["break_start"] and ":" in str(row["break_start"]):
                bsh, bsm = str(row["break_start"]).split(":")
                bs_time = time(int(bsh), int(bsm))
            end_t = None
            if row["end_time"] and ":" in str(row["end_time"]):
                eh, em = str(row["end_time"]).split(":")
                end_t = time(int(eh), int(em))
            user_shifts[(str(row["user_id"]), row["week_start"], int(row["dow"]))] = {
                "start_time": time(int(hh), int(mm)),
                "end_time": end_t,
                "break_start": bs_time,
                "break_end": be_time,
                "grace_minutes": int(row["grace_minutes"]),
            }
        except:
            pass

    def_sched_df = pd.read_sql_query(
        """
        SELECT week_start, dow, start_time, end_time, start_time_2, grace_minutes
        FROM schedules
        WHERE week_start >= %s AND week_start <= %s
    """,
        conn,
        params=(start_week_str, end_week_str),
    )

    default_schedules = {}
    for _, row in def_sched_df.iterrows():
        try:
            hh, mm = row["start_time"].split(":")
            st2 = None
            start_time_2 = row.get("start_time_2", "")
            if start_time_2 and ":" in str(start_time_2):
                hh2, mm2 = str(start_time_2).split(":")
                st2 = time(int(hh2), int(mm2))
            end_t = None
            if row.get("end_time") and ":" in str(row["end_time"]):
                eh, em = str(row["end_time"]).split(":")
                end_t = time(int(eh), int(em))
            default_schedules[(row["week_start"], int(row["dow"]))] = {
                "start_time": time(int(hh), int(mm)),
                "end_time": end_t,
                "start_time_2": st2,
                "grace_minutes": int(row["grace_minutes"]),
            }
        except:
            pass

    conn.close()

    # 5. Agrupar por usuario y día
    entries_df = df.sort_values("ts")
    grouped = entries_df.groupby(["user_id", "day"])

    late_dict = {}
    justified_dict = {}

    for (user_id, day), group in grouped:
        d = day
        uid = str(user_id)

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

        exc = exceptions_dict.get((uid, d.isoformat()))
        lrs = user_date_lrs.get((uid, d), [])

        # 1. Mañana: Primer punch del día
        r0 = punches[0][1]
        expected_start = datetime.combine(d, sched["start_time"])
        late_m, just_m = _evaluate_punch_justification(
            r0, expected_start, sched, exc, lrs, is_break=False
        )
        if late_m is not None and late_m > 0:
            late_dict[int(r0["id"])] = late_m
        if just_m is not None:
            justified_dict[int(r0["id"])] = just_m

        # 2. Break: Buscar el siguiente punch que sea Entrada (0 o 2) dentro de la ventana real
        be = sched.get("break_end") or sched.get("start_time_2")
        if be:
            expected_be = datetime.combine(d, be)
            num_punches = len(punches)
            for _, r in punches[1:]:
                if r["punch"] in (0, 2):
                    if not _is_plausible_break_return(r["ts"], sched, num_punches):
                        continue
                    late_b, just_b = _evaluate_punch_justification(
                        r, expected_be, sched, exc, lrs, is_break=True
                    )
                    if late_b is not None and late_b > 0:
                        late_dict[int(r["id"])] = late_b
                    if just_b is not None:
                        justified_dict[int(r["id"])] = just_b
                    break

    if return_justified:
        return late_dict, justified_dict
    return late_dict
