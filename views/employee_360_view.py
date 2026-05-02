import calendar
import textwrap
import pandas as pd
import streamlit as st
from datetime import datetime, date, time, timedelta

from database_conn.connection import db_conn
from utils.auth import require_role
from database_conn.queries import get_all_employees, is_holiday
from services.analytics import schedule_for_user_date, fetch_attendance_between

def get_employee_exceptions(year, month, user_id):
    conn = db_conn()
    start_date = date(year, month, 1).isoformat()
    end_date = date(year, month, calendar.monthrange(year, month)[1]).isoformat()
    df = pd.read_sql_query(
        "SELECT date as fecha, type as tipo, notes as notas FROM exceptions WHERE user_id = ? AND date >= ? AND date <= ?",
        conn, params=(str(user_id), start_date, end_date)
    )
    conn.close()
    return df

def get_employee_raw_punches(year, month, user_id):
    first_day = date(year, month, 1)
    last_day = date(year, month, calendar.monthrange(year, month)[1])
    start_dt = datetime.combine(first_day, time(0, 0))
    end_dt = datetime.combine(last_day + timedelta(days=1), time(0, 0))
    
    df = fetch_attendance_between(start_dt, end_dt)
    if df.empty:
        return pd.DataFrame()
    user_df = df[df["user_id"] == str(user_id)]
    return user_df

def page_employee_360():
    require_role("admin", "nomina")
    st.markdown("<h2 style='text-align:center; font-weight:700;'>📇 Expediente 360</h2>", unsafe_allow_html=True)
    st.markdown("<p style='text-align:center; color:gray; font-size:1.1rem; margin-bottom:2rem;'>Vista gerencial avanzada del historial de tiempos por empleado.</p>", unsafe_allow_html=True)

    # Controles de Filtro
    today = date.today()
    col1, col2, col3 = st.columns([1, 1, 2])
    with col1:
        selected_year = st.number_input("Año", min_value=2020, max_value=2100, value=today.year, step=1)
    with col2:
        selected_month = st.number_input("Mes", min_value=1, max_value=12, value=today.month, step=1)
    with col3:
        emp_df = get_all_employees()
        if emp_df.empty:
            st.warning("No hay empleados en la base de datos.")
            return
            
        emp_options = emp_df["user_id"].astype(str) + " - " + emp_df["full_name"]
        selected_emp_raw = st.selectbox("Empleado", options=emp_options)
        selected_user_id = selected_emp_raw.split(" - ")[0]
        selected_user_name = selected_emp_raw.split(" - ")[1]

    st.markdown("---")
    
    with st.spinner("Compilando biografía de tiempos..."):
        exc_df = get_employee_exceptions(selected_year, selected_month, selected_user_id)
        exceptions_dict = {row["fecha"]: row["tipo"] for _, row in exc_df.iterrows()}
        
        raw_df = get_employee_raw_punches(selected_year, selected_month, selected_user_id)
        if not raw_df.empty:
            raw_df["fecha_str"] = raw_df["ts"].dt.strftime("%Y-%m-%d")
            punches_por_dia = raw_df.groupby("fecha_str")["ts"].apply(list).to_dict()
        else:
            punches_por_dia = {}
            
        dias_del_mes = calendar.monthrange(selected_year, selected_month)[1]
        start_weekday = date(selected_year, selected_month, 1).weekday() # 0=Lun, 6=Dom
        
        llegadas_tarde_count = 0
        minutos_tarde_acum = 0
        faltas_count = 0
        novedades_count = len(exc_df)
        
        calendar_cells = []
        feed_incidentes = []
        
        conn = db_conn()
        
        for d in range(1, dias_del_mes + 1):
            current_date = date(selected_year, selected_month, d)
            date_str = current_date.isoformat()
            
            sched = schedule_for_user_date(selected_user_id, current_date, conn)
            novedad = exceptions_dict.get(date_str, "-")
            dia_punches = sorted(punches_por_dia.get(date_str, []))
            
            # Tipos: normal, retardo, falta, novedad, libre, pendiente, huerfano (marco sin turno)
            tipo_dia = "normal" 
            late_min = 0
            turno_str = sched["start_time"].strftime("%H:%M") if sched else "Libre"
            marcaciones_str = " | ".join([p.strftime("%H:%M") for p in dia_punches]) if dia_punches else "Sin registros"
            
            am_punches = [p.strftime("%H:%M") for p in dia_punches if p.time() < time(13, 0)]
            pm_punches = [p.strftime("%H:%M") for p in dia_punches if p.time() >= time(13, 0)]
            marcaciones_am = " | ".join(am_punches) if am_punches else "--:--"
            marcaciones_pm = " | ".join(pm_punches) if pm_punches else "--:--"
            
            es_festivo = is_holiday(current_date)
            
            if novedad != "-":
                tipo_dia = "novedad"
            elif sched is None:
                if dia_punches:
                    tipo_dia = "huerfano"
                else:
                    tipo_dia = "libre"
            else:
                if not dia_punches and current_date < date.today():
                    if es_festivo:
                        tipo_dia = "festivo"
                    else:
                        tipo_dia = "falta"
                        faltas_count += 1
                elif not dia_punches and current_date >= date.today():
                    tipo_dia = "pendiente"
                else:
                    primer_punch = dia_punches[0]
                    sched_start = datetime.combine(current_date, sched["start_time"])
                    grace = timedelta(minutes=sched["grace_minutes"])
                    late_after = sched_start + grace
                    
                    if primer_punch > late_after:
                        late_min = int((primer_punch - late_after).total_seconds() // 60)
                        tipo_dia = "retardo"
                        llegadas_tarde_count += 1
                        minutos_tarde_acum += late_min

            # Guardar para el feed si es anomalia
            if tipo_dia in ["retardo", "falta", "huerfano"]:
                emoji = "⏰" if tipo_dia == "retardo" else ("❌" if tipo_dia == "falta" else "⚠️")
                desc = f"Llegó {late_min} min tarde" if tipo_dia == "retardo" else ("No se presentó al turno" if tipo_dia == "falta" else "Marcó en día libre")
                feed_incidentes.append({
                    "fecha": current_date.strftime("%d %b"),
                    "icono": emoji,
                    "titulo": f"Incidencia el {current_date.strftime('%A')}",
                    "desc": desc,
                    "turno": turno_str,
                    "marcado": " | ".join([p.strftime("%H:%M") for p in dia_punches]) if dia_punches else "Sin registros",
                    "clase": tipo_dia
                })
            
            # Generar datos de la celda
            calendar_cells.append({
                "day": d,
                "date": date_str,
                "type": tipo_dia,
                "turno": turno_str,
                "has_punches": bool(dia_punches),
                "marcaciones_am": marcaciones_am,
                "marcaciones_pm": marcaciones_pm,
                "novedad": novedad,
                "es_festivo": es_festivo
            })
            
        conn.close()

    # ==========================
    # RENDERIZADO VISUAL HTML/CSS
    # ==========================
    
    # 1. TARJETAS KPI MODERNAS (Glassmorphism)
    st.markdown(f"""
    <style>
    .kpi-container {{
        display: flex; gap: 1.5rem; margin-bottom: 2rem; flex-wrap: wrap;
    }}
    .kpi-card {{
        flex: 1; min-width: 200px;
        background: linear-gradient(135deg, rgba(255,255,255,0.1), rgba(255,255,255,0.05));
        border: 1px solid rgba(255,255,255,0.2);
        border-radius: 15px; padding: 1.5rem;
        box-shadow: 0 8px 32px 0 rgba(0, 0, 0, 0.1);
        backdrop-filter: blur(10px);
        text-align: center;
        transition: transform 0.3s ease;
    }}
    .kpi-card:hover {{ transform: translateY(-5px); }}
    .kpi-title {{ font-size: 1rem; color: #6c757d; font-weight: 600; text-transform: uppercase; letter-spacing: 1px; }}
    .kpi-value {{ font-size: 2.5rem; font-weight: 800; margin: 10px 0; }}
    .kpi-sub {{ font-size: 0.9rem; font-weight: 500; padding: 4px 8px; border-radius: 20px; display: inline-block; }}
    
    /* Colores Específicos */
    .kpi-retardos .kpi-value {{ color: #fd7e14; }}
    .kpi-retardos .kpi-sub {{ background: rgba(253,126,20,0.15); color: #fd7e14; }}
    
    .kpi-faltas .kpi-value {{ color: #dc3545; }}
    .kpi-faltas .kpi-sub {{ background: rgba(220,53,69,0.15); color: #dc3545; }}
    
    .kpi-novedad .kpi-value {{ color: #0d6efd; }}
    .kpi-novedad .kpi-sub {{ background: rgba(13,110,253,0.15); color: #0d6efd; }}
    
    /* Tema oscuro automático text colors */
    @media (prefers-color-scheme: dark) {{
        .kpi-title {{ color: #adb5bd; }}
    }}
    </style>
    
    <div class="kpi-container">
        <div class="kpi-card kpi-retardos">
            <div class="kpi-title">Llegadas Tarde</div>
            <div class="kpi-value">{llegadas_tarde_count} <span style="font-size:1.2rem;">días</span></div>
            <div class="kpi-sub">+{minutos_tarde_acum} mins perdidos</div>
        </div>
        <div class="kpi-card kpi-faltas">
            <div class="kpi-title">Faltas Injustificadas</div>
            <div class="kpi-value">{faltas_count} <span style="font-size:1.2rem;">días</span></div>
            <div class="kpi-sub">Auditoría Requerida</div>
        </div>
        <div class="kpi-card kpi-novedad">
            <div class="kpi-title">Novedades (Permisos)</div>
            <div class="kpi-value">{novedades_count}</div>
            <div class="kpi-sub">Días legales autorizados</div>
        </div>
    </div>
    """, unsafe_allow_html=True)

    # 2. CALENDARIO MAPA DE CALOR
    
    # Construir HTML del grid
    meses_nombres = ["", "Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio", "Julio", "Agosto", "Septiembre", "Octubre", "Noviembre", "Diciembre"]
    
    grid_html = f"""
<style>
.cal-wrapper {{
  background: transparent;
  border: 1px solid rgba(128,128,128,0.2);
  border-radius: 12px; padding: 2rem; margin-bottom: 2rem;
  box-shadow: 0 4px 12px rgba(0,0,0,0.05);
  overflow-x: auto;
}}
.cal-header {{ 
  display: flex; min-width: 800px;
  text-align: center; font-weight: 700; margin-bottom: 10px; color: var(--text-color); opacity: 0.8; font-size:0.9rem; 
}}
.cal-header > div {{ flex: 1; }}
.cal-grid {{ 
  display: flex; flex-wrap: wrap; gap: 8px; min-width: 800px;
}}
.cal-cell {{
  width: calc(14.28% - 7px); min-height: 120px; border-radius: 8px; padding: 8px;
  display: flex; flex-direction: column;
  cursor: default; position: relative; transition: all 0.2s;
  border: 1px solid rgba(128,128,128,0.2);
}}
.cal-cell:hover {{ transform: scale(1.02); z-index: 10; box-shadow: 0 4px 8px rgba(0,0,0,0.1); }}

.day-number {{ align-self: flex-end; font-weight: 700; font-size: 1.1rem; color: var(--text-color); opacity: 0.9; margin-bottom: 5px; }}
.info-line {{ font-size: 0.8rem; font-weight: 500; margin-bottom: 3px; padding: 2px 4px; border-radius: 4px; }}

.info-turno {{ background: rgba(128,128,128,0.15); color: var(--text-color); opacity: 0.9; }}
.info-marcado {{ background: rgba(128,128,128,0.25); color: var(--text-color); font-weight: 700; }}
.info-novedad {{ background: rgba(234,179,8,0.2); color: var(--text-color); font-weight: 600; border: 1px solid rgba(234,179,8,0.4); }}
.info-falta {{ background: rgba(220,53,69,0.15); color: #dc3545; font-weight: 600; border: 1px solid rgba(220,53,69,0.2); }}

/* Tipos de día (Fondos). Usamos rgba para que se adapten al fondo oscuro/claro de Streamlit */
.day-empty {{ background: transparent; pointer-events: none; border: none; }}
.day-libre {{ background: rgba(128,128,128,0.05); border: 1px dashed rgba(128,128,128,0.3); }}
.day-pendiente {{ background: rgba(128,128,128,0.02); opacity: 0.7; }}
.day-normal {{ background: rgba(25,135,84,0.15); border-color: rgba(25,135,84,0.3); }}
.day-retardo {{ background: rgba(253,126,20,0.15); border-color: rgba(253,126,20,0.3); }}
.day-falta {{ background: rgba(220,53,69,0.15); border-color: rgba(220,53,69,0.3); }}
.day-novedad {{ background: rgba(234,179,8,0.15); border-color: rgba(234,179,8,0.3); }}
.day-huerfano {{ background: rgba(111,66,193,0.15); border-color: rgba(111,66,193,0.3); }}
.day-festivo {{ background: rgba(13,202,240,0.15); border-color: rgba(13,202,240,0.3); }}
</style>

<div class="cal-wrapper">
<h3 style="margin-top:0; color:#0D6EFD;">Matriz de Asistencia: {meses_nombres[selected_month]} {selected_year}</h3>

<div class="cal-header">
<div>Lunes</div><div>Martes</div><div>Miércoles</div><div>Jueves</div><div>Viernes</div><div>Sábado</div><div>Domingo</div>
</div>
<div class="cal-grid">
"""
    
    # Rellenar celdas vacias al inicio
    for _ in range(start_weekday):
        grid_html += '<div class="cal-cell day-empty"></div>'
        
    for cell in calendar_cells:
        inner_content = f'<div class="day-number">{cell["day"]}</div>'
        
        if cell["type"] != "libre" and cell["type"] != "empty":
            inner_content += f'<div class="info-line info-turno">Turno: {cell["turno"]}</div>'
        
        if cell["has_punches"]:
            inner_content += f'<div class="info-line info-marcado">AM: {cell["marcaciones_am"]}</div>'
            inner_content += f'<div class="info-line info-marcado">PM: {cell["marcaciones_pm"]}</div>'
        elif cell["type"] == "falta":
            inner_content += f'<div class="info-line info-falta">Inasistencia</div>'
            
        if cell["novedad"] != "-":
            inner_content += f'<div class="info-line info-novedad">⚠️ {cell["novedad"]}</div>'
        
        if cell["es_festivo"]:
            inner_content += f'<div class="info-line" style="background: rgba(13,202,240,0.2); color: #0dcaf0; font-weight: 600; border: 1px solid rgba(13,202,240,0.4);">🎉 Festivo</div>'
            
        grid_html += f'<div class="cal-cell day-{cell["type"]}">{inner_content}</div>'
        
    grid_html += """
</div>
</div>
"""
    
    st.markdown(grid_html, unsafe_allow_html=True)
    
    # 3. FEED DE ANOMALÍAS
    st.markdown("### 📋 Bitácora de Incidentes del Mes")
    if not feed_incidentes:
        st.success(f"¡Excelente historial! **{selected_user_name}** no tuvo ninguna inasistencia o retardo durante este periodo.")
    else:
        feed_html = """
<style>
.feed-container { display: flex; flex-direction: column; gap: 15px; }
.feed-item {
  display: flex; gap: 15px; background: rgba(128,128,128,0.05); border-left: 4px solid gray;
  padding: 15px; border-radius: 0 8px 8px 0; align-items: center;
}
.feed-item.retardo { border-left-color: #fd7e14; }
.feed-item.falta { border-left-color: #dc3545; }
.feed-item.huerfano { border-left-color: #6f42c1; }
.feed-icon { font-size: 2rem; background: white; width: 50px; height: 50px; display: flex; align-items: center; justify-content: center; border-radius: 50%; box-shadow: 0 2px 5px rgba(0,0,0,0.1); }
.feed-content { flex: 1; }
.feed-title { font-weight: 700; font-size: 1.1rem; margin-bottom: 2px; }
.feed-desc { color: gray; font-size: 0.95rem; margin-bottom: 5px; }
.feed-meta { font-size: 0.85rem; background: rgba(0,0,0,0.05); padding: 3px 8px; border-radius: 10px; display: inline-block; font-family: monospace;}
</style>
<div class="feed-container">
"""
        for inc in feed_incidentes:
            feed_html += f"""
<div class="feed-item {inc["clase"]}">
<div class="feed-icon">{inc["icono"]}</div>
<div class="feed-content">
<div class="feed-title">{inc["fecha"]} - {inc["titulo"]}</div>
<div class="feed-desc">{inc["desc"]}</div>
<div class="feed-meta">Turno: {inc["turno"]} | Marcó: {inc["marcado"]}</div>
</div>
</div>
"""
        feed_html += "</div>"
        st.markdown(feed_html, unsafe_allow_html=True)
