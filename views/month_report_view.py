import calendar
import pandas as pd
import streamlit as st
from datetime import datetime, date, time, timedelta

from database_conn.connection import db_conn
from services.analytics import compute_month_lateness, fetch_attendance_between, to_excel_bytes
from utils.auth import require_role

def page_lateness_report():
    require_role("admin", "nomina")
    st.title("📉 Reporte Consolidado de Incidencias y Retardos")
    st.write("Genera el archivo avalado para procesos disciplinarios y descuentos de nómina.")

    today = date.today()
    col1, col2, col3 = st.columns(3)
    with col1:
        year = st.number_input("Año Fiscal", min_value=2020, max_value=2100, value=today.year, step=1)
    with col2:
        month = st.number_input("Mes de Nómina", min_value=1, max_value=12, value=today.month, step=1)
    with col3:
        search_dni = st.text_input("Buscar DNI", value="", placeholder="Ej: 100646459")

    st.markdown("<br>", unsafe_allow_html=True)
    if st.button("⚙️ Generar Analítica de Tardanzas", type="primary"):
        with st.spinner("Procesando histórico de marcaciones cruzado con horarios..."):
            summary_df, detail_df = compute_month_lateness(int(year), int(month))

        if summary_df.empty:
            st.success("🎉 ¡Excelente! No se registran tardanzas (o faltan asignaciones previas).")
            return
            
        # Top 3 de personas con más faltas
        st.markdown("### 🏆 Top 3: Mayores Incidencias")
        top3 = summary_df.head(3)
        cols = st.columns(3)
        for i, (idx, row) in enumerate(top3.iterrows()):
            if i < 3:
                cols[i].metric(f"ID {row['user_id']}", f"{row['minutos_tarde_total']} mins", f"{row['dias_tarde']} días infractores", delta_color="inverse")

        st.markdown("---")
        st.markdown("### 📊 Tabla General Resumida")
        
        # Unir nombre en el summary temporalmente para mostrarlo bonito
        conn = db_conn()
        nombres = pd.read_sql_query("SELECT user_id, full_name, department FROM employees", conn)
        conn.close()
        
        summary_view = pd.merge(summary_df, nombres, on="user_id", how="left")
        
        if search_dni:
            summary_view = summary_view[summary_view["user_id"].astype(str).str.contains(search_dni.strip())]
            
        summary_view = summary_view[["user_id", "full_name", "department", "dias_tarde", "minutos_tarde_total", "fechas_tarde"]]
        summary_view.columns = ["DNI Empleado", "Nombre", "Departamento", "Días con Retraso", "Minutos Totales Adeudados", "Fechas de Retraso"]
        
        st.dataframe(summary_view, use_container_width=True, hide_index=True)

        first_day = date(int(year), int(month), 1)
        last_day = date(int(year), int(month), calendar.monthrange(int(year), int(month))[1])

        raw_df = fetch_attendance_between(datetime.combine(first_day, time(0, 0)),
                                          datetime.combine(last_day + timedelta(days=1), time(0, 0)))

        excel_bytes = to_excel_bytes(summary_df, detail_df, raw_df)
        st.markdown("<br>", unsafe_allow_html=True)
        st.download_button(
            "📥 Descargar Archivo Oficial (Excel Completo)",
            data=excel_bytes,
            file_name=f"Reporte_Tardanzas_Dolormed_{year}_{month:02d}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True
        )