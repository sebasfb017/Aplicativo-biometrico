import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import date, timedelta

from database_conn.connection import db_conn
from services.analytics import compute_month_lateness

def page_dashboard():
    st.title("📊 Panel Principal - Dolormed")
    st.write("Resumen rápido del sistema de Recursos Humanos.")
    
    conn = db_conn()
    cur = conn.cursor()
    
    # KPIs
    cur.execute("SELECT COUNT(*) FROM employees")
    total_empleados = cur.fetchone()[0]
    
    cur.execute("SELECT COUNT(*) FROM leave_requests WHERE status LIKE 'PENDING%'")
    novedades_pend = cur.fetchone()[0]
    
    cur.execute("SELECT COUNT(*) FROM attendance_raw WHERE date(ts) = date('now', 'localtime') AND is_ignored = 0")
    marcaciones_hoy = cur.fetchone()[0]
    
    col1, col2, col3 = st.columns(3)
    with col1:
        with st.container(border=True):
            st.metric("👥 Empleados Activos", total_empleados, "Base de datos")
    with col2:
        with st.container(border=True):
            st.metric("⏱️ Marcaciones de Hoy", marcaciones_hoy, "Actividad de red")
    with col3:
        with st.container(border=True):
            st.metric("🔔 Permisos Pendientes", novedades_pend, "- Requieren revisión", delta_color="inverse")
    
    st.markdown("---")
    
    # Gráfico de Llegadas Tarde por Área (Este Mes)
    st.subheader("⏱️ Minutos de Retraso por Área (Mes Actual)")
    st.write("Cálculo dinámico cruzando horarios oficiales con marcaciones del biométrico.")
    
    try:
        summary_df, _ = compute_month_lateness(date.today().year, date.today().month)
        
        if summary_df.empty:
            st.success("¡Excelente! No hay tardanzas acumuladas este mes.")
        else:
            nombres = pd.read_sql_query("SELECT user_id, department FROM employees", conn)
            
            merged = pd.merge(summary_df, nombres, on="user_id", how="left")
            
            def get_main_area(dept):
                if not dept or pd.isna(dept): return "Sin Área"
                if " - " in str(dept): return str(dept).split(" - ")[0]
                return str(dept)
                
            merged['Area'] = merged['department'].apply(get_main_area)
            
            area_tarde = merged.groupby('Area')['minutos_tarde_total'].sum().reset_index()
            area_tarde = area_tarde[area_tarde['minutos_tarde_total'] > 0]
            
            if not area_tarde.empty:
                fig_tarde = px.bar(area_tarde, x='Area', y='minutos_tarde_total', color='Area',
                             labels={'Area': 'Área', 'minutos_tarde_total': 'Minutos Acumulados'},
                             title="")
                st.plotly_chart(fig_tarde, use_container_width=True)
            else:
                st.success("¡Excelente! No hay tardanzas acumuladas este mes.")
                
    except Exception as e:
        st.error(f"No se pudo cargar el gráfico de tardanzas: {e}")

    st.markdown("---")
    
    # Gráfico de Marcaciones Recientes (Últimos 7 días)
    st.subheader("📈 Actividad del Biométrico (Últimos 7 Días)")
    df_act = pd.read_sql_query("""
        SELECT date(ts) as fecha, COUNT(*) as cantidad 
        FROM attendance_raw 
        WHERE date(ts) >= date('now', '-7 days') AND is_ignored = 0
        GROUP BY date(ts)
        ORDER BY date(ts)
    """, conn)
    
    if not df_act.empty:
        fig = px.bar(df_act, x='fecha', y='cantidad', labels={'fecha':'Fecha', 'cantidad':'Marcaciones Totales'}, title="", color_discrete_sequence=['#0D6EFD'])
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No hay suficientes datos recientes para mostrar el gráfico.")

    st.markdown("---")
    st.subheader("🔔 Panel de Auto-Auditoría (Alertas RRHH)")
    st.write("El sistema analiza posibles incidencias para que no se te escapen.")
    
    col_a1, col_a2 = st.columns(2)
    with col_a1:
        st.markdown("**🚨 Posibles Faltas de Ayer**")
        yesterday = (date.today() - timedelta(days=1)).isoformat()
        
        missing_query = """
            SELECT e.full_name, sa.user_id 
            FROM shift_assignments sa
            JOIN employees e ON sa.user_id = e.user_id
            WHERE sa.week_start = ? AND sa.dow = ? 
            AND sa.user_id NOT IN (
                SELECT user_id FROM attendance_raw WHERE date(ts) = ? AND is_ignored = 0
            )
            AND sa.user_id NOT IN (
                SELECT user_id FROM exceptions WHERE date = ?
            )
        """
        y_date = date.today() - timedelta(days=1)
        y_ws = (y_date - timedelta(days=y_date.weekday())).isoformat()
        y_dow = y_date.weekday()
        
        miss_df = pd.read_sql_query(missing_query, conn, params=(y_ws, y_dow, yesterday, yesterday))
        
        if miss_df.empty:
            st.success(f"Todo en orden. No hay faltas detectadas para el {yesterday}.")
        else:
            for _, r in miss_df.iterrows():
                st.error(f"Falta ayer: {r['full_name']} (ID: {r['user_id']})")
                
    with col_a2:
        st.markdown("**⚠️ Empleados sin Turno Asignado**")
        this_week_start = (date.today() - timedelta(days=date.today().weekday())).isoformat()
        no_shift_query = """
            SELECT full_name, user_id
            FROM employees 
            WHERE user_id NOT IN (
                SELECT DISTINCT user_id FROM shift_assignments WHERE week_start = ?
            )
        """
        no_sch_df = pd.read_sql_query(no_shift_query, conn, params=(this_week_start,))
        conn.close()
        
        if no_sch_df.empty:
            st.success("Toda la planilla tiene turnos asignados esta semana.")
        else:
            if len(no_sch_df) > 10:
                st.warning(f"Hay {len(no_sch_df)} empleados sin asignación de horario esta semana.")
            else:
                for _, r in no_sch_df.iterrows():
                    st.warning(f"Sin horario: {r['full_name']} (ID: {r['user_id']})")