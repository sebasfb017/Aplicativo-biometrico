from datetime import date, timedelta

import pandas as pd
import plotly.express as px
import streamlit as st

from database_conn.connection import db_conn
from services.analytics import compute_month_lateness


def page_dashboard():
    st.markdown(
        """
        <style>
        /* Glassmorphism y animaciones para KPIs */
        div[data-testid="stMetric"] {
            background: linear-gradient(135deg, rgba(13, 110, 253, 0.05), rgba(13, 110, 253, 0.01));
            border-radius: 12px;
            padding: 20px;
            box-shadow: 0 4px 15px rgba(0, 0, 0, 0.05);
            transition: transform 0.2s ease, box-shadow 0.2s ease;
        }
        div[data-testid="stMetric"]:hover {
            transform: translateY(-5px);
            box-shadow: 0 10px 25px rgba(13, 110, 253, 0.15);
        }
        </style>
    """,
        unsafe_allow_html=True,
    )

    st.title("📊 Panel Principal - Dolormed")
    st.write("Resumen rápido y visual del sistema de Recursos Humanos.")

    conn = db_conn()
    cur = conn.cursor()

    # KPIs
    cur.execute("SELECT COUNT(*) FROM employees")
    total_empleados = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM leave_requests WHERE status LIKE 'PENDING%'")
    novedades_pend = cur.fetchone()[0]

    cur.execute(
        "SELECT COUNT(*) FROM attendance_raw WHERE date(ts) = date('now', 'localtime') AND is_ignored = 0"
    )
    marcaciones_hoy = cur.fetchone()[0]

    col1, col2, col3 = st.columns(3)

    def render_kpi(icon, title, value, subtitle, color):
        html = f"""
        <div style="background: linear-gradient(135deg, rgba(255,255,255,0.03), rgba(0,0,0,0));
                    border: 1px solid rgba(255,255,255,0.05); border-radius: 16px; padding: 20px;
                    box-shadow: 0 8px 32px rgba(0,0,0,0.1); border-bottom: 3px solid {color};
                    animation: slideUp 0.6s cubic-bezier(0.16, 1, 0.3, 1) forwards; opacity: 0; transform: translateY(20px);">
            <style>
            @keyframes slideUp {{
                to {{ opacity: 1; transform: translateY(0); }}
            }}
            </style>
            <div style="font-size: 1.0rem; color: #9CA3AF; font-weight: 500; margin-bottom: 5px; display: flex; align-items: center; gap: 8px;">
                <span style="font-size: 1.3rem;">{icon}</span> {title}
            </div>
            <div style="font-size: 2.8rem; font-weight: 700; color: #F3F4F6; line-height: 1.1; text-shadow: 0 0 20px {color}40;">
                {value}
            </div>
            <div style="font-size: 0.85rem; color: {color}; margin-top: 8px; font-weight: 600;">
                {subtitle}
            </div>
        </div>
        """
        return html

    with col1:
        st.markdown(
            render_kpi(
                "👥",
                "Empleados Activos",
                total_empleados,
                "↑ Base de datos actualizada",
                "#3B82F6",
            ),
            unsafe_allow_html=True,
        )
    with col2:
        st.markdown(
            render_kpi(
                "⏱️",
                "Marcaciones Hoy",
                marcaciones_hoy,
                "⚡ Actividad de red en vivo",
                "#10B981",
            ),
            unsafe_allow_html=True,
        )
    with col3:
        alert_color = "#EF4444" if novedades_pend > 0 else "#6B7280"
        alert_sub = (
            "Requieren revisión urgente" if novedades_pend > 0 else "Todo al día"
        )
        st.markdown(
            render_kpi(
                "🔔", "Permisos Pendientes", novedades_pend, alert_sub, alert_color
            ),
            unsafe_allow_html=True,
        )

    st.markdown("---")

    # Gráfico de Llegadas Tarde por Área (Este Mes)
    st.subheader("⏱️ Minutos de Retraso por Área")
    st.write(
        "Cálculo dinámico cruzando horarios oficiales con marcaciones del biométrico."
    )

    today = date.today()
    default_year = today.year
    default_month = today.month

    if today.day <= 5:
        default_month -= 1
        if default_month == 0:
            default_month = 12
            default_year -= 1

    meses = [
        "Enero",
        "Febrero",
        "Marzo",
        "Abril",
        "Mayo",
        "Junio",
        "Julio",
        "Agosto",
        "Septiembre",
        "Octubre",
        "Noviembre",
        "Diciembre",
    ]

    c_m1, c_m2 = st.columns(2)
    with c_m1:
        sel_month_name = st.selectbox(
            "Mes", meses, index=default_month - 1, key="dash_month"
        )
        sel_month = meses.index(sel_month_name) + 1
    with c_m2:
        sel_year = st.selectbox(
            "Año", list(range(today.year - 2, today.year + 2)), index=2, key="dash_year"
        )

    try:
        summary_df, _ = compute_month_lateness(sel_year, sel_month)

        if summary_df.empty:
            st.success("¡Excelente! No hay tardanzas acumuladas este mes.")
        else:
            from database_conn.queries import get_cached_employees

            nombres = get_cached_employees()

            merged = pd.merge(summary_df, nombres, on="user_id", how="left")

            def get_main_area(dept):
                if not dept or pd.isna(dept):
                    return "Sin Área"
                if " - " in str(dept):
                    return str(dept).split(" - ")[0]
                return str(dept)

            merged["Area"] = merged["department"].apply(get_main_area)

            col_chart, col_top5 = st.columns([2, 1])
            with col_chart:
                area_tarde = (
                    merged.groupby("Area")["minutos_tarde_total"].sum().reset_index()
                )
                area_tarde = area_tarde[area_tarde["minutos_tarde_total"] > 0]

                if not area_tarde.empty:
                    fig_tarde = px.bar(
                        area_tarde,
                        x="Area",
                        y="minutos_tarde_total",
                        color="Area",
                        labels={
                            "Area": "Área",
                            "minutos_tarde_total": "Minutos Acumulados",
                        },
                        title="",
                    )
                    st.plotly_chart(fig_tarde, use_container_width=True)
                else:
                    st.success("¡Excelente! No hay tardanzas acumuladas este mes.")

            with col_top5:
                st.markdown("**Top 5: Retrasos Críticos**")
                top5_df = merged.sort_values(
                    by="minutos_tarde_total", ascending=False
                ).head(5)
                top5_df = top5_df[top5_df["minutos_tarde_total"] > 0]
                if not top5_df.empty:
                    html_t5 = "<div class='premium-table-container'><table class='premium-table'>"
                    html_t5 += "<thead><tr><th>Empleado</th><th>Minutos</th></tr></thead><tbody>"
                    for _, r in top5_df.iterrows():
                        html_t5 += f"<tr><td><b>{r['full_name']}</b></td><td><span class='badge-modern badge-amber'>⏱️ {r['minutos_tarde_total']} min</span></td></tr>"
                    html_t5 += "</tbody></table></div>"
                    st.markdown(html_t5, unsafe_allow_html=True)
                else:
                    st.info("No hay retrasos registrados.")

    except Exception as e:
        st.error(f"No se pudo cargar el gráfico de tardanzas: {e}")

    st.markdown("---")

    # Gráfico de Marcaciones Recientes (Últimos 7 días)
    st.subheader("📈 Actividad del Biométrico (Últimos 7 Días)")
    df_act = pd.read_sql_query(
        """
        SELECT date(ts) as fecha, COUNT(*) as cantidad 
        FROM attendance_raw 
        WHERE date(ts) >= date('now', '-7 days') AND is_ignored = 0
        GROUP BY date(ts)
        ORDER BY date(ts)
    """,
        conn,
    )

    if not df_act.empty:
        fig = px.bar(
            df_act,
            x="fecha",
            y="cantidad",
            labels={"fecha": "Fecha", "cantidad": "Marcaciones Totales"},
            title="",
            color_discrete_sequence=["#0D6EFD"],
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No hay suficientes datos recientes para mostrar el gráfico.")

    st.markdown("---")

    # Solicitudes Pendientes de Acción
    st.subheader("📋 Solicitudes Pendientes de Acción")
    st.write("Los permisos más recientes que esperan aprobación.")

    df_pendientes = pd.read_sql_query(
        """
        SELECT lr.id as Radicado, lr.request_date as Fecha, u.full_name as Empleado, 
               e.department as Departamento, lr.reason_type as Motivo, lr.status as Estado
        FROM leave_requests lr
        JOIN users_app u ON lr.user_id = u.username
        LEFT JOIN employees e ON u.username = e.user_id
        WHERE lr.status LIKE 'PENDING_%'
        ORDER BY lr.id DESC LIMIT 10
    """,
        conn,
    )

    if not df_pendientes.empty:
        html_table = (
            "<div class='premium-table-container'><table class='premium-table'>"
        )
        html_table += "<thead><tr><th>Radicado</th><th>Fecha</th><th>Empleado</th><th>Departamento</th><th>Motivo</th><th>Estado</th></tr></thead><tbody>"
        for _, r in df_pendientes.iterrows():
            st_badge = "badge-amber"
            estado_str = (
                str(r["Estado"])
                .replace("PENDING_COORD", "🕒 Coord.")
                .replace("PENDING_JEFE", "🕒 Jefe Área")
                .replace("PENDING_RRHH", "🕒 RRHH")
            )
            html_table += f"<tr><td>#{r['Radicado']}</td><td>{r['Fecha']}</td><td><b>{r['Empleado']}</b></td><td>{r.get('Departamento', 'N/A')}</td><td>{r['Motivo']}</td><td><span class='badge-modern {st_badge}'>{estado_str}</span></td></tr>"
        html_table += "</tbody></table></div>"
        st.markdown(html_table, unsafe_allow_html=True)
    else:
        st.success("¡Todo al día! No hay solicitudes pendientes de aprobación.")

    st.markdown("---")
    st.subheader("🔔 Panel de Auto-Auditoría (Alertas RRHH)")
    st.write("El sistema analiza posibles incidencias para que no se te escapen.")

    col_a1, col_a2 = st.columns(2)
    with col_a1:
        st.markdown("**🚨 Posibles Faltas de Ayer**")
        yesterday = (date.today() - timedelta(days=1)).isoformat()

        missing_query = """
            SELECT e.full_name as Empleado, sa.user_id as ID
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

        miss_df = pd.read_sql_query(
            missing_query, conn, params=(y_ws, y_dow, yesterday, yesterday)
        )

        if miss_df.empty:
            st.success(f"Todo en orden. No hay faltas detectadas para el {yesterday}.")
        else:
            st.warning(f"Se detectaron {len(miss_df)} posibles faltas.")
            with st.expander("Ver lista de empleados con posibles faltas"):
                st.dataframe(miss_df, hide_index=True, use_container_width=True)

    with col_a2:
        st.markdown("**⚠️ Empleados sin Turno Asignado**")
        this_week_start = (
            date.today() - timedelta(days=date.today().weekday())
        ).isoformat()
        no_shift_query = """
            SELECT full_name as Empleado, user_id as ID
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
            st.error(
                f"Hay {len(no_sch_df)} empleados sin asignación de horario esta semana."
            )
            with st.expander("Ver lista de empleados sin turno"):
                st.dataframe(no_sch_df, hide_index=True, use_container_width=True)
