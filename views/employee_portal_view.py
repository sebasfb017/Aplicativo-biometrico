import streamlit as st
import pandas as pd

from database_conn.connection import db_conn
from database_conn.queries import db_create_leave_request
from services.notifications import generate_fth012_html

@st.dialog("Detalles de Mi Solicitud (F-TH-012)")
def show_leave_request_details(req_id: int):
    conn = db_conn()
    df_req = pd.read_sql_query("SELECT * FROM leave_requests WHERE id = ?", conn, params=(req_id,))
    
    # Extraer trazabilidad de aprobadores
    df_audit = pd.read_sql_query("""
        SELECT user_id, action, timestamp 
        FROM audit_logs 
        WHERE details LIKE ? AND action LIKE 'APPROVE_%'
        ORDER BY timestamp ASC
    """, conn, params=(f"%Permiso #{req_id} %",))
    
    conn.close()
    
    if df_req.empty:
        st.error("No se encontró la solicitud.")
        return
        
    req = df_req.iloc[0]
    
    st.markdown(f"### Radicado: #{req['id']}")
    
    if req['status'] == 'APPROVED':
        st.success("✅ Esta solicitud ha sido Aprobada definitivamente.")
        html_fth012 = generate_fth012_html(req, df_audit)
        st.download_button(
            label="📄 Descargar F-TH-012 (HTML/PDF)",
            data=html_fth012,
            file_name=f"F-TH-012_{req['id']}.html",
            mime="text/html",
            type="primary",
            use_container_width=True
        )
    else:
        st.markdown(f"**Estado Actual:** `{req['status']}`")
    
    c1, c2 = st.columns(2)
    with c1:
        st.markdown(f"**Fecha de Solicitud:** {req['request_date']}")
        st.markdown(f"**Fechas de Ausencia:** {req['leave_date_start']} al {req['leave_date_end']}")
        st.markdown(f"**Remunerado:** {'✅ Sí' if req['is_paid'] else '❌ No'}")
    with c2:
        h_in = req['start_time'] if req['start_time'] else "N/A"
        h_out = req['end_time'] if req['end_time'] else "N/A"
        st.markdown(f"**Hora Salida:** {h_in}")
        st.markdown(f"**Hora Entrada:** {h_out}")
        st.markdown(f"**Tiempo Total:** {req['total_time']}")
        
    st.divider()
    st.write(f"**Motivo General:** {req['reason_type']}")
    
    st.markdown("**Mi Justificación / Detalles:**")
    st.info(req['reason_description'] if req['reason_description'] else "Sin detalles ingresados.")
    
    if not req['is_paid'] and req['how_to_makeup']:
        st.markdown("**Acuerdo de Reposición Prometido:**")
        st.warning(req['how_to_makeup'])

    if not df_audit.empty:
        st.divider()
        st.markdown("**Trazabilidad de Aprobaciones:**")
        for _, row_a in df_audit.iterrows():
            level = "Jefatura" if row_a['action'] == "APPROVE_LEAVE_L1" else "Gestión Humana"
            st.caption(f"✓ **{level}**: {row_a['user_id']} ({row_a['timestamp']})")


def page_employee_portal():
    user = st.session_state["user"]
    st.title("🧑‍⚕️ Mi Portal de Autogestión (F-TH-012)")
    st.write(f"Bienvenido/a **{user['full_name']}** | {user.get('department', 'Sin Área Definida')}")
    
    t1, t2 = st.tabs(["📝 Radicar Nuevo Permiso", "🗂️ Mis Solicitudes"])
    
    with t1:
        st.subheader("Solicitud de Permisos Laborales Y/O Personales")
        st.info("Llena el siguiente formulario digital equivalente al formato F-TH-012 físico.")
        
        with st.form("form_leave_request"):
            c1, c2 = st.columns(2)
            with c1:
                leave_dates = st.date_input("Fecha(s) del Permiso", value=[], help="Selecciona uno o varios días de ausencia.", format="YYYY-MM-DD")
                reason_type = st.selectbox("Motivo (Laboral o Personal)", ["Personal", "Laboral", "Cita Médica", "Calamidad", "Vacaciones", "Licencia"])
                is_paid = st.radio("¿Permiso Remunerado?", ["No", "Sí"], horizontal=True)
            with c2:
                time_s = st.time_input("Hora de Salida (Opcional)", value=None)
                time_e = st.time_input("Hora de Entrada (Opcional)", value=None)
                total_time = st.text_input("Tiempo Total del Permiso", placeholder="Ej: 4 Horas, 1 Día, Completo")
                
            r_desc = st.text_area("Justificación / Detalles del permiso")
            makeup = st.text_input("¿Cómo se repone el tiempo? (Dejar en blanco si es remunerado/laboral)")
            
            submitted = st.form_submit_button("Firmar y Enviar a RRHH", type="primary")
            
        if submitted:
            if not leave_dates:
                st.error("Debes seleccionar obligatoriamente al menos una fecha de inicio.")
            elif type(leave_dates) not in (tuple, list) and not leave_dates:
                 st.error("Revisa la fecha seleccionada.")
            else:
                d_start = leave_dates[0] if type(leave_dates) in (tuple, list) else leave_dates
                d_end = leave_dates[1] if (type(leave_dates) in (tuple, list) and len(leave_dates) > 1) else d_start
                str_ts = time_s.strftime("%H:%M") if time_s else ""
                str_te = time_e.strftime("%H:%M") if time_e else ""
                
                db_create_leave_request(
                    user["username"], d_start, d_end, str_ts, str_te,
                    total_time, reason_type, r_desc, makeup, is_paid == "Sí"
                )
                st.success("✅ Solicitud enviada exitosamente. Conserva tu historial en la pestaña 'Mis Solicitudes'.")
                
    with t2:
        conn = db_conn()
        df_reqs = pd.read_sql_query("""
            SELECT id as Radicado, request_date as Fecha_Solicitud, leave_date_start as Desde, leave_date_end as Hasta, reason_type as Motivo, status as Estado 
            FROM leave_requests WHERE user_id = ? ORDER BY id DESC
        """, conn, params=(user["username"],))
        conn.close()
        
        if df_reqs.empty:
            st.info("No tienes solicitudes históricas radicas.")
        else:
            st.info("💡 Haz clic en una solicitud para ver todos sus detalles.")
            
            # Using session_state for st.dataframe selection to allow clearing it
            event = st.dataframe(df_reqs, use_container_width=True, hide_index=True, on_select="rerun", selection_mode="single-row", key="emp_reqs_table")
            
            if len(event.selection.rows) > 0:
                row_idx = event.selection.rows[0]
                selected_req_id = int(df_reqs.iloc[row_idx]["Radicado"])
                # Before showing dialog, clear selection so on close it doesn't trigger again
                st.session_state.emp_reqs_table.selection.rows.clear()
                show_leave_request_details(selected_req_id)