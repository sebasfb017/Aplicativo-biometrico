import streamlit as st
import pandas as pd
from datetime import datetime, date, timedelta

from database_conn.connection import db_conn
from database_conn.queries import db_create_leave_request
from services.notifications import generate_fth012_html
from services.email_service import send_novedad_alert

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
        
        c1, c2 = st.columns(2)
        with c1:
            leave_dates = st.date_input("Fecha(s) del Permiso", value=[], help="Selecciona uno o varios días de ausencia.", format="YYYY-MM-DD")
            reason_type = st.selectbox("Motivo (Laboral o Personal)", ["Personal", "Laboral", "Cita Médica", "Calamidad", "Vacaciones", "Licencia"])
            is_paid = st.radio("¿Permiso Remunerado?", ["No", "Sí"], horizontal=True)
        with c2:
            time_s = st.time_input("Hora de Salida (Opcional)", value=None)
            time_e = st.time_input("Hora de Entrada (Opcional)", value=None)
            
            # =========================================================================
            # BLOQUE DE CÁLCULO MATEMÁTICO EN TIEMPO REAL
            # =========================================================================
            # Al no encerrar todo el formulario en un `st.form` restrictivo, la
            # interfaz es reactiva. Apenas el empleado escoge fechas, Streamlit recarga
            # la pantalla súper rápido y deduce el tiempo automáticamente.
            
            calculated_time = ""
            if leave_dates:
                # Caso A: El empleado eligió más de un día. El sistema asume todo como días completos.
                if type(leave_dates) in (tuple, list) and len(leave_dates) > 1:
                    dias = (leave_dates[1] - leave_dates[0]).days + 1
                    calculated_time = f"{dias} Día(s)"
                # Caso B: El empleado eligió solo 1 fecha PERO introdujo horas específicas.
                elif time_s and time_e:
                    # Envolvemos las horas en objetos DateTime (matemáticos) usando el día de hoy
                    # simplemente como puente para que Python sea capaz de restar Horas.
                    ts_dt = datetime.combine(date.today(), time_s)
                    te_dt = datetime.combine(date.today(), time_e)
                    
                    if te_dt > ts_dt:
                        diff = te_dt - ts_dt
                        total_mins = diff.seconds // 60
                        h = total_mins // 60
                        m = total_mins % 60
                        
                        # Traductor de sistema a lenguaje humano ("1 Hora", "2 Horas y 15 Minutos")
                        parts = []
                        if h > 0:
                            parts.append(f"{h} Hora{'s' if h > 1 else ''}")
                        if m > 0:
                            parts.append(f"{m} Minuto{'s' if m > 1 else ''}")
                            
                        calculated_time = " y ".join(parts) if parts else "0 Minutos"
                    else:
                        calculated_time = "Error: Horas inválidas"
                else:
                    calculated_time = "1 Día"
            
            total_time = st.text_input("Tiempo Total del Permiso (Automático o Manual)", value=calculated_time, placeholder="Ej: 4 Horas, 1 Día")
            
        r_desc = st.text_area("Justificación / Detalles del permiso")
        makeup = st.text_input("¿Cómo se repone el tiempo? (Dejar en blanco si es remunerado/laboral)")
        
        submitted = st.button("Firmar y Enviar a RRHH", type="primary")
            
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
                
                req_id = db_create_leave_request(
                    user["username"], d_start, d_end, str_ts, str_te,
                    total_time, reason_type, r_desc, makeup, is_paid == "Sí"
                )
                
                # --- ALERTA DE CORREO INTELIGENTE ---
                try:
                    conn = db_conn()
                    # Verificar el estado en el que quedó
                    req_df = pd.read_sql_query("SELECT status FROM leave_requests WHERE id = ?", conn, params=(req_id,))
                    
                    if not req_df.empty:
                        target_status = req_df.iloc[0]['status']
                        target_emails = []
                        
                        if target_status == 'PENDING_COORD':
                            # Buscar departamento del empleado en la tabla employees
                            emp_df = pd.read_sql_query("SELECT department FROM employees WHERE user_id = ?", conn, params=(user["username"],))
                            dept = emp_df.iloc[0]['department'] if not emp_df.empty else ""
                            
                            coord_df = pd.read_sql_query("SELECT emp_email FROM users_app WHERE role = 'coordinador' AND managed_department = ? AND active = 1 AND emp_email IS NOT NULL AND emp_email != ''", conn, params=(dept,))
                            target_emails = coord_df['emp_email'].tolist()
                            
                        elif target_status == 'PENDING_JEFE':
                            # Buscar área del empleado en la tabla users_app
                            user_app_df = pd.read_sql_query("SELECT emp_area FROM users_app WHERE username = ?", conn, params=(user["username"],))
                            area = user_app_df.iloc[0]['emp_area'] if not user_app_df.empty else ""
                            
                            jefe_df = pd.read_sql_query("SELECT emp_email FROM users_app WHERE role = 'jefe_area' AND managed_area = ? AND active = 1 AND emp_email IS NOT NULL AND emp_email != ''", conn, params=(area,))
                            target_emails = jefe_df['emp_email'].tolist()
                            
                        elif target_status == 'PENDING_RRHH':
                            admin_df = pd.read_sql_query("SELECT emp_email FROM users_app WHERE role IN ('admin', 'nomina') AND active = 1 AND emp_email IS NOT NULL AND emp_email != ''", conn)
                            target_emails = admin_df['emp_email'].tolist()
                            
                        conn.close()
                        
                        if target_emails:
                            ok, msg = send_novedad_alert(target_emails, user["full_name"], reason_type, r_desc, total_time, d_start)
                            if not ok:
                                st.warning(f"La solicitud fue creada, pero hubo un fallo enviando la alerta: {msg}")
                        else:
                            st.info("Solicitud creada. (No se encontró un correo configurado para el aprobador en este nivel).")
                except Exception as e:
                    st.warning(f"Error interno al enviar correo: {e}")
                
                st.success("✅ Solicitud enviada exitosamente. Conserva tu historial en la pestaña 'Mis Solicitudes'.")
                
    with t2:
        conn = db_conn()
        df_reqs = pd.read_sql_query("""
            SELECT 
                id as Radicado, 
                request_date as Fecha_Solicitud, 
                leave_date_start, 
                leave_date_end, 
                total_time as Duración,
                reason_type as Motivo, 
                status as Estado 
            FROM leave_requests WHERE user_id = ? ORDER BY id DESC
        """, conn, params=(user["username"],))
        conn.close()
        
        if df_reqs.empty:
            st.info("No tienes solicitudes históricas radicas.")
        else:
            df_reqs["Fechas"] = df_reqs.apply(
                lambda r: r['leave_date_start'] if r['leave_date_start'] == r['leave_date_end'] 
                else f"{r['leave_date_start']} al {r['leave_date_end']}", 
                axis=1
            )
            
            # =========================================================================
            # ARQUITECTURA "MOBILE-FRIENDLY" DE TARJETAS (CARDS)
            # =========================================================================
            # En lugar de usar st.dataframe (lo cual genera un scroll horizontal muy
            # molesto en teléfonos pequeños), pintamos la información en cajas hermosas.
            # Al usar `st.columns` aquí dentro, Streamlit detectará cuando la pantalla
            # sea pequeña (Smartphone) y automáticamente apilará la Columna 2 debajo
            # de la Columna 1, eliminando eficientemente el scroll.
            
            st.info("💡 Desliza hacia abajo o haz clic en 'Ver Detalles' en la solicitud que desees auditar.")
            
            for _, r in df_reqs.iterrows():
                # Borde en la tarjeta para resaltar individualidad
                with st.container(border=True):
                    # Usamos columnas asimétricas: texto amplio a la izquierda, botón a la derecha
                    cols = st.columns([3, 1])
                    with cols[0]:
                        st.markdown(f"🗓️ **{r['Fechas']}** | Radicado: `#{r['Radicado']}`")
                        st.write(f"**Motivo:** {r['Motivo']} | **Duración:** {r['Duración']}")
                        st.caption(f"Enviado el {r['Fecha_Solicitud']} — Estado actual: **{r['Estado']}**")
                    with cols[1]:
                        if st.button("👁️ Ver Detalles", key=f"btn_detalles_{r['Radicado']}", use_container_width=True):
                            show_leave_request_details(r['Radicado'])