import streamlit as st
import pandas as pd
from datetime import datetime, date

# --- Componente Visual de Trazabilidad (Barra de Progreso) ---
def create_status_tracker(current_status, reason_type):
    """
    Crea una barra de progreso visual (Tracker) indicando en qué paso está el permiso.
    """
    solo_rrhh = reason_type == "Incapacidad"
    
    requiere_jefe = reason_type in [
        "Vacaciones", 
        "Calamidad Doméstica", 
        "Licencia de Luto", 
        "Licencia de Paternidad", 
        "Licencia por Votación", 
        "Licencia por Jurado de Votación", 
        "Licencia Remunerada", 
        "Licencia No Remunerada"
    ]

    if solo_rrhh:
        steps = ["Enviado", "RRHH (Final)"]
        status_order = {
            "PENDING_RRHH": 0,
            "APPROVED": 2,
            "REJECTED": -2,
            "CANCELLED": -3
        }
    else:
        steps = ["Enviado", "Coord.", "Jefe Área (Final)"] if requiere_jefe else ["Enviado", "Coord.", "RRHH (Final)"]
        status_order = {
            "PENDING_COORD": 0,
            "PENDING_RRHH": 1,
            "PENDING_JEFE": 2 if requiere_jefe else -1, # Ignorado si no requiere jefe
            "APPROVED": 4 if requiere_jefe else 3,
            "REJECTED": -2, # Estado terminal de rechazo
            "CANCELLED": -3 # Estado terminal de cancelación
        }

    # Mapeo de estados a un texto más legible para el usuario
    status_labels = {
        "PENDING_COORD": "Enviado (Coord.)",
        "PENDING_RRHH": "Validando (RRHH)",
        "PENDING_JEFE": "Firma Jefe (Jefe Área)",
        "APPROVED": "Aprobado Final",
        "REJECTED": "Rechazado",
        "CANCELLED": "Cancelado"
    }

    current_step = status_order.get(current_status, -1)

    # Si es un estado de rechazo, muestra una barra simple en rojo
    if current_step == -1:
        return f"""
        <div style="text-align: center; background-color: #ffcdd2; color: #c62828; padding: 10px; border-radius: 8px; font-weight: bold;">
            {status_labels.get(current_status, "Estado Desconocido")}
        </div>
        """

    # Construcción de la barra de progreso con HTML y CSS (Responsive)
    css = """
    <style>
    .tracker-container { display: flex; justify-content: space-between; align-items: center; font-size: 0.8rem; margin-top: 10px; }
    .tracker-step { padding: 5px 10px; border-radius: 15px; font-weight: bold; text-align: center; z-index: 2; }
    .tracker-line { flex-grow: 1; height: 2px; margin: 0 -5px; z-index: 1; }
    @media (max-width: 600px) {
        .tracker-container { flex-direction: column; align-items: flex-start; gap: 8px; }
        .tracker-line { display: none; }
        .tracker-step { width: 100%; text-align: left; padding: 8px 15px; }
    }
    </style>
    """
    html = css + '<div class="tracker-container">'
    if requiere_jefe:
        steps = ["Enviado", "Coord.", "RRHH", "Jefe Área"]
    else:
        steps = ["Enviado", "Coord.", "RRHH (Final)"]

    for i, step_name in enumerate(steps):
        # Determinar el estilo de cada paso (completado, actual, pendiente)
        if i < current_step:
            style = 'background-color: #C8E6C9; color: #2E7D32; border: 1px solid #2E7D32;'
            icon = "✅ "
        elif i == current_step:
            style = 'background-color: #BBDEFB; color: #0D47A1; border: 1px solid #0D47A1;'
            icon = "⏳ "
        else:
            style = 'background-color: #E0E0E0; color: #616161; border: 1px solid #BDBDBD;'
            icon = "⚪ "

        html += f'<div class="tracker-step" style="{style}">{icon}{step_name}</div>'

        # Añadir una línea de conexión entre los pasos (excepto en el último)
        if i < len(steps) - 1:
            line_color = '#2E7D32' if i < current_step else '#BDBDBD'
            html += f'<div class="tracker-line" style="background-color: {line_color};"></div>'

    html += '</div>'
    return html

# --- Fin del Componente Visual ---


from database_conn.connection import db_conn
from database_conn.queries import db_create_leave_request, db_cancel_leave_request
from services.notifications import generate_fth012_html
from services.email_service import send_novedad_alert

@st.dialog("Detalles de Mi Solicitud (F-TH-012)")
def show_leave_request_details(req_id: int):
    conn = db_conn()
    df_req = pd.read_sql_query("SELECT * FROM leave_requests WHERE id = ?", conn, params=(req_id,))
    
    df_audit = pd.read_sql_query("""
        SELECT a.user_id, a.action, a.timestamp, u.full_name
        FROM audit_logs a
        LEFT JOIN users_app u ON a.user_id = u.username
        WHERE a.details LIKE ? AND a.action LIKE 'APPROVE_%'
        ORDER BY a.timestamp ASC
    """, conn, params=(f"%Permiso #{req_id} %",))
    
    conn.close()
    
    if df_req.empty:
        st.error("No se encontró la solicitud.")
        return
        
    req = df_req.iloc[0]
    
    st.markdown(f"### Radicado: #{req['id']}")
    
    # Usamos el nuevo componente visual
    st.markdown(create_status_tracker(req['status'], req['reason_type']), unsafe_allow_html=True)
    st.markdown("<br>", unsafe_allow_html=True)

    if req['status'] == 'APPROVED':
        html_fth012 = generate_fth012_html(req, df_audit)
        st.download_button(
            label="📄 Descargar F-TH-012 (HTML/PDF)",
            data=html_fth012,
            file_name=f"F-TH-012_{req['id']}.html",
            mime="text/html",
            type="primary",
            use_container_width=True
        )
    elif req['status'] == 'REJECTED':
        st.error(f"❌ Solicitud Rechazada. Motivo: {req['rejection_reason']}")
    elif req['status'] == 'CANCELLED':
        st.warning(f"🚫 Solicitud Cancelada por el empleado. Motivo: {req['cancellation_reason']}")


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
        
    # --- GESTIÓN DOCUMENTAL: Soporte Médico o Legal Adjunto ---
    if 'attachment_path' in req and pd.notna(req['attachment_path']) and req['attachment_path']:
        import os
        from database_conn.connection import DATA_DIR
        file_path = os.path.join(DATA_DIR, "uploads", req['attachment_path'])
        
        st.divider()
        st.markdown("**Soporte Adjunto (Incapacidad/Certificado):**")
        
        if os.path.exists(file_path):
            with st.expander("👁️ Previsualizar Soporte Adjunto", expanded=False):
                ext = os.path.splitext(req['attachment_path'])[1].lower()
                if ext in [".png", ".jpg", ".jpeg", ".webp"]:
                    st.image(file_path, use_container_width=True)
                elif ext == ".pdf":
                    try:
                        import base64
                        with open(file_path, "rb") as f:
                            base64_pdf = base64.b64encode(f.read()).decode('utf-8')
                        pdf_display = f'<iframe src="data:application/pdf;base64,{base64_pdf}" width="100%" height="500" type="application/pdf"></iframe>'
                        st.markdown(pdf_display, unsafe_allow_html=True)
                    except Exception as e:
                        st.error(f"No se pudo cargar el PDF: {e}")
                else:
                    st.info("Vista previa no disponible para este tipo de archivo.")
                    
            with open(file_path, "rb") as f:
                file_bytes = f.read()
            st.download_button(
                label="📎 Descargar Evidencia Adjunta",
                data=file_bytes,
                file_name=req['attachment_path'],
                use_container_width=True
            )
        else:
            st.error("El archivo adjunto no se encuentra en el servidor. Puede haber sido eliminado.")
    # -----------------------------------------------------------
    if not df_audit.empty:
        st.divider()
        st.markdown("**Trazabilidad de Aprobaciones:**")
        for _, row_a in df_audit.iterrows():
            level = "Jefatura" if row_a['action'] == "APPROVE_LEAVE_L1" else "Gestión Humana"
            approver_name = row_a['full_name'] if pd.notna(row_a['full_name']) else row_a['user_id']
            st.caption(f"✓ **{level}**: {approver_name} ({row_a['timestamp']})")

@st.dialog("Cancelar Solicitud de Permiso")
def cancel_leave_request_dialog(req_id: int, user_id: str, full_name: str, reason_type: str):
    st.write(f"Cancelando solicitud #{req_id} de {full_name} ({reason_type}).")
    reason = st.text_area("Por favor, ingresa el motivo de la cancelación:", key=f"cancellation_reason_{req_id}")

    col1, col2 = st.columns(2)
    with col1:
        if st.button("Confirmar Cancelación", type="primary", use_container_width=True):
            if reason:
                success = db_cancel_leave_request(req_id, user_id, reason)
                if success:
                    st.success("Solicitud cancelada exitosamente.")
                    st.rerun()
                else:
                    st.error("No se pudo cancelar la solicitud. Es posible que ya haya sido procesada.")
            else:
                st.error("El motivo de cancelación no puede estar vacío.")
    with col2:
        if st.button("Volver", use_container_width=True):
            st.session_state[f"show_cancellation_dialog_{req_id}"] = False
            st.rerun()

def page_employee_portal():
    user = st.session_state["user"]
    
    # CSS Global para Optimización Móvil
    mobile_css = """
    <style>
    @media (max-width: 600px) {
        /* Reducir padding lateral en móviles */
        .block-container {
            padding-left: 1rem !important;
            padding-right: 1rem !important;
            padding-top: 1.5rem !important;
        }
        /* Botones e inputs touch-friendly */
        .stButton>button, .stDownloadButton>button, .stFormSubmitButton>button {
            min-height: 44px !important;
            border-radius: 8px !important;
        }
        input, select, textarea {
            font-size: 16px !important; /* Evita zoom automático en iOS */
        }
        /* Asegurar que las tabs no se corten */
        .stTabs [data-baseweb="tab-list"] {
            overflow-x: auto;
            white-space: nowrap;
        }
    }
    </style>
    """
    st.markdown(mobile_css, unsafe_allow_html=True)

    st.title("🧑‍⚕️ Mi Portal de Autogestión (F-TH-012)")
    
    area = user.get('emp_area') or 'Sin Área Definida'
    subarea = user.get('emp_subarea')
    area_display = f"{area} - {subarea}" if area != 'Sin Área Definida' and subarea else area
    st.write(f"Bienvenido/a **{user['full_name']}** | {area_display}")
    
    t1, t2 = st.tabs(["📝 Radicar Nuevo Permiso", "🗂️ Mis Solicitudes"])
    
    with t1:
        st.subheader("Solicitud de Permisos Laborales Y/O Personales")
        st.info("Llena el siguiente formulario digital equivalente al formato F-TH-012 físico.")
        
        with st.container(border=True):
            c1, c2 = st.columns(2)
            with c1:
                leave_dates = st.date_input("Fecha(s) del Permiso", value=[], help="Selecciona uno o varios días de ausencia.", format="YYYY-MM-DD")
                
                categoria = st.selectbox("Categoría de Novedad", ["Citas", "Permisos", "Licencias", "Vacaciones", "Incapacidad"])
                
                if categoria == "Citas":
                    reason_type = st.selectbox("Detalle", ["Cita Médica", "Cita Médica con desplazamiento a otra ciudad"])
                elif categoria == "Permisos":
                    reason_type = st.selectbox("Detalle", ["Permiso Personal", "Permiso Laboral"])
                elif categoria == "Licencias":
                    reason_type = st.selectbox("Detalle", ["Calamidad Doméstica", "Licencia de Luto", "Licencia de Paternidad", "Licencia por Votación", "Licencia por Jurado de Votación", "Licencia Remunerada", "Licencia No Remunerada"])
                elif categoria == "Incapacidad":
                    reason_type = "Incapacidad"
                    st.text_input("Detalle", value="Incapacidad", disabled=True)
                else: # Vacaciones
                    reason_type = "Vacaciones"
                    st.text_input("Detalle", value="Vacaciones", disabled=True)
                
                is_paid = st.radio("¿Permiso Remunerado?", ["No", "Sí"], horizontal=True)
            with c2:
                time_s = st.time_input("Hora de Salida (Opcional)", value=None)
                time_e = st.time_input("Hora de Entrada (Opcional)", value=None)

                calculated_time = ""
                if leave_dates:
                    if isinstance(leave_dates, (tuple, list)) and len(leave_dates) > 1:
                        dias = (leave_dates[1] - leave_dates[0]).days + 1
                        calculated_time = f"{dias} Día(s)"
                    elif time_s and time_e:
                        ts_dt = datetime.combine(date.today(), time_s)
                        te_dt = datetime.combine(date.today(), time_e)
                        if te_dt > ts_dt:
                            diff = te_dt - ts_dt
                            total_mins = diff.seconds // 60
                            h, m = divmod(total_mins, 60)
                            parts = []
                            if h > 0: parts.append(f"{h} Hora{'s' if h > 1 else ''}")
                            if m > 0: parts.append(f"{m} Minuto{'s' if m > 1 else ''}")
                            calculated_time = " y ".join(parts) if parts else "0 Minutos"
                        else:
                            calculated_time = "Error: Horas inválidas"
                    else:
                        calculated_time = "1 Día"

                st.text_input("Tiempo Total Calculado (Automático)", value=calculated_time, disabled=True)
                total_time = calculated_time

            r_desc = st.text_area("Justificación / Detalles del permiso")
            makeup = st.text_input("¿Cómo se repone el tiempo? (Dejar en blanco si es remunerado/laboral)")
            
            st.markdown("---")
            st.write("📄 **Documento de Soporte (Opcional)**")
            uploaded_file = st.file_uploader("Adjunta tu incapacidad, certificado médico o soporte legal. Tamaño máximo: 20MB", type=["pdf", "png", "jpg", "jpeg"])
            
            submitted = st.button("Firmar y Enviar a RRHH", type="primary", use_container_width=True)
            
        if submitted:
            # --- Validación de Tamaño del Archivo ---
            MAX_FILE_SIZE_MB = 20
            file_is_valid = True

            if uploaded_file is not None:
                # Obtenemos el tamaño del archivo subido en bytes y lo convertimos a MB
                file_size_mb = uploaded_file.size / (1024 * 1024)
                if file_size_mb > MAX_FILE_SIZE_MB:
                    st.error(f"❌ El archivo '{uploaded_file.name}' es demasiado grande ({file_size_mb:.1f} MB). El tamaño máximo permitido es {MAX_FILE_SIZE_MB} MB. Por favor, comprime el archivo antes de subirlo.")
                    file_is_valid = False
            # ---------------------------------------

            if not file_is_valid:
                pass # Se detiene el proceso si el archivo es muy grande, mostrando el error
            elif not leave_dates:
                st.error("Debes seleccionar obligatoriamente al menos una fecha de inicio.")
            elif "Error" in calculated_time:
                st.error("Las horas ingresadas son inválidas. La hora de entrada debe ser mayor a la hora de salida.")
            else:
                d_start = leave_dates[0] if isinstance(leave_dates, (list, tuple)) else leave_dates
                d_end = leave_dates[1] if isinstance(leave_dates, (list, tuple)) and len(leave_dates) > 1 else d_start
                str_ts = time_s.strftime("%H:%M") if time_s else ""
                str_te = time_e.strftime("%H:%M") if time_e else ""
                
                attachment_path = None
                if uploaded_file is not None:
                    import os
                    import time
                    from database_conn.connection import DATA_DIR
                    
                    uploads_dir = os.path.join(DATA_DIR, "uploads")
                    os.makedirs(uploads_dir, exist_ok=True)
                    
                    file_extension = os.path.splitext(uploaded_file.name)[1]
                    safe_filename = f"{user['username']}_{int(time.time())}{file_extension}"
                    full_path = os.path.join(uploads_dir, safe_filename)
                    
                    with open(full_path, "wb") as f:
                        f.write(uploaded_file.getbuffer())
                    attachment_path = safe_filename
                
                req_id = db_create_leave_request(
                    user["username"], d_start, d_end, str_ts, str_te,
                    total_time, reason_type, r_desc, makeup, is_paid == "Sí", attachment_path
                )
                
                try:
                    with db_conn() as conn:
                        req_df = pd.read_sql_query("SELECT status FROM leave_requests WHERE id = ?", conn, params=(req_id,))
                        if not req_df.empty:
                            target_status = req_df.iloc[0]['status']
                            target_emails = []
                            
                            if target_status == 'PENDING_COORD':
                                user_app_df = pd.read_sql_query("SELECT emp_subarea FROM users_app WHERE username = ?", conn, params=(user["username"],))
                                subarea = user_app_df.iloc[0]['emp_subarea'] if not user_app_df.empty else ""
                                target_coord_dept = subarea
                                if subarea == 'Servicios Generales': target_coord_dept = 'Calidad'
                                elif subarea == 'Orientador': target_coord_dept = 'Seguridad'
                                coord_all = pd.read_sql_query("SELECT emp_email, managed_department FROM users_app WHERE role = 'coordinador' AND active = 1 AND emp_email IS NOT NULL AND emp_email != ''", conn)
                                target_emails = []
                                for _, c_row in coord_all.iterrows():
                                    c_depts = [d.strip() for d in c_row['managed_department'].split(',') if d.strip()]
                                    if target_coord_dept in c_depts:
                                        target_emails.append(c_row['emp_email'])
                            elif target_status == 'PENDING_JEFE':
                                user_app_df = pd.read_sql_query("SELECT emp_area, emp_subarea FROM users_app WHERE username = ?", conn, params=(user["username"],))
                                area = user_app_df.iloc[0]['emp_area'] if not user_app_df.empty else ""
                                subarea = user_app_df.iloc[0]['emp_subarea'] if not user_app_df.empty else ""
                                target_jefe_area = area
                                if subarea == 'Admisiones': target_jefe_area = 'Administrativo'
                                elif subarea == 'Auditor Médico': target_jefe_area = 'Auditoria Médica'
                                elif subarea == 'Control Interno': target_jefe_area = 'Control Interno'
                                jefe_df = pd.read_sql_query("SELECT emp_email FROM users_app WHERE role = 'jefe_area' AND managed_area = ? AND active = 1 AND emp_email IS NOT NULL AND emp_email != ''", conn, params=(target_jefe_area,))
                                target_emails = jefe_df['emp_email'].tolist()
                            elif target_status == 'PENDING_RRHH':
                                admin_df = pd.read_sql_query("SELECT emp_email FROM users_app WHERE role IN ('admin', 'nomina') AND active = 1 AND emp_email IS NOT NULL AND emp_email != ''", conn)
                                target_emails = admin_df['emp_email'].tolist()
                            
                            if target_emails:
                                ok, msg = send_novedad_alert(target_emails, user["full_name"], reason_type, r_desc, total_time, d_start)
                                if not ok: st.warning(f"Solicitud creada, pero fallo la alerta: {msg}")
                            else:
                                st.info("Solicitud creada. (No se encontró correo para el aprobador).")
                except Exception as e:
                    st.warning(f"Error interno al enviar correo: {e}")
                
                st.success("✅ Solicitud enviada exitosamente.")
                
    with t2:
        with db_conn() as conn:
            df_reqs = pd.read_sql_query("""
                SELECT id as Radicado, request_date as Fecha_Solicitud, leave_date_start, 
                       leave_date_end, total_time as Duración, reason_type as Motivo, status as Estado,
                       full_name
                FROM leave_requests lr
                JOIN users_app ua ON lr.user_id = ua.username
                WHERE lr.user_id = ? AND (lr.hidden_by_employee IS NULL OR lr.hidden_by_employee = 0)
                ORDER BY id DESC
            """, conn, params=(user["username"],))
        
        if df_reqs.empty:
            st.info("No tienes solicitudes históricas radicas.")
        else:
            df_reqs["Fechas"] = df_reqs.apply(
                lambda r: r['leave_date_start'] if r['leave_date_start'] == r['leave_date_end'] 
                else f"{r['leave_date_start']} al {r['leave_date_end']}", 
                axis=1
            )

            st.info("💡 Desliza hacia abajo o haz clic en 'Ver Detalles' en la solicitud que desees auditar.")
            
            # Filtro de solicitudes
            filter_status = st.radio(
                "Mostrar solicitudes:",
                ("Todas", "Pendientes", "Aprobadas", "Rechazadas/Canceladas"),
                horizontal=True,
                key="employee_portal_filter"
            )

            filtered_df_reqs = df_reqs.copy()

            if filter_status == "Pendientes":
                filtered_df_reqs = filtered_df_reqs[filtered_df_reqs['Estado'].isin(['PENDING_COORD', 'PENDING_JEFE', 'PENDING_RRHH'])]
            elif filter_status == "Aprobadas":
                filtered_df_reqs = filtered_df_reqs[filtered_df_reqs['Estado'] == 'APPROVED']
            elif filter_status == "Rechazadas/Canceladas":
                filtered_df_reqs = filtered_df_reqs[filtered_df_reqs['Estado'].isin(['REJECTED', 'CANCELLED'])]

            if filtered_df_reqs.empty:
                st.info("No tienes solicitudes en esta categoría.")
            else:
                for _, r in filtered_df_reqs.iterrows(): # Usamos el DataFrame filtrado
                    with st.container(border=True):
                        cols = st.columns([3, 2])
                        with cols[0]:
                            status_badge = ""
                            if r['Estado'] == 'REJECTED':
                                status_badge = "🔴 RECHAZADA"
                            elif r['Estado'] == 'CANCELLED':
                                status_badge = "⚪ CANCELADA"

                            st.markdown(f"🗓️ **{r['Fechas']}** | Radicado: `#{r['Radicado']}` {status_badge}")
                            st.write(f"**Motivo:** {r['Motivo']} | **Duración:** {r['Duración']}")
                            
                            # Reemplazamos el texto simple por el tracker visual
                            st.markdown(create_status_tracker(r['Estado'], r['Motivo']), unsafe_allow_html=True)
                        
                        with cols[1]:
                            if st.button("👁️ Ver Detalles", key=f"btn_detalles_{r['Radicado']}", use_container_width=True):
                                show_leave_request_details(r['Radicado'])

                            # Botón de Cancelar solo si el estado es PENDIENTE
                            if r['Estado'] in ['PENDING_COORD', 'PENDING_JEFE', 'PENDING_RRHH']:
                                if st.button("❌ Cancelar Solicitud", key=f"btn_cancel_{r['Radicado']}", use_container_width=True):
                                    st.session_state[f"show_cancellation_dialog_{r['Radicado']}"] = True
                                    st.rerun()

                                if st.session_state.get(f"show_cancellation_dialog_{r['Radicado']}", False):
                                    cancel_leave_request_dialog(r['Radicado'], user['username'], r['full_name'], r['Motivo'])
                            
                            # Botón de Ocultar/Eliminar solo si el estado no es PENDIENTE (incluyendo terminales y desconocidos)
                            elif r['Estado'] not in ['PENDING_COORD', 'PENDING_JEFE', 'PENDING_RRHH']:
                                if st.button("🗑️ Eliminar del Historial", key=f"btn_hide_{r['Radicado']}", use_container_width=True):
                                    from database_conn.queries import db_hide_leave_request
                                    if db_hide_leave_request(r['Radicado'], user['username']):
                                        st.success("Permiso eliminado del historial.")
                                        st.rerun()
                                    else:
                                        st.error("Error al intentar ocultar el registro.")