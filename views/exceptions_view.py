import streamlit as st
import pandas as pd
from datetime import date, timedelta, datetime

from database_conn.connection import db_session, db_conn
from database_conn.queries import (upsert_exception, get_exceptions_df, 
                                   db_approve_leave_request_coord, db_approve_leave_request_jefe, 
                                   db_reject_leave_request)
from services.notifications import log_audit, notify_employee_status
from utils.auth import require_role
from views.employee_portal_view import show_leave_request_details

# --- Componente de Detección de Conflictos para Jefes ---
def check_schedule_conflicts(request: pd.Series, approver_role: str, managed_entity: str):
    """
    Verifica si una solicitud de permiso se solapa con otras ausencias ya aprobadas
    dentro del mismo equipo o área.
    """
    start_date = request['leave_date_start']
    end_date = request['leave_date_end']
    
    # La consulta busca otras solicitudes aprobadas que se crucen en el rango de fechas
    # y que pertenezcan al mismo grupo de gestión (departamento o área).
    query = """
        SELECT lr.id, e.full_name
        FROM leave_requests lr
        JOIN employees e ON lr.user_id = e.user_id
        WHERE lr.status = 'APPROVED'
          AND lr.id != ?
          AND (
              (lr.leave_date_start <= ? AND lr.leave_date_end >= ?) OR
              (lr.leave_date_start <= ? AND lr.leave_date_end >= ?) OR
              (lr.leave_date_start >= ? AND lr.leave_date_end <= ?)
          )
    """
    
    params = [request['id'], start_date, start_date, end_date, end_date, start_date, end_date]
    
    # Ajustar el filtro de la consulta según el rol del aprobador
    if approver_role == 'coordinador':
        depts = [d.strip() for d in managed_entity.split(',') if d.strip()]
        if not depts:
            depts = ['']
        like_conds = " OR ".join(["e.department LIKE ?"] * len(depts))
        query += f" AND ({like_conds})"
        for d in depts:
            params.append(f"% - {d}")
    elif approver_role == 'jefe_area':
        query += " AND e.department LIKE ?"
        params.append(f"{managed_entity} - %")
        
    with db_conn() as conn:
        conflicts_df = pd.read_sql_query(query, conn, params=params)
        
    if not conflicts_df.empty:
        names = ", ".join(conflicts_df['full_name'].tolist())
        count = len(conflicts_df)
        st.warning(f"⚠️ **Alerta de Cruce:** Ya hay {count} persona(s) de esta área con permiso en estas fechas: **{names}**.")

# --- Fin del Componente ---

@st.dialog("Detalles Completos de la Novedad/Permiso")
def show_exception_details(exc_id: int):
    with db_session() as conn:
        df_exc = pd.read_sql_query("""
            SELECT ex.user_id, e.full_name, ex.date, ex.type, ex.notes, ex.created_at
            FROM exceptions ex
            LEFT JOIN employees e ON ex.user_id = e.user_id
            WHERE ex.id = ?
        """, conn, params=(exc_id,))
        
        if df_exc.empty:
            st.error("No se encontró la novedad.")
            return
            
        exc = df_exc.iloc[0]
        st.markdown(f"#### **Empleado:** {exc['full_name']} (ID: {exc['user_id']})")
        st.markdown(f"**Fecha Afectada:** {exc['date']} | **Tipo:** {exc['type']}")
        st.write(f"**Observación General:** {exc['notes']}")
        st.caption(f"Registrado el: {exc['created_at']}")
        
        st.divider()
        
        # Buscar si existe una solicitud digital de portal asociada
        df_req = pd.read_sql_query("""
            SELECT *
            FROM leave_requests
            WHERE user_id = ? AND status = 'APPROVED'
              AND leave_date_start <= ? AND leave_date_end >= ?
            ORDER BY id DESC LIMIT 1
        """, conn, params=(exc['user_id'], exc['date'], exc['date']))
        
    if not df_req.empty:
        req = df_req.iloc[0]
        st.markdown("### 📄 Detalles de la Solicitud (Portal F-TH-012)")
        
        c1, c2 = st.columns(2)
        with c1:
            st.markdown(f"**Radicado:** #{req['id']}")
            st.markdown(f"**Fecha de Solicitud:** {req['request_date']}")
            st.markdown(f"**Remunerado:** {'✅ Sí' if req['is_paid'] else '❌ No'}")
        with c2:
            h_in = req['start_time'] if req['start_time'] else "N/A"
            h_out = req['end_time'] if req['end_time'] else "N/A"
            st.markdown(f"**Hora Salida:** {h_in}")
            st.markdown(f"**Hora Entrada:** {h_out}")
            st.markdown(f"**Tiempo Total:** {req['total_time']}")
            
        st.write(f"**Motivo Original:** {req['reason_type']}")
        
        st.markdown("**Justificación del Empleado:**")
        st.info(req['reason_description'] if req['reason_description'] else "Sin detalles adicionales.")
        
        if not req['is_paid'] and req['how_to_makeup']:
            st.markdown("**Acuerdo de Reposición (Tiempo):**")
            st.warning(req['how_to_makeup'])
            
        with db_session() as conn:
            df_audit = pd.read_sql_query("""
                SELECT a.user_id, a.action, a.timestamp, u.full_name
                FROM audit_logs a
                LEFT JOIN users_app u ON a.user_id = u.username
                WHERE a.details LIKE ? AND a.action LIKE 'APPROVE_%'
                ORDER BY a.timestamp ASC
            """, conn, params=(f"%Permiso #{req['id']} %",))
        
        if not df_audit.empty:
            st.divider()
            st.markdown("**Trazabilidad de Aprobaciones:**")
            for _, row_a in df_audit.iterrows():
                level = "Jefatura" if row_a['action'] == "APPROVE_LEAVE_L1" else "Gestión Humana"
                approver_name = row_a['full_name'] if pd.notna(row_a['full_name']) else row_a['user_id']
                st.caption(f"✓ **{level}**: {approver_name} ({row_a['timestamp']})")
    else:
        st.info("ℹ️ Esta novedad no parece tener una solicitud digital asociada del portal de empleados (o fue ingresada manualmente).")

@st.dialog("Motivo de Rechazo")
def rejection_reason_dialog(req_id, user_id, full_name, reason_type):
    st.write(f"Rechazando solicitud #{req_id} de {full_name} ({reason_type}).")
    reason = st.text_area("Por favor, ingresa el motivo del rechazo:", key=f"rejection_reason_{req_id}")
    
    col1, col2 = st.columns(2)
    with col1:
        if st.button("Confirmar Rechazo", type="primary", use_container_width=True):
            if reason:
                db_reject_leave_request(req_id, user_id, reason)
                log_audit("REJECT_LEAVE_L1", f"Permiso #{req_id} ({reason_type}) de {full_name} rechazado por {user_id}. Motivo: {reason}")
                notify_employee_status(user_id, full_name, req_id, reason_type, "RECHAZADA", f"Tu permiso fue rechazado por la jefatura/RRHH. Motivo: {reason}", st.session_state["user"]["full_name"])
                st.success("Solicitud rechazada y empleado notificado.")
                st.rerun()
            else:
                st.error("El motivo de rechazo no puede estar vacío.")
    with col2:
        if st.button("Cancelar", use_container_width=True):
            st.session_state[f"show_rejection_dialog_{req_id}"] = False
            st.rerun()


def page_exceptions():
    require_role("admin", "nomina", "jefe_area", "coordinador")
    st.title("🛡️ Novedades y Justificaciones")
    user = st.session_state["user"]

    if user["role"] in ["coordinador", "jefe_area"]:
        st.write(f"Panel de Gestión para: **{user.get('managed_department') or user.get('managed_area')}**")
        
        tab1, tab2 = st.tabs(["📥 Bandeja de Aprobación", "🕰️ Historial de Decisiones"])
        
        with tab1:
            if user["role"] == "coordinador":
                managed_depts = [d.strip() for d in user.get('managed_department', '').split(',') if d.strip()]
                if not managed_depts:
                    managed_depts = ['']
                    
                placeholders = ','.join(['?'] * len(managed_depts))
                cond_serv_gen = "OR (ua.emp_subarea = 'Servicios Generales')" if 'Calidad' in managed_depts else ""
                cond_orientador = "OR (ua.emp_subarea = 'Orientador')" if 'Seguridad' in managed_depts else ""
                
                query = f"""
                    SELECT lr.id, lr.user_id, e.full_name, lr.request_date, lr.leave_date_start, lr.leave_date_end,
                           lr.reason_type, lr.reason_description, lr.is_paid, lr.status, lr.attachment_path
                    FROM leave_requests lr
                    JOIN employees e ON lr.user_id = e.user_id
                    JOIN users_app ua ON lr.user_id = ua.username
                    WHERE lr.status = 'PENDING_COORD' AND 
                          (
                              ua.emp_subarea IN ({placeholders})
                              {cond_serv_gen}
                              {cond_orientador}
                          )
                    ORDER BY lr.id ASC
                """
                params = tuple(managed_depts)
            else:
                query = """
                    SELECT lr.id, lr.user_id, e.full_name, lr.request_date, lr.leave_date_start, lr.leave_date_end,
                           lr.reason_type, lr.reason_description, lr.is_paid, lr.status, lr.attachment_path,
                           (SELECT full_name FROM users_app WHERE username = lr.approved_by_coord) as coord_name,
                           (SELECT full_name FROM users_app WHERE username = lr.approved_by_rrhh) as rrhh_name
                    FROM leave_requests lr
                    JOIN employees e ON lr.user_id = e.user_id
                    JOIN users_app ua ON lr.user_id = ua.username
                    WHERE lr.status = 'PENDING_JEFE' AND 
                          (
                              ua.emp_area = ? OR 
                              (ua.emp_subarea = 'Admisiones' AND ? = 'Administrativo') OR
                              (ua.emp_subarea = 'Auditor Médico' AND ? = 'Auditoria Médica') OR
                              (ua.emp_subarea = 'Control Interno' AND ? = 'Control Interno')
                          )
                    ORDER BY lr.id ASC
                """
                params = (user.get('managed_area', ''), user.get('managed_area', ''), user.get('managed_area', ''), user.get('managed_area', ''))
                
            with db_session() as conn:
                df_pend = pd.read_sql_query(query, conn, params=params)
            
            if df_pend.empty:
                st.success("No hay solicitudes pendientes de revisión para tu área.")
            else:
                st.write(f"Tienes **{len(df_pend)}** solicitud(es) por revisar.")
                for _, r in df_pend.iterrows():
                    with st.container(border=True):
                        cols = st.columns([3, 1])
                        with cols[0]:
                            st.markdown(f"**{r['full_name']}** (ID: {r['user_id']}) - *{r['reason_type']}*")
                            st.write(f"**Fechas:** {r['leave_date_start']} al {r['leave_date_end']} | **Remunerado:** {'Sí' if r['is_paid'] else 'No'}")
                            if 'coord_name' in r and pd.notna(r['coord_name']):
                                st.info(f"✅ **Visto Bueno Previo:** Coordinador {r['coord_name']}")
                            if 'rrhh_name' in r and pd.notna(r['rrhh_name']):
                                st.info(f"✅ **Revisado por RRHH:** {r['rrhh_name']}")
                            st.write(f"**Justificación:** {r['reason_description']}")
                            
                            # --- Llamada al detector de conflictos ---
                            managed_entity = user.get('managed_department') if user["role"] == "coordinador" else user.get('managed_area')
                            check_schedule_conflicts(r, user["role"], managed_entity)
                            
                            if r['attachment_path']:
                                import os
                                from database_conn.connection import DATA_DIR
                                file_path = os.path.join(DATA_DIR, "uploads", r['attachment_path'])
                                if os.path.exists(file_path):
                                    with st.expander("👁️ Previsualizar Soporte Adjunto", expanded=False):
                                        ext = os.path.splitext(r['attachment_path'])[1].lower()
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
                                        st.download_button("📎 Descargar Soporte Adjunto", data=f.read(), file_name=r['attachment_path'], key=f"dl_coord_{r['id']}", use_container_width=True)
                        with cols[1]:
                            if st.button("👍 Aprobar", key=f"btn_acc_{r['id']}", type="primary", use_container_width=True):
                                if user["role"] == "coordinador":
                                    db_approve_leave_request_coord(r['id'], user['username'])
                                    next_status = 'PENDING_RRHH'
                                    
                                    # Notificar a RRHH
                                    with db_session() as conn:
                                        admin_df = pd.read_sql_query("SELECT emp_email FROM users_app WHERE role IN ('admin', 'nomina') AND active = 1 AND emp_email IS NOT NULL AND emp_email != ''", conn)
                                        if not admin_df.empty:
                                            target_emails = admin_df['emp_email'].tolist()
                                            from services.email_service import send_novedad_alert
                                            send_novedad_alert(target_emails, r['full_name'], r['reason_type'], r['reason_description'], "N/A", r['leave_date_start'], user['full_name'])
                                    
                                    log_audit("APPROVE_LEAVE_L1", f"Permiso #{r['id']} ({r['reason_type']}) de {r['full_name']} aprobado por {user['role']}. Pasa a {next_status}")
                                    notify_employee_status(r['user_id'], r['full_name'], r['id'], r['reason_type'], "PRE-APROBADA", f"Tu solicitud avanzó en el flujo de firmas hacia el siguiente aprobador ({next_status}).", user['full_name'])
                                    st.rerun()
                                    
                                else:
                                    db_approve_leave_request_jefe(r['id'], user['username'])
                                    next_status = 'APPROVED'
                                    
                                    with db_session() as conn:
                                        cur = conn.cursor()
                                        # Inyectar en excepciones (eximir faltas)
                                        d_start = date.fromisoformat(r['leave_date_start'])
                                        d_end = date.fromisoformat(r['leave_date_end'])
                                        delta = d_end - d_start
                                        for i in range(delta.days + 1):
                                            day_to_log = (d_start + timedelta(days=i)).isoformat()
                                            cur.execute("""
                                                INSERT INTO exceptions(user_id, date, type, notes, created_at)
                                                VALUES(?,?,?,?,?)
                                                ON CONFLICT(user_id, date) DO UPDATE SET type=excluded.type, notes=excluded.notes
                                            """, (r['user_id'], day_to_log, r['reason_type'], f"Aprobado de Portal: {r['reason_description']}", datetime.now().isoformat(timespec="seconds")))
                                    
                                    log_audit("APPROVE_LEAVE_FINAL", f"Permiso #{r['id']} ({r['reason_type']}) de {r['full_name']} APROBADO FINAL por Jefe de Área.")
                                    notify_employee_status(r['user_id'], r['full_name'], r['id'], r['reason_type'], "APROBACIÓN FINAL", "Tu solicitud fue completamente aprobada por tu Jefatura y registrada oficialmente en el sistema.", user['full_name'])
                                    st.rerun()
                                
                            if st.button("❌ Rechazar", key=f"btn_rej_{r['id']}", use_container_width=True):
                                st.session_state[f"show_rejection_dialog_{r['id']}"] = True
                                st.rerun()
                            
                            if st.session_state.get(f"show_rejection_dialog_{r['id']}", False):
                                rejection_reason_dialog(r['id'], r['user_id'], r['full_name'], r['reason_type'])
                                
        with tab2:
            if user["role"] == "coordinador":
                managed_depts = [d.strip() for d in user.get('managed_department', '').split(',') if d.strip()]
                if not managed_depts:
                    managed_depts = ['']
                    
                placeholders = ','.join(['?'] * len(managed_depts))
                cond_serv_gen = "OR (ua.emp_subarea = 'Servicios Generales')" if 'Calidad' in managed_depts else ""
                cond_orientador = "OR (ua.emp_subarea = 'Orientador')" if 'Seguridad' in managed_depts else ""
                
                query_hist = f"""
                    SELECT lr.id, lr.user_id, e.full_name, lr.request_date, lr.leave_date_start, lr.leave_date_end,
                           lr.reason_type, lr.status
                    FROM leave_requests lr
                    JOIN employees e ON lr.user_id = e.user_id
                    JOIN users_app ua ON lr.user_id = ua.username
                    WHERE lr.approved_by_coord = ? 
                       OR (lr.status = 'REJECTED' AND (
                              ua.emp_subarea IN ({placeholders})
                              {cond_serv_gen}
                              {cond_orientador}
                          ))
                    ORDER BY lr.id DESC
                """
                params_hist = [user['username']] + managed_depts
            else:
                query_hist = """
                    SELECT lr.id, lr.user_id, e.full_name, lr.request_date, lr.leave_date_start, lr.leave_date_end,
                           lr.reason_type, lr.status
                    FROM leave_requests lr
                    JOIN employees e ON lr.user_id = e.user_id
                    JOIN users_app ua ON lr.user_id = ua.username
                    WHERE lr.approved_by_jefe = ?
                       OR (lr.status = 'REJECTED' AND (
                              ua.emp_area = ? OR 
                              (ua.emp_subarea = 'Admisiones' AND ? = 'Administrativo') OR
                              (ua.emp_subarea = 'Auditor Médico' AND ? = 'Auditoria Médica') OR
                              (ua.emp_subarea = 'Control Interno' AND ? = 'Control Interno')
                          ))
                    ORDER BY lr.id DESC
                """
                params_hist = (user['username'], user.get('managed_area', ''), user.get('managed_area', ''), user.get('managed_area', ''), user.get('managed_area', ''))

            with db_session() as conn:
                df_hist = pd.read_sql_query(query_hist, conn, params=params_hist)
                
            if df_hist.empty:
                st.info("Aún no has procesado ninguna solicitud.")
            else:
                st.write(f"Has procesado **{len(df_hist)}** solicitud(es) históricamente.")
                
                df_hist["Fechas"] = df_hist.apply(
                    lambda r: r['leave_date_start'] if r['leave_date_start'] == r['leave_date_end'] 
                    else f"{r['leave_date_start']} al {r['leave_date_end']}", 
                    axis=1
                )
                
                display_df = df_hist[["id", "user_id", "full_name", "Fechas", "reason_type", "status", "request_date"]]
                display_df.columns = ["Radicado", "DNI", "Empleado", "Fechas", "Tipo", "Estado de Aprobación", "Fecha Solicitud"]
                
                st.info("💡 Haz clic en cualquier fila para ver los detalles completos.")
                
                if 'last_processed_hist' not in st.session_state:
                    st.session_state.last_processed_hist = None
                    
                event_h = st.dataframe(display_df, use_container_width=True, hide_index=True, on_select="rerun", selection_mode="single-row", key="admin_hist_table")
                
                if len(event_h.selection.rows) > 0:
                    row_idx = event_h.selection.rows[0]
                    req_id = int(display_df.iloc[row_idx]["Radicado"])
                    
                    if req_id != st.session_state.last_processed_hist:
                        st.session_state.last_processed_hist = req_id
                        show_leave_request_details(req_id)
                else:
                    st.session_state.last_processed_hist = None

        return

    st.write("Registra permisos, incapacidades médicas o vacaciones. El sistema **no penalizará** a estos empleados en los reportes de tardanzas para los días seleccionados.")

    with db_session() as conn:
        emp_df = pd.read_sql_query("SELECT user_id, full_name, department FROM employees ORDER BY full_name", conn)

    if emp_df.empty:
        st.warning("No hay empleados en el directorio.")
        return

    tab1, tab2, tab3, tab4 = st.tabs(["📝 Registrar Novedad Manual", "📋 Listado de Novedades", "📥 Solicitudes Digitales de Empleados", "🌐 Monitoreo Global"])

    with tab1:
        with st.form("form_exceptions"):
            col1, col2 = st.columns(2)
            with col1:
                selected_emp = st.selectbox(
                    "Empleado",
                    options=emp_df['user_id'].tolist(),
                    format_func=lambda uid: f"{uid} - {emp_df[emp_df['user_id']==uid]['full_name'].values[0]}"
                )
                date_range = st.date_input("Rango de Fechas (Inicio y Fin)", value=[], help="Escoge el día de inicio y fin de la novedad.")
            with col2:
                exc_type = st.selectbox("Tipo de Novedad", ["Incapacidad Médica", "Vacaciones", "Permiso Remunerado", "Permiso NO Remunerado", "Licencia Luto/Maternidad"])
                notes = st.text_area("Observaciones", placeholder="Escribe detalles del permiso si es necesario...")
            
            submit_exc = st.form_submit_button("Guardar Novedad", type="primary")

        if submit_exc:
            if type(date_range) is not tuple and type(date_range) is not list:
                d_start = date_range
                d_end = date_range
            elif len(date_range) == 0:
                st.error("Debes seleccionar al menos una fecha (o un rango).")
                d_start = None
            else:
                d_start = date_range[0]
                d_end = date_range[1] if len(date_range) > 1 else d_start
                
            if d_start:
                delta = d_end - d_start
                for i in range(delta.days + 1):
                    day_to_log = (d_start + timedelta(days=i)).isoformat()
                    upsert_exception(selected_emp, day_to_log, exc_type, notes)
                st.success(f"Novedad registrada del {d_start} al {d_end} para el usuario {selected_emp}.")

    with tab2:
        st.info("💡 Haz clic en cualquier fila para ver los detalles completos del permiso o novedad.")
        df_exc = get_exceptions_df()
        if df_exc.empty:
            st.info("No hay novedades registradas.")
        else:
            df_exc.columns = ["ID", "Usuario", "Nombre", "Fecha", "Tipo", "Observaciones", "Registrado El"]
            
            # =========================================================================
            # PREVENCIÓN ANTIGUOS POPUPS FANTASMAS (BUG "DOBLE CLIC")
            # =========================================================================
            # Este tracker (memoria de estado de la sesión) guarda en disco
            # el ID del último popup que abrió exitosamente. Así cuando ocurre
            # un re-render inadvertido de otra pestaña, bloquea eventos dobles fantasmas.
            if 'last_processed_exc' not in st.session_state:
                st.session_state.last_processed_exc = None
                
            event = st.dataframe(df_exc, use_container_width=True, hide_index=True, on_select="rerun", selection_mode="single-row", key="admin_exc_table")
            
            if len(event.selection.rows) > 0:
                row_idx = event.selection.rows[0]
                selected_id = int(df_exc.iloc[row_idx]["ID"])
                
                if selected_id != st.session_state.last_processed_exc:
                    st.session_state.last_processed_exc = selected_id
                    show_exception_details(selected_id)
            else:
                st.session_state.last_processed_exc = None

    with tab3:
        st.subheader("Bandeja de Aprobación Final de Permisos (Gestión Humana)")
        with db_session() as conn:
            df_pend = pd.read_sql_query("""
                SELECT lr.id, lr.user_id, e.full_name, lr.request_date, lr.leave_date_start, lr.leave_date_end,
                       lr.reason_type, lr.reason_description, lr.is_paid, lr.status, lr.attachment_path,
                       (SELECT full_name FROM users_app WHERE username = lr.approved_by_coord) as coord_name,
                       (SELECT full_name FROM users_app WHERE username = lr.approved_by_jefe) as jefe_name
                FROM leave_requests lr
                JOIN employees e ON lr.user_id = e.user_id
                WHERE lr.status = 'PENDING_RRHH'
                ORDER BY lr.id ASC
            """, conn)
        
        if df_pend.empty:
            st.success("No hay solicitudes pendientes de revisión final.")
        else:
            st.write(f"Tienes **{len(df_pend)}** solicitud(es) por procesar definitivamente.")
            for _, r in df_pend.iterrows():
                with st.container(border=True):
                    cols = st.columns([3, 1])
                    with cols[0]:
                        badge = "🟣 RRHH FINAL"
                        st.markdown(f"**{r['full_name']}** (ID: {r['user_id']}) - *{r['reason_type']}* | {badge}")
                        st.write(f"**Fechas:** {r['leave_date_start']} al {r['leave_date_end']} | **Remunerado:** {'Sí' if r['is_paid'] else 'No'}")
                        
                        if ('coord_name' in r and pd.notna(r['coord_name'])) or ('jefe_name' in r and pd.notna(r['jefe_name'])):
                            with st.expander("✅ Ver Historial de Aprobaciones Previas", expanded=True):
                                if 'coord_name' in r and pd.notna(r['coord_name']):
                                    st.markdown(f"- **Visto Bueno (Coordinador):** {r['coord_name']}")
                                if 'jefe_name' in r and pd.notna(r['jefe_name']):
                                    st.markdown(f"- **Firma (Jefe de Área):** {r['jefe_name']}")
                        
                        st.write(f"**Justificación:** {r['reason_description']}")
                        
                        if r['attachment_path']:
                            import os
                            from database_conn.connection import DATA_DIR
                            file_path = os.path.join(DATA_DIR, "uploads", r['attachment_path'])
                            if os.path.exists(file_path):
                                with st.expander("👁️ Previsualizar Soporte Adjunto", expanded=False):
                                    ext = os.path.splitext(r['attachment_path'])[1].lower()
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
                                    st.download_button("📎 Descargar Soporte Adjunto", data=f.read(), file_name=r['attachment_path'], key=f"dl_rrhh_{r['id']}", use_container_width=True)
                    with cols[1]:
                        requiere_jefe = r['reason_type'] in [
                            "Vacaciones", 
                            "Calamidad Doméstica", 
                            "Licencia de Luto", 
                            "Licencia de Paternidad", 
                            "Licencia por Votación", 
                            "Licencia por Jurado de Votación", 
                            "Licencia Remunerada", 
                            "Licencia No Remunerada"
                        ]
                        btn_label = "✅ Visto Bueno (A Jefe)" if requiere_jefe else "✅ Aprobar Final"
                        
                        if st.button(btn_label, key=f"btn_acc_hr_{r['id']}", type="primary", use_container_width=True):
                            from database_conn.queries import db_approve_leave_request_rrhh
                            if requiere_jefe:
                                db_approve_leave_request_rrhh(r['id'], user['username'], is_final=False)
                                next_status = 'PENDING_JEFE'
                                
                                # Notificar al jefe de área
                                with db_session() as conn:
                                    jefe_df = pd.read_sql_query("""
                                        SELECT emp_email FROM users_app 
                                        WHERE role = 'jefe_area' AND active = 1 AND emp_email IS NOT NULL AND emp_email != '' 
                                        AND managed_area = (
                                            SELECT CASE 
                                                WHEN emp_subarea = 'Admisiones' THEN 'Administrativo' 
                                                WHEN emp_subarea = 'Auditor Médico' THEN 'Auditoria Médica'
                                                WHEN emp_subarea = 'Control Interno' THEN 'Control Interno'
                                                ELSE emp_area 
                                            END
                                            FROM users_app WHERE username = ?
                                        )
                                    """, conn, params=(r['user_id'],))
                                    if not jefe_df.empty:
                                        target_emails = jefe_df['emp_email'].tolist()
                                        from services.email_service import send_novedad_alert
                                        send_novedad_alert(target_emails, r['full_name'], r['reason_type'], r['reason_description'], "N/A", r['leave_date_start'], user['full_name'])
                                
                                log_audit("APPROVE_LEAVE_L2", f"Permiso #{r['id']} ({r['reason_type']}) de {r['full_name']} aprobado por RRHH. Pasa a {next_status}")
                                notify_employee_status(r['user_id'], r['full_name'], r['id'], r['reason_type'], "PRE-APROBADA", "Tu solicitud fue revisada por RRHH y avanzó al Jefe de Área para aprobación final.", user['full_name'])
                                st.rerun()
                            else:
                                db_approve_leave_request_rrhh(r['id'], user['username'], is_final=True)
                                
                                # Generar fechas e insertar en exceptions
                                start_d = datetime.strptime(r['leave_date_start'], "%Y-%m-%d").date()
                                end_d = datetime.strptime(r['leave_date_end'], "%Y-%m-%d").date()
                                days_diff = (end_d - start_d).days + 1
                                
                                with db_session() as conn:
                                    cur = conn.cursor()
                                    for i in range(days_diff):
                                        day_to_log = (start_d + timedelta(days=i)).strftime("%Y-%m-%d")
                                        cur.execute("""
                                            INSERT INTO exceptions (user_id, date, type, notes, created_at)
                                            VALUES (?, ?, ?, ?, ?)
                                            ON CONFLICT(user_id, date) DO UPDATE SET type=excluded.type, notes=excluded.notes
                                        """, (r['user_id'], day_to_log, r['reason_type'], f"Aprobado de Portal (RRHH): {r['reason_description']}", datetime.now().isoformat(timespec="seconds")))
                                
                                log_audit("APPROVE_LEAVE_FINAL", f"Permiso #{r['id']} ({r['reason_type']}) de {r['full_name']} APROBADO FINAL por RRHH.")
                                notify_employee_status(r['user_id'], r['full_name'], r['id'], r['reason_type'], "APROBACIÓN FINAL", "Tu solicitud fue completamente aprobada por Gestión Humana y registrada oficialmente en el sistema.", user['full_name'])
                                st.rerun()
                            
                        if st.button("❌ Rechazar Final", key=f"btn_rej_hr_{r['id']}", use_container_width=True):
                            st.session_state[f"show_rejection_dialog_rrhh_{r['id']}"] = True
                            st.rerun()
                        
                        if st.session_state.get(f"show_rejection_dialog_rrhh_{r['id']}", False):
                            rejection_reason_dialog(r['id'], r['user_id'], r['full_name'], r['reason_type'])

    with tab4:
        # Obtener el rol de la sesión actual
        user_role = st.session_state["user"]["role"]
        
        # Mapeo de rol efectivo a 'nomina' si el usuario es auxiliar de Nómina o Talento Humano
        if user_role == "empleado" and st.session_state["user"].get("emp_subarea") in ["Nomina", "Talento humano"]:
            user_role = "nomina"
            
        # Solo administradores o rol nomina efectivo pueden ver el monitoreo global
        if user_role in ["admin", "nomina"]:
            st.subheader("Monitoreo Global de Permisos (Todas las Áreas)")
            st.info("Vista exclusiva para directivos. Aquí observas el estado de **todas** las solicitudes en curso en toda la empresa.")
            
            with db_session() as conn:
                df_g = pd.read_sql_query("""
                    SELECT lr.id, lr.user_id, e.full_name, e.department, 
                           lr.leave_date_start, lr.leave_date_end,
                           lr.reason_type, lr.status, lr.request_date
                    FROM leave_requests lr
                    JOIN employees e ON lr.user_id = e.user_id
                    WHERE lr.status LIKE 'PENDING_%'
                    ORDER BY lr.request_date DESC
                """, conn)
            
            if df_g.empty:
                st.success("Toda la tubería está limpia. No hay solicitudes estancadas.")
            else:
                st.write(f"Hay **{len(df_g)}** solicitudes esperando aprobación en algún nivel.")
                
                # =========================================================================
                # COMPRESIÓN INTELIGENTE DE COLUMNAS (FUSIÓN DE FECHAS)
                # =========================================================================
                # Usamos una función anónima (Lambda) de Pandas. Si la persona 
                # pidió permiso para un solo día (inicio = fin), imprimimos un día.
                # De lo contrario (vacaciones cruzadas), concatenamos con un " al ".
                # Esto ahorra un 20% del espacio invaluable en el ancho del monitor.
                df_g["Fechas"] = df_g.apply(
                    lambda r: r['leave_date_start'] if r['leave_date_start'] == r['leave_date_end'] 
                    else f"{r['leave_date_start']} al {r['leave_date_end']}", 
                    axis=1
                )
                
                display_df = df_g[["id", "user_id", "full_name", "department", "Fechas", "reason_type", "status", "request_date"]]
                display_df.columns = ["Radicado", "DNI", "Empleado", "Área/Departamento", "Fechas", "Tipo", "Estado de Aprobación", "Fecha Solicitud"]
                
                st.info("💡 Haz clic en cualquier fila para ver los detalles completos de la solicitud.")
                
                # Prevenidor de Popups Fantasmas replicado para esta tabla Global
                if 'last_processed_global' not in st.session_state:
                    st.session_state.last_processed_global = None
                    
                event_g = st.dataframe(display_df, use_container_width=True, hide_index=True, on_select="rerun", selection_mode="single-row", key="admin_global_table")
                
                if len(event_g.selection.rows) > 0:
                    row_idx = event_g.selection.rows[0]
                    req_id = int(display_df.iloc[row_idx]["Radicado"])
                    
                    # Interceptor lógico: ¿Es un click fresco en una fila NUEVA o quitada?
                    # Si coincide con la memoria sucia, ignóralo para no interrumpir al Administrador.
                    if req_id != st.session_state.last_processed_global:
                        st.session_state.last_processed_global = req_id
                        
                        # Invocación directa a la tarjeta visual que diseñamos en employee_portal (Modularidad)
                        show_leave_request_details(req_id)
                else:
                    # En caso de que el admnistrador un-clickee la fila para "cerrar" visualmente.
                    st.session_state.last_processed_global = None
        else:
            st.warning("No tienes permisos de Administrador para ver la panorámica global de todas las áreas.")