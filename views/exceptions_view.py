import streamlit as st
import pandas as pd
from datetime import date, timedelta, datetime

from database_conn.connection import db_session
from database_conn.queries import upsert_exception, get_exceptions_df
from services.notifications import log_audit, notify_employee
from utils.auth import require_role
from views.employee_portal_view import show_leave_request_details

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
                SELECT user_id, action, timestamp 
                FROM audit_logs 
                WHERE details LIKE ? AND action LIKE 'APPROVE_%'
                ORDER BY timestamp ASC
            """, conn, params=(f"%Permiso #{req['id']} %",))
        
        if not df_audit.empty:
            st.divider()
            st.markdown("**Trazabilidad de Aprobaciones:**")
            for _, row_a in df_audit.iterrows():
                level = "Jefatura" if row_a['action'] == "APPROVE_LEAVE_L1" else "Gestión Humana"
                st.caption(f"✓ **{level}**: {row_a['user_id']} ({row_a['timestamp']})")
    else:
        st.info("ℹ️ Esta novedad no parece tener una solicitud digital asociada del portal de empleados (o fue ingresada manualmente).")

def page_exceptions():
    require_role("admin", "nomina", "jefe_area", "coordinador")
    st.title("🛡️ Novedades y Justificaciones")
    user = st.session_state["user"]

    if user["role"] in ["coordinador", "jefe_area"]:
        st.write(f"Bandeja de Aprobación para: **{user.get('managed_department') or user.get('managed_area')}**")
        
        if user["role"] == "coordinador":
            query = """
                SELECT lr.id, lr.user_id, e.full_name, lr.request_date, lr.leave_date_start, lr.leave_date_end,
                       lr.reason_type, lr.reason_description, lr.is_paid, lr.status
                FROM leave_requests lr
                JOIN employees e ON lr.user_id = e.user_id
                WHERE lr.status = 'PENDING_INMEDIATO' AND e.department = ?
                ORDER BY lr.id ASC
            """
            params = (user.get('managed_department', ''),)
        else:
            query = """
                SELECT lr.id, lr.user_id, e.full_name, lr.request_date, lr.leave_date_start, lr.leave_date_end,
                       lr.reason_type, lr.reason_description, lr.is_paid, lr.status
                FROM leave_requests lr
                JOIN employees e ON lr.user_id = e.user_id
                WHERE lr.status = 'PENDING_AREA' AND e.department LIKE ?
                ORDER BY lr.id ASC
            """
            params = (f"{user.get('managed_area', '')} - %",)
            
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
                        st.write(f"**Justificación:** {r['reason_description']}")
                    with cols[1]:
                        if st.button("👍 Aprobar", key=f"btn_acc_{r['id']}", type="primary", use_container_width=True):
                            if user["role"] == "coordinador":
                                if r['reason_type'] in ["Vacaciones", "Día de la familia", "Votaciones", "Licencia de luto"]:
                                    next_status = 'PENDING_AREA'
                                else:
                                    next_status = 'PENDING_RRHH'
                            else:
                                next_status = 'PENDING_RRHH'
                                
                            with db_session() as conn:
                                cur = conn.cursor()
                                cur.execute("UPDATE leave_requests SET status = ? WHERE id = ?", (next_status, r['id']))
                            
                            log_audit("APPROVE_LEAVE_L1", f"Permiso #{r['id']} ({r['reason_type']}) de {r['full_name']} pre-aprobado. Pasa a {next_status}")
                            notify_employee(r['user_id'], f"Dolormed: Novedad #{r['id']} Pre-Aprobada", f"Hola {r['full_name']},<br>Tu permiso de {r['reason_type']} fue pre-aprobado por tu jefatura. Pasa a estado: {next_status}.")
                            st.rerun()
                            
                        if st.button("❌ Rechazar", key=f"btn_rej_{r['id']}", use_container_width=True):
                            with db_session() as conn:
                                cur = conn.cursor()
                                cur.execute("UPDATE leave_requests SET status = 'REJECTED' WHERE id = ?", (r['id'],))
                                
                            log_audit("REJECT_LEAVE_L1", f"Permiso #{r['id']} ({r['reason_type']}) de {r['full_name']} rechazado.")
                            notify_employee(r['user_id'], f"Dolormed: Novedad #{r['id']} Rechazada", f"Hola {r['full_name']},<br>Tu permiso de {r['reason_type']} fue RECHAZADO por tu jefatura.")
                            st.rerun()
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
                       lr.reason_type, lr.reason_description, lr.is_paid, lr.status
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
                        st.write(f"**Justificación:** {r['reason_description']}")
                    with cols[1]:
                        btn_label = "✅ Aprobar Final"
                        if st.button(btn_label, key=f"btn_acc_hr_{r['id']}", type="primary", use_container_width=True):
                            with db_session() as conn:
                                cur = conn.cursor()
                                cur.execute("UPDATE leave_requests SET status = 'APPROVED' WHERE id = ?", (r['id'],))
                                
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
                                
                            log_audit("APPROVE_LEAVE_FINAL", f"Permiso #{r['id']} ({r['reason_type']}) de {r['full_name']} APROBADO FINAL por RRHH.")
                            st.rerun()
                            
                        if st.button("❌ Rechazar Final", key=f"btn_rej_hr_{r['id']}", use_container_width=True):
                            with db_session() as conn:
                                cur = conn.cursor()
                                cur.execute("UPDATE leave_requests SET status = 'REJECTED' WHERE id = ?", (r['id'],))
                            
                            log_audit("REJECT_LEAVE_FINAL", f"Permiso #{r['id']} ({r['reason_type']}) de {r['full_name']} RECHAZADO FINAL por RRHH.")
                            st.rerun()

    with tab4:
        user_role = st.session_state["user"]["role"]
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
                    req_id = int(df_g.iloc[row_idx]["Radicado"])
                    
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