from datetime import date, datetime, timedelta

import pandas as pd
import streamlit as st
from streamlit_option_menu import option_menu

from database_conn.connection import db_conn, db_session
from database_conn.queries import (
    db_approve_leave_request_coord,
    db_approve_leave_request_jefe,
    db_reject_leave_request,
    get_exceptions_df,
    get_cached_dataframe,
    is_holiday,
    upsert_exception,
)
from services.notifications import log_audit, notify_employee_status
from utils.auth import require_role
from utils.constants import ZARZAL_EMPLOYEES
from views.employee_portal_view import show_leave_request_details


def get_reason_icon(reason: str) -> str:
    """Devuelve un emoji representativo según el motivo del permiso."""
    if not reason:
        return "📝"
    r_lower = reason.lower()
    if "vacaciones" in r_lower:
        return "🏖️"
    if "cita" in r_lower or "médic" in r_lower or "medic" in r_lower:
        return "🏥"
    if "calamidad" in r_lower:
        return "🌧️"
    if "votación" in r_lower or "votacion" in r_lower:
        return "🗳️"
    if "personal" in r_lower:
        return "👤"
    if "laboral" in r_lower:
        return "💼"
    if "remunerada" in r_lower:
        return "📄"
    return "📝"


# --- Componente de Detección de Conflictos para Jefes ---
def check_schedule_conflicts(
    request: pd.Series, approver_role: str, managed_entity: str
):
    """
    Verifica si una solicitud de permiso se solapa con otras ausencias ya aprobadas
    dentro del mismo equipo o área.
    """
    start_date = request["leave_date_start"]
    end_date = request["leave_date_end"]

    # La consulta busca otras solicitudes aprobadas que se crucen en el rango de fechas
    # y que pertenezcan al mismo grupo de gestión (departamento o área).
    query = """
        SELECT lr.id, e.full_name
        FROM leave_requests lr
        JOIN employees e ON lr.user_id = e.user_id
        WHERE lr.status = 'APPROVED'
          AND lr.id != %s
          AND (
              (lr.leave_date_start <= %s AND lr.leave_date_end >= %s) OR
              (lr.leave_date_start <= %s AND lr.leave_date_end >= %s) OR
              (lr.leave_date_start >= %s AND lr.leave_date_end <= %s)
          )
    """

    params = [
        request["id"],
        start_date,
        start_date,
        end_date,
        end_date,
        start_date,
        end_date,
    ]

    # Ajustar el filtro de la consulta según el rol del aprobador
    if approver_role == "coordinador":
        depts = [d.strip() for d in managed_entity.split(",") if d.strip()]
        if not depts:
            depts = [""]
        like_conds = " OR ".join(["e.department LIKE %s"] * len(depts))
        query += f" AND ({like_conds})"
        for d in depts:
            params.append(f"% - {d}")
    elif approver_role == "jefe_area":
        query += " AND e.department LIKE %s"
        params.append(f"{managed_entity} - %")

    with db_conn() as conn:
        conflicts_df = get_cached_dataframe(query, params=params)

    if not conflicts_df.empty:
        names = ", ".join(conflicts_df["full_name"].tolist())
        count = len(conflicts_df)
        st.warning(
            f"⚠️ **Alerta de Cruce:** Ya hay {count} persona(s) de esta área con permiso en estas fechas: **{names}**."
        )


# --- Fin del Componente ---


@st.dialog("Detalles Completos de la Novedad/Permiso")
def show_exception_details(exc_id: int):
    with db_session() as conn:
        df_exc = get_cached_dataframe(
            """
            SELECT ex.user_id, e.full_name, ex.date, ex.type, ex.notes, ex.created_at
            FROM exceptions ex
            LEFT JOIN employees e ON ex.user_id = e.user_id
            WHERE ex.id = %s
        """, params=(exc_id,),
        )

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
        df_req = get_cached_dataframe(
            """
            SELECT *
            FROM leave_requests
            WHERE user_id = %s AND status = 'APPROVED'
              AND leave_date_start <= %s AND leave_date_end >= %s
            ORDER BY id DESC LIMIT 1
        """, params=(exc["user_id"], exc["date"], exc["date"]),
        )

    if not df_req.empty:
        req = df_req.iloc[0]
        st.markdown("### 📄 Detalles de la Solicitud (Portal F-TH-012)")

        c1, c2 = st.columns(2)
        with c1:
            st.markdown(f"**Radicado:** #{req['id']}")
            st.markdown(f"**Fecha de Solicitud:** {req['request_date']}")
            if (
                "specific_dates" in req
                and pd.notna(req.get("specific_dates"))
                and str(req.get("specific_dates")).strip() != "None"
                and str(req.get("specific_dates")).strip() != ""
            ):
                formatted_dates = str(req["specific_dates"]).replace(",", ", ")
                st.markdown(f"**Fechas de Ausencia:** {formatted_dates}")
            elif req["leave_date_start"] == req["leave_date_end"]:
                st.markdown(f"**Fechas de Ausencia:** {req['leave_date_start']}")
            else:
                st.markdown(
                    f"**Fechas de Ausencia:** {req['leave_date_start']} al {req['leave_date_end']}"
                )
            st.markdown(f"**Remunerado:** {'✅ Sí' if req['is_paid'] else '❌ No'}")
        with c2:
            h_in = req["start_time"] if req["start_time"] else "N/A"
            h_out = req["end_time"] if req["end_time"] else "N/A"
            st.markdown(f"**Hora Salida:** {h_in}")
            st.markdown(f"**Hora Entrada:** {h_out}")
            st.markdown(f"**Tiempo Total:** {req['total_time']}")

        st.write(f"**Motivo Original:** {req['reason_type']}")

        st.markdown("**Justificación del Empleado:**")
        st.info(
            req["reason_description"]
            if req["reason_description"]
            else "Sin detalles adicionales."
        )

        if req["how_to_makeup"]:
            st.markdown("**Acuerdo de Reposición Prometido:**")
            st.warning(req["how_to_makeup"])

        if pd.notna(req["attachment_path"]) and str(req["attachment_path"]).strip():
            st.write("**Soporte Adjunto:**")
            import os

            from database_conn.connection import DATA_DIR

            file_path = os.path.join(DATA_DIR, "uploads", str(req["attachment_path"]))
            if os.path.exists(file_path):
                with st.expander("👁️ Previsualizar Soporte Adjunto", expanded=False):
                    ext = os.path.splitext(file_path)[1].lower()
                    if ext in [".png", ".jpg", ".jpeg", ".webp"]:
                        st.image(file_path, use_container_width=True)
                    elif ext == ".pdf":
                        try:
                            import shutil

                            static_dir = os.path.join(os.getcwd(), "static")
                            os.makedirs(static_dir, exist_ok=True)
                            static_file_path = os.path.join(
                                static_dir, str(req["attachment_path"])
                            )

                            if not os.path.exists(static_file_path):
                                shutil.copy2(file_path, static_file_path)

                            pdf_url = f"/app/static/{req['attachment_path']}"
                            pdf_display = f'<iframe src="{pdf_url}" width="100%" height="600" style="border: none;"></iframe>'
                            st.markdown(pdf_display, unsafe_allow_html=True)
                        except Exception as e:
                            st.error(f"No se pudo cargar el PDF: {e}")
                    else:
                        st.info("Vista previa no disponible para este tipo de archivo.")

                with open(file_path, "rb") as f:
                    st.download_button(
                        "📎 Descargar Soporte Adjunto",
                        data=f.read(),
                        file_name=str(req["attachment_path"]),
                        key=f"dl_det_{req['id']}",
                        use_container_width=True,
                    )

        with db_session() as conn:
            df_audit = get_cached_dataframe(
                """
                SELECT a.user_id, a.action, a.timestamp, u.full_name, a.details, u.role
                FROM audit_logs a
                LEFT JOIN users_app u ON a.user_id = u.username
                WHERE a.details LIKE %s AND a.action LIKE 'APPROVE_%%'
                ORDER BY a.timestamp ASC
            """, params=(f"%Permiso #{req['id']} %",),
            )

        if not df_audit.empty:
            st.divider()
            st.markdown("**Trazabilidad de Aprobaciones:**")
            for _, row_a in df_audit.iterrows():
                role_val = row_a.get("role")
                if role_val == "coordinador":
                    level = "Coordinador"
                elif role_val == "jefe_area":
                    level = "Jefe de Área"
                elif role_val in ["admin", "nomina"]:
                    level = "Gestión Humana"
                else:
                    if row_a["action"] == "APPROVE_LEAVE_L1":
                        level = "Coordinador"
                    elif "Jefe de Área" in str(row_a.get("details", "")):
                        level = "Jefe de Área"
                    else:
                        level = "Gestión Humana"
                approver_name = (
                    row_a["full_name"]
                    if pd.notna(row_a["full_name"])
                    else row_a["user_id"]
                )
                st.caption(f"✓ **{level}**: {approver_name} ({row_a['timestamp']})")
    else:
        st.info(
            "ℹ️ Esta novedad no parece tener una solicitud digital asociada del portal de empleados (o fue ingresada manualmente)."
        )


@st.dialog("👁️ Soporte Adjunto")
def preview_attachment_dialog(attachment_path, employee_name):
    st.markdown(f"**Empleado:** {employee_name}")
    st.markdown(f"**Archivo:** `{attachment_path}`")
    st.divider()

    import os

    from database_conn.connection import DATA_DIR

    file_path = os.path.join(DATA_DIR, "uploads", str(attachment_path))
    if not os.path.exists(file_path):
        st.error("El archivo soporte no se encuentra en el servidor.")
        return

    ext = os.path.splitext(attachment_path)[1].lower()
    if ext in [".png", ".jpg", ".jpeg", ".webp"]:
        st.image(file_path, use_container_width=True)
    elif ext == ".pdf":
        try:
            import shutil

            # Streamlit >= 1.18 permite servir archivos estáticos desde ./static
            static_dir = os.path.join(os.getcwd(), "static")
            os.makedirs(static_dir, exist_ok=True)
            static_file_path = os.path.join(static_dir, str(attachment_path))

            if not os.path.exists(static_file_path):
                shutil.copy2(file_path, static_file_path)

            pdf_url = f"/app/static/{attachment_path}"
            pdf_display = f'<iframe src="{pdf_url}" width="100%" height="600" style="border: none;"></iframe>'
            st.markdown(pdf_display, unsafe_allow_html=True)
        except Exception as e:
            st.error(f"No se pudo cargar el PDF: {e}")
    else:
        st.info("Vista previa no disponible en pantalla para este tipo de archivo.")

    st.divider()
    with open(file_path, "rb") as f:
        st.download_button(
            "📎 Descargar Soporte Adjunto",
            data=f.read(),
            file_name=str(attachment_path),
            use_container_width=True,
            key=f"dl_dialog_{attachment_path}",
        )


@st.dialog("Motivo de Rechazo")
def rejection_reason_dialog(req_id, user_id, full_name, reason_type):
    st.write(f"Rechazando solicitud #{req_id} de {full_name} ({reason_type}).")
    reason = st.text_area(
        "Por favor, ingresa el motivo del rechazo:", key=f"rejection_reason_{req_id}"
    )

    col1, col2 = st.columns(2)
    with col1:
        if st.button("Confirmar Rechazo", type="primary", use_container_width=True):
            if reason:
                db_reject_leave_request(
                    req_id, st.session_state["user"]["username"], reason
                )
                log_audit(
                    "REJECT_LEAVE_L1",
                    f"Permiso #{req_id} ({reason_type}) de {full_name} rechazado por {st.session_state['user']['full_name']}. Motivo: {reason}",
                )
                notify_employee_status(
                    user_id,
                    full_name,
                    req_id,
                    reason_type,
                    "RECHAZADA",
                    f"Tu permiso fue rechazado por la jefatura/RRHH. Motivo: {reason}",
                    st.session_state["user"]["full_name"],
                )
                st.toast("Solicitud rechazada y empleado notificado.")
                st.rerun()
            else:
                st.error("El motivo de rechazo no puede estar vacío.")
    with col2:
        if st.button("Cancelar", use_container_width=True):
            st.session_state[f"show_rejection_dialog_{req_id}"] = False
            st.rerun()


# --- CALLBACK DE APROBACIÓN (PREVENCIÓN DE ERRORES UI) ---
# Extraemos la lógica de aprobación a este "Callback". Al hacerlo, le indicamos a Streamlit
# que ejecute toda la lógica pesada de base de datos y envío de correos en SEGUNDO PLANO
# antes de refrescar la pantalla. Si hacíamos esto directamente dentro del botón (inline),
# el navegador colapsaba (NotFoundError) al intentar borrar visores de PDF pesados
# al mismo tiempo que la página se reiniciaba con st.rerun().
def handle_approve_callback(r_dict, user):
    if user["role"] == "coordinador":
        success = db_approve_leave_request_coord(r_dict["id"], user["username"])
        if not success:
            st.toast("La solicitud ya fue procesada o no se encontró.", icon="⚠️")
            return

        next_status = "PENDING_RRHH"

        with db_session() as conn:
            admin_df = get_cached_dataframe(
                "SELECT emp_email FROM users_app WHERE role IN ('admin', 'nomina') AND active = 1 AND emp_email IS NOT NULL AND emp_email != ''")
            if not admin_df.empty:
                target_emails = admin_df["emp_email"].tolist()
                from services.email_service import send_novedad_alert

                send_novedad_alert(
                    target_emails,
                    r_dict["full_name"],
                    r_dict["reason_type"],
                    r_dict["reason_description"],
                    "N/A",
                    r_dict["leave_date_start"],
                    user["full_name"],
                )

        log_audit(
            "APPROVE_LEAVE_L1",
            f"Permiso #{r_dict['id']} ({r_dict['reason_type']}) de {r_dict['full_name']} aprobado por {user['role']}. Pasa a {next_status}",
        )
        notify_employee_status(
            r_dict["user_id"],
            r_dict["full_name"],
            r_dict["id"],
            r_dict["reason_type"],
            "PRE-APROBADA",
            f"Tu solicitud avanzó en el flujo de firmas hacia el siguiente aprobador ({next_status}).",
            user["full_name"],
        )

    else:
        success = db_approve_leave_request_jefe(r_dict["id"], user["username"])
        if not success:
            st.toast("La solicitud ya fue procesada o no se encontró.", icon="⚠️")
            return

        next_status = "PENDING_RRHH"

        log_audit(
            "APPROVE_LEAVE_L1",
            f"Permiso #{r_dict['id']} ({r_dict['reason_type']}) de {r_dict['full_name']} aprobado por Jefe de Área. Pasa a {next_status}",
        )
        notify_employee_status(
            r_dict["user_id"],
            r_dict["full_name"],
            r_dict["id"],
            r_dict["reason_type"],
            "PRE-APROBADA",
            f"Tu solicitud avanzó en el flujo de firmas hacia RRHH para su revisión y aplicación final.",
            user["full_name"],
        )


def render_absence_calendar(user):
    import calendar

    st.write(
        "Visualiza las ausencias programadas y pendientes del personal a tu cargo."
    )

    # 1. Filtros de Fecha
    hoy = date.today()
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
        selected_month_name = st.selectbox(
            "Mes", meses, index=hoy.month - 1, key="cal_sel_month"
        )
        selected_month = meses.index(selected_month_name) + 1
    with c_m2:
        selected_year = st.selectbox(
            "Año", list(range(hoy.year - 1, hoy.year + 2)), index=1, key="cal_sel_year"
        )

    # Leyenda
    st.markdown(
        """
    <div style='display: flex; gap: 15px; margin-bottom: 15px; font-size: 0.85rem; background-color: rgba(128,128,128,0.05); padding: 8px 12px; border-radius: 6px; border: 1px solid rgba(128,128,128,0.1);'>
        <div><span style='display:inline-block; width:12px; height:12px; background-color:#3b82f6; border-radius:3px; vertical-align:middle; margin-right:4px;'></span><strong>Botón Azul:</strong> Solicitud Aprobada (Confirmado)</div>
        <div><span style='display:inline-block; width:12px; height:12px; background-color:#eab308; border-radius:3px; vertical-align:middle; margin-right:4px;'></span><strong>Botón Amarillo:</strong> Solicitud Pendiente (Advertencia)</div>
    </div>
    """,
        unsafe_allow_html=True,
    )

    # 2. Consultar Solicitudes del mes
    first_day = date(selected_year, selected_month, 1)
    dias_del_mes = calendar.monthrange(selected_year, selected_month)[1]
    last_day = date(selected_year, selected_month, dias_del_mes)

    first_day_str = first_day.isoformat()
    last_day_str = last_day.isoformat()

    status_filter = "('PENDING_COORD', 'PENDING_JEFE', 'PENDING_RRHH', 'APPROVED')"

    effective_role = user["role"]
    if effective_role == "empleado" and user.get("emp_subarea") in [
        "Nomina",
        "Talento humano",
    ]:
        effective_role = "nomina"

    if effective_role not in ["admin", "nomina", "coordinador", "jefe_area"]:
        return

    query = ""
    params = ()

    if effective_role in ["admin", "nomina"]:
        query = f"""
            SELECT lr.id, lr.user_id, e.full_name, lr.leave_date_start, lr.leave_date_end, 
                   lr.reason_type, lr.total_time, lr.status, lr.reason_description
            FROM leave_requests lr
            JOIN employees e ON lr.user_id = e.user_id
            LEFT JOIN users_app ua ON lr.user_id = ua.username
            WHERE lr.status IN {status_filter}
              AND lr.leave_date_start <= %s AND lr.leave_date_end >= %s
            ORDER BY lr.leave_date_start ASC
        """
        params = (last_day_str, first_day_str)
    elif effective_role == "coordinador":
        managed_dept_str = user.get("managed_department", "")
        cond_serv_gen = (
            "OR (ua.emp_subarea = 'Servicios Generales')"
            if "Calidad" in managed_dept_str
            else ""
        )
        cond_orientador = (
            "OR (ua.emp_subarea = 'Orientador')"
            if "Seguridad" in managed_dept_str
            else ""
        )
        cond_zarzal = (
            f"OR (lr.user_id IN ({','.join(['%s'] * len(ZARZAL_EMPLOYEES))}))"
            if user["username"] == "111644844"
            else ""
        )

        query = f"""
            SELECT lr.id, lr.user_id, e.full_name, lr.leave_date_start, lr.leave_date_end, 
                   lr.reason_type, lr.total_time, lr.status, lr.reason_description
            FROM leave_requests lr
            JOIN employees e ON lr.user_id = e.user_id
            LEFT JOIN users_app ua ON lr.user_id = ua.username
            WHERE lr.status IN {status_filter}
              AND lr.leave_date_start <= %s AND lr.leave_date_end >= %s
              AND (
                  %s LIKE '%%' || ua.emp_subarea || '%%'
                  {cond_serv_gen}
                  {cond_orientador}
                  {cond_zarzal}
              )
            ORDER BY lr.leave_date_start ASC
        """
        params = [last_day_str, first_day_str, managed_dept_str]
        if user["username"] == "111644844":
            params += ZARZAL_EMPLOYEES
        params = tuple(params)
    elif effective_role == "jefe_area":
        if user.get("managed_area", "") == "Control Interno":
            query = f"""
                SELECT lr.id, lr.user_id, e.full_name, lr.leave_date_start, lr.leave_date_end, 
                       lr.reason_type, lr.total_time, lr.status, lr.reason_description
                FROM leave_requests lr
                JOIN employees e ON lr.user_id = e.user_id
                LEFT JOIN users_app ua ON lr.user_id = ua.username
                WHERE lr.status IN {status_filter}
                  AND lr.leave_date_start <= %s AND lr.leave_date_end >= %s
                ORDER BY lr.leave_date_start ASC
            """
            params = (last_day_str, first_day_str)
        else:
            query = f"""
                SELECT lr.id, lr.user_id, e.full_name, lr.leave_date_start, lr.leave_date_end, 
                       lr.reason_type, lr.total_time, lr.status, lr.reason_description
                FROM leave_requests lr
                JOIN employees e ON lr.user_id = e.user_id
                LEFT JOIN users_app ua ON lr.user_id = ua.username
                WHERE lr.status IN {status_filter}
                  AND lr.leave_date_start <= %s AND lr.leave_date_end >= %s
                  AND (
                      (ua.username IN ('119279359', '111627893') AND %s = 'Administrativo') OR
                      (ua.username NOT IN ('119279359', '111627893') AND ua.emp_area = %s AND ua.emp_subarea NOT IN ('Admisiones', 'Enfermería', 'Rehabilitación', 'Tecnólogo Rayos X', 'Auditor Médico', 'Medico', 'Farmacia', 'Control Interno', 'Cirugía', 'Mantenimiento', 'Seguridad', 'Orientador')) OR 
                      (ua.emp_subarea IN ('Rehabilitación', 'Tecnólogo Rayos X', 'Farmacia', 'Mantenimiento', 'Seguridad', 'Orientador') AND %s = 'Administrativo') OR
                      (ua.emp_subarea = 'Admisiones' AND %s = 'Financiera') OR
                      (lr.user_id IN ({",".join(["%s"] * len(ZARZAL_EMPLOYEES))}) AND %s = 'Administrativo')
                  )
                ORDER BY lr.leave_date_start ASC
            """
            params = [last_day_str, first_day_str]
            params += [
                user.get("managed_area", ""),
                user.get("managed_area", ""),
                user.get("managed_area", ""),
                user.get("managed_area", ""),
            ]
            params.extend(ZARZAL_EMPLOYEES)
            params.append(user.get("managed_area", ""))
            params = tuple(params)

    with db_session() as conn:
        df_reqs = get_cached_dataframe(query, params=params)

    # Agrupar eventos por día
    events_by_day = {}
    for d in range(1, dias_del_mes + 1):
        curr_date = date(selected_year, selected_month, d)
        date_str = curr_date.isoformat()
        events_by_day[date_str] = []
        for _, r in df_reqs.iterrows():
            start_d = date.fromisoformat(r["leave_date_start"])
            end_d = date.fromisoformat(r["leave_date_end"])
            if start_d <= curr_date <= end_d:
                events_by_day[date_str].append(r)

    # Construir HTML del grid con Estilo Super Premium
    grid_html = """
<style>
.cal-wrapper { 
  background: rgba(255, 255, 255, 0.02);
  backdrop-filter: blur(10px);
  border: 1px solid rgba(128,128,128,0.15); 
  border-radius: 18px; 
  padding: 24px;
  box-shadow: 0 12px 36px rgba(0,0,0,0.08);
  overflow-x: auto;
  margin-bottom: 2rem;
}
.cal-header { 
  display: flex; min-width: 800px;
  text-align: center; font-weight: 700; margin-bottom: 15px; opacity: 0.8; font-size: 0.95rem; 
}
.cal-header > div { flex: 1; }
.cal-grid { 
  display: flex; flex-wrap: wrap; gap: 10px; min-width: 800px;
}
.cal-cell {
  width: calc(14.28% - 9px); min-height: 120px; border-radius: 14px; padding: 10px;
  display: flex; flex-direction: column;
  position: relative; transition: all 0.3s cubic-bezier(0.175, 0.885, 0.32, 1.275);
  border: 1px solid rgba(128,128,128,0.12);
  background-color: rgba(128,128,128,0.02);
  box-shadow: 0 4px 10px rgba(0,0,0,0.02);
}
.cal-cell:hover { 
  transform: translateY(-5px) scale(1.03); 
  z-index: 10; 
  box-shadow: 0 15px 30px rgba(13, 110, 253, 0.15); 
  border-color: rgba(13, 110, 253, 0.5); 
}

.day-number { align-self: flex-end; font-weight: 700; font-size: 1.05rem; opacity: 0.9; margin-bottom: 5px; }

/* Tipos de celda */
.day-empty { background: transparent; pointer-events: none; border: none; }
.day-today { border-color: #0d6efd !important; background-color: rgba(13, 110, 253, 0.03) !important; }

/* Enlaces de eventos */
.event-link {
  display: block;
  padding: 4px 8px;
  margin-bottom: 4px;
  border-radius: 5px;
  font-size: 0.78rem;
  font-weight: 600;
  text-decoration: none !important;
  transition: all 0.2s;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
  box-shadow: 0 1px 3px rgba(0,0,0,0.1);
}
.event-link:hover {
  transform: translateY(-1px);
  box-shadow: 0 2px 5px rgba(0,0,0,0.15);
}
.event-approved {
  background-color: rgba(13, 110, 253, 0.2) !important;
  color: #3b82f6 !important;
  border: 1px solid rgba(13, 110, 253, 0.4) !important;
}
.event-approved:hover {
  background-color: rgba(13, 110, 253, 0.35) !important;
  color: #60a5fa !important;
}
.event-pending {
  background-color: rgba(234, 179, 8, 0.2) !important;
  color: #facc15 !important;
  border: 1px solid rgba(234, 179, 8, 0.4) !important;
}
.event-pending:hover {
  background-color: rgba(234, 179, 8, 0.35) !important;
  color: #fef08a !important;
}
</style>
<div class="cal-wrapper">
<div class="cal-header">
    <div>Lunes</div><div>Martes</div><div>Miércoles</div><div>Jueves</div><div>Viernes</div><div>Sábado</div><div>Domingo</div>
</div>
<div class="cal-grid">
"""

    start_weekday = first_day.weekday()  # 0=Lun, 6=Dom

    # Rellenar celdas vacías al inicio
    for _ in range(start_weekday):
        grid_html += '<div class="cal-cell day-empty"></div>'

    import urllib.parse

    session_token = st.session_state.get("session_token", "")
    session_param = f"&session_token={session_token}" if session_token else ""
    current_tab = st.session_state.get("exceptions_active_tab", 0)
    tab_param = f"&tab_sel={current_tab}"

    menu_name = (
        "Autorización de Permisos"
        if effective_role in ["coordinador", "jefe_area"]
        else "Novedades y Excepciones"
    )
    menu_param_val = urllib.parse.quote_plus(menu_name)

    for d in range(1, dias_del_mes + 1):
        curr_date = date(selected_year, selected_month, d)
        date_str = curr_date.isoformat()
        day_events = events_by_day.get(date_str, [])

        # Clases de la celda
        cell_classes = ["cal-cell"]
        if curr_date == date.today():
            cell_classes.append("day-today")

        grid_html += f'<div class="{" ".join(cell_classes)}">'
        grid_html += f'<div class="day-number">{d}</div>'

        for r in day_events:
            icon = get_reason_icon(r["reason_type"])
            status = r["status"]
            event_class = "event-approved" if status == "APPROVED" else "event-pending"
            anchor_id = f"req-{r['id']}"
            grid_html += f'<a id="{anchor_id}" href="?selected_req_id={r["id"]}&menu_sel={menu_param_val}{session_param}{tab_param}#{anchor_id}" target="_self" class="event-link {event_class}">{icon} {r["full_name"]}</a>'

        grid_html += "</div>"

    grid_html += """
</div>
</div>
"""
    st.markdown(grid_html, unsafe_allow_html=True)


def page_exceptions():
    require_role("admin", "nomina", "jefe_area", "coordinador")

    # Interceptar clic en el calendario (parámetro de consulta)
    if "selected_req_id" in st.query_params:
        try:
            req_id = int(st.query_params["selected_req_id"])
            if st.session_state.get("last_opened_req_id") != req_id:
                # Abrir el modal en esta recarga
                st.session_state["last_opened_req_id"] = req_id
                show_leave_request_details(req_id)
            else:
                # En la siguiente interacción o recarga, limpiamos la URL para no reabrir el modal
                del st.query_params["selected_req_id"]
                st.session_state["last_opened_req_id"] = None
        except Exception:
            pass

    st.title("🛡️ Novedades y Justificaciones")
    user = st.session_state["user"]

    # --- PERSISTENCIA DE PESTAÑA ACTIVA ---
    if "tab_sel" in st.query_params:
        try:
            st.session_state["exceptions_active_tab"] = int(st.query_params["tab_sel"])
        except Exception:
            pass
        st.query_params.pop("tab_sel", None)

    default_tab_idx = 0
    if "exceptions_active_tab" in st.session_state:
        default_tab_idx = st.session_state["exceptions_active_tab"]

    if user["role"] in ["coordinador", "jefe_area"]:
        st.write(
            f"Panel de Gestión para: **{user.get('managed_department') or user.get('managed_area')}**"
        )

        tab_options = [
            "📥 Bandeja de Aprobación",
            "📅 Calendario de Ausencias",
            "🕰️ Historial de Decisiones",
        ]
        sel_tab = option_menu(
            menu_title=None,
            options=tab_options,
            icons=["inbox", "calendar", "clock-history"],
            menu_icon="cast",
            default_index=default_tab_idx,
            orientation="horizontal",
            key="coord_tabs_menu",
            styles={
                "container": {
                    "padding": "0!important",
                    "background-color": "transparent",
                    "border": "none",
                },
                "icon": {"color": "#0066cc", "font-size": "14px"},
                "nav-link": {
                    "font-size": "14px",
                    "text-align": "center",
                    "margin": "0px",
                    "--hover-color": "rgba(128,128,128,0.15)",
                },
                "nav-link-selected": {
                    "background-color": "#0066cc",
                    "color": "white",
                    "icon-color": "white",
                },
            },
        )
        st.session_state["exceptions_active_tab"] = tab_options.index(sel_tab)

        if sel_tab == "📥 Bandeja de Aprobación":
            col_t, col_btn = st.columns([7, 3])
            with col_btn:
                if st.button(
                    "🔄 Actualizar Bandeja",
                    use_container_width=True,
                    key="btn_refresh_coord",
                    help="Obtener nuevas solicitudes sin recargar toda la página",
                ):
                    st.rerun()
                st.caption(
                    f"Última actualización: {datetime.now().strftime('%H:%M:%S')}"
                )

            if user["role"] == "coordinador":
                managed_dept_str = user.get("managed_department", "")
                cond_serv_gen = (
                    "OR (ua.emp_subarea = 'Servicios Generales')"
                    if "Calidad" in managed_dept_str
                    else ""
                )
                cond_orientador = (
                    "OR (ua.emp_subarea = 'Orientador')"
                    if "Seguridad" in managed_dept_str
                    else ""
                )
                cond_zarzal = (
                    f"OR (lr.user_id IN ({','.join(['%s'] * len(ZARZAL_EMPLOYEES))}))"
                    if user["username"] == "111644844"
                    else ""
                )

                query = f"""
                    SELECT lr.id, lr.user_id, e.full_name, lr.request_date, lr.leave_date_start, lr.leave_date_end,
                           lr.start_time, lr.end_time, lr.total_time,
                           lr.reason_type, lr.reason_description, lr.is_paid, lr.status, lr.attachment_path, lr.specific_dates, lr.how_to_makeup
                    FROM leave_requests lr
                    JOIN employees e ON lr.user_id = e.user_id
                    LEFT JOIN users_app ua ON lr.user_id = ua.username
                    WHERE lr.status = 'PENDING_COORD' AND 
                          (
                              %s LIKE '%%' || ua.emp_subarea || '%%'
                              {cond_serv_gen}
                              {cond_orientador}
                              {cond_zarzal}
                          )
                    ORDER BY lr.leave_date_start ASC, lr.id ASC
                """
                params = (managed_dept_str,)
                if user["username"] == "111644844":
                    params += tuple(ZARZAL_EMPLOYEES)
            elif user.get("managed_area", "") == "Control Interno":
                query = """
                    SELECT lr.id, lr.user_id, e.full_name, lr.request_date, lr.leave_date_start, lr.leave_date_end,
                           lr.start_time, lr.end_time, lr.total_time,
                           lr.reason_type, lr.reason_description, lr.is_paid, lr.status, lr.attachment_path, lr.specific_dates, lr.how_to_makeup,
                           ua.emp_area, ua.emp_subarea,
                           (SELECT full_name FROM users_app WHERE username = lr.approved_by_coord) as coord_name,
                           (SELECT full_name FROM users_app WHERE username = lr.approved_by_rrhh) as rrhh_name
                    FROM leave_requests lr
                    JOIN employees e ON lr.user_id = e.user_id
                    LEFT JOIN users_app ua ON lr.user_id = ua.username
                    WHERE lr.status = 'PENDING_JEFE'
                    ORDER BY lr.leave_date_start ASC, lr.id ASC
                """
                params = ()
            else:
                query = f"""
                    SELECT lr.id, lr.user_id, e.full_name, lr.request_date, lr.leave_date_start, lr.leave_date_end,
                           lr.start_time, lr.end_time, lr.total_time,
                           lr.reason_type, lr.reason_description, lr.is_paid, lr.status, lr.attachment_path, lr.specific_dates, lr.how_to_makeup,
                           ua.emp_area, ua.emp_subarea,
                           (SELECT full_name FROM users_app WHERE username = lr.approved_by_coord) as coord_name,
                           (SELECT full_name FROM users_app WHERE username = lr.approved_by_rrhh) as rrhh_name
                    FROM leave_requests lr
                    JOIN employees e ON lr.user_id = e.user_id
                    LEFT JOIN users_app ua ON lr.user_id = ua.username
                    WHERE lr.status = 'PENDING_JEFE' AND 
                          (
                              (ua.username IN ('119279359', '111627893') AND %s = 'Administrativo') OR
                              (ua.username NOT IN ('119279359', '111627893') AND ua.emp_area = %s AND ua.emp_subarea NOT IN ('Admisiones', 'Enfermería', 'Rehabilitación', 'Tecnólogo Rayos X', 'Auditor Médico', 'Medico', 'Farmacia', 'Control Interno', 'Cirugía', 'Mantenimiento', 'Seguridad', 'Orientador')) OR 
                              (ua.emp_subarea IN ('Rehabilitación', 'Tecnólogo Rayos X', 'Farmacia', 'Mantenimiento', 'Seguridad', 'Orientador') AND %s = 'Administrativo') OR
                              (ua.emp_subarea = 'Admisiones' AND %s = 'Financiera') OR
                              (lr.user_id IN ({",".join(["%s"] * len(ZARZAL_EMPLOYEES))}) AND %s = 'Administrativo')
                          )
                    ORDER BY lr.leave_date_start ASC, lr.id ASC
                """
                params = (
                    user.get("managed_area", ""),
                    user.get("managed_area", ""),
                    user.get("managed_area", ""),
                    user.get("managed_area", ""),
                )
                params += tuple(ZARZAL_EMPLOYEES)
                params += (user.get("managed_area", ""),)

            with db_session() as conn:
                df_pend = get_cached_dataframe(query, params=params)

            if df_pend.empty:
                st.toast("No hay solicitudes pendientes de revisión para tu área.")
            else:
                # --- Filtros Interactivos para Coordinadores/Jefes ---
                with st.expander("🔍 Buscar y Filtrar Pendientes", expanded=False):
                    f_col1, f_col2, f_col3 = st.columns(3)
                    with f_col1:
                        f_name = st.text_input(
                            "Buscar Empleado (Nombre o ID)", key="f_pend_name_coord"
                        )
                    with f_col2:
                        f_types = sorted(
                            df_pend["reason_type"].dropna().unique().tolist()
                        )
                        f_sel_types = st.multiselect(
                            "Filtrar por Tipo",
                            options=f_types,
                            key="f_pend_types_coord",
                        )
                    with f_col3:
                        # Rango de fechas
                        f_date_range = st.date_input(
                            "Rango de Fechas (Inicio y Fin)",
                            value=[],
                            key="f_pend_dates_coord",
                        )

                # Aplicar filtros dinámicos
                df_filtered = df_pend.copy()
                if f_name.strip():
                    term = f_name.strip().lower()
                    df_filtered = df_filtered[
                        (
                            df_filtered["full_name"]
                            .astype(str)
                            .str.lower()
                            .str.contains(term)
                        )
                        | (
                            df_filtered["user_id"]
                            .astype(str)
                            .str.lower()
                            .str.contains(term)
                        )
                    ]
                if f_sel_types:
                    df_filtered = df_filtered[
                        df_filtered["reason_type"].isin(f_sel_types)
                    ]

                if isinstance(f_date_range, (list, tuple)) and len(f_date_range) > 0:
                    start_f = f_date_range[0]
                    end_f = f_date_range[1] if len(f_date_range) > 1 else start_f

                    df_filtered["temp_date_start"] = pd.to_datetime(
                        df_filtered["leave_date_start"]
                    ).dt.date
                    df_filtered["temp_date_end"] = pd.to_datetime(
                        df_filtered["leave_date_end"]
                    ).dt.date
                    df_filtered = df_filtered[
                        (df_filtered["temp_date_end"] >= start_f)
                        & (df_filtered["temp_date_start"] <= end_f)
                    ]
                    df_filtered = df_filtered.drop(
                        columns=["temp_date_start", "temp_date_end"]
                    )

                if df_filtered.empty:
                    st.warning(
                        "⚠️ No se encontraron solicitudes con los filtros seleccionados."
                    )
                else:
                    if user.get("managed_area", "") == "Control Interno":
                        st.write(
                            f"Tienes **{len(df_filtered)}** solicitud(es) por revisar en total."
                        )

                        areas = sorted(
                            df_filtered["emp_area"].dropna().unique().tolist()
                        )
                        subareas = sorted(
                            df_filtered["emp_subarea"].dropna().unique().tolist()
                        )

                        c1, c2 = st.columns(2)
                        with c1:
                            filter_area = st.selectbox(
                                "Filtrar por Área", ["Todas"] + areas
                            )
                        with c2:
                            filter_subarea = st.selectbox(
                                "Filtrar por Sub-área", ["Todas"] + subareas
                            )

                        if filter_area != "Todas":
                            df_filtered = df_filtered[
                                df_filtered["emp_area"] == filter_area
                            ]
                        if filter_subarea != "Todas":
                            df_filtered = df_filtered[
                                df_filtered["emp_subarea"] == filter_subarea
                            ]

                        st.write(
                            f"Mostrando **{len(df_filtered)}** solicitud(es) filtrada(s)."
                        )
                    else:
                        st.write(
                            f"Tienes **{len(df_filtered)}** solicitud(es) por revisar."
                        )

                    for _, r in df_filtered.iterrows():
                        with st.container(border=True, key=f"container_pend_{r['id']}"):
                            cols = st.columns([3, 1])
                            with cols[0]:
                                icon = get_reason_icon(r["reason_type"])
                                st.markdown(
                                    f"**{r['full_name']}** (ID: {r['user_id']}) - *{icon} {r['reason_type']}*"
                                )
                                st.write(
                                    f"**Fechas:** {r['leave_date_start']} al {r['leave_date_end']} | **Remunerado:** {'Sí' if r['is_paid'] else 'No'}"
                                )
                                if pd.notna(r.get("start_time")) and r.get(
                                    "start_time"
                                ):
                                    st.write(
                                        f"**Horario:** {r['start_time']} a {r['end_time']} | **Tiempo Total:** {r['total_time']}"
                                    )
                                if "coord_name" in r and pd.notna(r["coord_name"]):
                                    st.info(
                                        f"✅ **Visto Bueno Previo:** Coordinador {r['coord_name']}"
                                    )
                                if "rrhh_name" in r and pd.notna(r["rrhh_name"]):
                                    st.info(
                                        f"✅ **Revisado por RRHH:** {r['rrhh_name']}"
                                    )
                                st.write(
                                    f"**Justificación:** {r['reason_description']}"
                                )
                                if (
                                    pd.notna(r.get("how_to_makeup"))
                                    and str(r["how_to_makeup"]).strip()
                                ):
                                    st.warning(
                                        f"**Acuerdo de Reposición (Tiempo):** {r['how_to_makeup']}"
                                    )

                                # --- Llamada al detector de conflictos ---
                                managed_entity = (
                                    user.get("managed_department")
                                    if user["role"] == "coordinador"
                                    else user.get("managed_area")
                                )
                                check_schedule_conflicts(
                                    r, user["role"], managed_entity
                                )

                                if r["attachment_path"]:
                                    import os

                                    from database_conn.connection import DATA_DIR

                                    file_path = os.path.join(
                                        DATA_DIR, "uploads", r["attachment_path"]
                                    )
                                    if os.path.exists(file_path):
                                        if st.button(
                                            "👁️ Ver Soporte Adjunto",
                                            key=f"btn_preview_{r['id']}",
                                            use_container_width=True,
                                        ):
                                            preview_attachment_dialog(
                                                r["attachment_path"], r["full_name"]
                                            )
                            with cols[1]:
                                st.button(
                                    "👍 Aprobar",
                                    key=f"btn_acc_{r['id']}",
                                    type="primary",
                                    use_container_width=True,
                                    on_click=handle_approve_callback,
                                    args=(r.to_dict(), user),
                                )

                                if st.button(
                                    "❌ Rechazar",
                                    key=f"btn_rej_{r['id']}",
                                    use_container_width=True,
                                ):
                                    st.session_state[
                                        f"show_rejection_dialog_{r['id']}"
                                    ] = True
                                    st.rerun()

                                if st.session_state.get(
                                    f"show_rejection_dialog_{r['id']}", False
                                ):
                                    rejection_reason_dialog(
                                        r["id"],
                                        r["user_id"],
                                        r["full_name"],
                                        r["reason_type"],
                                    )

        elif sel_tab == "📅 Calendario de Ausencias":
            render_absence_calendar(user)

        elif sel_tab == "🕰️ Historial de Decisiones":
            if user["role"] == "coordinador":
                managed_dept_str = user.get("managed_department", "")
                cond_serv_gen = (
                    "OR (ua.emp_subarea = 'Servicios Generales')"
                    if "Calidad" in managed_dept_str
                    else ""
                )
                cond_orientador = (
                    "OR (ua.emp_subarea = 'Orientador')"
                    if "Seguridad" in managed_dept_str
                    else ""
                )

                query_hist = f"""
                    SELECT lr.id, lr.user_id, e.full_name, lr.request_date, lr.leave_date_start, lr.leave_date_end,
                           lr.reason_type, lr.status
                    FROM leave_requests lr
                    JOIN employees e ON lr.user_id = e.user_id
                    JOIN users_app ua ON lr.user_id = ua.username
                    WHERE lr.approved_by_coord = %s 
                       OR (lr.status = 'REJECTED' AND (
                              %s LIKE '%%' || ua.emp_subarea || '%%'
                              {cond_serv_gen}
                              {cond_orientador}
                          ))
                    ORDER BY lr.id DESC
                """
                params_hist = [user["username"], managed_dept_str]
            else:
                query_hist = """
                    SELECT lr.id, lr.user_id, e.full_name, lr.request_date, lr.leave_date_start, lr.leave_date_end,
                           lr.reason_type, lr.status
                    FROM leave_requests lr
                    JOIN employees e ON lr.user_id = e.user_id
                    JOIN users_app ua ON lr.user_id = ua.username
                    WHERE lr.approved_by_jefe = %s
                       OR (lr.status = 'REJECTED' AND (
                              ua.emp_area = %s OR 
                              (ua.emp_subarea = 'Admisiones' AND %s = 'Financiera') OR
                              (ua.emp_subarea = 'Auditor Médico' AND %s = 'Auditoria Médica') OR
                              (ua.emp_subarea = 'Control Interno' AND %s = 'Control Interno')
                          ))
                    ORDER BY lr.id DESC
                """
                params_hist = (
                    user["username"],
                    user.get("managed_area", ""),
                    user.get("managed_area", ""),
                    user.get("managed_area", ""),
                    user.get("managed_area", ""),
                )

            with db_session() as conn:
                df_hist = get_cached_dataframe(query_hist, params=params_hist)

            if df_hist.empty:
                st.info("Aún no has procesado ninguna solicitud.")
            else:
                st.write(
                    f"Has procesado **{len(df_hist)}** solicitud(es) históricamente."
                )

                df_hist["Fechas"] = df_hist.apply(
                    lambda r: (
                        r["specific_dates"].replace(",", ", ")
                        if "specific_dates" in r
                        and pd.notna(r["specific_dates"])
                        and str(r["specific_dates"]).strip() != "None"
                        and str(r["specific_dates"]).strip() != ""
                        else (
                            r["leave_date_start"]
                            if r["leave_date_start"] == r["leave_date_end"]
                            else f"{r['leave_date_start']} al {r['leave_date_end']}"
                        )
                    ),
                    axis=1,
                )

                display_df = df_hist[
                    [
                        "id",
                        "user_id",
                        "full_name",
                        "Fechas",
                        "reason_type",
                        "status",
                        "request_date",
                    ]
                ]
                display_df.columns = [
                    "Radicado",
                    "DNI",
                    "Empleado",
                    "Fechas",
                    "Tipo",
                    "Estado de Aprobación",
                    "Fecha Solicitud",
                ]

                st.info(
                    "💡 Haz clic en cualquier fila para ver los detalles completos."
                )

                if "last_processed_hist" not in st.session_state:
                    st.session_state.last_processed_hist = None

                event_h = st.dataframe(
                    display_df,
                    use_container_width=True,
                    hide_index=True,
                    on_select="rerun",
                    selection_mode="single-row",
                    key="admin_hist_table",
                )

                if len(event_h.selection.rows) > 0:
                    row_idx = event_h.selection.rows[0]
                    if row_idx < len(display_df):
                        req_id = int(display_df.iloc[row_idx]["Radicado"])

                        if req_id != st.session_state.last_processed_hist:
                            st.session_state.last_processed_hist = req_id
                            show_leave_request_details(req_id)
                    else:
                        st.session_state.last_processed_hist = None
                else:
                    st.session_state.last_processed_hist = None

        return

    st.write(
        "Registra permisos, incapacidades médicas o vacaciones. El sistema **no penalizará** a estos empleados en los reportes de tardanzas para los días seleccionados."
    )

    from database_conn.queries import get_cached_employees

    emp_df = get_cached_employees()

    if emp_df.empty:
        st.warning("No hay empleados en el directorio.")
        return

    # --- PERSISTENCIA DE PESTAÑA ACTIVA ---
    if "tab_sel" in st.query_params:
        try:
            st.session_state["exceptions_active_tab"] = int(st.query_params["tab_sel"])
        except Exception:
            pass
        st.query_params.pop("tab_sel", None)

    default_tab_idx = 0
    if "exceptions_active_tab" in st.session_state:
        default_tab_idx = st.session_state["exceptions_active_tab"]

    tab_options = [
        "📝 Registrar Novedad Manual",
        "📋 Listado de Novedades",
        "📥 Solicitudes Digitales de Empleados",
        "📅 Calendario de Ausencias",
        "🌐 Monitoreo Global",
        "🏢 Trámites en Línea",
    ]
    sel_tab = option_menu(
        menu_title=None,
        options=tab_options,
        icons=["pencil", "list-task", "download", "calendar", "globe", "file-earmark-pdf"],
        menu_icon="cast",
        default_index=default_tab_idx,
        orientation="horizontal",
        key="admin_tabs_menu",
        styles={
            "container": {
                "padding": "0!important",
                "background-color": "transparent",
                "border": "none",
            },
            "icon": {"color": "#0066cc", "font-size": "14px"},
            "nav-link": {
                "font-size": "14px",
                "text-align": "center",
                "margin": "0px",
                "--hover-color": "rgba(128,128,128,0.15)",
            },
            "nav-link-selected": {
                "background-color": "#0066cc",
                "color": "white",
                "icon-color": "white",
            },
        },
    )
    st.session_state["exceptions_active_tab"] = tab_options.index(sel_tab)

    if sel_tab == "📝 Registrar Novedad Manual":
        with st.form("form_exceptions"):
            col1, col2 = st.columns(2)
            with col1:
                selected_emp = st.selectbox(
                    "Empleado",
                    options=emp_df["user_id"].tolist(),
                    format_func=lambda uid: (
                        f"{uid} - {emp_df[emp_df['user_id'] == uid]['full_name'].values[0]}"
                    ),
                )
                date_range = st.date_input(
                    "Rango de Fechas (Inicio y Fin)",
                    value=[],
                    help="Escoge el día de inicio y fin de la novedad.",
                )
            with col2:
                exc_type = st.selectbox(
                    "Tipo de Novedad",
                    [
                        "Incapacidad Médica",
                        "Vacaciones",
                        "Permiso Remunerado",
                        "Permiso NO Remunerado",
                        "Licencia Luto/Maternidad",
                    ],
                )
                notes = st.text_area(
                    "Observaciones",
                    placeholder="Escribe detalles del permiso si es necesario...",
                )

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
                st.toast(
                    f"Novedad registrada del {d_start} al {d_end} para el usuario {selected_emp}."
                )

    elif sel_tab == "📋 Listado de Novedades":
        col_t, col_btn = st.columns([8, 2])
        with col_t:
            st.info(
                "💡 Haz clic en cualquier fila para ver los detalles completos del permiso o novedad."
            )
        with col_btn:
            if st.button(
                "🔄 Actualizar Lista",
                use_container_width=True,
                key="btn_refresh_admin",
                help="Obtener novedades recientes sin recargar la página",
            ):
                st.rerun()
            st.caption(f"Última actualización: {datetime.now().strftime('%H:%M:%S')}")

        df_exc = get_exceptions_df()
        if df_exc.empty:
            st.info("No hay novedades registradas.")
        else:
            df_exc.columns = [
                "ID",
                "Usuario",
                "Nombre",
                "Fecha Inicio",
                "Fecha Fin",
                "Total Días",
                "Tipo",
                "Observaciones",
                "Registrado El",
            ]

            # Reorganizar columnas para que Total Días quede junto a las fechas (Ya viene así de queries)
            df_exc = df_exc[
                [
                    "ID",
                    "Usuario",
                    "Nombre",
                    "Fecha Inicio",
                    "Fecha Fin",
                    "Total Días",
                    "Tipo",
                    "Observaciones",
                    "Registrado El",
                ]
            ]

            # Inicializar variables de estado para los filtros si no existen
            if "filter_exc_name" not in st.session_state:
                st.session_state.filter_exc_name = ""
            if "filter_exc_type" not in st.session_state:
                st.session_state.filter_exc_type = []
            if "filter_exc_dates" not in st.session_state:
                st.session_state.filter_exc_dates = [
                    date.today() - timedelta(days=30),
                    date.today(),
                ]

            # --- Buscador y Filtros Avanzados ---
            with st.expander("🔍 Buscador y Filtros Avanzados", expanded=False):
                col_f1, col_f2, col_f3 = st.columns(3)
                with col_f1:
                    filter_name = st.text_input(
                        "Buscar Empleado (Cédula o Nombre)", key="filter_exc_name"
                    )
                with col_f2:
                    # Extraer tipos únicos existentes en la base de datos
                    existing_types = sorted(list(df_exc["Tipo"].dropna().unique()))
                    selected_types = st.multiselect(
                        "Filtrar por Tipo de Novedad",
                        options=existing_types,
                        key="filter_exc_type",
                    )
                with col_f3:
                    # Rango de fechas
                    filter_dates = st.date_input(
                        "Rango de Fechas (Novedad)",
                        key="filter_exc_dates",
                        help="Filtrar novedades que ocurran dentro de este periodo.",
                    )

                # Botón para limpiar filtros
                col_clear, _ = st.columns([1, 2])
                with col_clear:
                    if st.button("🧹 Limpiar Filtros", use_container_width=True):
                        st.session_state.filter_exc_name = ""
                        st.session_state.filter_exc_type = []
                        st.session_state.filter_exc_dates = [
                            date.today() - timedelta(days=30),
                            date.today(),
                        ]
                        st.rerun()

            # Aplicar filtros dinámicos en el DataFrame
            filtered_df = df_exc.copy()

            # 1. Filtro por nombre o cédula
            if filter_name.strip():
                term = filter_name.strip().lower()
                filtered_df = filtered_df[
                    (filtered_df["Usuario"].astype(str).str.lower().str.contains(term))
                    | (filtered_df["Nombre"].astype(str).str.lower().str.contains(term))
                ]

            # 2. Filtro por tipo de novedad
            if selected_types:
                filtered_df = filtered_df[filtered_df["Tipo"].isin(selected_types)]

            # 3. Filtro por rango de fechas
            if isinstance(filter_dates, (list, tuple)) and len(filter_dates) > 0:
                start_f = filter_dates[0]
                end_f = filter_dates[1] if len(filter_dates) > 1 else start_f

                # Convertir la columna Fecha a datetime.date para comparar correctamente
                filtered_df["temp_date"] = pd.to_datetime(
                    filtered_df["Fecha Inicio"]
                ).dt.date
                filtered_df = filtered_df[
                    (filtered_df["temp_date"] >= start_f)
                    & (filtered_df["temp_date"] <= end_f)
                ]
                filtered_df = filtered_df.drop(columns=["temp_date"])

            # Mostrar la tabla filtrada
            if filtered_df.empty:
                st.warning(
                    "⚠️ No se encontraron novedades con los filtros seleccionados."
                )
            else:
                # Mostrar KPIs y Resumen
                st.markdown("#### 📊 Resumen del Listado Filtrado")
                kpi_col1, kpi_col2, kpi_col3 = st.columns(3)
                with kpi_col1:
                    st.metric("Total Novedades", len(filtered_df))
                with kpi_col2:
                    # Tipo más frecuente
                    most_common = filtered_df["Tipo"].mode()
                    most_common_str = (
                        most_common.iloc[0] if not most_common.empty else "N/A"
                    )
                    st.metric("Tipo más Frecuente", most_common_str)
                with kpi_col3:
                    # Empleados afectados
                    unique_emps = filtered_df["Usuario"].nunique()
                    st.metric("Empleados Afectados", unique_emps)

                st.divider()
                dl_col1, dl_col2 = st.columns([1, 1])
                with dl_col1:
                    st.write(f"Mostrando **{len(filtered_df)}** novedades.")
                    st.caption(
                        "Usa el botón de la derecha para descargar en formato Excel correcto."
                    )
                with dl_col2:
                    import io

                    excel_buffer = io.BytesIO()
                    try:
                        filtered_df.to_excel(
                            excel_buffer, index=False, engine="openpyxl"
                        )
                        excel_buffer.seek(0)
                        st.download_button(
                            label="📥 Descargar a Excel (.xlsx)",
                            data=excel_buffer,
                            file_name=f"Listado_Novedades_{datetime.now().strftime('%Y%m%d')}.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            use_container_width=True,
                        )
                    except Exception as e:
                        st.error(
                            f"No se pudo generar Excel. Requiere openpyxl. (Error: {e})"
                        )
                        
                    # CSV Export
                    csv_data = filtered_df.to_csv(index=False, sep=";").encode("utf-8-sig")
                    st.download_button(
                        label="📥 Descargar a CSV (.csv)",
                        data=csv_data,
                        file_name=f"Listado_Novedades_{datetime.now().strftime('%Y%m%d')}.csv",
                        mime="text/csv",
                        use_container_width=True,
                    )

                # =========================================================================
                # PREVENCIÓN ANTIGUOS POPUPS FANTASMAS (BUG "DOBLE CLIC")
                # =========================================================================
                if "last_processed_exc" not in st.session_state:
                    st.session_state.last_processed_exc = None

                event = st.dataframe(
                    filtered_df,
                    use_container_width=True,
                    hide_index=True,
                    on_select="rerun",
                    selection_mode="single-row",
                    key="admin_exc_table",
                )

                if len(event.selection.rows) > 0:
                    row_idx = event.selection.rows[0]
                    if row_idx < len(filtered_df):
                        selected_id = int(filtered_df.iloc[row_idx]["ID"])

                        if selected_id != st.session_state.last_processed_exc:
                            st.session_state.last_processed_exc = selected_id
                            show_exception_details(selected_id)
                    else:
                        st.session_state.last_processed_exc = None
                else:
                    st.session_state.last_processed_exc = None

    elif sel_tab == "📥 Solicitudes Digitales de Empleados":
        st.write(
            "Gestiona la revisión final de las incapacidades, permisos y vacaciones tramitadas por el personal."
        )

        col_t, col_btn = st.columns([8, 2])
        with col_btn:
            if st.button(
                "🔄 Actualizar Bandeja",
                use_container_width=True,
                key="btn_refresh_admin",
            ):
                st.rerun()

        with db_session() as conn:
            pass # Keep block valid if needed, or remove later
        df_pend_active = get_cached_dataframe(
            """
            SELECT lr.id, lr.user_id, e.full_name, lr.request_date, lr.leave_date_start, lr.leave_date_end,
                   lr.start_time, lr.end_time, lr.total_time,
                   lr.reason_type, lr.reason_description, lr.is_paid, lr.status, lr.attachment_path, lr.specific_dates, lr.how_to_makeup,
                   (SELECT full_name FROM users_app WHERE username = lr.approved_by_coord) as coord_name,
                   (SELECT full_name FROM users_app WHERE username = lr.approved_by_jefe) as jefe_name
            FROM leave_requests lr
            JOIN employees e ON lr.user_id = e.user_id
            WHERE lr.status IN ('PENDING_RRHH', 'PENDING_COORD', 'PENDING_JEFE')
            ORDER BY lr.id DESC
        """
        )
        df_pend_done = get_cached_dataframe(
            """
            SELECT lr.id, lr.user_id, e.full_name, lr.request_date, lr.leave_date_start, lr.leave_date_end,
                   lr.start_time, lr.end_time, lr.total_time,
                   lr.reason_type, lr.reason_description, lr.is_paid, lr.status, lr.attachment_path, lr.specific_dates, lr.how_to_makeup,
                   (SELECT full_name FROM users_app WHERE username = lr.approved_by_coord) as coord_name,
                   (SELECT full_name FROM users_app WHERE username = lr.approved_by_jefe) as jefe_name
            FROM leave_requests lr
            JOIN employees e ON lr.user_id = e.user_id
            WHERE lr.status IN ('APPROVED', 'REJECTED')
            ORDER BY lr.id DESC
            LIMIT 150
        """
        )
        df_pend = pd.concat([df_pend_active, df_pend_done], ignore_index=True)
        if not df_pend.empty:
            df_pend = df_pend.sort_values(by="id", ascending=False)

        if df_pend.empty:
            st.toast("No hay solicitudes pendientes de revisión final.")
        else:
            # --- Buscador Inteligente Siempre Visible ---
            st.markdown(
                "<h4 style='color: #4f46e5; margin-bottom: 5px;'>Buscador Inteligente</h4>",
                unsafe_allow_html=True,
            )
            f_col1, f_col2, f_col3 = st.columns([1.5, 1, 1])
            with f_col1:
                f_name_rrhh = st.text_input(
                    "🔍 Buscar por Nombre o Cédula",
                    placeholder="Escribe aquí...",
                    key="f_pend_name_rrhh",
                )
            with f_col2:
                f_types_rrhh = sorted(df_pend["reason_type"].dropna().unique().tolist())
                f_sel_types_rrhh = st.multiselect(
                    "🏷️ Filtrar por Tipo", options=f_types_rrhh, key="f_pend_types_rrhh"
                )
            with f_col3:
                f_date_range_rrhh = st.date_input(
                    "📅 Rango de Fechas (Inicio y Fin)",
                    value=[],
                    key="f_pend_dates_rrhh",
                )
            st.markdown(
                "<hr style='margin-top: 10px; margin-bottom: 20px; border-color: rgba(255,255,255,0.1);'>",
                unsafe_allow_html=True,
            )

            # Aplicar filtros dinámicos
            df_filtered = df_pend.copy()
            if f_name_rrhh.strip():
                term = f_name_rrhh.strip().lower()
                df_filtered = df_filtered[
                    (
                        df_filtered["full_name"]
                        .astype(str)
                        .str.lower()
                        .str.contains(term)
                    )
                    | (
                        df_filtered["user_id"]
                        .astype(str)
                        .str.lower()
                        .str.contains(term)
                    )
                ]
            if f_sel_types_rrhh:
                df_filtered = df_filtered[
                    df_filtered["reason_type"].isin(f_sel_types_rrhh)
                ]

            if (
                isinstance(f_date_range_rrhh, (list, tuple))
                and len(f_date_range_rrhh) > 0
            ):
                start_f = f_date_range_rrhh[0]
                end_f = f_date_range_rrhh[1] if len(f_date_range_rrhh) > 1 else start_f

                df_filtered["temp_date_start"] = pd.to_datetime(
                    df_filtered["leave_date_start"]
                ).dt.date
                df_filtered["temp_date_end"] = pd.to_datetime(
                    df_filtered["leave_date_end"]
                ).dt.date
                df_filtered = df_filtered[
                    (df_filtered["temp_date_end"] >= start_f)
                    & (df_filtered["temp_date_start"] <= end_f)
                ]
                df_filtered = df_filtered.drop(
                    columns=["temp_date_start", "temp_date_end"]
                )

            if df_filtered.empty:
                st.warning(
                    "⚠️ No se encontraron solicitudes con los filtros seleccionados."
                )
            else:
                st.subheader("🗂️ Tablero de Gestión de Solicitudes")

                # --- Píldoras de Filtrado Rápido (Quick Filters) ---
                # Aplicamos CSS para convertir el Radio nativo en botones tipo "Píldoras"
                st.markdown(
                    """
                <style>
                /* Estilizar el contenedor de radio buttons horizontal */
                div[role="radiogroup"] {
                    gap: 10px;
                    margin-bottom: 20px;
                }
                /* Hacer que los radio elements parezcan pastillas */
                div[role="radiogroup"] > label {
                    background: var(--glass-bg);
                    border: 1px solid var(--glass-border);
                    padding: 8px 16px !important;
                    border-radius: 25px !important;
                    cursor: pointer;
                    transition: all 0.3s ease;
                }
                div[role="radiogroup"] > label:hover {
                    border-color: rgba(99, 102, 241, 0.5);
                    transform: translateY(-2px);
                }
                /* Ocultar el círculo nativo del radio (depende de la versión de Streamlit, ocultamos el primer hijo si es el circulo) */
                div[role="radiogroup"] > label > div:first-child:not([data-testid="stMarkdownContainer"]) {
                    display: none !important;
                }
                /* Si está seleccionado, cambiar fondo (hack visual) */
                div[role="radiogroup"] > label[data-checked="true"], 
                div[role="radiogroup"] > label:has(input:checked) {
                    background: rgba(99, 102, 241, 0.15);
                    border-color: #6366f1;
                }
                </style>
                """,
                    unsafe_allow_html=True,
                )

                q_filter = st.radio(
                    "Filtros Rápidos",
                    options=[
                        "Todas",
                        "🏖️ Vacaciones",
                        "🏥 Incapacidad",
                        "⚠️ Permiso Personal",
                        "💼 Licencia",
                    ],
                    horizontal=True,
                    label_visibility="collapsed",
                )

                # Aplicar el filtro de la pastilla
                if q_filter != "Todas":
                    kw = q_filter.split(" ", 1)[1].strip()  # Ej: "Vacaciones"
                    df_filtered = df_filtered[
                        df_filtered["reason_type"].str.contains(
                            kw, case=False, na=False
                        )
                    ]

                if df_filtered.empty:
                    st.info(f"No hay solicitudes en la categoría: {q_filter}")

                col_k1, col_k2, col_k3 = st.columns(3)

                df_rrhh = df_filtered[df_filtered["status"] == "PENDING_RRHH"]
                df_jefe = df_filtered[
                    df_filtered["status"].isin(["PENDING_COORD", "PENDING_JEFE"])
                ]
                df_done = df_filtered[
                    df_filtered["status"].isin(["APPROVED", "REJECTED"])
                ].head(30)

                def render_card(r, mode):
                    with st.container(border=True, key=f"kanban_{r['id']}"):
                        icon = get_reason_icon(r["reason_type"])

                        # Definir colores de los badges
                        color_map = {
                            "Vacaciones": "background-color: rgba(13, 110, 253, 0.15); color: #60a5fa; border: 1px solid rgba(13, 110, 253, 0.3);",
                            "Incapacidad": "background-color: rgba(220, 53, 69, 0.15); color: #f87171; border: 1px solid rgba(220, 53, 69, 0.3);",
                        }
                        badge_style = color_map.get(
                            r["reason_type"],
                            "background-color: rgba(25, 135, 84, 0.15); color: #4ade80; border: 1px solid rgba(25, 135, 84, 0.3);",
                        )

                        st.markdown(
                            f"""
                        <div style="display: flex; justify-content: space-between; align-items: flex-start; margin-bottom: 8px;">
                            <strong style="font-size: 1.1rem; line-height: 1.2;">{r["full_name"]}</strong>
                            <span style="font-size: 0.75rem; padding: 3px 8px; border-radius: 12px; font-weight: 600; white-space: nowrap; margin-left: 10px; {badge_style}">
                                {icon} {r["reason_type"]}
                            </span>
                        </div>
                        <div style="color: #94a3b8; font-size: 0.85rem; margin-bottom: 12px;">
                            <span style="margin-right: 15px;">📅 <b>Inicio:</b> {r["leave_date_start"]}</span>
                        </div>
                        """,
                            unsafe_allow_html=True,
                        )

                        # Botón para ver info detallada
                        if st.button(
                            "🔍 Ver Soportes",
                            key=f"k_det_{r['id']}",
                            use_container_width=True,
                        ):
                            from views.employee_portal_view import (
                                show_leave_request_details,
                            )

                            show_leave_request_details(r["id"])

                        if mode == "rrhh":
                            requiere_jefe_tipo = r["reason_type"] in [
                                "Vacaciones",
                                "Calamidad Doméstica",
                                "Licencia de Luto",
                                "Licencia de Paternidad",
                                "Licencia por Votación",
                                "Licencia por Jurado de Votación",
                                "Licencia Remunerada",
                                "Licencia No Remunerada",
                            ]
                            is_special_user = str(r["user_id"]) in [
                                "119279359",
                                "111627893",
                            ]
                            if is_special_user and r["reason_type"] in [
                                "Permiso Personal",
                                "Permiso Laboral",
                            ]:
                                requiere_jefe = False
                            else:
                                requiere_jefe = requiere_jefe_tipo or (
                                    pd.isna(r.get("coord_name"))
                                    and r["reason_type"] != "Incapacidad"
                                )

                            if (
                                pd.notna(r.get("jefe_name"))
                                and str(r.get("jefe_name")).strip()
                            ):
                                requiere_jefe = False

                            btn_label = (
                                "✅ Enviar a Jefe" if requiere_jefe else "✅ Aprobar"
                            )
                            if st.button(
                                btn_label,
                                key=f"k_hr_{r['id']}",
                                type="primary",
                                use_container_width=True,
                            ):
                                from database_conn.queries import (
                                    db_approve_leave_request_rrhh,
                                )

                                if requiere_jefe:
                                    success = db_approve_leave_request_rrhh(
                                        r["id"],
                                        st.session_state["user"]["username"],
                                        is_final=False,
                                    )
                                    if not success:
                                        st.toast("La solicitud ya fue procesada.", icon="⚠️")
                                        st.rerun()
                                    # ... omitir correo para esta demo
                                else:
                                    success = db_approve_leave_request_rrhh(
                                        r["id"],
                                        st.session_state["user"]["username"],
                                        is_final=True,
                                    )
                                    if not success:
                                        st.toast("La solicitud ya fue procesada.", icon="⚠️")
                                        st.rerun()
                                        
                                    with db_session() as conn:
                                        cur = conn.cursor()
                                        d_start = date.fromisoformat(r["leave_date_start"])
                                        d_end = date.fromisoformat(r["leave_date_end"])
                                        
                                        dates_to_process = []
                                        if "specific_dates" in r and pd.notna(r["specific_dates"]) and str(r["specific_dates"]).strip() not in ["None", ""]:
                                            dates_to_process = [date.fromisoformat(d.strip()) for d in str(r["specific_dates"]).split(",") if d.strip()]
                                        else:
                                            delta = d_end - d_start
                                            dates_to_process = [d_start + timedelta(days=i) for i in range(delta.days + 1)]
                                            
                                        days_deducted = 0
                                        for curr_date in dates_to_process:
                                            if r["reason_type"] == "Vacaciones":
                                                if (
                                                    curr_date.weekday() == 6
                                                    or is_holiday(curr_date)
                                                ):
                                                    continue
                                            day_to_log = curr_date.isoformat()
                                            cur.execute(
                                                """
                                                INSERT INTO exceptions(user_id, date, type, notes, created_at)
                                                VALUES(%s,%s,%s,%s,%s)
                                                ON CONFLICT(user_id, date) DO UPDATE SET type=excluded.type, notes=excluded.notes
                                            """,
                                                (
                                                    r["user_id"],
                                                    day_to_log,
                                                    r["reason_type"],
                                                    f"Aprobado de Portal: {r['reason_description']}",
                                                    datetime.now().isoformat(
                                                        timespec="seconds"
                                                    ),
                                                ),
                                            )
                                            days_deducted += 1

                                        if (
                                            r["reason_type"] == "Vacaciones"
                                            and days_deducted > 0
                                        ):
                                            cur.execute(
                                                "UPDATE users_app SET vacation_balance = vacation_balance - %s WHERE username = %s",
                                                (days_deducted, r["user_id"]),
                                            )
                                    log_audit(
                                        "APPROVE_LEAVE_FINAL",
                                        f"Permiso #{r['id']} ({r['reason_type']}) de {r['full_name']} APROBADO FINAL por RRHH.",
                                    )
                                    notify_employee_status(
                                        r["user_id"],
                                        r["full_name"],
                                        r["id"],
                                        r["reason_type"],
                                        "APROBACIÓN FINAL",
                                        "Tu solicitud fue completamente aprobada por RRHH y registrada oficialmente en el sistema.",
                                        st.session_state["user"]["full_name"],
                                    )
                                st.rerun()

                            if st.button(
                                "❌ Rechazar",
                                key=f"k_rej_{r['id']}",
                                use_container_width=True,
                            ):
                                rejection_reason_dialog(
                                    r["id"],
                                    r["user_id"],
                                    r["full_name"],
                                    r["reason_type"],
                                )

                        elif mode == "jefe":
                            st.info(
                                f"⏳ Esperando a: {r['status'].replace('PENDING_', '')}"
                            )
                        else:
                            color = "green" if r["status"] == "APPROVED" else "red"
                            st.markdown(
                                f"<span style='color:{color}'><b>{r['status']}</b></span>",
                                unsafe_allow_html=True,
                            )

                            if st.session_state["user"]["role"] in [
                                "admin",
                                "nomina",
                            ] or (
                                st.session_state["user"]["role"] == "empleado"
                                and st.session_state["user"].get("emp_subarea")
                                in ["Nomina", "Talento humano"]
                            ):
                                if st.button(
                                    "↩️ Revertir a Pendiente",
                                    key=f"rev_hr_{r['id']}",
                                    use_container_width=True,
                                ):
                                    from database_conn.queries import (
                                        db_revert_leave_request,
                                    )

                                    success = db_revert_leave_request(
                                        r["id"], st.session_state["user"]["username"]
                                    )
                                    if success:
                                        st.toast(
                                            f"Permiso #{r['id']} devuelto a estado Pendiente."
                                        )
                                        st.rerun()

                with col_k1:
                    st.markdown(
                        f"<div style='background:rgba(255, 165, 0, 0.2); padding: 10px; border-radius:10px; text-align:center;'><b>🟠 Por Aprobar (RRHH) ({len(df_rrhh)})</b></div>",
                        unsafe_allow_html=True,
                    )
                    st.write("")
                    for _, r in df_rrhh.iterrows():
                        render_card(r, "rrhh")

                with col_k2:
                    st.markdown(
                        f"<div style='background:rgba(255, 255, 0, 0.1); padding: 10px; border-radius:10px; text-align:center;'><b>🟡 Pendiente Jefes ({len(df_jefe)})</b></div>",
                        unsafe_allow_html=True,
                    )
                    st.write("")
                    for _, r in df_jefe.iterrows():
                        render_card(r, "jefe")

                with col_k3:
                    st.markdown(
                        f"<div style='background:rgba(0, 255, 0, 0.1); padding: 10px; border-radius:10px; text-align:center;'><b>🟢 Completados ({len(df_done)})</b></div>",
                        unsafe_allow_html=True,
                    )
                    st.write("")
                    for _, r in df_done.iterrows():
                        render_card(r, "done")

    elif sel_tab == "📅 Calendario de Ausencias":
        render_absence_calendar(user)

    elif sel_tab == "🌐 Monitoreo Global":
        # Obtener el rol de la sesión actual
        user_role = st.session_state["user"]["role"]

        # Mapeo de rol efectivo a 'nomina' si el usuario es auxiliar de Nómina o Talento Humano
        if user_role == "empleado" and st.session_state["user"].get("emp_subarea") in [
            "Nomina",
            "Talento humano",
        ]:
            user_role = "nomina"

        # Solo administradores o rol nomina efectivo pueden ver el monitoreo global
        if user_role in ["admin", "nomina"]:
            st.subheader("Monitoreo Global de Permisos (Todas las Áreas)")
            st.info(
                "Vista exclusiva para directivos. Aquí observas el estado de **todas** las solicitudes en curso en toda la empresa."
            )

            df_g = get_cached_dataframe(
                """
                SELECT lr.id, lr.user_id, e.full_name, e.department, 
                       lr.leave_date_start, lr.leave_date_end,
                       lr.reason_type, lr.status, lr.request_date,
                       ua.emp_area, ua.emp_subarea, ua.role as user_role, ua.managed_department as user_managed_dept
                FROM leave_requests lr
                JOIN employees e ON lr.user_id = e.user_id
                LEFT JOIN users_app ua ON lr.user_id = ua.username
                WHERE lr.status LIKE 'PENDING_%'
                ORDER BY lr.request_date DESC
            """
            )

            with db_session() as conn:
                # Obtener coordinadores y jefes activos para mapeo dinámico de nombres
                cur = conn.cursor()
                cur.execute(
                    "SELECT full_name, managed_department FROM users_app WHERE role IN ('coordinador', 'nomina') AND active = 1"
                )
                coordinators = cur.fetchall()
                cur.execute(
                    "SELECT full_name, managed_area FROM users_app WHERE role = 'jefe_area' AND active = 1"
                )
                jefes = cur.fetchall()

            if df_g.empty:
                st.toast(
                    "Toda la tubería está limpia. No hay solicitudes estancadas."
                )
            else:
                st.write(
                    f"Hay **{len(df_g)}** solicitudes esperando aprobación en algún nivel."
                )

                # =========================================================================
                # COMPRESIÓN INTELIGENTE DE COLUMNAS (FUSIÓN DE FECHAS)
                # =========================================================================
                # Usamos una función anónima (Lambda) de Pandas. Si la persona
                # pidió permiso para un solo día (inicio = fin), imprimimos un día.
                # De lo contrario (vacaciones cruzadas), concatenamos con un " al ".
                # Esto ahorra un 20% del espacio invaluable en el ancho del monitor.
                df_g["Fechas"] = df_g.apply(
                    lambda r: (
                        r["specific_dates"].replace(",", ", ")
                        if "specific_dates" in r
                        and pd.notna(r["specific_dates"])
                        and str(r["specific_dates"]).strip() != "None"
                        and str(r["specific_dates"]).strip() != ""
                        else (
                            r["leave_date_start"]
                            if r["leave_date_start"] == r["leave_date_end"]
                            else f"{r['leave_date_start']} al {r['leave_date_end']}"
                        )
                    ),
                    axis=1,
                )

                # Calcular quién tiene pendiente la aprobación (Nombres reales)
                def get_pending_approver(row):
                    status = row["status"]
                    if status == "PENDING_RRHH":
                        return "Gestión Humana"
                    elif status == "PENDING_COORD":
                        subarea = row["emp_subarea"] or row["department"]
                        if not subarea:
                            return "Coordinador"
                        target_subarea = subarea
                        if target_subarea == "Servicios Generales":
                            target_subarea = "Calidad"
                        elif target_subarea == "Orientador":
                            target_subarea = "Seguridad"

                        matching_coords = []
                        for name, m_depts in coordinators:
                            m_depts_str = m_depts or ""
                            if target_subarea in m_depts_str or subarea in m_depts_str:
                                matching_coords.append(name)
                        if matching_coords:
                            return ", ".join(matching_coords)
                        return "Coordinador"
                    elif status == "PENDING_JEFE":
                        area = row["emp_area"]
                        subarea = row["emp_subarea"] or row["department"]
                        u_role = row["user_role"]
                        u_managed = row["user_managed_dept"]

                        target_jefe_area = area
                        if str(row["user_id"]) in ["119279359", "111627893"] or subarea in [
                            "Rehabilitación",
                            "Tecnólogo Rayos X",
                            "Farmacia",
                        ]:
                            target_jefe_area = "Administrativo"
                        elif subarea == "Admisiones":
                            target_jefe_area = "Financiera"
                        elif subarea in [
                            "Enfermería",
                            "Auditor Médico",
                            "Medico",
                            "Control Interno",
                            "Cirugía",
                        ]:
                            target_jefe_area = "Control Interno"
                        elif u_role == "coordinador" and u_managed:
                            c_depts = [
                                d.strip() for d in u_managed.split(",") if d.strip()
                            ]
                            if any(
                                dept in c_depts
                                for dept in [
                                    "Rehabilitación",
                                    "Tecnólogo Rayos X",
                                    "Farmacia",
                                ]
                            ):
                                target_jefe_area = "Administrativo"
                            elif "Admisiones" in c_depts:
                                target_jefe_area = "Financiera"
                            elif any(
                                dept in c_depts
                                for dept in [
                                    "Enfermería",
                                    "Auditor Médico",
                                    "Medico",
                                    "Control Interno",
                                    "Cirugía",
                                ]
                            ):
                                target_jefe_area = "Control Interno"

                        matching_jefes = []
                        for name, m_area in jefes:
                            if m_area == target_jefe_area:
                                matching_jefes.append(name)
                        if matching_jefes:
                            return ", ".join(matching_jefes)
                        return (
                            f"Jefe ({target_jefe_area})"
                            if target_jefe_area
                            else "Jefe de Área"
                        )
                    return status

                df_g["Pendiente de"] = df_g.apply(get_pending_approver, axis=1)

                display_df = df_g[
                    [
                        "id",
                        "user_id",
                        "full_name",
                        "department",
                        "Fechas",
                        "reason_type",
                        "status",
                        "Pendiente de",
                        "request_date",
                    ]
                ]
                display_df.columns = [
                    "Radicado",
                    "DNI",
                    "Empleado",
                    "Área/Departamento",
                    "Fechas",
                    "Tipo",
                    "Estado",
                    "Pendiente De",
                    "Fecha Solicitud",
                ]

                st.info(
                    "💡 Haz clic en cualquier fila para ver los detalles completos de la solicitud."
                )

                # Prevenidor de Popups Fantasmas replicado para esta tabla Global
                if "last_processed_global" not in st.session_state:
                    st.session_state.last_processed_global = None

                event_g = st.dataframe(
                    display_df,
                    use_container_width=True,
                    hide_index=True,
                    on_select="rerun",
                    selection_mode="single-row",
                    key="admin_global_table",
                )

                if len(event_g.selection.rows) > 0:
                    row_idx = event_g.selection.rows[0]
                    if row_idx < len(display_df):
                        req_id = int(display_df.iloc[row_idx]["Radicado"])

                        # Interceptor lógico: ¿Es un click fresco en una fila NUEVA o quitada?
                        # Si coincide con la memoria sucia, ignóralo para no interrumpir al Administrador.
                        if req_id != st.session_state.last_processed_global:
                            st.session_state.last_processed_global = req_id

                            # Invocación directa a la tarjeta visual que diseñamos en employee_portal (Modularidad)
                            show_leave_request_details(req_id)
                    else:
                        st.session_state.last_processed_global = None
                else:
                    # En caso de que el admnistrador un-clickee la fila para "cerrar" visualmente.
                    st.session_state.last_processed_global = None
        else:
            st.warning(
                "No tienes permisos de Administrador para ver la panorámica global de todas las áreas."
            )
            
    elif sel_tab == "🏢 Trámites en Línea":
        user_role = st.session_state["user"]["role"]
        if user_role == "empleado" and st.session_state["user"].get("emp_subarea") in ["Nomina", "Talento humano"]:
            user_role = "nomina"
            
        if user_role in ["admin", "nomina"]:
            st.header("🏢 Gestión de Trámites en Línea")
            st.info("Bandeja central de Trámites Radicados por Empleados (Cesantías, EPS, etc.)")
            
            from database_conn.queries import db_get_pending_hr_procedures, db_update_hr_procedure_status
            from services.email_service import send_hr_procedure_status_update
            
            pending_df = db_get_pending_hr_procedures()
            
            if pending_df.empty:
                st.toast("🎉 ¡No hay trámites pendientes por gestionar!")
            else:
                for idx, row in pending_df.iterrows():
                    proc_id = row["id"]
                    emp_name = row["full_name"]
                    user_id = row["user_id"]
                    proc_type = row["procedure_type"]
                    created_at = row["created_at"]
                    
                    with st.expander(f"📁 #{proc_id} - {emp_name} | {proc_type} ({created_at})"):
                        col_info, col_actions = st.columns([2, 1])
                        
                        with col_info:
                            st.markdown(f"**Usuario:** {user_id} - {emp_name}")
                            st.markdown(f"**Departamento:** {row['department']}")
                            st.markdown(f"**Detalles:**")
                            st.json(row["details"])
                            
                            if row["attachment_path"]:
                                import os
                                if os.path.exists(row["attachment_path"]):
                                    with open(row["attachment_path"], "rb") as f:
                                        st.download_button(
                                            label="📄 Descargar Soporte (PDF Consolidado)",
                                            data=f,
                                            file_name=os.path.basename(row["attachment_path"]),
                                            mime="application/pdf",
                                            key=f"dl_btn_{proc_id}"
                                        )
                                else:
                                    st.warning("⚠️ El archivo adjunto no se encontró en el servidor.")
                        
                        with col_actions:
                            st.markdown("### Acciones")
                            # Agregar un formulario para aprobación/rechazo y evitar recargas inmediatas
                            with st.form(f"hr_action_{proc_id}"):
                                action_opt = st.radio("Acción a tomar", ["Aprobar/Completar", "Rechazar"])
                                notes = st.text_area("Observaciones (Opcional)")
                                if st.form_submit_button("Procesar Trámite", use_container_width=True):
                                    new_status = "COMPLETED" if action_opt == "Aprobar/Completar" else "REJECTED"
                                    db_update_hr_procedure_status(proc_id, new_status, notes)
                                    
                                    # Obtener correo del empleado (usando el db_session global)
                                    with db_session() as conn:
                                        c = conn.cursor()
                                        c.execute("SELECT email FROM users_app WHERE username = %s", (user_id,))
                                        res = c.fetchone()
                                        emp_email = res[0] if res else None
                                        
                                    send_hr_procedure_status_update(emp_email, emp_name, proc_type, new_status)
                                    
                                    st.toast("✅ Trámite actualizado correctamente.")
                                    st.rerun()
        else:
            st.warning("No tienes permisos para acceder a la bandeja de Trámites.")
