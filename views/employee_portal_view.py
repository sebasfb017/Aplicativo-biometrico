from datetime import date, datetime, timedelta

import pandas as pd
import streamlit as st
import google.generativeai as genai

from database_conn.connection import db_conn


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
        "Licencia No Remunerada",
    ]

    if solo_rrhh:
        steps = ["Enviado", "RRHH (Final)"]
        status_order = {
            "PENDING_RRHH": 1,
            "APPROVED": 2,
            "REJECTED": -2,
            "CANCELLED": -3,
        }
    else:
        steps = (
            ["Enviado", "Coord.", "Jefe Área (Final)"]
            if requiere_jefe
            else ["Enviado", "Coord.", "RRHH (Final)"]
        )
        status_order = {
            "PENDING_COORD": 1,
            "PENDING_RRHH": 2,
            "PENDING_JEFE": 3 if requiere_jefe else -1,
            "APPROVED": 4 if requiere_jefe else 3,
            "REJECTED": -2,  # Estado terminal de rechazo
            "CANCELLED": -3,  # Estado terminal de cancelación
        }

    # Mapeo de estados a un texto más legible para el usuario
    status_labels = {
        "PENDING_COORD": "Enviado (Coord.)",
        "PENDING_RRHH": "Validando (RRHH)",
        "PENDING_JEFE": "Firma Jefe (Jefe Área)",
        "APPROVED": "Aprobado Final",
        "REJECTED": "Rechazado",
        "CANCELLED": "Cancelado",
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
            style = (
                "background-color: #C8E6C9; color: #2E7D32; border: 1px solid #2E7D32;"
            )
            icon = "✅ "
        elif i == current_step:
            style = (
                "background-color: #BBDEFB; color: #0D47A1; border: 1px solid #0D47A1;"
            )
            icon = "⏳ "
        else:
            style = (
                "background-color: #E0E0E0; color: #616161; border: 1px solid #BDBDBD;"
            )
            icon = "⚪ "

        html += f'<div class="tracker-step" style="{style}">{icon}{step_name}</div>'

        # Añadir una línea de conexión entre los pasos (excepto en el último)
        if i < len(steps) - 1:
            line_color = "#2E7D32" if i < current_step else "#BDBDBD"
            html += f'<div class="tracker-line" style="background-color: {line_color};"></div>'

    html += "</div>"
    return html


# --- Fin del Componente Visual ---


from database_conn.queries import (
    db_cancel_leave_request,
    db_create_leave_request,
    db_create_hr_procedure,
    db_get_employee_procedures,
    get_cached_dataframe,
)
from services.email_service import send_novedad_alert
from services.notifications import generate_fth012_pdf


@st.dialog("Detalles de Mi Solicitud (F-TH-012)")
def show_leave_request_details(req_id: int):
    conn = db_conn()
    df_req = pd.read_sql_query(
        "SELECT * FROM leave_requests WHERE id = %s", conn, params=(req_id,)
    )

    df_audit = pd.read_sql_query(
        """
        SELECT a.user_id, a.action, a.timestamp, u.full_name, a.details, u.role
        FROM audit_logs a
        LEFT JOIN users_app u ON a.user_id = u.username
        WHERE a.details LIKE %s AND (a.action LIKE 'APPROVE_%%' OR a.action LIKE 'REJECT_%%')
        ORDER BY a.timestamp ASC
    """,
        conn,
        params=(f"%Permiso #{req_id} %",),
    )

    conn.close()

    if df_req.empty:
        st.error("No se encontró la solicitud.")
        return

    req = df_req.iloc[0]

    st.markdown(f"### Radicado: #{req['id']}")

    # Usamos el nuevo componente visual
    st.markdown(
        create_status_tracker(req["status"], req["reason_type"]), unsafe_allow_html=True
    )
    st.markdown("<br>", unsafe_allow_html=True)

    if req["status"] == "APPROVED":
        pdf_fth012 = generate_fth012_pdf(req, df_audit)
        st.download_button(
            label="📄 Descargar Comprobante F-TH-012 (PDF)",
            data=pdf_fth012,
            file_name=f"F-TH-012_{req['id']}.pdf",
            mime="application/pdf",
            type="primary",
            use_container_width=True,
        )
    elif req["status"] == "REJECTED":
        st.error(f"❌ Solicitud Rechazada. Motivo: {req['rejection_reason']}")
    elif req["status"] == "CANCELLED":
        st.warning(
            f"🚫 Solicitud Cancelada por el empleado. Motivo: {req['cancellation_reason']}"
        )

    html_info = f"""
    <div style="background: rgba(255,255,255,0.03); border: 1px solid rgba(255,255,255,0.08); border-radius: 12px; padding: 20px; margin-bottom: 20px; box-shadow: 0 4px 15px rgba(0,0,0,0.1);">
        <div style="display: flex; justify-content: space-between; border-bottom: 1px solid rgba(255,255,255,0.1); padding-bottom: 15px; margin-bottom: 15px;">
            <div>
                <div style="color: #9CA3AF; font-size: 0.85em; margin-bottom: 4px;">Fecha de Solicitud</div>
                <div style="font-weight: 600;">{req["request_date"]}</div>
            </div>
            <div>
                <div style="color: #9CA3AF; font-size: 0.85em; margin-bottom: 4px;">Fechas de Ausencia</div>
                <div style="font-weight: 600;">{req["leave_date_start"]} al {req["leave_date_end"]}</div>
            </div>
            <div>
                <div style="color: #9CA3AF; font-size: 0.85em; margin-bottom: 4px;">Remunerado</div>
                <div style="font-weight: 600;">{"✅ Sí" if req["is_paid"] else "❌ No"}</div>
            </div>
        </div>
        <div style="display: flex; justify-content: space-between;">
            <div>
                <div style="color: #9CA3AF; font-size: 0.85em; margin-bottom: 4px;">Hora Salida</div>
                <div style="font-weight: 600;">{req["start_time"] if req["start_time"] else "N/A"}</div>
            </div>
            <div>
                <div style="color: #9CA3AF; font-size: 0.85em; margin-bottom: 4px;">Hora Entrada</div>
                <div style="font-weight: 600;">{req["end_time"] if req["end_time"] else "N/A"}</div>
            </div>
            <div>
                <div style="color: #9CA3AF; font-size: 0.85em; margin-bottom: 4px;">Tiempo Total</div>
                <div style="font-weight: 600; color: #6366f1;">{req["total_time"]}</div>
            </div>
        </div>
    </div>
    """
    st.markdown(html_info, unsafe_allow_html=True)
    st.write(f"**Motivo General:** {req['reason_type']}")

    st.markdown("**Mi Justificación / Detalles:**")
    st.info(
        req["reason_description"]
        if req["reason_description"]
        else "Sin detalles ingresados."
    )

    if req["how_to_makeup"]:
        st.markdown("**Acuerdo de Reposición Prometido:**")
        st.warning(req["how_to_makeup"])

    if not df_audit.empty:
        st.divider()
        st.markdown("**Trazabilidad de Aprobación:**")
        for _, row in df_audit.iterrows():
            is_approve = "APPROVE" in row["action"]
            icon = "✅" if is_approve else "❌"
            action_text = "Aprobado por" if is_approve else "Rechazado por"
            date_str = pd.to_datetime(row["timestamp"]).strftime("%Y-%m-%d %H:%M")
            role_map = {
                "admin": "Administrador",
                "nomina": "Nómina/RRHH",
                "jefe_area": "Jefe de Área",
                "coordinador": "Coordinador",
            }
            rol_name = role_map.get(row["role"], "Autorizador")
            st.info(
                f"{icon} **{action_text}:** {row['full_name']} ({rol_name}) - *{date_str}*"
            )

    # --- GESTIÓN DOCUMENTAL: Soporte Médico o Legal Adjunto ---
    if (
        "attachment_path" in req
        and pd.notna(req["attachment_path"])
        and req["attachment_path"]
    ):
        import os

        from database_conn.connection import DATA_DIR

        file_path = os.path.join(DATA_DIR, "uploads", req["attachment_path"])

        st.divider()
        st.markdown("**Soporte Adjunto (Incapacidad/Certificado):**")

        if os.path.exists(file_path):
            st.markdown(
                """
            <div style="background: rgba(0,0,0,0.2); padding: 10px; border-radius: 12px; border: 1px dashed rgba(255,255,255,0.2); text-align: center; margin-bottom: 15px;">
                <span style="color: gray; font-size: 0.9em;">Vista previa del documento adjunto</span>
            </div>
            """,
                unsafe_allow_html=True,
            )

            ext = os.path.splitext(req["attachment_path"])[1].lower()
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
                    pdf_display = f'<iframe src="{pdf_url}" width="100%" height="600" style="border: none; border-radius: 12px;"></iframe>'
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
                file_name=req["attachment_path"],
                use_container_width=True,
            )
        else:
            st.error(
                "El archivo adjunto no se encuentra en el servidor. Puede haber sido eliminado."
            )
    # -----------------------------------------------------------
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
                row_a["full_name"] if pd.notna(row_a["full_name"]) else row_a["user_id"]
            )
            st.caption(f"✓ **{level}**: {approver_name} ({row_a['timestamp']})")


@st.dialog("Cancelar Solicitud de Permiso")
def cancel_leave_request_dialog(
    req_id: int, user_id: str, full_name: str, reason_type: str
):
    st.write(f"Cancelando solicitud #{req_id} de {full_name} ({reason_type}).")
    reason = st.text_area(
        "Por favor, ingresa el motivo de la cancelación:",
        key=f"cancellation_reason_{req_id}",
    )

    col1, col2 = st.columns(2)
    with col1:
        if st.button(
            "Confirmar Cancelación",
            type="primary",
            use_container_width=True,
            key=f"confirm_cancel_{req_id}",
        ):
            if reason:
                success = db_cancel_leave_request(req_id, user_id, reason)
                if success:
                    st.success("Solicitud cancelada exitosamente.")
                    st.rerun()
                else:
                    st.error(
                        "No se pudo cancelar la solicitud. Es posible que ya haya sido procesada."
                    )
            else:
                st.error("El motivo de cancelación no puede estar vacío.")
    with col2:
        if st.button("Volver", use_container_width=True, key=f"volver_cancel_{req_id}"):
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

    conn_vac = db_conn()
    cur_vac = conn_vac.cursor()
    cur_vac.execute(
        "SELECT vacation_balance FROM users_app WHERE username = %s", (user["username"],)
    )
    row_vac = cur_vac.fetchone()
    conn_vac.close()
    saldo_vac = int(row_vac[0]) if row_vac and row_vac[0] is not None else 0

    header_col, vac_col = st.columns([3, 1])
    with header_col:
        st.title("🧑‍⚕️ Mi Portal de Autogestión (F-TH-012)")
        area = user.get("emp_area") or "Sin Área Definida"
        subarea = user.get("emp_subarea")
        area_display = (
            f"{area} - {subarea}" if area != "Sin Área Definida" and subarea else area
        )
        st.write(f"Bienvenido/a **{user['full_name']}** | {area_display}")

    with vac_col:
        st.markdown(
            f"""
        <div style="background-color: rgba(255, 255, 255, 0.05); padding: 15px; border-radius: 10px; border: 1px solid rgba(255, 255, 255, 0.1); text-align: center; height: 100%; display: flex; flex-direction: column; justify-content: center; margin-top: 20px;">
            <p style="margin:0; font-size: 0.85em; color: #a0aec0; text-transform: uppercase; letter-spacing: 1px;">Vacaciones a Favor</p>
            <p style="margin:0; font-size: 1.8em; font-weight: bold; color: #e2e8f0;">🏖️ {saldo_vac} <span style="font-size: 0.5em; font-weight: normal; color: #a0aec0;">días</span></p>
        </div>
        """,
            unsafe_allow_html=True,
        )

    st.write("")  # Espaciador

    t1, t2, t3 = st.tabs(
        [
            "📝 Radicar Nuevo Permiso",
            "🗂️ Mis Solicitudes",
            "🏢 Mis Trámites en Línea",
        ]
    )

    with t1:
        st.subheader("Solicitud de Permisos Laborales Y/O Personales")
        st.info(
            "Llena el siguiente formulario digital equivalente al formato F-TH-012 físico."
        )

        if st.session_state.get("submit_success"):
            from utils.animations import render_lottie_success
            render_lottie_success(height=120)
            st.success(
                "✅ Solicitud enviada exitosamente. El formulario ha sido limpiado."
            )
            st.session_state.submit_success = False

        # --- SISTEMA DE REINICIO DE FORMULARIO (form_key) ---
        # Usamos una variable en session_state llamada 'form_key' que se añade como sufijo
        # a los identificadores (key) de todos los campos del formulario. Cuando el formulario
        # se envía con éxito, incrementamos este valor (+1), lo que obliga a Streamlit a
        # renderizar componentes completamente nuevos y "vacíos", logrando así limpiar la pantalla.
        if "form_key" not in st.session_state:
            st.session_state.form_key = 0
        fk = st.session_state.form_key

        with st.container(border=True):
            dias_solicitados = 0

            with st.container(border=True):
                st.subheader("1. Seleccionar Novedad, Fechas y Horas")
                col_cat, col_det = st.columns(2)
                with col_cat:
                    categoria = st.selectbox(
                        "Categoría de Novedad",
                        ["Citas", "Permisos", "Licencias", "Vacaciones", "Incapacidad", "Cambio de Turno"],
                        index=None,
                        placeholder="Selecciona...",
                        key=f"categoria_{fk}",
                    )

                with col_det:
                    reason_type = None
                    if categoria == "Citas":
                        reason_type = st.selectbox(
                            "Detalle",
                            [
                                "Cita Médica",
                                "Cita Médica con desplazamiento a otra ciudad",
                            ],
                            index=None,
                            placeholder="Selecciona...",
                            key=f"rt_citas_{fk}",
                        )
                    elif categoria == "Permisos":
                        reason_type = st.selectbox(
                            "Detalle",
                            ["Permiso Personal", "Permiso Laboral"],
                            index=None,
                            placeholder="Selecciona...",
                            key=f"rt_permisos_{fk}",
                        )
                    elif categoria == "Licencias":
                        reason_type = st.selectbox(
                            "Detalle",
                            [
                                "Calamidad Doméstica",
                                "Licencia de Luto",
                                "Licencia de Paternidad",
                                "Licencia por Votación",
                                "Licencia por Jurado de Votación",
                                "Licencia Remunerada",
                                "Licencia No Remunerada",
                            ],
                            index=None,
                            placeholder="Selecciona...",
                            key=f"rt_licencias_{fk}",
                        )
                    elif categoria == "Incapacidad":
                        reason_type = "Incapacidad"
                        st.text_input(
                            "Detalle",
                            value="Incapacidad",
                            disabled=True,
                            key=f"rt_incap_{fk}",
                        )
                    elif categoria == "Vacaciones":
                        reason_type = "Vacaciones"
                        st.text_input(
                            "Detalle",
                            value="Vacaciones",
                            disabled=True,
                            key=f"rt_vac_{fk}",
                        )
                    elif categoria == "Cambio de Turno":
                        reason_type = "Cambio de Turno"
                        st.text_input(
                            "Detalle",
                            value="Cambio de Turno",
                            disabled=True,
                            key=f"rt_cambioturno_{fk}",
                        )
                    else:
                        st.selectbox(
                            "Detalle",
                            [],
                            disabled=True,
                            index=None,
                            placeholder="Primero elige categoría",
                            key=f"rt_dummy_{fk}",
                        )

                is_reason_selected = reason_type is not None
                is_paid = st.radio(
                    "¿Permiso Remunerado?",
                    ["No", "Sí"],
                    horizontal=True,
                    key=f"is_paid_{fk}",
                    disabled=not is_reason_selected,
                )
                duracion_permiso = st.radio(
                    "¿Duración del permiso?",
                    ["Por Horas", "Por Días"],
                    horizontal=True,
                    index=None,
                    key=f"duracion_permiso_{fk}",
                    disabled=not is_reason_selected,
                )

                is_duration_selected = duracion_permiso is not None

                c1, c2 = st.columns(2)
                with c1:
                    if not is_duration_selected:
                        tipo_fechas = st.radio(
                            "Opciones de Días",
                            ["Días consecutivos", "Días específicos"],
                            horizontal=True,
                            disabled=True,
                            key=f"tipo_fechas_dummy_{fk}",
                        )
                    elif duracion_permiso == "Por Días":
                        tipo_fechas = st.radio(
                            "Opciones de Días",
                            ["Días consecutivos", "Días específicos"],
                            horizontal=True,
                            key=f"tipo_fechas_{fk}",
                        )
                    else:
                        tipo_fechas = "Día único"

                    leave_dates = []
                    specific_dates_str = None
                    if not is_duration_selected:
                        st.date_input(
                            "Fecha(s) del Permiso",
                            disabled=True,
                            key=f"leave_dates_dummy_{fk}",
                        )
                    elif tipo_fechas == "Días consecutivos":
                        leave_dates = st.date_input(
                            "Fecha(s) del Permiso",
                            value=[],
                            help="Selecciona un día o un rango de días.",
                            format="YYYY-MM-DD",
                            key=f"leave_dates_{fk}",
                        )
                        specific_dates_str = None
                    elif tipo_fechas == "Día único":
                        single_date = st.date_input(
                            "Fecha del Permiso",
                            value=date.today(),
                            help="Selecciona el día exacto del permiso.",
                            format="YYYY-MM-DD",
                            key=f"single_date_{fk}",
                        )
                        leave_dates = [single_date, single_date]
                        specific_dates_str = None
                    else:
                        if f"specific_dates_list_{fk}" not in st.session_state:
                            st.session_state[f"specific_dates_list_{fk}"] = []

                        cc1, cc2 = st.columns([3, 1])
                        with cc1:
                            d_input = st.date_input(
                                "Seleccionar Día",
                                format="YYYY-MM-DD",
                                key=f"d_input_{fk}",
                            )
                        with cc2:
                            st.write("")  # Spacer
                            if st.button("➕ Añadir"):
                                if (
                                    d_input
                                    not in st.session_state[f"specific_dates_list_{fk}"]
                                ):
                                    st.session_state[
                                        f"specific_dates_list_{fk}"
                                    ].append(d_input)
                                    st.session_state[f"specific_dates_list_{fk}"].sort()

                        if st.session_state[f"specific_dates_list_{fk}"]:
                            st.markdown("**Días Seleccionados:**")
                            for d in st.session_state[f"specific_dates_list_{fk}"]:
                                st.write(f"• {d.strftime('%Y-%m-%d')}")
                            if st.button("🗑️ Limpiar Días", key=f"btn_clean_{fk}"):
                                st.session_state[f"specific_dates_list_{fk}"] = []
                                st.rerun()

                        if st.session_state[f"specific_dates_list_{fk}"]:
                            leave_dates = [
                                st.session_state[f"specific_dates_list_{fk}"][0],
                                st.session_state[f"specific_dates_list_{fk}"][-1],
                            ]
                            specific_dates_str = ",".join(
                                [
                                    d.strftime("%Y-%m-%d")
                                    for d in st.session_state[
                                        f"specific_dates_list_{fk}"
                                    ]
                                ]
                            )
                        else:
                            leave_dates = []
                            specific_dates_str = None
                # (Categoría y tipo de novedad movidos al principio del expander)
            with c2:
                tipo_tiempo = None
                orig_start = None
                orig_end = None
                nuevo_start = None
                nuevo_end = None
                
                if categoria == "Cambio de Turno":
                    tipo_tiempo = "Cambio de Turno"
                    st.markdown("**Horario Original (Que tenías asignado)**")
                    col_o1, col_o2 = st.columns(2)
                    with col_o1:
                        orig_start = st.time_input("Hora de Inicio", value=None, key=f"orig_s_{fk}")
                    with col_o2:
                        orig_end = st.time_input("Hora de Fin", value=None, key=f"orig_e_{fk}")
                        
                    st.markdown("**Horario Solicitado (Nuevo horario)**")
                    col_n1, col_n2 = st.columns(2)
                    with col_n1:
                        nuevo_start = st.time_input("Nueva Hora Inicio", value=None, key=f"new_s_{fk}")
                    with col_n2:
                        nuevo_end = st.time_input("Nueva Hora Fin", value=None, key=f"new_e_{fk}")
                else:
                    if not is_duration_selected:
                        st.radio(
                            "¿Qué tipo de permiso es?",
                            ["Rango de Horas", "Llegada Tarde", "Salida Temprano"],
                            horizontal=True,
                            disabled=True,
                            key=f"tipo_tiempo_dummy2_{fk}",
                        )
                    elif duracion_permiso == "Por Días":
                        tipo_tiempo = "Por Días"
                        st.radio(
                            "Tipo de permiso",
                            ["Días completos (Auto)"],
                            horizontal=True,
                            key=f"tipo_tiempo_dummy_{fk}",
                            disabled=True,
                        )
                    else:
                        tipo_tiempo = st.radio(
                            "¿Qué tipo de permiso es?",
                            ["Rango de Horas", "Llegada Tarde", "Salida Temprano"],
                            horizontal=True,
                            key=f"tipo_tiempo_{fk}",
                        )

                time_s = None
                time_e = None

                if categoria != "Cambio de Turno":
                    if not is_duration_selected:
                        st.time_input("Hora Inicial", disabled=True, key=f"ts_dummy_{fk}")
                        st.time_input("Hora Final", disabled=True, key=f"te_dummy_{fk}")
                    elif tipo_tiempo == "Rango de Horas":
                        time_s = st.time_input(
                            "Hora de Salida a la diligencia",
                            value=None,
                            key=f"ts_rango_{fk}",
                        )
                        time_e = st.time_input(
                            "Hora de Regreso al trabajo", value=None, key=f"te_rango_{fk}"
                        )
                    elif tipo_tiempo == "Llegada Tarde":
                        time_s = st.time_input(
                            "¿A qué hora iniciaba tu turno hoy?",
                            value=None,
                            key=f"ts_tarde_{fk}",
                        )
                        time_e = st.time_input(
                            "Hora en la que llegaste al trabajo",
                            value=None,
                            key=f"te_tarde_{fk}",
                        )
                    elif tipo_tiempo == "Salida Temprano":
                        time_s = st.time_input(
                            "Hora a la que te fuiste del trabajo",
                            value=None,
                            key=f"ts_temprano_{fk}",
                        )
                        time_e = st.time_input(
                            "¿A qué hora terminaba tu turno hoy?",
                            value=None,
                            key=f"te_temprano_{fk}",
                        )

                # --- CÁLCULO DINÁMICO DE TIEMPO ---
                # Dependiendo de lo que seleccione el usuario, calculamos el tiempo automáticamente.
                # Las variables time_s y time_e capturan las horas seleccionadas por el usuario.
                calculated_time = ""
                if tipo_tiempo == "Cambio de Turno":
                    if nuevo_start and nuevo_end:
                        ts_dt = datetime.combine(date.today(), nuevo_start)
                        te_dt = datetime.combine(date.today(), nuevo_end)
                        if te_dt <= ts_dt:
                            te_dt += timedelta(days=1)
                        diff = te_dt - ts_dt
                        total_mins = diff.seconds // 60
                        h, m = divmod(total_mins, 60)
                        parts = []
                        if h > 0:
                            parts.append(f"{h} Hora{'s' if h > 1 else ''}")
                        if m > 0:
                            parts.append(f"{m} Minuto{'s' if m > 1 else ''}")
                        calculated_time = f"Nuevo Turno: {' y '.join(parts) if parts else '0 Minutos'}"
                    else:
                        calculated_time = "Ingresa ambas horas del Nuevo horario para calcular"
                elif tipo_tiempo == "Por Días":
                    if specific_dates_str:
                        # Si son fechas específicas, simplemente contamos la cantidad de fechas
                        dias_solicitados = len(
                            st.session_state[f"specific_dates_list_{fk}"]
                        )
                        calculated_time = f"{dias_solicitados} Día(s)"
                    elif leave_dates:
                        if (
                            isinstance(leave_dates, (tuple, list))
                            and len(leave_dates) > 1
                        ):
                            d_start = leave_dates[0]
                            d_end = leave_dates[1]
                            delta_days = (d_end - d_start).days + 1

                            if reason_type == "Vacaciones":
                                from database_conn.queries import is_holiday

                                count = 0
                                for i in range(delta_days):
                                    curr_date = d_start + timedelta(days=i)
                                    if curr_date.weekday() != 6 and not is_holiday(
                                        curr_date
                                    ):
                                        count += 1
                                dias_solicitados = count
                            else:
                                dias_solicitados = delta_days

                            calculated_time = f"{dias_solicitados} Día(s)"
                        else:
                            dias_solicitados = 1
                            if reason_type == "Vacaciones":
                                from database_conn.queries import is_holiday

                                curr_date = (
                                    leave_dates[0]
                                    if isinstance(leave_dates, (list, tuple))
                                    else leave_dates
                                )
                                if curr_date.weekday() == 6 or is_holiday(curr_date):
                                    dias_solicitados = 0
                            calculated_time = f"{dias_solicitados} Día(s)"
                else:  # Rango de Horas, Llegada Tarde, Salida Temprano
                    if time_s and time_e:
                        ts_dt = datetime.combine(date.today(), time_s)
                        te_dt = datetime.combine(date.today(), time_e)
                        if te_dt > ts_dt:
                            diff = te_dt - ts_dt
                            total_mins = diff.seconds // 60
                            h, m = divmod(total_mins, 60)
                            parts = []
                            if h > 0:
                                parts.append(f"{h} Hora{'s' if h > 1 else ''}")
                            if m > 0:
                                parts.append(f"{m} Minuto{'s' if m > 1 else ''}")
                            calculated_time = (
                                " y ".join(parts) if parts else "0 Minutos"
                            )
                        else:
                            calculated_time = (
                                "Error: La hora fin debe ser mayor a la inicial"
                            )
                    else:
                        calculated_time = "Ingresa ambas horas para calcular fracción"

                # ATENCIÓN: Este campo NO tiene el parámetro `key`. Esto es intencional.
                # Si le ponemos `key`, Streamlit guardaría en caché el valor inicial (ej. vacío)
                # y no se actualizaría visualmente en tiempo real al cambiar las horas arriba.
                st.text_input(
                    "Tiempo Total Calculado (Automático)",
                    value=calculated_time,
                    disabled=True,
                )
                total_time = calculated_time

            with st.container(border=True):
                st.subheader("2. Motivo y Justificación")
                
                r_desc = st.text_area(
                    "Justificación / Detalles", key=f"r_desc_{fk}"
                )
                if (
                    categoria in ["Licencias", "Vacaciones", "Incapacidad", "Cambio de Turno"]
                    or reason_type == "Permiso Laboral"
                ):
                    makeup = ""
                else:
                    makeup = st.text_input(
                        "¿Cómo se repone el tiempo? (Opcional)", key=f"makeup_{fk}"
                    )

            with st.container(border=True):
                st.subheader("3. Soportes y Envío")
                st.write(
                    "📄 **Documento de Soporte (Obligatorio para Incapacidad y Jurado/Votación)**"
                )
                uploaded_files = st.file_uploader(
                    "Adjunta tu incapacidad, certificado médico o soporte legal (puedes subir varios). Tamaño máximo total: 20MB",
                    type=["pdf", "png", "jpg", "jpeg"],
                    accept_multiple_files=True,
                    key=f"upload_{fk}",
                )

                st.write("")
                submitted = st.button(
                    "✅ Firmar y Enviar a RRHH",
                    type="primary",
                    use_container_width=True,
                )

        if submitted:
            # --- Validación de Tamaño del Archivo ---
            MAX_FILE_SIZE_MB = 20
            file_is_valid = True

            if uploaded_files:
                # Obtenemos el tamaño de los archivos subidos en bytes y lo convertimos a MB
                total_size = sum(uf.size for uf in uploaded_files)
                file_size_mb = total_size / (1024 * 1024)
                if file_size_mb > MAX_FILE_SIZE_MB:
                    st.error(
                        f"❌ El tamaño total de los archivos es demasiado grande ({file_size_mb:.1f} MB). El tamaño máximo permitido es {MAX_FILE_SIZE_MB} MB. Por favor, comprime los archivos antes de subirlos."
                    )
                    file_is_valid = False
            # ---------------------------------------

            is_valid_form = file_is_valid

            if is_valid_form:
                if not categoria or not reason_type:
                    st.error(
                        "❌ Debes seleccionar la Categoría y el Detalle de la Novedad."
                    )
                    is_valid_form = False
                elif not leave_dates:
                    st.error(
                        "❌ Debes seleccionar obligatoriamente al menos una fecha de inicio."
                    )
                    is_valid_form = False
                elif categoria == "Cambio de Turno":
                    if orig_start is None or orig_end is None or nuevo_start is None or nuevo_end is None:
                        st.error("❌ Debes especificar las horas de inicio y fin para ambos horarios.")
                        is_valid_form = False
                    else:
                        r_desc = f"CAMBIO DE TURNO:\nDe: {orig_start.strftime('%H:%M')} a {orig_end.strftime('%H:%M')}\nPara: {nuevo_start.strftime('%H:%M')} a {nuevo_end.strftime('%H:%M')}\n\nMotivo: {r_desc}"
                
                if is_valid_form and ("Error" in calculated_time or "Ingresa ambas" in calculated_time):
                    st.error(
                        "❌ Las horas ingresadas son inválidas o incompletas. Verifica el tiempo calculado."
                    )
                    is_valid_form = False
                elif not r_desc or not r_desc.strip():
                    st.error(
                        "❌ Debes ingresar una Justificación / Detalles del permiso de manera obligatoria."
                    )
                    is_valid_form = False
                elif reason_type == "Vacaciones" and dias_solicitados > saldo_vac:
                    st.error(
                        f"❌ Has solicitado {dias_solicitados} días de vacaciones, pero solo tienes {saldo_vac} días a favor."
                    )
                    is_valid_form = False
                else:
                    requires_attachment = categoria == "Incapacidad" or reason_type in [
                        "Licencia por Votación",
                        "Licencia por Jurado de Votación",
                    ]
                    if requires_attachment and not uploaded_files:
                        st.error(
                            f"❌ Para **{reason_type}** es OBLIGATORIO adjuntar el documento de soporte."
                        )
                        is_valid_form = False

            if is_valid_form:
                d_start = (
                    leave_dates[0]
                    if isinstance(leave_dates, (list, tuple))
                    else leave_dates
                )
                d_end = (
                    leave_dates[1]
                    if isinstance(leave_dates, (list, tuple)) and len(leave_dates) > 1
                    else d_start
                )
                str_ts = time_s.strftime("%H:%M") if time_s else ""
                str_te = time_e.strftime("%H:%M") if time_e else ""

                attachment_path = None
                if uploaded_files:
                    import os
                    import time
                    import zipfile

                    from database_conn.connection import DATA_DIR

                    uploads_dir = os.path.join(DATA_DIR, "uploads")
                    os.makedirs(uploads_dir, exist_ok=True)

                    if len(uploaded_files) == 1:
                        uf = uploaded_files[0]
                        file_extension = os.path.splitext(uf.name)[1]
                        safe_filename = (
                            f"{user['username']}_{int(time.time())}{file_extension}"
                        )
                        full_path = os.path.join(uploads_dir, safe_filename)
                        with open(full_path, "wb") as f:
                            f.write(uf.getbuffer())
                        attachment_path = safe_filename
                    else:
                        safe_filename = f"{user['username']}_{int(time.time())}.zip"
                        full_path = os.path.join(uploads_dir, safe_filename)
                        with zipfile.ZipFile(full_path, "w") as zf:
                            for i, uf in enumerate(uploaded_files):
                                file_extension = os.path.splitext(uf.name)[1]
                                original_name = os.path.splitext(uf.name)[0]
                                name_in_zip = f"{original_name}_{i + 1}{file_extension}"
                                zf.writestr(name_in_zip, uf.getbuffer())
                        attachment_path = safe_filename

                req_id = db_create_leave_request(
                    user["username"],
                    d_start,
                    d_end,
                    str_ts,
                    str_te,
                    total_time,
                    reason_type,
                    r_desc,
                    makeup,
                    is_paid == "Sí",
                    attachment_path,
                    specific_dates_str,
                )

                try:
                    with db_conn() as conn:
                        req_df = pd.read_sql_query(
                            "SELECT status FROM leave_requests WHERE id = %s",
                            conn,
                            params=(req_id,),
                        )
                        if not req_df.empty:
                            target_status = req_df.iloc[0]["status"]
                            target_emails = []
                            target_phones = []

                            if target_status == "PENDING_COORD":
                                user_app_df = pd.read_sql_query(
                                    "SELECT emp_subarea FROM users_app WHERE username = %s",
                                    conn,
                                    params=(user["username"],),
                                )
                                subarea = (
                                    user_app_df.iloc[0]["emp_subarea"]
                                    if not user_app_df.empty
                                    else ""
                                )
                                target_coord_dept = subarea
                                if subarea == "Servicios Generales":
                                    target_coord_dept = "Calidad"
                                elif subarea == "Orientador":
                                    target_coord_dept = "Seguridad"
                                coord_all = pd.read_sql_query(
                                    "SELECT emp_email, emp_phone, managed_department FROM users_app WHERE role IN ('coordinador', 'nomina', 'jefe_area') AND active = 1",
                                    conn,
                                )
                                for _, c_row in coord_all.iterrows():
                                    m_dept = c_row.get("managed_department") or ""
                                    if target_coord_dept in str(m_dept):
                                        if c_row["emp_email"]: target_emails.append(c_row["emp_email"])
                                        if c_row["emp_phone"]: target_phones.append(c_row["emp_phone"])
                            elif target_status == "PENDING_JEFE":
                                user_app_df = pd.read_sql_query(
                                    "SELECT role, emp_area, emp_subarea, managed_department FROM users_app WHERE username = %s",
                                    conn,
                                    params=(user["username"],),
                                )
                                u_role = (
                                    user_app_df.iloc[0]["role"]
                                    if not user_app_df.empty
                                    else ""
                                )
                                area = (
                                    user_app_df.iloc[0]["emp_area"]
                                    if not user_app_df.empty
                                    else ""
                                )
                                subarea = (
                                    user_app_df.iloc[0]["emp_subarea"]
                                    if not user_app_df.empty
                                    else ""
                                )
                                managed_dept = (
                                    user_app_df.iloc[0]["managed_department"]
                                    if not user_app_df.empty
                                    else ""
                                )

                                target_jefe_area = area
                                if subarea in [
                                    "Rehabilitación",
                                    "Tecnólogo Rayos X",
                                    "Farmacia",
                                ]:
                                    target_jefe_area = "Administrativo"
                                elif subarea == "Admisiones":
                                    target_jefe_area = "Financiera"

                                # Enrutamiento especial a Control Interno
                                special_areas = [
                                    "Enfermería",
                                    "Auditor Médico",
                                    "Medico",
                                    "Control Interno",
                                    "Cirugía",
                                ]
                                is_special = subarea in special_areas
                                if (
                                    not is_special
                                    and u_role == "coordinador"
                                    and managed_dept
                                ):
                                    c_depts = [
                                        d.strip()
                                        for d in managed_dept.split(",")
                                        if d.strip()
                                    ]
                                    if any(dept in c_depts for dept in special_areas):
                                        is_special = True

                                if is_special:
                                    target_jefe_area = "Control Interno"
                                jefe_df = pd.read_sql_query(
                                    """
                                    SELECT emp_email, emp_phone FROM users_app 
                                    WHERE role = 'jefe_area' AND active = 1 
                                    AND (managed_area = %s OR managed_area = 'Control Interno')
                                """,
                                    conn,
                                    params=(target_jefe_area,),
                                )
                                target_emails = [e for e in jefe_df["emp_email"].tolist() if e]
                                target_phones = [p for p in jefe_df["emp_phone"].tolist() if p]
                            elif target_status == "PENDING_RRHH":
                                admin_df = pd.read_sql_query(
                                    "SELECT emp_email, emp_phone FROM users_app WHERE role IN ('admin', 'nomina') AND active = 1",
                                    conn,
                                )
                                target_emails = [e for e in admin_df["emp_email"].tolist() if e]
                                target_phones = [p for p in admin_df["emp_phone"].tolist() if p]

                            if target_emails:
                                ok, msg = send_novedad_alert(
                                    target_emails,
                                    user["full_name"],
                                    reason_type,
                                    r_desc,
                                    total_time,
                                    d_start,
                                )
                                if not ok:
                                    st.warning(f"Fallo envío de correo al aprobador: {msg}")
                                    
                            if target_phones:
                                from services.whatsapp_service import send_novedad_alert_whatsapp
                                send_novedad_alert_whatsapp(
                                    target_phones, user["full_name"], reason_type, r_desc, total_time, d_start
                                )
                                
                            if not target_emails and not target_phones:
                                st.info("Solicitud creada. (No se encontró contacto del aprobador).")
                except Exception as e:
                    st.warning(f"Error interno al enviar correo a aprobadores: {e}")

                # Eliminado el envío redundante de "RADICADA" al propio empleado para no duplicar alertas en n8n

                st.session_state.submit_success = True
                
                # Incrementar la llave para generar widgets nuevos
                st.session_state.form_key += 1
                new_fk = st.session_state.form_key
                
                # Forzar que la nueva sesión nazca completamente limpia
                st.session_state[f"categoria_{new_fk}"] = None
                st.session_state[f"is_paid_{new_fk}"] = "No"
                st.session_state[f"duracion_permiso_{new_fk}"] = None
                st.session_state[f"r_desc_{new_fk}"] = ""
                st.session_state[f"specific_dates_list_{new_fk}"] = []
                
                st.rerun()

    with t2:
        df_reqs = get_cached_dataframe(
            """
            SELECT id as "Radicado", request_date as "Fecha_Solicitud", leave_date_start, 
                   leave_date_end, total_time as "Duración", reason_type as "Motivo", status as "Estado",
                   full_name
            FROM leave_requests lr
            JOIN users_app ua ON lr.user_id = ua.username
            WHERE lr.user_id = %s AND (lr.hidden_by_employee IS NULL OR lr.hidden_by_employee = 0)
            ORDER BY id DESC
        """,
            params=(user["username"],),
        )

        if df_reqs.empty:
            st.info("No tienes solicitudes históricas radicas.")
        else:
            df_reqs["Fechas"] = df_reqs.apply(
                lambda r: (
                    r["leave_date_start"]
                    if r["leave_date_start"] == r["leave_date_end"]
                    else f"{r['leave_date_start']} al {r['leave_date_end']}"
                ),
                axis=1,
            )

            st.info(
                "💡 Desliza hacia abajo o haz clic en 'Ver Detalles' en la solicitud que desees auditar."
            )

            # Filtro de solicitudes
            filter_status = st.radio(
                "Mostrar solicitudes:",
                ("Todas", "Pendientes", "Aprobadas", "Rechazadas/Canceladas"),
                horizontal=True,
                key="employee_portal_filter",
            )

            filtered_df_reqs = df_reqs.copy()

            if filter_status == "Pendientes":
                filtered_df_reqs = filtered_df_reqs[
                    filtered_df_reqs["Estado"].isin(
                        ["PENDING_COORD", "PENDING_JEFE", "PENDING_RRHH"]
                    )
                ]
            elif filter_status == "Aprobadas":
                filtered_df_reqs = filtered_df_reqs[
                    filtered_df_reqs["Estado"] == "APPROVED"
                ]
            elif filter_status == "Rechazadas/Canceladas":
                filtered_df_reqs = filtered_df_reqs[
                    filtered_df_reqs["Estado"].isin(["REJECTED", "CANCELLED"])
                ]

            if filtered_df_reqs.empty:
                st.info("No tienes solicitudes en esta categoría.")
            else:
                for _, r in filtered_df_reqs.iterrows():  # Usamos el DataFrame filtrado
                    # Colores mágicos según estado
                    if r["Estado"] == "APPROVED":
                        bg_color = "rgba(16, 185, 129, 0.1)"
                        border_color = "rgba(16, 185, 129, 0.4)"
                        title_color = "#10b981"
                        icon = "✅"
                        status_text = "APROBADA"
                    elif r["Estado"] in ["REJECTED", "CANCELLED"]:
                        bg_color = "rgba(239, 68, 68, 0.1)"
                        border_color = "rgba(239, 68, 68, 0.4)"
                        title_color = "#ef4444"
                        icon = "🔴"
                        status_text = "RECHAZADA / CANCELADA"
                    else:
                        bg_color = "rgba(245, 158, 11, 0.1)"
                        border_color = "rgba(245, 158, 11, 0.4)"
                        title_color = "#f59e0b"
                        icon = "⏳"
                        status_text = "PENDIENTE"

                    card_html = f"""
                    <div style="background: linear-gradient(135deg, {bg_color}, rgba(0,0,0,0)); border: 1px solid {border_color}; border-radius: 16px; padding: 20px; box-shadow: 0 8px 32px rgba(0,0,0,0.05); margin-bottom: 5px; transition: transform 0.3s ease;">
                        <h4 style="margin-top:0; color: {title_color}; display: flex; justify-content: space-between; align-items: center;">
                            <span>{icon} {status_text}</span>
                            <span style="font-size: 0.7em; color: gray;">Radicado #{r["Radicado"]}</span>
                        </h4>
                        <p style="margin: 5px 0;"><strong>🗓️ {r["Fechas"]}</strong></p>
                        <p style="margin: 5px 0;"><strong>Motivo:</strong> {r["Motivo"]} | <strong>Duración:</strong> {r["Duración"]}</p>
                        <div style="margin-top: 15px; border-top: 1px solid rgba(255,255,255,0.05); padding-top: 15px;">
                            {create_status_tracker(r["Estado"], r["Motivo"])}
                        </div>
                    </div>
                    """

                    with st.container(border=False):
                        cols = st.columns([7, 3])
                        with cols[0]:
                            st.markdown(card_html, unsafe_allow_html=True)

                        with cols[1]:
                            st.write("")  # Espaciador superior
                            st.write("")
                            if st.button(
                                "👁️ Ver Detalles",
                                key=f"btn_detalles_{r['Radicado']}",
                                use_container_width=True,
                            ):
                                show_leave_request_details(r["Radicado"])

                            # Botón de Cancelar solo si el estado es PENDIENTE
                            if r["Estado"] in [
                                "PENDING_COORD",
                                "PENDING_JEFE",
                                "PENDING_RRHH",
                            ]:
                                if st.button(
                                    "❌ Cancelar Solicitud",
                                    key=f"btn_cancel_{r['Radicado']}",
                                    use_container_width=True,
                                ):
                                    cancel_leave_request_dialog(
                                        r["Radicado"],
                                        user["username"],
                                        r["full_name"],
                                        r["Motivo"],
                                    )

                            # Botón de Ocultar/Eliminar solo si el estado no es PENDIENTE (incluyendo terminales y desconocidos)
                            elif r["Estado"] not in [
                                "PENDING_COORD",
                                "PENDING_JEFE",
                                "PENDING_RRHH",
                            ]:
                                if st.button(
                                    "🗑️ Eliminar del Historial",
                                    key=f"btn_hide_{r['Radicado']}",
                                    use_container_width=True,
                                ):
                                    from database_conn.queries import (
                                        db_hide_leave_request,
                                    )

                                    if db_hide_leave_request(
                                        r["Radicado"], user["username"]
                                    ):
                                        st.success("Permiso eliminado del historial.")
                                        st.rerun()
                                    else:
                                        st.error(
                                            "Error al intentar ocultar el registro."
                                        )

    with t3:
        st.subheader("Gestión de Trámites en Línea")
        st.info("Radica solicitudes a Talento Humano y Nómina. Adjunta soportes si es necesario.")
        
        if "procedure_success" not in st.session_state:
            st.session_state.procedure_success = False
            
        if st.session_state.procedure_success:
            st.success("✅ Trámite radicado exitosamente.")
            st.session_state.procedure_success = False

        if "proc_form_key" not in st.session_state:
            st.session_state.proc_form_key = 0
            
        pfk = st.session_state.proc_form_key
        
        proc_type = st.selectbox(
            "Tipo de Solicitud a Realizar *",
            [
                "Certificación Laboral",
                "Desprendible de Pago de Nómina",
                "Certificado de Ingresos y Retenciones",
                "Retiro de Cesantías",
                "Cambio de EPS",
                "Inclusión Beneficiario EPS",
                "Inclusión Beneficiario Caja de Compensación",
                "Otro"
            ],
            key=f"proc_type_{pfk}"
        )
        
        # --- SELECTORES SECUNDARIOS FUERA DEL FORM PARA QUE SE ACTUALICEN EN TIEMPO REAL ---
        causal_cesantias = "Ninguna"
        eps_name = "Ninguna"
        bene_type = "Ninguno"
        
        st.markdown(f"### Detalles Adicionales para: **{proc_type}**")
        
        if proc_type == "Retiro de Cesantías":
            cesantias_options = ["Compra de Vivienda", "Mejoramiento de Vivienda", "Estudio para el Trabajador, Cónyuge o Hijo", "Pago de Impuestos de Vivienda", "Sustitución Patronal", "Abono a Crédito Hipotecario"]
            causal_cesantias = st.selectbox("Causal para Retiro de Cesantías", ["Ninguna"] + cesantias_options, key=f"proc_causal_cesantias_{pfk}")
            
            if causal_cesantias == "Compra de Vivienda":
                st.info("""**Documentos Requeridos:**\n- Carta de Solicitud de Retiro de Cesantías\n- Contrato de Compraventa Autenticado\n- Certificado de Tradición del predio o vivienda a comprar ("debe aparecer como dueño legítimo el vendedor")\n- Cédula de las partes del contrato\n- Certificado Bancario a nombre del colaborador""")
            elif causal_cesantias == "Mejoramiento de Vivienda":
                st.info("""**Documentos Requeridos:**\n- Carta de Solicitud de Retiro de Cesantías\n- Certificado de Tradición del predio o vivienda (poseedor del colaborador)\n- Cotización de Materiales\n- Contrato de Obra Civil (No requiere estar autenticado)\n- Cédulas de las partes del contrato\n- Carnet de constructor o RUT de la persona contratada\n- Certificado Bancario a nombre del colaborador""")
            elif causal_cesantias == "Estudio para el Trabajador, Cónyuge o Hijo":
                st.info("""**Documentos Requeridos:**\n- Recibo de Matrícula\n- Documento de Identidad del Colaborador, Hijo o Cónyuge (Para hijo: Registro Civil de Nacimiento. Para cónyuge: Certificado de Matrimonio o Declaración Juramentada de convivencia)\n- Certificado Bancario a nombre del colaborador\n\n*Nota: El recurso será consignado directamente a la Institución o Universidad.*""")
            elif causal_cesantias == "Pago de Impuestos de Vivienda":
                st.info("""**Documentos Requeridos:**\n- Solicitud de Retiro de Cesantías\n- Certificado de Tradición del predio o vivienda\n- Cédula del(a) Colaborador(a)\n- Recibo de Predial\n- Certificado Bancario a nombre del colaborador\n\n*En caso que la vivienda sea del cónyuge debe adjuntar Acta de Matrimonio o Declaración Juramentada.*""")
            elif causal_cesantias == "Sustitución Patronal":
                st.info("""**Documentos Requeridos:**\n- Solicitud de retiro de cesantías (mencionando que tuvo sustitución patronal)\n- Certificado Bancario a nombre del colaborador""")
            elif causal_cesantias == "Abono a Crédito Hipotecario":
                st.info("""**Documentos Requeridos:**\n- Carta de Solicitud de Retiro de Cesantías\n- Estado de cuenta del crédito hipotecario actualizado\n- Certificado Bancario a nombre del colaborador""")
        
        elif proc_type == "Cambio de EPS":
            eps_options = ["Sanitas", "EPS Sura", "Salud Total", "Nueva EPS", "SOS", "Famisanar", "Emssanar", "Asmet Salud", "Coosalud"]
            eps_name = st.selectbox("A qué EPS desea trasladarse", ["Ninguna"] + eps_options, key=f"proc_eps_{pfk}")
            if eps_name != "Ninguna":
                st.info("""**Documentos a Adjuntar:**\n- Cédula del Colaborador(a) y documentos de Identidad en caso que tenga beneficiarios en su núcleo Familiar.\n\n*Nota: Este proceso se puede demorar entre 2 y 4 meses dependiendo de la entidad.*""")
        
        elif proc_type == "Inclusión Beneficiario EPS":
            bene_options = ["Hijo(a)", "Padre y/o Madre", "Cónyuge", "Hijastro(a)"]
            bene_type = st.selectbox("Tipo de Inclusión (Beneficiario)", ["Ninguno"] + bene_options, key=f"proc_bene_{pfk}")
            if bene_type == "Hijo(a)":
                st.info("""**Requisitos:**\n- Cédula del(a) Colaborador(a)\n- Registro Civil Hijo(a)\n- Tarjeta de identidad Hijo(a) (Solo si es mayor de 8 años)""")
            elif bene_type == "Padre y/o Madre":
                st.info("""**Requisitos:**\n- Cédula del(a) Colaborador(a)\n- Registro Civil del Colaborador(a)\n- Cédula del Padre y/o Madre a incluir""")
            elif bene_type == "Cónyuge":
                st.info("""**Requisitos:**\n- Cédula del(a) Colaborador(a)\n- Cédula del(a) Cónyuge\n- Acta de Matrimonio o Declaración Juramentada de Convivencia""")
            elif bene_type == "Hijastro(a)":
                st.info("""**Requisitos:**\n- Cédula del Colaborador y del Cónyuge\n- Acta de Matrimonio o Declaración Juramentada de Convivencia\n- Registro Civil del Hijastro(a) y Tarjeta de Identidad (si aplica)\n- Declaración de dependencia económica del Hijastro(a) a incluir""")

        elif proc_type == "Inclusión Beneficiario Caja de Compensación":
            bene_options = ["Hijo(a)", "Padre y/o Madre", "Cónyuge", "Hijastro(a)"]
            bene_type = st.selectbox("Tipo de Inclusión (Beneficiario Caja)", ["Ninguno"] + bene_options, key=f"proc_bene_{pfk}")
            if bene_type != "Ninguno":
                st.info("Debe adjuntar los documentos de identidad, registro civil / acta de matrimonio, y el Formulario de Declaración Juramentada de la Caja de Compensación según corresponda al tipo de beneficiario.")
        
        # --- AHORA SI EL FORMULARIO PARA LOS DATOS RESTANTES Y EL BOTÓN ---
        with st.form(key=f"form_procedures_{pfk}"):
            selected_months = []
            selected_years = []
            other_text = ""
            
            if proc_type == "Desprendible de Pago de Nómina":
                months_options = ["Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio", "Julio", "Agosto", "Septiembre", "Octubre", "Noviembre", "Diciembre"]
                selected_months = st.multiselect("Meses requeridos (Desprendibles)", months_options, key=f"proc_months_{pfk}")
            
            elif proc_type == "Certificado de Ingresos y Retenciones":
                years_options = ["2022", "2023", "2024", "2025"]
                selected_years = st.multiselect("Años requeridos (Retenciones)", years_options, key=f"proc_years_{pfk}")
            
            elif proc_type == "Otro":
                other_text = st.text_area("Por favor describe detalladamente tu solicitud", key=f"proc_other_{pfk}")
            
            needs_attachment = proc_type in ["Retiro de Cesantías", "Cambio de EPS", "Inclusión Beneficiario EPS", "Inclusión Beneficiario Caja de Compensación"]
            
            if needs_attachment:
                st.markdown("---")
                st.markdown("### Soporte Documental Obligatorio 📎")
                st.warning("Debe escanear y adjuntar un ÚNICO archivo PDF con todos los soportes requeridos para esta solicitud.")
            else:
                st.markdown("---")
                st.markdown("### Soporte Documental (Opcional)")
                st.info("Puedes adjuntar un documento si crees que es necesario para tu trámite.")
                
            uploaded_files = st.file_uploader("Adjuntar archivo PDF", type=["pdf"], key=f"proc_file_{pfk}", accept_multiple_files=True)
            
            submitted = st.form_submit_button("Radicar Trámite", type="primary", use_container_width=True)
            
            if submitted:
                import json
                import os
                from PyPDF2 import PdfMerger
                from services.email_service import send_hr_procedure_alert
                
                # Build JSON details
                details_dict = {}
                if proc_type == "Desprendible de Pago de Nómina" and selected_months:
                    details_dict["Meses"] = selected_months
                if proc_type == "Certificado de Ingresos y Retenciones" and selected_years:
                    details_dict["Años"] = selected_years
                if proc_type == "Retiro de Cesantías" and causal_cesantias != "Ninguna":
                    details_dict["Causal"] = causal_cesantias
                if proc_type == "Cambio de EPS" and eps_name != "Ninguna":
                    details_dict["Nueva EPS"] = eps_name
                if "Inclusión Beneficiario" in proc_type and bene_type != "Ninguno":
                    details_dict["Tipo Beneficiario"] = bene_type
                if other_text:
                    details_dict["Notas"] = other_text
                    
                details_json = json.dumps(details_dict, ensure_ascii=False)
                
                # Validation rules
                needs_causal = proc_type == "Retiro de Cesantías" and causal_cesantias == "Ninguna"
                needs_eps = proc_type == "Cambio de EPS" and eps_name == "Ninguna"
                needs_bene = "Inclusión Beneficiario" in proc_type and bene_type == "Ninguno"
                
                if needs_causal or needs_eps or needs_bene:
                    st.error("❌ Por favor selecciona una opción válida en el menú desplegable (Causal, EPS o Beneficiario) antes de radicar.")
                else:
                    # Save attachment(s)
                    file_path = None
                    if uploaded_files:
                        os.makedirs("data/attachments", exist_ok=True)
                        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
                        file_path = f"data/attachments/{user['username']}_tramite_{timestamp}_consolidado.pdf"
                        
                        try:
                            if len(uploaded_files) > 1:
                                merger = PdfMerger()
                                for pdf in uploaded_files:
                                    merger.append(pdf)
                                with open(file_path, "wb") as f_out:
                                    merger.write(f_out)
                                merger.close()
                            else:
                                # Just one file
                                with open(file_path, "wb") as f:
                                    f.write(uploaded_files[0].getbuffer())
                        except Exception as e:
                            st.error(f"Error procesando los PDFs: {e}")
                            file_path = None
                    
                    if needs_attachment and not file_path:
                        st.error("❌ Este trámite requiere subir soportes documentales en PDF obligatoriamente.")
                    else:
                        db_create_hr_procedure(user["username"], proc_type, details_json, file_path)
                        # Send alert
                        send_hr_procedure_alert(user["full_name"], proc_type, datetime.now().strftime("%Y-%m-%d %H:%M"))
                        
                        st.session_state.procedure_success = True
                        st.session_state.proc_form_key += 1
                        st.rerun()

        st.divider()
        st.subheader("Historial de Trámites Radicados")
        mis_tramites = db_get_employee_procedures(user["username"])
        
        if not mis_tramites:
            st.info("No has radicado ningún trámite en línea recientemente.")
        else:
            for tram in mis_tramites:
                with st.expander(f"📌 {tram['procedure_type']} - Radicado el {tram['created_at'].split('T')[0]}"):
                    st.write(f"**Estado Actual:** `{tram['status']}`")
                    if tram["details"] and tram["details"] != "{}":
                        import json
                        try:
                            d = json.loads(tram["details"])
                            for k, v in d.items():
                                st.write(f"- **{k}:** {v}")
                        except:
                            st.write(f"- **Detalles:** {tram['details']}")
                    if tram["attachment_path"]:
                        st.write(f"📎 **Soporte adjunto:** `{tram['attachment_path'].split('/')[-1]}`")
