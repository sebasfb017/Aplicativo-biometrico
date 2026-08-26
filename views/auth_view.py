import random
from datetime import datetime, timedelta

import bcrypt
import pandas as pd
import streamlit as st

from database_conn.connection import db_conn
from database_conn.queries import db_create_session, get_users_by_role
from services.email_service import (
    send_password_changed_email,
    send_password_reset_pin,
    send_welcome_email,
)
from utils.auth import validate_password, verify_login  # Importar validate_password
from utils.constants import AREA_MAPPING


@st.dialog("📝 Registro en Portal de Empleados", width="large")
def register_employee_dialog():
    st.write("Crea tu cuenta segura para acceder al Portal de Autogestión.")

    if "reg_step" not in st.session_state:
        st.session_state["reg_step"] = 1
    if "reg_dni" not in st.session_state:
        st.session_state["reg_dni"] = ""
    if "reg_name" not in st.session_state:
        st.session_state["reg_name"] = ""
    if "reg_error" not in st.session_state:
        st.session_state["reg_error"] = ""

    def verify_cedula():
        st.session_state["reg_error"] = ""
        cedula_reg = st.session_state.get("reg_cedula_input", "").strip()

        if not cedula_reg:
            st.session_state["reg_error"] = "Por favor ingresa tu cédula."
            return

        conn = db_conn()
        emp_df = pd.read_sql_query(
            "SELECT full_name FROM employees WHERE user_id = %s",
            conn,
            params=(cedula_reg,),
        )
        if emp_df.empty:
            st.session_state["reg_error"] = (
                f"❌ La cédula {cedula_reg} no se encuentra en el listado maestro de empleados. Pide a Recursos Humanos que te registre en la pestaña 'Empleados' del Área Administrativa."
            )
            conn.close()
            return

        full_name = emp_df.iloc[0]["full_name"]

        user_df = pd.read_sql_query(
            "SELECT username FROM users_app WHERE username = %s",
            conn,
            params=(cedula_reg,),
        )
        if not user_df.empty:
            st.session_state["reg_error"] = (
                f"ℹ️ El usuario DNI {cedula_reg} ya se encuentra registrado. Si olvidaste tu contraseña, contacta a RRHH/Sistemas."
            )
            conn.close()
            return

        conn.close()
        st.session_state["reg_dni"] = cedula_reg
        st.session_state["reg_name"] = full_name
        st.session_state["reg_step"] = 2

    def create_account():
        st.session_state["reg_error"] = ""
        pass1 = st.session_state.get("reg_pass1", "")
        pass2 = st.session_state.get("reg_pass2", "")
        sel_area = st.session_state.get("reg_sel_area", "Administrativo")
        sel_subarea = st.session_state.get("reg_sel_subarea", "")
        phone = st.session_state.get("reg_phone", "").strip()
        email = st.session_state.get("reg_email", "").strip()

        role = st.session_state.get("reg_role", "empleado")
        if role == "coordinador":
            managed_depts = st.session_state.get("reg_managed_depts", [])
            managed_dept = ", ".join(managed_depts)
        else:
            managed_dept = ""
        managed_area = st.session_state.get("reg_managed_area", "")

        if not pass1 or not pass2 or not phone or not email:
            st.session_state["reg_error"] = (
                "Todos los campos de Registro (Teléfono, Correo y Contraseñas) son obligatorios."
            )
            return

        if pass1 != pass2:
            st.session_state["reg_error"] = "Las contraseñas no coinciden."
            return

        # Validar contraseña con la nueva función
        is_valid, error_msg = validate_password(pass1)
        if not is_valid:
            st.session_state["reg_error"] = error_msg
            return

        pw_hash = bcrypt.hashpw(pass1.encode("utf-8"), bcrypt.gensalt())
        conn = db_conn()
        cur = conn.cursor()
        try:
            cur.execute(
                """
                INSERT INTO users_app(username, full_name, role, password_hash, active, created_at, managed_department, emp_area, emp_subarea, emp_phone, emp_email, managed_area)
                VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """,
                (
                    st.session_state["reg_dni"],
                    st.session_state["reg_name"],
                    role,
                    pw_hash,
                    1,
                    datetime.now().isoformat(timespec="seconds"),
                    managed_dept,
                    sel_area,
                    sel_subarea,
                    phone,
                    email,
                    managed_area,
                ),
            )
            conn.commit()

            # Limpiar la caché de usuarios para que el Administrador pueda verlo inmediatamente
            get_users_by_role.clear()

            # Intentar enviar correo de bienvenida de forma silente
            try:
                send_welcome_email(
                    email,
                    st.session_state["reg_name"],
                    st.session_state["reg_dni"],
                    pass1,
                )
            except Exception:
                pass

            st.session_state["reg_step"] = 3
        except Exception as e:
            st.session_state["reg_error"] = f"Error al crear el usuario: {e!s}"
        finally:
            conn.close()

    def go_back():
        st.session_state["reg_error"] = ""
        st.session_state["reg_step"] = 1

    if st.session_state["reg_step"] == 1:
        st.info("Paso 1: Verificación de Identidad")
        st.text_input(
            "Número de Cédula (DNI) registrado en la empresa", key="reg_cedula_input"
        )

        if st.session_state["reg_error"]:
            st.error(st.session_state["reg_error"])

        st.button("Verificar Cédula", type="primary", on_click=verify_cedula)

    elif st.session_state["reg_step"] == 2:
        st.success(
            f"¡Hola, {st.session_state['reg_name']}! Completa tus datos para crear la cuenta."
        )
        st.info("Paso 2: Datos de Contacto, Área y Seguridad")

        with st.form("reg_form", border=False):
            col_c1, col_c2 = st.columns(2)
            with col_c1:
                st.text_input("Teléfono Móvil", key="reg_phone")
            with col_c2:
                st.text_input("Correo Electrónico", key="reg_email")

            st.markdown("---")
            st.selectbox(
                "Área a la que perteneces", list(AREA_MAPPING.keys()), key="reg_sel_area"
            )

            selected_a = st.session_state.get("reg_sel_area", "Administrativo")
            if selected_a not in AREA_MAPPING:
                selected_a = "Administrativo"

            st.selectbox(
                "Sub-área / Cargo", AREA_MAPPING[selected_a], key="reg_sel_subarea"
            )

            st.markdown("---")
            rol_options = {
                "empleado": "Auxiliar",
                "coordinador": "Coordinador de Departamento",
                "jefe_area": "Jefe de Área",
            }
            selected_role = st.selectbox(
                "Rol en el Sistema",
                list(rol_options.keys()),
                format_func=lambda x: rol_options[x],
                key="reg_role",
            )

            if selected_role == "coordinador":
                all_subareas = []
                for subs in AREA_MAPPING.values():
                    all_subareas.extend(subs)
                depts = sorted(list(set(all_subareas)))
                st.multiselect(
                    "¿Qué Departamentos coordinas?", options=depts, key="reg_managed_depts"
                )

            elif selected_role == "jefe_area":
                areas = list(AREA_MAPPING.keys()) + ["Auditoria Médica", "Control Interno"]
                st.selectbox(
                    "¿Qué Área tienes a cargo?",
                    [""] + sorted(areas),
                    key="reg_managed_area",
                )

            st.markdown("---")
            st.text_input("Ingresa una Contraseña nueva", type="password", key="reg_pass1")
            st.text_input("Confirma tu Contraseña", type="password", key="reg_pass2")

            if st.session_state["reg_error"]:
                st.error(st.session_state["reg_error"])

            col1, col2 = st.columns(2)
            with col1:
                st.form_submit_button(
                    "Crear mi Cuenta",
                    type="primary",
                    use_container_width=True,
                    on_click=create_account,
                )
            with col2:
                st.form_submit_button("Volver atrás", use_container_width=True, on_click=go_back)

    elif st.session_state["reg_step"] == 3:
        st.toast(f"🎉 ¡Cuenta creada con éxito para {st.session_state['reg_name']}!")
        st.write(
            "Tu usuario es tu número de cédula. Ya puedes cerrar esta ventana y utilizar tus nuevas credenciales para iniciar sesión en el Portal de Empleados."
        )
        if st.button("Cerrar Ventana", type="primary", use_container_width=True):
            st.session_state["reg_step"] = 1
            st.session_state["reg_dni"] = ""
            st.session_state["reg_name"] = ""
            st.session_state["reg_error"] = ""
            st.rerun()


@st.dialog("🔐 Recuperación de Contraseña", width="large")
def forgot_password_dialog():
    st.write("Sigue los pasos para restablecer tu contraseña de forma autónoma.")

    if "fp_step" not in st.session_state:
        st.session_state["fp_step"] = 1
    if "fp_dni" not in st.session_state:
        st.session_state["fp_dni"] = ""
    if "fp_error" not in st.session_state:
        st.session_state["fp_error"] = ""

    def check_dni():
        st.session_state["fp_error"] = ""
        dni = st.session_state.get("fp_dni_input", "").strip()
        if not dni:
            st.session_state["fp_error"] = "Ingresa tu número de documento."
            return

        conn = db_conn()
        df = pd.read_sql_query(
            "SELECT full_name, emp_email FROM users_app WHERE username = %s",
            conn,
            params=(dni,),
        )
        if df.empty:
            st.session_state["fp_error"] = (
                "Esa cédula no está registrada. Contacta a Recursos Humanos."
            )
            conn.close()
            return

        emp_email = df.iloc[0]["emp_email"]
        full_name = df.iloc[0]["full_name"]

        if not emp_email:
            st.session_state["fp_error"] = (
                "Tu perfil no tiene un correo electrónico configurado. Para tu seguridad, debes contactar a Nómina."
            )
            conn.close()
            return

        # Generar PIN aleatorio de 6 dígitos
        pin = str(random.randint(100000, 999999))
        expires = (datetime.now() + timedelta(minutes=5)).isoformat(timespec="seconds")

        from database_conn.connection import DB_PATH

        print("====== STREAMLIT DB_PATH IS ======", DB_PATH)

        # Asegurar columnas en tiempo de ejecución por si hay un fallo de sync con SQLite WAL
        try:
            cur = conn.cursor()
            cur.execute("ALTER TABLE users_app ADD COLUMN reset_pin TEXT;")
            conn.commit()
        except Exception:
            pass

        try:
            cur = conn.cursor()
            cur.execute("ALTER TABLE users_app ADD COLUMN reset_expires TEXT;")
            conn.commit()
        except Exception:
            pass

        cur = conn.cursor()
        cur.execute(
            "UPDATE users_app SET reset_pin = %s, reset_expires = %s WHERE username = %s",
            (pin, expires, dni),
        )
        conn.commit()
        conn.close()

        # Enviar correo con PIN
        send_password_reset_pin(emp_email, full_name, pin)

        st.session_state["fp_dni"] = dni
        st.session_state["fp_step"] = 2

    def reset_pw():
        st.session_state["fp_error"] = ""
        pin = st.session_state.get("fp_pin_input", "").strip()
        pw1 = st.session_state.get("fp_pw1", "")
        pw2 = st.session_state.get("fp_pw2", "")

        if not pin or not pw1 or not pw2:
            st.session_state["fp_error"] = "Llena todos los campos."
            return
        if pw1 != pw2:
            st.session_state["fp_error"] = "Las contraseñas nuevas no coinciden."
            return

        # Validar contraseña con la nueva función
        is_valid, error_msg = validate_password(pw1)
        if not is_valid:
            st.session_state["fp_error"] = error_msg
            return

        conn = db_conn()
        df = pd.read_sql_query(
            "SELECT reset_pin, reset_expires FROM users_app WHERE username = %s",
            conn,
            params=(st.session_state["fp_dni"],),
        )

        if df.empty or df.iloc[0]["reset_pin"] != pin:
            st.session_state["fp_error"] = (
                "PIN incorrecto. Revisa el correo electrónico."
            )
            conn.close()
            return

        expires = datetime.fromisoformat(df.iloc[0]["reset_expires"])
        if datetime.now() > expires:
            st.session_state["fp_error"] = (
                "El PIN ha expirado (pasaron más de 5 minutos). Por favor, solicita uno nuevo."
            )
            cur = conn.cursor()
            cur.execute(
                "UPDATE users_app SET reset_pin = NULL, reset_expires = NULL WHERE username = %s",
                (st.session_state["fp_dni"],),
            )
            conn.commit()
            conn.close()
            return

        # Hashear nueva contraseña
        pw_hash = bcrypt.hashpw(pw1.encode("utf-8"), bcrypt.gensalt())
        cur = conn.cursor()
        cur.execute(
            "UPDATE users_app SET password_hash = %s, reset_pin = NULL, reset_expires = NULL WHERE username = %s",
            (pw_hash, st.session_state["fp_dni"]),
        )
        conn.commit()

        # Consultar de forma segura para enviar correo
        df_mail = pd.read_sql_query(
            "SELECT full_name, emp_email FROM users_app WHERE username = %s",
            conn,
            params=(st.session_state["fp_dni"],),
        )
        conn.close()

        if not df_mail.empty and df_mail.iloc[0]["emp_email"]:
            send_password_changed_email(
                df_mail.iloc[0]["emp_email"], df_mail.iloc[0]["full_name"], pw1
            )

        st.session_state["fp_step"] = 3

    if st.session_state["fp_step"] == 1:
        st.info("Paso 1: Identificación")
        st.text_input("Ingresa tu Número de Cédula", key="fp_dni_input")
        if st.session_state["fp_error"]:
            st.error(st.session_state["fp_error"])

        col_btn1, col_btn2 = st.columns(2)
        with col_btn1:
            st.button(
                "Enviar PIN al Correo",
                type="primary",
                use_container_width=True,
                on_click=check_dni,
            )
        with col_btn2:
            if st.button("Cancelar", use_container_width=True):
                st.rerun()

    elif st.session_state["fp_step"] == 2:
        st.info("Paso 2: Digita el PIN y la Nueva Clave")
        st.toast(
            "✅ Documento validado correctamente. Puedes continuar con el paso 2. Revisa también la carpeta de SPAM o Correos no deseados. El PIN expira en 5 minutos."
        )
        with st.form("reset_form", border=False):
            st.text_input("PIN de 6 dígitos", key="fp_pin_input")
            st.text_input("Nueva Contraseña", type="password", key="fp_pw1")
            st.text_input("Confirmar Nueva Contraseña", type="password", key="fp_pw2")
            if st.session_state["fp_error"]:
                st.error(st.session_state["fp_error"])

            col_btn1, col_btn2 = st.columns(2)
            with col_btn1:
                st.form_submit_button(
                    "Cambiar Contraseña",
                    type="primary",
                    use_container_width=True,
                    on_click=reset_pw,
                )
            with col_btn2:
                def fp_go_back():
                    st.session_state["fp_step"] = 1
                    st.session_state["fp_error"] = ""
                st.form_submit_button("Volver a solicitar PIN", use_container_width=True, on_click=fp_go_back)

    elif st.session_state["fp_step"] == 3:
        st.toast("🎉 ¡Tu contraseña ha sido cambiada exitosamente!")
        st.write(
            "Ya puedes cerrar esta ventana e iniciar sesión con tu nueva clave en la pantalla principal."
        )
        if st.button("Cerrar Ventana", type="primary", use_container_width=True):
            st.session_state["fp_step"] = 1
            st.session_state["fp_dni"] = ""
            st.rerun()


def page_login():
    # Inyección de CSS global para un acabado visual Inmersivo (Glassmorphism Centrado):
    st.markdown(
        """
        <style>
        /* Fondo Animado de Pantalla Completa para el Login */
        .stApp {
            background: linear-gradient(-45deg, #0f172a, #1e1b4b, #312e81, #172554) !important;
            background-size: 400% 400% !important;
            animation: gradientBG 15s ease infinite !important;
        }
        @keyframes gradientBG {
            0% { background-position: 0% 50%; }
            50% { background-position: 100% 50%; }
            100% { background-position: 0% 50%; }
        }
        
        /* Ocultar header y sidebar */
        [data-testid="stHeader"], [data-testid="stSidebar"] { display: none !important; }
        
        /* Añadir padding superior para centrar verticalmente */
        .block-container {
            padding-top: 5vh !important;
            max-width: 100% !important;
        }
        
        /* Tarjeta Central Glassmorphism Premium */
        div[data-testid="stVerticalBlockBorderWrapper"] {
            background: rgba(15, 23, 42, 0.7) !important;
            backdrop-filter: blur(20px) !important;
            -webkit-backdrop-filter: blur(20px) !important;
            border: 1px solid rgba(255, 255, 255, 0.1) !important;
            border-top: 1px solid rgba(255, 255, 255, 0.2) !important;
            border-radius: 24px !important;
            padding: 40px 30px !important;
            box-shadow: 0 25px 50px -12px rgba(0, 0, 0, 0.5), 0 0 40px rgba(99, 102, 241, 0.2) !important;
            animation: fadeInCard 1s cubic-bezier(0.16, 1, 0.3, 1);
        }
        @keyframes fadeInCard {
            from { opacity: 0; transform: translateY(30px) scale(0.95); }
            to { opacity: 1; transform: translateY(0) scale(1); }
        }
        
        /* Botones Premium */
        .stButton>button {
            border-radius: 12px;
            background: linear-gradient(90deg, #6366f1, #8b5cf6) !important;
            border: none !important;
            color: white !important;
            transition: all 0.3s ease !important;
            box-shadow: 0 4px 15px rgba(99, 102, 241, 0.4) !important;
            font-weight: 600 !important;
            height: 45px !important;
            margin-top: 10px;
        }
        .stButton>button:hover {
            transform: translateY(-3px) scale(1.02);
            box-shadow: 0 8px 25px rgba(99, 102, 241, 0.6) !important;
        }
        
        /* Tabs centrados y estilizados */
        .stTabs [data-baseweb="tab-list"] {
            justify-content: center;
            background: rgba(255,255,255,0.03);
            border-radius: 12px;
            padding: 5px;
            margin-bottom: 25px;
        }
        .stTabs [data-baseweb="tab"] {
            border-radius: 8px;
            padding: 10px 20px;
            color: #94a3b8 !important;
        }
        .stTabs [aria-selected="true"] {
            background: rgba(255,255,255,0.15) !important;
            color: white !important;
        }
        
        /* Inputs estilizados */
        div[data-baseweb="input"] {
            border-radius: 10px;
            background: rgba(255,255,255,0.03);
            border: 1px solid rgba(255,255,255,0.1);
            transition: all 0.3s ease;
        }
        div[data-baseweb="input"]:focus-within {
            border-color: #6366f1;
            box-shadow: 0 0 0 1px #6366f1;
        }
        
        /* Color de textos */
        .stMarkdown p, .stMarkdown strong {
            color: #cbd5e1;
        }
        
        /* Línea separadora */
        hr {
            border-color: rgba(255,255,255,0.1) !important;
            margin: 1.5rem 0;
        }
        </style>
    """,
        unsafe_allow_html=True,
    )

    st.markdown("<br>", unsafe_allow_html=True)
    _, col_center, _ = st.columns([1, 1.5, 1])

    with col_center:
        with st.container(border=True):
            st.markdown(
                """
                <div style='text-align: center; margin-bottom: 25px;'>
                    <div style='display: inline-flex; align-items: center; justify-content: center; width: 80px; height: 80px; border-radius: 20px; background: linear-gradient(135deg, #6366f1, #3b82f6); box-shadow: 0 10px 25px rgba(99,102,241,0.5); margin-bottom: 15px;'>
                        <span style='font-size: 40px;'>🏥</span>
                    </div>
                    <h1 style='margin: 0; font-size: 2.5rem; background: linear-gradient(to right, #fff, #a5b4fc); -webkit-background-clip: text; -webkit-text-fill-color: transparent; font-weight: 700;'>Dolormed</h1>
                    <p style='color: #94a3b8; font-size: 1rem; margin-top: 5px; font-weight: 300;'>Portal Web de Empleados y Administración</p>
                </div>
                """,
                unsafe_allow_html=True,
            )

            tab1, tab2 = st.tabs(
                ["🔒 Administración", "🧑‍⚕️ Portal Empleado"]
            )

            with tab1:
                username = st.text_input("Usuario", placeholder="admin")
                password = st.text_input("Contraseña", type="password", placeholder="••••••••")

                if st.button(
                    "Iniciar Sesión", type="primary", use_container_width=True, key="btn_admin"
                ):
                    user = verify_login(username.strip(), password)
                    if user and "error" not in user:
                        st.session_state["user"] = user
                        token = db_create_session(user["username"])
                        st.session_state["session_token"] = token
                        st.query_params["session_token"] = token
                        st.toast(f"¡Bienvenido, {user['full_name']}!", icon="👋")
                        import time
                        time.sleep(0.5)
                        st.rerun()
                    else:
                        st.error(
                            user["error"] if user else "❌ Credenciales inválidas."
                        )

            with tab2:
                cedula_log = st.text_input(
                    "Cédula de Identidad", key="emp_login_ced", placeholder="Ej: 17xxxxxx"
                )
                pw_log = st.text_input(
                    "Contraseña", type="password", key="emp_login_pw", placeholder="••••••••"
                )

                if st.button(
                    "Ingresar al Portal", type="primary", use_container_width=True, key="btn_emp"
                ):
                    if cedula_log.strip() and pw_log:
                        user = verify_login(cedula_log.strip(), pw_log)
                        if user and "error" not in user:
                            st.session_state["user"] = user
                            token = db_create_session(user["username"])
                            st.session_state["session_token"] = token
                            st.query_params["session_token"] = token
                            st.toast(f"Acceso exitoso: {user['full_name']}", icon="👋")
                            import time
                            time.sleep(0.5)
                            st.rerun()
                        else:
                            st.error(
                                user["error"]
                                if user
                                else "❌ Credenciales incorrectas."
                            )
                    else:
                        st.warning(
                            "⚠️ Debes digitar tu número de documento completo y la contraseña."
                        )

                st.divider()
                st.write("<p style='text-align: center; font-size: 0.9rem; margin-bottom: 10px;'>¿Primera vez o problemas de acceso?</p>", unsafe_allow_html=True)
                colA, colB = st.columns(2)
                with colA:
                    if st.button("Olvidé la Clave", use_container_width=True, key="btn_forgot"):
                        forgot_password_dialog()
                with colB:
                    if st.button("Crear Cuenta", use_container_width=True, key="btn_register"):
                        register_employee_dialog()
