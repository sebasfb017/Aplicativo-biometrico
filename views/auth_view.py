import bcrypt
import pandas as pd
import streamlit as st
import random
from datetime import datetime, timedelta

from database_conn.connection import db_conn
from utils.auth import verify_login
from utils.constants import AREA_MAPPING
from services.email_service import send_welcome_email, send_password_reset_pin, send_password_changed_email

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
        emp_df = pd.read_sql_query("SELECT full_name FROM employees WHERE user_id = ?", conn, params=(cedula_reg,))
        if emp_df.empty:
            st.session_state["reg_error"] = f"❌ La cédula {cedula_reg} no se encuentra en el listado maestro de empleados. Pide a Recursos Humanos que te registre en la pestaña 'Empleados' del Área Administrativa."
            conn.close()
            return
            
        full_name = emp_df.iloc[0]['full_name']
        
        user_df = pd.read_sql_query("SELECT username FROM users_app WHERE username = ?", conn, params=(cedula_reg,))
        if not user_df.empty:
            st.session_state["reg_error"] = f"ℹ️ El usuario DNI {cedula_reg} ya se encuentra registrado. Si olvidaste tu contraseña, contacta a RRHH/Sistemas."
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
        managed_dept = st.session_state.get("reg_managed_dept", "")
        managed_area = st.session_state.get("reg_managed_area", "")
        
        if not pass1 or not pass2 or not phone or not email:
            st.session_state["reg_error"] = "Todos los campos de Registro (Teléfono, Correo y Contraseñas) son obligatorios."
            return
            
        if pass1 != pass2:
            st.session_state["reg_error"] = "Las contraseñas no coinciden."
            return
            
        if len(pass1) < 4:
            st.session_state["reg_error"] = "La contraseña debe tener al menos 4 caracteres."
            return
            
        pw_hash = bcrypt.hashpw(pass1.encode("utf-8"), bcrypt.gensalt())
        conn = db_conn()
        cur = conn.cursor()
        try:
            cur.execute("""
                INSERT INTO users_app(username, full_name, role, password_hash, active, created_at, managed_department, emp_area, emp_subarea, emp_phone, emp_email, managed_area)
                VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
            """, (st.session_state["reg_dni"], st.session_state["reg_name"], role, pw_hash, 1, datetime.now().isoformat(timespec="seconds"), managed_dept, sel_area, sel_subarea, phone, email, managed_area))
            conn.commit()
            
            # Intentar enviar correo de bienvenida de forma silente
            try:
                send_welcome_email(email, st.session_state["reg_name"], st.session_state["reg_dni"], pass1)
            except Exception:
                pass
                
            st.session_state["reg_step"] = 3
        except Exception as e:
            st.session_state["reg_error"] = f"Error al crear el usuario: {str(e)}"
        finally:
            conn.close()

    def go_back():
        st.session_state["reg_error"] = ""
        st.session_state["reg_step"] = 1



    if st.session_state["reg_step"] == 1:
        st.info("Paso 1: Verificación de Identidad")
        st.text_input("Número de Cédula (DNI) registrado en la empresa", key="reg_cedula_input")
        
        if st.session_state["reg_error"]:
            st.error(st.session_state["reg_error"])
            
        st.button("Verificar Cédula", type="primary", on_click=verify_cedula)

    elif st.session_state["reg_step"] == 2:
        st.success(f"¡Hola, {st.session_state['reg_name']}! Completa tus datos para crear la cuenta.")
        st.info("Paso 2: Datos de Contacto, Área y Seguridad")
        
        col_c1, col_c2 = st.columns(2)
        with col_c1:
            st.text_input("Teléfono Móvil", key="reg_phone")
        with col_c2:
            st.text_input("Correo Electrónico", key="reg_email")
            
        st.markdown("---")
        st.selectbox("Área a la que perteneces", list(AREA_MAPPING.keys()), key="reg_sel_area")
        
        selected_a = st.session_state.get("reg_sel_area", "Administrativo")
        if selected_a not in AREA_MAPPING: selected_a = "Administrativo"
            
        st.selectbox("Sub-área / Cargo", AREA_MAPPING[selected_a], key="reg_sel_subarea")
        
        st.markdown("---")
        rol_options = {"empleado": "Empleado Regular", "coordinador": "Coordinador de Departamento", "jefe_area": "Jefe de Área"}
        selected_role = st.selectbox("Rol en el Sistema", list(rol_options.keys()), format_func=lambda x: rol_options[x], key="reg_role")
        
        if selected_role == "coordinador":
            depts = ["Sistemas", "Facturación", "Glosas", "Cartera", "Admisiones", "Enfermería", "SIAU", "Calidad", "SST", "Contabilidad", "Imagenología"]
            st.selectbox("¿Qué Departamento coordinas?", [""] + sorted(depts), key="reg_managed_dept")
            
        elif selected_role == "jefe_area":
            areas = ["Administrativa", "Financiera", "Asistencial", "Médica"]
            st.selectbox("¿Qué Área tienes a cargo?", [""] + sorted(areas), key="reg_managed_area")
            
        st.markdown("---")
        st.text_input("Ingresa una Contraseña nueva", type="password", key="reg_pass1")
        st.text_input("Confirma tu Contraseña", type="password", key="reg_pass2")
        
        if st.session_state["reg_error"]:
            st.error(st.session_state["reg_error"])
            
        col1, col2 = st.columns(2)
        with col1:
            st.button("Crear mi Cuenta", type="primary", use_container_width=True, on_click=create_account)
        with col2:
            st.button("Volver atrás", use_container_width=True, on_click=go_back)

    elif st.session_state["reg_step"] == 3:
        st.success(f"🎉 ¡Cuenta creada con éxito para {st.session_state['reg_name']}!")
        st.write("Tu usuario es tu número de cédula. Ya puedes cerrar esta ventana y utilizar tus nuevas credenciales para iniciar sesión en el Portal de Empleados.")
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
        df = pd.read_sql_query("SELECT full_name, emp_email FROM users_app WHERE username = ?", conn, params=(dni,))
        if df.empty:
            st.session_state["fp_error"] = "Esa cédula no está registrada. Contacta a Recursos Humanos."
            conn.close()
            return
            
        emp_email = df.iloc[0]['emp_email']
        full_name = df.iloc[0]['full_name']
        
        if not emp_email:
            st.session_state["fp_error"] = "Tu perfil no tiene un correo electrónico configurado. Para tu seguridad, debes contactar a Nómina."
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
        except:
            pass
            
        try:
            cur = conn.cursor()
            cur.execute("ALTER TABLE users_app ADD COLUMN reset_expires TEXT;")
            conn.commit()
        except:
            pass
            
        cur = conn.cursor()
        cur.execute("UPDATE users_app SET reset_pin = ?, reset_expires = ? WHERE username = ?", (pin, expires, dni))
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
            
        conn = db_conn()
        df = pd.read_sql_query("SELECT reset_pin, reset_expires FROM users_app WHERE username = ?", conn, params=(st.session_state["fp_dni"],))
        
        if df.empty or df.iloc[0]['reset_pin'] != pin:
            st.session_state["fp_error"] = "PIN incorrecto. Revisa el correo electrónico."
            conn.close()
            return
            
        expires = datetime.fromisoformat(df.iloc[0]['reset_expires'])
        if datetime.now() > expires:
            st.session_state["fp_error"] = "El PIN ha expirado (pasaron más de 5 minutos). Por favor, solicita uno nuevo."
            cur = conn.cursor()
            cur.execute("UPDATE users_app SET reset_pin = NULL, reset_expires = NULL WHERE username = ?", (st.session_state["fp_dni"],))
            conn.commit()
            conn.close()
            return
            
        # Hashear nueva contraseña
        pw_hash = bcrypt.hashpw(pw1.encode("utf-8"), bcrypt.gensalt())
        cur = conn.cursor()
        cur.execute("UPDATE users_app SET password_hash = ?, reset_pin = NULL, reset_expires = NULL WHERE username = ?", (pw_hash, st.session_state["fp_dni"]))
        conn.commit()
        
        # Consultar de forma segura para enviar correo
        df_mail = pd.read_sql_query("SELECT full_name, emp_email FROM users_app WHERE username = ?", conn, params=(st.session_state["fp_dni"],))
        conn.close()
        
        if not df_mail.empty and df_mail.iloc[0]['emp_email']:
            send_password_changed_email(df_mail.iloc[0]['emp_email'], df_mail.iloc[0]['full_name'], pw1)
        
        st.session_state["fp_step"] = 3

    if st.session_state["fp_step"] == 1:
        st.info("Paso 1: Identificación")
        st.text_input("Ingresa tu Número de Cédula", key="fp_dni_input")
        if st.session_state["fp_error"]:
            st.error(st.session_state["fp_error"])
        
        col_btn1, col_btn2 = st.columns(2)
        with col_btn1:
            st.button("Enviar PIN al Correo", type="primary", use_container_width=True, on_click=check_dni)
        with col_btn2:
            if st.button("Cancelar", use_container_width=True):
                st.rerun()
        
    elif st.session_state["fp_step"] == 2:
        st.info("Paso 2: Digita el PIN y la Nueva Clave")
        st.warning("Hemos enviado un PIN de 6 dígitos a tu correo. Revisa también la carpeta de SPAM o Correos no deseados. El PIN expira en 5 minutos.")
        st.text_input("PIN de 6 dígitos", key="fp_pin_input")
        st.text_input("Nueva Contraseña", type="password", key="fp_pw1")
        st.text_input("Confirmar Nueva Contraseña", type="password", key="fp_pw2")
        if st.session_state["fp_error"]:
            st.error(st.session_state["fp_error"])
            
        col_btn1, col_btn2 = st.columns(2)
        with col_btn1:
            st.button("Cambiar Contraseña", type="primary", use_container_width=True, on_click=reset_pw)
        with col_btn2:
            if st.button("Volver a solicitar PIN", use_container_width=True):
                st.session_state["fp_step"] = 1
                st.rerun()
        
    elif st.session_state["fp_step"] == 3:
        st.success("🎉 ¡Tu contraseña ha sido cambiada exitosamente!")
        st.write("Ya puedes cerrar esta ventana e iniciar sesión con tu nueva clave en la pantalla principal.")
        if st.button("Cerrar Ventana", type="primary", use_container_width=True):
            st.session_state["fp_step"] = 1
            st.session_state["fp_dni"] = ""
            st.rerun()
def page_login():
    # Inyección de CSS global para un acabado visual sutil:
    st.markdown("""
        <style>
        .stButton>button {
            border-radius: 8px;
            transition: all 0.2s ease-in-out;
        }
        .stButton>button:hover {
            transform: translateY(-2px);
            box-shadow: 0 4px 8px rgba(0,0,0,0.1);
        }
        /* Ajustar ancho máximo de las notificaciones */
        .stAlert {
            border-radius: 8px;
        }
        </style>
    """, unsafe_allow_html=True)

    st.markdown("<br><br>", unsafe_allow_html=True)
    _, col_center, _ = st.columns([1, 2, 1])
    
    with col_center:
        with st.container(border=True):
            st.markdown("<h1 style='text-align: center; font-size: 3.5rem; margin-bottom: 0; color: #0D6EFD;'>Dolormed</h1>", unsafe_allow_html=True)
            st.markdown("<p style='text-align: center; color: gray; font-size: 1.1rem; margin-top: 0;'>Portal Web de Empleados y Administración</p>", unsafe_allow_html=True)
            st.markdown("<br>", unsafe_allow_html=True)

            tab1, tab2 = st.tabs(["🔒 Ingreso Administrativo", "🧑‍⚕️ Portal de Empleados"])

            with tab1:
                st.write("**Credenciales Base:**")
                username = st.text_input("Usuario Administrativo")
                password = st.text_input("Contraseña", type="password")
                st.markdown("<br>", unsafe_allow_html=True)

                if st.button("Ingresar al Sistema", type="primary", use_container_width=True):
                    user = verify_login(username.strip(), password)
                    if user:
                        st.session_state["user"] = user
                        st.success(f"¡Bienvenido, {user['full_name']}!")
                        import time
                        time.sleep(0.5)
                        st.rerun()
                    else:
                        st.error("❌ Credenciales inválidas o acceso deshabilitado.")

            with tab2:
                st.write("**Acceso por Documento de Identidad:**")
                
                cedula_log = st.text_input("Número de Cédula de Ciudadanía", key="emp_login_ced")
                pw_log = st.text_input("Contraseña Personal", type="password", key="emp_login_pw")
                st.markdown("<br>", unsafe_allow_html=True)
                
                if st.button("Ingresar al Portal", type="primary", use_container_width=True):
                    if cedula_log.strip() and pw_log:
                        user = verify_login(cedula_log.strip(), pw_log)
                        if user:
                            st.session_state["user"] = user
                            st.success(f"Acceso exitoso: {user['full_name']}")
                            import time
                            time.sleep(0.5)
                            st.rerun()
                        else:
                            st.error("❌ Credenciales incorrectas.")
                    else:
                        st.warning("⚠️ Debes digitar tu número de documento completo y la contraseña.")
                        
                st.divider()
                st.write("¿Problemas de acceso o primera vez en el portal?")
                colA, colB = st.columns(2)
                with colA:
                    if st.button("Olvidé mi Contraseña 🔄", use_container_width=True):
                        forgot_password_dialog()
                with colB:
                    if st.button("Asignar Primera Clave 🔑", use_container_width=True):
                        register_employee_dialog()