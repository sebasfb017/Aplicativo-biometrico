import bcrypt
import pandas as pd
import streamlit as st
from datetime import datetime

from database_conn.connection import db_conn
from services.notifications import log_audit
from database_conn.queries import get_users_by_role, get_all_employees
from utils.auth import require_role
from utils.constants import AREA_MAPPING
from services.email_service import load_smtp_config, save_smtp_config, send_welcome_email

@st.dialog("✏️ Editar Usuario", width="large")
def edit_user_dialog(username: str, emp_df: pd.DataFrame):
    conn = db_conn()
    df_u = pd.read_sql_query("SELECT * FROM users_app WHERE username = ?", conn, params=(username,))
    conn.close()
    
    if df_u.empty:
        st.error("Usuario no encontrado.")
        return
        
    u = df_u.iloc[0]
    
    st.markdown(f"### Editando ID/DNI: `{u['username']}`")
    st.markdown(f"**Nombre:** {u['full_name']}")
    
    roles_list = ["admin", "nomina", "jefe_area", "coordinador", "empleado"]
    role_idx = roles_list.index(u['role']) if u['role'] in roles_list else len(roles_list)-1
    new_role = st.selectbox("Rol", roles_list, index=role_idx)
    
    depts = sorted([d for d in emp_df['department'].unique() if d])
    dept_idx = 0
    if u.get('managed_department') in depts:
        dept_idx = depts.index(u['managed_department']) + 1
        
    new_managed_dept = st.selectbox("Departamento a Cargo (Aplica para Coordinadores)", options=[""] + depts, index=dept_idx)
    
    areas_list = list(AREA_MAPPING.keys())
    area_idx = 0
    if u.get('managed_area') in areas_list:
        area_idx = areas_list.index(u['managed_area']) + 1
        
    new_managed_area = st.selectbox("Área a Cargo (Aplica para Jefes de Área)", options=[""] + areas_list, index=area_idx)

    st.markdown("---")
    st.write("**Área y Sub-área del Empleado (Para el Portal)**")
    
    current_area = u.get("emp_area")
    if current_area not in AREA_MAPPING: current_area = "Administrativo"
        
    new_emp_area = st.selectbox("Área", list(AREA_MAPPING.keys()), index=list(AREA_MAPPING.keys()).index(current_area))
    
    current_subarea = u.get("emp_subarea")
    if current_subarea not in AREA_MAPPING[new_emp_area]: current_subarea = AREA_MAPPING[new_emp_area][0]
    
    new_emp_subarea = st.selectbox("Sub-área / Cargo", AREA_MAPPING[new_emp_area], index=AREA_MAPPING[new_emp_area].index(current_subarea))
    
    st.markdown("---")
    st.write("**Datos de Contacto**")
    c3, c4 = st.columns(2)
    with c3:
        new_emp_phone = st.text_input("Teléfono Móvil", value=u.get("emp_phone", ""))
    with c4:
        new_emp_email = st.text_input("Correo Electrónico", value=u.get("emp_email", ""))
        
    st.markdown("---")
    
    c1, c2 = st.columns(2)
    with c1:
        new_active = st.checkbox("Activo (Puede iniciar sesión)", value=bool(u['active']))
    with c2:
        new_pw = st.text_input("Nueva Contraseña (Dejar en blanco para no cambiar)", type="password")
        
    submit_edit = st.button("Guardar Cambios", type="primary")
        
    st.markdown("---")
    with st.expander("⚠️ Zona de Peligro"):
        st.warning("Eliminar a este usuario revocará su acceso al sistema de forma permanente.")
        confirm_delete = st.checkbox("Entiendo que esta acción es irreversible y quiero eliminar este usuario.")
        submit_delete = st.button("🗑️ Eliminar Usuario", type="primary", disabled=not confirm_delete)
        
    if submit_edit:
        if new_role != "coordinador":
            new_managed_dept = ""
        if new_role != "jefe_area":
            new_managed_area = ""
            
        conn = db_conn()
        cur = conn.cursor()
        
        try:
            if new_pw:
                pw_hash = bcrypt.hashpw(new_pw.encode("utf-8"), bcrypt.gensalt())
                cur.execute("""
                    UPDATE users_app 
                    SET role = ?, managed_department = ?, active = ?, password_hash = ?, emp_area = ?, emp_subarea = ?, emp_phone = ?, emp_email = ?, managed_area = ?
                    WHERE username = ?
                """, (new_role, new_managed_dept, 1 if new_active else 0, pw_hash, new_emp_area, new_emp_subarea, new_emp_phone, new_emp_email, new_managed_area, username))
            else:
                cur.execute("""
                    UPDATE users_app 
                    SET role = ?, managed_department = ?, active = ?, emp_area = ?, emp_subarea = ?, emp_phone = ?, emp_email = ?, managed_area = ?
                    WHERE username = ?
                """, (new_role, new_managed_dept, 1 if new_active else 0, new_emp_area, new_emp_subarea, new_emp_phone, new_emp_email, new_managed_area, username))
                
            conn.commit()
            log_audit("EDIT_USER", f"Usuario actualizado: {username} (Rol: {new_role})")
            st.success("✅ Cambios guardados correctamente.")
            get_users_by_role.clear() # Evitamos que el caché retenga data vieja
            if "admin_users_table" in st.session_state: del st.session_state["admin_users_table"]
            if "emp_users_table" in st.session_state: del st.session_state["emp_users_table"]
            st.rerun()
        except Exception as e:
            st.error(f"Error actualizando usuario: {e}")
        finally:
            conn.close()

    if submit_delete:
        conn = db_conn()
        cur = conn.cursor()
        cur.execute("DELETE FROM users_app WHERE username = ?", (username,))
        conn.commit()
        conn.close()
        log_audit("DELETE_USER", f"Usuario eliminado del sistema: {username}")
        st.success(f"🗑️ Usuario {username} eliminado del sistema.")
        get_users_by_role.clear() # Limpiamos el caché inmediatamente de la base de datos
        if "admin_users_table" in st.session_state: del st.session_state["admin_users_table"]
        if "emp_users_table" in st.session_state: del st.session_state["emp_users_table"]
        st.rerun()

def page_users_admin():
    require_role("admin")
    st.title("👥 Gestión de Usuarios")
    st.write("Administra los accesos al portal de Nómina Dolormed.")
    
    tab1, tab2, tab3, tab4 = st.tabs(["📝 Registrar Nuevo", "👔 Portal Administrativo", "🛠️ Portal Empleados", "⚙️ Servidor de Correos"])

    with tab1:
        st.subheader("Datos del Nuevo Usuario")
        conn = db_conn()
        emp_df = pd.read_sql_query("SELECT user_id, full_name, department FROM employees ORDER BY full_name", conn)
        conn.close()
        
        if emp_df.empty:
            st.warning("No hay empleados en el directorio. Importa empleados primero antes de crear usuarios.")
        else:
            selected_emp = st.selectbox(
                "Empleado (A quien se le creará el usuario)",
                options=emp_df['user_id'].tolist(),
                format_func=lambda uid: f"{uid} - {emp_df[emp_df['user_id']==uid]['full_name'].values[0]}"
            )
            role = st.selectbox("Rol", ["admin", "nomina", "jefe_area", "coordinador", "empleado"])
            
            depts = sorted([d for d in emp_df['department'].unique() if d])
            managed_dept = st.selectbox("Departamento a Cargo (Solo aplica para Coordinadores)", options=[""] + depts)
            
            areas = list(AREA_MAPPING.keys())
            managed_area = st.selectbox("Área a Cargo (Solo aplica para Jefes de Área)", options=[""] + areas)
            
            st.markdown("---")
            sel_area = st.selectbox("Área", list(AREA_MAPPING.keys()))
            sel_subarea = st.selectbox("Sub-área / Cargo", AREA_MAPPING[sel_area])
            st.markdown("---")
            
            c_p1, c_p2 = st.columns(2)
            with c_p1:
                new_phone = st.text_input("Teléfono Móvil (Opcional)")
            with c_p2:
                new_email = st.text_input("Correo Electrónico (Opcional)")
                
            st.markdown("---")
            
            pw = st.text_input("Contraseña inicial", type="password")
            active = st.checkbox("Activo", value=True)
            submit = st.button("Crear / Actualizar Usuario", type="primary")

            if submit:
                if not pw:
                    st.error("Completa la contraseña.")
                else:
                    u = str(selected_emp).strip()
                    full = emp_df[emp_df['user_id']==selected_emp]['full_name'].values[0]
                    
                    if role != "coordinador":
                        managed_dept = ""
                    if role != "jefe_area":
                        managed_area = ""

                    pw_hash = bcrypt.hashpw(pw.encode("utf-8"), bcrypt.gensalt())
                    conn = db_conn()
                    cur = conn.cursor()
                    cur.execute("""
                        INSERT INTO users_app(username, full_name, role, password_hash, active, created_at, managed_department, emp_area, emp_subarea, emp_phone, emp_email, managed_area)
                        VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
                        ON CONFLICT(username) DO UPDATE SET
                            full_name=excluded.full_name,
                            role=excluded.role,
                            password_hash=excluded.password_hash,
                            active=excluded.active,
                            managed_department=excluded.managed_department,
                            emp_area=excluded.emp_area,
                            emp_subarea=excluded.emp_subarea,
                            emp_phone=excluded.emp_phone,
                            emp_email=excluded.emp_email,
                            managed_area=excluded.managed_area
                    """, (u, full, role, pw_hash, 1 if active else 0, datetime.now().isoformat(timespec="seconds"), managed_dept, sel_area, sel_subarea, new_phone, new_email, managed_area))
                    conn.commit()
                    conn.close()
                    
                    log_audit("CREATE_USER", f"Usuario creado/actualizado: {u} ({full}) con rol {role}")
                    st.success(f"Usuario {u} ({full}) creado/actualizado correctamente.")
                    
                    if new_email and active:
                        st.info("Intentando enviar correo de bienvenida...")
                        ok, msg = send_welcome_email(new_email, full, u, pw)
                        if ok:
                            st.success("📧 Correo de bienvenida enviado exitosamente.")
                        else:
                            st.warning(f"⚠️ El usuario fue creado, pero falló el envío del correo: {msg}")
                            
                    get_users_by_role.clear()

    with tab2:
        admin_df = get_users_by_role(['admin', 'nomina', 'jefe_area', 'coordinador'])
    
        if not admin_df.empty:
            admin_df_view = admin_df.copy()
            admin_df_view['active'] = admin_df_view['active'].apply(lambda x: '✅ Sí' if x == 1 else '❌ No')
            admin_df_view.columns = ["Usuario (DNI)", "Nombre Completo", "Rol", "Depto. a Cargo", "Activo", "Creado el"]
        
            st.info("💡 Haz clic en una fila para editar o eliminar.")
            
            if 'last_processed_admin_user' not in st.session_state:
                st.session_state.last_processed_admin_user = None
                
            event_admin = st.dataframe(
                admin_df_view, use_container_width=True, hide_index=True, 
                on_select="rerun", selection_mode="single-row", key="admin_users_table"
            )
        
            if len(event_admin.selection.rows) > 0:
                row_idx = event_admin.selection.rows[0]
                if row_idx < len(admin_df):
                    selected_username = str(admin_df.iloc[row_idx]["username"]) 
                    
                    if selected_username != st.session_state.last_processed_admin_user:
                        st.session_state.last_processed_admin_user = selected_username
                        emp_df = get_all_employees()
                        edit_user_dialog(selected_username, emp_df)
            else:
                st.session_state.last_processed_admin_user = None

    with tab3:
        emp_users_df = get_users_by_role(['empleado'])
    
        if not emp_users_df.empty:
            emp_users_df_view = emp_users_df.copy()
            emp_users_df_view['active'] = emp_users_df_view['active'].apply(lambda x: '✅ Sí' if x == 1 else '❌ No')
            emp_users_df_view.columns = ["Usuario (DNI)", "Nombre Completo", "Área", "Sub-área", "Activo", "Creado el"]
        
            if 'last_processed_emp_user' not in st.session_state:
                st.session_state.last_processed_emp_user = None
                
            event_emp = st.dataframe(
                emp_users_df_view, use_container_width=True, hide_index=True, 
                on_select="rerun", selection_mode="single-row", key="emp_users_table"
            )
        
            if len(event_emp.selection.rows) > 0:
                row_idx = event_emp.selection.rows[0]
                if row_idx < len(emp_users_df):
                    selected_username = str(emp_users_df.iloc[row_idx]["username"])
                    
                    if selected_username != st.session_state.last_processed_emp_user:
                        st.session_state.last_processed_emp_user = selected_username
                        emp_df = get_all_employees()
                        edit_user_dialog(selected_username, emp_df)
            else:
                st.session_state.last_processed_emp_user = None

    with tab4:
        st.subheader("Configuración del Servidor de Correo (SMTP)")
        st.write("Configura la cuenta de correo desde donde el sistema enviará las alertas y credenciales.")
        
        cfg = load_smtp_config()
        with st.form("smtp_form"):
            s_host = st.text_input("Servidor SMTP (Ej. smtp.gmail.com)", value=cfg.get("smtp_server", "smtp.gmail.com"))
            s_port = st.number_input("Puerto (Ej. 587 para TLS o 465 para SSL)", value=int(cfg.get("smtp_port", 587)), min_value=1)
            s_user = st.text_input("Correo Emisor", value=cfg.get("smtp_user", ""))
            s_pass = st.text_input("Contraseña de Aplicación", value=cfg.get("smtp_password", ""), type="password")
            s_name = st.text_input("Nombre del Remitente", value=cfg.get("sender_name", "Nómina Dolormed"))
            
            st.info("Nota: Si usas Gmail, recuerda habilitar la Verificación en 2 Pasos y generar una 'Contraseña de Aplicación'. No uses tu contraseña normal.")
            sub_smtp = st.form_submit_button("💾 Guardar Configuración", type="primary")
            if sub_smtp:
                new_cfg = {
                    "smtp_server": s_host.strip(),
                    "smtp_port": s_port,
                    "smtp_user": s_user.strip(),
                    "smtp_password": s_pass.strip(),
                    "sender_name": s_name.strip()
                }
                if save_smtp_config(new_cfg):
                    st.success("✅ Configuración SMTP guardada correctamente.")
                else:
                    st.error("❌ Error al guardar la configuración.")