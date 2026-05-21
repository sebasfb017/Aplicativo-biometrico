import os
import streamlit as st
from streamlit_option_menu import option_menu

# --- Inicialización del Tema (Debe ser lo primero en el script) ---
# En lugar de usar la sesión (que se pierde al recargar), 
# leemos qué tema está configurado actualmente en config.toml.
def get_current_theme():
    config_path = os.path.join(os.path.dirname(__file__), ".streamlit", "config.toml")
    if os.path.exists(config_path):
        with open(config_path, "r", encoding="utf-8") as f:
            if 'base="dark"' in f.read():
                return "dark"
    return "light" # Fallback si no existe o dice light

def apply_theme(theme):
    config_path = os.path.join(os.path.dirname(__file__), ".streamlit", "config.toml")
    new_content = f'''[theme]
primaryColor="#0D6EFD"
base="{theme}"
font="sans serif"
'''
    os.makedirs(os.path.dirname(config_path), exist_ok=True)
    with open(config_path, "w", encoding="utf-8") as f:
        f.write(new_content)

# Leemos el tema directamente del archivo de configuración, esto lo hace persistente siempre.
current_theme = get_current_theme()

# Si deseas forzar el tema oscuro por defecto la primera vez, 
# puedes habilitar esto, pero actualmente respetará lo que diga el archivo.
if "theme_initialized" not in st.session_state:
    st.session_state.theme_initialized = True
    # Si quisieras que el predeterminado absoluto fuera oscuro para nuevos usuarios, harías:
    # si el archivo no existiera, lo creas con dark. 
    # Por ahora, simplemente confiaremos en get_current_theme()

# --- Fin de la inicialización del Tema ---

from database_conn.connection import db_conn
from database_conn.setup import init_db
from utils.auth import require_role
from views.auth_view import page_login
from views.dashboard_view import page_dashboard
from views.employees_view import page_employees
from views.sync_zkteco_view import page_sync
from views.employee_portal_view import page_employee_portal
from views.schedules_view import page_shifts, page_assign_shifts, page_bulk_assign_shifts
from views.exceptions_view import page_exceptions
from views.attendance_view import page_view_attendance
from views.month_report_view import page_lateness_report
from views.users_admin_view import page_users_admin
from views.employee_360_view import page_employee_360


def main():
    st.set_page_config(page_title="Nómina Dolormed", layout="wide", page_icon="🏢")
    init_db()

    # --- INICIO BACKGROUND SCHEDULER ---
    @st.cache_resource
    def init_scheduler():
        try:
            from apscheduler.schedulers.background import BackgroundScheduler
            from services.zk_service import automated_daily_sync
            scheduler = BackgroundScheduler()
            # Sincronizar todos los días a las 23:59
            scheduler.add_job(automated_daily_sync, 'cron', hour=23, minute=59)
            scheduler.start()
            return scheduler
        except ImportError:
            return None
            
    _ = init_scheduler()
    # --- FIN BACKGROUND SCHEDULER ---

    user = st.session_state.get("user")
    if not user:
        col1, col2, col3 = st.columns([1, 2, 1])
        with col2:
            page_login()
        return

    # --- INACTIVITY TIMEOUT (10 min) ---
    from datetime import datetime, timedelta
    last_activity = st.session_state.get("last_activity")
    now = datetime.now()
    if last_activity:
        if now - last_activity > timedelta(minutes=10):
            st.session_state.clear()
            st.warning("⏱️ Sesión cerrada automáticamente por 10 minutos de inactividad por seguridad.")
            st.rerun()
    st.session_state["last_activity"] = now
    # ------------------------------------

    st.sidebar.markdown(f"<h2 style='text-align: center; color: #0066cc;'>Dolormed RRHH</h2>", unsafe_allow_html=True)
    st.sidebar.markdown(f"<div style='text-align: center; color: gray; margin-bottom: 20px;'>Hola, <b>{user['full_name']}</b><br><small>({user['role'].upper()})</small></div>", unsafe_allow_html=True)

    ROLES_MENU = {
        "admin": (["Dashboard", "Reportes Mensuales", "Expediente 360", "Novedades y Excepciones", "Sincronizar Relojes", "Visualizar Data", "---", "Empleados", "Turnos y Asignación", "Usuarios"],
                  ["house", "bar-chart-line", "person-badge-fill", "journal-medical", "arrow-repeat", "table", "", "people", "calendar-check", "person-badge"]),
        "empleado": (["Mi Portal de Autogestión"], ["person-vcard"]),
        "coordinador": (["Dashboard", "Autorización de Permisos", "Visualizar Data"], ["house", "check2-square", "table"]),
        "nomina": (["Dashboard", "Reportes Mensuales", "Expediente 360", "Novedades y Excepciones", "Sincronizar Relojes", "Visualizar Data"], 
                   ["house", "bar-chart-line", "person-badge-fill", "journal-medical", "arrow-repeat", "table"])
    }
    menu_options, menu_icons = ROLES_MENU.get(user["role"], ROLES_MENU["nomina"])

    with st.sidebar:
        sel = option_menu(
            menu_title=None,
            options=menu_options,
            icons=menu_icons,
            menu_icon="cast",
            default_index=0,
            styles={
                "container": {"padding": "0!important", "background-color": "transparent"},
                "icon": {"color": "#0066cc", "font-size": "18px"},
                "nav-link": {"font-size": "15px", "text-align": "left", "margin":"0px", "--hover-color": "#e9ecef"},
                "nav-link-selected": {"background-color": "#0066cc", "color": "white", "icon-color":"white"},
            }
        )
        st.markdown("<br><br>", unsafe_allow_html=True)
        
        current_mode_icon = "🌙" if current_theme == "dark" else "☀️"
        current_mode_text = "Oscuro" if current_theme == "dark" else "Claro"
        
        if st.button(f"Cambiar a Tema {current_mode_icon} {current_mode_text}", use_container_width=True):
            new_theme = "dark" if current_theme == "light" else "light"
            apply_theme(new_theme)
            import time
            time.sleep(0.3) # Pequeña pausa para asegurar que el archivo se escriba antes del rerun
            st.rerun()
            
        if st.button("Cerrar Sesión", type="primary", use_container_width=True):
            st.session_state.clear()
            st.rerun()

    router = {
        "Dashboard": page_dashboard,
        "Sincronizar Relojes": page_sync,
        "Visualizar Data": page_view_attendance,
        "Reportes Mensuales": page_lateness_report,
        "Expediente 360": page_employee_360,
        "Novedades y Excepciones": page_exceptions,
        "Autorización de Permisos": page_exceptions,
        "Empleados": page_employees,
        "Usuarios": page_users_admin,
        "Mi Portal de Autogestión": page_employee_portal
    }

    if sel == "Turnos y Asignación":
        tab1, tab2, tab3 = st.tabs(["🏗️ Crear Turnos", "📝 Asignar a Empleados", "📥 Carga Masiva (Excel)"])
        with tab1:
            page_shifts()
        with tab2:
            page_assign_shifts()
        with tab3:
            page_bulk_assign_shifts()
    elif sel in router:
        router[sel]()

if __name__ == "__main__":
    main()