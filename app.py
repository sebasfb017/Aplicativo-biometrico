import os
import streamlit as st
from streamlit_option_menu import option_menu

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

    # --- CSS GLOBAL (ESTÉTICA PREMIUM Y FLUIDEZ) ---
    # Inyectamos estilos CSS personalizados para darle a la aplicación un aspecto moderno.
    # Evitamos usar el archivo de configuración global config.toml para no bloquear 
    # el botón nativo de Modo Oscuro/Claro del navegador de Streamlit.
    st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;500;600;700&display=swap');
    
    /* Tipografía Global y Variables */
    html, body, [class*="css"] {
        font-family: 'Outfit', sans-serif !important;
    }
    
    :root {
        --primary-blue: #0D6EFD;
        --glow-blue: rgba(13, 110, 253, 0.4);
        --glass-bg: rgba(255, 255, 255, 0.03);
        --glass-border: rgba(200, 200, 200, 0.15);
    }
    
    /* Animación principal de renderizado (Fade-In) */
    @keyframes fadeIn {
        0% { opacity: 0; transform: translateY(15px); }
        100% { opacity: 1; transform: translateY(0); }
    }
    
    .main .block-container {
        animation: fadeIn 0.6s cubic-bezier(0.16, 1, 0.3, 1) forwards;
    }
    
    /* Efecto Glassmorphism y Elevación Premium en Métricas */
    div[data-testid="stMetric"] {
        border-radius: 16px;
        padding: 20px;
        background: var(--glass-bg);
        backdrop-filter: blur(12px);
        -webkit-backdrop-filter: blur(12px);
        box-shadow: 0 8px 32px rgba(0, 0, 0, 0.05);
        transition: transform 0.3s cubic-bezier(0.175, 0.885, 0.32, 1.275), box-shadow 0.3s ease;
        border: 1px solid var(--glass-border);
    }
    div[data-testid="stMetric"]:hover {
        transform: translateY(-6px) scale(1.02);
        box-shadow: 0 15px 30px var(--glow-blue);
        border-color: var(--primary-blue);
    }
    
    /* Estilizar botones para efecto premium */
    /* ATENCIÓN: Forzamos el color azul aquí (var(--primary-blue)) para los botones principales,
       de modo que sigan siendo azules en el Modo Oscuro nativo de Streamlit en lugar de cambiar a rojo. */
    button[kind="primary"] {
        background-color: var(--primary-blue) !important;
        color: white !important;
        border-color: var(--primary-blue) !important;
        border-radius: 10px !important;
        font-weight: 600 !important;
        letter-spacing: 0.5px;
        transition: all 0.3s ease !important;
    }
    button[kind="primary"]:hover {
        transform: translateY(-3px);
        box-shadow: 0 6px 18px var(--glow-blue) !important;
    }
    
    /* Contenedores Expander con hover Premium */
    div[data-testid="stExpander"] {
        border-radius: 12px !important;
        background: var(--glass-bg);
        border: 1px solid var(--glass-border);
        transition: all 0.3s ease;
        overflow: hidden;
    }
    div[data-testid="stExpander"]:hover {
        border-color: var(--primary-blue);
        box-shadow: 0 4px 15px rgba(0,0,0,0.05);
    }

    /* Skeleton Loader para carga asíncrona */
    @keyframes skeleton-loading {
        0% { background-position: 200% 0; }
        100% { background-position: -200% 0; }
    }
    
    .skeleton-box {
        display: block;
        height: 120px;
        width: 100%;
        background: linear-gradient(90deg, #f0f0f0 25%, #e0e0e0 50%, #f0f0f0 75%);
        background-size: 200% 100%;
        border-radius: 12px;
        animation: skeleton-loading 1.5s infinite linear;
        margin-bottom: 1rem;
        border: 1px solid var(--glass-border);
    }
    
    /* Adaptación del skeleton a modo oscuro */
    @media (prefers-color-scheme: dark) {
        .skeleton-box {
            background: linear-gradient(90deg, #2b2b2b 25%, #3b3b3b 50%, #2b2b2b 75%);
            background-size: 200% 100%;
        }
    }
    </style>
    """, unsafe_allow_html=True)

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

    # Obtener el rol real del usuario
    user_role = user["role"]
    
    # Asignación de rol efectivo: si el rol es auxiliar ('empleado') y su sub-área es Nómina o Talento Humano,
    # le asignamos dinámicamente los permisos y la interfaz del rol administrativo 'nomina' para que 
    # puedan acceder al panel de RRHH sin ser administradores globales.
    if user_role == "empleado" and user.get("emp_subarea") in ["Nomina", "Talento humano"]:
        user_role = "nomina"

    # --- INACTIVITY TIMEOUT (10 min) ---
    # Sistema de seguridad que cierra la sesión automáticamente si el usuario 
    # no interactúa con la aplicación durante 10 minutos seguidos.
    from datetime import datetime, timedelta
    last_activity = st.session_state.get("last_activity")
    now = datetime.now()
    if last_activity:
        if now - last_activity > timedelta(minutes=10):
            st.session_state.clear()
            st.error("Sesión cerrada automáticamente por 10 minutos de inactividad por seguridad.")
            st.rerun()
    st.session_state["last_activity"] = now
    # ------------------------------------

    # --- NOTIFICACIONES EN PANTALLA (TOASTS) ---
    if not st.session_state.get("notified"):
        st.session_state["notified"] = True
        
        if user_role == "empleado":
            st.toast(f"¡Hola {user['full_name'].split()[0]}! Bienvenido a tu Portal de Autogestión.", icon="👋")
        # Ocultar o eliminar el mensaje emergente de pendientes a petición del usuario.
        pass
    # -------------------------------------------

    st.sidebar.markdown(f"<h2 style='text-align: center; color: #0066cc;'>Dolormed RRHH</h2>", unsafe_allow_html=True)
    st.sidebar.markdown(f"<div style='text-align: center; color: gray; margin-bottom: 20px;'>Hola, <b>{user['full_name']}</b><br><small>({user['role'].upper()})</small></div>", unsafe_allow_html=True)

    ROLES_MENU = {
        "admin": (["Dashboard", "Mi Portal de Autogestión", "Reportes Mensuales", "Expediente 360", "Novedades y Excepciones", "Sincronizar Relojes", "Visualizar Data", "---", "Empleados", "Turnos y Asignación", "Usuarios"],
                  ["house", "person-vcard", "bar-chart-line", "person-badge-fill", "journal-medical", "arrow-repeat", "table", "", "people", "calendar-check", "person-badge"]),
        "empleado": (["Mi Portal de Autogestión"], ["person-vcard"]),
        "coordinador": (["Mi Portal de Autogestión", "Autorización de Permisos", "Carga Masiva de Turnos"], ["person-vcard", "check2-square", "file-earmark-excel"]),
        "jefe_area": (["Mi Portal de Autogestión", "Autorización de Permisos"], ["person-vcard", "check2-square"]),
        "nomina": (["Dashboard", "Mi Portal de Autogestión", "Reportes Mensuales", "Expediente 360", "Novedades y Excepciones", "Sincronizar Relojes", "Visualizar Data", "---", "Empleados", "Turnos y Asignación", "Usuarios"], 
                   ["house", "person-vcard", "bar-chart-line", "person-badge-fill", "journal-medical", "arrow-repeat", "table", "", "people", "calendar-check", "person-badge"])
    }
    menu_options, menu_icons = ROLES_MENU.get(user_role, ROLES_MENU["nomina"])

    with st.sidebar:
        # --- CAMPANA DE NOTIFICACIONES ---
        from database_conn.queries import (
            db_get_unread_notifications_count,
            db_get_recent_notifications,
            db_mark_all_notifications_read
        )
        unread_count = db_get_unread_notifications_count(user['username'])
        bell_label = f"🔔 Notificaciones ({unread_count})" if unread_count > 0 else "🔔 Notificaciones"
        
        with st.popover(bell_label, use_container_width=True):
            st.markdown("<h4 style='margin:0;'>Mis Notificaciones</h4>", unsafe_allow_html=True)
            st.markdown("---")
            notifs = db_get_recent_notifications(user['username'])
            if not notifs:
                st.write("No tienes notificaciones recientes.")
            else:
                for n in notifs:
                    icon = "🟢" if not n['is_read'] else "⚪"
                    st.markdown(f"**{icon} {n['title']}**\n<small>{n['message']}</small>\n<small style='color:gray; font-size: 0.75rem; display: block; margin-top: 2px;'>{n['created_at']}</small>", unsafe_allow_html=True)
                    st.markdown("<hr style='margin: 8px 0;'>", unsafe_allow_html=True)
                if st.button("Marcar todas como leídas", key="mark_all_read_top_btn", use_container_width=True):
                    db_mark_all_notifications_read(user['username'])
                    st.rerun()
        st.markdown("<br>", unsafe_allow_html=True)
        # ----------------------------------
        
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
        
        if st.button("Cerrar Sesión", type="primary", use_container_width=True):
            st.session_state.clear()
            st.rerun()

    # --- ENRUTADOR DE VISTAS (ROUTER) ---
    # Diccionario que mapea los nombres de las pestañas en el menú lateral 
    # a las funciones correspondientes que renderizan las páginas.
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

    # El enrutador maneja la navegación a las diferentes páginas

    if sel == "Turnos y Asignación":
        tab1, tab2, tab3 = st.tabs(["🏗️ Crear Turnos", "📝 Asignar a Empleados", "📥 Carga Masiva (Excel)"])
        with tab1:
            page_shifts()
        with tab2:
            page_assign_shifts()
        with tab3:
            page_bulk_assign_shifts()
    elif sel == "Carga Masiva de Turnos":
        page_bulk_assign_shifts()
    elif sel in router:
        router[sel]()

if __name__ == "__main__":
    main()

# --- EXPOSICIÓN DE ATRIBUTOS Y FUNCIONES PARA PRUEBAS (TESTS BACKWARD COMPATIBILITY) ---
import bcrypt # noqa: E402, F401
from datetime import datetime # noqa: E402, F401
import types
import sys

from database_conn.connection import DATA_DIR as _DATA_DIR, DB_PATH as _DB_PATH # noqa: E402, F401
from database_conn.setup import init_db # noqa: E402, F401
from utils.auth import get_user, verify_login # noqa: E402, F401
from views.schedules_view import ( # noqa: E402, F401
    ensure_schedules_columns,
    maybe_load_default_schedules,
    upsert_schedule_df,
    resolve_shift_from_code,
    upsert_shifts_from_code_csv,
    generate_rotating_schedule,
    auto_assign_shifts_from_schedules
)
from services.analytics import ( # noqa: E402, F401
    schedule_for_date,
    compute_month_lateness,
    to_excel_bytes,
    get_shift_for_user_date,
    schedule_for_user_date,
    get_late_punch_ids
)
from services.zk_service import upsert_attendance, load_devices # noqa: E402, F401
from database_conn.queries import ( # noqa: E402, F401
    upsert_employees_df,
    upsert_shift,
    get_shifts_df,
    assign_shift,
    is_holiday,
    get_profile_by_name,
    calculate_overnight_surcharge
)

# Contenedor de espacio de nombres personalizado para permitir enrutamiento dinámico y parches durante las pruebas
class AppNamespace(types.ModuleType):
    @property
    def DATA_DIR(self):
        import database_conn.connection
        return database_conn.connection.DATA_DIR

    @DATA_DIR.setter
    def DATA_DIR(self, value):
        import database_conn.connection
        database_conn.connection.DATA_DIR = value

    @property
    def DB_PATH(self):
        import database_conn.connection
        return database_conn.connection.DB_PATH

    @DB_PATH.setter
    def DB_PATH(self, value):
        import database_conn.connection
        database_conn.connection.DB_PATH = value
        database_conn.connection.DATA_DIR = os.path.dirname(value)

    @property
    def DEVICES_YAML(self):
        import services.zk_service
        return services.zk_service.DEVICES_YAML

    @DEVICES_YAML.setter
    def DEVICES_YAML(self, value):
        import services.zk_service
        services.zk_service.DEVICES_YAML = value

sys.modules[__name__].__class__ = AppNamespace
