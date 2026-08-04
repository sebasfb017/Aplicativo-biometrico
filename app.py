import os
from datetime import datetime, timedelta

import streamlit as st
from streamlit_option_menu import option_menu

# --- Fin de la inicialización del Tema ---
from database_conn.setup import init_db
from views.attendance_view import page_view_attendance
from views.auth_view import page_login
from views.dashboard_view import page_dashboard
from views.employee_360_view import page_employee_360
from views.employee_portal_view import page_employee_portal
from views.employees_view import page_employees
from views.exceptions_view import page_exceptions
from views.month_report_view import page_lateness_report
from views.schedules_view import (
    page_assign_shifts,
    page_bulk_assign_shifts,
    page_shifts,
)
from views.sync_zkteco_view import page_sync
from views.users_admin_view import page_users_admin


def main():
    st.set_page_config(page_title="Nómina Dolormed", layout="wide", page_icon="🏢")
    init_db()

    # --- INYECCIÓN PWA ---
    import streamlit.components.v1 as components

    components.html(
        """
    <script>
        const parent = window.parent.document;
        if (!parent.querySelector('link[rel="manifest"]')) {
            const manifestLink = parent.createElement('link');
            manifestLink.rel = 'manifest';
            manifestLink.href = '/app/static/manifest.json';
            parent.head.appendChild(manifestLink);
            
            // Meta tags para PWA móvil (Apple)
            const metaApple = parent.createElement('meta');
            metaApple.name = 'apple-mobile-web-app-capable';
            metaApple.content = 'yes';
            parent.head.appendChild(metaApple);
            
            const metaAppleTitle = parent.createElement('meta');
            metaAppleTitle.name = 'apple-mobile-web-app-title';
            metaAppleTitle.content = 'Dolormed';
            parent.head.appendChild(metaAppleTitle);
            
            const appleIcon = parent.createElement('link');
            appleIcon.rel = 'apple-touch-icon';
            appleIcon.href = '/app/static/icon-192x192.png';
            parent.head.appendChild(appleIcon);
            
            if ('serviceWorker' in window.parent.navigator) {
                window.parent.navigator.serviceWorker.register('/app/static/sw.js').then(function(registration) {
                    console.log('ServiceWorker registration successful');
                }).catch(function(err) {
                    console.log('ServiceWorker registration failed: ', err);
                });
            }
        }
    </script>
    """,
        height=0,
        width=0,
    )

    # --- CSS GLOBAL (ESTÉTICA PREMIUM Y FLUIDEZ) ---
    # Inyectamos estilos CSS personalizados para darle a la aplicación un aspecto moderno.
    # Evitamos usar el archivo de configuración global config.toml para no bloquear
    # el botón nativo de Modo Oscuro/Claro del navegador de Streamlit.
    st.markdown(
        """
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;500;600;700&display=swap');
    
    /* Tipografía Global y Variables */
    html, body, [class*="css"] {
        font-family: 'Outfit', sans-serif !important;
    }
    
    :root {
        --primary-blue: #6366f1;
        --glow-blue: rgba(99, 102, 241, 0.4);
        --glass-bg: rgba(255, 255, 255, 0.05);
        --glass-border: rgba(255, 255, 255, 0.1);
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
        border-radius: 14px !important;
        background: var(--glass-bg);
        border: 1px solid var(--glass-border);
        transition: all 0.3s ease;
        overflow: hidden;
    }
    div[data-testid="stExpander"]:hover {
        border-color: var(--primary-blue);
        box-shadow: 0 4px 15px rgba(0,0,0,0.05);
    }
    
    /* Sombras y bordes redondeados para Tablas y Dataframes */
    div[data-testid="stDataFrame"] > div, div[data-testid="stTable"] > div {
        border-radius: 14px !important;
        overflow: hidden !important;
        box-shadow: 0 4px 16px rgba(0,0,0,0.06) !important;
        border: 1px solid var(--glass-border) !important;
    }
    
    /* Inputs, Selects y TextAreas redondeados */
    div[data-testid="stTextInput"] input, 
    div[data-testid="stSelectbox"] div[data-baseweb="select"], 
    div[data-testid="stNumberInput"] input, 
    div[data-testid="stTextArea"] textarea {
        border-radius: 10px !important;
        border: 1px solid rgba(128,128,128,0.2) !important;
        transition: box-shadow 0.2s, border-color 0.2s !important;
    }
    div[data-testid="stTextInput"] input:focus, 
    div[data-testid="stSelectbox"] div[data-baseweb="select"]:focus-within, 
    div[data-testid="stTextArea"] textarea:focus {
        box-shadow: 0 0 0 2px var(--glow-blue) !important;
        border-color: var(--primary-blue) !important;
    }
    
    /* Estilizar los Formularios como Tarjetas (Cards) */
    div[data-testid="stForm"] {
        background-color: var(--glass-bg);
        border-radius: 16px !important;
        padding: 24px !important;
        box-shadow: 0 8px 24px rgba(0,0,0,0.04) !important;
        border: 1px solid var(--glass-border) !important;
    }
    
    /* Estilizar la barra lateral (Sidebar) */
    section[data-testid="stSidebar"] {
        background: linear-gradient(180deg, #111827 0%, #1F2937 100%) !important;
        box-shadow: 4px 0 20px rgba(0,0,0,0.3) !important;
        border-right: 1px solid rgba(255,255,255,0.05) !important;
    }
    
    /* Botones del Menú Lateral (Sidebar) */
    section[data-testid="stSidebar"] .stButton>button {
        border-radius: 12px !important;
        background: rgba(255,255,255,0.03) !important;
        border: 1px solid rgba(255,255,255,0.05) !important;
        color: #e5e7eb !important;
        text-align: left !important;
        justify-content: flex-start !important;
        padding-left: 20px !important;
        font-weight: 500 !important;
        transition: all 0.3s cubic-bezier(0.25, 0.8, 0.25, 1) !important;
        margin-bottom: 8px !important;
    }
    
    section[data-testid="stSidebar"] .stButton>button:hover {
        background: var(--primary-blue) !important;
        border-color: var(--primary-blue) !important;
        color: white !important;
        transform: translateX(6px) !important;
        box-shadow: 0 4px 15px var(--glow-blue) !important;
    }
    
    /* Separador sutil del sidebar */
    section[data-testid="stSidebar"] hr {
        border-top: 1px solid rgba(255,255,255,0.08) !important;
    }
    
    /* White-Labeling (Ocultar branding de Streamlit) */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    header[data-testid="stHeader"] {background: transparent; box-shadow: none;}
    
    /* Pestañas (Tabs) estilo "Pill" modernas */
    button[data-baseweb="tab"] {
        border-radius: 12px 12px 0 0 !important;
        background-color: var(--glass-bg) !important;
        border: 1px solid var(--glass-border) !important;
        border-bottom: none !important;
        margin-right: 4px;
        transition: all 0.3s ease;
    }
    button[data-baseweb="tab"]:hover {
        background-color: rgba(99, 102, 241, 0.1) !important;
        transform: translateY(-2px);
    }
    button[aria-selected="true"] {
        background-color: var(--primary-blue) !important;
        color: white !important;
        border-color: var(--primary-blue) !important;
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
        border-radius: 14px;
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
    
    /* Scrollbar Personalizada (Barras de desplazamiento) */
    ::-webkit-scrollbar {
        width: 8px;
        height: 8px;
    }
    ::-webkit-scrollbar-track {
        background: transparent;
    }
    ::-webkit-scrollbar-thumb {
        background: rgba(13, 110, 253, 0.3);
        border-radius: 10px;
    }
    ::-webkit-scrollbar-thumb:hover {
        background: rgba(13, 110, 253, 0.7);
    }

    /* Animación de Carga (Spinners) y Barra Superior */
    div[data-testid="stSpinner"] circle {
        stroke: var(--primary-blue) !important;
    }
    div[data-testid="stSpinner"] div {
        border-top-color: var(--primary-blue) !important;
        border-left-color: var(--primary-blue) !important;
    }
    /* El indicador de "Running..." arriba a la derecha (Custom Loading) */
    div[data-testid="stStatusWidget"] {
        background: linear-gradient(90deg, #1E293B, #0F172A) !important;
        border: 1px solid rgba(255,255,255,0.1) !important;
        border-radius: 20px !important;
        box-shadow: 0 4px 15px rgba(0,0,0,0.3), 0 0 10px rgba(99,102,241,0.2) !important;
        padding: 4px 16px !important;
    }
    div[data-testid="stStatusWidget"] label {
        color: #e5e7eb !important;
        font-weight: 500 !important;
        letter-spacing: 0.5px;
    }
    /* Ocultar muñequito corriendo (svg) si existe en la versión de streamlit */
    div[data-testid="stStatusWidget"] svg {
        display: none !important;
    }
    /* Añadir nuestro propio spinner (círculo) antes del texto */
    div[data-testid="stStatusWidget"]::before {
        content: "";
        display: inline-block;
        width: 14px;
        height: 14px;
        border: 2px solid rgba(99, 102, 241, 0.3);
        border-radius: 50%;
        border-top-color: #6366f1;
        animation: spin 1s ease-in-out infinite;
        margin-right: 8px;
        vertical-align: middle;
    }
    @keyframes spin {
        to { transform: rotate(360deg); }
    }
    /* === UI/UX 3.0: Sidebar Glassmorphism === */
    [data-testid="stSidebar"] {
        background-color: rgba(15, 23, 42, 0.95) !important;
        backdrop-filter: blur(20px) !important;
        -webkit-backdrop-filter: blur(20px) !important;
        border-right: 1px solid rgba(255, 255, 255, 0.1) !important;
        box-shadow: 5px 0 25px rgba(0,0,0,0.2);
    }
    
    /* Removed custom Tab CSS to prevent layout breaking */
    
    /* Animación de entrada de contenido de Pestañas */
    div[data-testid="stVerticalBlock"] > div > div[data-testid="stVerticalBlock"] {
        animation: slideUpTab 0.5s cubic-bezier(0.16, 1, 0.3, 1) forwards;
    }
    @keyframes slideUpTab {
        0% { opacity: 0; transform: translateY(15px); }
        100% { opacity: 1; transform: translateY(0); }
    }
    
    /* === UI/UX 3.0: Tarjetas HTML Personalizadas (Tablas) === */
    .premium-table-container {
        border-radius: 16px;
        overflow: hidden;
        box-shadow: 0 10px 30px -5px rgba(0, 0, 0, 0.1);
        border: 1px solid var(--glass-border);
        background: var(--glass-bg);
        margin-bottom: 20px;
    }
    .premium-table {
        width: 100%;
        border-collapse: collapse;
        font-family: 'Outfit', sans-serif;
    }
    .premium-table th {
        background: var(--glass-bg);
        color: var(--text-color);
        opacity: 0.8;
        font-weight: 600;
        text-transform: uppercase;
        font-size: 0.75rem;
        letter-spacing: 1px;
        padding: 15px 20px;
        text-align: left;
        border-bottom: 1px solid var(--glass-border);
    }
    .premium-table td {
        padding: 15px 20px;
        color: var(--text-color);
        font-size: 0.95rem;
        border-bottom: 1px solid var(--glass-border);
        transition: background-color 0.2s ease;
    }
    .premium-table tbody tr:hover td {
        background-color: rgba(99, 102, 241, 0.1);
    }
    .premium-table tbody tr:last-child td {
        border-bottom: none;
    }
    .badge-modern {
        padding: 6px 12px;
        border-radius: 20px;
        font-size: 0.8rem;
        font-weight: 600;
        display: inline-flex;
        align-items: center;
        gap: 6px;
    }
    .badge-blue { background: rgba(59, 130, 246, 0.1); color: #3b82f6; border: 1px solid rgba(59, 130, 246, 0.2); }
    .badge-emerald { background: rgba(16, 185, 129, 0.1); color: #10b981; border: 1px solid rgba(16, 185, 129, 0.2); }
    .badge-amber { background: rgba(245, 158, 11, 0.1); color: #d97706; border: 1px solid rgba(245, 158, 11, 0.2); }
    .badge-slate { background: rgba(100, 116, 139, 0.1); color: #475569; border: 1px solid rgba(100, 116, 139, 0.2); }
    </style>
    """,
        unsafe_allow_html=True,
    )

    # --- INICIO BACKGROUND SCHEDULER ---
    @st.cache_resource
    def init_scheduler():
        try:
            from apscheduler.schedulers.background import BackgroundScheduler

            from services.zk_service import automated_daily_sync

            scheduler = BackgroundScheduler()
            # Sincronizar todos los días a las 23:59
            scheduler.add_job(automated_daily_sync, "cron", hour=23, minute=59)
            scheduler.start()
            return scheduler
        except ImportError:
            return None

    _ = init_scheduler()
    # --- FIN BACKGROUND SCHEDULER ---

    from database_conn.queries import db_delete_session, db_validate_session

    # --- PERSISTENCIA DE SESIÓN POR TOKEN (QUERY PARAMS) ---
    if not st.session_state.get("user") and "session_token" in st.query_params:
        token = st.query_params["session_token"]
        user_info = db_validate_session(token)
        if user_info:
            st.session_state["user"] = user_info
            st.session_state["session_token"] = token

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
    if user_role == "empleado" and user.get("emp_subarea") in [
        "Nomina",
        "Talento humano",
    ]:
        user_role = "nomina"

    # --- INACTIVITY TIMEOUT (10 min) ---
    # Sistema de seguridad que cierra la sesión automáticamente si el usuario
    # no interactúa con la aplicación durante 10 minutos seguidos.
    last_activity = st.session_state.get("last_activity")
    now = datetime.now()
    if last_activity:
        if now - last_activity > timedelta(minutes=10):
            token = st.session_state.get("session_token")
            if token:
                try:
                    db_delete_session(token)
                except Exception:
                    pass
            st.query_params.clear()
            st.session_state.clear()
            st.error(
                "Sesión cerrada automáticamente por 10 minutos de inactividad por seguridad."
            )
            st.rerun()
    st.session_state["last_activity"] = now
    # ------------------------------------

    # --- NOTIFICACIONES EN PANTALLA (TOASTS) ---
    if not st.session_state.get("notified"):
        st.session_state["notified"] = True

        if user_role == "empleado":
            st.toast(
                f"¡Hola {user['full_name'].split()[0]}! Bienvenido a tu Portal de Autogestión.",
                icon="👋",
            )
        # Ocultar o eliminar el mensaje emergente de pendientes a petición del usuario.
    # -------------------------------------------

    current_hour = datetime.now().hour
    if current_hour < 12:
        greeting_msg = "☕ Buenos días"
    elif current_hour < 19:
        greeting_msg = "🌤️ Buenas tardes"
    else:
        greeting_msg = "🌙 Buenas noches"

    st.sidebar.markdown(
        "<h2 style='text-align: center; color: #0066cc;'>Dolormed RRHH</h2>",
        unsafe_allow_html=True,
    )
    st.sidebar.markdown(
        f"<div style='text-align: center; color: gray; margin-bottom: 20px;'>{greeting_msg}, <b>{user['full_name'].split()[0]}</b><br><small>({user['role'].upper()})</small></div>",
        unsafe_allow_html=True,
    )

    ROLES_MENU = {
        "admin": (
            [
                "Dashboard",
                "Mi Portal de Autogestión",
                "Reportes Mensuales",
                "Expediente 360",
                "Novedades y Excepciones",
                "Sincronizar Relojes",
                "Visualizar Data",
                "---",
                "Empleados",
                "Turnos y Asignación",
                "Usuarios",
            ],
            [
                "house",
                "person-vcard",
                "bar-chart-line",
                "person-badge-fill",
                "journal-medical",
                "arrow-repeat",
                "table",
                "",
                "people",
                "calendar-check",
                "person-badge",
            ],
        ),
        "empleado": (["Mi Portal de Autogestión"], ["person-vcard"]),
        "coordinador": (
            [
                "Mi Portal de Autogestión",
                "Autorización de Permisos",
                "Carga Masiva de Turnos",
            ],
            ["person-vcard", "check2-square", "file-earmark-excel"],
        ),
        "jefe_area": (
            ["Mi Portal de Autogestión", "Autorización de Permisos"],
            ["person-vcard", "check2-square"],
        ),
        "nomina": (
            [
                "Dashboard",
                "Mi Portal de Autogestión",
                "Reportes Mensuales",
                "Expediente 360",
                "Novedades y Excepciones",
                "Sincronizar Relojes",
                "Visualizar Data",
                "---",
                "Empleados",
                "Turnos y Asignación",
                "Usuarios",
            ],
            [
                "house",
                "person-vcard",
                "bar-chart-line",
                "person-badge-fill",
                "journal-medical",
                "arrow-repeat",
                "table",
                "",
                "people",
                "calendar-check",
                "person-badge",
            ],
        ),
    }
    menu_options, menu_icons = ROLES_MENU.get(user_role, ROLES_MENU["nomina"])

    with st.sidebar:
        # --- CAMPANA DE NOTIFICACIONES ---
        from database_conn.queries import (
            db_get_recent_notifications,
            db_get_unread_notifications_count,
            db_mark_all_notifications_read,
        )

        unread_count = db_get_unread_notifications_count(user["username"])
        bell_label = (
            f"🔔 Notificaciones ({unread_count})"
            if unread_count > 0
            else "🔔 Notificaciones"
        )

        with st.popover(bell_label, use_container_width=True):
            st.markdown(
                "<h4 style='margin:0;'>Mis Notificaciones</h4>", unsafe_allow_html=True
            )
            st.markdown("---")
            notifs = db_get_recent_notifications(user["username"])
            if not notifs:
                st.write("No tienes notificaciones recientes.")
            else:
                for n in notifs:
                    icon = "🟢" if not n["is_read"] else "⚪"
                    st.markdown(
                        f"**{icon} {n['title']}**\n<small>{n['message']}</small>\n<small style='color:gray; font-size: 0.75rem; display: block; margin-top: 2px;'>{n['created_at']}</small>",
                        unsafe_allow_html=True,
                    )
                    st.markdown("<hr style='margin: 8px 0;'>", unsafe_allow_html=True)
                if st.button(
                    "Marcar todas como leídas",
                    key="mark_all_read_top_btn",
                    use_container_width=True,
                ):
                    db_mark_all_notifications_read(user["username"])
                    st.rerun()
        # --- PERSISTENCIA DEL MENÚ ACTIVO ---
        if "menu_sel" in st.query_params:
            stored_sel = st.query_params["menu_sel"]
            if stored_sel in menu_options:
                st.session_state["menu_selection"] = stored_sel
            st.query_params.pop("menu_sel", None)

        default_idx = 0
        if "menu_selection" in st.session_state:
            stored_sel = st.session_state["menu_selection"]
            if stored_sel in menu_options:
                default_idx = menu_options.index(stored_sel)

        sel = option_menu(
            menu_title=None,
            options=menu_options,
            icons=menu_icons,
            menu_icon="cast",
            default_index=default_idx,
            key="sidebar_menu",
            styles={
                "container": {
                    "padding": "0!important",
                    "background-color": "transparent",
                },
                "icon": {"color": "#0066cc", "font-size": "18px"},
                "nav-link": {
                    "font-size": "15px",
                    "text-align": "left",
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
        st.session_state["menu_selection"] = sel
        st.markdown("<br><br>", unsafe_allow_html=True)

        if st.button("Cerrar Sesión", type="primary", use_container_width=True):
            token = st.session_state.get("session_token")
            if token:
                try:
                    db_delete_session(token)
                except Exception:
                    pass
            st.query_params.clear()
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
        "Mi Portal de Autogestión": page_employee_portal,
    }

    # El enrutador maneja la navegación a las diferentes páginas

    if sel == "Turnos y Asignación":
        tab1, tab2, tab3 = st.tabs(
            ["🏗️ Crear Turnos", "📝 Asignar a Empleados", "📥 Carga Masiva (Excel)"]
        )
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
import sys
import types
from datetime import datetime

import bcrypt  # noqa: F401

from database_conn.connection import DATA_DIR as _DATA_DIR, DB_PATH as _DB_PATH, db_conn  # noqa: F401
from database_conn.setup import init_db # noqa: F401
from database_conn.queries import (  # noqa: F401
    assign_shift,
    calculate_overnight_surcharge,
    get_profile_by_name,
    get_shifts_df,
    is_holiday,
    upsert_employees_df,
    upsert_shift,
)
from services.analytics import (  # noqa: F401
    compute_month_lateness,
    get_late_punch_ids,
    get_shift_for_user_date,
    schedule_for_date,
    schedule_for_user_date,
    to_excel_bytes,
)
from services.zk_service import load_devices, upsert_attendance  # noqa: F401
from utils.auth import get_user, verify_login, require_role  # noqa: F401
from views.schedules_view import (  # noqa: F401
    auto_assign_shifts_from_schedules,
    ensure_schedules_columns,
    generate_rotating_schedule,
    maybe_load_default_schedules,
    resolve_shift_from_code,
    upsert_schedule_df,
    upsert_shifts_from_code_csv,
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
