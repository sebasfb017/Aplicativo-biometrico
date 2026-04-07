import os
import io
import yaml
import bcrypt
import sqlite3
import calendar
import pandas as pd
import streamlit as st
from streamlit_option_menu import option_menu

from datetime import datetime, date, time, timedelta

from database_conn.connection import db_conn, DATA_DIR, DB_PATH

# -----------------------------
# Auth
from utils.auth import get_user, verify_login, require_role

# -----------------------------
# DEVICES ZK
from services.zk_service import load_devices, save_devices, download_attendance_from_device, sync_device_time, upsert_attendance

# -----------------------------
# NOTIFICATIONS
from services.notifications import send_notification_email, notify_employee, log_audit, generate_fth012_html

# -----------------------------
# ANALYTICS
from services.analytics import compute_month_lateness, fetch_attendance_between, to_excel_bytes, schedule_for_user_date, schedule_for_date

# -----------------------------
# DB
from database_conn.setup import init_db
from database_conn.queries import (
    upsert_employees_df, db_create_leave_request, is_holiday, 
    get_shifts_df, upsert_shift, assign_shift, 
    upsert_exception, get_exceptions_df,
    get_users_by_role, get_all_employees  # <-- Estas son las dos funciones que faltaban
)

# -----------------------------
# CONSTANTS
from utils.constants import ROLES, AREA_MAPPING

# -----------------------------
# VIEWS
from views.auth_view import page_login, register_employee_dialog
from views.dashboard_view import page_dashboard
from views.employees_view import page_employees
from views.sync_zkteco_view import page_sync
from views.employee_portal_view import page_employee_portal
from views.schedules_view import (
    page_schedules, page_shifts, page_assign_shifts, 
    ensure_schedules_columns, maybe_load_default_schedules
)
from views.exceptions_view import page_exceptions
from views.attendance_view import page_view_attendance
from views.month_report_view import page_lateness_report
from views.users_admin_view import page_users_admin


APP_DIR = os.path.dirname(os.path.abspath(__file__))
# helper for default schedules file path (must be computed at runtime
# because DATA_DIR can be monkeypatched in tests).

def toggle_theme_config():
    config_path = os.path.join(os.path.dirname(__file__), ".streamlit", "config.toml")
    is_dark = False
    if os.path.exists(config_path):
        with open(config_path, "r", encoding="utf-8") as f:
            if 'base="dark"' in f.read():
                is_dark = True
                
    new_mode = "light" if is_dark else "dark"
    new_content = f'''[theme]
primaryColor="#0D6EFD"
base="{new_mode}"
font="sans serif"
'''
    os.makedirs(os.path.dirname(config_path), exist_ok=True)
    with open(config_path, "w", encoding="utf-8") as f:
        f.write(new_content)


def main():
    st.set_page_config(page_title="Nómina Dolormed", layout="wide", page_icon="🏢")
    init_db()

    user = st.session_state.get("user")
    if not user:
        # Centrar login form
        col1, col2, col3 = st.columns([1,2,1])
        with col2:
            page_login()
        return

    # Sidebar Header
    st.sidebar.markdown(f"<h2 style='text-align: center; color: #0066cc;'>Dolormed RRHH</h2>", unsafe_allow_html=True)
    st.sidebar.markdown(f"<div style='text-align: center; color: gray; margin-bottom: 20px;'>Hola, <b>{user['full_name']}</b><br><small>({user['role'].upper()})</small></div>", unsafe_allow_html=True)

    if user["role"] == "admin":
        menu_options = ["Dashboard", "Reportes Mensuales", "Novedades y Excepciones", "Sincronizar Relojes", "Visualizar Data", "---", "Empleados", "Horarios", "Turnos y Asignación", "Usuarios"]
        menu_icons = ["house", "bar-chart-line", "journal-medical", "arrow-repeat", "table", "", "people", "clock", "calendar-check", "person-badge"]
    elif user["role"] == "empleado":
        menu_options = ["Mi Portal de Autogestión"]
        menu_icons = ["person-vcard"]
    elif user["role"] == "coordinador":
        menu_options = ["Dashboard", "Autorización de Permisos", "Visualizar Data"]
        menu_icons = ["house", "check2-square", "table"]
    else:
        menu_options = ["Dashboard", "Reportes Mensuales", "Novedades y Excepciones", "Sincronizar Relojes", "Visualizar Data"]
        menu_icons = ["house", "bar-chart-line", "journal-medical", "arrow-repeat", "table"]

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
        
        config_p = os.path.join(os.path.dirname(__file__), ".streamlit", "config.toml")
        current_mode = "☀️ Claro"
        if os.path.exists(config_p):
            with open(config_p, "r", encoding="utf-8") as f:
                if 'base="dark"' in f.read():
                    current_mode = "🌙 Oscuro"
                    
        if st.button(f"Cambiar Tema ({current_mode})", use_container_width=True):
            toggle_theme_config()
            import time
            time.sleep(0.3)
            st.rerun()
            
        if st.button("Cerrar Sesión", type="primary", use_container_width=True):
            st.session_state.pop("user", None)
            st.rerun()

    # Router
    if sel == "Dashboard":
        page_dashboard()
    elif sel == "Sincronizar Relojes":
        page_sync()
    elif sel == "Visualizar Data":
        page_view_attendance()
    elif sel == "Reportes Mensuales":
        page_lateness_report()
    elif sel in ["Novedades y Excepciones", "Autorización de Permisos"]:
        page_exceptions()
    elif sel == "Empleados":
        page_employees()
    elif sel == "Horarios":
        page_schedules()
    elif sel == "Turnos y Asignación":
        tab1, tab2 = st.tabs(["🏗️ Crear Turnos", "📝 Asignar a Empleados"])
        with tab1:
            page_shifts()
        with tab2:
            page_assign_shifts()
    elif sel == "Usuarios":
        page_users_admin()
    elif sel == "Mi Portal de Autogestión":
        page_employee_portal()


if __name__ == "__main__":
    main()
