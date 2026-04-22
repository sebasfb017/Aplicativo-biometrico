import streamlit as st
import pandas as pd
from database_conn.connection import db_conn, db_session
from database_conn.queries import upsert_employees_df, get_all_employees
from utils.auth import require_role
from utils.constants import AREA_MAPPING

@st.dialog("✏️ Editar Empleado", width="large")
def edit_employee_dialog(user_id):
    with db_session() as conn:
        emp_df = pd.read_sql_query("SELECT full_name, department, profile_id FROM employees WHERE user_id = ?", conn, params=(user_id,))
        profiles_df = pd.read_sql_query("SELECT profile_id, name FROM profiles ORDER BY name", conn)
    
    if emp_df.empty:
        st.error("No se encontró el empleado.")
        return
        
    e = emp_df.iloc[0]
    
    profile_opts = profiles_df["name"].tolist()
    
    current_prof_id = e.get("profile_id")
    current_prof_name = ""
    if pd.notna(current_prof_id):
        prof_row = profiles_df[profiles_df['profile_id'] == current_prof_id]
        if not prof_row.empty:
            current_prof_name = prof_row.iloc[0]["name"]
            
    prof_index = profile_opts.index(current_prof_name) if current_prof_name in profile_opts else 0
    
    current_dept = str(e['department']) if pd.notna(e['department']) else ""
    def_area = list(AREA_MAPPING.keys())[0]
    def_subarea = ""
    
    if " - " in current_dept:
        parts = current_dept.split(" - ", 1)
        if parts[0] in AREA_MAPPING:
            def_area = parts[0]
            def_subarea = parts[1]
    else:
        for k, v in AREA_MAPPING.items():
            if current_dept in v:
                def_area = k
                def_subarea = current_dept
                break
        else:
            if current_dept in AREA_MAPPING:
                def_area = current_dept
                
    st.write(f"**DNI / ID Biométrico:** {user_id}")
    new_name = st.text_input("Nombre Completo*", value=e['full_name'])
    
    area_opts = list(AREA_MAPPING.keys())
    area_idx = area_opts.index(def_area) if def_area in area_opts else 0
    new_area = st.selectbox("Área Principal*", options=area_opts, index=area_idx)
    
    subarea_opts = AREA_MAPPING[new_area]
    subarea_idx = subarea_opts.index(def_subarea) if def_subarea in subarea_opts else 0
    new_subarea = st.selectbox("Sub-área / Departamento*", options=subarea_opts, index=subarea_idx)
    
    new_prof_name = st.selectbox("Perfil Asignado*", options=profile_opts, index=prof_index)
    
    st.markdown("---")
    submitted = st.button("Guardar Cambios", type="primary")
    
    if submitted:
        if not new_name.strip() or not new_area or not new_subarea:
            st.error("Nombre, Área y Sub-área son obligatorios.")
            return
            
        new_prof_id = None
        prof_row = profiles_df[profiles_df['name'] == new_prof_name]
        if not prof_row.empty:
            new_prof_id = int(prof_row.iloc[0]["profile_id"])
            
        final_dept = f"{new_area} - {new_subarea}"
        
        try:
            with db_session() as conn:
                cur = conn.cursor()
                cur.execute("""
                    UPDATE employees 
                    SET full_name = ?, department = ?, profile_id = ?
                    WHERE user_id = ?
                """, (new_name.strip(), final_dept, new_prof_id, str(user_id)))
            
            get_all_employees.clear()
            st.success("✅ Cambios guardados correctamente.")
            st.rerun()
        except Exception as exc:
            st.error(f"Error al guardar los cambios: {exc}")
            
    with st.expander("🚨 Zona de Peligro - Eliminar Empleado"):
        st.warning("Esta acción es irreversible y eliminará el registro de empleado y sus accesos si existen.")
        confirm_del = st.checkbox("Entiendo que esta acción es permanente.", key=f"del_emp_{user_id}")
        
        if st.button("🗑️ Eliminar Definitivamente", type="primary", disabled=not confirm_del):
            try:
                with db_session() as conn:
                    cur = conn.cursor()
                    cur.execute("DELETE FROM employees WHERE user_id = ?", (str(user_id),))
                    cur.execute("DELETE FROM users_app WHERE username = ?", (str(user_id),))
                
                get_all_employees.clear()
                from database_conn.queries import get_users_by_role
                get_users_by_role.clear()
                
                st.success("🗑️ Empleado eliminado.")
                st.rerun()
            except Exception as exc:
                st.error(f"Error al eliminar: {exc}")

def page_employees():
    require_role("admin")
    st.title("👨‍💼 Directorio de Empleados")
    st.write("Administra la plantilla de personal y asocia los perfiles de Dolormed.")

    # Mostrar perfiles disponibles
    with db_session() as conn:
        profiles_df = pd.read_sql_query("SELECT profile_id, name, description, works_holidays FROM profiles ORDER BY name", conn)
    
    with st.expander("ℹ️ Ver Perfiles y Reglas Base Creados en el Sistema"):
        if not profiles_df.empty:
            profiles_df['works_holidays'] = profiles_df['works_holidays'].apply(lambda x: '✅ Sí' if x == 1 else '❌ No')
            profiles_df.columns = ["ID", "Nombre Perfil", "Descripción", "¿Trabaja Festivos?"]
            st.dataframe(profiles_df, use_container_width=True, hide_index=True)
        else:
            st.info("No hay perfiles configurados.")

    st.markdown("---")
    tab1, tab2, tab3 = st.tabs(["👥 Plantilla Activa", "➕ Crear Empleado Manual", "📥 Importar CSV masivo"])

    with tab1:
        st.subheader("Directorio Actual")
        st.write("Selecciona una fila para editar la información del empleado.")
        with db_session() as conn:
            emp = pd.read_sql_query("""
                SELECT e.user_id, e.full_name, e.department, COALESCE(p.name, 'Sin asignar') as profile, e.created_at
                FROM employees e
                LEFT JOIN profiles p ON e.profile_id = p.profile_id
                ORDER BY e.user_id
            """, conn)
        
        if emp.empty:
            st.warning("El directorio está vacío.")
        else:
            emp.columns = ["DNI / ID Biométrico", "Nombre Completo", "Área / Departamento", "Perfil Asignado", "Fecha Registro"]
            
            # Filtro rápido
            deptss = ["Todos"] + list(emp["Área / Departamento"].dropna().unique())
            filtro_dep = st.selectbox("Filtrar por Departamento:", deptss)
            
            df_show = emp if filtro_dep == "Todos" else emp[emp["Área / Departamento"] == filtro_dep]
            
            st.metric("Total en vista", len(df_show))
            
            if 'last_processed_employee' not in st.session_state:
                st.session_state.last_processed_employee = None
                
            event = st.dataframe(df_show, use_container_width=True, hide_index=True, on_select="rerun", selection_mode="single-row", key="employees_table")
            
            selected_rows = event.selection.rows
            if selected_rows:
                idx = selected_rows[0]
                selected_user_id = str(df_show.iloc[idx]["DNI / ID Biométrico"])
                
                if selected_user_id != st.session_state.last_processed_employee:
                    st.session_state.last_processed_employee = selected_user_id
                    edit_employee_dialog(selected_user_id)
            else:
                st.session_state.last_processed_employee = None

    with tab2:
        st.subheader("Registrar Empleado Manualmente")
        st.info("Utiliza este formulario para crear un registro individual en la base de datos de manera inmediata.")
        c1, c2 = st.columns(2)
        with c1:
            e_id = st.text_input("Número de Documento (DNI)*", placeholder="Ej: 100123456")
            e_name = st.text_input("Nombre Completo*", placeholder="Apellidos y Nombres")
            e_email = st.text_input("Correo Electrónico (Opcional)", placeholder="usuario@dolormed.com")
        with c2:
            e_area_main = st.selectbox("Área Principal*", options=list(AREA_MAPPING.keys()))
            e_subarea = st.selectbox("Sub-área / Departamento*", options=AREA_MAPPING[e_area_main])
            e_prof = st.selectbox("Perfil Asignado*", options=[p for p in profiles_df["Nombre Perfil"].tolist()])
            
        submitted = st.button("Crear Empleado", type="primary")

        if submitted:
            e_dept = f"{e_area_main} - {e_subarea}"
            if not e_id.strip() or not e_name.strip() or not e_dept.strip():
                st.error("Por favor completa los campos obligatorios (*).")
            else:
                try:
                    df_new = pd.DataFrame([{
                        "user_id": e_id.strip(),
                        "full_name": e_name.strip(),
                        "email": e_email.strip(),
                        "department": e_dept.strip(),
                        "profile_id": e_prof
                    }])
                    upsert_employees_df(df_new)
                    get_all_employees.clear()
                    st.success(f"✅ Empleado {e_name} creado exitosamente.")
                except Exception as e:
                    st.error(f"Fallo al registrar empleado: {e}")

    with tab3:
        st.subheader("📥 Cargar Plantilla Masiva CSV")
        st.info("Sube un archivo CSV con las columnas: `user_id, full_name, email, department` (Opcional: `profile_id` o el nombre del perfil exacto).")
        csv_file = st.file_uploader("Arrastra tu documento CSV aquí", type=["csv"], key="emp_csv")
        if csv_file is not None:
            df = pd.read_csv(csv_file)
            try:
                upsert_employees_df(df)
                get_all_employees.clear()
                st.success("✅ Base de datos de empleados actualizada satisfactoriamente.")
            except Exception as e:
                st.error(f"Fallo al procesar el documento: {e}")