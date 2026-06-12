import streamlit as st
import pandas as pd
from utils.auth import require_role
from services.zk_service import (
    load_devices, save_devices, sync_device_time, download_attendance_from_device, upsert_attendance,
    get_device_users_status, upload_user_to_device, delete_user_from_device, sync_all_devices
)

@st.dialog("⚙️ Editar Reloj Biométrico")
def edit_device_dialog(device_idx, devices_list):
    if device_idx is not None:
        d = devices_list[device_idx]
        is_new = False
    else:
        d = {"name": "", "ip": "", "port": 4370, "password": 0, "timeout": 10}
        is_new = True
        
    new_name = st.text_input("Nombre Visual", value=d.get("name", ""))
    new_ip = st.text_input("Dirección IP", value=d.get("ip", ""))
    
    col1, col2 = st.columns(2)
    with col1:
        new_port_str = st.text_input("Puerto (Default 4370)", value=str(d.get("port", 4370)))
    with col2:
        new_pwd_str = st.text_input("Contraseña", value=str(d.get("password", 0)))
        
    new_timeout = st.number_input("Timeout (s)", value=int(d.get("timeout", 10)), min_value=1, step=1)
    
    st.markdown("---")
    
    c1, c2 = st.columns(2)
    with c1:
        if st.button("💾 Guardar Cambios", type="primary", use_container_width=True):
            if not new_ip.strip():
                st.error("La Dirección IP es obligatoria.")
                return
                
            try:
                port_val = int(new_port_str)
            except ValueError:
                st.error("El puerto debe ser numérico.")
                return
                
            try:
                pwd_val = int(new_pwd_str)
            except ValueError:
                st.error("La contraseña debe ser numérica.")
                return
                
            updated_d = {
                "name": new_name.strip() if new_name.strip() else new_ip.strip(),
                "ip": new_ip.strip(),
                "port": port_val,
                "password": pwd_val,
                "timeout": int(new_timeout)
            }
            
            if is_new:
                devices_list.append(updated_d)
            else:
                devices_list[device_idx] = updated_d
                
            if save_devices(devices_list):
                st.success("✅ Dispositivo guardado correctamente. Cerrando...")
                import time
                time.sleep(0.75)
                if "devices_table" in st.session_state:
                    del st.session_state["devices_table"]
                st.rerun()
            else:
                st.error("Error al guardar en devices.yaml.")
                
    with c2:
        if not is_new:
            del_confirm = st.checkbox("Confirmar eliminación")
            if st.button("🗑️ Eliminar Reloj", type="secondary", disabled=not del_confirm, use_container_width=True):
                devices_list.pop(device_idx)
                if save_devices(devices_list):
                    st.success("🗑️ Dispositivo eliminado. Cerrando...")
                    import time
                    time.sleep(0.75)
                    if "devices_table" in st.session_state:
                        del st.session_state["devices_table"]
                    st.rerun()
                else:
                    st.error("Error al guardar en devices.yaml.")

@st.dialog("✏️ Editar Usuario Biométrico")
def edit_remote_user_dialog(user_data, device, devices_list):
    """
    Ventana emergente interactiva para editar o eliminar un usuario directamente en la memoria del reloj biométrico.
    
    Permite modificar el nombre corto del empleado o eliminarlo de forma definitiva.
    Integra una funcionalidad de propagación masiva ('Aplicar a TODOS') que itera 
    sobre los dispositivos de la red para mantener sincronizada la base de datos de los equipos.
    
    Parámetros:
    -----------
    user_data : dict
        Diccionario con los datos del usuario seleccionados de la tabla (ej. user_id, name, privilege).
    device : dict
        El reloj biométrico principal del cual se extrajo la información.
    devices_list : list
        Lista completa de todos los relojes ZKTeco configurados en el sistema (para sincronización en cascada).
    """
    st.write(f"Editando Cédula: **{user_data['user_id']}** (Encontrado en {device.get('name', device['ip'])})")
    
    new_name = st.text_input("Nombre Corto (Ej. J. PEREZ)", value=user_data['name'])
    apply_all = st.checkbox("Aplicar cambio (o eliminación) en TODOS los relojes de la red", value=True)
    
    st.markdown("---")
    c1, c2 = st.columns(2)
    with c1:
        if st.button("💾 Guardar Nuevo Nombre", type="primary", use_container_width=True):
            if not new_name.strip():
                st.error("El nombre no puede estar vacío.")
                return
            
            devices_to_process = devices_list if apply_all else [device]
            success_count = 0
            with st.spinner("Transmitiendo a los relojes..."):
                for d in devices_to_process:
                    ok, err = upload_user_to_device(d, str(user_data['user_id']), new_name.strip(), user_data.get('privilege', 0))
                    if ok:
                        success_count += 1
                        # Limpiar cache de la vista
                        if "dev_users_" + d["ip"] in st.session_state:
                            del st.session_state["dev_users_" + d["ip"]]
                            
            if success_count > 0:
                st.success(f"✅ Usuario modificado en {success_count} relojes.")
                import time
                time.sleep(0.75)
                st.rerun()
            else:
                st.error("❌ No se pudo modificar el usuario en ningún reloj.")

    with c2:
        del_confirm = st.checkbox("Confirmar eliminación")
        if st.button("🗑️ Eliminar Usuario", type="secondary", disabled=not del_confirm, use_container_width=True):
            devices_to_process = devices_list if apply_all else [device]
            success_count = 0
            with st.spinner("Eliminando de los relojes..."):
                # Para eliminar, necesitamos buscar el UID en CADA dispositivo
                for d in devices_to_process:
                    users_status, err = get_device_users_status(d)
                    if users_status:
                        for u in users_status:
                            if str(u['user_id']) == str(user_data['user_id']):
                                ok, err_del = delete_user_from_device(d, u['uid'])
                                if ok:
                                    success_count += 1
                                    if "dev_users_" + d["ip"] in st.session_state:
                                        del st.session_state["dev_users_" + d["ip"]]
                                break
                                
            if success_count > 0:
                st.success(f"🗑️ Usuario eliminado en {success_count} relojes.")
                import time
                time.sleep(0.75)
                st.rerun()
            else:
                st.error("❌ No se pudo eliminar el usuario en ningún reloj.")

def page_sync():
    require_role("admin", "nomina")
    st.title("🔄 Sincronización Biométrica")
    
    tab_sync, tab_conf, tab_ctrl = st.tabs(["⏬ Sincronizar Datos", "⚙️ Configurar Dispositivos", "🎛️ Centro de Control (Usuarios)"])
    
    with tab_sync:
        st.write("Selecciona los relojes de los que deseas descargar las marcaciones recientes.")

        devices = load_devices()
        if not devices:
            st.error("No hay dispositivos configurados. Ve a la pestaña '⚙️ Configurar Dispositivos' para crearlos.")
        else:
            with st.expander("🛠️ Lista de Relojes Biométricos Disponibles", expanded=True):
                st.write("Marca o desmarca los dispositivos que quieras sincronizar:")
                
                selected_devices = []
                for d in devices:
                    label = f"📱 {d.get('name', d['ip'])} (IP: {d['ip']})"
                    if st.checkbox(label, value=True, key=f"chk_{d['ip']}"):
                        selected_devices.append(d)
                        
                st.markdown("<br>", unsafe_allow_html=True)
                col_btn1, col_btn2 = st.columns(2)
                with col_btn1:
                    btn_sync = st.button("Descargar Marcaciones de los Relojes Seleccionados", type="primary", use_container_width=True)
                with col_btn2:
                    btn_set_time = st.button("Sincronizar Hora Biométricos", use_container_width=True)

            if btn_set_time:
                if not selected_devices:
                    st.warning("Debes dejar marcado al menos un reloj para hacer la sincronización.")
                else:
                    with st.spinner("Sincronizando hora de los relojes seleccionados..."):
                        for d in selected_devices:
                            label = f"{d.get('name', d['ip'])} ({d['ip']})"
                            success, err = sync_device_time(d)
                            if success:
                                st.success(f"{label} ✅ Hora sincronizada con el servidor.")
                            else:
                                st.error(f"{label} ❌ Error de conexión al sincronizar: {err}")

            if btn_sync:
                if not selected_devices:
                    st.warning("Debes dejar marcado al menos un reloj para hacer la descarga.")
                else:
                    total_inserted = 0
                    total_skipped = 0

                    progress_bar = st.progress(0, "Iniciando descarga...")

                    for i, d in enumerate(selected_devices):
                        label = f"{d.get('name', d['ip'])} ({d['ip']})"
                        progress_text = f"({i+1}/{len(selected_devices)}) Conectando y descargando datos de: {label}"
                        progress_bar.progress((i + 1) / len(selected_devices), text=progress_text)

                        rows, err = download_attendance_from_device(d)
                        if err:
                            st.error(f"{label} ❌ Error de conexión: {err}")
                            continue

                        ins, skp = upsert_attendance(rows)
                        total_inserted += ins
                        total_skipped += skp
                        st.success(f"{label} ✅ Correcto (Nuevos: {ins} | Duplicados ignorados: {skp} | Total leídos: {len(rows)})")

                    progress_bar.empty()
                    st.info(f"**RESUMEN TOTAL** -> Nuevas marcaciones: **{total_inserted}** | Ignoradas: **{total_skipped}**")

    with tab_conf:
        st.write("Agrega, edita o elimina los relojes biométricos de tu red.")
        
        devices_config = load_devices()
        
        cbtn1, _ = st.columns([1, 2])
        with cbtn1:
            if st.button("➕ Agregar Nuevo Reloj", type="primary", use_container_width=True):
                edit_device_dialog(None, devices_config)
                
        if not devices_config:
            st.info("No hay dispositivos registrados en el sistema. Haz clic en 'Agregar Nuevo Reloj'.")
            
        else:
            df_dev = pd.DataFrame(devices_config)
            if "name" not in df_dev.columns: df_dev["name"] = df_dev["ip"]
            if "port" not in df_dev.columns: df_dev["port"] = 4370
            if "timeout" not in df_dev.columns: df_dev["timeout"] = 10
            if "password" not in df_dev.columns: df_dev["password"] = 0
                
            df_show = df_dev[["name", "ip", "port", "password", "timeout"]]
            df_show.columns = ["Nombre Visual", "Dirección IP", "Puerto", "Contraseña", "Timeout(s)"]
            
            st.info("💡 Selecciona un reloj en la tabla de abajo para poder editarlo o eliminarlo.")
            
            if 'last_processed_device' not in st.session_state:
                st.session_state.last_processed_device = None
                
            event = st.dataframe(
                df_show, 
                use_container_width=True, 
                hide_index=True, 
                on_select="rerun", 
                selection_mode="single-row", 
                key="devices_table"
            )
            
            if len(event.selection.rows) > 0:
                row_idx = event.selection.rows[0]
                dev_ip = str(df_show.iloc[row_idx]["Dirección IP"])
                
                if dev_ip != st.session_state.last_processed_device:
                    st.session_state.last_processed_device = dev_ip
                    edit_device_dialog(row_idx, devices_config)
            else:
                st.session_state.last_processed_device = None

    with tab_ctrl:
        st.write("Audita y administra los usuarios almacenados directamente en la memoria interna de cada reloj biométrico.")
        
        devices_config = load_devices()
        if not devices_config:
            st.error("No hay dispositivos configurados.")
        else:
            dev_opts = {f"{d.get('name', d['ip'])} (IP: {d['ip']})": d for d in devices_config}
            sel_dev_label = st.selectbox("1. Selecciona el Reloj Biométrico a Consultar", list(dev_opts.keys()))
            selected_dev = dev_opts[sel_dev_label]
            
            st.markdown("---")
            
            st.subheader("🌐 Sincronización Total (Modo Espejo)")
            st.write("Iguala **todos los usuarios y huellas** entre todos los relojes biométricos. Si falta una huella en un reloj pero está en el otro, el sistema la copiará automáticamente.")
            
            if st.button("🌟 Igualar Todos los Relojes Automáticamente", type="primary", use_container_width=True):
                with st.spinner("Analizando y clonando memorias (Esto puede tardar varios segundos)..."):
                    logs = sync_all_devices(devices_config)
                    for log in logs:
                        if log.startswith("✅") or log.startswith("🚀"):
                            st.success(log)
                        else:
                            st.error(log)
                            
            st.markdown("---")
            
            c1, c2 = st.columns([1.5, 1])
            with c1:
                st.subheader("Auditoría de Memoria Local")
                if st.button("📥 Descargar Usuarios de este Reloj", use_container_width=True):
                    with st.spinner(f"Conectando a {selected_dev['ip']}..."):
                        users_data, err = get_device_users_status(selected_dev)
                        if err:
                            st.error(f"Error de conexión: {err}")
                        elif not users_data:
                            st.info("No se encontraron usuarios en la memoria del reloj.")
                        else:
                            st.session_state["dev_users_" + selected_dev["ip"]] = users_data
                            
                users_data = st.session_state.get("dev_users_" + selected_dev["ip"], [])
                if users_data:
                    df_users = pd.DataFrame(users_data)
                    df_users["Huella Registrada"] = df_users["has_fingerprint"].apply(lambda x: "✅ Sí" if x else "❌ No")
                    df_users["user_id"] = df_users["user_id"].astype(str)
                    
                    st.write(f"Usuarios alojados en memoria: **{len(df_users)}**")
                    st.info("💡 Selecciona un usuario en la tabla para **Editar** su nombre o **Eliminarlo**.")
                    
                    if 'last_edited_user' not in st.session_state:
                        st.session_state.last_edited_user = None
                        
                    event_users = st.dataframe(
                        df_users[["user_id", "name", "Huella Registrada"]].rename(columns={"user_id": "Cédula", "name": "Nombre en Reloj"}),
                        use_container_width=True, hide_index=True, on_select="rerun", selection_mode="single-row", key=f"tbl_users_{selected_dev['ip']}"
                    )
                    
                    if len(event_users.selection.rows) > 0:
                        selected_idx = event_users.selection.rows[0]
                        sel_user = users_data[selected_idx]
                        
                        # Evitar re-lanzamientos infinitos del dialog si la tabla se vuelve a renderizar
                        if str(sel_user['user_id']) != st.session_state.last_edited_user:
                            st.session_state.last_edited_user = str(sel_user['user_id'])
                            edit_remote_user_dialog(sel_user, selected_dev, devices_config)
                    else:
                        st.session_state.last_edited_user = None
                    
            with c2:
                st.subheader("Crear Remotamente")
                st.write("Inscribe un nuevo empleado en **TODOS** los relojes de la red.")
                with st.form("new_user_zk_form"):
                    new_uid_str = st.text_input("Cédula (user_id)")
                    new_name_str = st.text_input("Nombre Corto (Ej. J. PEREZ)")
                    sub_new = st.form_submit_button("🚀 Transmitir a Toda la Red", type="primary", use_container_width=True)
                    
                    if sub_new:
                        if not new_uid_str.strip() or not new_name_str.strip():
                            st.error("Debes llenar la Cédula y el Nombre.")
                        else:
                            with st.spinner("Escribiendo en la memoria de todos los dispositivos..."):
                                success_count = 0
                                for d in devices_config:
                                    ok, err2 = upload_user_to_device(d, new_uid_str.strip(), new_name_str.strip())
                                    if ok:
                                        success_count += 1
                                    else:
                                        st.error(f"Error inscribiendo en {d['ip']}: {err2}")
                                
                                if success_count == len(devices_config):
                                    st.success(f"¡Usuario {new_name_str} inscrito exitosamente en todos los equipos! Ahora debe enrolar su huella en CUALQUIER reloj.")
                                else:
                                    st.warning(f"Inscrito en {success_count} de {len(devices_config)} equipos.")
                                    
                                if "dev_users_" + selected_dev["ip"] in st.session_state:
                                    del st.session_state["dev_users_" + selected_dev["ip"]]

            # --- HERRAMIENTA AVANZADA: REASIGNACIÓN DE MARCACIONES ---
            # Este módulo permite corregir errores humanos de enrolamiento en el biométrico.
            # Cuando un empleado inscribe su huella bajo una cédula equivocada, esta herramienta 
            # ejecuta un UPDATE masivo en SQL para trasladar sus marcaciones históricas 
            # a la cédula verdadera, dejando un rastro de auditoría por seguridad.
            st.markdown("---")
            with st.expander("🛠️ Herramientas Avanzadas (Corrección de Datos Históricos)"):
                st.write("Si una huella fue enrolada con el número de cédula equivocado, utiliza esta herramienta para trasladar todo el historial de marcaciones de la cédula incorrecta a la correcta.")
                
                with st.form("reassign_punches_form"):
                    col1, col2 = st.columns(2)
                    with col1:
                        bad_id = st.text_input("Cédula de Origen (INCORRECTA)")
                    with col2:
                        good_id = st.text_input("Cédula de Destino (CORRECTA)")
                        
                    submit_reassign = st.form_submit_button("🔄 Trasladar Marcaciones Históricas", type="primary")
                    
                    if submit_reassign:
                        if not bad_id.strip() or not good_id.strip():
                            st.error("Debes llenar ambas cédulas.")
                        else:
                            try:
                                from database_conn.connection import db_conn
                                from datetime import datetime
                                
                                conn = db_conn()
                                cur = conn.cursor()
                                
                                # Verificar cuántas existen
                                cur.execute("SELECT COUNT(*) FROM attendance_raw WHERE user_id = ?", (bad_id.strip(),))
                                records_count = cur.fetchone()[0]
                                
                                if records_count == 0:
                                    st.warning(f"No se encontraron marcaciones históricas para la cédula {bad_id.strip()}.")
                                else:
                                    # Ejecutar actualización
                                    cur.execute("UPDATE attendance_raw SET user_id = ? WHERE user_id = ?", (good_id.strip(), bad_id.strip()))
                                    
                                    # Loggear la acción de seguridad
                                    admin_user = st.session_state.get("user", {}).get("username", "admin_unknown")
                                    details = f"Traslado masivo: Movió {records_count} marcaciones de la cédula {bad_id.strip()} hacia {good_id.strip()}."
                                    cur.execute("INSERT INTO audit_logs (user_id, action, details, timestamp) VALUES (?, ?, ?, ?)", 
                                                (admin_user, "DATA_CORRECTION", details, datetime.now().isoformat(timespec="seconds")))
                                    
                                    conn.commit()
                                    st.success(f"✅ ¡Éxito! Se han transferido {records_count} marcaciones a la cédula {good_id.strip()}.")
                                    st.info("💡 IMPORTANTE: Recuerda borrar la huella equivocada desde la tabla superior y pedirle al empleado que ponga su huella nuevamente con su cédula correcta, para que el problema no se repita mañana.")
                                
                                conn.close()
                            except Exception as e:
                                st.error(f"Error en base de datos: {e}")