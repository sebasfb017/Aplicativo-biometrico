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
                    for d in selected_devices:
                        label = f"{d.get('name', d['ip'])} ({d['ip']})"
                        with st.spinner(f"Ajustando hora en: {label}"):
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

                    for d in selected_devices:
                        label = f"{d.get('name', d['ip'])} ({d['ip']})"

                        with st.spinner(f"Conectando y descargando datos de: {label}"):
                            rows, err = download_attendance_from_device(d)
                            if err:
                                st.error(f"{label} ❌ Error de conexión: {err}")
                                continue

                            ins, skp = upsert_attendance(rows)
                            total_inserted += ins
                            total_skipped += skp
                            st.success(f"{label} ✅ Correcto (Nuevos: {ins} | Duplicados ignorados: {skp} | Total leídos: {len(rows)})")

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
                    st.dataframe(
                        df_users[["user_id", "name", "Huella Registrada"]].rename(columns={"user_id": "Cédula", "name": "Nombre en Reloj"}),
                        use_container_width=True, hide_index=True
                    )
                    
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