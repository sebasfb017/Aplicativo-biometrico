import io
import sqlite3
import pandas as pd
import streamlit as st
from datetime import datetime, date, time, timedelta

from database_conn.connection import db_conn
from services.notifications import log_audit
from utils.auth import require_role

@st.dialog("✏️ Editar Marcación", width="large")
def edit_attendance_dialog(record_id: int):
    conn = db_conn()
    df_rec = pd.read_sql_query("""
        SELECT a.*, COALESCE(e.full_name, 'Sin registrar') as full_name
        FROM attendance_raw a 
        LEFT JOIN employees e ON a.user_id = e.user_id
        WHERE a.id = ?
    """, conn, params=(record_id,))
    
    if df_rec.empty:
        st.error("No se encontró el registro.")
        conn.close()
        return
        
    rec = df_rec.iloc[0]
    
    st.markdown(f"**Usuario:** {rec['full_name']} (ID: {rec['user_id']})")
    st.markdown(f"**Dispositivo:** {rec['device_name']} ({rec['device_ip']})")
    
    try:
        current_dt = datetime.fromisoformat(rec['ts'].replace(" ", "T"))
    except ValueError:
        # Fallback if format is unexpected
        current_dt = datetime.strptime(rec['ts'], "%Y-%m-%d %H:%M:%S")

    col1, col2 = st.columns(2)
    with col1:
        new_date = st.date_input("Fecha Marcación", value=current_dt.date())
    with col2:
        new_time = st.time_input("Hora Marcación", value=current_dt.time(), step=60)
        
    current_punch = int(rec['punch'])
    punch_opts = {0: "0 - Entrada", 1: "1 - Salida", 2: "2 - Break In", 3: "3 - Break Out", 4: "4 - OT In", 5: "5 - OT Out"}
    punch_keys = list(punch_opts.keys())
    
    punch_idx = punch_keys.index(current_punch) if current_punch in punch_keys else 0
    new_punch_key = st.selectbox("Tipo de Marcación (Punch)", options=punch_keys, format_func=lambda x: punch_opts.get(x, str(x)), index=punch_idx)
    
    st.markdown("---")
    
    c1, c2 = st.columns(2)
    with c1:
        submitted = st.button("Guardar Cambios", type="primary", use_container_width=True)
    with c2:
        confirm_del = st.checkbox("Confirmar eliminación", key=f"del_att_{record_id}")
        submitted_del = st.button("🗑️ Eliminar Registro", type="secondary", disabled=not confirm_del, use_container_width=True)

    if submitted:
        new_ts = datetime.combine(new_date, new_time).isoformat(sep=" ", timespec="seconds")
        try:
            cur = conn.cursor()
            is_manual = rec.get("is_manual", 0)
            if is_manual == 1:
                cur.execute("""
                    UPDATE attendance_raw 
                    SET ts = ?, punch = ? 
                    WHERE id = ?
                """, (new_ts, new_punch_key, record_id))
            else:
                cur.execute("UPDATE attendance_raw SET is_ignored = 1 WHERE id = ?", (record_id,))
                cur.execute("""
                    INSERT INTO attendance_raw (device_name, device_ip, user_id, ts, status, punch, uid, downloaded_at, is_ignored, is_manual)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, 1)
                """, (str(rec['device_name']), str(rec['device_ip']), str(rec['user_id']), new_ts, int(rec['status']), int(new_punch_key), int(rec['uid']), str(rec['downloaded_at'])))
            conn.commit()
            log_audit("EDIT_ATTENDANCE", f"Marcación #{record_id} modif: {rec['ts']} -> {new_ts}")
            st.success("✅ Marcación actualizada. Cerrando...")
            import time as time_mod
            time_mod.sleep(0.75)
            if "view_attendance_table" in st.session_state:
                del st.session_state["view_attendance_table"]
            st.rerun()
        except sqlite3.IntegrityError:
            st.error("Error: Ya existe un registro idéntico (mismo usuario, fecha, hora y tipo).")
        except Exception as e:
            st.error(f"Error al actualizar: {e}")
        finally:
            conn.close()

    if submitted_del:
        try:
            cur = conn.cursor()
            is_manual = rec.get("is_manual", 0)
            if is_manual == 1:
                cur.execute("DELETE FROM attendance_raw WHERE id = ?", (record_id,))
            else:
                cur.execute("UPDATE attendance_raw SET is_ignored = 1 WHERE id = ?", (record_id,))
            conn.commit()
            log_audit("DELETE_ATTENDANCE", f"Marcación #{record_id} eliminada/ignorada.")
            st.success("🗑️ Registro eliminado. Cerrando...")
            import time as time_mod
            time_mod.sleep(0.75)
            if "view_attendance_table" in st.session_state:
                del st.session_state["view_attendance_table"]
            st.rerun()
        except Exception as e:
            st.error(f"Error al eliminar: {e}")
        finally:
            conn.close()
            
    if conn:
        try:
            conn.close()
        except:
            pass

def page_view_attendance():
    require_role("admin", "nomina")
    st.title("🗂️ Visor Multidimensional de Marcaciones")
    st.write("Consulta y exporta la analítica en bruto de los accesos biométricos registrados.")

    conn = db_conn()
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT device_name FROM attendance_raw ORDER BY device_name")
    devices = [row[0] for row in cur.fetchall()]
    conn.close()

    with st.expander("🔍 Buscador y Filtros Avanzados", expanded=True):
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            start_date = st.date_input("Desde el", value=date.today() - timedelta(days=7))
        with col2:
            end_date = st.date_input("Hasta el", value=date.today())
        with col3:
            selected_device = st.selectbox("Reloj de Origen", options=["Todos los Dispositivos"] + devices)
        with col4:
            user_filter = st.text_input("Filtrar Empleado (DNI o Nombre)")

    conn = db_conn()
    start_dt = datetime.combine(start_date, time(0, 0))
    end_dt = datetime.combine(end_date + timedelta(days=1), time(0, 0))

    query = """
        SELECT 
            a.id,
            a.device_name, 
            a.device_ip, 
            a.user_id,
            COALESCE(e.full_name, 'Sin registrar') as employee_name,
            a.ts, 
            a.status, 
            a.punch, 
            a.downloaded_at
        FROM attendance_raw a
        LEFT JOIN employees e ON a.user_id = e.user_id
        WHERE a.ts >= ? AND a.ts < ? AND a.is_ignored = 0
    """
    params = [start_dt.isoformat(sep=" ", timespec="seconds"), 
              end_dt.isoformat(sep=" ", timespec="seconds")]

    if selected_device != "Todos los Dispositivos":
        query += " AND a.device_name = ?"
        params.append(selected_device)

    if user_filter.strip():
        query += " AND (a.user_id LIKE ? OR e.full_name LIKE ?)"
        search_term = f"%{user_filter.strip()}%"
        params.extend([search_term, search_term])

    query += " ORDER BY a.ts DESC"

    df = pd.read_sql_query(query, conn, params=params)
    conn.close()

    if df.empty:
        st.warning("No hay marcaciones en el rango seleccionado.")
    else:
        df = df.rename(columns={
            "id": "ID",
            "device_name": "Dispositivo",
            "device_ip": "IP",
            "user_id": "ID Usuario",
            "employee_name": "Nombre",
            "ts": "Hora Marcación",
            "status": "Status",
            "punch": "Tipo",
            "downloaded_at": "Descargado en"
        })
        
        punch_map = {0: "Entrada", 1: "Salida", 2: "Break In", 3: "Break Out", 4: "OT In", 5: "OT Out"}
        df["Tipo"] = df["Tipo"].map(punch_map).fillna(df["Tipo"])

        st.info(f"💡 Selecciona una marcación para editar manualmente su hora o eliminarla. Total de registros: {len(df)}")
        
        if 'last_processed_attendance' not in st.session_state:
            st.session_state.last_processed_attendance = None
            
        event = st.dataframe(df, use_container_width=True, hide_index=True, on_select="rerun", selection_mode="single-row", key="view_attendance_table")
        
        if len(event.selection.rows) > 0:
            row_idx = event.selection.rows[0]
            selected_id = int(df.iloc[row_idx]["ID"])
            
            if selected_id != st.session_state.last_processed_attendance:
                st.session_state.last_processed_attendance = selected_id
                edit_attendance_dialog(selected_id)
        else:
            st.session_state.last_processed_attendance = None

        excel_bytes = io.BytesIO()
        with pd.ExcelWriter(excel_bytes, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name="Marcaciones")
        excel_bytes.seek(0)

        st.download_button(
            "📥 Descargar registros en Excel",
            data=excel_bytes.getvalue(),
            file_name=f"marcaciones_{start_date.isoformat()}_{end_date.isoformat()}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )