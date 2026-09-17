from datetime import datetime
import pandas as pd
import streamlit as st
from database_conn.connection import db_conn


def format_ts(ts):
    """Formatea una estampa de tiempo ISO a 'DD/Mon/YYYY hh:mm AM/PM'."""
    if not ts:
        return ""
    try:
        if isinstance(ts, str):
            ts_clean = ts.replace("T", " ").split(".")[0]
            dt = datetime.strptime(ts_clean, "%Y-%m-%d %H:%M:%S")
        elif isinstance(ts, (datetime, pd.Timestamp)):
            dt = ts
        else:
            return str(ts)
        months = [
            "Ene", "Feb", "Mar", "Abr", "May", "Jun", 
            "Jul", "Ago", "Sep", "Oct", "Nov", "Dic"
        ]
        m_str = months[dt.month - 1]
        time_str = dt.strftime("%I:%M %p")
        return f"{dt.day}/{m_str}/{dt.year} {time_str}"
    except Exception:
        return str(ts)


def render_approval_timeline(req_id: int):
    """
    Consulta la base de datos y renderiza la Cronología de Firmas (Audit Trail)
    paso a paso para el radicado especificado.
    """
    req_id = int(req_id)
    with db_conn() as conn:
        df_req = pd.read_sql_query(
            "SELECT * FROM leave_requests WHERE id = %s", conn, params=(req_id,)
        )
        if df_req.empty:
            return
        
        req = df_req.iloc[0]
        requester_id = req["user_id"]

        # Obtener nombre completo del empleado solicitante
        df_user = pd.read_sql_query(
            "SELECT full_name, role FROM users_app WHERE username = %s", conn, params=(requester_id,)
        )
        requester_name = df_user.iloc[0]["full_name"] if not df_user.empty else requester_id

        # Consultar historial de firmas en audit_logs
        df_audit = pd.read_sql_query(
            """
            SELECT a.user_id, a.action, a.timestamp, a.details, u.full_name, u.role
            FROM audit_logs a
            LEFT JOIN users_app u ON a.user_id = u.username
            WHERE (a.details LIKE %s OR a.details LIKE %s)
              AND (a.action LIKE 'APPROVE_%%' OR a.action LIKE 'REJECT_%%' OR a.action LIKE 'CANCEL_%%')
            ORDER BY a.timestamp ASC
        """,
            conn,
            params=(f"%Permiso #{req_id} %", f"%solicitud #{req_id}%"),
        )

    # Construir eventos del timeline
    events = []

    # 1. Hito de Radicación Inicial
    events.append({
        "title": "📝 Solicitud Radicada y Firmada",
        "actor": requester_name,
        "role_badge": "Empleado",
        "timestamp": format_ts(req["created_at"]),
        "status_color": "#3b82f6", # Azul
        "border_color": "#60a5fa",
        "icon": "📝"
    })

    # 2. Hitos de Aprobación / Rechazo / Cancelación
    for _, row in df_audit.iterrows():
        action = row["action"]
        user_name = row["full_name"] or row["user_id"]
        role = (row["role"] or "").lower()
        
        role_label = "Aprobador"
        if role == "coordinador":
            role_label = "Coordinador/a"
        elif role == "jefe_area":
            role_label = "Jefe de Área"
        elif role in ["admin", "nomina"]:
            role_label = "Gestión Humana (RRHH)"
        elif role == "empleado":
            role_label = "Empleado"

        if "APPROVE" in action:
            events.append({
                "title": f"✅ Aprobado por {role_label}",
                "actor": user_name,
                "role_badge": role_label,
                "timestamp": format_ts(row["timestamp"]),
                "status_color": "#10b981", # Verde
                "border_color": "#34d399",
                "icon": "✅"
            })
        elif "REJECT" in action:
            events.append({
                "title": f"❌ Rechazado por {role_label}",
                "actor": user_name,
                "role_badge": role_label,
                "timestamp": format_ts(row["timestamp"]),
                "status_color": "#ef4444", # Rojo
                "border_color": "#f87171",
                "icon": "❌"
            })
        elif "CANCEL" in action:
            events.append({
                "title": "🚫 Solicitud Cancelada",
                "actor": user_name,
                "role_badge": role_label,
                "timestamp": format_ts(row["timestamp"]),
                "status_color": "#f59e0b", # Naranja
                "border_color": "#fbbf24",
                "icon": "🚫"
            })

    # 3. Hito Pendiente Actual (si sigue en revisión)
    req_status = req["status"]
    if req_status == "PENDING_COORD":
        events.append({
            "title": "⏳ Pendiente por Coordinador/a",
            "actor": "Esperando revisión del Coordinador asignado",
            "role_badge": "Pendiente",
            "timestamp": "En espera",
            "status_color": "#8b5cf6", # Morado
            "border_color": "#a78bfa",
            "icon": "⏳"
        })
    elif req_status == "PENDING_JEFE":
        events.append({
            "title": "⏳ Pendiente por Jefe de Área",
            "actor": "Esperando revisión del Jefe de Área",
            "role_badge": "Pendiente",
            "timestamp": "En espera",
            "status_color": "#8b5cf6",
            "border_color": "#a78bfa",
            "icon": "⏳"
        })
    elif req_status == "PENDING_RRHH":
        events.append({
            "title": "⏳ Pendiente por Gestión Humana (RRHH)",
            "actor": "Esperando revisión y auditoría final de RRHH",
            "role_badge": "Pendiente",
            "timestamp": "En espera",
            "status_color": "#8b5cf6",
            "border_color": "#a78bfa",
            "icon": "⏳"
        })

    # Renderizar el HTML de la Cronología sin sangría para evitar bloques de código markdown
    st.markdown("### 🕒 Cronología de Firmas (Audit Trail)")
    
    html_items = []
    for idx, ev in enumerate(events):
        is_last = idx == len(events) - 1
        line_html = "" if is_last else '<div style="width: 2px; height: 35px; background: rgba(255,255,255,0.15); margin-left: 19px;"></div>'
        
        item_html = (
            f'<div style="display: flex; flex-direction: column;">'
            f'<div style="display: flex; align-items: flex-start; gap: 12px;">'
            f'<div style="width: 40px; height: 40px; border-radius: 50%; background: rgba(255,255,255,0.05); border: 2px solid {ev["border_color"]}; display: flex; align-items: center; justify-content: center; font-size: 1.1rem; flex-shrink: 0;">'
            f'{ev["icon"]}'
            f'</div>'
            f'<div style="background: rgba(255,255,255,0.03); border: 1px solid rgba(255,255,255,0.08); border-radius: 8px; padding: 10px 14px; width: 100%;">'
            f'<div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 2px;">'
            f'<span style="font-weight: 600; color: #f8fafc; font-size: 0.95rem;">{ev["title"]}</span>'
            f'<span style="background: rgba(255,255,255,0.08); color: #cbd5e1; font-size: 0.75rem; padding: 2px 8px; border-radius: 12px;">{ev["timestamp"]}</span>'
            f'</div>'
            f'<div style="color: #94a3b8; font-size: 0.85rem;">'
            f'👤 <strong>{ev["actor"]}</strong>'
            f'</div>'
            f'</div>'
            f'</div>'
            f'{line_html}'
            f'</div>'
        )
        html_items.append(item_html)

    all_items_str = "".join(html_items)
    container_html = (
        f'<div style="background: rgba(15, 23, 42, 0.4); border: 1px solid rgba(255, 255, 255, 0.1); border-radius: 12px; padding: 18px; margin-top: 10px; margin-bottom: 15px;">'
        f'{all_items_str}'
        f'</div>'
    )
    st.markdown(container_html, unsafe_allow_html=True)
