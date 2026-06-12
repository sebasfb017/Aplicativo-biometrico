import streamlit as st
import pandas as pd
from datetime import datetime
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from database_conn.connection import db_conn

from services.email_service import _send_email

def send_notification_email(to_email, subject, body):
    """Envía correos automáticos usando la configuración SMTP."""
    if not to_email:
        return
    
    # Delegar al enviador de correos real
    success, msg = _send_email(to_email, subject, body)
    if not success:
        print(f"Error enviando correo: {msg}")
    else:
        print(f"📧 [EMAIL] Enviado a {to_email} | Asunto: {subject}")

def notify_employee_status(user_id, full_name, req_id, reason_type, new_status, message, approver_name=None):
    """Busca el correo del empleado y le envía una notificación HTML de actualización de estado."""
    conn = db_conn()
    cur = conn.cursor()
    cur.execute("SELECT emp_email FROM users_app WHERE username = ?", (user_id,))
    row = cur.fetchone()
    conn.close()
    if row and row[0]:
        from services.email_service import send_status_update_email
        send_status_update_email(row[0], full_name, req_id, reason_type, new_status, message, approver_name)

def log_audit(action, details):
    """Registra una acción en la tabla de auditoría."""
    conn = db_conn()
    cur = conn.cursor()
    user_id = "SISTEMA"
    if "user" in st.session_state and st.session_state["user"]:
        user_id = st.session_state["user"].get("username", "SISTEMA")
        
    try:
        cur.execute("""
            INSERT INTO audit_logs (user_id, action, details, timestamp)
            VALUES (?, ?, ?, ?)
        """, (user_id, action, details, datetime.now().isoformat(timespec="seconds")))
        conn.commit()
    except Exception as e:
        print(f"Error escribiendo log de auditoria: {e}")
    finally:
        conn.close()

def generate_fth012_html(req, df_audit):
    """Genera el HTML del comprobante digital F-TH-012."""
    approvers_html = ""
    if not df_audit.empty:
        approvers_html = "<h4>Firmas / Aprobaciones Digitales:</h4><ul>"
        for _, r in df_audit.iterrows():
            level = "Jefatura" if r['action'] == "APPROVE_LEAVE_L1" else "Gestión Humana"
            date_f = r['timestamp'].replace('T', ' ')
            approver_name = r['full_name'] if 'full_name' in r and pd.notna(r['full_name']) else r['user_id']
            approvers_html += f"<li><strong>{level} ({approver_name})</strong> - <em>{date_f}</em></li>"
        approvers_html += "</ul>"

    html_content = f"""
    <html>
    <head>
        <meta charset="utf-8">
        <title>F-TH-012 - Permiso {req['id']}</title>
        <style>
            body {{ font-family: sans-serif; padding: 20px; max-width: 800px; margin: 0 auto; }}
            h1 {{ color: #0D6EFD; text-align: center; border-bottom: 2px solid #0D6EFD; padding-bottom: 10px; }}
            h3 {{ color: #1E293B; text-align: center; margin-top: -10px; }}
            .box {{ border: 1px solid #E9ECEF; padding: 15px; margin-bottom: 15px; border-radius: 8px; background: #F8F9FA; }}
            .footer {{ margin-top: 40px; border-top: 1px solid #ccc; padding-top: 10px; font-size: 12px; color: #555; text-align: center; }}
        </style>
    </head>
    <body>
        <h1>DOLORMED</h1>
        <h3>FORMATO F-TH-012: Solicitud de Novedad Oficial</h3>
        
        <div class="box">
            <p><strong>Radicado:</strong> #{req['id']}</p>
            <p><strong>Fecha de Solicitud:</strong> {req['request_date']}</p>
            <p><strong>Estado Final:</strong> {req['status']}</p>
        </div>
        
        <div class="box">
            <p><strong>Motivo/Clasificación:</strong> {req['reason_type']}</p>
            <p><strong>Justificación Adicional:</strong> {req['reason_description']}</p>
            <p><strong>Fechas de Ausencia:</strong> Del {req['leave_date_start']} al {req['leave_date_end']}</p>
            <p><strong>Remunerado:</strong> {'Sí' if req['is_paid'] else 'No'}</p>
        </div>
        
        <div class="box">
            {approvers_html}
        </div>
        
        <div class="footer">
            <p>Documento generado digitalmente por el Portal de Nómina Dolormed. Válido sin firma manuscrita para control interno y RRHH.</p>
        </div>
    </body>
    </html>
    """
    return html_content