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

from fpdf import FPDF

class FTH012PDF(FPDF):
    def header(self):
        # Fondo decorativo para la cabecera
        self.set_fill_color(240, 244, 248)
        self.rect(10, 10, 190, 24, 'F')
        
        self.set_y(12)
        self.set_font('helvetica', 'B', 15)
        self.set_text_color(13, 110, 253) # Azul Dolormed
        self.cell(0, 7, 'DOLORMED S.A.S.', align='C', new_x="LMARGIN", new_y="NEXT")
        
        self.set_font('helvetica', 'B', 9)
        self.set_text_color(30, 41, 59) # Gris oscuro
        self.cell(0, 5, 'FORMATO F-TH-012: SOLICITUD DE NOVEDAD OFICIAL', align='C', new_x="LMARGIN", new_y="NEXT")
        self.ln(10)
        
    def footer(self):
        self.set_y(-25)
        self.set_font('helvetica', 'I', 8)
        self.set_text_color(100, 116, 139)
        self.line(10, self.get_y(), 200, self.get_y())
        self.ln(2)
        self.multi_cell(0, 4, 'Documento generado digitalmente por el Portal de Nómina Dolormed.\nEste soporte digital es válido sin firma manuscrita para control interno, auditoría y RRHH.', align='C')

def generate_fth012_pdf(req, df_audit):
    """Genera el PDF del comprobante digital F-TH-012 y retorna los bytes."""
    pdf = FTH012PDF(orientation="P", unit="mm", format="A4")
    pdf.set_margins(15, 15, 15)
    pdf.add_page()
    pdf.set_auto_page_break(auto=True, margin=25)
    
    # --- 1. Información General de la Solicitud ---
    pdf.set_font('helvetica', 'B', 10)
    pdf.set_text_color(13, 110, 253)
    pdf.cell(0, 8, '1. INFORMACIÓN GENERAL DE LA SOLICITUD', new_x="LMARGIN", new_y="NEXT")
    pdf.line(15, pdf.get_y(), 195, pdf.get_y())
    pdf.ln(3)
    
    pdf.set_font('helvetica', '', 9)
    pdf.set_text_color(30, 41, 59)
    
    col_w = 45
    val_w = 45
    
    # Fila 1
    pdf.set_font('helvetica', 'B', 9)
    pdf.cell(col_w, 6, 'Radicado Nro:')
    pdf.set_font('helvetica', '', 9)
    pdf.cell(val_w, 6, f"#{req['id']}")
    
    pdf.set_font('helvetica', 'B', 9)
    pdf.cell(col_w, 6, 'Fecha de Solicitud:')
    pdf.set_font('helvetica', '', 9)
    pdf.cell(val_w, 6, str(req['request_date']), new_x="LMARGIN", new_y="NEXT")
    
    # Fila 2
    pdf.set_font('helvetica', 'B', 9)
    pdf.cell(col_w, 6, 'Estado Actual:')
    pdf.set_font('helvetica', '', 9)
    pdf.cell(val_w, 6, str(req['status']))
    
    pdf.set_font('helvetica', 'B', 9)
    pdf.cell(col_w, 6, 'Tipo de Novedad:')
    pdf.set_font('helvetica', '', 9)
    pdf.cell(val_w, 6, str(req['reason_type']), new_x="LMARGIN", new_y="NEXT")
    
    pdf.ln(4)
    
    # --- 2. Información del Empleado ---
    pdf.set_font('helvetica', 'B', 10)
    pdf.set_text_color(13, 110, 253)
    pdf.cell(0, 8, '2. INFORMACIÓN DEL EMPLEADO', new_x="LMARGIN", new_y="NEXT")
    pdf.line(15, pdf.get_y(), 195, pdf.get_y())
    pdf.ln(3)
    
    pdf.set_font('helvetica', '', 9)
    pdf.set_text_color(30, 41, 59)
    
    # Fila 1
    pdf.set_font('helvetica', 'B', 9)
    pdf.cell(col_w, 6, 'Nombre Completo:')
    pdf.set_font('helvetica', '', 9)
    pdf.cell(val_w, 6, str(req.get('full_name', 'N/A')))
    
    pdf.set_font('helvetica', 'B', 9)
    pdf.cell(col_w, 6, 'Cédula de Ciudadanía:')
    pdf.set_font('helvetica', '', 9)
    pdf.cell(val_w, 6, str(req.get('user_id', 'N/A')), new_x="LMARGIN", new_y="NEXT")
    
    # Fila 2
    pdf.set_font('helvetica', 'B', 9)
    pdf.cell(col_w, 6, 'Departamento/Área:')
    pdf.set_font('helvetica', '', 9)
    pdf.cell(val_w, 6, str(req.get('department', 'N/A')))
    
    pdf.set_font('helvetica', 'B', 9)
    pdf.cell(col_w, 6, 'Correo Electrónico:')
    pdf.set_font('helvetica', '', 9)
    pdf.cell(val_w, 6, str(req.get('email', 'N/A')), new_x="LMARGIN", new_y="NEXT")
    
    pdf.ln(4)
    
    # --- 3. Detalles de Ausencia y Tiempos ---
    pdf.set_font('helvetica', 'B', 10)
    pdf.set_text_color(13, 110, 253)
    pdf.cell(0, 8, '3. DETALLES DE AUSENCIA Y TIEMPOS', new_x="LMARGIN", new_y="NEXT")
    pdf.line(15, pdf.get_y(), 195, pdf.get_y())
    pdf.ln(3)
    
    pdf.set_font('helvetica', '', 9)
    pdf.set_text_color(30, 41, 59)
    
    # Fila 1
    pdf.set_font('helvetica', 'B', 9)
    pdf.cell(col_w, 6, 'Fecha de Inicio:')
    pdf.set_font('helvetica', '', 9)
    pdf.cell(val_w, 6, str(req['leave_date_start']))
    
    pdf.set_font('helvetica', 'B', 9)
    pdf.cell(col_w, 6, 'Fecha de Fin:')
    pdf.set_font('helvetica', '', 9)
    pdf.cell(val_w, 6, str(req['leave_date_end']), new_x="LMARGIN", new_y="NEXT")
    
    # Fila 2
    pdf.set_font('helvetica', 'B', 9)
    pdf.cell(col_w, 6, 'Hora Salida:')
    pdf.set_font('helvetica', '', 9)
    pdf.cell(val_w, 6, str(req.get('start_time') or 'N/A'))
    
    pdf.set_font('helvetica', 'B', 9)
    pdf.cell(col_w, 6, 'Hora Entrada:')
    pdf.set_font('helvetica', '', 9)
    pdf.cell(val_w, 6, str(req.get('end_time') or 'N/A'), new_x="LMARGIN", new_y="NEXT")
    
    # Fila 3
    pdf.set_font('helvetica', 'B', 9)
    pdf.cell(col_w, 6, 'Tiempo Total:')
    pdf.set_font('helvetica', '', 9)
    pdf.cell(val_w, 6, str(req['total_time']))
    
    pdf.set_font('helvetica', 'B', 9)
    pdf.cell(col_w, 6, 'Permiso Remunerado:')
    pdf.set_font('helvetica', '', 9)
    pdf.cell(val_w, 6, 'Sí' if req['is_paid'] else 'No', new_x="LMARGIN", new_y="NEXT")
    
    pdf.ln(3)
    
    # Justificación
    pdf.set_font('helvetica', 'B', 9)
    pdf.cell(0, 5, 'Justificación o Descripción del Motivo:', new_x="LMARGIN", new_y="NEXT")
    pdf.set_font('helvetica', '', 9)
    pdf.multi_cell(0, 5, str(req['reason_description'] or 'Sin justificación ingresada.'))
    pdf.ln(2)
    
    if not req['is_paid'] and req.get('how_to_makeup'):
        pdf.set_font('helvetica', 'B', 9)
        pdf.cell(0, 5, 'Acuerdo de Reposición de Tiempo:', new_x="LMARGIN", new_y="NEXT")
        pdf.set_font('helvetica', '', 9)
        pdf.multi_cell(0, 5, str(req['how_to_makeup']))
        
    pdf.ln(4)
    
    # --- 4. Firmas y Aprobaciones Digitales ---
    pdf.set_font('helvetica', 'B', 10)
    pdf.set_text_color(13, 110, 253)
    pdf.cell(0, 8, '4. FIRMAS Y APROBACIONES DIGITALES', new_x="LMARGIN", new_y="NEXT")
    pdf.line(15, pdf.get_y(), 195, pdf.get_y())
    pdf.ln(3)
    
    pdf.set_font('helvetica', '', 9)
    pdf.set_text_color(30, 41, 59)
    
    if df_audit.empty:
        pdf.set_font('helvetica', 'I', 9)
        pdf.cell(0, 6, 'No hay aprobaciones digitales registradas en el sistema aún.', new_x="LMARGIN", new_y="NEXT")
    else:
        for _, r in df_audit.iterrows():
            level = "Jefatura de Área" if r['action'] == "APPROVE_LEAVE_L1" else "Gestión Humana / RRHH"
            date_f = r['timestamp'].replace('T', ' ')
            approver_name = r['full_name'] if 'full_name' in r and pd.notna(r['full_name']) else r['user_id']
            
            pdf.set_font('helvetica', 'B', 9)
            pdf.cell(45, 6, f"{level}:")
            pdf.set_font('helvetica', '', 9)
            pdf.cell(0, 6, f"Aprobado digitalmente por {approver_name} el {date_f}", new_x="LMARGIN", new_y="NEXT")
            
    return bytes(pdf.output())