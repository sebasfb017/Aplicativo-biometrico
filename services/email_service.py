import os
import smtplib
import yaml
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from database_conn.connection import BASE_DIR

SMTP_YAML = os.path.join(BASE_DIR, "smtp_config.yaml")

def load_smtp_config():
    if not os.path.exists(SMTP_YAML):
        return {
            "smtp_server": "smtp.gmail.com",
            "smtp_port": 587,
            "smtp_user": "",
            "smtp_password": "",
            "sender_name": "Nómina Dolormed"
        }
    try:
        with open(SMTP_YAML, "r", encoding="utf-8") as f:
            cfg = yaml.safe_load(f) or {}
            return cfg
    except Exception:
        return {
            "smtp_server": "smtp.gmail.com",
            "smtp_port": 587,
            "smtp_user": "",
            "smtp_password": "",
            "sender_name": "Nómina Dolormed"
        }

def save_smtp_config(config: dict):
    try:
        with open(SMTP_YAML, "w", encoding="utf-8") as f:
            yaml.dump(config, f, allow_unicode=True, sort_keys=False)
        return True
    except Exception as e:
        print(f"Error guardando smtp_config.yaml: {e}")
        return False

def _send_email_sync(to_email, subject, html_content, text_content=""):
    config = load_smtp_config()
    
    sender_email = config.get("smtp_user", "")
    sender_password = config.get("smtp_password", "")
    smtp_server = config.get("smtp_server", "smtp.gmail.com")
    smtp_port = config.get("smtp_port", 587)
    sender_name = config.get("sender_name", "Nómina Dolormed")
    
    if not sender_email or not sender_password:
        return False, "Las credenciales SMTP no están configuradas."
        
    try:
        import email.utils
        from email.header import Header
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        
        # Codificar correctamente el nombre si tiene tildes (RFC 5322)
        formatted_from = email.utils.formataddr((str(Header(sender_name, 'utf-8')), sender_email))
        msg["From"] = formatted_from
        msg["Date"] = email.utils.formatdate(localtime=True)
        msg["Message-ID"] = email.utils.make_msgid(domain=sender_email.split('@')[-1] if '@' in sender_email else 'dolormed.co')
        
        # Si to_email es una lista de destinatarios, lo convertimos en un string separado por comas
        if isinstance(to_email, list):
            # Filtrar correos vacíos
            valid_emails = [e for e in to_email if e and "@" in e]
            if not valid_emails:
                return False, "No hay correos de destinatario válidos."
            msg["To"] = ", ".join(valid_emails)
            recipient = valid_emails
        else:
            if not to_email or "@" not in to_email:
                return False, "Correo de destinatario inválido."
            msg["To"] = to_email
            recipient = [to_email]
            
        if text_content:
            part1 = MIMEText(text_content, "plain")
            msg.attach(part1)
            
        part2 = MIMEText(html_content, "html")
        msg.attach(part2)
        
        import ssl
        context = ssl._create_unverified_context()
        timeout_seconds = 15
        
        if int(smtp_port) == 465:
            server = smtplib.SMTP_SSL(smtp_server, int(smtp_port), context=context, timeout=timeout_seconds)
        else:
            server = smtplib.SMTP(smtp_server, int(smtp_port), timeout=timeout_seconds)
            server.starttls(context=context)
            
        server.login(sender_email, sender_password)
        server.sendmail(sender_email, recipient, msg.as_string())
        server.quit()
        
        return True, "Correo enviado correctamente."
    except Exception as e:
        return False, str(e)

def _send_email(to_email, subject, html_content, text_content=""):
    """
    Despacha el envío de correos electrónicos en un hilo en segundo plano (Asíncrono).
    
    Esta envoltura evita que la interfaz gráfica de Streamlit se bloquee o experimente
    retrasos mientras se establece la conexión con el servidor SMTP.
    
    Parámetros:
    -----------
    to_email : str | list
        Dirección(es) de correo electrónico del destinatario.
    subject : str
        Asunto del correo electrónico.
    html_content : str
        Cuerpo del mensaje en formato HTML para clientes modernos.
    text_content : str, opcional
        Cuerpo del mensaje en texto plano como respaldo.
        
    Retorna:
    --------
    tuple(bool, str)
        Estado de la operación inicial y un mensaje de confirmación.
    """
    import threading
    
    def background_task():
        success, msg = _send_email_sync(to_email, subject, html_content, text_content)
        if not success:
            print(f"Error asíncrono enviando correo a {to_email}: {msg}")
            
    thread = threading.Thread(target=background_task)
    thread.daemon = True
    thread.start()
    return True, "Enviando en segundo plano..."


def _get_base_template(title, body_content, header_color="#0D6EFD"):
    return f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="utf-8">
        <style>
            body {{ font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; background-color: #f8fafc; color: #334155; margin: 0; padding: 0; }}
            .email-wrapper {{ width: 100%; background-color: #f8fafc; padding: 40px 0; }}
            .container {{ max-width: 600px; margin: 0 auto; background: #ffffff; border-radius: 12px; overflow: hidden; box-shadow: 0 10px 15px -3px rgba(0, 0, 0, 0.1); }}
            .header {{ background-color: {header_color}; padding: 35px; text-align: center; color: white; }}
            .header img {{ width: 70px; height: 70px; margin-bottom: 15px; filter: drop-shadow(0px 4px 4px rgba(0,0,0,0.2)); }}
            .header h2 {{ margin: 0; font-size: 26px; font-weight: 600; letter-spacing: 0.5px; text-shadow: 0 2px 4px rgba(0,0,0,0.2); }}
            .content {{ padding: 40px 35px; line-height: 1.7; font-size: 16px; color: #475569; }}
            .btn-container {{ text-align: center; margin-top: 40px; margin-bottom: 10px; }}
            .btn {{ display: inline-block; background-color: {header_color}; color: #ffffff !important; text-decoration: none; padding: 14px 35px; border-radius: 8px; font-weight: bold; font-size: 16px; letter-spacing: 0.5px; box-shadow: 0 4px 6px rgba(0,0,0,0.15); transition: background-color 0.3s; }}
            .footer {{ background-color: #f1f5f9; padding: 25px; text-align: center; font-size: 13px; color: #64748b; border-top: 1px solid #e2e8f0; }}
            .info-box {{ background: #f8fafc; border-left: 4px solid {header_color}; padding: 20px; border-radius: 6px; margin: 25px 0; box-shadow: inset 0 2px 4px rgba(0,0,0,0.02); }}
            .info-box p {{ margin: 8px 0; }}
            .info-box ul {{ margin-top: 10px; margin-bottom: 10px; padding-left: 20px; }}
            strong {{ color: #1e293b; }}
        </style>
    </head>
    <body>
        <div class="email-wrapper">
            <div class="container">
                <div class="header">
                    <!-- Usamos un icono premium de hospital como logo por defecto -->
                    <img src="https://img.icons8.com/fluency/96/ffffff/hospital.png" alt="Dolormed Logo">
                    <h2>{title}</h2>
                </div>
                <div class="content">
                    {body_content}
                    <div class="btn-container">
                        <a href="http://localhost:8501" class="btn">Ir al Portal Web</a>
                    </div>
                </div>
                <div class="footer">
                    <p>Este es un mensaje automático del <strong>Sistema de Gestión Humana Dolormed</strong>.<br>Por favor, no respondas a este correo.</p>
                </div>
            </div>
        </div>
    </body>
    </html>
    """

def send_welcome_email(to_email, full_name, username, password):
    subject = "Bienvenido al Portal de Nómina Dolormed"
    title = "👋 Bienvenido a Dolormed"
    
    body = f"""
    <p>Hola <strong>{full_name}</strong>,</p>
    <p>Se te ha creado una cuenta segura para acceder al Portal de Autogestión de Empleados. Desde allí podrás consultar tus turnos, asistencias y radicar solicitudes de permisos (Novedades).</p>
    
    <div class="info-box">
      <p style="margin-bottom:15px; font-weight:bold;">Tus credenciales de acceso provisionales son:</p>
      <p><strong>👤 Usuario (Cédula):</strong> {username}</p>
      <p><strong>🔑 Contraseña:</strong> {password}</p>
    </div>
    
    <p>Te recomendamos cambiar tu contraseña una vez que ingreses al sistema por motivos de seguridad.</p>
    """
    
    html = _get_base_template(title, body)
    text = f"Hola {full_name}, Tus credenciales de acceso son: Usuario: {username}, Contraseña: {password}"
    return _send_email(to_email, subject, html, text)

def send_novedad_alert(to_emails, full_name, reason_type, details, total_time, start_date, prev_approver=None):
    subject = f"Nueva Solicitud Radicada: {full_name} ({reason_type})"
    title = "⚠️ Alerta de Nueva Novedad"
    
    prev_appr_html = f'<p style="background-color: #dcfce7; padding: 12px; border-radius: 6px; color: #166534; border: 1px solid #bbf7d0;"><strong>✅ Aprobado previamente por:</strong> {prev_approver}</p>' if prev_approver else ''
    
    body = f"""
    <p>El empleado <strong>{full_name}</strong> acaba de radicar una nueva solicitud en el portal que requiere revisión.</p>
    
    <div class="info-box">
      <ul>
        <li><strong>Tipo de Solicitud:</strong> {reason_type}</li>
        <li><strong>Fecha de Inicio:</strong> {start_date}</li>
        <li><strong>Tiempo Solicitado:</strong> {total_time}</li>
        <li><strong>Detalles/Justificación:</strong> {details}</li>
      </ul>
    </div>
    
    {prev_appr_html}
    
    <p>Por favor, ingresa al portal administrativo en la sección de <strong>Flujos y Autorizaciones</strong> para revisar y gestionar esta solicitud.</p>
    """
    
    html = _get_base_template(title, body, header_color="#eab308") # Yellow
    text = f"Alerta de Novedad: El empleado {full_name} acaba de radicar una nueva solicitud."
    return _send_email(to_emails, subject, html, text)

def send_password_reset_pin(to_email: str, full_name: str, pin: str):
    subject = f"Código de Recuperación de Contraseña - Dolormed"
    title = "🔐 Recuperación de Contraseña"
    
    body = f"""
    <p>Hola <strong>{full_name}</strong>,</p>
    <p>Hemos recibido una solicitud para restablecer tu contraseña en el Portal de Autogestión.</p>
    <p>Ingresa el siguiente PIN de seguridad de 6 dígitos en la pantalla. <strong>Este PIN expirará en 5 minutos.</strong></p>
    
    <div style="text-align: center; margin: 30px 0;">
        <span style="background: #f1f5f9; border: 2px dashed #94a3b8; padding: 15px 30px; font-size: 36px; font-weight: bold; letter-spacing: 8px; color: #334155; border-radius: 8px;">{pin}</span>
    </div>
    
    <p style="color: #ef4444; font-size: 14px; text-align: center;">Si no fuiste tú quien solicitó esto, ignora este mensaje y tu contraseña seguirá intacta.</p>
    """
    
    html = _get_base_template(title, body, header_color="#64748b") # Slate
    text = f"Tu PIN temporal de recuperación es: {pin}"
    return _send_email([to_email], subject, html, text)

def send_password_changed_email(to_email: str, full_name: str, new_password: str):
    subject = f"Contraseña Actualizada - Dolormed"
    title = "✅ ¡Contraseña Cambiada!"
    
    body = f"""
    <p>Hola <strong>{full_name}</strong>,</p>
    <p>Te confirmamos que la contraseña de tu cuenta en el Portal de Empleados ha sido actualizada correctamente a través del sistema de recuperación.</p>
    <p>Tus nuevas credenciales de acceso generadas automáticamente son:</p>
    
    <div style="text-align: center; margin: 30px 0;">
        <span style="background: #ecfdf5; border: 2px solid #34d399; padding: 15px 30px; font-size: 24px; font-weight: bold; color: #065f46; border-radius: 8px;">{new_password}</span>
    </div>
    
    <p style="color: #64748b; font-size: 14px; text-align: center;">Te recomendamos eliminar este correo inmediatamente por seguridad.</p>
    """
    
    html = _get_base_template(title, body, header_color="#10b981") # Emerald
    text = f"Tu nueva contraseña es: {new_password}"
    return _send_email([to_email], subject, html, text)

def send_status_update_email(to_email: str, full_name: str, req_id: int, reason_type: str, new_status: str, message: str, approver_name: str = None):
    subject = f"Actualización de Solicitud #{req_id} ({reason_type})"
    
    if "RECHAZA" in new_status.upper():
        color = "#ef4444" # Red
        icon = "❌"
    elif "FINAL" in new_status.upper() or "APROBAD" in new_status.upper():
        color = "#10b981" # Emerald
        icon = "✅"
    else:
        color = "#3b82f6" # Blue
        icon = "⏳"
        
    title = f"{icon} Actualización de Novedad"
    
    body = f"""
    <p>Hola <strong>{full_name}</strong>,</p>
    <p>Te informamos que tu solicitud de <strong>{reason_type}</strong> (Radicado #{req_id}) ha cambiado de estado en nuestro sistema.</p>
    
    <div class="info-box" style="border-left-color: {color};">
      <p><strong>Nuevo Estado:</strong> <span style="color: {color}; font-weight: bold;">{new_status}</span></p>
      <p><strong>Observaciones:</strong> {message}</p>
      {f'<p><strong>Procesado por:</strong> {approver_name}</p>' if approver_name else ''}
    </div>
    
    <p>Puedes consultar el historial completo o descargar tus comprobantes directamente en el Portal de Empleados.</p>
    """
    
    html = _get_base_template(title, body, header_color=color)
    text = f"Tu solicitud ha cambiado a: {new_status}"
    return _send_email([to_email], subject, html, text)
