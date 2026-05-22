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

def send_welcome_email(to_email, full_name, username, password):
    subject = "Bienvenido al Portal de Nómina Dolormed"
    
    html = f"""
    <html>
      <head>
        <style>
          body {{ font-family: Arial, sans-serif; background-color: #f4f7f6; color: #333; }}
          .container {{ max-width: 600px; margin: 20px auto; background: #fff; border-radius: 8px; padding: 30px; box-shadow: 0 4px 6px rgba(0,0,0,0.1); }}
          .header {{ text-align: center; border-bottom: 2px solid #0D6EFD; padding-bottom: 10px; margin-bottom: 20px; }}
          .header h2 {{ color: #0D6EFD; margin: 0; }}
          .content {{ line-height: 1.6; }}
          .info-box {{ background: #f8f9fa; border-left: 4px solid #0D6EFD; padding: 15px; border-radius: 5px; margin: 20px 0; }}
          .info-box p {{ margin: 5px 0; }}
          .footer {{ text-align: center; margin-top: 30px; font-size: 12px; color: #777; }}
        </style>
      </head>
      <body>
        <div class="container">
          <div class="header">
            <h2>👋 Bienvenido a Nómina Dolormed</h2>
          </div>
          <div class="content">
            <p>Hola <strong>{full_name}</strong>,</p>
            <p>Se te ha creado un usuario para acceder al Portal de Autogestión de Empleados. Desde allí podrás consultar tus turnos, asistencias y radicar solicitudes de permisos (Novedades).</p>
            
            <div class="info-box">
              <p>Tus credenciales de acceso son:</p>
              <p><strong>👤 Usuario:</strong> {username}</p>
              <p><strong>🔑 Contraseña:</strong> {password}</p>
            </div>
            
            <p>Te recomendamos cambiar tu contraseña una vez que ingreses al sistema por motivos de seguridad.</p>
          </div>
          <div class="footer">
            <p>Este es un mensaje automático del Sistema de RRHH Dolormed. No respondas a este correo.</p>
          </div>
        </div>
      </body>
    </html>
    """
    text = f"""
Bienvenido al Portal de Autogestión
Hola {full_name},
Tus credenciales de acceso son:
Usuario: {username}
Contraseña: {password}
Te recomendamos cambiar tu contraseña una vez que ingreses.
"""
    return _send_email(to_email, subject, html, text)

def send_novedad_alert(to_emails, full_name, reason_type, details, total_time, start_date):
    subject = f"Nueva Solicitud Radicada: {full_name} ({reason_type})"
    
    html = f"""
    <html>
      <head>
        <style>
          body {{ font-family: Arial, sans-serif; background-color: #f4f7f6; color: #333; }}
          .container {{ max-width: 600px; margin: 20px auto; background: #fff; border-radius: 8px; padding: 30px; box-shadow: 0 4px 6px rgba(0,0,0,0.1); }}
          .header {{ text-align: center; border-bottom: 2px solid #ffc107; padding-bottom: 10px; margin-bottom: 20px; }}
          .header h2 {{ color: #ffc107; margin: 0; }}
          .content {{ line-height: 1.6; }}
          .info-box {{ background: #f8f9fa; border-left: 4px solid #ffc107; padding: 15px; border-radius: 5px; margin: 20px 0; }}
          .info-box ul {{ margin: 0; padding-left: 20px; }}
          .footer {{ text-align: center; margin-top: 30px; font-size: 12px; color: #777; }}
        </style>
      </head>
      <body>
        <div class="container">
          <div class="header">
            <h2>⚠️ Alerta de Nueva Novedad</h2>
          </div>
          <div class="content">
            <p>El empleado <strong>{full_name}</strong> acaba de radicar una nueva solicitud en el portal.</p>
            
            <div class="info-box">
              <ul>
                <li><strong>Tipo de Solicitud:</strong> {reason_type}</li>
                <li><strong>Fecha de Inicio:</strong> {start_date}</li>
                <li><strong>Tiempo Solicitado:</strong> {total_time}</li>
                <li><strong>Detalles/Justificación:</strong> {details}</li>
              </ul>
            </div>
            
            <p>Por favor, ingresa al portal administrativo en la sección de <strong>Control de Novedades</strong> para revisar y aprobar/rechazar esta solicitud.</p>
          </div>
          <div class="footer">
            <p>Este es un mensaje automático del Sistema de RRHH Dolormed. No respondas a este correo.</p>
          </div>
        </div>
      </body>
    </html>
    """
    text = f"""
Alerta de Novedad (Permiso Radicado)
El empleado {full_name} acaba de radicar una nueva solicitud.
Ingresa al portal para revisarla.
"""
    return _send_email(to_emails, subject, html, text)

def send_password_reset_pin(to_email: str, full_name: str, pin: str):
    """Envia el PIN temporal de recuperación de contraseña."""
    subject = f"Código de Recuperación de Contraseña - Dolormed"
    
    html = f"""
    <html>
      <head>
        <style>
          body {{ font-family: Arial, sans-serif; background-color: #f4f7f6; color: #333; }}
          .container {{ max-width: 600px; margin: 20px auto; background: #fff; border-radius: 8px; padding: 30px; box-shadow: 0 4px 6px rgba(0,0,0,0.1); }}
          .header {{ text-align: center; border-bottom: 2px solid #dc3545; padding-bottom: 10px; margin-bottom: 20px; }}
          .header h2 {{ color: #dc3545; margin: 0; }}
          .content {{ line-height: 1.6; text-align: center; }}
          .pin-box {{ background: #f8f9fa; border-left: 4px solid #dc3545; padding: 20px; border-radius: 5px; margin: 20px auto; font-size: 32px; font-weight: bold; letter-spacing: 5px; width: fit-content; }}
          .footer {{ text-align: center; margin-top: 30px; font-size: 12px; color: #777; }}
        </style>
      </head>
      <body>
        <div class="container">
          <div class="header">
            <h2>🔐 Recuperación de Contraseña</h2>
          </div>
          <div class="content">
            <p>Hola <strong>{full_name}</strong>,</p>
            <p>Hemos recibido una solicitud para restablecer tu contraseña en el Portal de Autogestión.</p>
            <p>Ingresa el siguiente PIN de seguridad en la pantalla. <strong>Este PIN expirará en 5 minutos.</strong></p>
            
            <div class="pin-box">
              {pin}
            </div>
            
            <p style="color: #d9534f; font-size: 14px;">Si no fuiste tú quien solicitó esto, ignora este mensaje y tu contraseña seguirá siendo la misma.</p>
          </div>
          <div class="footer">
            <p>Este es un mensaje automático del Sistema de RRHH Dolormed. No respondas a este correo.</p>
          </div>
        </div>
      </body>
    </html>
    """
    text = f"Tu PIN temporal de recuperación es: {pin}"
    return _send_email([to_email], subject, html, text)

def send_password_changed_email(to_email: str, full_name: str, new_password: str):
    """Envia una confirmación de que la contraseña ha sido cambiada, incluyendo la nueva contraseña."""
    subject = f"Contraseña Actualizada - Dolormed"
    
    html = f"""
    <html>
      <head>
        <style>
          body {{ font-family: Arial, sans-serif; background-color: #f4f7f6; color: #333; }}
          .container {{ max-width: 600px; margin: 20px auto; background: #fff; border-radius: 8px; padding: 30px; box-shadow: 0 4px 6px rgba(0,0,0,0.1); }}
          .header {{ text-align: center; border-bottom: 2px solid #28a745; padding-bottom: 10px; margin-bottom: 20px; }}
          .header h2 {{ color: #28a745; margin: 0; }}
          .content {{ line-height: 1.6; text-align: center; }}
          .info-box {{ background: #f8f9fa; border-left: 4px solid #28a745; padding: 15px; border-radius: 5px; margin: 20px auto; font-size: 20px; font-weight: bold; width: fit-content; }}
          .footer {{ text-align: center; margin-top: 30px; font-size: 12px; color: #777; }}
        </style>
      </head>
      <body>
        <div class="container">
          <div class="header">
            <h2>✅ ¡Contraseña Cambiada!</h2>
          </div>
          <div class="content">
            <p>Hola <strong>{full_name}</strong>,</p>
            <p>Te confirmamos que la contraseña de tu cuenta en el Portal de Empleados ha sido actualizada correctamente.</p>
            <p>Tus nuevas credenciales de acceso son:</p>
            
            <div class="info-box">
              {new_password}
            </div>
            
            <p style="color: #6c757d; font-size: 14px;">Te recomendamos eliminar este correo una vez hayas memorizado tu contraseña por razones de seguridad.</p>
          </div>
          <div class="footer">
            <p>Este es un mensaje automático del Sistema de RRHH Dolormed. No respondas a este correo.</p>
          </div>
        </div>
      </body>
    </html>
    """
    text = f"Tu nueva contraseña es: {new_password}"
    return _send_email([to_email], subject, html, text)

def send_status_update_email(to_email: str, full_name: str, req_id: int, reason_type: str, new_status: str, message: str):
    """
    Notifica al empleado sobre cambios en el estado de sus solicitudes (Novedades).
    
    Genera y envía una plantilla HTML dinámica que ajusta sus colores e íconos 
    dependiendo de si la solicitud fue aprobada, rechazada o está en trámite.
    
    Parámetros:
    -----------
    to_email : str
        Correo electrónico del empleado destino.
    full_name : str
        Nombre completo del empleado para personalización del mensaje.
    req_id : int
        Identificador único (Radicado) de la solicitud en la base de datos.
    reason_type : str
        Tipo de permiso solicitado (Ej. 'Vacaciones', 'Incapacidad Médica').
    new_status : str
        El estado actual al que avanzó la solicitud (Ej. 'PRE-APROBADA', 'RECHAZADA').
    message : str
        Observación o justificación adicional provista por el aprobador.
    """
    subject = f"Actualización de Solicitud #{req_id} ({reason_type})"
    
    if "RECHAZA" in new_status.upper():
        color = "#dc3545"
        icon = "❌"
    elif "FINAL" in new_status.upper() or "APROBAD" in new_status.upper():
        color = "#198754"
        icon = "✅"
    else:
        color = "#0D6EFD"
        icon = "⏳"
        
    html = f"""
    <html>
      <head>
        <style>
          body {{ font-family: Arial, sans-serif; background-color: #f4f7f6; color: #333; }}
          .container {{ max-width: 600px; margin: 20px auto; background: #fff; border-radius: 8px; padding: 30px; box-shadow: 0 4px 6px rgba(0,0,0,0.1); }}
          .header {{ text-align: center; border-bottom: 2px solid {color}; padding-bottom: 10px; margin-bottom: 20px; }}
          .header h2 {{ color: {color}; margin: 0; }}
          .content {{ line-height: 1.6; }}
          .status-box {{ background: #f8f9fa; border-left: 4px solid {color}; padding: 15px; border-radius: 5px; margin: 20px 0; }}
          .footer {{ text-align: center; margin-top: 30px; font-size: 12px; color: #777; }}
        </style>
      </head>
      <body>
        <div class="container">
          <div class="header">
            <h2>{icon} Actualización de Solicitud</h2>
          </div>
          <div class="content">
            <p>Hola <strong>{full_name}</strong>,</p>
            <p>Te informamos que tu solicitud de <strong>{reason_type}</strong> (Radicado #{req_id}) ha cambiado de estado.</p>
            
            <div class="status-box">
              <p><strong>Nuevo Estado:</strong> {new_status}</p>
              <p><strong>Mensaje/Detalle:</strong> {message}</p>
            </div>
            
            <p>Puedes verificar los detalles completos ingresando al Portal de Autogestión.</p>
          </div>
          <div class="footer">
            <p>Este es un mensaje automático del Sistema de RRHH Dolormed. No respondas a este correo.</p>
          </div>
        </div>
      </body>
    </html>
    """
    text = f"""
Actualización de Solicitud #{req_id}
Hola {full_name},
Tu solicitud de {reason_type} ha cambiado de estado.
Nuevo Estado: {new_status}
Mensaje: {message}
    """
    return _send_email([to_email], subject, html, text)
