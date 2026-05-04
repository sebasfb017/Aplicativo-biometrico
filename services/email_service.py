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

def _send_email(to_email, subject, html_content, text_content=""):
    config = load_smtp_config()
    
    sender_email = config.get("smtp_user", "")
    sender_password = config.get("smtp_password", "")
    smtp_server = config.get("smtp_server", "smtp.gmail.com")
    smtp_port = config.get("smtp_port", 587)
    sender_name = config.get("sender_name", "Nómina Dolormed")
    
    if not sender_email or not sender_password:
        return False, "Las credenciales SMTP no están configuradas."
        
    try:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = f"{sender_name} <{sender_email}>"
        
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
        
        if int(smtp_port) == 465:
            server = smtplib.SMTP_SSL(smtp_server, int(smtp_port))
        else:
            server = smtplib.SMTP(smtp_server, int(smtp_port))
            server.starttls()
            
        server.login(sender_email, sender_password)
        server.sendmail(sender_email, recipient, msg.as_string())
        server.quit()
        
        return True, "Correo enviado correctamente."
    except Exception as e:
        return False, str(e)

def send_welcome_email(to_email, full_name, username, password):
    subject = "Bienvenido al Portal de Nómina Dolormed"
    
    html = f"""
    <html>
      <head>
        <style>
          body {{ font-family: Arial, sans-serif; background-color: #f4f7f6; color: #333; }}
          .container {{ max-width: 600px; margin: 20px auto; background: #fff; border-radius: 8px; padding: 30px; box-shadow: 0 4px 6px rgba(0,0,0,0.1); }}
          .header {{ text-align: center; border-bottom: 2px solid #0d6efd; padding-bottom: 10px; margin-bottom: 20px; }}
          .header h2 {{ color: #0d6efd; margin: 0; }}
          .content {{ line-height: 1.6; }}
          .credentials {{ background: #f8f9fa; border: 1px dashed #ccc; padding: 15px; border-radius: 5px; margin: 20px 0; }}
          .credentials strong {{ display: inline-block; width: 100px; }}
          .footer {{ text-align: center; margin-top: 30px; font-size: 12px; color: #777; }}
        </style>
      </head>
      <body>
        <div class="container">
          <div class="header">
            <h2>Bienvenido a Nómina Dolormed</h2>
          </div>
          <div class="content">
            <p>Hola <strong>{full_name}</strong>,</p>
            <p>Se te ha creado un usuario para acceder al Portal de Autogestión de Empleados. Desde allí podrás consultar tus turnos, asistencias y radicar solicitudes de permisos (Novedades).</p>
            
            <div class="credentials">
              <p>Tus credenciales de acceso son:</p>
              <p><strong>Usuario:</strong> {username}</p>
              <p><strong>Contraseña:</strong> {password}</p>
            </div>
            
            <p>Te recomendamos cambiar tu contraseña una vez que ingreses al sistema por motivos de seguridad.</p>
          </div>
          <div class="footer">
            <p>Este es un mensaje automático. Por favor, no respondas a este correo.</p>
          </div>
        </div>
      </body>
    </html>
    """
    text = f"""
Hola {full_name},

Se te ha creado un usuario para acceder al Portal de Autogestión de Empleados. 
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
          .info-box {{ background: #fcf8e3; border: 1px solid #faebcc; padding: 15px; border-radius: 5px; margin: 20px 0; color: #8a6d3b; }}
          .info-box ul {{ margin: 0; padding-left: 20px; }}
          .footer {{ text-align: center; margin-top: 30px; font-size: 12px; color: #777; }}
        </style>
      </head>
      <body>
        <div class="container">
          <div class="header">
            <h2>Alerta de Novedad (Permiso Radicado)</h2>
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
            <p>Este es un mensaje automático. Por favor, no respondas a este correo.</p>
          </div>
        </div>
      </body>
    </html>
    """
    text = f"""
Alerta de Novedad (Permiso Radicado)

El empleado {full_name} acaba de radicar una nueva solicitud:
- Tipo: {reason_type}
- Fecha: {start_date}
- Tiempo: {total_time}
- Detalles: {details}

Por favor ingresa al portal administrativo para aprobar o rechazar esta solicitud.
"""
    return _send_email(to_emails, subject, html, text)
