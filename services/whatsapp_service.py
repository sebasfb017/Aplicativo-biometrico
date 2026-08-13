import os
import requests
import streamlit as st

# Webhook de n8n para enviar mensajes por WhatsApp.
# Se recomienda colocar esto en los st.secrets de Streamlit a futuro.
N8N_WEBHOOK_URL = os.environ.get("N8N_WHATSAPP_WEBHOOK", "http://192.168.52.29:5678/webhook/d4387103-a20e-4576-8ef1-6eb644d9aa7b")

def send_status_update_whatsapp(
    phone_number: str,
    full_name: str,
    req_id: int,
    reason_type: str,
    new_status: str,
    message: str,
    approver_name: str = None,
):
    """
    Envía una notificación por WhatsApp al empleado cuando cambia el estado de su solicitud,
    replicando el mismo formato del correo electrónico.
    """
    if not phone_number:
        print(f"⚠️ No hay número de teléfono para enviar WhatsApp (Req #{req_id})")
        return False

    # Limpiar el número (remover espacios, signos + si es necesario, etc.)
    # Si los números en la BD no tienen código de país (ej. Colombia es 57), deberías agregarlo aquí.
    clean_phone = phone_number.replace("+", "").replace(" ", "").strip()
    if len(clean_phone) == 10:  # Si es número local colombiano sin indicativo
        clean_phone = f"57{clean_phone}"

    # Construir el mensaje formateado con negritas para WhatsApp
    whatsapp_text = (
        f"✅ *Actualización de Novedad*\n\n"
        f"Hola *{full_name}*,\n"
        f"Te informamos que tu solicitud de *{reason_type}* (Radicado #{req_id}) ha cambiado de estado en nuestro sistema.\n\n"
        f"📌 *Nuevo Estado:* {new_status}\n"
        f"📝 *Observaciones:* {message}\n"
    )

    if approver_name:
        whatsapp_text += f"👤 *Procesado por:* {approver_name}\n"

    whatsapp_text += "\n_Puedes consultar el historial completo o descargar tus comprobantes directamente en el Portal Web._"

    payload = {
        "phone": clean_phone,
        "message": whatsapp_text,
        "req_id": req_id,
        "program_id": "zkteco_nomina_app"
    }

    try:
        # Enviar petición POST al Webhook de n8n
        response = requests.post(N8N_WEBHOOK_URL, json=payload, timeout=10)
        
        if response.status_code in [200, 201]:
            print(f"📱 [WHATSAPP] Notificación enviada a {clean_phone} | Req #{req_id}")
            return True
        else:
            print(f"❌ [WHATSAPP ERROR] HTTP {response.status_code}: {response.text}")
            return False
            
    except requests.exceptions.RequestException as e:
        print(f"❌ [WHATSAPP ERROR] No se pudo contactar a n8n: {e}")
        return False

def send_novedad_alert_whatsapp(
    target_phones: list,
    full_name: str,
    reason_type: str,
    details: str,
    total_time: str,
    start_date: str,
    prev_approver=None,
):
    """
    Envía una alerta a los líderes (Coordinador, Jefe, RRHH) informando que
    el empleado acaba de radicar una nueva solicitud.
    """
    if not target_phones:
        return False

    # Construir el mensaje
    whatsapp_text = (
        f"⚠️ *Alerta de Nueva Novedad*\n\n"
        f"El empleado *{full_name}* acaba de radicar una nueva solicitud en el portal que requiere revisión.\n\n"
        f"📋 *Tipo de Solicitud:* {reason_type}\n"
        f"📅 *Fecha de Inicio:* {start_date}\n"
        f"⏳ *Tiempo Solicitado:* {total_time}\n"
        f"📝 *Detalles:* {details}\n"
    )

    if prev_approver:
        whatsapp_text += f"\n✅ _Aprobado previamente por:_ {prev_approver}\n"

    whatsapp_text += "\n_Ingresa al portal administrativo para gestionar esta solicitud._"

    success_count = 0
    for phone in set(target_phones):
        if not phone: continue
        
        clean_phone = str(phone).replace("+", "").replace(" ", "").strip()
        if len(clean_phone) == 10:
            clean_phone = f"57{clean_phone}"

        payload = {
            "phone": clean_phone,
            "message": whatsapp_text,
            "req_id": 0, # 0 indicando que es una alerta general (opcional)
            "program_id": "zkteco_nomina_app"
        }

        try:
            res = requests.post(N8N_WEBHOOK_URL, json=payload, timeout=10)
            if res.status_code in [200, 201]:
                print(f"📱 [WHATSAPP ALERT] Enviado a líder {clean_phone}")
                success_count += 1
        except Exception as e:
            print(f"❌ [WHATSAPP ERROR LÍDER] {e}")

    return success_count > 0
