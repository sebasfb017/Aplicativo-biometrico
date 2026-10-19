import logging
import os
import requests
import streamlit as st

logger = logging.getLogger("whatsapp_service")


def get_n8n_webhook_url() -> str:
    """Obtiene la URL del webhook de WhatsApp priorizando variables de entorno y secrets."""
    url = os.getenv("N8N_WEBHOOK_URL") or os.getenv("N8N_WHATSAPP_WEBHOOK")
    if url:
        return url
    try:
        if hasattr(st, "secrets") and "n8n" in st.secrets:
            sec_url = st.secrets["n8n"].get("webhook_url")
            if sec_url:
                return sec_url
    except Exception:
        pass
    return "http://192.168.52.29:5678/webhook/d4387103-a20e-4576-8ef1-6eb644d9aa7b"


N8N_WEBHOOK_URL = get_n8n_webhook_url()


def _send_status_update_whatsapp_sync(
    phone_number: str,
    full_name: str,
    req_id: int,
    reason_type: str,
    new_status: str,
    message: str,
    approver_name: str = None,
) -> bool:
    """Envía la petición HTTP a n8n de forma síncrona con timeout de 5 segundos."""
    if not phone_number:
        logger.warning("No hay número de teléfono para enviar WhatsApp (Req #%s)", req_id)
        return False

    clean_phone = phone_number.replace("+", "").replace(" ", "").strip()
    if len(clean_phone) == 10:  # Si es número local colombiano sin indicativo
        clean_phone = f"57{clean_phone}"

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
        "program_id": "zkteco_nomina_app",
    }

    try:
        target_url = get_n8n_webhook_url()
        # Timeout estricto de 5 segundos para resiliencia de red
        response = requests.post(target_url, json=payload, timeout=5)

        if response.status_code in [200, 201]:
            logger.info("📱 [WHATSAPP] Notificación enviada a %s | Req #%s", clean_phone, req_id)
            return True
        else:
            err_msg = f"HTTP {response.status_code}: {response.text}"
            logger.error("❌ [WHATSAPP ERROR] %s", err_msg)
            from services.notifications import log_notification_failure

            log_notification_failure("WhatsApp", clean_phone, err_msg, req_id=req_id)
            return False

    except requests.exceptions.RequestException as e:
        err_msg = f"No se pudo contactar a n8n: {e}"
        logger.error("❌ [WHATSAPP ERROR] %s", err_msg)
        from services.notifications import log_notification_failure

        log_notification_failure("WhatsApp", clean_phone, err_msg, req_id=req_id)
        return False
    except Exception as e:
        err_msg = f"Error inesperado enviando WhatsApp: {e}"
        logger.error("❌ [WHATSAPP ERROR] %s", err_msg)
        from services.notifications import log_notification_failure

        log_notification_failure("WhatsApp", clean_phone, err_msg, req_id=req_id)
        return False


def send_status_update_whatsapp(
    phone_number: str,
    full_name: str,
    req_id: int,
    reason_type: str,
    new_status: str,
    message: str,
    approver_name: str = None,
    sync: bool = False,
):
    """
    Envía una notificación por WhatsApp al empleado cuando cambia el estado de su solicitud.
    Por defecto se ejecuta de forma asíncrona no bloqueante (Fire-and-Forget en worker pool)
    para garantizar respuesta ultra-rápida en la UI de Streamlit.
    """
    if sync:
        return _send_status_update_whatsapp_sync(
            phone_number,
            full_name,
            req_id,
            reason_type,
            new_status,
            message,
            approver_name,
        )

    from services.notifications import dispatch_notification_async

    return dispatch_notification_async(
        _send_status_update_whatsapp_sync,
        phone_number,
        full_name,
        req_id,
        reason_type,
        new_status,
        message,
        approver_name,
    )


def _send_novedad_alert_whatsapp_sync(
    target_phones: list,
    full_name: str,
    reason_type: str,
    details: str,
    total_time: str,
    start_date: str,
    prev_approver=None,
) -> bool:
    """Envía alertas a líderes de forma síncrona con timeout de 5 segundos."""
    if not target_phones:
        return False

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
        if not phone:
            continue

        clean_phone = str(phone).replace("+", "").replace(" ", "").strip()
        if len(clean_phone) == 10:
            clean_phone = f"57{clean_phone}"

        payload = {
            "phone": clean_phone,
            "message": whatsapp_text,
            "req_id": 0,
            "program_id": "zkteco_nomina_app",
        }

        try:
            target_url = get_n8n_webhook_url()
            res = requests.post(target_url, json=payload, timeout=5)
            if res.status_code in [200, 201]:
                logger.info("📱 [WHATSAPP ALERT] Enviado a líder %s", clean_phone)
                success_count += 1
            else:
                err_msg = f"HTTP {res.status_code}: {res.text}"
                logger.error("❌ [WHATSAPP ERROR LÍDER] %s", err_msg)
                from services.notifications import log_notification_failure

                log_notification_failure("WhatsApp", clean_phone, err_msg)
        except requests.exceptions.RequestException as e:
            err_msg = f"Timeout o error de red hacia n8n: {e}"
            logger.error("❌ [WHATSAPP ERROR LÍDER] %s", err_msg)
            from services.notifications import log_notification_failure

            log_notification_failure("WhatsApp", clean_phone, err_msg)
        except Exception as e:
            err_msg = f"Error inesperado al enviar WhatsApp a líder: {e}"
            logger.error("❌ [WHATSAPP ERROR LÍDER] %s", err_msg)
            from services.notifications import log_notification_failure

            log_notification_failure("WhatsApp", clean_phone, err_msg)

    return success_count > 0


def send_novedad_alert_whatsapp(
    target_phones: list,
    full_name: str,
    reason_type: str,
    details: str,
    total_time: str,
    start_date: str,
    prev_approver=None,
    sync: bool = False,
):
    """
    Envía una alerta a los líderes informando nueva radicación de solicitud.
    Despachado asíncronamente por defecto para no congelar la UI.
    """
    if sync:
        return _send_novedad_alert_whatsapp_sync(
            target_phones,
            full_name,
            reason_type,
            details,
            total_time,
            start_date,
            prev_approver,
        )

    from services.notifications import dispatch_notification_async

    return dispatch_notification_async(
        _send_novedad_alert_whatsapp_sync,
        target_phones,
        full_name,
        reason_type,
        details,
        total_time,
        start_date,
        prev_approver,
    )
