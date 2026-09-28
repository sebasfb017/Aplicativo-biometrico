"""
reminder_job.py
---------------
Job de recordatorios automáticos para aprobadores con permisos pendientes.

Lógica:
- Detecta permisos con más de DAYS_THRESHOLD días sin respuesta.
- Agrupa los permisos pendientes por aprobador (correo).
- Envía UN solo correo resumen a cada aprobador con la lista completa.
- Se ejecuta en segundo plano (hilo daemon) al iniciar sesión.

La clave 'reminder_job_last_run' en session_state evita que se dispare
múltiples veces en la misma sesión/día.
"""

from datetime import date, datetime

DAYS_THRESHOLD = 2  # Días sin respuesta antes de enviar recordatorio


def run_reminder_job_if_needed():
    """
    Punto de entrada principal. Llama esta función en el flujo de login.
    Solo se ejecuta una vez por sesión y una vez por día.
    """
    import threading
    import streamlit as st

    today_str = str(date.today())
    last_run = st.session_state.get("reminder_job_last_run", "")

    if last_run == today_str:
        return  # Ya se ejecutó hoy en esta sesión

    st.session_state["reminder_job_last_run"] = today_str

    # Lanzar en hilo daemon para no bloquear el login
    thread = threading.Thread(target=_execute_reminder_job, daemon=True)
    thread.start()


def _execute_reminder_job():
    """
    Lógica interna del job. Se ejecuta en segundo plano.
    """
    try:
        from database_conn.queries import db_get_stale_pending_requests
        from services.email_service import send_approver_reminder

        stale_requests = db_get_stale_pending_requests(days_threshold=DAYS_THRESHOLD)

        if not stale_requests:
            return  # No hay nada que recordar

        # Calcular días de espera y agrupar por aprobador
        today = date.today()
        by_approver = {}

        for req in stale_requests:
            # Calcular cuántos días lleva esperando
            try:
                req_date = datetime.strptime(req["request_date"], "%Y-%m-%d").date()
                req["days_waiting"] = (today - req_date).days
            except Exception:
                req["days_waiting"] = "?"

            approver_email = req.get("approver_email")
            if not approver_email:
                continue

            if approver_email not in by_approver:
                by_approver[approver_email] = {
                    "role": req.get("approver_role", "Aprobador"),
                    "requests": [],
                }
            by_approver[approver_email]["requests"].append(req)

        # Enviar un correo por cada aprobador
        for email, data in by_approver.items():
            send_approver_reminder(
                approver_email=email,
                approver_role=data["role"],
                pending_requests=data["requests"],
            )

        print(
            f"[ReminderJob] {datetime.now().strftime('%Y-%m-%d %H:%M')} — "
            f"Recordatorios enviados a {len(by_approver)} aprobador(es) "
            f"para {len(stale_requests)} solicitud(es) estancada(s)."
        )

    except Exception as e:
        print(f"[ReminderJob] Error en job de recordatorios: {e}")
