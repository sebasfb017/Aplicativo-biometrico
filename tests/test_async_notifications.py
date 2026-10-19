import time
from unittest.mock import patch, MagicMock
import pytest
import requests

from services.notifications import (
    NOTIFICATION_EXECUTOR,
    dispatch_notification_async,
    notify_employee_status,
    log_audit,
    log_notification_failure,
)
from services.whatsapp_service import (
    send_status_update_whatsapp,
    send_novedad_alert_whatsapp,
    get_n8n_webhook_url,
)
from services.email_service import (
    send_novedad_alert,
    send_approver_reminder,
    _send_email_sync,
)
from database_conn.connection import db_session


def test_thread_pool_executor_configuration():
    """Fase 2.1: El ThreadPoolExecutor compartido debe tener exactamente max_workers=5."""
    assert NOTIFICATION_EXECUTOR._max_workers == 5
    # Verificar que dispatch_notification_async ejecuta en el pool
    future = dispatch_notification_async(lambda x: x * 2, 21)
    assert future.result(timeout=2.0) == 42


def test_ui_approval_latency_under_300ms():
    """
    Criterio de Aceptación 1:
    Una aprobación o rechazo en la UI debe retornar el control en menos de 300 ms,
    incluso si WhatsApp y SMTP sufren de alta latencia de red (simulada en 1.5s cada uno).
    """
    def slow_post(*args, **kwargs):
        time.sleep(1.0)
        resp = MagicMock()
        resp.status_code = 200
        return resp

    def slow_smtp(*args, **kwargs):
        time.sleep(1.0)
        return True, "Enviado con delay"

    # Preparar usuario de prueba en BD
    with db_session() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO users_app (username, full_name, role, password_hash, created_at, emp_email, emp_phone)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (username) DO UPDATE SET emp_email = EXCLUDED.emp_email, emp_phone = EXCLUDED.emp_phone
                """,
                ("test_latency_user", "Test Latency User", "empleado", b"hash", "2026-10-19", "test@dolormed.co", "3001234567"),
            )

    with patch("requests.post", side_effect=slow_post), patch(
        "services.email_service._send_email_sync", side_effect=slow_smtp
    ):
        start_time = time.perf_counter()

        # Invocación de la notificación de aprobación desde la UI
        future = notify_employee_status(
            user_id="test_latency_user",
            full_name="Test Latency User",
            req_id=999,
            reason_type="Permiso Personal",
            new_status="PRE-APROBADA",
            message="Aprobado por jefe inmediato",
            approver_name="Dr. Gomez",
            sync=False,
        )

        elapsed_ms = (time.perf_counter() - start_time) * 1000

        # Debe retornar inmediatamente al hilo de Streamlit (< 300 ms)
        assert elapsed_ms < 300, f"Latencia de UI excesiva: {elapsed_ms:.2f} ms (límite 300 ms)"

        # Esperar a que la tarea en segundo plano termine para verificar ejecución completa
        if future:
            future.result(timeout=4.0)


def test_send_status_update_whatsapp_async_and_timeout():
    """Fase 2.2 y 2.4: send_status_update_whatsapp despacha async y timeout estricto de 5s."""
    captured_timeout = None

    def mock_post(url, json=None, timeout=None):
        nonlocal captured_timeout
        captured_timeout = timeout
        time.sleep(0.5)
        resp = MagicMock()
        resp.status_code = 200
        return resp

    with patch("requests.post", side_effect=mock_post):
        start_t = time.perf_counter()
        fut = send_status_update_whatsapp(
            phone_number="3101112233",
            full_name="Carlos Ruiz",
            req_id=101,
            reason_type="Calamidad Doméstica",
            new_status="APROBADA",
            message="Tu solicitud fue aprobada",
            approver_name="Talento Humano",
            sync=False,
        )
        duration_ms = (time.perf_counter() - start_t) * 1000

        # UI retorna de inmediato
        assert duration_ms < 150
        assert fut is not None
        assert fut.result(timeout=2.0) is True
        assert captured_timeout == 5


def test_whatsapp_failure_logged_to_audit_logs():
    """Fase 3.1: Si n8n responde 500 o Timeout, se registra NOTIFICATION_FAILURE en audit_logs."""
    with patch("requests.post", side_effect=requests.exceptions.Timeout("n8n timeout 5s")):
        success = send_status_update_whatsapp(
            phone_number="3009998877",
            full_name="Maria Perez",
            req_id=202,
            reason_type="Cita Médica",
            new_status="RECHAZADA",
            message="No cumple requisitos",
            approver_name="Jefe Inmediato",
            sync=True,
        )
        assert success is False

    # Verificar que se grabó en audit_logs
    with db_session() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT action, details FROM audit_logs
                WHERE action = 'NOTIFICATION_FAILURE'
                ORDER BY id DESC LIMIT 1
                """
            )
            row = cur.fetchone()
            assert row is not None
            assert row[0] == "NOTIFICATION_FAILURE"
            assert "WHATSAPP" in row[1].upper()
            assert "573009998877" in row[1]
            assert "202" in row[1]


def test_email_failure_logged_to_audit_logs():
    """Fase 3.1: Si SMTP falla, se registra NOTIFICATION_FAILURE en audit_logs."""
    with patch("services.email_service._send_email_sync", return_value=(False, "Connection refused 587")):
        ok, msg = send_novedad_alert(
            to_emails=["lider@dolormed.co"],
            full_name="Ana Lopez",
            reason_type="Vacaciones",
            details="15 dias habiles",
            total_time="15 dias",
            start_date="2026-11-01",
            sync=True,
        )
        assert ok is False

    # Disparo asíncrono con fallo
    with patch("services.email_service._send_email_sync", return_value=(False, "Authentication rejected")):
        send_approver_reminder(
            approver_email="jefe@dolormed.co",
            approver_role="Jefe de Area",
            pending_requests=[{"id": 303, "employee_name": "Ana", "reason_type": "Vacaciones", "leave_date_start": "2026-11-01", "days_waiting": 3}],
            sync=False,
        )
        # Dar breve tiempo para que el worker procese el fallo
        time.sleep(0.3)

    # Verificar registro en audit_logs
    with db_session() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT action, details FROM audit_logs
                WHERE action = 'NOTIFICATION_FAILURE' AND details LIKE '%%EMAIL%%'
                ORDER BY id DESC LIMIT 1
                """
            )
            row = cur.fetchone()
            assert row is not None
            assert "jefe@dolormed.co" in row[1] or "lider@dolormed.co" in row[1]


def test_services_down_deliberate_ui_survival():
    """Fase 3.2: Cuando n8n y SMTP están totalmente caídos, la app no arroja excepciones a la UI."""
    with patch("requests.post", side_effect=ConnectionRefusedError("n8n offline")), patch(
        "smtplib.SMTP", side_effect=ConnectionRefusedError("SMTP offline")
    ):
        # notify_employee_status debe completar sin lanzar excepción
        fut = notify_employee_status(
            user_id="test_latency_user",
            full_name="Test Latency User",
            req_id=404,
            reason_type="Permiso",
            new_status="FINALIZADA",
            message="Completado sin red",
            approver_name="RRHH",
            sync=False,
        )
        if fut:
            fut.result(timeout=3.0)
