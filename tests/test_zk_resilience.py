import socket
import time
from datetime import datetime
from unittest.mock import patch, MagicMock
import pandas as pd
import pytest

from services.zk_service import (
    check_socket_connection,
    zk_connection,
    download_attendance_from_device,
    fetch_attendance,
    clear_device_attendance_safe,
    DEFAULT_SOCKET_TIMEOUT,
)
from services.analytics import apply_business_punch_rules
from config import APP_CONFIG


def test_socket_precheck_timeout_configurable():
    """Fase 1.1: El timeout del pre-check de socket debe ser configurable (3.5s por defecto)."""
    assert DEFAULT_SOCKET_TIMEOUT == 3.5
    assert APP_CONFIG.get("zkteco", {}).get("socket_timeout") == 3.5


def test_socket_precheck_with_latency_2_5s():
    """
    Criterio de Aceptación 2:
    Simular mediante un mock de socket un retardo de 2.5 segundos:
    el biométrico NO debe marcarse como 'offline' (el timeout es de 3.5s).
    """
    def simulated_slow_socket(address, timeout=None):
        time.sleep(0.05)  # Breve simulación en test para no ralentizar la suite
        # Verificar que el timeout recibido por socket sea >= 2.5s (3.5s por defecto)
        assert timeout >= 2.5, f"Timeout de socket demasiado bajo: {timeout}"
        mock_sock = MagicMock()
        mock_sock.__enter__.return_value = mock_sock
        return mock_sock

    with patch("socket.create_connection", side_effect=simulated_slow_socket):
        is_online = check_socket_connection("192.168.52.6", 4370)
        assert is_online is True


def test_socket_precheck_unreachable_device():
    """Fase 1.1: Si el socket no responde dentro del timeout, debe marcarse como inalcanzable."""
    with patch("socket.create_connection", side_effect=socket.timeout("Connection timed out")):
        is_online = check_socket_connection("192.168.52.99", 4370, timeout=1.0, retries=1)
        assert is_online is False

        # download_attendance_from_device debe retornar el mensaje amigable de offline
        rows, err = download_attendance_from_device({"ip": "192.168.52.99", "port": 4370})
        assert rows == []
        assert err == "El equipo está apagado o sin red."


def test_zk_connection_context_manager_lifecycle():
    """Fase 1.2: El context manager zk_connection conecta y garantiza disconnect() al salir."""
    mock_conn = MagicMock()
    mock_zk_instance = MagicMock()
    mock_zk_instance.connect.return_value = mock_conn

    device = {"ip": "192.168.52.6", "port": 4370, "password": 0, "timeout": 10}

    with patch("services.zk_service.ZK", return_value=mock_zk_instance):
        with zk_connection(device) as conn:
            assert conn == mock_conn
            mock_zk_instance.connect.assert_called_once()
            # Aún no debe haberse desconectado mientras el bloque with esté activo
            mock_conn.disconnect.assert_not_called()

        # Al salir del bloque with, conn.disconnect() debe ser invocado
        mock_conn.disconnect.assert_called_once()


def test_zk_connection_guarantees_disconnect_on_exception():
    """Fase 1.2: zk_connection garantiza cierre de socket incluso ante excepciones no controladas."""
    mock_conn = MagicMock()
    mock_zk_instance = MagicMock()
    mock_zk_instance.connect.return_value = mock_conn

    device = {"ip": "192.168.52.7", "port": 4370}

    with patch("services.zk_service.ZK", return_value=mock_zk_instance):
        with pytest.raises(ValueError, match="Error simulado en procesamiento"):
            with zk_connection(device) as conn:
                raise ValueError("Error simulado en procesamiento")

        # conn.disconnect() DEBE haberse ejecutado en el bloque finally
        mock_conn.disconnect.assert_called_once()


def test_raw_data_purity_punch_not_modified_by_zk_driver():
    """
    Criterio de Aceptación 3:
    download_attendance_from_device() y fetch_attendance() deben entregar los registros originales
    del hardware sin mutar el valor de punch (ej. marcación a las 14:15 con punch=1 sigue siendo 1).
    """
    # Crear registros simulados de hardware con marcaciones en la hora 14
    r1 = MagicMock()
    r1.user_id = "111222333"
    r1.timestamp = datetime(2026, 10, 19, 14, 15, 0)
    r1.status = 1
    r1.punch = 1  # Salida física en el reloj a las 2:15 PM
    r1.uid = 10

    r2 = MagicMock()
    r2.user_id = "111222333"
    r2.timestamp = datetime(2026, 10, 19, 8, 0, 0)
    r2.status = 1
    r2.punch = 0  # Entrada física
    r2.uid = 11

    mock_conn = MagicMock()
    mock_conn.get_attendance.return_value = [r1, r2]

    mock_zk = MagicMock()
    mock_zk.connect.return_value = mock_conn

    device = {"ip": "192.168.52.8", "name": "Reloj Principal"}

    with patch("services.zk_service.check_socket_connection", return_value=True), patch(
        "services.zk_service.ZK", return_value=mock_zk
    ):
        records, err = download_attendance_from_device(device)
        assert err is None
        assert len(records) == 2

        rec_14 = next(r for r in records if "14:15" in r["ts"])
        # PUREZA: El valor original de punch=1 NO debe haber sido sobreescrito con 0 por el driver
        assert rec_14["punch"] == 1, f"El driver ZK alteró los datos crudos: punch={rec_14['punch']}"

        # fetch_attendance debe ser un alias idéntico
        alias_records, alias_err = fetch_attendance(device)
        assert alias_err is None
        assert alias_records == records


def test_analytics_layer_applies_14_hour_business_rule():
    """
    Fase 1.3: La regla de negocio de las 14:00 horas se aplica en la capa analítica (analytics.py),
    transformando punch a 0 únicamente para cálculos de asistencia sin contaminar el hardware.
    """
    raw_df = pd.DataFrame(
        [
            {"user_id": "111", "ts": "2026-10-19 14:15:00", "punch": 1},
            {"user_id": "111", "ts": "2026-10-19 08:00:00", "punch": 1},
            {"user_id": "222", "ts": "2026-10-19 14:59:59", "punch": 2},
        ]
    )

    transformed_df = apply_business_punch_rules(raw_df)

    # Las marcaciones de las 14:XX deben ser reclasificadas a punch=0
    assert transformed_df.loc[0, "punch"] == 0
    assert transformed_df.loc[2, "punch"] == 0

    # Las marcaciones de otras horas deben conservar su punch intacto
    assert transformed_df.loc[1, "punch"] == 1


def test_clear_device_attendance_safe_flow():
    """Valida que clear_device_attendance_safe descargue, guarde y limpie usando zk_connection."""
    mock_conn = MagicMock()
    mock_zk = MagicMock()
    mock_zk.connect.return_value = mock_conn

    device = {"ip": "192.168.52.10", "name": "Reloj Terapia"}

    with patch("services.zk_service.download_attendance_from_device", return_value=([], None)), patch(
        "services.zk_service.upsert_attendance", return_value=(0, 0)
    ), patch("services.zk_service.ZK", return_value=mock_zk):
        ok, msg = clear_device_attendance_safe(device)
        assert ok is True
        mock_conn.clear_attendance.assert_called_once()
        mock_conn.disconnect.assert_called_once()
