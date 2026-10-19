# Tareas: 002 - Resiliencia de Servicios Externos y Hardware ZKTeco

## Lista de Tareas Secuenciales

### Fase 1: Resiliencia de Red y Hardware ZKTeco
- [x] 1.1 Modificar en `services/zk_service.py` el timeout del pre-check de socket de 1.5s a 3.5s configurable vía `APP_CONFIG`.
- [x] 1.2 Implementar el context manager `zk_connection()` para garantizar cierre estricto del socket ZK y evitar estados zombies en los relojes.
- [x] 1.3 Extraer la regla de sobreescritura de las 14:00 horas (`services/zk_service.py:138-142`) y reubicarla en `services/analytics.py`.
- [x] 1.4 Reemplazar todas las apariciones de `print()` en `services/zk_service.py` por `logging.getLogger("zk_service")`.

### Fase 2: Desacoplamiento Asíncrono de Mensajería
- [x] 2.1 Crear un `ThreadPoolExecutor(max_workers=5)` compartido para tareas en segundo plano en `services/notifications.py` o módulo de utilidades.
- [x] 2.2 Envolver `send_status_update_whatsapp` para ejecutarse de forma asíncrona no bloqueante cuando es invocado desde la UI.
- [x] 2.3 Envolver `send_novedad_alert` y `send_approver_reminder` en `services/email_service.py` para despacho asíncrono.
- [x] 2.4 Agregar timeout de 5s y manejo controlado de excepciones en peticiones hacia el webhook de n8n.

### Fase 3: Trazabilidad y Fallbacks
- [x] 3.1 Registrar fallos de entrega de WhatsApp o correo en `audit_logs` con acción `NOTIFICATION_FAILURE`.
- [x] 3.2 Probar el comportamiento de la interfaz cuando el servicio n8n o el servidor SMTP están deliberadamente caídos.

---

## Criterios de Aceptación y Autoverificación con Pytest

1. **Latencia de Aprobación en UI**:
   - Una aprobación o rechazo en la interfaz de Streamlit debe retornar el control a la UI en menos de 300 ms, incluso con alta latencia de red simulada en el envío de correo o WhatsApp.
2. **Resiliencia de Hardware**:
   - Simular mediante un mock de socket un retardo de 2.5 segundos: el biométrico NO debe marcarse como "offline".
3. **Pureza de Datos Crudos**:
   - `fetch_attendance()` debe entregar los registros originales del hardware sin mutar el valor de `punch`.
4. **Prueba Automatizada**:
   - Crear test unitario `tests/test_async_notifications.py` que valide que la función de aprobación completa su flujo sin bloquear el hilo principal.
