# Plan Técnico: 002 - Resiliencia de Servicios Externos y Hardware ZKTeco

## 1. Arquitectura de Solución Propuesta

### 1.1. Manejador Robusto de Conexión a Biométricos (`services/zk_service.py`)
- Modificar el pre-check de socket TCP:
  - Elevar el timeout por defecto de 1.5s a un rango inteligente: 3.5 segundos con 2 reintentos rápidos antes de declarar el dispositivo como inalcanzable.
  - Soportar detección automática de modo VPN / WAN.
- Envoltorio de contexto para sesiones ZK:
  ```python
  @contextmanager
  def zk_connection(device_config):
      # Crea cliente, conecta con reintentos y asegura disconnect() en finally
  ```
- Separación de datos crudos: `fetch_attendance()` debe retornar las marcaciones intactas tal como las reporta el reloj (`punch_type` original). La regla de negocio de reclasificación horaria se traslada a la consulta de análisis o normalización en `analytics.py`.

### 1.2. Despacho Asíncrono de Notificaciones (`services/async_notifier.py` o módulo interno)
- Crear un despachador en segundo plano no bloqueante para Streamlit:
  ```python
  import threading
  
  def dispatch_notification_async(target_func, *args, **kwargs):
      t = threading.Thread(target=target_func, args=args, kwargs=kwargs, daemon=True)
      t.start()
  ```
- Integrar este despachador en:
  - `whatsapp_service.send_status_update_whatsapp(...)`
  - `email_service.send_novedad_alert(...)`
  - `email_service.send_approver_reminder(...)`
- Con esta arquitectura, el usuario que aprueba o rechaza en la UI experimenta una respuesta instantánea (< 150 ms) mientras las peticiones HTTP/SMTP se resuelven de fondo en hilos daemon.

### 1.3. Circuit Breaker y Timeouts en Webhooks n8n
- Configurar timeout estricto de 5 segundos en `requests.post()` para WhatsApp.
- Si n8n responde con error 5xx o timeout, registrar el evento en `audit_logs` con estado `FAILED_DELIVERY` sin romper la navegación del usuario.

### 1.4. Logging Estructurado
- Reemplazar todas las sentencias `print()` en `services/` por `logging.getLogger(__name__)`.
- Formatear con marca temporal, nivel de severidad y contexto del dispositivo (`device_ip`, `device_name`).

---

## 2. Plan de Rollback y Mitigación de Riesgos
- **Riesgo**: Desborde de hilos daemon si hay avalancha masiva de notificaciones simultáneas.
  - *Mitigación*: Implementar un `ThreadPoolExecutor(max_workers=5)` reutilizable a nivel de proceso en lugar de crear hilos sin límite.
- **Riesgo**: Discrepancias en reportes de asistencia si se remueve la reclasificación forzada de las 14:00 horas.
  - *Mitigación*: Mantener la regla de las 14:00 hs como capa de transformación de datos durante el cálculo de nómina en `services/analytics.py` (de forma transparente para el usuario final).
